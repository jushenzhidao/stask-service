"""兜底 sweeper（设计 §8、§9）：卡死收敛 / 超龄判死 / 槽位校准 / 结果清理。

本服务零资金动作，所以不需要「对账」——卡死任务的处理规则很简单：

1. **卡死重投**（``sweep_stale``）：非终态且长时间无进展的任务，若派发锁
   已过期（= 没有调用在飞），重新入队执行；锁还在则跳过等下一轮。
   补的是「入队消息丢失」这条路径——任务落库了但队列消息没了。
2. **超龄判死**（``sweep_overdue``）：超过最大生命期（默认 6h）仍非终态
   → 直接 FAILURE。必须先于 new-api 的 24h 超时清理收敛自己的行。
3. **槽位校准**（``recalibrate_slots``）：Redis 计数按 tasks 表事实双向修正。
4. **结果清理**（``purge_results``）：超期 ``upstream_response`` 置空。
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Sequence
from typing import TypeVar

from app.config import settings
from app.logging import log
from app.redis import K_DISPATCH, K_SWEEP_LOCK, r
from app.schemas import ACTIVE, FAILURE
from app.services import dynconf, slots, taskstore, tokensession

T = TypeVar("T")

#: 批内并发上限。批最大 200 条、每条 2~3 次 DB + 1 次 Redis 往返，
#: 串行时一轮要几百个 RTT；开并发但必须有界——sweeper 与在线请求
#: 共用同一个连接池，无界 gather 会把池吃干导致提交链路排队。
_SWEEP_CONCURRENCY = 8


def now() -> int:
    """时间入口统一走 taskstore（测试可注入固定时钟）。"""
    return taskstore.now()


async def _lock(job: str, ttl: int) -> bool:
    """定时任务重入锁（scheduler 误起多份时保证同轮单跑）。"""
    try:
        return bool(await r.set(K_SWEEP_LOCK.format(job=job), "1", ex=ttl, nx=True))
    except Exception:
        log.opt(exception=True).warning("sweep lock unavailable: job={}", job)
        return False


async def _gather_bounded(
    items: Sequence[T],
    handler: Callable[[T], Awaitable[bool]],
    *,
    label: str,
) -> int:
    """有界并发跑一批，返回成功计数；单条异常只记日志不中断整批。"""
    sem = asyncio.Semaphore(_SWEEP_CONCURRENCY)

    async def run(item: T) -> bool:
        async with sem:
            try:
                return await handler(item)
            except Exception:
                log.opt(exception=True).error("{} sweep item failed: {}", label, item)
                return False

    results = await asyncio.gather(*(run(item) for item in items))
    return sum(1 for ok in results if ok)


async def _kill(task: dict, reason: str) -> bool:
    """判死一个非终态任务：CAS → 释放槽 → 清会话 → 可选回调。

    入参是**已查好的元数据行**（不是 task_id）：调用方扫描时已经拿到了
    整行，再回查一次纯属浪费——批量判死时那是每条一条多余查询。
    """
    task_id = str(task["task_id"])
    if task["status"] not in ACTIVE:
        return False
    data: dict = task.get("data") or {}
    won = await taskstore.cas(
        task_id, ACTIVE, FAILURE, fail_reason=reason,
    )
    if not won:
        return False
    token_hash = str(data.get("token_hash") or "")
    if token_hash:
        await slots.release(token_hash)
    await tokensession.clear(task_id)
    callback_url = str(data.get("callback_url") or "")
    if callback_url:
        from app.queue import publish_notify

        try:
            await publish_notify(task_id)
        except Exception:
            log.opt(exception=True).warning("notify enqueue failed: task_id={}", task_id)
    return True


async def sweep_stale() -> dict:
    """卡死任务收敛——补上「入队消息丢失」这条路径。

    问题：提交链路落库 QUEUED 之后才入队。如果 broker 抖动/Redis 重启
    把消息丢了，worker 永远不会执行这个任务，客户端查询永远 202。

    做法：超龄无进展的非终态任务，看派发锁——
    - **锁已过期** = 没有调用在飞 = 重投安全（真正的防重仍由锁把关）；
    - **锁还在** = 一次调用可能在飞 = 跳过，等锁过期后的下一轮。

    超龄阈值取 ``worker_timeout + margin + 60``：正常执行中的任务绝不会
    被误捞（IN_PROGRESS 期间 updated_at 就是派发那一刻，一次调用最长
    worker_timeout，锁 TTL = timeout + margin）。
    """
    if not await _lock("stale", 110):
        return {"skipped": "locked"}

    threshold = (await dynconf.get_int("worker_timeout")
                 + await dynconf.get_int("dispatch_lock_margin_seconds") + 60)
    tasks = await taskstore.stale_active(
        threshold, await dynconf.get_int("sweep_batch_limit")
    )
    killed = 0

    async def handle(task: dict) -> bool:
        nonlocal killed
        task_id = str(task["task_id"])
        if task["status"] not in ACTIVE:
            return False
        try:
            lock_alive = await r.get(K_DISPATCH.format(task_id=task_id)) is not None
        except Exception:
            lock_alive = True              # Redis 不可用：保守视为在飞
        if lock_alive:
            return False
        # 锁已过期：上一次调用（若有）早已结束。已派发过（epoch>0）的
        # 任务结果已不可得，判死；从未派发过的（消息丢失）重投一次。
        if int((task.get("data") or {}).get("dispatch_epoch") or 0) > 0:
            if await _kill(task, "stale after dispatch (result unavailable)"):
                killed += 1
            return False
        from app.queue import publish_execute

        await publish_execute(task_id)
        await taskstore.patch_data(task_id, {"requeued_at": now()})
        log.warning("stale task requeued: task_id={}", task_id)
        return True

    requeued = await _gather_bounded(tasks, handle, label="stale")
    if requeued or killed:
        log.info("stale sweep: scanned={} requeued={} killed={}",
                 len(tasks), requeued, killed)
    return {"scanned": len(tasks), "requeued": requeued, "killed": killed}


async def sweep_overdue() -> dict:
    """超龄判死：超过最大生命期仍非终态 → FAILURE。

    生命期（默认 6h）必须**远小于** new-api 的 24h 超时清理线——上游的
    sweepTimedOutTasks 不过滤 platform，我们必须先于它收敛自己的行
    （quota=0 使得它即便动到本行，退款也是 0，双保险）。
    """
    if not await _lock("overdue", 110):
        return {"skipped": "locked"}
    lifetime = await dynconf.get_int("task_max_lifetime_seconds")
    tasks = await taskstore.overdue_active(
        lifetime, await dynconf.get_int("sweep_batch_limit")
    )
    reason = f"task exceeded max lifetime ({lifetime}s)"
    killed = await _gather_bounded(
        tasks, lambda task: _kill(task, reason), label="overdue"
    )
    if killed:
        log.info("overdue sweep: scanned={} killed={}", len(tasks), killed)
    return {"scanned": len(tasks), "killed": killed}


async def recalibrate_slots() -> dict:
    """并发槽计数按 tasks 表事实回写。

    两个方向都要修：释放失败让计数虚高（用户被永久限流），崩溃丢计数
    让计数虚低（并发保护失效）。事实源是 tasks 表的活跃任务数，直接覆盖。
    """
    if not await _lock("slots", 280):
        return {"skipped": "locked"}
    truth = await taskstore.active_counts_by_token()

    async def fix(item: tuple[str, int]) -> bool:
        token_hash, count = item
        if await slots.current(token_hash) == count:
            return False
        await slots.reset(token_hash, count)
        return True

    fixed = await _gather_bounded(list(truth.items()), fix, label="slots")
    if fixed:
        log.info("slot recalibration: tokens={} fixed={}", len(truth), fixed)
    return {"tokens": len(truth), "fixed": fixed}


async def purge_results() -> dict:
    """清理超期结果体（§9）：只置空 ``upstream_response``，状态行保留。"""
    if not await _lock("purge", 3500):
        return {"skipped": "locked"}
    ttl = await dynconf.get_int("result_ttl_seconds")
    batch = settings.result_purge_batch_limit
    total = 0
    while True:
        n = await taskstore.purge_expired_results(ttl, batch)
        total += n
        if n < batch:
            break
    if total:
        log.info("result purge done: rows={}", total)
    return {"purged": total}
