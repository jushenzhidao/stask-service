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

from typing import Any

import asyncio
from collections.abc import Awaitable, Callable, Sequence
from typing import TypeVar

from app.config import settings
from app.logging import log
from app.redis import K_DISPATCH, K_DUE, K_SWEEP_LOCK, r
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


async def _kill(task: dict[str, Any], reason: str) -> bool:
    """判死一个非终态任务：CAS → 释放槽 → 清会话 → 可选回调。

    入参是**已查好的元数据行**（不是 task_id）：调用方扫描时已经拿到了
    整行，再回查一次纯属浪费——批量判死时那是每条一条多余查询。
    """
    task_id = str(task["task_id"])
    if task["status"] not in ACTIVE:
        return False
    data: dict[str, Any] = task.get("data") or {}
    won = await taskstore.cas(
        task_id, ACTIVE, FAILURE, fail_reason=reason,
    )
    if not won:
        return False
    await slots.release_for_task(data)
    await tokensession.clear(task_id)
    callback_url = str(data.get("callback_url") or "")
    if callback_url:
        from app.queue import publish_notify

        try:
            await publish_notify(task_id)
        except Exception:
            log.opt(exception=True).warning("notify enqueue failed: task_id={}", task_id)
    return True


async def sweep_stale() -> dict[str, Any]:
    """卡死任务收敛——补上「入队消息丢失」这条路径。

    问题：提交链路落库 QUEUED 之后才入队。如果 broker 抖动/Redis 重启
    把消息丢了，worker 永远不会执行这个任务，客户端查询永远 202。

    做法：超龄无进展的非终态任务，看派发锁——
    - **锁已过期** = 没有调用在飞 = 重投安全（真正的防重仍由锁把关）；
    - **锁还在** = 一次调用可能在飞 = 跳过，等锁过期后的下一轮。

    超龄阈值取 ``worker_timeout + margin + 60``：正常执行中的任务绝不会
    被误捞（IN_PROGRESS 期间 updated_at 就是派发那一刻，一次调用最长
    worker_timeout，锁 TTL = timeout + margin）。

    重投分两条路（见 :func:`handle` 内注释）：已持槽的立即任务直接重投消息，
    未持槽的等待态任务（批次成员 / 计划任务）必须经 ``dispatch.release``
    占槽——直接 publish 会绕过三层闸门，而这条路径很容易被触发。
    """
    if not await _lock("stale", 110):
        return {"skipped": "locked"}

    threshold = (await dynconf.get_int("worker_timeout")
                 + await dynconf.get_int("dispatch_lock_margin_seconds") + 60)
    tasks = await taskstore.stale_active(
        threshold, await dynconf.get_int("sweep_batch_limit")
    )
    killed = 0
    rescheduled = 0

    async def handle(task: dict[str, Any]) -> bool:
        nonlocal killed, rescheduled
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

        # 重投必须区分「是否已持有并发槽」——这是 B2 的落点。
        #
        # 立即路径在提交时就已经占了第一层（掩码 > 0），入队消息丢了只是
        # 「再送一次消息」，**不能再占一次槽**，否则同一个任务占两份额度，
        # 该 token 会被自己挤到 429。
        #
        # 而批次成员 / 计划任务在放行前掩码恒为 0：它们必须走
        # ``dispatch.release`` 由放行单点占槽后再入队。直接 publish 会**绕过
        # 三层闸门**——而且这条路径很容易被触发：stale 阈值约 210s
        # （worker_timeout + margin + 60），而 ``batch_wait`` 可以配到 3600s，
        # 于是「正在正常等待凑批」的成员会先被判为卡死、再被无槽直接执行：
        # 既击穿了并发上限，也让攒批彻底失效。
        holds_slot = int((task.get("data") or {}).get("slot_flags") or 0) > 0
        if holds_slot:
            from app.queue import publish_execute

            await publish_execute(task_id)
            await taskstore.patch_data(task_id, {"requeued_at": now()})
            log.warning("stale task requeued (slot already held): task_id={}", task_id)
            return True

        from app.services import dispatch

        ceiling = max(10, await dynconf.get_int("batch_backoff_max_seconds"))
        outcome = await dispatch.release(task_id, source="stale")
        if outcome is dispatch.Released.OK:
            log.warning("stale task released via dispatch: task_id={}", task_id)
            return True
        if outcome is dispatch.Released.NO_SLOT:
            # 占不到槽：退避重排，不判死——客户端还在等这个结果
            await dispatch.requeue(task_id, backoff_ceiling=ceiling)
            rescheduled += 1
            return False
        # 已终态 / 已被放行 / 未到点：静默丢弃（release 内部已重挂索引）
        return False

    requeued = await _gather_bounded(tasks, handle, label="stale")
    rearmed = await _rearm_due_index()
    rebuilt = await _rebuild_batch_index()
    if requeued or killed or rescheduled or rearmed or rebuilt["members"]:
        log.info("stale sweep: scanned={} requeued={} killed={} rescheduled={} "
                 "due_rearmed={} batch_members_rebuilt={}",
                 len(tasks), requeued, killed, rescheduled, rearmed,
                 rebuilt["members"])
    return {"scanned": len(tasks), "requeued": requeued, "killed": killed,
            "rescheduled": rescheduled, "due_rearmed": rearmed,
            "batch_members_rebuilt": rebuilt["members"]}


async def _rebuild_batch_index() -> dict[str, Any]:
    """按 DB 事实重建攒批索引（``st:batch:{model}`` / ``st:batch:due``）。

    与 :func:`_rearm_due_index` 对称：那条补的是**延迟侧**的索引丢失，
    这条补的是**批次侧**的。两侧都补才是完整的——只补一侧就是「同一种故障
    在一个子系统能自愈、在另一个子系统不能」。

    不补的后果比「任务卡住」更隐蔽：批次索引一丢，N 触发永远不会命中，
    成员会被兜底扫描逐条单独放行——**任务照跑，只是攒批静默失效**，
    削峰能力没了而看板一切正常。这类「降级但不报错」最难被发现。

    ``rebuild_from_db`` 内部用 ``zadd(nx=True)``，重复调用不覆盖已有成员
    与已算好的 deadline，所以每 2min 跑一轮是幂等且安全的。
    """
    from app.services import batching

    try:
        stat = await batching.rebuild_from_db(now=now())
    except Exception:
        log.opt(exception=True).warning("batch index rebuild failed")
        # 兜底返回值必须与 ``rebuild_from_db`` 的真实返回**同形**（键名同源），
        # 否则「重建失败」与「重建成功但没东西可补」在 admin 上是两种形状，
        # 看板读不到字段只会静默显示空值。
        return {key: 0 for key in batching._REBUILD_STAT_KEYS}
    return stat


async def _rearm_due_index() -> int:
    """把 DB 里仍在等待期的计划任务补回 ``st:due`` 索引。

    补齐一个**不对称**：普通任务「入队消息丢了」有 ``sweep_stale`` 重投兜底，
    而延迟任务在等待期被 sweeper 豁免（不能重投也不能判死，这是对的），
    于是 Redis 一旦丢掉 ``st:due``（重启无持久化 / FLUSHDB / 键被逐出），
    就没有任何东西会把它们放回去——任务会一直卡到超龄被判 FAILURE。
    执行有恢复路径、延迟没有，这个不对称本身就是缺陷。

    架构 §9 风险登记承诺过「从 DB 事实回补，最迟 2min 恢复，功能不失效」，
    本函数是它的落地（本任务每 2min 跑一轮）。

    只补不覆盖：已存在于索引的保持原 score，避免把已算好的到期时刻重置。
    """
    pending = await taskstore.pending_scheduled(now())
    if not pending:
        return 0
    rearmed = 0
    for item in pending:
        task_id = str(item["task_id"])
        try:
            if await r.zscore(K_DUE, task_id) is not None:
                continue
        except Exception:
            log.opt(exception=True).warning("due index probe failed: task_id={}",
                                            task_id)
            return rearmed
        from app.services import dispatch

        await dispatch.schedule(task_id, int(item["scheduled_at"]))
        rearmed += 1
    if rearmed:
        log.warning("due index rearmed from db: count={}", rearmed)
    return rearmed


async def sweep_overdue() -> dict[str, Any]:
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


async def recalibrate_slots() -> dict[str, Any]:
    """并发槽计数按 tasks 表事实回写——**三层都要校准**。

    两个方向都要修：释放失败让计数虚高（用户被永久限流），崩溃丢计数
    让计数虚低（并发保护失效）。事实源是 tasks 表的活跃任务数，直接覆盖。

    第三层自 2026-09-11 接线后真正被占用，所以三层都得有校准口径：
    - 第一层 ``st:slot:{th}``：漏了它 → 该 token 被自己的漂移卡住；
    - 第二层 ``st:mslot:{th}:{model}``：漏了它 → 该 (模型,token) 组合在
      泄漏后持续排队，直到 ``slot_ttl_seconds``（6h）自然过期；
    - 第三层 ``st:gslot:{model}``：漏了它 → **在途明明是 0 却判定为满**，
      该模型永久卡死，且没有任何自愈路径。

    事实源谓词（``SQL_HOLDS_LAYER*``）只数**真正持有该层**的任务：等待期
    任务掩码为 0，若被算进来，校准会把闸门往紧里拉——自造漂移。
    """
    if not await _lock("slots", 280):
        return {"skipped": "locked"}

    token_truth = await taskstore.active_counts_by_token()
    mt_truth = await taskstore.active_counts_by_model_token()
    model_truth = await taskstore.active_counts_by_model()

    async def fix_token(item: tuple[str, int]) -> bool:
        token_hash, count = item
        if await slots.current(token_hash) == count:
            return False
        await slots.reset(token_hash, count)
        return True

    async def fix_model_token(item: tuple[tuple[str, str], int]) -> bool:
        (token_hash, model), count = item
        if await slots.current_model_token(token_hash, model) == count:
            return False
        await slots.reset_model_token(token_hash, model, count)
        return True

    async def fix_model(item: tuple[str, int]) -> bool:
        model, count = item
        if await slots.current_global(model) == count:
            return False
        await slots.reset_global(model, count)
        return True

    fixed = await _gather_bounded(list(token_truth.items()), fix_token,
                                  label="slots")
    fixed_mt = await _gather_bounded(list(mt_truth.items()), fix_model_token,
                                     label="slots-mt")
    fixed_g = await _gather_bounded(list(model_truth.items()), fix_model,
                                    label="slots-global")
    if fixed or fixed_mt or fixed_g:
        log.info("slot recalibration: tokens={} fixed={} model_token={} "
                 "fixed_mt={} models={} fixed_global={}",
                 len(token_truth), fixed, len(mt_truth), fixed_mt,
                 len(model_truth), fixed_g)
    return {"tokens": len(token_truth), "fixed": fixed,
            "model_token": len(mt_truth), "fixed_model_token": fixed_mt,
            "models": len(model_truth), "fixed_global": fixed_g}


async def purge_results() -> dict[str, Any]:
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
