"""超时对账 / 槽位校准 / 结果清理（设计 §8、§9）。

### 超时对账三态（AC-26 ~ AC-28）

超时的任务**绝不判死**——上游可能已经成功并扣了费。定时按 ``X-Task-Id``
查 new-api 消费日志：

| 查到成功扣费记录 | 补记 SUCCESS（结果体缺失，打告警——用户付了钱但拿不到图） |
| 确认窗口内无记录 | 判 FAILURE（没扣钱，安全）                              |
| 查询本身失败     | 保持挂起，超 ``ST_RECONCILE_TTL`` 转人工                 |

第三态和第二态**必须严格区分**：把「查不到」当成「没有」会把成功任务
误判为失败；把「没有」当成「查不到」会让任务永远挂着。

### 为什么补记 SUCCESS 却没有结果体

上游确实产出了图/音频，但响应在网络上丢了，本服务拿不到字节。补记
SUCCESS 是为了让计费口径一致（钱扣了，任务标成功），同时打 error 级
日志让运维知道有一笔「已付费未交付」——这条走 OPEN-DECISIONS ② 的
人工退款通道。查询端点此时返回 502 + ``upstream_no_body``。
"""

from __future__ import annotations

from app.config import settings
from app.logging import log
from app.redis import K_SWEEP_LOCK, r
from app.schemas import ACTIVE, FAILURE, IN_PROGRESS, SUBMITTED, SUCCESS
from app.services import dynconf, slots, taskstore, tokensession
from app.services.providers import BillingError, billing


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


async def _resolve_one(task: dict) -> str:
    """对单个挂起任务做一次对账。返回结果标签（供统计）。"""
    task_id = str(task["task_id"])
    data: dict = task.get("data") or {}
    token_hash = str(data.get("token_hash") or "")
    callback_url = str(data.get("callback_url") or "")
    created_at = int(task.get("created_at") or 0)

    raw_token = await tokensession.get(task_id)
    if not raw_token:
        # 会话已过期（48h）：无法查日志。超龄就转人工，否则继续等
        age = taskstore.now() - created_at
        if age > await dynconf.get_int("reconcile_ttl"):
            await _terminal(task_id, token_hash, FAILURE, callback_url,
                            reason="reconcile abandoned: token session expired")
            return "abandoned"
        await taskstore.patch_data(task_id, {"reconcile_checked_at": taskstore.now()})
        return "no_session"

    until = taskstore.now()
    window = await dynconf.get_int("reconcile_log_window_seconds")
    since = max(created_at - 60, until - window)

    try:
        charge = await billing.find_charge(
            raw_token, task_id=task_id, since=since, until=until
        )
    except BillingError as exc:
        # 第三态：查询失败，保持挂起
        log.warning("reconcile query failed: task_id={} status={} msg={}",
                    task_id, exc.status, exc.message)
        await _maybe_escalate(task_id, created_at)
        return "query_failed"
    except Exception:
        log.opt(exception=True).warning("reconcile query error: task_id={}", task_id)
        await _maybe_escalate(task_id, created_at)
        return "query_failed"

    if charge is not None:
        # 第一态：确实扣了费 → 补记 SUCCESS（无结果体）
        await _terminal(
            task_id, token_hash, SUCCESS, callback_url,
            patch={"reconciled": True, "reconcile_pending": False,
                   "reconcile_charge_id": str(charge.get("request_id") or "")},
        )
        log.error(
            "PAID-BUT-UNDELIVERED: task_id={} charge_request_id={} amount={} "
            "— result body lost, manual refund channel may be required",
            task_id, charge.get("request_id"), charge.get("amount"),
        )
        return "success_recovered"

    # 第二态：窗口内确认无扣费记录 → 判 FAILURE（安全，没花钱）
    await _terminal(
        task_id, token_hash, FAILURE, callback_url,
        patch={"reconciled": True, "reconcile_pending": False},
        reason="reconciled: no charge record found upstream",
    )
    return "failure_confirmed"


async def _maybe_escalate(task_id: str, created_at: int) -> None:
    """挂起超 TTL → 转人工（只打告警，不动状态：人工介入前不做任何判定）。"""
    await taskstore.patch_data(task_id, {"reconcile_checked_at": taskstore.now()})
    age = taskstore.now() - created_at
    ttl = await dynconf.get_int("reconcile_ttl")
    if age > ttl:
        log.error(
            "RECONCILE-MANUAL: task_id={} age={}s exceeds reconcile_ttl={} "
            "— manual intervention required",
            task_id, age, ttl,
        )


async def _terminal(task_id: str, token_hash: str, status: str,
                    callback_url: str, *, patch: dict | None = None,
                    reason: str = "") -> None:
    won = await taskstore.cas(
        task_id, (SUBMITTED, IN_PROGRESS), status,
        patch={**(patch or {}), "inflight_slot": False, "reconcile_pending": False},
        fail_reason=reason,
    )
    if not won:
        return
    if token_hash:
        await slots.release(token_hash)
    await tokensession.clear(task_id)
    if callback_url:
        from app.queue import publish_notify

        try:
            await publish_notify(task_id)
        except Exception:
            log.opt(exception=True).warning("notify enqueue failed: task_id={}", task_id)


async def run_reconcile() -> dict:
    """一轮对账（cron 每分钟 + ops 手工触发）。"""
    if not await _lock("reconcile", 55):
        return {"skipped": "locked"}
    tasks = await taskstore.reconcile_pending(
        await dynconf.get_int("reconcile_batch_limit")
    )
    stats: dict[str, int] = {}
    for task in tasks:
        try:
            label = await _resolve_one(task)
        except Exception:
            log.opt(exception=True).error(
                "reconcile item crashed: task_id={}", task.get("task_id")
            )
            label = "crashed"
        stats[label] = stats.get(label, 0) + 1
    if tasks:
        log.info("reconcile round done: scanned={} stats={}", len(tasks), stats)
    return {"scanned": len(tasks), "stats": stats}


async def recalibrate_slots() -> dict:
    """并发槽计数按 tasks 表事实回写。

    只**下调不上调**的做法是错的——释放失败会让计数虚高（用户被永久限流），
    而崩溃丢计数会让计数虚低（用户能超发）。两个方向都要修，事实源是
    tasks 表的活跃任务数，直接覆盖。
    """
    if not await _lock("slots", 280):
        return {"skipped": "locked"}
    truth = await taskstore.active_counts_by_token()
    fixed = 0
    for token_hash, count in truth.items():
        try:
            if await slots.current(token_hash) != count:
                await slots.reset(token_hash, count)
                fixed += 1
        except Exception:
            log.opt(exception=True).debug("slot recalibrate failed: {}", token_hash)
    if fixed:
        log.info("slot recalibration: tokens={} fixed={}", len(truth), fixed)
    return {"tokens": len(truth), "fixed": fixed}


async def sweep_stale() -> dict:
    """卡死任务兜底扫描（AC-33）——补上「入队消息丢失」这条路径。

    问题：提交链路落库 SUBMITTED 之后才入队。如果 broker 抖动/Redis 重启
    把消息丢了，worker 永远不会执行这个任务：

    - 它不是超时（从没调过上游，没人给它标 ``reconcile_pending``），
      所以对账扫不到；
    - 它是活跃态，所以槽位校准会把 Redis 计数「修正」回占用状态——
      用户的一个并发额度被永久吃掉；
    - 客户端查询永远拿到 202，长轮询永远超时。

    做法：把超龄非终态任务标记 ``reconcile_pending``，交给对账三态收敛。
    **不直接判死**——万一它其实已经被派发出去了（只是 updated_at 没刷新），
    直接判 FAILURE 就可能把已扣费的成功任务打掉。对账会去查消费日志。

    超龄阈值取 ``worker_timeout × 3 + margin``：足够长，正常执行中的任务
    绝不会被误捞（IN_PROGRESS 期间 updated_at 就是派发那一刻，
    一次调用最长 worker_timeout）。
    """
    if not await _lock("stale", 110):
        return {"skipped": "locked"}

    threshold = (await dynconf.get_int("worker_timeout") * 3
                 + await dynconf.get_int("dispatch_lock_margin_seconds"))
    task_ids = await taskstore.stale_active(
        threshold, await dynconf.get_int("reconcile_batch_limit")
    )
    marked = 0
    for task_id in task_ids:
        try:
            task = await taskstore.get_meta(task_id)
            if task is None or task["status"] not in ACTIVE:
                continue
            if (task.get("data") or {}).get("reconcile_pending"):
                continue                    # 已在对账队列里，别重复标记打乱退避
            await taskstore.patch_data(task_id, {
                "reconcile_pending": True,
                "reconcile_reason": "stale_no_progress",
                "reconcile_checked_at": 0,
            })
            marked += 1
            log.warning(
                "stale task handed to reconciliation: task_id={} status={} age={}s",
                task_id, task["status"], now() - int(task.get("updated_at") or 0),
            )
        except Exception:
            log.opt(exception=True).error("stale sweep item failed: {}", task_id)
    if marked:
        log.info("stale sweep: scanned={} marked={}", len(task_ids), marked)
    return {"scanned": len(task_ids), "marked": marked}


async def purge_results() -> dict:
    """清理超期结果体（AC-32）：只置空 ``upstream_response``，状态行保留。"""
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
