"""提交链路（web 侧，毫秒级）：

    token 限流
    → 鉴权 + 余额预检（共享库单 SQL + 双层缓存；401/402 在此拦截）
    → 显式幂等（带 Idempotency-Key 才去重；DB 有行 → 直接回放）
    → 幂等占位（Redis SET NX，护住创建在飞窗口，仅幂等提交需要）
    → 并发槽（固定上限，纯并发保护）
    → 落库 QUEUED → 令牌会话 → 入队 → 202

本服务**零计费代码**：预扣/退款/流水全部由上游 relay 在任务执行时自理。
提交前的余额预检只是准入闸门（余额 ≤ 0 → 402 不建任务）。

**失败即回滚**：任何一步失败都必须归还已获取的资源（占位 CAS 归还 +
并发槽归还），否则一次失败的提交会永久扣掉一个槽。

顺序上的两个讲究：
- **占槽在落库前**：先落库再占槽的话，占槽失败要删已落库的行（DELETE
  比 CAS 危险得多，行可能已被 worker 领走）；
- **令牌会话在入队前**：先入队的话 worker 可能在会话写入前就取令牌，
  拿到 None 直接失败。
"""

from __future__ import annotations

import re
from collections.abc import Awaitable, Callable

from app.logging import log
from app.schemas import QUEUED, SubmitPlan
from app.services import dynconf, idem, slots, taskstore, tokensession
from app.services.dynconf import RuntimeConfig

_NON_ALNUM = re.compile(r"[^a-z0-9]+")


class SubmitConflict(Exception):
    """同幂等键真并发且创建未完成 → 409（客户端稍后重试即可回放）。"""


class SlotExhausted(Exception):
    """并发槽已满 → 429 + Retry-After。"""

    def __init__(self, limit: int) -> None:
        super().__init__(f"concurrent task limit reached: {limit}")
        self.limit = limit


def model_slug(model: str) -> str:
    """模型名 → task_id 前缀片段（小写、非字母数字替 ``_``、截断 16）。

    task_id 总长必须 ≤53（tasks.task_id 是 varchar(64)，留余量）：
    16 + 1 + 32 = 49。空模型名回落 ``task``。
    """
    slug = _NON_ALNUM.sub("_", (model or "").strip().lower()).strip("_")
    return (slug[:16].rstrip("_") or "task")


def build_task_data(plan: SubmitPlan) -> dict:
    """tasks.data 的初始 JSON（契约见 SPEC §6）。"""
    return {
        "source": "stask",
        "model": plan.model,
        "token_hash": plan.token_hash,
        "idempotency_key": plan.idempotency_key,
        "callback_url": plan.callback_url,
        "request_method": plan.method,
        "request_path": plan.path,
        "request_query": plan.query,
        "request_headers": plan.headers,
        "request_body": plan.body_b64,
        "body_truncated": plan.body_truncated,
        "upstream_base_url": plan.upstream_base_url,
        "upstream_response": "",
        "upstream_content_type": "",
        "upstream_status": 0,
        "dispatch_epoch": 0,
    }


async def submit(
    raw_token: str,
    plan: SubmitPlan,
    *,
    enqueue: Callable[[str], Awaitable[None]],
    config: RuntimeConfig | None = None,
) -> tuple[str, bool]:
    """执行完整提交链路。返回 ``(task_id, replayed)``。

    ``replayed=True`` 表示命中显式幂等（``Idempotency-Key``），未创建
    新任务（也未占新槽）。``plan.task_id`` 已由路由层算好。

    ``enqueue`` 用回调而非直接 import：让路由层决定入队实现，
    测试里换成记录器即可，不必 monkeypatch taskiq broker。
    """
    if config is None:
        config = await dynconf.get_runtime_config()

    task_id = plan.task_id
    th = plan.token_hash
    idempotent = bool(plan.idempotency_key)

    # ---- 1. 显式幂等：DB 是事实源（仅带 Idempotency-Key 的提交）----
    if idempotent:
        if await taskstore.exists(task_id):
            log.info("idempotent replay: task_id={}", task_id)
            return task_id, True

        # ---- 2. 创建窗口占位（护住同 key 真并发的几百毫秒）----
        if not await idem.acquire(task_id):
            if await idem.wait_row(task_id, taskstore.exists):
                return task_id, True
            raise SubmitConflict("concurrent request with same idempotency key")

    slot_taken = False
    created = False
    try:
        # ---- 3. 并发槽（固定上限，纯并发保护，零资金语义）----
        if not await slots.acquire(th, config.max_slots,
                                   ttl_seconds=config.slot_ttl_seconds):
            raise SlotExhausted(config.max_slots)
        slot_taken = True

        # ---- 4. 落库 QUEUED ----
        await taskstore.create(
            task_id=task_id,
            action=plan.path[:32],
            data=build_task_data(plan),
            user_id=plan.user_id,
        )
        created = True

        # ---- 5. 令牌会话（必须在入队前）----
        await tokensession.store(task_id, raw_token)

        # ---- 6. 入队 ----
        await enqueue(task_id)

    except Exception as exc:
        # 提交链路任何一步失败都先记录阶段与任务上下文再上抛（路由层
        # 转 500）。没有这条日志时，落库/入队失败在 logfire 里只有
        # FastAPI 的裸 500，看不到是哪一步、哪个任务。
        log.bind(task_id=task_id, phase="submit", model=plan.model,
                 request_path=plan.path).opt(exception=True).error(
            "submit pipeline failed: task_id={} model={} path={} error={}",
            task_id, plan.model, plan.path, type(exc).__name__,
        )
        # 回滚顺序与获取顺序相反：先还槽，再处理占位。
        if slot_taken:
            await slots.release(th)
        if created:
            # 行已存在但永远不会被执行：CAS 判死，别留僵尸 QUEUED。
            # 行保留 = 幂等重试会回放到这个 FAILURE（换 Idempotency-Key
            # 即可重跑）——比归还占位让重试重建更安全：入队失败时 broker
            # 可能已收下消息，重建会导致上游被调两次。
            try:
                await taskstore.cas(
                    task_id, (QUEUED,), "FAILURE",
                    fail_reason="submit pipeline aborted",
                )
                await tokensession.clear(task_id)
            except Exception:
                log.opt(exception=True).error(
                    "submit rollback failed to mark task: task_id={}", task_id
                )
        elif idempotent:
            await idem.release(task_id)
        raise
    finally:
        if created and idempotent:
            await idem.settle(task_id)

    log.info("task submitted: task_id={} model={} path={}",
             task_id, plan.model, plan.path)
    return task_id, False


async def retry_after_seconds(config: RuntimeConfig | None = None) -> int:
    """429 的 Retry-After 建议值：一个 worker 超时周期的 1/4，下限 1s。"""
    if config is None:
        config = await dynconf.get_runtime_config()
    return max(1, config.worker_timeout // 4)
