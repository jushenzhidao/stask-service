"""提交链路重构版（可读性优化）。

相比原版 submit.py 的改进：
1. 用上下文管理器管理资源回滚，消除嵌套的 try/except
2. 将回滚逻辑封装为独立类，职责更清晰
3. 保持原有的失败即回滚语义不变

使用方式：将 app/routers/proxy.py 中的 import 从
    from app.services.submit import submit
改为
    from app.services.submit_v2 import submit
即可无缝替换。
"""

from __future__ import annotations

import uuid
from collections.abc import Awaitable, Callable

from app.deps.auth import Caller
from app.logging import log
from app.schemas import SubmitPlan
from app.services import idem, pricing, slots, taskstore, tokensession


class SubmitConflict(Exception):
    """同一 Idempotency-Key 真并发且占位未回填（AC-06）→ 409。"""


class SlotExhausted(Exception):
    """并发槽已满（AC-08）→ 429 + Retry-After。"""

    def __init__(self, limit: int) -> None:
        super().__init__(f"concurrent task limit reached: {limit}")
        self.limit = limit


def new_task_id(model: str) -> str:
    """``{model_slug}_{uuid4hex}``，总长 ≤49（表列 varchar(64)）。"""
    return f"{pricing.model_slug(model)}_{uuid.uuid4().hex}"


def build_task_data(plan: SubmitPlan) -> dict:
    """tasks.data 的初始 JSON（契约见 SPEC §6）。

    ``freeze_amount: 0`` + ``settled: true`` 是**给 atask sweeper 看的**：
    它扫「终态但未结算」的任务做兜底解冻，本服务的行恒为已结算，天然
    被跳过。两个服务共享 tasks 表，这是零改动共存的关键。
    """
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
        "freeze_amount": 0,
        "settled": True,
        "inflight_slot": True,
        "upstream_response": "",
        "upstream_content_type": "",
        "upstream_status": 0,
        "dispatch_epoch": 0,
        "reconcile_pending": False,
        "reconcile_checked_at": 0,
    }


class _RollbackStack:
    """回滚栈：资源获取成功时记录，失败时按 LIFO 顺序释放。"""

    def __init__(self, token_hash: str) -> None:
        self.token_hash = token_hash
        self.idempotency_key: str = ""
        self.slot_taken = False
        self.idem_backfilled = False
        self.task_id: str = ""

    async def rollback(self) -> None:
        """按获取的反序释放：槽 → 幂等占位 → 任务判死。"""
        if self.slot_taken:
            await slots.release(self.token_hash)

        if self.idempotency_key and not self.idem_backfilled:
            # 未回填的占位要归还（CAS 删除）；已回填的保留，让重试回放到判死的任务
            await idem.release(self.token_hash, self.idempotency_key)

        if self.task_id:
            # 落库后才失败：任务行已存在但不会被执行，直接 CAS 判死
            try:
                await taskstore.cas(
                    self.task_id, ("SUBMITTED",), "FAILURE",
                    patch={"inflight_slot": False},
                    fail_reason="submit pipeline aborted",
                )
                await tokensession.clear(self.task_id)
            except Exception:
                log.opt(exception=True).error(
                    "submit rollback failed to mark task: task_id={}", self.task_id
                )


async def submit(
    caller: Caller,
    plan_factory: Callable[[str], SubmitPlan],
    *,
    idempotency_key: str,
    model: str,
    balance: float | None,
    enqueue: Callable[[str], Awaitable[None]],
) -> tuple[str, bool]:
    """执行完整提交链路。返回 ``(task_id, replayed)``。

    ``replayed=True`` 表示命中幂等回放，未创建新任务（也未占新槽）。
    """
    th = caller.token_hash
    rb = _RollbackStack(th)

    try:
        # ---- 1. 幂等占位 ----
        if idempotency_key:
            owned, replay = await idem.acquire(th, idempotency_key)
            if not owned:
                if replay:
                    log.info("idempotent replay: key={} task_id={}", idempotency_key, replay)
                    return replay, True
                waited = await idem.wait_task_id(th, idempotency_key)
                if waited:
                    return waited, True
                raise SubmitConflict("concurrent request with same Idempotency-Key")
            rb.idempotency_key = idempotency_key

        # ---- 2. 余额额度占槽 ----
        limit = await pricing.slots_for_live(balance, model)
        if not await slots.acquire(th, limit):
            raise SlotExhausted(limit)
        rb.slot_taken = True

        # ---- 3. 落库 SUBMITTED ----
        task_id = new_task_id(model)
        rb.task_id = task_id
        plan = plan_factory(task_id)
        await taskstore.create(
            task_id=task_id,
            user_id=caller.identity.user_id,
            action=plan.path[:32],
            data=build_task_data(plan),
        )

        # ---- 4. 令牌会话 ----
        await tokensession.store(task_id, caller.raw_token)

        # ---- 5. 幂等回填（必须在入队前，见原版注释）----
        if rb.idempotency_key:
            await idem.set_task_id(th, rb.idempotency_key, task_id)
            rb.idem_backfilled = True

        # ---- 6. 入队 ----
        await enqueue(task_id)

        log.info(
            "task submitted: task_id={} user_id={} model={} path={}",
            task_id, caller.identity.user_id, model, plan.path,
        )
        return task_id, False

    except Exception:
        await rb.rollback()
        raise


def retry_after_seconds() -> int:
    """429 的 Retry-After 建议值：一个 worker 超时周期的 1/4，下限 1s。"""
    from app.config import settings
    return max(1, settings.worker_timeout // 4)
