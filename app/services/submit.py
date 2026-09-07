"""提交链路（web 侧，毫秒级）：

    token 限流
    → 请求指纹 task_id（自动幂等）
    → DB 有行 → 直接回放（不占资源）
    → 幂等占位（Redis SET NX，护住创建在飞窗口）
    → 并发槽（固定上限，纯并发保护）
    → 落库 NOT_START → 令牌会话 → 入队 → 202

本服务**不做计费、不做 key 管理**：Authorization 原样透传给上游，
有效性由上游判定（无效令牌 = 上游 401 = 任务 FAILURE）。

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
from app.schemas import SubmitPlan
from app.services import dynconf, idem, slots, taskstore, tokensession
from app.services.dynconf import RuntimeConfig

_NON_ALNUM = re.compile(r"[^a-z0-9]+")


class SubmitConflict(Exception):
    """同请求真并发且创建未完成 → 409（客户端稍后重试即可回放）。"""


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

    ``replayed=True`` 表示命中自动幂等，未创建新任务（也未占新槽）。
    ``plan.task_id`` 已由路由层按请求指纹算好（确定性）。

    ``enqueue`` 用回调而非直接 import：让路由层决定入队实现，
    测试里换成记录器即可，不必 monkeypatch taskiq broker。
    """
    if config is None:
        config = await dynconf.get_runtime_config()

    task_id = plan.task_id
    th = plan.token_hash

    # ---- 1. 自动幂等：DB 是事实源 ----
    if await taskstore.exists(task_id):
        log.info("idempotent replay: task_id={}", task_id)
        return task_id, True

    # ---- 2. 创建窗口占位（护住真并发的几百毫秒）----
    if not await idem.acquire(task_id):
        if await idem.wait_row(task_id, taskstore.exists):
            return task_id, True
        raise SubmitConflict("concurrent identical request in flight")

    slot_taken = False
    created = False
    try:
        # ---- 3. 并发槽（固定上限，纯并发保护，零资金语义）----
        if not await slots.acquire(th, config.max_slots,
                                   ttl_seconds=config.slot_ttl_seconds):
            raise SlotExhausted(config.max_slots)
        slot_taken = True

        # ---- 4. 落库 NOT_START ----
        await taskstore.create(
            task_id=task_id,
            action=plan.path[:32],
            data=build_task_data(plan),
        )
        created = True

        # ---- 5. 令牌会话（必须在入队前）----
        await tokensession.store(task_id, raw_token)

        # ---- 6. 入队 ----
        await enqueue(task_id)

    except Exception:
        # 回滚顺序与获取顺序相反：先还槽，再处理占位。
        if slot_taken:
            await slots.release(th)
        if created:
            # 行已存在但永远不会被执行：CAS 判死，别留僵尸 NOT_START。
            # 行保留 = 自动幂等会把重试回放到这个 FAILURE（重试换
            # Idempotency-Key 盐即可重跑）——比归还占位让重试重建更安全：
            # 入队失败时 broker 可能已收下消息，重建会导致上游被调两次。
            try:
                await taskstore.cas(
                    task_id, ("NOT_START",), "FAILURE",
                    fail_reason="submit pipeline aborted",
                )
                await tokensession.clear(task_id)
            except Exception:
                log.opt(exception=True).error(
                    "submit rollback failed to mark task: task_id={}", task_id
                )
        else:
            await idem.release(task_id)
        raise
    finally:
        if created:
            await idem.settle(task_id)

    log.info("task submitted: task_id={} model={} path={}",
             task_id, plan.model, plan.path)
    return task_id, False


async def retry_after_seconds(config: RuntimeConfig | None = None) -> int:
    """429 的 Retry-After 建议值：一个 worker 超时周期的 1/4，下限 1s。"""
    if config is None:
        config = await dynconf.get_runtime_config()
    return max(1, config.worker_timeout // 4)
