"""提交链路（web 侧，毫秒级）。设计 §4：

    token 限流 ‖ 幂等占位
    → 身份内省 + 余额（Redis 缓存 30s）
    → upstream 地址校验（§7）+ 路径准入
    → 余额额度占槽（Redis Lua）
    → 落库 → 令牌会话 → 入队 → 202

**失败即回滚**：任何一步失败都必须归还已获取的资源（幂等占位 CAS 归还 +
并发槽归还），否则用户会被一次失败的提交永久扣掉一个槽。这里用显式的
``_Rollback`` 栈而不是 try/except 层层嵌套——嵌套写法在加新步骤时极易
漏掉某条回滚路径。

顺序上的两个讲究：
- **占槽在落库前**：先落库再占槽的话，占槽失败要删已落库的行（DELETE
  比 CAS 归还危险得多，而且行已经可能被 worker 领走了）；
- **令牌会话在入队前**：先入队的话 worker 可能在会话写入前就取 sk，
  拿到 None 直接失败。
"""

from __future__ import annotations

import uuid
from collections.abc import Awaitable, Callable

from app.deps.auth import Caller
from app.logging import log
from app.schemas import SubmitPlan
from app.services import dynconf, idem, pricing, slots, taskstore, tokensession
from app.services.dynconf import RuntimeConfig


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


async def submit(
    caller: Caller,
    plan_factory: Callable[[str], SubmitPlan],
    *,
    idempotency_key: str,
    model: str,
    balance: float | None,
    enqueue: Callable[[str], Awaitable[None]],
    config: RuntimeConfig | None = None,
) -> tuple[str, bool]:
    """执行完整提交链路。返回 ``(task_id, replayed)``。

    ``replayed=True`` 表示命中幂等回放，未创建新任务（也未占新槽）。

    参数用回调（``plan_factory`` / ``enqueue``）而非直接传值/直接 import：
    - ``plan_factory(task_id)``：task_id 依赖 model，而 plan 依赖 task_id，
      循环依赖用工厂打破；
    - ``enqueue``：让路由层决定入队实现，测试里换成记录器即可，
      不必 monkeypatch taskiq broker。

    ``config``：路由层已取的运行时快照，整条链路复用同一份（槽上限、
    兜底单价、槽键 TTL 都从它取）。未传时自取一次。
    """
    if config is None:
        config = await dynconf.get_runtime_config()

    th = caller.token_hash
    owned_idem = False
    idem_backfilled = False

    # ---- 1. 幂等占位（原子，AC-05/AC-06）----
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
        owned_idem = True

    slot_taken = False
    task_id = ""
    try:
        # ---- 2. 余额额度占槽（AC-07/AC-08）----
        # 走快照：max_slots、兜底单价、槽键 TTL 都可在管理页热改，无需重启
        limit = pricing.slots_for(
            balance, model,
            max_slots=config.max_slots,
            default_price=config.ref_price_default,
        )
        if not await slots.acquire(th, limit,
                                   ttl_seconds=config.slot_ttl_seconds):
            raise SlotExhausted(limit)
        slot_taken = True

        # ---- 3. 落库 SUBMITTED ----
        task_id = new_task_id(model)
        plan = plan_factory(task_id)
        await taskstore.create(
            task_id=task_id,
            user_id=caller.identity.user_id,
            action=plan.path[:32],
            data=build_task_data(plan),
        )

        # ---- 4. 令牌会话（必须在入队前）----
        await tokensession.store(task_id, caller.raw_token)

        # ---- 5. 幂等回填 ----
        # 时机很讲究：**必须在入队之前**。
        # 反过来（先入队后回填）时，若回填失败会走回滚把占位归还，而任务
        # 其实已经入队并在跑——客户端拿原键重试就会重建第二个任务，上游
        # 被调两次，用户双扣。宁可让「已回填但入队失败」的键指向一个
        # FAILURE 任务（重试看到失败，换个键即可），也不能开双扣的口子。
        if owned_idem:
            await idem.set_task_id(th, idempotency_key, task_id)
            idem_backfilled = True

        # ---- 6. 入队 ----
        await enqueue(task_id)

    except Exception:
        # 回滚顺序与获取顺序相反：先还槽（影响后续提交），再还占位。
        # 占位归还是 CAS（仅 pending 才删）——已回填的键会被有意保留，
        # 让重试回放到下面判死的那个 FAILURE 任务，而不是重建。
        if slot_taken:
            await slots.release(th)
        if owned_idem and not idem_backfilled:
            await idem.release(th, idempotency_key)
        if task_id:
            # 落库之后才失败的（会话/回填/入队）：任务行已存在但永远不会被
            # 执行，直接 CAS 判死，别留一条僵尸 SUBMITTED 占着状态分布
            try:
                await taskstore.cas(
                    task_id, ("SUBMITTED",), "FAILURE",
                    patch={"inflight_slot": False},
                    fail_reason="submit pipeline aborted",
                )
                await tokensession.clear(task_id)
            except Exception:
                log.opt(exception=True).error(
                    "submit rollback failed to mark task: task_id={}", task_id
                )
        raise

    log.info(
        "task submitted: task_id={} user_id={} model={} path={}",
        task_id, caller.identity.user_id, model, plan.path,
    )
    return task_id, False


async def retry_after_seconds(config: RuntimeConfig | None = None) -> int:
    """429 的 Retry-After 建议值：一个 worker 超时周期的 1/4，下限 1s。

    ``config`` 由调用方传入以复用本次请求已取的快照。
    """
    if config is None:
        config = await dynconf.get_runtime_config()
    return max(1, config.worker_timeout // 4)
