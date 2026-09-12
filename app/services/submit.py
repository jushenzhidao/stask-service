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

from typing import Any

import dataclasses
import re
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

from app.logging import log
from app.schemas import QUEUED, SubmitPlan
from app.config import settings
from app.services import dynconf, idem, modelpolicy, slots, taskstore, tokensession
from app.services.dynconf import RuntimeConfig

_NON_ALNUM = re.compile(r"[^a-z0-9]+")


class SubmitConflict(Exception):
    """同幂等键真并发且创建未完成 → 409（客户端稍后重试即可回放）。"""


class SlotExhausted(Exception):
    """并发槽已满 → 429 + Retry-After。

    ``layer`` 标明是**哪一层**满的（``token`` / ``model_token`` / ``global``）。
    三层满起来客户端看到的都是同一个 429，但处置完全不同（换 key / 换模型 /
    等上游容量），所以它必须进 429 的响应体——否则排障只能靠猜。
    """

    def __init__(self, limit: int, *, layer: str = "token") -> None:
        super().__init__(f"concurrent task limit reached: {layer} limit {limit}")
        self.limit = limit
        self.layer = layer


def model_slug(model: str) -> str:
    """模型名 → task_id 前缀片段（小写、非字母数字替 ``_``、截断 16）。

    task_id 总长必须 ≤53（tasks.task_id 是 varchar(64)，留余量）：
    16 + 1 + 32 = 49。空模型名回落 ``task``。
    """
    slug = _NON_ALNUM.sub("_", (model or "").strip().lower()).strip("_")
    return (slug[:16].rstrip("_") or "task")


def build_task_data(plan: SubmitPlan) -> dict[str, Any]:
    """tasks.data 的初始 JSON（契约见 SPEC §6）。

    ``request_body`` / ``upstream_response`` 默认是**明文**（小体且合法
    UTF-8 时），只在超 ``plain_max_bytes`` 或含二进制字节时才转
    gzip+base64；两者各配一个 ``*_encoding`` 兄弟字段显式标记形态，
    读侧不做嗅探（见 ``app.services.codec``）。
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
        "request_body": plan.body,
        "request_body_encoding": plan.body_encoding,
        "body_truncated": plan.body_truncated,
        "upstream_base_url": plan.upstream_base_url,
        "upstream_response": "",
        "upstream_response_encoding": "",
        "upstream_content_type": "",
        "upstream_status": 0,
        "dispatch_epoch": 0,
        #: 计划执行时刻（0 = 无延迟）。等待期任务保持 QUEUED，不引入新状态；
        #: sweeper 靠它豁免未到点的任务，生命期计时口径也取 max(created_at, 它)。
        "scheduled_at": plan.scheduled_at,
    }


def batch_fields(
    policy: modelpolicy.ResolvedPolicy, *, queued: bool, model: str,
    slot_flags: int = 0, delayed: bool = False, key: str = "",
) -> dict[str, Any]:
    """排队/延迟相关的 ``data`` 字段。

    **生效参数必须落库**（不只是 ``batch_state``）：策略是热改的，在途任务
    要按创建时那一套走完生命周期。放行时若回头读当前配置，一次热改就会让
    在途任务的等待窗口跳变，重建索引时算出的到期时刻也会与入批时的不一致。

    ``slot_model`` 落归一化后的模型名——占槽与释放必须用同一个字符串。

    ``slot_flags`` 必须是**本次真实占到的掩码**，绝不能对立即路径写死 0：
    立即路径在提交时就占了第一层，写 0 会让终态的 ``release_for_task``
    认定「从未占槽」而一层都不还，``st:slot:{th}`` 于是单调累积——该 token
    在 ``max_slots`` 条任务之后永久 429。排队/延迟路径此刻确实还没占槽
    （占槽点在 ``dispatch.release``），保持 0 是对的。

    ``delayed`` 用 ``batch_state="scheduled"`` 区分于批次成员（``"waiting"``）：
    两者都「可被放行」，但延迟任务不在 ``st:batch:{model}`` 里，若混用
    ``"waiting"``，``batch_waiting()`` 会把延迟任务当成批次成员重建进错误的
    索引。``scheduled_at`` 才是「是否延迟」的唯一判据。
    """
    if delayed:
        # 延迟任务此刻不入批，但归组键**先算好落库**：到点由 admit_due 入批时
        # 必须复用同一个键，否则它会进到另一个批次里（成员永远凑不齐）。
        return {"batch_state": "scheduled", "slot_model": model,
                "slot_flags": 0, "batch_key": key}
    if not queued:
        return {
            "batch_state": "immediate",
            "slot_model": model,
            "slot_flags": slot_flags,
        }
    return {
        "batch_state": "waiting",
        "slot_model": model,
        "slot_flags": slot_flags,
        "batch_key": key,
        # 落的是**生效值**（客户端头覆盖后的结果），不是策略原值：
        # 客户端声明 N=3 而策略写 N=10 时，入批与 N 触发都按 3 走。
        "batch_size": policy.batch,
        "batch_wait": policy.batch_wait,
        "batch_policy_source": policy.source,
        "requeue_attempts": 0,
    }


def apply_batch_overrides(
    policy: modelpolicy.ResolvedPolicy, plan: SubmitPlan, *,
    default_wait: int, batch_enabled: bool,
) -> modelpolicy.ResolvedPolicy:
    """把客户端分批头叠加到服务端策略上，返回**生效策略**（R-14~R-17）。

    语义：**逐字段显式优先**，未声明的维度回落策略值。
    - 客户端给 N 不给 T → T 取策略值，策略没写则取 ``max_batch_wait_seconds``
      （AC-57：只声明 N **不得**变成无限等待）；
    - 客户端给 T 不给 N → N 取策略值。

    客户端可以**用头开启策略未声明的攒批**（把 N 从 0 抬到 >=2）：这是
    「客户端主动要求攒批」的正当用法，总开关 ``batch_enabled`` 仍是最终闸门。
    反方向也允许（客户端把 N 调小），因为它只让自己的批次更快放行。

    **``batch_enabled=False`` 时本函数直接原样返回策略**：这一层是防御性的
    第二道闸。上层 ``queued = config.batch_enabled and policy.queues`` 已经拦了
    一次，但那是「调用方记得拦」；把闸门同时做进函数里，才不会出现「新加一个
    调用方忘了拦」就绕过总开关的情况。``batch_enabled`` 从「声明了却不用的
    参数」变成真正的闸门——参数表本身就是契约，不能写着一个不生效的东西。
    """
    if not batch_enabled:
        return policy
    if plan.batch_size is None and plan.batch_wait is None:
        return policy
    size = plan.batch_size if plan.batch_size is not None else policy.batch
    wait = plan.batch_wait if plan.batch_wait is not None else policy.batch_wait
    if wait <= 0:
        # 策略没声明 T 时必须有兜底，否则 LUA 里 due_at = now 会让整批
        # 立刻到期——等价于「攒批完全不生效」，而且还占着 waiting 状态。
        wait = max(1, int(default_wait))
    return dataclasses.replace(policy, batch=size, batch_wait=wait)


async def join_batch(
    task_id: str, key: str, policy: modelpolicy.ResolvedPolicy
) -> bool:
    """入批并在攒够 N 时投递放行任务。返回是否真的入批（供回滚判断）。

    ``key`` 是**归组键**（见 ``batching.group_key``），不是模型名。


    ``batch < 2`` 但 ``limit_global > 0`` 的模型不攒批，只是需要排队等槽——
    直接挂重排通道，由 ticker 在下一轮尝试放行。

    N 触发只 **投递** 一个放行任务就返回，绝不在提交响应里同步放行整批。
    投递失败不上抛：批次已在 Redis 里，T 触发会兜住它，最坏多等 ``batch_wait``
    秒。为一次投递抖动让整个提交 500 得不偿失。
    """
    from app.services import batching, dispatch

    if policy.batch < 2:
        await dispatch.schedule(task_id, taskstore.now())
        return False

    count, due_at = await batching.join(
        task_id, key,
        batch_size=policy.batch, batch_wait=policy.batch_wait,
    )
    # deadline 落库：Redis 索引丢失后 rebuild_from_db 靠它重建 T 触发时刻。
    await taskstore.patch_data(task_id, {"batch_due_at": due_at})

    if count >= policy.batch:
        try:
            from app.queue import publish_release_batch

            await publish_release_batch(key, "size")
        except Exception:
            log.opt(exception=True).warning(
                "batch size trigger publish failed, will fall back to timeout: "
                "key={} count={}", key, count,
            )
    return True


@dataclass(frozen=True, slots=True)
class Submitted:
    """提交结果。

    带上生效的批次信息是为了让 202 响应能直接回报（R-20）——否则路由层要
    再查一次库才拿得到 ``batch_key``，给提交热路径平白加一次 DB 往返。
    """

    task_id: str
    replayed: bool
    batch_key: str = ""
    batch_state: str = ""


async def submit(
    raw_token: str,
    plan: SubmitPlan,
    *,
    enqueue: Callable[[str], Awaitable[None]],
    config: RuntimeConfig | None = None,
) -> Submitted:
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
            # 回放不改动任何调度/批次状态（AC-60）：批次信息由路由层从库里读。
            return Submitted(task_id=task_id, replayed=True)

        # ---- 2. 创建窗口占位（护住同 key 真并发的几百毫秒）----
        if not await idem.acquire(task_id):
            if await idem.wait_row(task_id, taskstore.exists):
                return Submitted(task_id=task_id, replayed=True)
            raise SubmitConflict("concurrent request with same idempotency key")

    # ---- 3. 解析模型策略：决定「收到即发」还是「排队放行」----
    policy = modelpolicy.resolve(
        model=plan.model,
        path=plan.path,
        policies=await dynconf.get_model_policies(),
        default_limit_per_token=config.max_slots,
    )
    # 客户端分批头叠加到策略上（R-14~R-17）——必须在算 ``queues`` **之前**，
    # 因为客户端可以把 N 抬到 >=2 从而开启攒批，queues 的判定要看得是生效值。
    policy = apply_batch_overrides(
        policy, plan,
        default_wait=config.max_batch_wait_seconds,
        batch_enabled=config.batch_enabled,
    )
    queued = config.batch_enabled and policy.queues
    model = modelpolicy.normalize_model(plan.model)
    # 归组键：X-Batch-Key 优先，否则按 batch_group_by 配置的维度算。
    # 只在使用批次时才落库（R-20：无批次时 batch_key 为空串）。
    from app.services import batching as _batching

    key = ""
    if queued:
        key = _batching.group_key(
            plan.batch_key or "", model=model, token_hash=plan.token_hash,
            group_by=settings.batch_group_by,
        )
    # 延迟任务：提交时既不占槽也不入批，只挂进到期索引（st:due）。
    # 是否延迟以 scheduled_at 为唯一判据，与 batch_state 正交。
    delayed = plan.scheduled_at > taskstore.now()

    slot_taken = False
    slot_mask = 0
    created = False
    joined = False
    try:
        # ---- 4. 并发槽：只有「收到即发」才在提交时占槽 ----
        #
        # 排队路径**不能**在这里占槽：任务要等最多 batch_wait 秒才放行，
        # 提交时就占着槽等于把等待期也算进并发额度，攒批越久闸门越紧,
        # 一批还没放行就先把自己的槽耗光了。排队路径的占槽点在
        # dispatch.release（放行的那一刻）。
        #
        # 延迟路径同理，而且更极端：等待期可长达数小时，占槽等于把用户的
        # 并发配额锁死整个等待窗口，正常请求全部 429。
        if not queued and not delayed:
            if policy.layered_at_submit:
                # 「满则拒」路径：提交时就用三层原子占槽，第二/三层在这里
                # 就判定，满则 429 让客户端退避重试——背压交回客户端。
                #
                # 与排队路径的区别不是「快一点」：它**不挂 st:due**，所以不依赖
                # tick 进程，也没有「等 tick 的 0~15s」；代价是队列不再兜底，
                # 客户端必须自己重试。
                #
                # 掩码是 release_for_task 的唯一依据，必须随行落库（同下）。
                limits = {
                    "token": policy.limit_per_token or config.max_slots,
                    "model_token": policy.limit_model_token,
                    "global": policy.limit_global,
                }
                # acquire_layered 返回 0 有两个含义（「某层超限」与「三层都没启
                # 用」）。这里三层里至少有第一层为正（max_slots ≥ 1）+ 一个分层
                # 上限为正（layered_at_submit 的成立条件），所以 0 只能是超限。
                slot_mask = await slots.acquire_layered(
                    th, model,
                    limit_per_token=limits["token"],
                    limit_model_token=limits["model_token"],
                    limit_global=limits["global"],
                    ttl_seconds=config.slot_ttl_seconds,
                )
                if not slot_mask:
                    # 哪个层满的要报出来：三层都是 429，但处置完全不同
                    # （换 key / 换模型 / 等上游）。只在错误路径读计数。
                    layer = await slots.binding_layer(
                        th, model,
                        limit_per_token=limits["token"],
                        limit_model_token=limits["model_token"],
                        limit_global=limits["global"],
                    )
                    raise SlotExhausted(limits[layer] or limits["token"], layer=layer)
                slot_taken = True
            else:
                if not await slots.acquire(th, policy.limit_per_token,
                                           ttl_seconds=config.slot_ttl_seconds):
                    raise SlotExhausted(policy.limit_per_token)
                slot_taken = True
                # 单层入口只占第一层，掩码就是 FLAG_TOKEN。它必须随行落库，
                # 否则终态释放读到 0 会一层都不还（见 batch_fields 注释）。
                slot_mask = slots.FLAG_TOKEN

        # ---- 5. 落库 QUEUED ----
        data = build_task_data(plan)
        data.update(batch_fields(policy, queued=queued, model=model,
                                 slot_flags=slot_mask, delayed=delayed, key=key))
        await taskstore.create(
            task_id=task_id,
            action=plan.path[:32],
            data=data,
            user_id=plan.user_id,
        )
        created = True

        # ---- 6. 令牌会话（必须在入队/入批前）----
        await tokensession.store(task_id, raw_token)

        # ---- 7. 入队 / 入批 / 挂到期索引 ----
        if delayed:
            # 只挂索引，不入队也不入批。到点由 ticker 走 dispatch.release
            # 重新做一次准入决策（届时若该模型配了攒批，会入批而不是直接放行）。
            from app.services import dispatch

            await dispatch.schedule(task_id, plan.scheduled_at)
        elif not queued:
            await enqueue(task_id)
        else:
            joined = await join_batch(task_id, key, policy)

    except Exception as exc:
        # 提交链路任何一步失败都先记录阶段与任务上下文再上抛（路由层
        # 转 500）。没有这条日志时，落库/入队失败在 logfire 里只有
        # FastAPI 的裸 500，看不到是哪一步、哪个任务。
        log.bind(task_id=task_id, phase="submit", model=plan.model,
                 request_path=plan.path).opt(exception=True).error(
            "submit pipeline failed: task_id={} model={} path={} error={}",
            task_id, plan.model, plan.path, type(exc).__name__,
        )
        # 回滚顺序与获取顺序相反：先还槽/退批，再处理占位。
        #
        # 归还与获取必须**成对同源**：分层路径占了三层，只还第一层会让
        # st:mslot / st:gslot 单调累积（校准能修计数，修不了「谁的槽被还掉
        # 了」）。这里按 policy.layered_at_submit 分派而不是按掩码猜，
        # 与上面占槽时的分派条件是同一个表达式，不会漂移。
        if slot_taken:
            if policy.layered_at_submit:
                await slots.release_layered(th, model, slot_mask)
            else:
                await slots.release(th)
        if joined:
            # 入批成功但后续步骤炸了：必须退批，否则这条已判死的任务
            # 会占着批次计数，让整批凑不满 N 只能干等 T。
            from app.services import batching

            await batching.leave(task_id, key)
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
    # 回报生效的批次信息（R-20）。立即路径没有批次，两项为空串。
    state = str(data.get("batch_state") or "")
    return Submitted(
        task_id=task_id, replayed=False,
        batch_key=str(data.get("batch_key") or ""),
        batch_state=state,
    )


async def retry_after_seconds(config: RuntimeConfig | None = None) -> int:
    """429 的 Retry-After 建议值：一个 worker 超时周期的 1/4，下限 1s。"""
    if config is None:
        config = await dynconf.get_runtime_config()
    return max(1, config.worker_timeout // 4)
