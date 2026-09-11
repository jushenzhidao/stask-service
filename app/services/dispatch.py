"""放行 · 「等待中的任务 → 真正入队」的唯一通道。

## 为什么必须单点

放行要做四件必须一起成立的事：**抢放行权 → 占三层槽 → 落库占位掩码
→ 入队**。任何一条分支自己实现一遍，就会漏掉其中一步：

- 漏占槽 → 上游并发闸门形同虚设；
- 占了槽但落库失败 → 槽泄漏，等 TTL 或校准才回收；
- 漏「非 QUEUED 静默丢弃」→ 已取消的任务被放出去，钱花在用户已经不要的
  结果上；
- 漏幂等 → N 触发与 T 触发同时命中同一条任务，上游被调两次。

所以攒批放行、到期重排放行、人工放行三条路径全部收敛到 :func:`release`。

## 幂等的落点

``taskstore.claim_for_release`` 做条件更新（``WHERE status='QUEUED' AND
batch_state='waiting'``），受影响行数 0 = 别人已经放过或任务已终态 →
本次直接返回 :attr:`Released.SKIPPED`。这让重复放行天然安全，是
「绝不重复调上游」纪律在放行侧的实现。Redis 侧的批次 claim 只保证
**批次级**互斥，救不了同一成员被两条路径捞到，所以 DB 这一关不能省。

注意顺序：**先 claim 再占槽**。反过来的话，claim 失败时已占的槽要靠额外的
回滚代码还回去，而回滚本身也可能失败。

## 令牌 TTL 是等待的真天花板

放行时任务已经等了最多 ``batch_wait`` 秒，而用户令牌只在 Redis 会话里
（TTL ``sk_session_ttl_seconds``，绝不落库）。等过头 → 执行时取不到令牌 →
``token_missing`` 判死。写侧 ``modelpolicy.validate`` 已把 T 卡在 TTL 之内，
这里**不再重复校验**：放行是热路径，且真正的判定点在执行时。
"""

from __future__ import annotations

import enum
import random
from typing import Any

from app.logging import log
from app.redis import K_DUE, r
from app.schemas import PENDING
from app.services import dynconf, modelpolicy, slots, taskstore


class Released(enum.Enum):
    """放行结果。调用方靠它区分「成功」「没槽」「不该放」。"""

    OK = "ok"
    #: 三层闸门里有一层满了 → 调用方退避重排（**不是**失败）
    NO_SLOT = "no_slot"
    #: 已终态 / 已被别人放行 / 行不存在 → 静默丢弃，绝不重复下发
    SKIPPED = "skipped"


async def release(task_id: str, *, source: str = "manual",
                  now: int | None = None) -> Released:
    """把一条等待中的任务放行入队。幂等：重复调用只有第一次生效。

    ``now`` 由调用方注入（ticker 传本轮的统一时刻），默认取真实时钟。
    一轮扫描共用同一时刻，避免「同一条任务被两个谓词读到两个不同 now」
    这类只在跨秒边界出现的诡异判定。

    **只读元数据投影**（``get_meta``）而不是整行：本函数在到期通道与批次
    放行里是每任务一次的热路径，而整行含 ``request_body``（≤2MB）与
    ``upstream_response``（≤10MB）——两者这里一个字节都用不到。
    """
    row = await taskstore.get_meta(task_id)
    if row is None:
        log.warning("release: task not found: task_id={}", task_id)
        return Released.SKIPPED

    data = row.get("data") or {}
    # 占槽用的模型名必须取**落库的归一化值**：占与释放用同一个字符串，
    # 否则归一化规则一变，在途任务就会「占 A 释放 B」造成永久漂移。
    model = str(data.get("slot_model") or modelpolicy.normalize_model(
        str(data.get("model") or "")))
    token_hash = str(data.get("token_hash") or "")

    # 延迟未到点 → 绝不提前放行（AC-44 的最后一道路障）。
    # 到期索引只是「候选集」，真正的判定必须在这里再做一次：索引可能因
    # 人工干预、重建或时钟回拨而提前包含这条任务，而提前放行会直接摧毁
    # 延迟语义（客户端要求的时刻没到，上游已经被调了）。
    scheduled_at = int(data.get("scheduled_at") or 0)
    ts = taskstore.now() if now is None else now
    if scheduled_at > ts:
        await schedule(task_id, scheduled_at)
        log.info("release: not yet due, rescheduled: task_id={} at={} source={}",
                 task_id, scheduled_at, source)
        return Released.SKIPPED

    cfg = await dynconf.get_runtime_config()
    policy = modelpolicy.resolve(
        model=str(data.get("model") or ""),
        # 键名是 ``request_path``（落库时的字段名），不是 ``path``。
        # 写错不会报错——`data.get("path")` 静默返回 None，于是按端点前缀
        # 配的策略（``/v1/videos`` 之类）在放行路径上**永远匹配不上**，
        # 一律回落到 __default__/settings。这类「声明了却不生效」只能靠
        # 端到端用例发现，单测 modelpolicy 本身是绿的。
        path=str(data.get("request_path") or ""),
        policies=await dynconf.get_model_policies(),
        default_limit_per_token=cfg.max_slots,
    )

    # 抢占前记下真实的等待态，占槽失败时原样退回。不能靠 scheduled_at 反推：
    # 延迟任务入批后两个属性同时成立，反推会把它误标回 scheduled（详见
    # taskstore.unclaim_for_release）。
    prev_state = str(data.get("batch_state") or "waiting")

    # 1) 先抢占「放行权」：条件更新，一行只可能被一方 claim 成功。
    if not await taskstore.claim_for_release(task_id):
        log.info("release: already claimed or terminal: task_id={} source={}",
                 task_id, source)
        return Released.SKIPPED

    # 2) 占三层槽。任一层满 → 回退 claim，让调用方退避重排。
    #    三层上限全部来自本次解析的策略（含第二层 (模型,token)）。
    mask = await slots.acquire_layered(
        token_hash, model,
        limit_per_token=policy.limit_per_token or cfg.max_slots,
        limit_model_token=policy.limit_model_token,
        limit_global=policy.limit_global,
        ttl_seconds=cfg.slot_ttl_seconds,
    )
    if not mask:
        await taskstore.unclaim_for_release(task_id, restore=prev_state)
        log.info("release: no slot, will retry: task_id={} model={} source={}",
                 task_id, model, source)
        return Released.NO_SLOT

    # 3) 落库占位掩码 + 放行标记。掩码是释放时的唯一依据（不变式：绝不按
    #    当前配置重算该释放哪几层），必须与占槽同一轮写下。
    #    状态仍是 QUEUED —— 执行侧才 CAS 到 IN_PROGRESS。
    try:
        await taskstore.patch_data(task_id, {
            "batch_state": "released",
            "slot_flags": mask,
            "slot_model": model,
            "released_at": taskstore.now(),
        })
    except Exception:
        await slots.release_layered(token_hash, model, mask)
        await taskstore.unclaim_for_release(task_id, restore=prev_state)
        log.opt(exception=True).error("release: persist failed: task_id={}", task_id)
        return Released.NO_SLOT

    # 4) 入队。入队失败必须把槽还回去——否则这一条永远不会有终态处理去还它。
    #    行保留为 FAILURE 不删（幂等不变式：删了重试会重建、上游被调两次）。
    try:
        from app.queue import publish_execute

        await publish_execute(task_id)
    except Exception:
        await slots.release_layered(token_hash, model, mask)
        await taskstore.cas(
            task_id, PENDING, "FAILURE",
            patch={"slot_flags": 0},
            fail_reason="enqueue failed after release",
        )
        log.opt(exception=True).error("release: enqueue failed: task_id={}", task_id)
        return Released.SKIPPED

    await unschedule(task_id)
    log.info("release: dispatched: task_id={} model={} flags={} source={}",
             task_id, model, mask, source)
    return Released.OK


# ---------------------------------------------------------------------------
# 退避重排通道（占槽失败的任务在这里等下一次放行机会）
# ---------------------------------------------------------------------------


async def schedule(task_id: str, due_at: int) -> None:
    """挂到重排通道。同 task_id 重复 ZADD 只是更新 score，天然幂等。"""
    await r.zadd(K_DUE, {task_id: float(due_at)})


async def requeue(task_id: str, *, backoff_ceiling: int) -> int:
    """占槽失败 → 指数退避 + ±10% 抖动重排。返回下次尝试时刻。

    **抖动是必需的，不是锦上添花**：一批 200 条同时占槽失败，若都按固定
    延迟重排，下一轮又会同时涌向同一个满的闸门——整批惊群会一直重复，
    直到某轮恰好有槽空出来。抖动把它们摊开到一个窗口里。

    退避次数落库（``data.requeue_attempts``）而不是放 Redis：Redis 掉数据后
    次数归零 = 退避重新从 1 秒起步，等于失去退避效果。
    """
    row = await taskstore.get_meta(task_id)
    attempts = int(((row or {}).get("data") or {}).get("requeue_attempts") or 0) + 1
    delay = min(backoff_ceiling, 2 ** min(attempts, 12))
    delay = max(1, int(delay * (1.0 + random.uniform(-0.1, 0.1))))
    due_at = taskstore.now() + delay
    await taskstore.patch_data(task_id, {
        "requeue_attempts": attempts,
        "requeue_due_at": due_at,
    })
    await schedule(task_id, due_at)
    log.info("requeue: task_id={} attempts={} delay={}s", task_id, attempts, delay)
    return due_at


async def unschedule(task_id: str) -> None:
    """从重排通道摘除（放行成功或任务取消后）。

    摘除失败只 debug：残留成员下一轮被捞到时 ``claim_for_release`` 会
    因状态不符而 SKIPPED，不会造成重复下发。
    """
    try:
        await r.zrem(K_DUE, task_id)
    except Exception:
        log.opt(exception=True).debug("unschedule failed: task_id={}", task_id)


async def due(*, now: int | None = None, limit: int = 500) -> list[str]:
    """到期待重试放行的任务（ticker 用，按 score 升序 = 等最久的先放）。"""
    ts = taskstore.now() if now is None else now
    ids = await r.zrangebyscore(K_DUE, "-inf", ts, start=0, num=limit)
    return [i.decode() if isinstance(i, bytes) else str(i) for i in (ids or [])]


async def stats() -> dict[str, Any]:
    """重排通道概览（ops 端点）。"""
    return {"requeue_pending": int(await r.zcard(K_DUE) or 0)}
