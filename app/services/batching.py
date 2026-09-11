"""攒批 · 「攒够 N 条或等够 T 秒 → 整批放行」。

## 两个触发器，一个放行点

    提交 ──入批(EVAL)──┬─ 成员数 ≥ N ──► release_model()   ← N 触发
                       └─ 否则等待
    ticker(每轮) ── batch:due 里 score ≤ now ──► release_model()  ← T 触发

两条路径都只调 :func:`release_model`，它内部 **原子摘取**整批成员
（``LUA_BATCH_CLAIM``：DEL 成员键 + ZREM 到期键在同一 EVAL 内），所以
N 触发与 T 触发并发时只有一方拿到成员列表——**不需要额外的放行锁**。

摘到成员后逐条走 :func:`dispatch.release`，那里还有一道 DB 级的
``claim_for_release`` 条件更新兜住「同一成员被两条路径捞到」。两层
（批次级 Redis 互斥 + 成员级 DB 幂等）都不能省：前者防重复摘批，
后者防重复下发。

## 事实源在 DB，Redis 只是可重建索引

Redis 掉一整个批次索引 = 成员在 DB 里仍是 ``batch_state='waiting'``，
:func:`rebuild_from_db` 按 ``batch_due_at`` 重建三个结构。这是「Redis 只放
可重建索引」原则的直接应用——所以入批时 **deadline 必须落库**，
不落库就没法重建 T 触发时刻。

## 按什么维度分批（可配置）

批次键 = **归组键**（``K_BATCH.format(key=...)``），由
:func:`group_key` 算出，优先级 ``X-Batch-Key`` > ``batch_group_by`` 配置：

- ``model``（默认）：按归一化模型名。跨 token 合并，批次更大、N 更容易触发；
- ``token_model``：按 ``token_hash + model``。ARCH Q5 与 PRD R-15/AC-58 的裁决
  口径——与并发维度（模型×token）对齐，「同一批放行的任务竞争同一个并发窗口」，
  凑批填满窗口才有意义。

两者都不改变「放行时各自占各自 token 的槽」这一事实，区别只在**谁和谁算同一批**。
跨模型混批默认不会发生（模型名是键的一部分），但客户端可用 ``X-Batch-Key``
显式指定来强制混批（R-15：显式优先）。
"""

from __future__ import annotations

import asyncio
import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from app.config import settings
from app.logging import log
from app.redis import (
    K_BATCH,
    K_BATCH_DUE,
    LUA_BATCH_CLAIM,
    LUA_BATCH_JOIN,
    LUA_BATCH_LEAVE,
    r,
)
from app.schemas import QUEUED
from app.services import dispatch, dynconf, modelpolicy, taskstore
from app.services.modelpolicy import MAX_BATCH

#: 批次键的 TTL 余量：到期时刻之后再留这么久，防「ticker 卡顿一轮」时
#: 批次键先过期、成员索引凭空消失（DB 兜底能修，但要等一个 sweeper 周期）。
_TTL_MARGIN = 3600


def _as_text(member: Any) -> str:
    """Redis 成员归一为 str：真 redis 返回 bytes，FakeRedis 返回 str。"""
    return member.decode() if isinstance(member, bytes) else str(member)


# ---------------------------------------------------------------------------
# 客户端分批参数（R-14~R-17 / AC-55~AC-58）
# ---------------------------------------------------------------------------

HEADER_SIZE = "x-batch-size"
HEADER_WAIT = "x-batch-wait"
HEADER_KEY = "x-batch-key"

#: ``X-Batch-Key`` 的长度上限（R-15）。超长**截断不报错**，与
#: ``Idempotency-Key`` 的既有处理一致——一个只在长度上超界的键不值得让
#: 整个请求失败。
MAX_KEY_LEN = 64

#: 归组键允许的字符集。归组键会**直接拼进 Redis 键名**（``st:batch:{key}``），
#: 所以必须过一遍白名单：客户端可控的字符串不经约束地进键名，会带来
#: 键空间污染（比如注入 ``:`` 故意与其他键碰撞）与不可读的键。
#: 非法字符替换为 ``_`` 而不是报错——归组键只是「谁和谁一批」的标记，
#: 语义不受个别字符影响，报错反而让客户端难以自查。
_KEY_SAFE = re.compile(r"[^A-Za-z0-9._:-]")


class BatchParamError(Exception):
    """分批头非法 → 400（错误码与字段对齐 PRD §4.3）。"""

    def __init__(self, message: str, code: str, param: str = "") -> None:
        super().__init__(message)
        self.status = 400
        self.message = message
        self.code = code
        self.param = param


@dataclass(frozen=True, slots=True)
class Overrides:
    """客户端在本次请求里声明的分批意图。``None`` = 未声明（交策略/默认）。"""

    size: int | None = None
    wait: int | None = None
    key: str | None = None


def _parse_positive_int(raw: str, *, code: str, header: str) -> int:
    text = raw.strip()
    try:
        value = int(text, 10)
    except ValueError as exc:
        raise BatchParamError(
            f"{header} must be a positive integer, got {raw!r}", code, header,
        ) from exc
    if value < 1:
        raise BatchParamError(
            f"{header} must be a positive integer, got {value}", code, header,
        )
    return value


def parse_overrides(
    headers: Mapping[str, str], *, max_wait: int
) -> Overrides:
    """解析三个分批头。非法值抛 :class:`BatchParamError`（路由层转 400）。

    ``X-Batch-Size`` / ``X-Batch-Wait`` 都是**正整数**（0 与负数一律拒），
    因为它们的语义是「凑够 N 条」与「等 T 秒」——声明 0 条或等 0 秒在语义上
    就是矛盾的，静默当成「不设」只会掩盖客户端的 bug。
    """
    size_raw = headers.get(HEADER_SIZE)
    wait_raw = headers.get(HEADER_WAIT)
    key_raw = headers.get(HEADER_KEY)

    size: int | None = None
    if size_raw is not None:
        size = _parse_positive_int(size_raw, code="invalid_batch_size",
                                  header=HEADER_SIZE)
        if size > MAX_BATCH:
            raise BatchParamError(
                f"X-Batch-Size {size} exceeds the maximum {MAX_BATCH}",
                "invalid_batch_size", HEADER_SIZE,
            )

    wait: int | None = None
    if wait_raw is not None:
        wait = _parse_positive_int(wait_raw, code="invalid_batch_size",
                                   header=HEADER_WAIT)
        if wait > max_wait:
            raise BatchParamError(
                f"X-Batch-Wait {wait}s exceeds max_batch_wait_seconds ({max_wait}s)",
                "batch_wait_too_long", HEADER_WAIT,
            )

    key: str | None = None
    if key_raw is not None:
        key = sanitize_key(key_raw)
        if not key:
            # 全是不安全字符 → 归一后为空。当成「未声明」而不是报错：
            # 空键等价于「不指定」，回落默认维度即可。
            key = None

    return Overrides(size=size, wait=wait, key=key)


def sanitize_key(raw: str) -> str:
    """归组键归一：去空白 → 截断 64 → 非法字符换 ``_``。"""
    return _KEY_SAFE.sub("_", raw.strip()[:MAX_KEY_LEN])


def group_key(
    batch_key: str, *, model: str, token_hash: str, group_by: str
) -> str:
    """算出归组键——**谁和谁算同一批**的唯一判据。

    优先级：``X-Batch-Key``（客户端显式指定）> ``batch_group_by`` 配置的维度。

    ``batch_group_by`` 见 ``config.Settings``：``model``（默认，跨 token 合并）
    或 ``token_model``（ARCH Q5 / AC-58 裁决口径）。未知取值回落 ``model``
    并已在校验处告警，不在这里抛——归组维度配错不该让整个提交链路挂掉。
    """
    if batch_key:
        return batch_key
    # 模型名在函数内**自己归一**，不假设调用方已经归过：归组键是「谁和谁
    # 同一批」的判据，两个调用方对同一模型传了不同大小写 = 同一批被劈成
    # 两批，N 永远凑不齐。归一化是幂等的，多算一次代价可忽略。
    model = modelpolicy.normalize_model(model)
    if group_by == "token_model":
        # 用 token_hash 前缀而非全量：它只是「同一批必须是同一个 token」的
        # 分隔符，前缀足够区分，也让键短一些便于人工排查。
        return sanitize_key(f"{token_hash[:12]}:{model}")
    return sanitize_key(model)


def _keys(key: str) -> tuple[str, str]:
    """成员 ZSET 与到期 ZSET（顺序即 Lua 的 KEYS 顺序，不可换）。"""
    return K_BATCH.format(key=key), K_BATCH_DUE


#: 对外暴露的 ``batch_state`` 取值集（PRD R-20 明确枚举了这三个）。
#: 内部还有 ``immediate`` / ``scheduled`` / ``releasing``，都是实现细节：
#: 直接漏出去会让「靠 batch_state 判断是否在排队」的客户端把**立即任务**
#: 误判成在批次里（``immediate`` 非空），从而去等一个永远不存在的批次事件。
_PUBLIC_STATES = frozenset({"waiting", "released"})


def public_batch_state(state: str) -> str:
    """内部 ``batch_state`` → 可对外暴露的值（PRD R-20）。

    映射规则：
    - ``waiting`` → ``waiting``（在批次里等 N/T）
    - ``releasing`` / ``released`` → ``released``（批次已放行，正准备/已经入队）
    - 其余（``""`` / ``immediate`` / ``scheduled``）→ ``""``，即「不在批次里」

    延迟任务不需要靠 ``batch_state`` 表达：``scheduled_at != 0`` 才是「计划中」
    的正规信号（也与 sweeper 的判据同源）。``releasing`` 是放行过程中的瞬态，
    对客户端而言「已经被放行」比「还在等」更接近事实。
    """
    if state == "waiting":
        return "waiting"
    if state in ("releasing", "released"):
        return "released"
    return ""


async def join(
    task_id: str,
    key: str,
    *,
    batch_size: int,
    batch_wait: int,
    now: int | None = None,
) -> tuple[int, int]:
    """入批。返回 ``(入批后成员数, 本批到期时刻)``。

    到期时刻由 **首个成员** 用 ``ZADD NX`` 写定（见 ``LUA_BATCH_JOIN``）：
    T 是「自本批开始攒起」的窗口，若后续成员都刷新 deadline，涓涓细流
    会让批次永远等不到放行。

    返回的 deadline 是调用方**落库**用的（``data.batch_due_at``）——
    Redis 索引丢失后 :func:`rebuild_from_db` 靠它重建到期索引。所以这里
    返回的必须是 **Redis 里真实生效的那个值**（脚本回读 ZSCORE），不是本
    调用者算出的 ``due_at``：NX 命中时后者偏晚，落库后一旦重建，整批放行
    时刻就会集体后移，T 语义失真。
    """
    ts = taskstore.now() if now is None else now
    due_at = ts + max(1, batch_wait)
    members, due_key = _keys(key)
    count, score = await r.eval(
        LUA_BATCH_JOIN, 2, members, due_key,
        task_id, str(ts), str(due_at), key, str(batch_wait + _TTL_MARGIN),
    )
    n = int(count or 0)
    # ZSCORE 返回字符串（可能是 '1789...' 或 '1.789e+09'），float 中转最稳。
    # 取不到（理论不可能，除键在 EVAL 后瞬间被清）时回落本地值，宁可偏晚
    # 也不能返回 0 —— 0 会让重建把整批判成「立刻到期」而提前放行。
    due_at = int(float(score)) if score is not None else due_at
    log.info("batch join: task_id={} key={} count={}/{} due_at={}",
             task_id, key, n, batch_size, due_at)
    return n, due_at


async def leave(task_id: str, key: str) -> None:
    """成员退批（取消时用）。

    被取消的成员必须从计数里摘掉：一批声明 N=100 而其中 5 条被取消，
    计数就永远差 5 条到不了 N，只能干等 T 兜底，等待时长凭空变长。

    失败只告警：残留成员被放行时 ``dispatch.release`` 会因状态已终态而
    SKIPPED，不会重复下发。
    """
    members, due_key = _keys(key)
    try:
        await r.eval(LUA_BATCH_LEAVE, 2, members, due_key, task_id, key)
    except Exception:
        log.opt(exception=True).warning(
            "batch leave failed: task_id={} key={}", task_id, key)


async def claim(key: str) -> list[str]:
    """原子摘取整批成员。空列表 = 本批已被别人取走。"""
    members, due_key = _keys(key)
    raw = await r.eval(LUA_BATCH_CLAIM, 2, members, due_key, key)
    return [_as_text(m) for m in (raw or [])]


async def release_model(key: str, *, source: str = "batch") -> dict[str, int]:
    """放行一个归组键的整批任务。**这是攒批的唯一放行入口**。

    占不到槽的成员挂回重排通道（``dispatch.schedule``）而不是丢弃或失败——
    客户端要的是「帮我排队」，429 只会把重试逻辑推回给客户端。

    有界并发（``batch_release_concurrency``）：一批 500 条若全并发放行，
    500 次 DB 条件更新 + 500 次 EVAL 会把连接池打满，反而拖慢正常提交。
    """
    task_ids = await claim(key)
    if not task_ids:
        return {"claimed": 0, "released": 0, "requeued": 0, "skipped": 0}

    cfg = await dynconf.get_runtime_config()
    limit = max(1, int(cfg.batch_release_concurrency))
    ceiling = max(10, int(cfg.batch_backoff_max_seconds))
    gate = asyncio.Semaphore(limit)
    tally = {"claimed": len(task_ids), "released": 0, "requeued": 0, "skipped": 0}

    async def requeue(task_id: str) -> None:
        await dispatch.requeue(task_id, backoff_ceiling=ceiling)

    async def one(task_id: str) -> None:
        async with gate:
            try:
                outcome = await dispatch.release(task_id, source=source)
            except Exception:
                # 单条放行炸掉绝不能带走整批：剩下的成员已被 claim 摘出
                # Redis，只有 DB 里的 waiting 状态能救它们（sweeper 兜底）。
                log.opt(exception=True).error(
                    "batch release: member failed: task_id={}", task_id)
                tally["requeued"] += 1
                await requeue(task_id)
                return
            if outcome is dispatch.Released.OK:
                tally["released"] += 1
            elif outcome is dispatch.Released.NO_SLOT:
                tally["requeued"] += 1
                await requeue(task_id)
            else:
                tally["skipped"] += 1

    await asyncio.gather(*(one(t) for t in task_ids))
    log.info("batch released: key={} source={} {}", key, source, tally)
    return tally


# ---------------------------------------------------------------------------
# T 触发 / 索引重建
# ---------------------------------------------------------------------------


async def due_keys(*, now: int | None = None, limit: int = 100) -> list[str]:
    """到期待放行的模型（ticker 用）。"""
    ts = taskstore.now() if now is None else now
    raw = await r.zrangebyscore(K_BATCH_DUE, "-inf", ts, start=0, num=limit)
    return [_as_text(m) for m in (raw or [])]


#: 自旋间隔（秒）与总轮数：4 轮 × 15s ≈ 60s，略短于 cron 周期避免叠跑。
_TICK_INTERVAL = 15
_TICK_ROUNDS = 4


async def admit_due(task_id: str, *, now: int) -> str:
    """到期任务的准入决策。返回 ``released`` / ``batched`` / ``requeued`` / ``skipped``。

    延迟任务在提交时只挂了到期索引、**没有**决定最终走哪条路——那个决定必须
    留到这一刻做，因为策略是热改的，也因为「先等到计划时刻，再参与批次聚合」
    是 PRD 明确允许的叠加语义（§4.1）。所以到期时重新解析一次策略：

    - 该模型配了攒批（``batch >= 2``）→ 作为批次成员入批，由 N/T 触发放行；
    - 否则 → 直接走 :func:`dispatch.release`（占槽失败会自动指数退避重排，
      等价于 R-19 要求的「放行时占不到槽不得报错、只退避」）。

    未到点（索引提前包含了它）或已非 QUEUED → ``skipped``，绝不提前放行。
    """
    from app.services import submit as submit_mod  # 延迟 import 破循环依赖

    # 元数据投影足够（status / scheduled_at / model / request_path / slot_model
    # 都在白名单里），不必拉整行——这条路径每个到期任务跑一次。
    row = await taskstore.get_meta(task_id)
    if row is None or row.get("status") != QUEUED:
        return "skipped"

    data = row.get("data") or {}
    scheduled_at = int(data.get("scheduled_at") or 0)
    if scheduled_at > now:
        # 索引里不该有它（放行侧也有同样的守卫），挂回去下轮再说
        await dispatch.schedule(task_id, scheduled_at)
        return "skipped"

    cfg = await dynconf.get_runtime_config()
    if cfg.batch_enabled:
        policy = modelpolicy.resolve(
            model=str(data.get("model") or ""),
            path=str(data.get("request_path") or ""),
            policies=await dynconf.get_model_policies(),
            default_limit_per_token=cfg.max_slots,
        )
        if policy.batch >= 2:
            model = str(data.get("slot_model") or
                        modelpolicy.normalize_model(str(data.get("model") or "")))
            # 归组键必须与提交时一致：数据里已落 batch_key，这里直接复用。
            # 若按模型重算，客户端用 X-Batch-Key 指定的批次在延迟到点后
            # 会被塞进**另一个**批次（默认维度那个），它的成员永远凑不齐。
            key = str(data.get("batch_key") or "") or group_key(
                "", model=model, token_hash=str(data.get("token_hash") or ""),
                group_by=settings.batch_group_by,
            )
            # 入批前置：batch_state 必须回到 waiting，否则 claim_for_release
            # 不放行它，N/T 触发会空转
            await taskstore.patch_data(
                task_id, {"batch_state": "waiting", "batch_key": key})
            await submit_mod.join_batch(task_id, key, policy)
            return "batched"

    outcome = await dispatch.release(task_id, source="due", now=now)
    if outcome is dispatch.Released.OK:
        return "released"
    if outcome is dispatch.Released.NO_SLOT:
        return "requeued"
    return "skipped"


#: ``tick_once`` 的统计键集，也是 ``tick`` 自旋聚合求和的键集。
#:
#: **必须是同一份常量**：这两处曾各写一份字面量，``tick_once`` 累加
#: ``batches`` 而 ``tick`` 按 ``models`` 求和，于是 cron 每次执行都抛
#: ``KeyError: 'models'``。别误判成「全面停摆」：第 1 轮的放行已在抛错前执行完，
#: 实际后果是 **4 轮自旋退化成 1 轮**（秒级 ``batch_wait`` 的精度从约 30s 劣化到
#: 约 60-90s）外加每分钟一个失败任务——降级 + 噪声，但足以把看板刷满红色。
#: 之所以没被测试拦住：全部用例只调 ``tick_once``，``tick`` 零覆盖，
#: 而它恰恰是 cron 唯一真正调用的入口。写成常量让"两处一致"由结构保证，
#: 而不是靠后来人记得同步改两行字面量。
_TICK_STAT_KEYS = ("batches", "released", "requeued", "skipped", "batched")


async def tick_once(*, now: int | None = None) -> dict[str, int]:
    """扫一轮：到期批次整批放行 + 到期任务（计划/重排）逐条准入。

    两条通道**必须在同一轮里扫**：占槽失败的任务挂在重排通道上，若只扫
    批次通道，这些任务要等到下一次有新提交才可能被带出来。

    重排任务占不到槽 → 再次退避重排（次数落库，退避会持续变长），
    所以一个长期没槽的模型不会把 ticker 拖成忙轮询。
    """
    ts = taskstore.now() if now is None else now
    cfg = await dynconf.get_runtime_config()
    stat = {key: 0 for key in _TICK_STAT_KEYS}

    # 批次放行（T 触发）受 ``batch_enabled`` 这个止血开关管辖……
    if cfg.batch_enabled:
        for key in await due_keys(now=ts):
            tally = await release_model(key, source="due")
            stat["batches"] += 1
            stat["released"] += tally["released"]
            stat["requeued"] += tally["requeued"]
            stat["skipped"] += tally["skipped"]

    # ……但**下面的到期通道绝不能被它连坐**。
    #
    # 这两件事共用本函数只是实现上的复用（都挂在一个 ticker 上），语义上毫无
    # 关系：下面扫的是「计划任务到点」与「占槽失败重排」，都不属于攒批。
    # 早前 ``queue.tick_batches`` 直接拿 ``batch_enabled`` 把整个 tick 掐掉，
    # 于是「关掉攒批止血」会把延迟任务与退避重排一起停掉——延迟任务还不像
    # 批次成员那样有 sweep_stale 兜底（等待期被豁免），只能等超龄判死。
    # 止血开关的语义必须是**精确的**：停掉要停的那件事，别顺手把无关功能关掉。
    ceiling = max(10, int(cfg.batch_backoff_max_seconds))
    for task_id in await dispatch.due(now=ts):
        # 先从索引摘除再准入：放行成功后 release 内部也会 unschedule，
        # 但占槽失败要重新 schedule，顺序反了会把新的到期时刻抹掉。
        await dispatch.unschedule(task_id)
        try:
            result = await admit_due(task_id, now=ts)
        except Exception:
            log.opt(exception=True).error(
                "tick: due task failed: task_id={}", task_id)
            await dispatch.requeue(task_id, backoff_ceiling=ceiling)
            stat["requeued"] += 1
            continue
        if result == "released":
            stat["released"] += 1
        elif result == "batched":
            stat["batched"] += 1
        elif result == "requeued":
            await dispatch.requeue(task_id, backoff_ceiling=ceiling)
            stat["requeued"] += 1
        else:
            stat["skipped"] += 1
    return stat


async def tick() -> dict[str, int]:
    """cron 入口：自旋 ``_TICK_ROUNDS`` 轮把扫描频率提到亚分钟级。

    ``batch_wait`` 可以配到秒级，而 cron 最小粒度是 1 分钟——不自旋的话
    ``batch_wait=30`` 的实际放行延迟最坏会被放大到 90s。
    """
    total = {"rounds": 0, **{key: 0 for key in _TICK_STAT_KEYS}}
    for index in range(_TICK_ROUNDS):
        if index:
            await asyncio.sleep(_TICK_INTERVAL)
        try:
            stat = await tick_once()
        except Exception:
            # 一轮炸掉不能带走后续轮次（Redis 抖动 / DB 瞬断）
            log.opt(exception=True).error("batch tick round failed")
            continue
        total["rounds"] += 1
        for key in _TICK_STAT_KEYS:
            total[key] += stat[key]
    return total


#: ``rebuild_from_db`` 的统计键集。**空批路径与成功路径必须同形**：早前
#: 「无等待成员」的提前返回写的是 ``models``，而成功路径返回 ``batches``，
#: 同一个函数随数据量给出不同键名——调用方（``sweeper._rebuild_batch_index``
#: 的异常兜底）也跟着复制了错的那一份。admin 上看板只是显示错字段，
#: 没有任何测试会失败，所以由常量把三处钉成一份。
_REBUILD_STAT_KEYS = ("batches", "members", "overdue")


async def rebuild_from_db(*, now: int | None = None) -> dict[str, int]:
    """按 DB 事实重建 Redis 批次索引（Redis 丢数据后的兜底）。

    只补不删：Redis 里已有的成员/到期时刻保持原样（``ZADD NX`` 语义），
    避免把正在攒的批次 deadline 重置成「现在 + T」而无限延后放行。

    **超期成员立即放行**：deadline 已过去的批次不该再等下一轮 ticker。
    """
    ts = taskstore.now() if now is None else now
    rows = await taskstore_batch_waiting()
    if not rows:
        return {key: 0 for key in _REBUILD_STAT_KEYS}

    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        # 归组键取落库的 batch_key；老行没有该键时回落归一化模型名
        # （等于改造前的行为），保证两类行都能被重建。
        raw_key = str(row.get("batch_key") or "")
        key = raw_key or modelpolicy.normalize_model(str(row.get("model") or ""))
        grouped.setdefault(sanitize_key(key), []).append(row)

    overdue: list[str] = []
    for key, items in grouped.items():
        members, due_key = _keys(key)
        due_at = min(
            (int(i.get("batch_due_at") or 0) for i in items if i.get("batch_due_at")),
            default=ts,
        )
        pipe = r.pipeline()
        for item in items:
            pipe.zadd(members, {str(item["task_id"]): float(ts)}, nx=True)
        pipe.zadd(due_key, {key: float(due_at)}, nx=True)
        pipe.expire(members, max(1, due_at - ts) + _TTL_MARGIN)
        await pipe.execute()
        if due_at <= ts:
            overdue.append(key)

    for key in overdue:
        await release_model(key, source="rebuild")

    stat = {
        "batches": len(grouped),
        "members": len(rows),
        "overdue": len(overdue),
    }
    log.info("batch index rebuilt from db: {}", stat)
    return stat


async def taskstore_batch_waiting() -> list[dict[str, Any]]:
    """等待放行的成员（延迟 import 打破 taskstore ↔ batching 的循环依赖）。"""
    from app.services import taskstore

    return await taskstore.batch_waiting()


async def stats() -> dict[str, Any]:
    """攒批概览（ops 端点）：每个模型攒了多少、还要等多久。"""
    members = await r.zrangebyscore(K_BATCH_DUE, "-inf", "+inf")
    ts = taskstore.now()
    out: list[dict[str, Any]] = []
    for raw_member in members or []:
        key = _as_text(raw_member)
        score = await r.zscore(K_BATCH_DUE, key)
        out.append({
            # 字段名是 key 而不是 model：归组键可能是 ``token:model`` 或
            # 客户端自定义串（X-Batch-Key），叫 model 会误导排障的人。
            "key": key,
            "waiting": int(await r.zcard(K_BATCH.format(key=key)) or 0),
            "due_in": (int(score) - ts) if score is not None else None,
        })
    out.sort(key=lambda row: row["due_in"] if row["due_in"] is not None else 0)
    return {"batches": out}
