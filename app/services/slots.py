"""并发槽 · 三层闸门（纯并发保护，零资金语义）。

三层各自回答一个不同的问题，缺一层就有一种超卖方式：

| 层 | 键 | 回答的问题 | 不启用时的后果 |
|---|---|---|---|
| 1. token 总量 | ``st:slot:{th}`` | 「这个 key 别把服务吃干」 | 单 key 可占满全部 worker |
| 2. (模型, token) | ``st:mslot:{th}:{m}`` | 「这个 key 别在同一个模型上扎堆」 | 贵模型上单个 key 就能打爆渠道 |
| 3. 模型全局 | ``st:gslot:{m}`` | 「所有人加起来别超过这个渠道的容量」 | **10 个 key 各占 3 条 = 30 条在途，上游 3 路配额照样被击穿** |

第三层是本轮新增的关键：上游只给某模型 3 路并发时，任何按 token 划的闸门
都拦不住「多个调用方合计超发」。而攒批只管**放行节奏**，它放行出去的
任务在途多久取决于执行时长，攒批本身并不保证在途数 ≤ 3——那个保证
只能来自这一层。

## 上位掩码（slot_flags）是释放的唯一依据

占槽返回一个位掩码（1=token / 2=(模型,token) / 4=模型全局），业务侧把它
**落进 ``tasks.data.slot_flags``**；释放时读这个掩码按位回退。

绝不能按「当前配置」重算该释放哪几层：运营把某模型的 ``limit_global``
从 0 改成 3 之后，新提交会占第三层，但**改造前提交的在途任务
（slot_flags 里没有第 4 位）**释放时若按新配置回退，就会 DECR 一个
从未 INCR 过的键——还掉别人的槽，制造永久漂移。校准能修计数，但修不了
「谁的槽被还掉了」这件事。

## 三重兜底（沿用既有设计，缺一不可）

1. **Lua 原子占槽**：三层在同一次 EVAL 内判定，任一层超限即在同块内
   回滚已占层，杜绝 check-then-act 竞态与跨层失败窗口；
2. **键 TTL**：进程在「占槽后、落库前」崩溃时槽位不永久泄漏；
3. **定时校准**（``sweeper.recalibrate_slots``）：对照 tasks 表活跃任务数
   回写真值，修正 TTL 兜不住的漂移。
"""

from __future__ import annotations

from app.logging import log
from app.redis import (
    K_GSLOT,
    K_MSLOT,
    K_SLOT,
    LUA_SLOT_ACQUIRE,
    LUA_SLOT_ACQUIRE3,
    LUA_SLOT_RELEASE,
    LUA_SLOT_RELEASE3,
    r,
)

#: 三层占位的位掩码（与 ``LUA_SLOT_ACQUIRE3`` 的返回码一一对应）
FLAG_TOKEN = 1
FLAG_MODEL_TOKEN = 2
FLAG_GLOBAL = 4
FLAG_ALL = FLAG_TOKEN | FLAG_MODEL_TOKEN | FLAG_GLOBAL


def keys_for(token_hash: str, model: str) -> tuple[str, str, str]:
    """三层的键（顺序即 Lua 的 KEYS 顺序，不可换）。"""
    return (
        K_SLOT.format(token_hash=token_hash),
        K_MSLOT.format(token_hash=token_hash, model=model),
        K_GSLOT.format(model=model),
    )


async def _ttl_seconds(ttl_seconds: int | None) -> int:
    """槽键 TTL：调用方给了就用（复用其快照），否则自取一次运行时值。"""
    if ttl_seconds is not None:
        return ttl_seconds
    from app.services import dynconf

    return (await dynconf.get_runtime_config()).slot_ttl_seconds


async def acquire(token_hash: str, limit: int, *,
                  ttl_seconds: int | None = None) -> bool:
    """占一个槽（仅第一层）。返回 False = 已达上限（调用方返 429 + Retry-After）。

    只有 TTL 取值来自动态配置——Lua 脚本本身与参数顺序**不得改动**，
    占槽正确性完全依赖它的原子性。

    保留这个单层入口是给「未配置模型策略」的部署用的：那条路径的行为
    必须与改造前逐字节一致，走单层脚本可以省掉两次 INCR 与一次 EVAL 参数
    展开。
    """
    ok = await r.eval(
        LUA_SLOT_ACQUIRE, 1,
        K_SLOT.format(token_hash=token_hash),
        str(limit), str(await _ttl_seconds(ttl_seconds)),
    )
    return bool(int(ok or 0))


async def acquire_layered(
    token_hash: str,
    model: str,
    *,
    limit_per_token: int,
    limit_model_token: int = 0,
    limit_global: int = 0,
    ttl_seconds: int | None = None,
) -> int:
    """三层一次原子占用。返回**已占层位掩码**，``0`` = 任一层超限未占到。

    上限传 ``0`` = 该层不启用（既不占也不判）。三层全为 0 时直接返回 0
    会让调用方误判为「超限」，所以调用方必须保证至少第一层为正——
    ``max_slots`` 的下限是 1，落库的配置也拒绝把 ``limit_per_token``
    配成 0 以外的非法值。
    """
    mask = await r.eval(
        LUA_SLOT_ACQUIRE3, 3,
        *keys_for(token_hash, model),
        str(limit_per_token), str(limit_model_token), str(limit_global),
        str(await _ttl_seconds(ttl_seconds)),
    )
    return int(mask or 0)


async def release_layered(token_hash: str, model: str, mask: int) -> None:
    """按占位掩码释放。``mask == 0`` 时**什么都不做**（这是必需的语义）。

    ``mask == 0`` 意味着这个任务从未占过任何槽（攒批等待期任务、或提交
    占槽失败但行已建的任务）。此时若无条件回退第一层，就会还掉同 token
    其他在途任务的槽——``LUA_SLOT_RELEASE`` 的下溢保护挡不住这种
    「有余额时的误扣」。这是 PRD 里 AC-47 要求的那条纪律。
    """
    if not mask:
        return
    try:
        await r.eval(LUA_SLOT_RELEASE3, 3, *keys_for(token_hash, model), str(mask))
    except Exception:
        log.opt(exception=True).warning(
            "layered slot release failed (recalibration will fix): "
            "token_hash={} model={} mask={}", token_hash, model, mask,
        )


async def release_for_task(data: dict) -> None:
    """按任务 ``data`` 归还它**实际占过**的层——终态处理的唯一释放入口。

    终态路径（execute 落终态 / flow 取消 / sweeper 判死）**必须**走这里，
    不能直接调 :func:`release`：``dispatch.release`` 占的是三层，只还第一层
    会让 ``st:mslot`` 与 ``st:gslot`` 单调累积，模型全局闸门在若干次任务后
    永久卡死（在途明明是 0，却判定为满）。这是三层闸门唯一的泄漏面。

    掩码与模型名都取**落库值**：占用时写的是哪个，归还就用哪个。

    **``slot_flags`` 缺失一律按 0（从未占槽）处理**，绝不回落成「至少还第一
    层」。缺失是真实存在的合法状态：攒批等待期的行还没占过槽，``release``
    里落库掩码失败回退的行也没有。此时凭猜测还一层，就会还掉同 token 其他
    在途任务的槽——``LUA_SLOT_RELEASE`` 的下溢保护只挡负数，挡不住这种
    「有余额时的误扣」，而校准能修计数、修不了「谁的槽被还掉了」。
    """
    token_hash = str(data.get("token_hash") or "")
    if not token_hash:
        return
    await release_layered(
        token_hash,
        str(data.get("slot_model") or ""),
        int(data.get("slot_flags") or 0),
    )


async def release(token_hash: str) -> None:
    """归还一个槽（幂等友好：Lua 内置下溢保护，DECR 到负数会被拉回 0）。

    释放失败只告警不抛——终态落库已经成功，为了一个计数把整个终态处理
    链路炸掉得不偿失；漂移由定时校准收敛。
    """
    try:
        await r.eval(LUA_SLOT_RELEASE, 1, K_SLOT.format(token_hash=token_hash))
    except Exception:
        log.opt(exception=True).warning(
            "slot release failed (recalibration will fix): token_hash={}", token_hash
        )


async def current(token_hash: str) -> int:
    """第一层当前占用数（ops 诊断用）。"""
    return await _current(K_SLOT.format(token_hash=token_hash))


async def current_model_token(token_hash: str, model: str) -> int:
    """第二层当前占用数。"""
    return await _current(K_MSLOT.format(token_hash=token_hash, model=model))


async def current_global(model: str) -> int:
    """第三层当前占用数（「这个模型现在在上游压了多少条」）。"""
    return await _current(K_GSLOT.format(model=model))


async def _current(key: str) -> int:
    value = await r.get(key)
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


async def reset(token_hash: str, value: int, *,
                ttl_seconds: int | None = None) -> None:
    """第一层校准回写（定时任务用；value ≤ 0 时直接删键）。"""
    await _reset(K_SLOT.format(token_hash=token_hash), value, ttl_seconds)


async def reset_model_token(token_hash: str, model: str, value: int, *,
                            ttl_seconds: int | None = None) -> None:
    """第二层校准回写。"""
    await _reset(K_MSLOT.format(token_hash=token_hash, model=model), value, ttl_seconds)


async def reset_global(model: str, value: int, *,
                       ttl_seconds: int | None = None) -> None:
    """第三层校准回写。"""
    await _reset(K_GSLOT.format(model=model), value, ttl_seconds)


async def _reset(key: str, value: int, ttl_seconds: int | None) -> None:
    if value <= 0:
        await r.delete(key)
    else:
        await r.set(key, str(value), ex=await _ttl_seconds(ttl_seconds))
