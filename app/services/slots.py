"""并发槽：余额额度闸门（设计 §6）。

语义 = 余额付得起几个在途任务。入队即占、终态释放。

三重兜底，缺一不可：
1. Lua 原子占槽——``INCR`` 后超限立刻 ``DECR`` 回滚，杜绝 check-then-act 竞态；
2. 键 TTL（``ST_SLOT_TTL_SECONDS``）——进程在「占槽后、落库前」崩溃时槽位
   不会永久泄漏，键整体过期即清零；
3. 定时校准（``reconcile.recalibrate_slots``）——对照 tasks 表活跃任务数
   回写真值，修正 TTL 兜底不了的漂移（比如释放失败导致的计数虚高）。

**软限制**：槽位数按当前余额算，余额下降不回收已占用的槽。在途任务
该跑完就跑完，不因为余额波动被中途掐死。
"""

from __future__ import annotations

from app.logging import log
from app.redis import K_SLOT, LUA_SLOT_ACQUIRE, LUA_SLOT_RELEASE, r


async def _ttl_seconds(ttl_seconds: int | None) -> int:
    """槽键 TTL：调用方给了就用（复用其快照），否则自取一次运行时值。"""
    if ttl_seconds is not None:
        return ttl_seconds
    from app.services import dynconf

    return (await dynconf.get_runtime_config()).slot_ttl_seconds


async def acquire(token_hash: str, limit: int, *,
                  ttl_seconds: int | None = None) -> bool:
    """占一个槽。返回 False = 已达上限（调用方返 429 + Retry-After）。

    只有 TTL 取值来自动态配置——Lua 脚本本身与参数顺序**不得改动**，
    占槽正确性完全依赖它的原子性。
    """
    ok = await r.eval(
        LUA_SLOT_ACQUIRE, 1,
        K_SLOT.format(token_hash=token_hash),
        str(limit), str(await _ttl_seconds(ttl_seconds)),
    )
    return bool(int(ok or 0))


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
    """当前占用数（ops 诊断用）。"""
    value = await r.get(K_SLOT.format(token_hash=token_hash))
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


async def reset(token_hash: str, value: int, *,
                ttl_seconds: int | None = None) -> None:
    """校准回写（定时任务用；value ≤ 0 时直接删键）。"""
    key = K_SLOT.format(token_hash=token_hash)
    if value <= 0:
        await r.delete(key)
    else:
        await r.set(key, str(value), ex=await _ttl_seconds(ttl_seconds))
