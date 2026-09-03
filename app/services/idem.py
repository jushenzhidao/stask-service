"""幂等占位：客户端重试不产生重复任务。

**原子占位**是这里的核心。朴素做法「先查重放、落库后回填」在两个请求
真并发时都查不到 → 双建任务 → 上游被调两次 → 用户被扣两次钱。

现在的流程把「先查后写」变成原子操作：
``SET NX`` 写 ``pending`` 占位 → 只有占位者继续创建链路 → 其余请求短轮询
等同一个键回填为真实 task_id → 回填后回放。

超时/占位消失一律返回 **409**，绝不放行重建：重建会双建双扣，409 让
客户端拿原键重试，资金侧零风险。

状态流转全部在同一 Redis 键上：``pending`` → ``task_id``。
"""

from __future__ import annotations

import asyncio
import time

from app.config import settings
from app.redis import K_IDEM, LUA_CAS_DELETE, r

#: 占位标记。task_id 形态是 ``{slug}_{32hex}``，绝不与本标记碰撞
PENDING = "pending"

_WAIT_INTERVAL_SECONDS = 0.05


def _key(token_hash: str, idem_key: str) -> str:
    return K_IDEM.format(token_hash=token_hash, key=idem_key)


async def get_task_id(token_hash: str, idem_key: str) -> str | None:
    """已回填的 task_id；占位中/键不存在 → None。"""
    value = await r.get(_key(token_hash, idem_key))
    if not value or value == PENDING:
        return None
    return str(value)


async def acquire(token_hash: str, idem_key: str) -> tuple[bool, str | None]:
    """原子占位。返回 ``(owned, replay_task_id)``：

    - ``(True, None)``：抢到占位，调用方走创建链路，落库后 ``set_task_id``
      回填（失败必须 ``release`` 归还）；
    - ``(False, task_id)``：键已回填，直接回放；
    - ``(False, None)``：他方占位中（同键真并发），调用方 ``wait_task_id``。
    """
    ok = await r.set(
        _key(token_hash, idem_key), PENDING,
        ex=settings.idem_pending_ttl_seconds, nx=True,
    )
    if ok:
        return True, None
    return False, await get_task_id(token_hash, idem_key)


async def wait_task_id(token_hash: str, idem_key: str) -> str | None:
    """短轮询等他方占位回填。超时/占位消失 → None（调用方按 409 处理）。"""
    deadline = time.monotonic() + settings.idem_replay_wait_seconds
    key = _key(token_hash, idem_key)
    while time.monotonic() < deadline:
        value = await r.get(key)
        if value is None:                 # 占位已消失：创建方失败，不再等
            return None
        if value != PENDING:
            return str(value)
        await asyncio.sleep(_WAIT_INTERVAL_SECONDS)
    return None


async def set_task_id(token_hash: str, idem_key: str, task_id: str) -> None:
    """占位回填（占位保证单写者，无需 NX；TTL 换成全量保留期）。"""
    await r.set(_key(token_hash, idem_key), task_id, ex=settings.idem_ttl)


async def release(token_hash: str, idem_key: str) -> None:
    """创建失败时归还占位（CAS：仅当值仍是 ``pending`` 才删）。

    CAS 而非直接 DEL：并发场景下本请求失败的同时可能已有别的路径回填了
    真实 task_id，直接删会把有效幂等记录抹掉，导致后续重试重建任务。
    """
    await r.eval(LUA_CAS_DELETE, 1, _key(token_hash, idem_key), PENDING)
