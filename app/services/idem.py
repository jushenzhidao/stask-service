"""自动幂等：**task_id 即幂等键**。

设计（v2）：task_id 由请求指纹决定——同一 token 对同一 method/path/query/body
的字节级相同请求，天然映射到同一个 task_id：

    task_id = f"{model_slug}_{sha256(token_hash | method | path | query | body | salt)[:32]}"

客户端重试**不需要**带任何头就自动幂等；想强制重跑同一请求时带
``Idempotency-Key`` 头作为盐（参与指纹计算）换一个 task_id 即可。

去重的事实源是 tasks 表（task_id 查得到 = 已创建过）；Redis 占位只护
「创建链路在飞」这几百毫秒的窗口，防止同键真并发双建：

    DB 有行 → 直接回放
    DB 无行 → SET NX 占位 → 占位者创建；其余短轮询等 DB 出现行 → 回放
    等超时 → 409（绝不放行重建——上游调用可能有副作用）
"""

from __future__ import annotations

import asyncio
import hashlib
import time

from app.config import settings
from app.redis import K_IDEM, LUA_CAS_DELETE, r

#: 占位值——task_id 形态是 ``{slug}_{32hex}``，绝不与本标记碰撞
PENDING = "pending"

_WAIT_INTERVAL_SECONDS = 0.05


def fingerprint_task_id(
    model_slug: str,
    token_hash: str,
    method: str,
    path: str,
    query: str,
    body: bytes,
    salt: str = "",
) -> str:
    """确定性 task_id：同请求恒同 id（自动幂等的根基）。

    ``salt`` 来自可选的 ``Idempotency-Key`` 头——带不同盐即可对同一
    请求体强制创建新任务。
    """
    h = hashlib.sha256()
    for part in (token_hash, method.upper(), path, query, salt):
        h.update(part.encode("utf-8"))
        h.update(b"\x00")
    h.update(body)
    return f"{model_slug}_{h.hexdigest()[:32]}"


async def acquire(task_id: str) -> bool:
    """创建窗口占位（SET NX）。True = 本请求负责走创建链路。"""
    ok = await r.set(
        K_IDEM.format(task_id=task_id), PENDING,
        ex=settings.idem_pending_ttl_seconds, nx=True,
    )
    return bool(ok)


async def release(task_id: str) -> None:
    """创建失败时归还占位（CAS：仅当值仍是 ``pending`` 才删）。"""
    await r.eval(LUA_CAS_DELETE, 1, K_IDEM.format(task_id=task_id), PENDING)


async def settle(task_id: str) -> None:
    """创建成功后清占位——DB 行已是事实源，占位使命完成。"""
    try:
        await r.delete(K_IDEM.format(task_id=task_id))
    except Exception:
        pass  # 占位有 TTL，删失败也会自行过期


async def wait_row(task_id: str, exists) -> bool:
    """短轮询等占位者把行写进 DB。

    ``exists``：``async (task_id) -> bool`` 回调（查 tasks 表）。
    返回 True = 行已出现（调用方回放）；False = 等超时/占位者失败
    （调用方按 409 处理，绝不放行重建）。
    """
    deadline = time.monotonic() + settings.idem_replay_wait_seconds
    key = K_IDEM.format(task_id=task_id)
    while time.monotonic() < deadline:
        if await exists(task_id):
            return True
        if await r.get(key) is None:      # 占位消失且行未出现：创建方失败
            return await exists(task_id)
        await asyncio.sleep(_WAIT_INTERVAL_SECONDS)
    return await exists(task_id)
