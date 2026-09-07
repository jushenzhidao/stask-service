"""提交速率限流（滑动窗口，Redis Lua 原子）。

窗口口径按 ``token_hash``——同一用户的所有客户端共享配额。
超限返回 429 + ``Retry-After``（保守取整窗，客户端不必猜）。

Redis 不可用时**放行**：限流是防滥用的软措施，不该成为可用性单点。
真正的资金闸门在上游，那里不放行。
"""

from __future__ import annotations

import time

from fastapi import HTTPException

from app.logging import log
from app.redis import K_RL, LUA_RATE_LIMIT, r


async def check(subject: str) -> None:
    """滑窗限流。超限抛 429。阈值与窗口走 dynconf（可在管理页热改）。"""
    from app.services import dynconf

    config = await dynconf.get_runtime_config()
    limit = config.rate_limit
    if limit <= 0:
        return
    window_seconds = max(config.rate_limit_window_seconds, 1)
    now_ms = int(time.time() * 1000)
    try:
        allowed = await r.eval(
            LUA_RATE_LIMIT, 1, K_RL.format(subject=subject),
            str(now_ms), str(window_seconds * 1000), str(limit),
        )
    except Exception:
        log.opt(exception=True).warning("rate limit check failed, allowing through")
        return
    if not int(allowed or 0):
        raise HTTPException(
            429, "submit rate limit exceeded",
            headers={"Retry-After": str(window_seconds)},
        )
