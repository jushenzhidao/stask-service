"""用户令牌会话：**task_id → Authorization 令牌** 的唯一查询处。

为什么必须存：worker 执行时请求上下文早已结束，而调用上游必须带
**用户本人的令牌**（本服务不做 key 管理，令牌只透传）。

安全口径（红线）：
- 只放 Redis（AOF everysec），**绝不**落 tasks 表、不进日志、不出任何
  HTTP 响应（ops 诊断端点只暴露存在性与剩余 TTL）；
- TTL ``ST_SK_SESSION_TTL_SECONDS``（默认 2h），终态立即清除；
- Redis 丢失的最坏后果是任务无法执行 → 判死 FAILURE。
  本服务零资金动作，不会造成钱款损失。
"""

from __future__ import annotations

from app.config import settings
from app.logging import log
from app.redis import K_SK, r


async def store(task_id: str, raw_token: str) -> None:
    await r.set(K_SK.format(task_id=task_id), raw_token,
                ex=settings.sk_session_ttl_seconds)
    log.debug("token session stored: task_id={} ttl={}s",
              task_id, settings.sk_session_ttl_seconds)


async def get(task_id: str) -> str | None:
    """worker 取用（内部专用，绝不外发）。"""
    token = await r.get(K_SK.format(task_id=task_id))
    if token is None:
        log.warning("token session missing: task_id={}", task_id)
        return None
    return str(token)


async def clear(task_id: str) -> None:
    try:
        await r.delete(K_SK.format(task_id=task_id))
    except Exception:
        log.opt(exception=True).warning("token session clear failed: task_id={}", task_id)


async def session_info(task_id: str) -> dict:
    """诊断视图（ops 端点）：只暴露存在性与剩余 TTL，绝不返回令牌本体。"""
    key = K_SK.format(task_id=task_id)
    exists = await r.get(key) is not None
    ttl = await r.ttl(key) if exists else -2
    return {"exists": exists, "ttl_seconds": int(ttl)}
