"""身份内省与余额查询（带 Redis 缓存）。

提交链路必须是毫秒级（设计 §4），而 billing 是独立进程 + MySQL 查询。
不缓存的话每次提交都要付两个 RTT。缓存口径：

- 身份内省：``ST_INSPECT_CACHE_TTL``（默认 30s）。令牌被封禁后最长 30s
  仍可提交——可接受，因为真正的扣费判定在上游，封禁令牌
  在上游会被 401 拒掉，任务落 FAILURE，零资金损失。
- 余额：``ST_BALANCE_CACHE_TTL``（默认 30s）。余额只用于**算并发槽位数**，
  是个软闸门，不参与任何资金判定，陈旧 30s 无风险。

缓存键用 ``token_hash`` 而非明文 token——Redis 里绝不出现 sk 明文
（唯一例外是令牌会话，见 tokensession.py，那是功能刚需）。
"""

from __future__ import annotations

import hashlib
import json

from app.logging import log
from app.redis import K_BALANCE, K_INSPECT, r
from app.schemas import UserIdentity
from app.services.providers import BillingError, billing


def token_hash(raw_token: str) -> str:
    """sk → 稳定短哈希（32 hex）。限流/占槽/落库的用户口径全用它。"""
    return hashlib.sha256(raw_token.encode("utf-8")).hexdigest()[:32]


async def inspect(raw_token: str, th: str, *,
                  ttl_seconds: int | None = None) -> UserIdentity | None:
    """令牌 → 身份（缓存 miss 才打 billing）。

    只缓存**成功**结果：401 不缓存，否则用户刚充值/解封还要等 TTL。

    ``ttl_seconds``：调用方从快照传入（在 MUTABLE 白名单）。未传时自取。
    """
    if ttl_seconds is None:
        from app.services import dynconf

        ttl_seconds = (await dynconf.get_runtime_config()).inspect_cache_ttl

    key = K_INSPECT.format(token_hash=th)
    try:
        cached = await r.get(key)
        if cached:
            return UserIdentity(**json.loads(cached))
    except Exception:
        log.opt(exception=True).debug("inspect cache read failed, falling through")

    identity = await billing.inspect(raw_token)
    if identity is None:
        return None
    try:
        await r.set(key, identity.model_dump_json(), ex=ttl_seconds)
    except Exception:
        log.opt(exception=True).debug("inspect cache write failed (non-fatal)")
    return identity


async def balance(raw_token: str, th: str, *,
                  ttl_seconds: int | None = None) -> float | None:
    """可用余额（USD）。查询失败返回 None——调用方按「余额未知」处理。

    余额未知时**不拒绝提交**：这是软闸门，billing 抖动不该让整个提交
    链路瘫痪。调用方回落到最小槽位数（1），既不完全放开也不硬拒。

    ``ttl_seconds``：调用方从快照传入（在 MUTABLE 白名单）。未传时自取。
    """
    if ttl_seconds is None:
        from app.services import dynconf

        ttl_seconds = (await dynconf.get_runtime_config()).balance_cache_ttl

    key = K_BALANCE.format(token_hash=th)
    try:
        cached = await r.get(key)
        if cached is not None:
            return float(cached)
    except Exception:
        log.opt(exception=True).debug("balance cache read failed, falling through")

    try:
        value = await billing.balance(raw_token)
    except BillingError as exc:
        log.warning("balance query failed: status={} msg={}", exc.status, exc.message)
        return None
    except Exception:
        log.opt(exception=True).warning("balance query error")
        return None

    try:
        await r.set(key, str(value), ex=ttl_seconds)
    except Exception:
        log.opt(exception=True).debug("balance cache write failed (non-fatal)")
    return value
