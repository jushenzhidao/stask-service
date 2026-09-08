"""提交前鉴权 + 余额预检 + user_id（按 ``AUTH_MODE`` 分流）。

| 模式 | 行为 |
|---|---|
| ``generic``（默认） | **零动作**——上游只是一个 HTTP 服务，key 有效性由它在任务执行时判定（无效 = 上游 401 = 任务 FAILURE 回放），user_id 落 0。提交链路零额外开销。 |
| ``newapi`` | 上游是 new-api 且与本服务共库（ADR-001）——共享库单 SQL 直查 tokens ⋈ users，提交前完成鉴权 + 余额预检 + user_id 回查。 |

## newapi 模式细节

一条 JOIN 拿全「key 有效性、token 状态/过期/额度、用户状态/额度、
user_id」（key 列 uniqueIndex，微秒级），判定语义对齐 new-api
``model.ValidateUserToken`` + 计费预检（源码核实）：

| 判定 | 结果 |
|---|---|
| 行不存在 / 软删除 / token status 禁用或过期 | 401 |
| expired_time != -1 且已过期 | 401 |
| 用户 status != 1（禁用）| 401 |
| token status = 4（耗尽）或非 unlimited 且 remain_quota ≤ 0 | 402 |
| 用户 quota ≤ 0（relay 实扣的就是它）| 402 |
| DB 不可用 | 502（无法鉴权就不放行提交）|

**计费仍零代码**：这里只是准入闸门，预扣/退款/流水由上游 relay 在任务
执行时自理。预检漏放的（缓存窗口内余额刚耗尽）由 relay 拒绝，任务
FAILURE，无资金风险。

缓存（双层，只缓存正向结果）：
1. Redis ``st:auth:{token_hash}``（TTL ``AUTH_CACHE_TTL_SECONDS``，默认
   300s）——跨进程共享、重启不丢；value 只存 user_id，**绝不存 key 本体**；
2. 进程内 dict（TTL 5s）——挡掉同 key 高频提交的 Redis RTT。

安全纪律：raw_token 只进 SQL 参数，绝不落日志/异常消息/Redis。
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass

from sqlalchemy import text

from app.config import settings
from app.logging import log
from app.redis import K_AUTH, r
from app.services import taskstore

#: 进程内短缓存 TTL（秒）：只为挡掉高频提交的 Redis RTT
_LOCAL_TTL = 5.0

_local: dict[str, tuple[float, AuthInfo]] = {}


class UpstreamAuthError(Exception):
    """鉴权/预检失败。``status`` 直接作为 HTTP 状态码抛给客户端。"""

    def __init__(self, status: int, message: str) -> None:
        super().__init__(message)
        self.status = status
        self.message = message


@dataclass(frozen=True, slots=True)
class AuthInfo:
    """一次鉴权的产物：任务归属用户（generic 模式恒 0）。"""

    user_id: int


#: generic 模式的固定产物
_ANONYMOUS = AuthInfo(user_id=0)


def _token_key(raw_token: str) -> str:
    """sk → tokens 表 key 列取值（对齐 new-api TokenAuth 的解析规则）。"""
    key = raw_token.removeprefix("sk-")
    return key.split("-", 1)[0]


# ---------------------------------------------------------------------------
# 缓存
# ---------------------------------------------------------------------------


def clear_cache() -> None:
    """清进程内缓存（测试用；Redis 层靠 TTL 自然过期）。"""
    _local.clear()


def _local_get(token_hash: str) -> AuthInfo | None:
    hit = _local.get(token_hash)
    if hit is None:
        return None
    at, info = hit
    if time.monotonic() - at > _LOCAL_TTL:
        _local.pop(token_hash, None)
        return None
    return info


def _local_put(token_hash: str, info: AuthInfo) -> None:
    if len(_local) > 10_000:            # 简单防无界：满了整体重建
        _local.clear()
    _local[token_hash] = (time.monotonic(), info)


async def _redis_get(token_hash: str) -> AuthInfo | None:
    try:
        raw = await r.get(K_AUTH.format(token_hash=token_hash))
        if not raw:
            return None
        return AuthInfo(user_id=int(json.loads(raw)["user_id"]))
    except Exception:
        return None                     # 缓存不可用 → 直查 DB，不阻塞


async def _redis_put(token_hash: str, info: AuthInfo) -> None:
    try:
        await r.set(
            K_AUTH.format(token_hash=token_hash),
            json.dumps({"user_id": info.user_id}),
            ex=settings.auth_cache_ttl_seconds,
        )
    except Exception:
        pass                            # 写缓存失败无害，下次直查


# ---------------------------------------------------------------------------
# DB 直查
# ---------------------------------------------------------------------------

_CREDENTIAL_SQL = text(
    """
    SELECT t.user_id, t.status, t.expired_time, t.remain_quota,
           t.unlimited_quota, u.status AS user_status, u.quota AS user_quota
    FROM tokens t JOIN users u ON u.id = t.user_id
    WHERE t.`key` = :k AND t.deleted_at IS NULL AND u.deleted_at IS NULL
    LIMIT 1
    """
)

#: new-api 常量：Enabled=1 / Disabled=2 / Expired=3 / Exhausted=4
_ENABLED = 1
_EXHAUSTED = 4


async def _fetch_credential(raw_token: str) -> dict | None:
    """按 key 查 tokens ⋈ users（共享库只读）。异常向上抛（→ 502）。"""
    from app.db import get_session_factory

    async with get_session_factory()() as db:
        row = (
            await db.execute(_CREDENTIAL_SQL, {"k": _token_key(raw_token)})
        ).mappings().first()
    return dict(row) if row else None


def _validate(row: dict) -> AuthInfo:
    """按 new-api ValidateUserToken 语义判定。失败抛 UpstreamAuthError。"""
    token_status = int(row.get("status") or 0)
    if token_status == _EXHAUSTED:
        raise UpstreamAuthError(402, "api key quota exhausted")
    if token_status != _ENABLED:
        raise UpstreamAuthError(401, "api key disabled or expired")

    expired_time = int(row.get("expired_time") or -1)
    if expired_time != -1 and expired_time < taskstore.now():
        raise UpstreamAuthError(401, "api key expired")

    if int(row.get("user_status") or 0) != _ENABLED:
        raise UpstreamAuthError(401, "user account disabled")

    unlimited = bool(row.get("unlimited_quota"))
    if not unlimited and int(row.get("remain_quota") or 0) <= 0:
        raise UpstreamAuthError(402, "insufficient api key quota")
    if int(row.get("user_quota") or 0) <= 0:
        raise UpstreamAuthError(402, "insufficient balance")

    return AuthInfo(user_id=int(row.get("user_id") or 0))


async def authenticate(raw_token: str, token_hash: str) -> AuthInfo:
    """鉴权 + 余额预检 + user_id。失败抛 :class:`UpstreamAuthError`。

    ``AUTH_MODE=generic``（默认）零动作直接放行——通用上游没有可查的
    凭证事实源，有效性由上游在任务执行时判定。

    ``AUTH_MODE=newapi`` 路径：进程内缓存(5s) → Redis 缓存(默认 300s)
    → 共享库单 SQL。
    """
    if settings.auth_mode == "generic":
        return _ANONYMOUS

    cached = _local_get(token_hash)
    if cached is not None:
        return cached

    cached = await _redis_get(token_hash)
    if cached is not None:
        _local_put(token_hash, cached)
        return cached

    try:
        row = await _fetch_credential(raw_token)
    except Exception as exc:
        log.opt(exception=True).error("credential lookup failed")
        raise UpstreamAuthError(502, "credential store unavailable") from exc

    if row is None:
        raise UpstreamAuthError(401, "invalid api key")

    info = _validate(row)
    _local_put(token_hash, info)
    await _redis_put(token_hash, info)
    return info
