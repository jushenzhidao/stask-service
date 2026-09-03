"""鉴权依赖：提取 Bearer 令牌 + 身份内省。

本服务不签发任何凭证——身份完全由终端用户的 ``sk-`` 令牌决定，
判定权在 billing 服务的 ``/auth/inspect``。
"""

from __future__ import annotations

from dataclasses import dataclass

from fastapi import HTTPException, Request

from app.schemas import UserIdentity
from app.services import identity as identity_svc


@dataclass(slots=True)
class Caller:
    """已鉴权的调用者。

    ``raw_token`` 只在请求内存与 Redis 令牌会话中流转，**绝不**进入
    tasks 表、日志或响应体（红线）。
    """

    raw_token: str
    token_hash: str
    identity: UserIdentity


def extract_bearer(request: Request) -> str:
    header = request.headers.get("authorization") or ""
    scheme, _, token = header.partition(" ")
    if scheme.lower() != "bearer" or not token.strip():
        raise HTTPException(401, "missing or malformed Authorization header")
    return token.strip()


async def require_caller(request: Request) -> Caller:
    """FastAPI 依赖：鉴权 + 内省（带 30s 缓存）。"""
    raw_token = extract_bearer(request)
    th = identity_svc.token_hash(raw_token)
    ident = await identity_svc.inspect(raw_token, th)
    if ident is None:
        raise HTTPException(401, "invalid token")
    return Caller(raw_token=raw_token, token_hash=th, identity=ident)
