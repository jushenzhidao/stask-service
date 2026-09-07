"""鉴权依赖：提取 Bearer 令牌。

本服务**不做 key 管理**——不签发凭证、不校验有效性。Authorization 只
透传给上游，有效性由上游判定（无效令牌 = 上游 401 = 任务 FAILURE）。
这里只要求「请求带了 Bearer 令牌」，并计算 token_hash 作为限流/占槽/
落库的用户口径。
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass

from fastapi import HTTPException, Request


@dataclass(slots=True)
class Caller:
    """携带令牌的调用者。

    ``raw_token`` 只在请求内存与 Redis 令牌会话中流转，**绝不**进入
    tasks 表、日志或响应体（红线）。
    """

    raw_token: str
    token_hash: str


def token_hash(raw_token: str) -> str:
    """令牌 → 稳定短哈希（32 hex）。限流/占槽/落库的用户口径全用它。"""
    return hashlib.sha256(raw_token.encode("utf-8")).hexdigest()[:32]


def require_caller(request: Request) -> Caller:
    """提取 Bearer 令牌（无令牌 401）。有效性由上游判定，此处不外呼。"""
    header = request.headers.get("authorization") or ""
    scheme, _, token = header.partition(" ")
    if scheme.lower() != "bearer" or not token.strip():
        raise HTTPException(401, "missing or malformed Authorization header")
    raw = token.strip()
    return Caller(raw_token=raw, token_hash=token_hash(raw))
