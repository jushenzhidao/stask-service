"""管理端鉴权：独立密钥，与终端用户 sk 完全分离。

为什么不复用用户 sk：`/ops` 原本用「任意合法 sk」鉴权，那对**只读聚合**
勉强够用，但管理端要能改全局配置——任何一个普通用户都能把 `max_slots`
改成 9999 或把限流关掉，这是提权。

`ADMIN_KEY` 未配置时管理端点**返回 404 而不是 401**：
- 404 不泄露「这里有个管理后台但你没密钥」；
- 默认不开启，忘配密钥不等于裸奔。

密钥比对用 `secrets.compare_digest`——普通 `==` 会在第一个不同字节处
短路返回，攒够样本可以逐字节爆破。
"""

from __future__ import annotations

import secrets

from fastapi import HTTPException, Request

from app.config import settings


def admin_enabled() -> bool:
    return bool(settings.admin_key.strip())


async def require_admin(request: Request) -> None:
    """FastAPI 依赖：校验 ``X-Admin-Key``（未配置密钥则整个管理面 404）。"""
    if not admin_enabled():
        raise HTTPException(404, "not found")

    provided = (request.headers.get("x-admin-key") or "").strip()
    if not provided:
        # 兼容 Bearer 形式，方便 curl 与浏览器 fetch 复用同一套写法
        header = request.headers.get("authorization") or ""
        scheme, _, token = header.partition(" ")
        if scheme.lower() == "bearer":
            provided = token.strip()

    if not provided or not secrets.compare_digest(provided, settings.admin_key.strip()):
        raise HTTPException(401, "invalid admin key")
