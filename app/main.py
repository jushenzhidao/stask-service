"""FastAPI 应用装配入口。

路由注册顺序即 Starlette 首匹配优先级：healthz → ops → proxy 通配。
``/async/{path:path}`` 永远最后——它吞掉一切。

冒烟纪律：``from app.main import app`` 在无 DB/Redis 环境下必须可导入
（引擎/客户端全部惰性创建）。CI 里没有中间件也要能 import 成功。
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.config import settings
from app.db import close_db
from app.errors import register_exception_handlers
from app.logging import setup_logging
from app.services import httpc


@asynccontextmanager
async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
    try:
        yield
    finally:
        await httpc.close_all()
        await close_db()


def create_app() -> FastAPI:
    """应用工厂：日志 → 异常处理器 → 路由（顺序不可换）。"""
    setup_logging()
    app = FastAPI(
        title="stask-service",
        version=settings.app_version,
        description="同步转异步任务网关：给 new-api 同步生成 API 加 /async 前缀即任务化",
        lifespan=lifespan,
    )

    register_exception_handlers(app)

    from app.healthz import router as health_router
    from app.routers.admin import router as admin_router
    from app.routers.ops import router as ops_router
    from app.routers.proxy import router as proxy_router

    app.include_router(health_router)   # /healthz/live /healthz/ready
    app.include_router(admin_router)    # /admin 看板 + /admin/api/*
    app.include_router(ops_router)      # /ops/*
    app.include_router(proxy_router)    # /async/{path:path} —— 永远最后
    return app


app = create_app()
