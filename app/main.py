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
from app.logging import log, setup_logging
from app.services import httpc


def _warn_coexistence_risks() -> None:
    """ADR-006 共存契约的启动期告警（只告警不阻断——单测/本地无 new-api 也要能起）。

    new-api 轮询 ``updateVideoTasks`` 中 ``CacheGetChannel(channel_id)`` 在
    adaptor nil 检查**之前**执行：channel_id 指向不存在的渠道时，该渠道下
    本服务的全部在途任务会被无 CAS 批量强制 FAILURE。platform 自定义值
    挡不住这条路径——必须配一个真实存在的渠道 id。
    """
    if settings.channel_id <= 0:
        log.warning(
            "ST_CHANNEL_ID 未配置（当前 {}）。若上游 new-api 的任务轮询开启，"
            "渠道 0 不存在会导致本服务的在途任务被其批量误判 FAILURE"
            "（CacheGetChannel 失败先于 adaptor nil 检查）。请在 new-api "
            "创建一个占位渠道并把其 id 配到 ST_CHANNEL_ID。",
            settings.channel_id,
        )


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
    _warn_coexistence_risks()
    app = FastAPI(
        title="stask-service",
        version=settings.app_version,
        description="独立异步队列服务：把同步生成接口变成长任务——毫秒返回 task_id，结果异步取回",
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
