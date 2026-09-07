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


#: 这些环境里 channel_id 缺失不再容忍：静默带病启动比起不来危险得多
_STRICT_ENVS = frozenset({"prod", "production"})


def _warn_coexistence_risks() -> None:
    """ADR-006 共存契约的启动期校验。

    new-api 轮询 ``updateVideoTasks`` 中 ``CacheGetChannel(channel_id)`` 在
    adaptor nil 检查**之前**执行：channel_id 指向不存在的渠道时，该渠道下
    本服务的全部在途任务会被无 CAS 批量强制 FAILURE。platform 自定义值
    挡不住这条路径——必须配一个真实存在的渠道 id。

    分级策略：
    - 非生产（dev / test / 本地）：只打 warning，保证单测和无 new-api 的
      本地环境照样能起来；
    - 生产（``ST_APP_ENV=prod|production``）：**直接抛错阻断启动**。
      渠道 0 不是"少个功能"，是上游会周期性误杀在途任务——让它起不来，
      比让它在监控盲区里慢慢吃任务要好。
    """
    if settings.channel_id > 0:
        return

    hint = (
        "ST_CHANNEL_ID 未配置（当前 {}）。上游 new-api 的任务轮询中 "
        "CacheGetChannel 先于 adaptor nil 检查执行，渠道不存在会导致本服务"
        "在途任务被其无 CAS 批量强制 FAILURE。请在 new-api 建一个**禁用状态**的"
        "占位渠道（渠道缓存含禁用渠道），把它的 id 填到这里。"
    )
    if settings.app_env.strip().lower() in _STRICT_ENVS:
        raise RuntimeError(hint.format(settings.channel_id))
    log.warning(hint, settings.channel_id)


@asynccontextmanager
async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
    try:
        yield
    finally:
        await httpc.close_all()
        await close_db()


def create_app() -> FastAPI:
    """应用工厂：日志 → observability → 异常处理器 → 路由（顺序不可换）。"""
    setup_logging()
    from app.observability import instrument_fastapi, setup as setup_observability

    setup_observability("web")
    _warn_coexistence_risks()
    app = FastAPI(
        title="stask-service",
        version=settings.app_version,
        description="独立异步队列服务：把同步生成接口变成长任务——毫秒返回 task_id，结果异步取回",
        lifespan=lifespan,
    )
    instrument_fastapi(app)

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
