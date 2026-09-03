"""MySQL 引擎/会话工厂。

**零自有表（ADR-001）**：本服务无任何建表职责，只连与 new-api 共享的实例，
读写 ``tasks`` 表里 ``platform='stask'`` 的自有行。无 create_all、无 alembic。

引擎惰性单例（post-fork 安全）：模块导入（gunicorn preload）时不建连接，
首个使用者（worker 进程事件循环内）触发创建。

事务纪律：调用方显式 ``commit``；``get_session`` 依赖在异常时自动 rollback。
终态迁移按「先 DB commit 后 Redis 释放槽」次序——先释放槽再落库时，若落库
失败，槽已还但任务还在跑，会超发。

连接数预算：进程数 × (pool_size + max_overflow) ≤ MySQL max_connections × 0.8
（与 new-api、atask 共享实例，三方相加不能打满）。
"""

from __future__ import annotations

from collections.abc import AsyncIterator

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from app.config import settings

_engine: AsyncEngine | None = None
_session_factory: async_sessionmaker[AsyncSession] | None = None


def get_engine() -> AsyncEngine:
    """惰性引擎单例（post-fork 安全）：首次调用才创建连接池。"""
    global _engine
    if _engine is None:
        _engine = create_async_engine(
            settings.database_url,
            pool_size=settings.db_pool_size,
            max_overflow=settings.db_max_overflow,
            pool_recycle=settings.db_pool_recycle,
            pool_pre_ping=settings.db_pool_pre_ping,
        )
    return _engine


def get_session_factory() -> async_sessionmaker[AsyncSession]:
    """会话工厂单例。"""
    global _session_factory
    if _session_factory is None:
        _session_factory = async_sessionmaker(get_engine(), expire_on_commit=False)
    return _session_factory


async def get_session() -> AsyncIterator[AsyncSession]:
    """FastAPI 依赖：请求级会话；异常自动 rollback（提交由调用方显式执行）。"""
    async with get_session_factory()() as session:
        try:
            yield session
        except Exception:
            await session.rollback()
            raise


async def close_db() -> None:
    """lifespan/worker 退出时释放连接池（幂等）。"""
    global _engine, _session_factory
    if _engine is not None:
        await _engine.dispose()
        _engine = None
    _session_factory = None
