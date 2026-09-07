"""健康检查端点（容器探针）。

- ``/healthz/live``：恒 200（零依赖，进程活着即通过）——用它做 liveness，
  依赖不可用时**不该重启进程**，重启治不了 DB 宕机；
- ``/healthz/ready``：Redis PING + DB SELECT 1，任一失败 503，编排层摘流量。

不抛异常、直接构造 JSONResponse——探针路径不该走 error 处理器（避免
异常处理器本身有问题时探针也跟着挂）。
"""

from __future__ import annotations

from fastapi import APIRouter
from fastapi.responses import JSONResponse
from sqlalchemy import text

from app.config import settings
from app.db import get_session_factory
from app.logging import log
from app.redis import r

router = APIRouter()


@router.get("/healthz/live")
async def healthz_live() -> dict[str, str]:
    return {"status": "ok"}


@router.get("/healthz/ready")
async def healthz_ready() -> JSONResponse:
    checks: dict[str, str] = {}

    try:
        await r.ping()
        checks["redis"] = "ok"
    except Exception as exc:
        checks["redis"] = f"fail: {type(exc).__name__}"
        log.warning("readiness redis check failed: {}", type(exc).__name__)

    try:
        async with get_session_factory()() as session:
            await session.execute(text("SELECT 1"))
        checks["db"] = "ok"
    except Exception as exc:
        checks["db"] = f"fail: {type(exc).__name__}"
        log.warning("readiness db check failed: {}", type(exc).__name__)

    ok = all(v == "ok" for v in checks.values())
    return JSONResponse(
        status_code=200 if ok else 503,
        content={
            "status": "ok" if ok else "unavailable",
            "checks": checks,
            # 只上报、不门禁：channel_id=0 在本地/测试是合法的，但放到生产
            # 意味着上游会周期性误杀在途任务。暴露出来让监控能直接抓到，
            # 而不是等人报"任务莫名其妙全 FAILURE"才发现。
            "config": {"channel_id": settings.channel_id},
        },
    )
