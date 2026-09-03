"""统一错误响应：所有非 2xx 归一为 OpenAI 风格 ``{"error": {...}}`` 形制。

路由内 ``raise HTTPException(status, message)`` 即可，处理器统一包装；
detail 已是 ``{"error": ...}`` 形制的原样透传（回放上游错误体的场景）。

注意：SUCCESS/FAILURE 的**字节级回放**不走这里——它直接返回 Response，
上游原文一个字节都不改（设计 §2 表格：「重放上游错误状态码 + 原文」）。
本模块只管**本地失败**（准入、限流、额度、幂等冲突、内部错）。
"""

from __future__ import annotations

from typing import Any

from fastapi import HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse


def error_body(message: str, error_type: str, *, code: str | None = None,
               param: str | None = None) -> dict[str, Any]:
    return {"error": {"message": message, "type": error_type, "param": param, "code": code}}


def _type_for(status_code: int) -> str:
    if status_code == 401:
        return "authentication_error"
    if status_code == 402:
        return "billing_error"
    if status_code == 403:
        return "permission_error"
    if status_code == 429:
        return "rate_limit_error"
    if status_code == 502:
        return "upstream_error"
    return "invalid_request_error" if status_code < 500 else "server_error"


async def http_exception_handler(_request: Request, exc: Exception) -> JSONResponse:
    assert isinstance(exc, HTTPException)
    if isinstance(exc.detail, dict) and "error" in exc.detail:
        body = exc.detail
    else:
        body = error_body(str(exc.detail), _type_for(exc.status_code))
    return JSONResponse(
        status_code=exc.status_code, content=body,
        headers=dict(exc.headers) if exc.headers else None,
    )


async def validation_exception_handler(_request: Request, exc: Exception) -> JSONResponse:
    assert isinstance(exc, RequestValidationError)
    param: str | None = None
    message = "request validation failed"
    errs = exc.errors()
    if errs:
        first = errs[0]
        loc = [str(p) for p in first.get("loc", ())
               if p not in ("body", "query", "path", "header", "cookie")]
        param = ".".join(loc) or None
        message = str(first.get("msg") or message)
    return JSONResponse(
        status_code=422,
        content=error_body(message, "invalid_request_error",
                           code="validation_error", param=param),
    )


async def unhandled_exception_handler(_request: Request, exc: Exception) -> JSONResponse:
    from app.logging import log

    log.exception("unhandled: {}", exc)
    return JSONResponse(
        status_code=500,
        content=error_body("internal server error", "server_error"),
    )


def register_exception_handlers(app: Any) -> None:
    """集中注册（main.py 装配与测试 app 复用同一注册点）。"""
    app.add_exception_handler(HTTPException, http_exception_handler)
    app.add_exception_handler(RequestValidationError, validation_exception_handler)
    app.add_exception_handler(Exception, unhandled_exception_handler)
