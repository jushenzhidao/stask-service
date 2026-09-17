"""统一错误响应：所有非 2xx 归一为 OpenAI 风格 ``{"error": {...}}`` 形制。

路由内 ``raise HTTPException(status, message)`` 即可，处理器统一包装；
detail 已是 ``{"error": ...}`` 形制的原样透传（回放上游错误体的场景）。

注意：SUCCESS/FAILURE 的**字节级回放**不走这里——它直接返回 Response，
上游原文一个字节都不改（设计 §2 表格：「重放上游错误状态码 + 原文」）。
本模块只管**本地失败**（准入、限流、额度、幂等冲突、内部错）。

``error.code`` 全部取自下面的 ``ErrorCode``——**任何模块都不得再出现裸字面量**
（守卫 ``test_guards.test_error_codes_are_centralized`` 会把回归拦在 CI）。
"""

from __future__ import annotations

from typing import Any

from fastapi import HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse


class ErrorCode:
    """对外 ``error.code`` 的**唯一**取值集。

    客户端按 ``code`` 分支（换 key / 换模型 / 等一会重试 / 无需重试），所以
    「有哪些码」是一项对外契约，必须能被枚举。裸字面量散在服务与路由里时
    它无法被枚举、也无法被守卫——新增一个码只会在告警里第一次被人看见。
    改动本类**必须**同步 ``docs/SPEC.md`` §5 的错误码说明。
    """

    #: 请求校验失败（FastAPI 校验器与显式 422 共用）
    VALIDATION = "validation_error"
    #: 准入：路径 / 上游地址
    PATH_DENIED = "path_denied"
    PATH_NOT_ALLOWED = "path_not_allowed"
    PATH_TRAVERSAL = "path_traversal"
    UPSTREAM_INVALID = "upstream_invalid"
    UPSTREAM_INVALID_SCHEME = "upstream_invalid_scheme"
    UPSTREAM_USERINFO = "upstream_userinfo"
    UPSTREAM_NOT_ALLOWED = "upstream_not_allowed"
    #: 调度头（延迟下发）
    INVALID_DELAY = "invalid_delay"
    INVALID_EXECUTE_AFTER = "invalid_execute_after"
    DELAY_TOO_LONG = "delay_too_long"
    CONFLICTING_SCHEDULE_HEADERS = "conflicting_schedule_headers"
    #: 分批头
    INVALID_BATCH_SIZE = "invalid_batch_size"
    BATCH_WAIT_TOO_LONG = "batch_wait_too_long"
    #: 幂等回放：目标行在 exists() 与本次读之间消失
    REPLAY_TARGET_MISSING = "replay_target_missing"
    #: 回放：无上游原文的本地失败 / 结果已过 TTL / 存储损坏
    UPSTREAM_NO_BODY = "upstream_no_body"
    RESULT_EXPIRED = "result_expired"
    RESULT_CORRUPTED = "result_corrupted"
    #: 未捕获异常
    INTERNAL_ERROR = "internal_error"

    #: 三层闸门的 429 码后缀——层名在 ``slot_exhausted()`` 里拼接
    SLOT_EXHAUSTED_SUFFIX = "_slot_exhausted"

    @staticmethod
    def slot_exhausted(layer: str) -> str:
        """满额 429 的 ``code``：``token`` / ``model_token`` / ``global`` 三层各自成形。

        层名必须进 ``code``：三层满起来是同一个 429，但处置完全不同
        （换 key / 等当前任务跑完或换模型 / 跨 token 容量饱和而换 key 无用）。
        只把层名写在人类可读的 message 里，客户端无法据以分支。
        """
        return f"{layer}{ErrorCode.SLOT_EXHAUSTED_SUFFIX}"


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
                           code=ErrorCode.VALIDATION, param=param),
    )


async def unhandled_exception_handler(_request: Request, exc: Exception) -> JSONResponse:
    from app.logging import log

    log.exception("unhandled: {}", exc)
    return JSONResponse(
        status_code=500,
        content=error_body("internal server error", "server_error",
                           code=ErrorCode.INTERNAL_ERROR),
    )


def register_exception_handlers(app: Any) -> None:
    """集中注册（main.py 装配与测试 app 复用同一注册点）。"""
    app.add_exception_handler(HTTPException, http_exception_handler)
    app.add_exception_handler(RequestValidationError, validation_exception_handler)
    app.add_exception_handler(Exception, unhandled_exception_handler)
