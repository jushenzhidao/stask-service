"""``/async/{path:path}`` 通配路由——本服务的全部业务入口。

三个方法映射到三件事：
- ``POST`` / ``PUT`` → 提交任务（202；带 ``Idempotency-Key`` 头才幂等）
- ``GET``            → 查询/回放（202 / 200 / 重放上游错误码）
- ``DELETE``         → 取消（200 / 409）

路由是通配的，所以**准入校验必须前置且严格**（见 services/admission.py）。
路由层只做参数编排与响应塑形，判定逻辑一律委托 services。
"""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import JSONResponse, Response

from app.deps import ratelimit
from app.deps.auth import Caller, require_caller
from app.errors import error_body
from app.logging import log
from app.schemas import QUEUED, SubmitPlan
from app.services import (
    admission,
    codec,
    dynconf,
    flow,
    submit,
    upstream,
)
from app.services.admission import AdmissionError
from app.services.idem import new_task_id
from app.services.submit import model_slug

router = APIRouter(prefix="/async")

#: 浅解析 body 提 model 的体量上限——只为拿一个字段，没必要 json.loads 一个 2MB 的串
_MODEL_PROBE_MAX_BYTES = 256 * 1024


def _normalize_path(path: str) -> str:
    """路由捕获的 path 不含前导斜杠，补上；同时拒绝路径穿越。"""
    normalized = "/" + path.lstrip("/")
    if ".." in normalized:
        raise AdmissionError(400, "path traversal is not allowed", "path_traversal")
    return normalized


def _extract_model(body: bytes, content_type: str) -> str:
    """浅解析 body 提 ``model``（仅用于 task_id 前缀）。

    失败一律返回空串——model 提不到不影响任何正确性，只是 task_id 前缀
    变成 ``task_``。绝不能因为 body 不是 JSON 就拒绝提交（上游可能接受
    multipart 音频等形态）。
    """
    if not body or len(body) > _MODEL_PROBE_MAX_BYTES:
        return ""
    if "json" not in content_type.lower():
        return ""
    try:
        import json

        parsed = json.loads(body)
    except (ValueError, UnicodeDecodeError):
        return ""
    if isinstance(parsed, dict):
        value = parsed.get("model")
        if isinstance(value, str):
            return value.strip()
    return ""


@router.api_route("/{path:path}", methods=["POST", "PUT"], status_code=202)
async def submit_task(path: str, request: Request) -> Response:
    """提交。带 ``Idempotency-Key`` 头时同 token 同 key 幂等回放。"""
    try:
        upstream_path = _normalize_path(path)
        admission.check_path(upstream_path)
        upstream_base = admission.resolve_upstream(
            request.headers.get("x-upstream-base-url")
        )
    except AdmissionError as exc:
        raise HTTPException(exc.status, exc.message) from exc

    caller: Caller = require_caller(request)
    await ratelimit.check(caller.token_hash)

    # 鉴权 + 余额预检：共享库单 SQL 直查（401 无效 key / 402 余额耗尽 /
    # 502 库不可用），双层缓存。计费仍由上游 relay 在任务执行时自理。
    try:
        auth = await upstream.authenticate(caller.raw_token, caller.token_hash)
    except upstream.UpstreamAuthError as exc:
        raise HTTPException(exc.status, exc.message) from exc

    # 一次请求只取一次快照，后续判定全部复用（body 上限可在管理页热改）
    config = await dynconf.get_runtime_config()

    body = await request.body()
    if len(body) > config.body_max_bytes:
        raise HTTPException(
            413, f"request body exceeds {config.body_max_bytes} bytes"
        )

    content_type = request.headers.get("content-type", "")
    model = _extract_model(body, content_type)

    headers = admission.clean_headers(dict(request.headers))
    query = request.url.query or ""
    # Idempotency-Key（可选）：带 = 显式幂等（同 token 同 key 恒同 task_id），
    # 不带 = 每次提交都是新任务。
    idem_key = (request.headers.get("idempotency-key") or "").strip()[:128]
    callback_url = (request.headers.get("x-callback-url") or "").strip()[:1024]

    task_id = new_task_id(model_slug(model), caller.token_hash, idem_key)

    plan = SubmitPlan(
        task_id=task_id,
        token_hash=caller.token_hash,
        user_id=auth.user_id,
        model=model,
        method=request.method,
        path=upstream_path,
        query=query,
        headers=headers,
        body_b64=codec.encode(body) if body else "",
        body_truncated=False,
        upstream_base_url=upstream_base,
        idempotency_key=idem_key,
        callback_url=callback_url,
    )

    async def enqueue(tid: str) -> None:
        from app.queue import publish_execute

        await publish_execute(tid)

    try:
        task_id, replayed = await submit.submit(
            caller.raw_token, plan, enqueue=enqueue, config=config,
        )
    except submit.SubmitConflict as exc:
        raise HTTPException(409, str(exc)) from exc
    except submit.SlotExhausted as exc:
        raise HTTPException(
            429, str(exc),
            headers={"Retry-After": str(await submit.retry_after_seconds(config))},
        ) from exc

    location = f"/async{upstream_path}/{task_id}"
    return JSONResponse(
        status_code=202,
        content={"task_id": task_id, "status": QUEUED, "replayed": replayed},
        headers={"Location": location},
    )


@router.get("/{path:path}")
async def get_task(
    path: str,
    wait: int = Query(0, ge=0),
) -> Response:
    """查询/回放。task_id 取路径末段，形态正则预筛。

    ``wait`` 的上限**不能**写进 ``Query(le=...)``：那个默认值在模块 import
    时求值，会把进程启动那一刻的 env 值烧死进 OpenAPI schema 与校验器，
    管理页热改 ``poll_wait_max_seconds`` 将完全不生效。
    """
    task_id = admission.extract_task_id(_normalize_path(path))
    if not task_id:
        raise HTTPException(404, "task id not found in path")

    config = await dynconf.get_runtime_config()
    if wait > config.poll_wait_max_seconds:
        raise HTTPException(
            422,
            error_body(
                f"Input should be less than or equal to {config.poll_wait_max_seconds}",
                "invalid_request_error", code="validation_error", param="wait",
            ),
        )
    return await flow.view(task_id, wait_seconds=wait, config=config)


@router.delete("/{path:path}")
async def cancel_task(path: str) -> Response:
    """取消。"""
    task_id = admission.extract_task_id(_normalize_path(path))
    if not task_id:
        raise HTTPException(404, "task id not found in path")
    return await flow.cancel(task_id)


@router.api_route("/{path:path}",
                  methods=["PATCH", "HEAD", "OPTIONS", "TRACE"], include_in_schema=False)
async def method_not_allowed(path: str) -> Response:
    """仅 POST/PUT/GET/DELETE。其余显式 405，不落到 404。"""
    log.debug("method not allowed on /async/{}", path)
    raise HTTPException(405, "method not allowed on /async")
