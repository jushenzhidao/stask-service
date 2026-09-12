"""``/async/{path:path}`` 通配路由——本服务的全部业务入口。

三个方法映射到三件事：
- ``POST`` / ``PUT`` → 提交任务（202；带 ``Idempotency-Key`` 头才幂等）
- ``GET``            → 查询/回放（202 / 200 / 重放上游错误码）
- ``DELETE``         → 取消（200 / 409）

路由是通配的，所以**准入校验必须前置且严格**（见 services/admission.py）。
路由层只做参数编排与响应塑形，判定逻辑一律委托 services。
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import JSONResponse, Response

from app.deps import ratelimit
from app.deps.auth import Caller, require_caller
from app.errors import error_body
from app.logging import log
from app.schemas import QUEUED, SubmitPlan
from app.services import (
    admission,
    batching,
    codec,
    dynconf,
    flow,
    schedule,
    submit,
    taskstore,
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
    """提交。带 ``Idempotency-Key`` 头时同 token 同 key 幂等回放。

    回放分支回报的是**库里那一行**的状态与创建/排期时刻（不是本次请求算出的值）：
    同一个 key 命中一个已结束的任务时，客户端必须能直接看出它是终态任务，
    而不是一个刚入队的新任务。
    """
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

    # 调度头（X-Delay-Seconds / X-Execute-After，均可选、互斥）。
    # 在落库前就判超限：超出令牌 TTL 容量的延迟是**必然失败**的任务，
    # 提前拒掉才不会留下一条注定判死的行。
    try:
        scheduled_at = schedule.parse(
            request.headers,
            now=taskstore.now(),
            max_delay=schedule.resolve_max_delay(config.max_delay_seconds),
        )
    except schedule.ScheduleError as exc:
        raise HTTPException(
            exc.status,
            error_body(exc.message, "invalid_request_error",
                       code=exc.code, param=exc.param),
        ) from exc

    # 分批头（X-Batch-Size / X-Batch-Wait / X-Batch-Key，均可选）。
    # 归组维度与上限都在服务端裁定，客户端只能声明意图。
    try:
        overrides = batching.parse_overrides(
            request.headers, max_wait=config.max_batch_wait_seconds,
        )
    except batching.BatchParamError as exc:
        raise HTTPException(
            exc.status,
            error_body(exc.message, "invalid_request_error",
                       code=exc.code, param=exc.param),
        ) from exc

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

    # 落库形态：小体明文（可直接 SQL 查看），超阈值或二进制才 gzip+base64
    stored_body, body_encoding = (
        codec.encode(body, config.plain_max_bytes) if body else ("", "")
    )

    plan = SubmitPlan(
        task_id=task_id,
        token_hash=caller.token_hash,
        user_id=auth.user_id,
        model=model,
        method=request.method,
        path=upstream_path,
        query=query,
        headers=headers,
        body=stored_body,
        body_encoding=body_encoding,
        body_truncated=False,
        upstream_base_url=upstream_base,
        idempotency_key=idem_key,
        callback_url=callback_url,
        scheduled_at=scheduled_at,
        batch_size=overrides.size,
        batch_wait=overrides.wait,
        batch_key=overrides.key,
    )

    async def enqueue(tid: str) -> None:
        from app.queue import publish_execute

        await publish_execute(tid)

    try:
        result = await submit.submit(
            caller.raw_token, plan, enqueue=enqueue, config=config,
        )
    except submit.SubmitConflict as exc:
        raise HTTPException(409, str(exc)) from exc
    except submit.SlotExhausted as exc:
        # 层名进 ``code``（``token`` / ``model_token`` / ``global`` + ``_slot_exhausted``）：
        # 三层满起来都是同一个 429，但处置完全不同（换 key / 等这条跑完或换模型 /
        # 跨 token 的容量饱和，换 key 没用）。只把层名写进人类可读的 message 里，
        # 客户端没法据此分支，排障也只能靠正则抠字。
        raise HTTPException(
            429,
            error_body(str(exc), "rate_limit_error",
                       code=f"{exc.layer}_slot_exhausted"),
            headers={"Retry-After": str(await submit.retry_after_seconds(config))},
        ) from exc

    # 幂等回放必须回报**库里那一行的原始事实**（排期 / 状态 / 创建时刻），
    # 不得因本次请求而重新排期（AC-60），也不得把终态任务粉饰成"新任务在排队"。
    #
    # 恒定 `status: QUEUED` 曾把客户端推进死循环：任务早已 FAILURE，客户端拿
    # 同一个 Idempotency-Key 反复提交，每次收到 202 + QUEUED（看起来像刚入队），
    # 于是「提交 → 等 → 轮询失败 → 再提交」永远转不出去。回放要报**真实状态**，
    # 让客户端一眼看出这是同一笔已结束的任务（要重来必须先换 key）。
    task_id, replayed = result.task_id, result.replayed
    stored: dict[str, Any] = {}
    status = QUEUED
    created_at = taskstore.now()
    if replayed:
        # 本次请求带的调度头/分批头一律不生效，所以回头读库里那一行，
        # 而不是用本次请求算出来的值（AC-60）。
        existing = await taskstore.get_meta(task_id)
        if existing is None:
            # 行在 ``submit`` 的 exists() 与这次读之间消失了（上游 new-api 的 24h
            # 清理线会删行，人工删行同样命中）。这里**没有**第二条能救的路径，
            # 所以既不粉饰成"新任务在排队"（客户端随后 GET 只会 404），也不假装
            # 成功：明确告诉它「这一笔的目标已不在」，让客户端重发——重发走创建
            # 分支，同 key 仍得到同一个 task_id，无需换键。
            log.warning("replay row vanished mid-request: task_id={}", task_id)
            raise HTTPException(
                409,
                error_body(
                    "idempotency replay target no longer exists; resend to recreate it",
                    "invalid_request_error", code="replay_target_missing",
                ),
            )
        stored = existing.get("data") or {}
        scheduled_at = int(stored.get("scheduled_at") or 0)
        status = str((existing or {}).get("status") or "") or QUEUED
        # 创建时刻同理：`now()` 会用回放时刻冒充原始时刻，让一个几天前
        # 就结束的任务看起来是刚创建的。
        created_at = int((existing or {}).get("created_at") or 0) or created_at

    location = f"/async{upstream_path}/{task_id}"
    return JSONResponse(
        status_code=202,
        content={
            "task_id": task_id,
            "status": status,
            "created_at": created_at,
            "scheduled_at": scheduled_at,
            "batch_key": stored.get("batch_key", "") or result.batch_key,
            # 与查询视图同口径：内部状态名不外漏（PRD R-20）
            "batch_state": batching.public_batch_state(
                stored.get("batch_state", "") or result.batch_state),
            "replayed": replayed,
        },
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
