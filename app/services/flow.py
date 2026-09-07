"""查询 / 回放 / 取消（设计 §2）。

**字节级回放**是这里的核心承诺：SUCCESS 时返回的必须是上游原生响应体，
一个字节都不改，Content-Type 原样回设。客户端把 `/async` 前缀去掉后，
拿到的东西应该和直接调同步接口完全一致——这是「加前缀即任务化」这个
产品定位能否成立的关键。

三态映射（设计 §2 表格）：
| NOT_START / IN_PROGRESS | 202 + {task_id,status,created_at}，支持 ?wait= |
| SUCCESS                 | 200 + 原文回放                                  |
| FAILURE / CANCELED      | 重放上游状态码 + 原文；本地失败用 {"error":{}} |
"""

from __future__ import annotations

import asyncio
import time

from fastapi import HTTPException
from fastapi.responses import JSONResponse, Response

from app.config import settings
from app.errors import error_body
from app.logging import log
from app.schemas import (
    ACTIVE,
    CANCELED,
    FAILURE,
    IN_PROGRESS,
    NOT_START,
    SUCCESS,
)
from app.services import codec, dynconf, slots, taskstore, tokensession
from app.services.dynconf import RuntimeConfig


def _view(task: dict) -> dict:
    return {
        "task_id": task["task_id"],
        "status": task["status"],
        "created_at": task.get("created_at", 0),
    }


def _replay(task: dict, config: RuntimeConfig) -> Response:
    """终态回放。结果已被 TTL 清理 → 410。

    410 而非 404 是有意的：404 意味着「没有这个任务」，客户端会以为
    task_id 写错了；410 明确表达「任务存在过、结果已过期」。
    """
    data: dict = task.get("data") or {}
    encoded = str(data.get("upstream_response") or "")
    upstream_status = int(data.get("upstream_status") or 0)
    content_type = str(data.get("upstream_content_type") or "application/json")

    # CANCELED 从来没调过上游，没有原文可回放——它不是"失败"，
    # 返回 200 + 状态视图，客户端一看就知道是自己取消的
    if task["status"] == CANCELED:
        return JSONResponse(status_code=200, content=_view(task))

    if not encoded:
        if data.get("result_purged"):
            return JSONResponse(
                status_code=410,
                content=error_body(
                    f"result expired after {config.result_ttl_seconds}s",
                    "invalid_request_error", code="result_expired",
                ),
            )
        # 无上游原文的本地失败（会话丢失、体超限、超时判死等）
        status = upstream_status if upstream_status >= 400 else 502
        return JSONResponse(
            status_code=status,
            content=error_body(
                str(task.get("fail_reason") or "task failed without upstream response"),
                "upstream_error" if status >= 500 else "invalid_request_error",
                code="upstream_no_body",
            ),
        )

    try:
        raw = codec.decode(encoded)
    except ValueError as exc:
        log.error("replay decode failed: task_id={} err={}", task["task_id"], exc)
        return JSONResponse(
            status_code=500,
            content=error_body("stored response corrupted", "server_error"),
        )

    # 字节级回放：Content-Length 由 Starlette 按 body 重算，不透传上游的
    return Response(content=raw, status_code=upstream_status or 200,
                    media_type=content_type)


async def view(task_id: str, wait_seconds: int = 0,
               config: RuntimeConfig | None = None) -> Response:
    """查询端点主逻辑。``wait_seconds > 0`` 时长轮询。"""
    if config is None:
        config = await dynconf.get_runtime_config()

    task = await taskstore.get(task_id)
    if task is None:
        raise HTTPException(404, "task not found")

    if task["status"] in (SUCCESS, FAILURE, CANCELED):
        return _replay(task, config)

    if wait_seconds > 0:
        task = await _long_poll(task_id, wait_seconds, config) or task
        if task["status"] in (SUCCESS, FAILURE, CANCELED):
            return _replay(task, config)

    return JSONResponse(status_code=202, content=_view(task))


async def _long_poll(task_id: str, wait_seconds: int,
                     config: RuntimeConfig) -> dict | None:
    """轮询到终态或超时。

    只查 ``status`` 列（不拉整行）——结果体可能有 10MB，每 0.5s 拉一次
    会把 DB 带宽打满。命中终态后才拉整行。指数退避降低长等待的查询频率。
    """
    budget = min(wait_seconds, config.poll_wait_max_seconds)
    deadline = time.monotonic() + budget
    interval = settings.poll_interval_seconds
    max_interval = min(5.0, budget / 4)  # 上限 5s 或预算的 1/4

    while time.monotonic() < deadline:
        await asyncio.sleep(interval)
        status = await taskstore.get_status(task_id)
        if status is None:
            return None
        if status not in ACTIVE:
            return await taskstore.get(task_id)
        interval = min(interval * 2, max_interval)
    return None


async def cancel(task_id: str) -> JSONResponse:
    """取消（设计 §2）：排队中 → CANCELED；执行中 → 409。

    「执行中不可取消」是上游是同步接口的必然结果——请求已经发出去了，
    HTTP 没有中止语义。硬取消只会让本地状态和上游实际情况脱节。
    """
    task = await taskstore.get_meta(task_id)
    if task is None:
        raise HTTPException(404, "task not found")

    status = task["status"]
    if status in (SUCCESS, FAILURE, CANCELED):
        raise HTTPException(409, f"task already terminal: {status}")
    if status == IN_PROGRESS:
        raise HTTPException(409, "task is in progress and cannot be canceled")

    won = await taskstore.cas(
        task_id, (NOT_START,), CANCELED,
        fail_reason="canceled by client",
    )
    if not won:
        # CAS 输了：worker 刚好领走了。重读真实状态回报，别撒谎说取消成功
        latest = await taskstore.get_meta(task_id)
        raise HTTPException(
            409, f"task advanced before cancel: {latest['status'] if latest else 'unknown'}"
        )

    token_hash = str((task.get("data") or {}).get("token_hash") or "")
    if token_hash:
        await slots.release(token_hash)
    await tokensession.clear(task_id)

    log.info("task canceled: task_id={}", task_id)
    return JSONResponse(status_code=200,
                        content={"task_id": task_id, "status": CANCELED})
