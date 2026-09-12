"""查询 / 回放 / 取消（设计 §2）。

**字节级回放**是这里的核心承诺：SUCCESS 时返回的必须是上游原生响应体，
一个字节都不改，Content-Type 原样回设。客户端把 `/async` 前缀去掉后，
拿到的东西应该和直接调同步接口完全一致——这是「加前缀即任务化」这个
产品定位能否成立的关键。

三态映射（设计 §2 表格）：
| QUEUED / IN_PROGRESS   | 202 + {task_id,status,created_at}，支持 ?wait= |
| SUCCESS                          | 200 + 原文回放                                  |
| FAILURE / CANCELED               | 重放上游状态码 + 原文；本地失败用 {"error":{}} |
"""

from __future__ import annotations

from typing import Any

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
    IN_PROGRESS,
    PENDING,
    TERMINAL,
)
from app.services import codec, dynconf, slots, statuscache, taskstore, tokensession
from app.services.batching import public_batch_state
from app.services.dynconf import RuntimeConfig


def _view(task: dict[str, Any]) -> dict[str, Any]:
    data = task.get("data") or {}
    return {
        "task_id": task["task_id"],
        "status": task["status"],
        "created_at": task.get("created_at", 0),
        # 调度/批次是增量字段：未使用时为 0 / 空串，既有字段语义不变。
        # 客户端据此区分「排队中」与「计划中」。
        "scheduled_at": int(data.get("scheduled_at") or 0),
        "batch_key": str(data.get("batch_key") or ""),
        # 内部状态名不外漏（PRD R-20 只允许 waiting/released/""）：
        # `immediate` 非空会让客户端误以为自己在等一个批次
        "batch_state": public_batch_state(str(data.get("batch_state") or "")),
    }


def _replay(task: dict[str, Any], config: RuntimeConfig) -> Response:
    """终态回放。结果已被 TTL 清理 → 410。

    410 而非 404 是有意的：404 意味着「没有这个任务」，客户端会以为
    task_id 写错了；410 明确表达「任务存在过、结果已过期」。
    """
    data: dict[str, Any] = task.get("data") or {}
    stored = str(data.get("upstream_response") or "")
    encoding = str(data.get("upstream_response_encoding") or "")
    upstream_status = int(data.get("upstream_status") or 0)
    content_type = str(data.get("upstream_content_type") or "application/json")

    # CANCELED 从来没调过上游，没有原文可回放——它不是"失败"，
    # 返回 200 + 状态视图，客户端一看就知道是自己取消的
    if task["status"] == CANCELED:
        return JSONResponse(status_code=200, content=_view(task))

    if not stored:
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
        raw = codec.decode(stored, encoding)
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
    """查询端点主逻辑。``wait_seconds > 0`` 时长轮询。

    先探状态再决定拉不拉整行：活跃任务走 202 只需要 status/created_at，
    ``get()`` 的 SELECT * 会把 data 里最大 2MB 的 request_body 整列拉
    回来白白烧带宽——只有确认终态才值得拉整行做回放。
    """
    if config is None:
        config = await dynconf.get_runtime_config()

    status = await _probe_status(task_id)
    if status is None:
        raise HTTPException(404, "task not found")

    if status in TERMINAL:
        task = await taskstore.get(task_id)
        if task is None:                        # 缓存幻影：以 DB 为准
            raise HTTPException(404, "task not found")
        return _replay(task, config)

    if wait_seconds > 0:
        task = await _long_poll(task_id, wait_seconds, config)
        if task is not None and task["status"] in TERMINAL:
            return _replay(task, config)

    meta = await taskstore.get_meta(task_id)
    if meta is None:
        raise HTTPException(404, "task not found")
    return JSONResponse(status_code=202, content=_view(meta))


async def _probe_status(task_id: str) -> str | None:
    """轻量状态探测：Redis 缓存优先，未命中回落 DB 并回填。"""
    status = await statuscache.get(task_id)
    if status is not None:
        return status
    status = await taskstore.get_status(task_id)
    if status is not None:
        await statuscache.set(task_id, status)  # 回填：后续探测不再打 DB
    return status


async def _long_poll(task_id: str, wait_seconds: int,
                     config: RuntimeConfig) -> dict[str, Any] | None:
    """轮询到终态或超时。

    等待期查询走 ``_probe_status``（Redis 优先）：终态由 ``taskstore.cas``
    write-through 秒级可见，N 个等待客户端的轮询几乎全部命中 Redis，
    DB 只在缓存未命中的第一跳被打一次。命中终态后才拉整行做回放。
    """
    budget = min(wait_seconds, config.poll_wait_max_seconds)
    deadline = time.monotonic() + budget
    interval = settings.poll_interval_seconds
    max_interval = min(5.0, budget / 4)  # 上限 5s 或预算的 1/4

    while time.monotonic() < deadline:
        await asyncio.sleep(interval)
        status = await _probe_status(task_id)
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
    if status in TERMINAL:
        raise HTTPException(409, f"task already terminal: {status}")
    if status == IN_PROGRESS:
        raise HTTPException(409, "task is in progress and cannot be canceled")

    won = await taskstore.cas(
        task_id, PENDING, CANCELED,
        fail_reason="canceled by client",
    )
    if not won:
        # CAS 输了：worker 刚好领走了。重读真实状态回报，别撒谎说取消成功
        latest = await taskstore.get_meta(task_id)
        raise HTTPException(
            409, f"task advanced before cancel: {latest['status'] if latest else 'unknown'}"
        )

    data: dict[str, Any] = task.get("data") or {}
    # 等待期任务从未占过槽（掩码 0），release_for_task 什么都不做——
    # 这正是 R-07 要求的「取消不得释放未占用的槽」。若这里无条件还第一层，
    # 就会还掉同 token 其他在途任务的槽。
    await slots.release_for_task(data)

    if int(data.get("scheduled_at") or 0) > 0:
        # 计划任务必须从到期索引摘掉：留着的话 ticker 每轮都会把它捞出来，
        # 虽然 dispatch.release 会因非 QUEUED 而 SKIPPED（不会误执行），
        # 但每次多一轮无用的 DB 读，且日志里持续出现「该放却没放」的噪音。
        from app.services import dispatch

        await dispatch.unschedule(task_id)
    elif data.get("batch_state") == "waiting":
        # 攒批等待期被取消：必须从批次计数里摘掉自己。留着的话这一条永远
        # 凑数但永远不会被放行执行——一批声明 N=100 而其中 5 条被取消，计数
        # 就永远差 5 条到不了 N，只能干等 T 兜底，等待时长凭空变长。
        #
        # 退批必须用**落库的归组键**（batch_key），不能用模型名：客户端可能用
        # X-Batch-Key 指定了别的维度，用模型名会去删另一个批次的成员——本批
        # 计数不减、另一个批次被误删成员。
        from app.services import batching

        key = str(data.get("batch_key") or "") or str(
            data.get("slot_model") or "")
        await batching.leave(task_id, key)

    await tokensession.clear(task_id)

    log.info("task canceled: task_id={}", task_id)
    return JSONResponse(status_code=200,
                        content={"task_id": task_id, "status": CANCELED})
