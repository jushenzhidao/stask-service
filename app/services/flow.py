"""查询 / 回放 / 取消（设计 §2）。

**字节级回放**是这里的核心承诺：SUCCESS 时返回的必须是上游原生响应体，
Content-Type 原样回设。客户端把 `/async` 前缀去掉后，拿到的东西应该和直接
调同步接口完全一致——这是「加前缀即任务化」这个产品定位能否成立的关键。

三态映射（设计 §2 表格）：
| QUEUED / IN_PROGRESS   | 202 + {task_id,status,created_at,updated_at}，支持 ?wait= |
| SUCCESS                | 200 + 原文回放；JSON 体顶层注入 status（小写）  |
| FAILURE / CANCELED     | 重放上游状态码 + 原文；本地失败用 {"error":{}}  |

## 大小写与状态旁路（三条分支的公共口径）

对外一律**小写**形态（``public_status``），库内与 new-api 共享表恒为大写原生
枚举——转换只发生在这里，绝不回写。

三条分支都挂 ``X-Stask-*`` 状态头。这不是冗余：**二进制结果体（音频/图片）
承载不了 ``status`` 字段**（往 bytes 里加键就是损坏制品），此时头是客户端唯一
能拿到机器可读状态的通道；对 JSON 体，头与体同时给出，客户端可选其一。
"""

from __future__ import annotations

from typing import Any

import asyncio
import json
import time

from fastapi import HTTPException
from fastapi.responses import JSONResponse, Response

from app.config import settings
from app.errors import ErrorCode, error_body
from app.logging import log
from app.schemas import (
    ACTIVE,
    CANCELED,
    IN_PROGRESS,
    PENDING,
    TERMINAL,
    public_status,
)
from app.services import codec, dynconf, slots, statuscache, taskstore, tokensession
from app.services.batching import public_batch_state
from app.services.dynconf import RuntimeConfig

# ---------------------------------------------------------------------------
# 状态旁路头
# ---------------------------------------------------------------------------

#: 状态头的名字统一带 ``X-Stask-`` 前缀（与回调签名的 ``X-Stask-Signature`` 同族），
#: 避免与上游/代理链可能回填的同名头混淆。
_HEADER_STATUS = "X-Stask-Task-Status"
_HEADER_TASK_ID = "X-Stask-Task-Id"
_HEADER_UPSTREAM_STATUS = "X-Stask-Upstream-Status"
_HEADER_RESULT_EXPIRED = "X-Stask-Result-Expired"
_HEADER_REPLAY = "X-Stask-Idempotent-Replay"


def status_headers(task_id: str, status: str, *,
                    upstream_status: int = 0,
                    result_expired: bool = False,
                    replayed: bool = False) -> dict[str, str]:
    """三条响应分支共用的状态旁路头。

    ``X-Stask-Task-Status`` 恒为小写形态，与响应体、回调体一致。响应头不在
    「字节级一致」承诺的范围内（AC-20 只约束**响应体**与 Content-Type），
    所以它可以无成本地补齐 body 给不出的信息。
    """
    headers = {
        _HEADER_STATUS: public_status(status),
        _HEADER_TASK_ID: task_id,
    }
    if upstream_status:
        headers[_HEADER_UPSTREAM_STATUS] = str(upstream_status)
    if result_expired:
        headers[_HEADER_RESULT_EXPIRED] = "true"
    if replayed:
        headers[_HEADER_REPLAY] = "true"
    return headers


def _inject_status(raw: bytes, content_type: str, status: str) -> bytes:
    """把终态状态注入 JSON 结果体的顶层；非 JSON 一律原样返回。

    只认 JSON 的理由：往 ``audio/mpeg`` / ``image/png`` 里塞字段就是损坏制品。
    形态判定以 ``Content-Type`` 为准（与 codec 的显式编码标记同一思路，
    **不做内容嗅探**），解析失败或顶层不是对象时退回原样——回放路径绝不因为
    注入失败而报错，宁可少一个字段也不能把结果打没。

    ``status`` 键冲突时以本服务的实际状态为准（客户端要的正是它）；上游原值
    不会丢失——``data.upstream_response`` 里仍是未改动的原文，管理面可查。
    """
    if not raw or "json" not in content_type.lower():
        return raw
    try:
        parsed = json.loads(raw)
    except (ValueError, UnicodeDecodeError):
        return raw
    if not isinstance(parsed, dict):
        return raw
    parsed["status"] = public_status(status)
    return json.dumps(parsed, ensure_ascii=False).encode("utf-8")


def _view(task: dict[str, Any]) -> dict[str, Any]:
    data = task.get("data") or {}
    return {
        "task_id": task["task_id"],
        "status": public_status(str(task.get("status") or "")),
        "created_at": task.get("created_at", 0),
        # 最后状态变更时刻。只有 created_at 时，「排队 10 分钟但一直在推进」
        # 与「真的卡死」在客户端看来完全一样，无法据此决定要不要重试。
        "updated_at": task.get("updated_at", 0),
        # 调度/批次是增量字段：未使用时为 0 / 空串，既有字段语义不变。
        # 客户端据此区分「排队中」与「计划中」。
        "scheduled_at": int(data.get("scheduled_at") or 0),
        "batch_key": str(data.get("batch_key") or ""),
        # 内部状态名不外漏（PRD R-20 只允许 waiting/released/""）：
        # `immediate` 非空会让客户端误以为自己在等一个批次
        "batch_state": public_batch_state(str(data.get("batch_state") or "")),
    }


def _terminal_json(status_dict: dict[str, Any], status: str) -> dict[str, Any]:
    """本地失败/过期的 error 形制补顶层 ``status``。

    口径统一：**终态 JSON 响应体顶层恒有 status**，客户端不必按分支记两套
    读法——回放分支读注入的字段，本地分支读这里补的字段，二进制分支读头。
    """
    return {**status_dict, "status": public_status(status)}


def _replay(task: dict[str, Any], config: RuntimeConfig) -> Response:
    """终态回放。结果已被 TTL 清理 → 410。

    410 而非 404 是有意的：404 意味着「没有这个任务」，客户端会以为
    task_id 写错了；410 明确表达「任务存在过、结果已过期」。
    """
    data: dict[str, Any] = task.get("data") or {}
    task_id = str(task["task_id"])
    status = str(task.get("status") or "")
    stored = str(data.get("upstream_response") or "")
    encoding = str(data.get("upstream_response_encoding") or "")
    upstream_status = int(data.get("upstream_status") or 0)
    content_type = str(data.get("upstream_content_type") or "application/json")

    # CANCELED 从来没调过上游，没有原文可回放——它不是"失败"，
    # 返回 200 + 状态视图，客户端一看就知道是自己取消的
    if status == CANCELED:
        return JSONResponse(status_code=200, content=_view(task),
                            headers=status_headers(task_id, status))

    if not stored:
        if data.get("result_purged"):
            return JSONResponse(
                status_code=410,
                content=_terminal_json(error_body(
                    f"result expired after {config.result_ttl_seconds}s",
                    "invalid_request_error", code=ErrorCode.RESULT_EXPIRED,
                ), status),
                headers=status_headers(task_id, status, result_expired=True),
            )
        # 无上游原文的本地失败（会话丢失、体超限、超时判死等）
        status_code = upstream_status if upstream_status >= 400 else 502
        return JSONResponse(
            status_code=status_code,
            content=_terminal_json(error_body(
                str(task.get("fail_reason") or "task failed without upstream response"),
                "upstream_error" if status_code >= 500 else "invalid_request_error",
                code=ErrorCode.UPSTREAM_NO_BODY,
            ), status),
            headers=status_headers(task_id, status,
                                    upstream_status=upstream_status),
        )

    try:
        raw = codec.decode(stored, encoding)
    except ValueError as exc:
        log.error("replay decode failed: task_id={} err={}", task_id, exc)
        return JSONResponse(
            status_code=500,
            content=_terminal_json(error_body(
                "stored response corrupted", "server_error",
                code=ErrorCode.RESULT_CORRUPTED,
            ), status),
            headers=status_headers(task_id, status,
                                    upstream_status=upstream_status),
        )

    # 字节级回放：Content-Length 由 Starlette 按 body 重算，不透传上游的。
    # JSON 体顶层注入 status（二进制体原样通过，靠响应头携带状态）。
    return Response(content=_inject_status(raw, content_type, status),
                    status_code=upstream_status or 200,
                    media_type=content_type,
                    headers=status_headers(task_id, status,
                                            upstream_status=upstream_status))


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
    return JSONResponse(status_code=202, content=_view(meta),
                        headers=status_headers(str(meta["task_id"]),
                                                str(meta.get("status") or "")))


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
    return JSONResponse(
        status_code=200,
        content={"task_id": task_id, "status": public_status(CANCELED)},
        headers=status_headers(task_id, CANCELED),
    )
