"""worker 执行链路（设计 §5）：

    出队 → CAS QUEUED→IN_PROGRESS
    → 派发锁 SET NX（TTL=超时+余量）；锁被占 → 不重发，跳过
    → 取令牌 → 调上游同步接口（原样 method/path/query/body + X-Task-Id）
    → 分流 → 终态落库（gzip）→ 释放槽 + 清会话 → 可选回调

**派发锁是这个模块的心脏**。队列是 at-least-once，崩溃/重启会重投同一个
task_id。上游调用可能有副作用（生成、扣费由上游自理）——重投一次就多调
一次。规则：

    锁在 = 一次调用已发出或在飞 = 绝不再调用上游。

锁 TTL = ``worker_timeout + margin``：调用最长就跑这么久，锁比它活得久
一点。锁**不主动释放**——它的作用是覆盖「本次调用的整个不确定窗口」。

分流（本服务零资金动作，判定只关乎状态正确性）：
| 2xx        | SUCCESS，原文落库供回放           |
| 4xx/5xx    | FAILURE，原文与状态码保留供回放    |
| 连接层错误 | 请求未到达上游 → 重试安全 → 耗尽判 FAILURE |
| 超时/传输断| 结果不可得 → FAILURE（结果拿不回来，留挂着毫无意义）|
"""

from __future__ import annotations

import asyncio

import httpx

from app.config import settings
from app.logging import log
from app.redis import K_DISPATCH, r
from app.schemas import ACTIVE, FAILURE, IN_PROGRESS, PENDING, SUCCESS
from app.services import codec, dynconf, httpc, slots, taskstore, tokensession


async def _dispatch_ttl() -> int:
    """派发锁 TTL = 调用超时 + 余量（必须 ≥ 一次调用的最长耗时）。"""
    return (await dynconf.get_int("worker_timeout")
            + await dynconf.get_int("dispatch_lock_margin_seconds"))


async def _acquire_dispatch_lock(task_id: str, epoch: int) -> bool:
    """派发锁 SET NX。返回 False = 已有一次派发在飞或已完成过。

    Redis 不可用时**返回 False**（保守）：宁可让任务等下一轮兜底，
    也不能在锁机制失效的情况下放开重复调用。
    """
    try:
        ok = await r.set(
            K_DISPATCH.format(task_id=task_id), str(epoch),
            ex=await _dispatch_ttl(), nx=True,
        )
        return bool(ok)
    except Exception:
        log.opt(exception=True).error(
            "dispatch lock unavailable, refusing to dispatch: task_id={}", task_id
        )
        return False


async def _finalize(task_id: str, token_hash: str, status: str, *,
                    patch: dict, fail_reason: str = "",
                    callback_url: str = "") -> None:
    """终态落库 → 释放槽 → 清会话 → 可选回调。

    次序不可换：**先 DB commit 再释放槽**。反过来的话落库失败时槽已还，
    任务还在跑却不占额度，并发保护形同虚设。

    ``cas`` 返回 False 表示别人（取消/兜底 sweeper）已经推进过——此时
    不重复释放槽（会造成计数下溢）也不重复回调。
    """
    won = await taskstore.cas(
        task_id, ACTIVE, status,
        patch=patch,
        fail_reason=fail_reason,
    )
    if not won:
        log.info("terminal race lost (already advanced): task_id={}", task_id)
        return

    await slots.release(token_hash)
    await tokensession.clear(task_id)

    if callback_url:
        from app.queue import publish_notify

        try:
            await publish_notify(task_id)
        except Exception:
            log.opt(exception=True).warning("notify enqueue failed: task_id={}", task_id)


async def run(task_id: str) -> None:
    """执行入口（taskiq 任务体调用）。任何异常都不再向上抛——
    抛出会触发 taskiq 重投，而重投正是我们要防的。"""
    task = await taskstore.get(task_id)
    if task is None:
        log.warning("execute: task not found: task_id={}", task_id)
        return
    if task["status"] not in ACTIVE:
        log.info("execute: task already terminal: task_id={} status={}",
                 task_id, task["status"])
        return

    data: dict = task.get("data") or {}
    token_hash = str(data.get("token_hash") or "")
    callback_url = str(data.get("callback_url") or "")
    epoch = int(data.get("dispatch_epoch") or 0) + 1

    # ---- 1. CAS 抢执行权 ----
    if not await taskstore.cas(
        task_id, PENDING, IN_PROGRESS, patch={"dispatch_epoch": epoch}
    ):
        # 已是 IN_PROGRESS：要么另一个 worker 在跑，要么是重投。
        # 无论哪种，派发锁都会挡住第二次调用，这里直接交给锁判定。
        log.info("execute: CAS to IN_PROGRESS lost: task_id={}", task_id)

    # ---- 2. 派发锁（防重复调用上游的核心）----
    if not await _acquire_dispatch_lock(task_id, epoch):
        log.warning("dispatch lock held, skipping: task_id={}", task_id)
        return

    # ---- 3. 取令牌 ----
    raw_token = await tokensession.get(task_id)
    if not raw_token:
        # 会话丢失：绝不能用别的凭证代打。直接判死——此时还没调过上游
        # （锁刚拿到，调用还没发出）。
        await _finalize(
            task_id, token_hash, FAILURE,
            patch={"upstream_status": 0},
            fail_reason="token session missing (expired or redis lost)",
            callback_url=callback_url,
        )
        return

    # ---- 4. 调上游（同步接口，HTTP 一次调用）----
    await _dispatch(task_id, data, raw_token, token_hash, callback_url)


async def _dispatch(task_id: str, data: dict, raw_token: str,
                    token_hash: str, callback_url: str) -> None:
    base_url = str(data.get("upstream_base_url") or settings.upstream_base_url)
    method = str(data.get("request_method") or "POST").upper()
    path = str(data.get("request_path") or "/")
    query = str(data.get("request_query") or "")
    headers = dict(data.get("request_headers") or {})
    body_b64 = str(data.get("request_body") or "")

    try:
        body = codec.decode(body_b64) if body_b64 else b""
    except ValueError as exc:
        await _finalize(task_id, token_hash, FAILURE,
                        patch={"upstream_status": 0}, fail_reason=str(exc),
                        callback_url=callback_url)
        return

    headers["Authorization"] = f"Bearer {raw_token}"
    # X-Task-Id：让上游把本任务 id 记进访问日志，排障时可精确反查
    headers["X-Task-Id"] = task_id

    url = f"{base_url}{path}"
    if query:
        url = f"{url}?{query}"

    # 超时随 dynconf 可变 → 必须请求级传，绝不能进 shared_client 的 key
    timeout = httpx.Timeout(await dynconf.get_int("worker_timeout"))
    client = httpc.shared_client("upstream")
    attempts_left = await dynconf.get_int("retry_max")
    connect_attempts_left = await dynconf.get_int("retry_max_connect")
    attempt = 0

    while True:
        attempt += 1
        try:
            resp = await client.request(
                method, url, headers=headers, content=body, timeout=timeout
            )
        except (httpx.ConnectError, httpx.ConnectTimeout) as exc:
            # 连接层失败 = 请求未到达上游 = 重试零副作用
            if connect_attempts_left > 0:
                connect_attempts_left -= 1
                await asyncio.sleep(settings.retry_backoff_base * attempt)
                continue
            await _finalize(task_id, token_hash, FAILURE,
                            patch={"upstream_status": 0},
                            fail_reason=f"upstream unreachable: {type(exc).__name__}",
                            callback_url=callback_url)
            return
        except httpx.HTTPError as exc:
            # 超时/传输层中断：请求已发出但结果拿不回来。上游可能已完成，
            # 但本服务无从取回结果——留挂着毫无意义，判死并注明原因。
            # 绝不重试：请求可能已在上游产生副作用。
            await _finalize(task_id, token_hash, FAILURE,
                            patch={"upstream_status": 0},
                            fail_reason=f"upstream timeout/broken: {type(exc).__name__}",
                            callback_url=callback_url)
            return

        status = resp.status_code

        # 5xx：默认 retry_max=0（上游调用可能有副作用）
        if status >= 500 and attempts_left > 0:
            attempts_left -= 1
            await asyncio.sleep(settings.retry_backoff_base * attempt)
            continue

        await _settle_response(task_id, token_hash, resp, callback_url)
        return


async def _settle_response(task_id: str, token_hash: str,
                           resp: httpx.Response, callback_url: str) -> None:
    """响应落库：2xx → SUCCESS，其余 → FAILURE。两者都保存原文供回放。"""
    raw = resp.content
    content_type = resp.headers.get("content-type", "application/json")
    status = resp.status_code

    config = await dynconf.get_runtime_config()
    if len(raw) > config.response_max_bytes:
        await _finalize(
            task_id, token_hash, FAILURE,
            patch={"upstream_status": status, "upstream_content_type": content_type,
                   "response_bytes": len(raw)},
            fail_reason=f"response too large: {len(raw)} bytes",
            callback_url=callback_url,
        )
        log.error("response_too_large: task_id={} bytes={}", task_id, len(raw))
        return

    patch = {
        "upstream_status": status,
        "upstream_content_type": content_type,
        "upstream_response": codec.encode(raw),
        "response_bytes": len(raw),
    }
    if 200 <= status < 300:
        await _finalize(task_id, token_hash, SUCCESS, patch=patch,
                        callback_url=callback_url)
        log.info("task success: task_id={} status={} bytes={}", task_id, status, len(raw))
        return

    await _finalize(
        task_id, token_hash, FAILURE, patch=patch,
        fail_reason=f"upstream {status}",
        callback_url=callback_url,
    )
    log.warning("task failure: task_id={} upstream_status={}", task_id, status)
