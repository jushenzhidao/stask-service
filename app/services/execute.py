"""worker 执行链路（设计 §5）：

    出队 → CAS SUBMITTED→IN_PROGRESS
    → 派发锁 SET NX（TTL=超时）；锁被占 → 不重发，转超时对账
    → 取 sk → 调上游同步接口（原样 method/path/query/body + X-Task-Id）
    → 分流 → 终态落库（gzip）→ 释放槽 + 清会话 → 可选回调

**派发锁是这个模块的心脏**。队列是 at-least-once，崩溃/重启会重投同一个
task_id。上游是同步扣费接口——重投一次就多扣一次钱。所以规则是：

    锁在 = 可能已扣费 = 绝不再调用上游，转对账让消费日志说话。

锁 TTL = ``worker_timeout + margin``：调用最长就跑这么久，锁比它活得久
一点，确保「调用还在飞」期间锁一定在。锁**不主动释放**——它的作用是覆盖
「本次调用的整个不确定窗口」，提前释放等于放开重投闸门。

四类分流（设计 §8 表）：
| 2xx        | SUCCESS，已实结，零动作          |
| 4xx        | FAILURE，上游未扣或已回滚        |
| 5xx/网络错 | 按 ADR-002 默认不重试 → FAILURE  |
| 超时       | 绝不判死，标 reconcile_pending   |
"""

from __future__ import annotations

import asyncio

import httpx

from app.config import settings
from app.logging import log
from app.redis import K_DISPATCH, r
from app.schemas import FAILURE, IN_PROGRESS, SUBMITTED, SUCCESS
from app.services import codec, dynconf, httpc, slots, taskstore, tokensession

#: 从上游响应尽力回填 channel_id 的候选头（OPEN-DECISIONS ④）
_CHANNEL_HEADERS = ("x-channel-id", "x-oneapi-channel-id", "x-newapi-channel-id")


async def _dispatch_ttl() -> int:
    """派发锁 TTL = 调用超时 + 余量。

    必须 ≥ 一次调用的最长耗时——锁比调用先过期就等于在「调用还在飞」
    期间放开了重投闸门，正是双扣的场景。
    """
    return (await dynconf.get_int("worker_timeout")
            + await dynconf.get_int("dispatch_lock_margin_seconds"))


async def _acquire_dispatch_lock(task_id: str, epoch: int) -> bool:
    """派发锁 SET NX。返回 False = 已有一次派发在飞或已完成过。

    Redis 不可用时**返回 False**（保守）：宁可让任务转对账，也不能在
    锁机制失效的情况下放开调用——那正是双扣的场景。
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


def _pick_channel_id(headers: httpx.Headers) -> int:
    for name in _CHANNEL_HEADERS:
        value = headers.get(name)
        if value and value.isdigit():
            return int(value)
    return 0


async def _finalize(task_id: str, token_hash: str, status: str, *,
                    patch: dict, fail_reason: str = "",
                    channel_id: int = 0, callback_url: str = "") -> None:
    """终态落库 → 释放槽 → 清会话 → 可选回调。

    次序不可换：**先 DB commit 再释放槽**。反过来的话落库失败时槽已还，
    任务还在跑却不占额度，用户能超发。

    ``cas`` 返回 False 表示别人（取消/对账）已经推进过——此时不重复释放
    槽（会造成计数下溢）也不重复回调。
    """
    won = await taskstore.cas(
        task_id, (SUBMITTED, IN_PROGRESS), status,
        patch={**patch, "inflight_slot": False},
        fail_reason=fail_reason,
        channel_id=channel_id or None,
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


async def _mark_reconcile_pending(task_id: str, reason: str) -> None:
    """超时/锁被占：保持非终态，标记待对账（AC-13/AC-17）。

    **绝不判死**——上游可能已经成功并扣了费，这里判 FAILURE 就是用户
    付了钱拿不到结果。槽也**不释放**：任务仍在途，释放会让用户超发。
    """
    await taskstore.patch_data(task_id, {
        "reconcile_pending": True,
        "reconcile_reason": reason,
        "reconcile_checked_at": 0,
    })
    log.warning("task marked reconcile_pending: task_id={} reason={}", task_id, reason)


async def run(task_id: str) -> None:
    """执行入口（taskiq 任务体调用）。任何异常都不再向上抛——
    抛出会触发 taskiq 重投，而重投正是我们要防的。"""
    task = await taskstore.get(task_id)
    if task is None:
        log.warning("execute: task not found: task_id={}", task_id)
        return
    if task["status"] not in (SUBMITTED, IN_PROGRESS):
        log.info("execute: task already terminal: task_id={} status={}",
                 task_id, task["status"])
        return

    data: dict = task.get("data") or {}
    token_hash = str(data.get("token_hash") or "")
    callback_url = str(data.get("callback_url") or "")
    epoch = int(data.get("dispatch_epoch") or 0) + 1

    # ---- 1. CAS 抢执行权（AC-12）----
    if not await taskstore.cas(
        task_id, (SUBMITTED,), IN_PROGRESS, patch={"dispatch_epoch": epoch}
    ):
        # 已是 IN_PROGRESS：要么另一个 worker 在跑，要么是重投。
        # 无论哪种，派发锁都会挡住第二次调用，这里直接交给锁判定。
        log.info("execute: CAS to IN_PROGRESS lost: task_id={}", task_id)

    # ---- 2. 派发锁（AC-13，防双扣的核心）----
    if not await _acquire_dispatch_lock(task_id, epoch):
        await _mark_reconcile_pending(task_id, "dispatch_lock_held")
        return

    # ---- 3. 取 sk ----
    raw_token = await tokensession.get(task_id)
    if not raw_token:
        # 会话丢失：绝不能用别的凭证代打。直接判死——此时还没调过上游
        # （锁刚拿到，调用还没发出），零资金风险。
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
    # X-Task-Id：让上游把本任务 id 记进消费日志，超时对账靠它精确反查
    headers["X-Task-Id"] = task_id

    url = f"{base_url}{path}"
    if query:
        url = f"{url}?{query}"

    # 超时与重试次数走 dynconf：确认上游 5xx 回滚语义后可在管理页直接
    # 打开重试，不必重启 worker（ADR-002）
    timeout_seconds = await dynconf.get_int("worker_timeout")
    client = httpc.shared_client(timeout=httpx.Timeout(timeout_seconds))
    attempts_left = await dynconf.get_int("retry_max")
    connect_attempts_left = await dynconf.get_int("retry_max_connect")
    attempt = 0

    while True:
        attempt += 1
        try:
            resp = await client.request(method, url, headers=headers, content=body)
        except (httpx.ConnectError, httpx.ConnectTimeout) as exc:
            # 连接层失败 = 请求未到达上游 = 零资金风险，重试安全
            if connect_attempts_left > 0:
                connect_attempts_left -= 1
                await asyncio.sleep(settings.retry_backoff_base * attempt)
                continue
            await _finalize(task_id, token_hash, FAILURE,
                            patch={"upstream_status": 0},
                            fail_reason=f"upstream unreachable: {type(exc).__name__}",
                            callback_url=callback_url)
            return
        except (httpx.ReadTimeout, httpx.WriteTimeout, httpx.PoolTimeout) as exc:
            # 请求已发出但没等到响应——上游**可能已经成功并扣费**（AC-17）
            await _mark_reconcile_pending(task_id, f"timeout:{type(exc).__name__}")
            return
        except httpx.HTTPError as exc:
            # 其余传输层异常（协议错、连接中断）：请求已在飞，同样不判死
            await _mark_reconcile_pending(task_id, f"transport:{type(exc).__name__}")
            return

        status = resp.status_code

        # 5xx：ADR-002 默认 retry_max=0（上游的预扣回滚语义未确认）
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
    channel_id = _pick_channel_id(resp.headers)
    status = resp.status_code

    config = await dynconf.get_runtime_config()
    if len(raw) > config.response_max_bytes:
        await _finalize(
            task_id, token_hash, FAILURE,
            patch={"upstream_status": status, "upstream_content_type": content_type,
                   "response_bytes": len(raw)},
            fail_reason=f"response too large: {len(raw)} bytes",
            channel_id=channel_id, callback_url=callback_url,
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
                        channel_id=channel_id, callback_url=callback_url)
        log.info("task success: task_id={} status={} bytes={}", task_id, status, len(raw))
        return

    await _finalize(
        task_id, token_hash, FAILURE, patch=patch,
        fail_reason=f"upstream {status}",
        channel_id=channel_id, callback_url=callback_url,
    )
    log.warning("task failure: task_id={} upstream_status={}", task_id, status)
