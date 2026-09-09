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

**每条终止路径都返回 ``Outcome``**（``app.services.outcome``）。它是 taskiq
的 ``return_value``，taskiq-admin 的任务详情页直接显示——排障不必先回 DB。
返回值只是「报告」，不参与任何控制流：状态早已落库，摘要错了也不影响正确性。
异常依旧一律不上抛（抛 = 重投 = 重复调上游），失败信息经 ``Outcome.ok=False``
表达，由 ``queue.OutcomeMiddleware`` 转成 admin 的 Error 列。
"""

from __future__ import annotations

import asyncio

import httpx

from app.config import settings
from app.logging import log
from app.redis import K_DISPATCH, r
from app.schemas import ACTIVE, FAILURE, IN_PROGRESS, PENDING, SUCCESS
from app.services import (
    artifacts,
    codec,
    dynconf,
    httpc,
    slots,
    taskstore,
    tokensession,
)
from app.services.outcome import Outcome, preview

#: 摘要里最多带几条制品 URL（预签名地址很长，多了会撑爆 result backend）
_OUTCOME_URL_LIMIT = 5


async def _dispatch_ttl() -> int:
    """派发锁 TTL = 调用超时 + 余量（必须 ≥ 一次调用的最长耗时）。"""
    return await dynconf.get_int("worker_timeout") + await dynconf.get_int(
        "dispatch_lock_margin_seconds"
    )


async def _acquire_dispatch_lock(task_id: str, epoch: int) -> bool:
    """派发锁 SET NX。返回 False = 已有一次派发在飞或已完成过。

    Redis 不可用时**返回 False**（保守）：宁可让任务等下一轮兜底，
    也不能在锁机制失效的情况下放开重复调用。
    """
    try:
        ok = await r.set(
            K_DISPATCH.format(task_id=task_id),
            str(epoch),
            ex=await _dispatch_ttl(),
            nx=True,
        )
        return bool(ok)
    except Exception:
        log.opt(exception=True).error(
            "dispatch lock unavailable, refusing to dispatch: task_id={}", task_id
        )
        return False


def _blog(task_id: str, **fields: object):
    """带 task_id 维度的日志 bind 拷贝：extra 字段在 logfire 里是顶层属性。"""
    return log.bind(task_id=task_id, **fields)


#: 非 2xx 响应体的日志预览。完整原文已落库（``upstream_response``），这里只
#: 截前缀供 logfire / taskiq-admin 排障。用户令牌走 Authorization header，绝不
#: 经过响应体，无泄露面；``diagnose=False`` 保证回溯不带变量。
_error_preview = preview


async def _finalize(
    task_id: str,
    token_hash: str,
    status: str,
    *,
    patch: dict,
    fail_reason: str = "",
    callback_url: str = "",
) -> None:
    """终态落库 → 释放槽 → 清会话 → 可选回调。

    次序不可换：**先 DB commit 再释放槽**。反过来的话落库失败时槽已还，
    任务还在跑却不占额度，并发保护形同虚设。

    ``cas`` 返回 False 表示别人（取消/兜底 sweeper）已经推进过——此时
    不重复释放槽（会造成计数下溢）也不重复回调。
    """
    try:
        won = await taskstore.cas(
            task_id,
            ACTIVE,
            status,
            patch=patch,
            fail_reason=fail_reason,
        )
    except Exception:
        # 终态落表失败（DB 挂/锁超时等）：任务停在原状态，交给
        # sweep_stale / sweep_overdue 兜底收敛。异常就地记录后吞掉——
        # 向上抛会触发 taskiq 重投，违反「任何异常不再向上抛」契约。
        _blog(task_id, to_status=status, fail_reason=fail_reason).opt(exception=True).error(
            "finalize: terminal CAS failed (db write error), left for sweeper: "
            "task_id={} to_status={} reason={}",
            task_id,
            status,
            fail_reason,
        )
        return
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


async def run(task_id: str) -> dict:
    """执行入口（taskiq 任务体调用）。任何异常都不再向上抛——
    抛出会触发 taskiq 重投，而重投正是我们要防的。

    返回执行摘要 dict（= taskiq ``return_value``，taskiq-admin 直接显示）。
    摘要纯属报告：控制流与状态正确性完全由落库决定，不看返回值。
    """
    try:
        return (await _run(task_id)).as_dict()
    except Exception as exc:
        # 最后一道网：读表 / CAS IN_PROGRESS 等落表读表异常在此就地记录。
        # 卡死任务由 sweep_stale（锁过期兜底重投/判死）收敛，不依赖重投。
        _blog(task_id).opt(exception=True).error(
            "execute: unexpected error, aborted (sweeper will converge): task_id={}",
            task_id,
        )
        return Outcome(
            task_id=task_id,
            stage="crashed",
            ok=False,
            fail_reason=f"{type(exc).__name__}: {exc}",
            detail=(
                f"execute crashed with {type(exc).__name__}; task left non-terminal "
                "for sweeper to converge"
            ),
        ).as_dict()


async def _run(task_id: str) -> Outcome:
    task = await taskstore.get(task_id)
    if task is None:
        log.warning("execute: task not found: task_id={}", task_id)
        return Outcome(
            task_id=task_id,
            stage="not_found",
            ok=False,
            detail="task row not found (wrong platform, or purged before execution)",
        )
    if task["status"] not in ACTIVE:
        log.info("execute: task already terminal: task_id={} status={}", task_id, task["status"])
        return Outcome(
            task_id=task_id,
            stage="already_terminal",
            status=str(task["status"]),
            detail=f"skipped: task already in terminal state {task['status']}",
        )

    data: dict = task.get("data") or {}
    token_hash = str(data.get("token_hash") or "")
    callback_url = str(data.get("callback_url") or "")
    epoch = int(data.get("dispatch_epoch") or 0) + 1

    # ---- 1. CAS 抢执行权 ----
    if not await taskstore.cas(task_id, PENDING, IN_PROGRESS, patch={"dispatch_epoch": epoch}):
        # 已是 IN_PROGRESS：要么另一个 worker 在跑，要么是重投。
        # 无论哪种，派发锁都会挡住第二次调用，这里直接交给锁判定。
        log.info("execute: CAS to IN_PROGRESS lost: task_id={}", task_id)

    # ---- 2. 派发锁（防重复调用上游的核心）----
    if not await _acquire_dispatch_lock(task_id, epoch):
        log.warning("dispatch lock held, skipping: task_id={}", task_id)
        return Outcome(
            task_id=task_id,
            stage="lock_held",
            detail=(
                "skipped: dispatch lock held — a call is already in flight or was "
                "already made (redelivery guard)"
            ),
        )

    # ---- 3. 取令牌 ----
    raw_token = await tokensession.get(task_id)
    if not raw_token:
        # 会话丢失：绝不能用别的凭证代打。直接判死——此时还没调过上游
        # （锁刚拿到，调用还没发出）。
        reason = "token session missing (expired or redis lost)"
        await _finalize(
            task_id,
            token_hash,
            FAILURE,
            patch={"upstream_status": 0},
            fail_reason=reason,
            callback_url=callback_url,
        )
        return Outcome(
            task_id=task_id,
            stage="token_missing",
            ok=False,
            status=FAILURE,
            fail_reason=reason,
            detail="failed before calling upstream: token session gone, refused to substitute",
        )

    # ---- 4. 调上游（同步接口，HTTP 一次调用）----
    return await _dispatch(task_id, data, raw_token, token_hash, callback_url)


async def _dispatch(
    task_id: str, data: dict, raw_token: str, token_hash: str, callback_url: str
) -> Outcome:
    base_url = str(data.get("upstream_base_url") or settings.upstream_base_url)
    method = str(data.get("request_method") or "POST").upper()
    path = str(data.get("request_path") or "/")
    query = str(data.get("request_query") or "")
    headers = dict(data.get("request_headers") or {})
    body_b64 = str(data.get("request_body") or "")

    model = str(data.get("model") or "")

    try:
        body = codec.decode(body_b64) if body_b64 else b""
    except ValueError as exc:
        await _finalize(
            task_id,
            token_hash,
            FAILURE,
            patch={"upstream_status": 0},
            fail_reason=str(exc),
            callback_url=callback_url,
        )
        return Outcome(
            task_id=task_id,
            stage="bad_request_body",
            ok=False,
            status=FAILURE,
            fail_reason=str(exc),
            model=model,
            request_path=path,
            detail=f"failed before calling upstream: stored request body undecodable ({exc})",
        )

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
            resp = await client.request(method, url, headers=headers, content=body, timeout=timeout)
        except (httpx.ConnectError, httpx.ConnectTimeout) as exc:
            # 连接层失败 = 请求未到达上游 = 重试零副作用
            if connect_attempts_left > 0:
                connect_attempts_left -= 1
                _blog(task_id, attempt=attempt, error=type(exc).__name__, url=url).opt(
                    exception=True
                ).warning(
                    "upstream connect error, retrying: task_id={} attempt={} error={}",
                    task_id,
                    attempt,
                    type(exc).__name__,
                )
                await asyncio.sleep(settings.retry_backoff_base * attempt)
                continue
            reason = f"upstream unreachable: {type(exc).__name__}"
            _blog(task_id, attempt=attempt, method=method, path=path, fail_reason=reason).opt(
                exception=True
            ).error(
                "task failure (upstream unreachable): task_id={} attempts={} reason={}",
                task_id,
                attempt,
                reason,
            )
            await _finalize(
                task_id,
                token_hash,
                FAILURE,
                patch={"upstream_status": 0},
                fail_reason=reason,
                callback_url=callback_url,
            )
            return Outcome(
                task_id=task_id,
                stage="unreachable",
                ok=False,
                status=FAILURE,
                fail_reason=reason,
                attempts=attempt,
                model=model,
                request_path=path,
                detail=(
                    f"request never reached upstream after {attempt} attempt(s): "
                    f"{type(exc).__name__}"
                ),
            )
        except httpx.HTTPError as exc:
            # 超时/传输层中断：请求已发出但结果拿不回来。上游可能已完成，
            # 但本服务无从取回结果——留挂着毫无意义，判死并注明原因。
            # 绝不重试：请求可能已在上游产生副作用。
            reason = f"upstream timeout/broken: {type(exc).__name__}"
            _blog(task_id, attempt=attempt, method=method, path=path, fail_reason=reason).opt(
                exception=True
            ).error(
                "task failure (timeout/broken): task_id={} attempts={} reason={}",
                task_id,
                attempt,
                reason,
            )
            await _finalize(
                task_id,
                token_hash,
                FAILURE,
                patch={"upstream_status": 0},
                fail_reason=reason,
                callback_url=callback_url,
            )
            return Outcome(
                task_id=task_id,
                stage="timeout",
                ok=False,
                status=FAILURE,
                fail_reason=reason,
                attempts=attempt,
                model=model,
                request_path=path,
                detail=(
                    f"call was sent but result unretrievable ({type(exc).__name__}); "
                    "not retried — upstream may already have side effects"
                ),
            )

        status = resp.status_code

        # 5xx：默认 retry_max=0（上游调用可能有副作用）
        if status >= 500 and attempts_left > 0:
            attempts_left -= 1
            _blog(task_id, upstream_status=status, attempt=attempt, url=url).warning(
                "upstream 5xx, retrying: task_id={} status={} attempt={}",
                task_id,
                status,
                attempt,
            )
            await asyncio.sleep(settings.retry_backoff_base * attempt)
            continue

        return await _settle_response(
            task_id, token_hash, resp, data, callback_url, attempts=attempt
        )


async def _settle_response(
    task_id: str,
    token_hash: str,
    resp: httpx.Response,
    data: dict,
    callback_url: str,
    *,
    attempts: int = 1,
) -> Outcome:
    """响应落库：2xx → SUCCESS，其余 → FAILURE。两者都保存原文供回放。"""
    raw = resp.content
    content_type = resp.headers.get("content-type", "application/json")
    status = resp.status_code
    model = str(data.get("model") or "")
    path = str(data.get("request_path") or "")

    config = await dynconf.get_runtime_config()
    if len(raw) > config.response_max_bytes:
        await _finalize(
            task_id,
            token_hash,
            FAILURE,
            patch={
                "upstream_status": status,
                "upstream_content_type": content_type,
                "response_bytes": len(raw),
            },
            fail_reason=f"response too large: {len(raw)} bytes",
            callback_url=callback_url,
        )
        _blog(
            task_id,
            upstream_status=status,
            response_bytes=len(raw),
            limit=config.response_max_bytes,
            model=model,
            request_path=path,
            content_type=content_type,
        ).error(
            "response_too_large: task_id={} status={} bytes={} limit={} model={} path={}",
            task_id,
            status,
            len(raw),
            config.response_max_bytes,
            model,
            path,
        )
        return Outcome(
            task_id=task_id,
            stage="response_too_large",
            ok=False,
            status=FAILURE,
            fail_reason=f"response too large: {len(raw)} bytes",
            upstream_status=status,
            attempts=attempts,
            model=model,
            request_path=path,
            content_type=content_type,
            response_bytes=len(raw),
            detail=(
                f"upstream returned {status} but body {len(raw)} bytes exceeds limit "
                f"{config.response_max_bytes}; body NOT stored"
            ),
        )

    patch = {
        "upstream_status": status,
        "upstream_content_type": content_type,
        "upstream_response": codec.encode(raw),
        "response_bytes": len(raw),
    }
    if 200 <= status < 300:
        # 制品解析与成功落库同一次写入：看板「制品」列读 data.artifacts，
        # 分两次写会出现「已成功但制品暂缺」的中间态。解析恒不抛（内部兜底），
        # 空清单是合法结果（纯文本任务本无制品），绝不因此改判失败。
        result = artifacts.parse_result(raw)
        found = result.items
        patch["artifacts"] = [a.to_dict() for a in found]
        patch["artifact_count"] = len(found)
        # 命中级别（known/walk/inline/none）：线上「成功却无制品」时，
        # 这一个字段即可区分「三级全空」与「首级误命中」，免回捞原始响应
        patch["artifact_parser"] = result.tier
        if found:
            primary = artifacts.primary_url(found)
            if primary:
                # 与 new-api 原生任务对齐：看板「结果」列读这个字段
                patch["result_url"] = primary
        await _finalize(task_id, token_hash, SUCCESS, patch=patch, callback_url=callback_url)
        _blog(
            task_id,
            upstream_status=status,
            response_bytes=len(raw),
            model=model,
            request_path=path,
            artifact_count=len(found),
            artifact_types=",".join(sorted({a.type for a in found})),
            artifact_parser=result.tier,
        ).info(
            "task success: task_id={} status={} bytes={} model={} path={} artifacts={} parser={}",
            task_id,
            status,
            len(raw),
            model,
            path,
            len(found),
            result.tier,
        )
        return Outcome(
            task_id=task_id,
            stage="success",
            ok=True,
            status=SUCCESS,
            upstream_status=status,
            attempts=attempts,
            model=model,
            request_path=path,
            content_type=content_type,
            response_bytes=len(raw),
            artifact_count=len(found),
            artifact_parser=result.tier,
            artifact_types=sorted({a.type for a in found}),
            artifact_urls=[a.url for a in found[:_OUTCOME_URL_LIMIT]],
            result_url=str(patch.get("result_url") or ""),
            detail=(
                f"upstream {status}, {len(raw)} bytes, {len(found)} artifact(s) "
                f"via tier={result.tier}"
            ),
        )

    fail_reason = f"upstream {status}"
    await _finalize(
        task_id,
        token_hash,
        FAILURE,
        patch=patch,
        fail_reason=fail_reason,
        callback_url=callback_url,
    )
    _blog(
        task_id,
        upstream_status=status,
        fail_reason=fail_reason,
        model=model,
        request_path=path,
        content_type=content_type,
        response_bytes=len(raw),
        upstream_preview=_error_preview(raw),
    ).warning(
        "task failure: task_id={} status={} reason={} model={} path={} ct={} bytes={} preview={}",
        task_id,
        status,
        fail_reason,
        model,
        path,
        content_type,
        len(raw),
        _error_preview(raw),
    )
    return Outcome(
        task_id=task_id,
        stage="upstream_error",
        ok=False,
        status=FAILURE,
        fail_reason=fail_reason,
        upstream_status=status,
        attempts=attempts,
        model=model,
        request_path=path,
        content_type=content_type,
        response_bytes=len(raw),
        upstream_preview=_error_preview(raw),
        detail=f"upstream returned {status}; full body stored for replay",
    )
