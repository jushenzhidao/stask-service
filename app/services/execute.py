"""worker 执行链路（设计 §5）：

    出队 → CAS QUEUED→IN_PROGRESS
    → 派发锁 SET NX（TTL=超时+余量）；锁被占 → 不重发，跳过
    → 取令牌 → 调上游同步接口（原样 method/path/query/body + X-Task-Id）
    → 分流 → 终态落库（明文优先，超限 gzip+b64）→ 释放槽 + 清会话 → 可选回调

请求与响应**两头都落日志**（``upstream call`` / ``task success`` / ``task
failure``，经 ``_digest`` 截断、不脱敏）：一条 trace 里即可回答「发了什么、
回来什么」，不必先去 DB 解 ``tasks.data``。凭证头是唯一例外（AC-30，见
``_log_headers``）。

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
from typing import Any

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
from app.services.outcome import Outcome, preview, upstream_error_detail
from app.services.logdigest import _digest, _log_headers

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


def _blog(task_id: str, **fields: object) -> Any:
    """带 task_id 维度的日志 bind 拷贝：extra 字段在 logfire 里是顶层属性。"""
    return log.bind(task_id=task_id, **fields)

# ---- 请求/响应体日志摘要（已抽出）----------------------------------------
# 体量上限与 `_digest` / `_log_headers` 全在 `app/services/logdigest.py`——
# 那是一整块纯函数，与执行流水线无耦合，抽出去让本文件回到可读区间。


async def _finalize(
    task_id: str,
    data: dict[str, Any],
    status: str,
    *,
    patch: dict[str, Any],
    fail_reason: str = "",
    callback_url: str = "",
    private_patch: dict[str, Any] | None = None,
) -> None:
    """终态落库 → 释放槽 → 清会话 → 可选回调。

    次序不可换：**先 DB commit 再释放槽**。反过来的话落库失败时槽已还，
    任务还在跑却不占额度，并发保护形同虚设。

    ``cas`` 返回 False 表示别人（取消/兜底 sweeper）已经推进过——此时
    不重复释放槽（会造成计数下溢）也不重复回调。

    收 ``data``（不是 ``token_hash``）是因为释放必须按**落库的占位掩码**
    ``slot_flags`` 逐层回退：三层闸门下只还第一层会让 ``st:mslot`` /
    ``st:gslot`` 单调累积，模型全局闸门在若干次任务后永久卡死。

    ``private_patch`` 合并进 ``tasks.private_data``（new-api 原生列），
    目前只用来写 ``result_url``——让宿主看板的 ``GetResultURL()`` 能读到
    我们的主产出，而不是回落到 ``fail_reason``（那是它的 legacy 兼容分支）。
    """
    try:
        won = await taskstore.cas(
            task_id,
            ACTIVE,
            status,
            patch=patch,
            fail_reason=fail_reason,
            private_patch=private_patch,
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

    await slots.release_for_task(data)
    await tokensession.clear(task_id)

    if callback_url:
        from app.queue import publish_notify

        try:
            await publish_notify(task_id)
        except Exception:
            log.opt(exception=True).warning("notify enqueue failed: task_id={}", task_id)


async def run(task_id: str) -> dict[str, Any]:
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

    data: dict[str, Any] = task.get("data") or {}
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
            data,
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
    task_id: str, data: dict[str, Any], raw_token: str, token_hash: str, callback_url: str
) -> Outcome:
    base_url = str(data.get("upstream_base_url") or settings.upstream_base_url)
    method = str(data.get("request_method") or "POST").upper()
    path = str(data.get("request_path") or "/")
    query = str(data.get("request_query") or "")
    headers = dict(data.get("request_headers") or {})
    stored_body = str(data.get("request_body") or "")
    body_encoding = str(data.get("request_body_encoding") or "")

    model = str(data.get("model") or "")

    try:
        body = codec.decode(stored_body, body_encoding) if stored_body else b""
    except ValueError as exc:
        await _finalize(
            task_id,
            data,
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

    # 发请求前落一条完整请求摘要：排障最先要回答的是「我们到底发了什么」。
    # 只在重试循环**外**记一次——重试发的是同一份内容，逐次重复是纯噪音。
    # ``_log_headers`` 会把上面刚注入的 Authorization 摘掉（AC-30 红线）。
    _blog(
        task_id, phase="upstream_call", method=method, model=model,
        request_path=path, request_bytes=len(body),
    ).info(
        "upstream call: task_id={} method={} url={} model={} bytes={} headers={} body={}",
        task_id, method, url, model, len(body), _log_headers(headers), _digest(body),
    )

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
                data,
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
                data,
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
    data: dict[str, Any],
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
            data,
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
        "response_bytes": len(raw),
    }
    # 落库形态：小体明文（SQL 直接可读），超阈值或二进制才 gzip+base64。
    # 编码标记与体同写，读侧不做嗅探（见 app.services.codec）。
    stored, encoding = codec.encode(raw, config.plain_max_bytes)
    patch["upstream_response"] = stored
    patch["upstream_response_encoding"] = encoding

    if 200 <= status < 300:
        # 制品解析与成功落库同一次写入：看板「制品」列读 data.artifacts，
        # 分两次写会出现「已成功但制品暂缺」的中间态。解析恒不抛（内部兜底），
        # 空清单是合法结果（纯文本任务本无制品），绝不因此改判失败。
        #
        # 走 ``parse_for_store``（而不是在这里 parse_result + primary_url 各调
        # 一次）有两个理由：一是同一响应只解析一遍；二是**避免出现两份实现**。
        # 此前这里内联了同样的四个字段，而 ``parse_for_store`` 成了没人调用的
        # 死代码却仍被测试覆盖——改了其中一份、另一份静默漂移，
        # 而测试只盯着那份不跑的，正好把问题盖住。
        parsed = artifacts.parse_for_store(raw)
        found = parsed["artifacts"]
        primary = str(parsed["result_url"])
        patch["artifacts"] = found
        patch["artifact_count"] = parsed["artifact_count"]
        # 命中级别（known/walk/inline/none）：线上「成功却无制品」时，
        # 这一个字段即可区分「三级全空」与「首级误命中」，免回捞原始响应
        patch["artifact_parser"] = parsed["artifact_parser"]
        # ``private_data.result_url`` 是 new-api 原生列的契约字段：宿主的
        # ``Task.GetResultURL()`` 先读它，为空才回落 ``fail_reason``
        # （历史兼容分支）。不写它 = 宿主看板的「结果」列对我们的行恒空。
        # ``data.result_url`` 同时保留：本服务自己的看板与 ops 视图读它，
        # 走的是轻量投影（``data ->> '$.result_url'``），不必再碰 private_data。
        # 空值不落库（保持与改造前一致：无主产出时不写该键，而不是写空串）。
        private_patch: dict[str, Any] = {}
        if primary:
            patch["result_url"] = primary
            private_patch["result_url"] = primary
        await _finalize(
            task_id, data, SUCCESS, patch=patch,
            callback_url=callback_url,
            private_patch=private_patch or None,
        )
        # 签名 URL 原样进日志（内部审计口径）：制品地址多是短期签名，
        # 回 DB 还得解 gzip+b64，日志里直接可点更省事。
        artifact_urls = [str(a.get("url") or "") for a in found[:_OUTCOME_URL_LIMIT]]
        _blog(
            task_id,
            upstream_status=status,
            response_bytes=len(raw),
            model=model,
            request_path=path,
            artifact_count=parsed["artifact_count"],
            artifact_types=",".join(sorted({str(a.get("type")) for a in found})),
            artifact_parser=parsed["artifact_parser"],
            response_encoding=encoding,
            result_url=primary,
        ).info(
            "task success: task_id={} status={} bytes={} model={} path={} artifacts={} "
            "parser={} result_url={} artifact_urls={} response={}",
            task_id,
            status,
            len(raw),
            model,
            path,
            len(found),
            parsed["artifact_parser"],
            primary,
            artifact_urls,
            _digest(raw),
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
            response_encoding=encoding,
            artifact_count=parsed["artifact_count"],
            artifact_parser=parsed["artifact_parser"],
            # ``found`` 现在是 ``to_dict()`` 之后的形态，取值一律走 ``.get``：
            # ``to_dict`` 只在字段非空时才写键（``url`` / ``mime_type`` 都可能缺），
            # 直接下标会在「内联制品无 URL」时 KeyError。
            artifact_types=sorted({str(a.get("type")) for a in found}),
            artifact_urls=artifact_urls,
            result_url=primary,
            detail=(
                f"upstream {status}, {len(raw)} bytes, {len(found)} artifact(s) "
                f"via tier={parsed['artifact_parser']}"
            ),
        )

    # 失败原因必须带上游的**具体错误消息**——只写 "upstream 400" 时，
    # 看板/回调/查询三处都只能说"上游拒了"，真实原因（模型不存在、
    # 参数非法、内容审核、无可用渠道）全躺在 upstream_response 里等人解码。
    detail = upstream_error_detail(raw, content_type)
    fail_reason = f"upstream {status}: {detail}" if detail else f"upstream {status}"
    await _finalize(
        task_id,
        data,
        FAILURE,
        patch=patch,
        fail_reason=fail_reason,
        callback_url=callback_url,
    )
    # 日志预览走 ``_digest``（保留 JSON 结构，上限 4KB，终端直接可读）；
    # ``Outcome.upstream_preview`` 仍是 ``preview`` 的 300 字符——那份要进
    # taskiq result backend，与日志是**体积预算不同的两个用途**，别合并。
    response_preview = _digest(raw)
    _blog(
        task_id,
        upstream_status=status,
        fail_reason=fail_reason,
        model=model,
        request_path=path,
        content_type=content_type,
        response_bytes=len(raw),
        response_encoding=encoding,
        response_preview=response_preview,
    ).warning(
        "task failure: task_id={} status={} reason={} model={} path={} ct={} bytes={} preview={}",
        task_id,
        status,
        fail_reason,
        model,
        path,
        content_type,
        len(raw),
        response_preview,
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
        response_encoding=encoding,
        upstream_preview=preview(raw),
        detail=(
            f"upstream returned {status}: {detail}" if detail
            else f"upstream returned {status}; full body stored for replay"
        ),
    )
