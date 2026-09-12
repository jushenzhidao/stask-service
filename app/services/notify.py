"""终态回调推送（AC-29）。

签名：``X-Stask-Signature: sha256=<hex>``，内容为
``HMAC-SHA256(CALLBACK_SECRET, f"{timestamp}.{body}")``，
配 ``X-Stask-Timestamp`` 头。接收方按同样规则重算比对，并校验时间戳
在合理窗口内（防重放）。

回调体**不含结果原文**——出图响应可能好几 MB，推给客户端的 webhook
端点既慢又容易被拒。只给状态与 task_id，客户端自己来 GET 取。

重试：指数退避（2^n 秒，上限 300s），最多 ``CALLBACK_MAX_ATTEMPTS`` 次。
回调 URL 的 host 受 ``CALLBACK_ALLOWLIST`` 约束（空 = 不限制，但生产
强烈建议配——否则回调就是一个 SSRF 出口）。
"""

from __future__ import annotations

from typing import Any

import hashlib
import hmac
import json
import time
from urllib.parse import urlsplit

import httpx

from app.config import settings
from app.logging import log
from app.services import httpc, taskstore

_SIGNATURE_HEADER = "X-Stask-Signature"
_TIMESTAMP_HEADER = "X-Stask-Timestamp"

#: 摘要里回显的对端响应/错误消息截断长度——摘要要进 result backend，
#: 对端可能回一整页 HTML 错误页，不设限会把 Redis 撑成日志存储。
_EXCERPT_LIMIT = 500


def sign(body: bytes, timestamp: int) -> str:
    """``sha256=<hex>``。密钥未配置时返回空串（不发签名头）。"""
    if not settings.callback_secret:
        return ""
    mac = hmac.new(
        settings.callback_secret.encode("utf-8"),
        f"{timestamp}.".encode() + body,
        hashlib.sha256,
    )
    return f"sha256={mac.hexdigest()}"


def _url_allowed(url: str) -> bool:
    parts = urlsplit(url)
    if parts.scheme not in ("http", "https") or not parts.hostname:
        return False
    if parts.username or parts.password:
        return False
    allow = settings.callback_allowlist
    if not allow:
        return True
    host = parts.netloc.lower()
    hostname = parts.hostname.lower()
    return any(
        (host == item.strip().lower() if ":" in item else hostname == item.strip().lower())
        for item in allow if item.strip()
    )


async def deliver(task_id: str, attempt: int = 1) -> dict[str, Any]:
    """推送一次；失败按退避重投（taskiq 任务体调用）。

    返回本次投递摘要，供 taskiq-admin 的 ``Return Value`` 直接排障：
    ``delivered`` / ``skipped``（无回调地址、任务行已消失、地址不在白名单）
    / ``rejected``（HTTP 非 2xx）/ ``transport_error`` / ``exhausted``。
    与 execute 一致：本函数**从不抛异常**，失败靠返回值表达。
    """
    # 元数据投影：回调体不含结果原文，没有任何理由把 10MB 的
    # upstream_response 拉进 worker 内存
    task = await taskstore.get_meta(task_id)
    if task is None:
        return {"ok": False, "task_id": task_id, "attempt": attempt,
                "result": "skipped", "reason": "task_row_missing"}
    data: dict[str, Any] = task.get("data") or {}
    url = str(data.get("callback_url") or "")
    if not url:
        return {"ok": True, "task_id": task_id, "attempt": attempt,
                "result": "skipped", "reason": "no_callback_url"}
    if not _url_allowed(url):
        log.warning("callback url rejected by allowlist: task_id={}", task_id)
        return {"ok": False, "task_id": task_id, "attempt": attempt,
                "result": "skipped", "reason": "url_not_allowlisted",
                "callback_host": urlsplit(url).netloc}

    payload = {
        "task_id": task_id,
        "status": task["status"],
        "created_at": task.get("created_at", 0),
        "finish_time": task.get("finish_time", 0),
        "upstream_status": int(data.get("upstream_status") or 0),
        "fail_reason": str(task.get("fail_reason") or ""),
    }
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    ts = int(time.time())
    headers = {"Content-Type": "application/json", _TIMESTAMP_HEADER: str(ts)}
    signature = sign(body, ts)
    if signature:
        headers[_SIGNATURE_HEADER] = signature

    # callback_timeout 是只读 env（不进 dynconf）→ 可安全作为构造参数
    client = httpc.shared_client("callback", timeout=settings.callback_timeout)
    summary: dict[str, Any] = {"task_id": task_id, "attempt": attempt,
                     "task_status": task["status"],
                     "callback_host": urlsplit(url).netloc}
    try:
        resp = await client.post(url, content=body, headers=headers)
        if 200 <= resp.status_code < 300:
            await taskstore.patch_data(task_id, {"callback_delivered": True})
            log.info("callback delivered: task_id={} attempt={}", task_id, attempt)
            return {**summary, "ok": True, "result": "delivered",
                    "http_status": resp.status_code}
        log.warning("callback rejected: task_id={} status={} attempt={}",
                    task_id, resp.status_code, attempt)
        summary |= {"result": "rejected", "http_status": resp.status_code,
                    "response_excerpt": resp.text[:_EXCERPT_LIMIT]}
    except httpx.HTTPError as exc:
        log.warning("callback transport error: task_id={} err={} attempt={}",
                    task_id, type(exc).__name__, attempt)
        summary |= {"result": "transport_error", "error_type": type(exc).__name__,
                    "error_message": str(exc)[:_EXCERPT_LIMIT]}

    if attempt >= settings.callback_max_attempts:
        await taskstore.patch_data(task_id, {"callback_delivered": False,
                                             "callback_attempts": attempt})
        log.error("callback exhausted: task_id={} attempts={}", task_id, attempt)
        return {**summary, "ok": False, "result": "exhausted",
                "last_failure": summary["result"],
                "max_attempts": settings.callback_max_attempts}

    from app.queue import publish_notify

    delay = min(2 ** attempt, 300)
    await publish_notify(task_id, attempt + 1, delay_seconds=delay)
    return {**summary, "ok": False, "retry_in_seconds": delay,
            "next_attempt": attempt + 1}
