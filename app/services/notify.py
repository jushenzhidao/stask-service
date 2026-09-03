"""终态回调推送（AC-29）。

签名：``X-Stask-Signature: sha256=<hex>``，内容为
``HMAC-SHA256(ST_CALLBACK_SECRET, f"{timestamp}.{body}")``，
配 ``X-Stask-Timestamp`` 头。接收方按同样规则重算比对，并校验时间戳
在合理窗口内（防重放）。

回调体**不含结果原文**——出图响应可能好几 MB，推给客户端的 webhook
端点既慢又容易被拒。只给状态与 task_id，客户端自己来 GET 取。

重试：指数退避（2^n 秒，上限 300s），最多 ``ST_CALLBACK_MAX_ATTEMPTS`` 次。
回调 URL 的 host 受 ``ST_CALLBACK_ALLOWLIST`` 约束（空 = 不限制，但生产
强烈建议配——否则回调就是一个 SSRF 出口）。
"""

from __future__ import annotations

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


async def deliver(task_id: str, attempt: int = 1) -> None:
    """推送一次；失败按退避重投（taskiq 任务体调用）。"""
    # 元数据投影：回调体不含结果原文，没有任何理由把 10MB 的
    # upstream_response 拉进 worker 内存
    task = await taskstore.get_meta(task_id)
    if task is None:
        return
    data: dict = task.get("data") or {}
    url = str(data.get("callback_url") or "")
    if not url:
        return
    if not _url_allowed(url):
        log.warning("callback url rejected by allowlist: task_id={}", task_id)
        return

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

    client = httpc.shared_client(timeout=settings.callback_timeout)
    try:
        resp = await client.post(url, content=body, headers=headers)
        if 200 <= resp.status_code < 300:
            await taskstore.patch_data(task_id, {"callback_delivered": True})
            log.info("callback delivered: task_id={} attempt={}", task_id, attempt)
            return
        log.warning("callback rejected: task_id={} status={} attempt={}",
                    task_id, resp.status_code, attempt)
    except httpx.HTTPError as exc:
        log.warning("callback transport error: task_id={} err={} attempt={}",
                    task_id, type(exc).__name__, attempt)

    if attempt >= settings.callback_max_attempts:
        await taskstore.patch_data(task_id, {"callback_delivered": False,
                                             "callback_attempts": attempt})
        log.error("callback exhausted: task_id={} attempts={}", task_id, attempt)
        return

    from app.queue import publish_notify

    delay = min(2 ** attempt, 300)
    await publish_notify(task_id, attempt + 1, delay_seconds=delay)
