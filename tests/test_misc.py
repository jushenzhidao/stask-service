"""其余单元：时间归一、codec 往返、回调签名、健康检查、ops、mypy 门禁。

对应 AC-29 / AC-30 / AC-31。
"""

from __future__ import annotations

import hashlib
import hmac
import json
import subprocess
import sys
from pathlib import Path

import httpx
import pytest

from app.services import codec, notify, taskstore
from tests.conftest import AUTH

ROOT = Path(__file__).resolve().parents[1]


# ---------------------------------------------------------------------------
# 时间归一（AC-31）
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("value,expected", [
    (1755000000, 1755000000),               # 秒，原样
    (1755000000123, 1755000000),            # 毫秒 → 秒
    ("1755000000", 1755000000),
    (None, 0),
    ("", 0),
    ("bogus", 0),
    (0, 0),
])
def test_as_unix_seconds(value, expected):
    """tasks 表被 new-api 原生模块用 UnixMilli 写过，读侧必须归一。"""
    assert taskstore.as_unix_seconds(value) == expected


def test_secs_sql_expression_covers_both_units():
    """SQL 侧归一：读侧 Python 归一救不了 `col < :cutoff` 这种比较。"""
    expr = taskstore._secs("created_at")
    assert "created_at > 100000000000" in expr
    assert "created_at DIV 1000" in expr


# ---------------------------------------------------------------------------
# codec
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("payload", [
    b"",
    b"{}",
    b'{"data":[{"url":"https://cdn/a.png"}]}',
    bytes(range(256)) * 100,                # 二进制（音频/图片）
    "中文内容测试".encode(),
])
def test_codec_roundtrip(payload):
    assert codec.decode(codec.encode(payload)) == payload


def test_codec_compresses_json():
    """JSON 型响应压缩率应显著——b64 会 ×4/3，压不动就是净亏。"""
    payload = json.dumps({"data": [{"url": "https://cdn/x.png"} for _ in range(200)]}
                         ).encode()
    assert len(codec.encode(payload)) < len(payload) * 0.5


def test_codec_rejects_garbage():
    with pytest.raises(ValueError):
        codec.decode("!!!not-base64!!!")


# ---------------------------------------------------------------------------
# 回调签名（AC-29）
# ---------------------------------------------------------------------------


def test_signature_is_verifiable(test_settings):
    body = b'{"task_id":"x","status":"SUCCESS"}'
    ts = 1755000000
    got = notify.sign(body, ts)

    expected = hmac.new(b"test-secret", f"{ts}.".encode() + body,
                        hashlib.sha256).hexdigest()
    assert got == f"sha256={expected}"


def test_signature_omitted_without_secret(monkeypatch, test_settings):
    monkeypatch.setattr(test_settings, "callback_secret", "")
    assert notify.sign(b"x", 1) == ""


def test_signature_binds_timestamp(test_settings):
    """时间戳参与签名 → 重放攻击可被接收方按窗口拒绝。"""
    assert notify.sign(b"x", 1) != notify.sign(b"x", 2)


@pytest.mark.parametrize("url,allowed", [
    ("http://cb.example/hook", True),
    ("https://cb.example/hook", True),
    ("ftp://cb.example/hook", False),
    ("http://user:pw@cb.example/hook", False),
    ("not-a-url", False),
])
def test_callback_url_validation(test_settings, url, allowed):
    assert notify._url_allowed(url) is allowed


def test_callback_allowlist_enforced(monkeypatch, test_settings):
    """空 allowlist = 不限制，但那样回调就是 SSRF 出口，生产必须配。"""
    monkeypatch.setattr(test_settings, "callback_allowlist", ("cb.example",))
    assert notify._url_allowed("http://cb.example/hook") is True
    assert notify._url_allowed("http://169.254.169.254/latest/meta-data") is False


async def test_callback_delivery_signs_request(task_store, patch_redis,
                                               test_settings, respx_router):
    task_id = "img_" + "d" * 32
    await task_store.create(task_id, "/x", {
        "callback_url": "http://cb.example/hook", "upstream_status": 200,
    })
    task_store.rows[task_id]["status"] = "SUCCESS"
    route = respx_router.post("http://cb.example/hook").mock(
        return_value=httpx.Response(200)
    )

    await notify.deliver(task_id)

    req = route.calls[0].request
    assert req.headers["x-stask-signature"].startswith("sha256=")
    assert req.headers["x-stask-timestamp"].isdigit()
    payload = json.loads(req.content)
    assert payload["task_id"] == task_id
    assert payload["status"] == "SUCCESS"
    # 回调体绝不含结果原文（可能好几 MB）
    assert "upstream_response" not in payload
    assert task_store.rows[task_id]["data"]["callback_delivered"] is True


async def test_callback_retries_with_backoff(task_store, patch_redis, test_settings,
                                             respx_router, queue_events):
    task_id = "img_" + "e" * 32
    await task_store.create(task_id, "/x", {"callback_url": "http://cb.example/h"})
    task_store.rows[task_id]["status"] = "FAILURE"
    respx_router.post("http://cb.example/h").mock(return_value=httpx.Response(500))

    await notify.deliver(task_id, attempt=1)

    assert queue_events.notify == [(task_id, 2, 2)]     # 指数退避 2^1


async def test_callback_exhausts_and_records(task_store, patch_redis, monkeypatch,
                                             test_settings, respx_router,
                                             queue_events):
    monkeypatch.setattr(test_settings, "callback_max_attempts", 2)
    task_id = "img_" + "f" * 32
    await task_store.create(task_id, "/x", {"callback_url": "http://cb.example/h"})
    task_store.rows[task_id]["status"] = "FAILURE"
    respx_router.post("http://cb.example/h").mock(return_value=httpx.Response(500))

    await notify.deliver(task_id, attempt=2)

    assert queue_events.notify == []
    assert task_store.rows[task_id]["data"]["callback_delivered"] is False


# ---------------------------------------------------------------------------
# 健康检查与 ops
# ---------------------------------------------------------------------------


def test_healthz_live_has_no_dependencies(client):
    """liveness 必须零依赖——DB 宕机时重启进程治不了病。"""
    resp = client.get("/healthz/live")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}


async def test_ops_stats(client, task_store):
    await task_store.create("a_" + "1" * 32, "/x", {"token_hash": "th"})
    resp = client.get("/ops/stats", headers=AUTH)
    assert resp.status_code == 200
    assert resp.json()["status_counts"]["NOT_START"] == 1


async def test_ops_task_detail_never_leaks_sk_or_body(client, task_store,
                                                      patch_redis):
    """AC-30：诊断端点只给存在性与 TTL，绝不返回令牌本体或结果原文。"""
    from app.services import tokensession

    task_id = "img_" + "9" * 32
    await task_store.create(task_id, "/x", {
        "token_hash": "th", "model": "dall-e-3",
        "request_body": codec.encode(b'{"prompt":"secret prompt"}'),
        "upstream_response": codec.encode(b'{"url":"x"}'),
        "response_bytes": 11,
    })
    await tokensession.store(task_id, "sk-test-token")

    resp = client.get(f"/ops/tasks/{task_id}", headers=AUTH)
    assert resp.status_code == 200
    body = resp.text
    assert "sk-test-token" not in body
    assert "secret prompt" not in body
    assert resp.json()["token_session"]["exists"] is True
    assert resp.json()["response_bytes"] == 11


def test_ops_requires_auth(client):
    assert client.get("/ops/stats").status_code == 401


# ---------------------------------------------------------------------------
# 静态检查门禁
# ---------------------------------------------------------------------------


def test_mypy_clean():
    """把类型检查做成一个测试用例——CI 里跑 pytest 就等于跑了 mypy。"""
    proc = subprocess.run(
        [sys.executable, "-m", "mypy", "app/"],
        cwd=ROOT, capture_output=True, text=True, timeout=300,
    )
    assert proc.returncode == 0, proc.stdout + proc.stderr


def test_ruff_clean():
    proc = subprocess.run(
        [sys.executable, "-m", "ruff", "check", "app", "tests"],
        cwd=ROOT, capture_output=True, text=True, timeout=120,
    )
    assert proc.returncode == 0, proc.stdout + proc.stderr


def test_no_emoji_in_source():
    """团队 P0 规则：源码与文档中不得出现 emoji 作为功能标识。"""
    import re

    pattern = re.compile(
        "[\U0001F300-\U0001F9FF\u2600-\u26FF\u2700-\u27BF"
        "\U0001FA00-\U0001FAFF\U0001F000-\U0001F0FF]"
    )
    offenders = []
    for path in list(ROOT.glob("app/**/*.py")) + list(ROOT.glob("tests/**/*.py")) \
            + list(ROOT.glob("docs/**/*.md")) + [ROOT / ".env.example"]:
        text = path.read_text(encoding="utf-8")
        if pattern.search(text):
            offenders.append(str(path.relative_to(ROOT)))
    assert not offenders, f"emoji found in: {offenders}"
