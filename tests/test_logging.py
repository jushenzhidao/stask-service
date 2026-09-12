"""日志口径门禁：级别地板、双 sink 同源、请求/响应摘要截断、凭证头过滤。

对应 2026-09-12 的日志策略（内部审计口径）：

1. ``LOG_LEVEL`` 默认 DEBUG = 全量，本地终端与 logfire **同源取级**；
2. WARNING 是硬地板，应急降噪压不下去（AC 之外的运维要求，但同样要机械守）；
3. 请求/响应体进日志但**按长度截断**（内联 b64 只留 MIME + 长度），不脱敏；
4. 唯一豁免：凭证头（AC-30 红线，用户 sk 不进日志）。

这些断言存在的理由与项目其他门禁一致：级别地板、截断上限这类契约一旦被
改回去，**功能上完全正常**（日志照打），没有任何功能性用例会变红。
"""

from __future__ import annotations

import json
from pathlib import Path

from loguru import logger

from app.config import settings
from app.logging import _effective_level, setup_logging
from app.services.logdigest import _digest, _log_headers

ROOT = Path(__file__).resolve().parents[1]

DEBUG, WARNING, TRACE, CRITICAL = 10, 30, 5, 50


def _sink_levels() -> set[int]:
    """所有 loguru sink 的数值级别。

    loguru 没有公开的 sink 枚举 API，只能读内部表；形状一旦变化这里会
    直接报错（而不是静默返回空集合把断言放过去——空集合会让 ``== {10}``
    失败，方向是安全的）。
    """
    return {handler.levelno for handler in logger._core.handlers.values()}


# ---------------------------------------------------------------------------
# 级别口径
# ---------------------------------------------------------------------------


def test_default_level_is_full_debug(monkeypatch):
    """默认全量：DEBUG 起收（"debug全部落，本地+远程" 的代码侧事实源）。"""
    monkeypatch.setattr(settings, "log_level", "DEBUG")
    assert logger.level(_effective_level()).no == DEBUG


def test_warning_is_a_hard_floor(monkeypatch):
    """调到 ERROR/CRITICAL 也只能降到 WARNING —— 告警不许被降噪吃掉。"""
    for configured in ("ERROR", "CRITICAL"):
        monkeypatch.setattr(settings, "log_level", configured)
        assert logger.level(_effective_level()).no == WARNING, configured


def test_more_verbose_setting_passes_through(monkeypatch):
    """比地板更啰嗦的档位（TRACE）不被地板限制——地板只托底，不设上限。"""
    monkeypatch.setattr(settings, "log_level", "TRACE")
    assert logger.level(_effective_level()).no == TRACE


def test_typo_falls_back_to_floor(monkeypatch):
    """env 手敲错级别不能让装配抛错起不来（与 gunicorn 的错值回落同哲学）。"""
    monkeypatch.setattr(settings, "log_level", "vrebose")
    assert logger.level(_effective_level()).no == WARNING


def test_setup_logging_wires_the_same_level_into_every_sink(monkeypatch):
    """**装配层**门禁：只测 ``_effective_level()`` 不足以证明 sink 真用了它。

    历史缺陷正是「两侧不一致」：logfire sink 没显式给 level，loguru 的
    ``add()`` 默认 level=0 全量放行，而 stderr 被 ``LOG_LEVEL`` 挡着——
    同一行日志两个地方不一样。
    """
    try:
        monkeypatch.setattr(settings, "log_level", "DEBUG")
        setup_logging()
        assert _sink_levels() == {DEBUG}

        monkeypatch.setattr(settings, "log_level", "CRITICAL")
        setup_logging()
        assert _sink_levels() == {WARNING}
    finally:
        setup_logging()          # logger.remove() 是全局状态，跑完复原


# ---------------------------------------------------------------------------
# 请求/响应摘要（截断但不脱敏）
# ---------------------------------------------------------------------------


def test_digest_keeps_json_structure():
    """JSON 体保留结构：日志里能按字段读，而不是一坨字符串。"""
    assert json.loads(_digest(b'{"model":"seedance","n":3}')) == {
        "model": "seedance", "n": 3,
    }


def test_digest_truncates_long_strings_with_real_length():
    """长字符串截断后必须标注**原始长度**，否则「截断了多少」无从判断。"""
    raw = json.dumps({"prompt": "x" * 5000}).encode()
    out = _digest(raw)
    assert "5000 chars total" in out
    assert len(out) < 1000


def test_digest_turns_inline_base64_into_a_marker():
    """内联 b64（i2v 的图）只留 MIME 与长度——保留 500 字符前缀毫无信息量。"""
    uri = "data:image/png;base64," + "A" * 40000
    out = _digest(json.dumps({"image": uri}).encode())
    assert f"<inline data:image/png;base64 ~{len(uri)} chars>" in out
    assert "AAAA" not in out


def test_digest_skips_parsing_oversized_bodies():
    """超大体重不解析（不为日志白花 CPU/峰值内存），但保留头部与总字节数。"""
    raw = b'{"huge":"' + b"A" * (300 * 1024) + b'"}'
    out = _digest(raw)
    assert "not parsed" in out
    assert str(len(raw)) in out


def test_digest_never_raises_on_non_json():
    """HTML 错误页 / 二进制 / 空体：容错解码，恒不抛（摘要只服务观测）。"""
    assert "502 Bad Gateway" in _digest(b"<html><body>502 Bad Gateway</body></html>")
    assert _digest(b"\xff\xfe\x00\x01")
    assert _digest(b"") == ""


# ---------------------------------------------------------------------------
# 凭证头（AC-30 红线）
# ---------------------------------------------------------------------------


def test_log_headers_drops_credentials_and_keeps_the_rest():
    """只摘凭证头名：其余（含自定义头、content-type）原样保留 = 「不脱敏」。"""
    got = _log_headers({
        "Authorization": "Bearer sk-secret",
        "cookie": "session=abc",
        "x-api-key": "k",
        "content-type": "application/json",
        "x-request-id": "r1",
    })
    assert got == {"content-type": "application/json", "x-request-id": "r1"}


def test_dispatch_log_passes_headers_through_the_filter():
    """源头门禁：dispatch 的请求日志必须经 ``_log_headers``。

    那一行记的是**注入 Authorization 之后**的 headers，忘记过滤就是
    把用户 sk 直接写进日志（AC-30）。这是本仓库唯一一处这样的调用点，
    改回裸 ``headers`` 不会有任何功能性用例变红，故用源码扫描守住。
    """
    source = (ROOT / "app" / "services" / "execute.py").read_text("utf-8")
    assert "_log_headers(headers)" in source
