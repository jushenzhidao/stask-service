"""准入校验：路径白名单/黑名单、upstream 三防线、task_id 形态、头清洗。

对应 AC-02 / AC-03 / AC-10 / AC-11。
"""

from __future__ import annotations

import pytest

from app.services import admission
from app.services.admission import AdmissionError


# ---------------------------------------------------------------------------
# 路径准入
# ---------------------------------------------------------------------------


def test_allow_prefix_passes(test_settings):
    admission.check_path("/v1/images/generations")
    admission.check_path("/v1/audio/speech")


@pytest.mark.parametrize("path", ["/v1/chat/completions", "/", "/v2/images"])
def test_not_in_allowlist_rejected(test_settings, path):
    """AC-03：未命中白名单 → 403。"""
    with pytest.raises(AdmissionError) as ei:
        admission.check_path(path)
    assert ei.value.status == 403
    assert ei.value.code == "path_not_allowed"


@pytest.mark.parametrize("path", ["/api/user/self", "/console/token"])
def test_deny_prefix_rejected(test_settings, path):
    """AC-02：命中黑名单 → 403。"""
    with pytest.raises(AdmissionError) as ei:
        admission.check_path(path)
    assert ei.value.code == "path_denied"


def test_deny_wins_over_allow(monkeypatch, test_settings):
    """AC-02 关键点：allow 写宽了（配成 /）时 deny 仍必须挡住管理面。

    两者顺序颠倒 = 一条直达 /api/user/self 的路，这是配置失误的兜底。
    """
    monkeypatch.setattr(test_settings, "async_allow_prefixes", ("/",))
    admission.check_path("/v1/images/generations")          # allow 生效
    with pytest.raises(AdmissionError) as ei:
        admission.check_path("/api/user/self")              # deny 仍然赢
    assert ei.value.code == "path_denied"


def test_empty_allowlist_rejects_everything(monkeypatch, test_settings):
    """未配置白名单 = 关闭服务，而不是放开一切。"""
    monkeypatch.setattr(test_settings, "async_allow_prefixes", ())
    with pytest.raises(AdmissionError):
        admission.check_path("/v1/images/generations")


# ---------------------------------------------------------------------------
# upstream 三防线
# ---------------------------------------------------------------------------


def test_header_wins_over_config(test_settings):
    assert admission.resolve_upstream("http://127.0.0.1:3000") == "http://127.0.0.1:3000"


def test_fallback_to_config_when_header_absent(test_settings):
    assert admission.resolve_upstream(None) == "http://newapi:3000"
    assert admission.resolve_upstream("   ") == "http://newapi:3000"


def test_trailing_slash_normalized(test_settings):
    assert admission.resolve_upstream("http://newapi:3000/") == "http://newapi:3000"


@pytest.mark.parametrize("bad", [
    "http://evil.com",
    "http://newapi:3001",          # 端口不符 → allowlist 条目带端口时必须全等
    "http://sub.newapi:3000",
])
def test_host_not_in_allowlist_rejected(test_settings, bad):
    """AC-10：host 不在 allowlist → 400。"""
    with pytest.raises(AdmissionError) as ei:
        admission.resolve_upstream(bad)
    assert ei.value.status == 400
    assert ei.value.code == "upstream_not_allowed"


@pytest.mark.parametrize("bad,code", [
    ("ftp://newapi:3000", "upstream_invalid_scheme"),
    ("file:///etc/passwd", "upstream_invalid_scheme"),
    ("http://user:pass@newapi:3000", "upstream_userinfo"),
    ("http://newapi:3000?x=1", "upstream_invalid"),
])
def test_scheme_and_userinfo_rejected(test_settings, bad, code):
    """AC-11：非 http(s) / 含 userinfo / 带 query 一律 400。

    userinfo 是 SSRF 常用绕过手法：``http://newapi:3000@evil.com`` 的真实
    host 是 evil.com，但粗糙的字符串匹配会以为它是 newapi。
    """
    with pytest.raises(AdmissionError) as ei:
        admission.resolve_upstream(bad)
    assert ei.value.code == code


def test_allowlist_entry_without_port_matches_any_port(monkeypatch, test_settings):
    monkeypatch.setattr(test_settings, "upstream_allowlist", ("newapi",))
    assert admission.resolve_upstream("http://newapi:9999") == "http://newapi:9999"
    assert admission.resolve_upstream("http://newapi") == "http://newapi"


# ---------------------------------------------------------------------------
# 头清洗
# ---------------------------------------------------------------------------


def test_clean_headers_drops_credentials_and_hop_by_hop():
    cleaned = admission.clean_headers({
        "Authorization": "Bearer sk-secret",
        "Cookie": "session=1",
        "Host": "gw.example.com",
        "Content-Length": "42",
        "Idempotency-Key": "k1",
        "X-Callback-Url": "http://cb",
        "X-Upstream-Base-Url": "http://evil",
        "Content-Type": "application/json",
        "X-Custom": "keep-me",
    })
    assert "Authorization" not in cleaned
    assert "Cookie" not in cleaned
    assert "Host" not in cleaned
    assert "Idempotency-Key" not in cleaned
    assert "X-Upstream-Base-Url" not in cleaned
    # 业务头必须原样保留（设计要求「原文存储原样转发」）
    assert cleaned["Content-Type"] == "application/json"
    assert cleaned["X-Custom"] == "keep-me"


def test_clean_headers_is_case_insensitive():
    assert admission.clean_headers({"AUTHORIZATION": "x", "authorization": "y"}) == {}


# ---------------------------------------------------------------------------
# task_id 形态提取
# ---------------------------------------------------------------------------


def test_extract_task_id_hit():
    tid = "dall_e_3_" + "a" * 32
    assert admission.extract_task_id(f"/v1/images/generations/{tid}") == tid


@pytest.mark.parametrize("path", [
    "/v1/images/generations",       # 纯路径，末段不是 task_id
    "/v1/images/generations/",
    "/v1/images/xyz",
    "/v1/images/" + "a" * 32,       # 缺 slug 前缀
    "/v1/images/task_" + "Z" * 32,  # 非 hex
])
def test_extract_task_id_miss(path):
    """形态正则预筛：避免把普通路径末段当 task_id 去查库。"""
    assert admission.extract_task_id(path) is None
