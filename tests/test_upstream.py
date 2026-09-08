"""鉴权 + 余额预检（services/upstream.py，共享库单 SQL 直查）。

外部边界是 DB（``_fetch_credential``），单测里 monkeypatch 成内存行——
判定逻辑（``_validate``）与缓存链路是被测对象，真实 SQL 由集成环境验证。
"""

from __future__ import annotations

import pytest

from app.config import settings
from app.services import upstream
from tests.conftest import AUTH

TH = "th-test"


def _row(**over) -> dict:
    """一条健康的 tokens ⋈ users 凭证行。"""
    base = {
        "user_id": 42, "status": 1, "expired_time": -1, "remain_quota": 100,
        "unlimited_quota": False, "user_status": 1, "user_quota": 5000,
    }
    return {**base, **over}


@pytest.fixture(autouse=True)
def _newapi_mode(monkeypatch, patch_redis):
    """本文件的判定用例全测 newapi 路径；generic 是默认模式，另有用例覆盖。"""
    monkeypatch.setattr(settings, "auth_mode", "newapi")
    upstream.clear_cache()
    yield
    upstream.clear_cache()


@pytest.fixture
def credential(monkeypatch):
    """DB 替身：holder.row = None 模拟 key 不存在；holder.error 模拟库挂。"""
    class _Holder:
        row: dict | None = _row()
        error: Exception | None = None
        calls: int = 0

    holder = _Holder()

    async def fake_fetch(_raw: str) -> dict | None:
        holder.calls += 1
        if holder.error is not None:
            raise holder.error
        return holder.row

    monkeypatch.setattr(upstream, "_fetch_credential", fake_fetch)
    return holder


async def test_valid_key_returns_user_id(credential):
    info = await upstream.authenticate("sk-x", TH)
    assert info.user_id == 42


async def test_unknown_key_401(credential):
    credential.row = None
    with pytest.raises(upstream.UpstreamAuthError) as e:
        await upstream.authenticate("sk-bad", TH)
    assert e.value.status == 401


@pytest.mark.parametrize("over,status", [
    ({"status": 2}, 401),                     # token 禁用
    ({"status": 3}, 401),                     # token 过期标记
    ({"status": 4}, 402),                     # token 耗尽标记
    ({"expired_time": 1}, 401),               # 时间过期
    ({"user_status": 2}, 401),                # 用户禁用
    ({"remain_quota": 0}, 402),               # token 额度耗尽
    ({"user_quota": 0}, 402),                 # 用户余额耗尽
])
async def test_invalid_credential_rejected(credential, over, status):
    credential.row = _row(**over)
    with pytest.raises(upstream.UpstreamAuthError) as e:
        await upstream.authenticate("sk-x", TH)
    assert e.value.status == status


async def test_unlimited_quota_skips_token_quota(credential):
    credential.row = _row(remain_quota=0, unlimited_quota=True)
    info = await upstream.authenticate("sk-x", TH)
    assert info.user_id == 42


async def test_db_down_502(credential):
    credential.error = RuntimeError("db down")
    with pytest.raises(upstream.UpstreamAuthError) as e:
        await upstream.authenticate("sk-x", TH)
    assert e.value.status == 502


async def test_positive_result_cached_in_redis(credential, patch_redis):
    """正向结果进 Redis（跨进程持久）；本地缓存清掉后仍不打 DB。"""
    await upstream.authenticate("sk-x", TH)
    assert credential.calls == 1
    assert f"st:auth:{TH}" in patch_redis.dump()

    upstream.clear_cache()                    # 模拟另一个进程/重启
    info = await upstream.authenticate("sk-x", TH)
    assert info.user_id == 42
    assert credential.calls == 1              # 没打第二次 DB


async def test_negative_result_not_cached(credential, patch_redis):
    """401 不缓存：key 修复后立即生效。"""
    credential.row = None
    with pytest.raises(upstream.UpstreamAuthError):
        await upstream.authenticate("sk-x", TH)
    assert f"st:auth:{TH}" not in patch_redis.dump()

    credential.row = _row()
    upstream.clear_cache()
    info = await upstream.authenticate("sk-x", TH)
    assert info.user_id == 42


async def test_redis_value_never_contains_key(credential, patch_redis):
    """红线：缓存值只有 user_id，绝不含 key/token 任何形态。"""
    await upstream.authenticate("sk-secret-token-abc", TH)
    cached = patch_redis.dump()[f"st:auth:{TH}"]
    assert "secret" not in cached and "sk-" not in cached


def test_token_key_parsing():
    """对齐 new-api TokenAuth：去 sk- 前缀、按 '-' 取首段。"""
    assert upstream._token_key("sk-abc123") == "abc123"
    assert upstream._token_key("sk-abc123-extra") == "abc123"
    assert upstream._token_key("plain") == "plain"


# ---------------------------------------------------------------------------
# generic 模式（默认）：零动作放行
# ---------------------------------------------------------------------------


async def test_generic_mode_skips_db_entirely(credential, monkeypatch):
    """默认模式不查凭证、不报错、user_id 落 0——通用上游没有凭证事实源。"""
    monkeypatch.setattr(settings, "auth_mode", "generic")
    credential.error = RuntimeError("db must not be touched")

    info = await upstream.authenticate("sk-x", TH)

    assert info.user_id == 0
    assert credential.calls == 0


async def test_generic_mode_accepts_key_newapi_would_reject(credential, monkeypatch):
    """generic 下即使凭证行是禁用态也放行——有效性交给上游执行时判定。"""
    monkeypatch.setattr(settings, "auth_mode", "generic")
    credential.row = _row(status=2, user_quota=0)

    assert (await upstream.authenticate("sk-x", TH)).user_id == 0


# ---------------------------------------------------------------------------
# 提交链路端到端（经 stub）
# ---------------------------------------------------------------------------

PATH = "/async/v1/images/generations"
BODY = {"model": "dall-e-3", "prompt": "x"}


def test_submit_rejected_on_invalid_key(client, upstream_auth, task_store):
    upstream_auth.fail = upstream.UpstreamAuthError(401, "invalid key")
    resp = client.post(PATH, json=BODY, headers=AUTH)
    assert resp.status_code == 401
    assert task_store.rows == {}


def test_submit_rejected_on_no_balance(client, upstream_auth, task_store):
    upstream_auth.fail = upstream.UpstreamAuthError(402, "insufficient balance")
    resp = client.post(PATH, json=BODY, headers=AUTH)
    assert resp.status_code == 402
    assert task_store.rows == {}


def test_user_id_lands_in_task_row(client, task_store):
    """鉴权拿到的 user_id 必须落进 tasks.user_id（stub 固定 42）。"""
    tid = client.post(PATH, json=BODY, headers=AUTH).json()["task_id"]
    assert task_store.rows[tid]["user_id"] == 42
