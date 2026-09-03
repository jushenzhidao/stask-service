"""提交端点端到端（经 ASGI 客户端）。

对应 AC-01 / AC-04 / AC-05 / AC-06 / AC-08 / AC-09 / AC-10。
"""

from __future__ import annotations

import pytest

from tests.conftest import AUTH

PATH = "/async/v1/images/generations"
BODY = {"model": "dall-e-3", "prompt": "a red cube", "n": 1}


def test_submit_returns_202_with_location(client, task_store, queue_events):
    """AC-01：202 + task_id + Location 头，且已入队。"""
    resp = client.post(PATH, json=BODY, headers=AUTH)
    assert resp.status_code == 202

    payload = resp.json()
    task_id = payload["task_id"]
    assert payload["status"] == "SUBMITTED"
    assert task_id.startswith("dall_e_3_")
    assert resp.headers["location"] == f"{PATH}/{task_id}"

    row = task_store.rows[task_id]
    assert row["status"] == "SUBMITTED"
    assert row["user_id"] == 42
    assert queue_events.execute == [task_id]


def test_submitted_data_contract(client, task_store):
    """data JSON 契约（SPEC §6）——尤其是让 atask sweeper 跳过的两个字段。"""
    task_id = client.post(PATH, json=BODY, headers=AUTH).json()["task_id"]
    data = task_store.rows[task_id]["data"]

    assert data["source"] == "stask"
    assert data["model"] == "dall-e-3"
    assert data["request_method"] == "POST"
    assert data["request_path"] == "/v1/images/generations"
    assert data["upstream_base_url"] == "http://newapi:3000"
    assert data["inflight_slot"] is True
    # 关键：atask sweeper 扫「终态未结算」，这两个字段让它天然跳过本服务的行
    assert data["freeze_amount"] == 0
    assert data["settled"] is True


def test_sk_never_persisted(client, task_store):
    """AC-30 红线：用户 sk 绝不进 tasks 表。"""
    task_id = client.post(PATH, json=BODY, headers=AUTH).json()["task_id"]
    serialized = str(task_store.rows[task_id])
    assert "sk-test-token" not in serialized
    assert "Authorization" not in task_store.rows[task_id]["data"]["request_headers"]


def test_sk_stored_in_session_only(client, patch_redis):
    task_id = client.post(PATH, json=BODY, headers=AUTH).json()["task_id"]
    assert patch_redis.dump()[f"st:sk:{task_id}"] == "sk-test-token"


def test_missing_auth_401(client):
    assert client.post(PATH, json=BODY).status_code == 401
    assert client.post(PATH, json=BODY,
                       headers={"Authorization": "Basic xyz"}).status_code == 401


def test_invalid_token_401(client, fake_billing):
    fake_billing.valid = False
    resp = client.post(PATH, json=BODY, headers=AUTH)
    assert resp.status_code == 401
    assert resp.json()["error"]["type"] == "authentication_error"


def test_denied_path_403(client):
    """AC-02。"""
    resp = client.post("/async/api/user/self", json={}, headers=AUTH)
    assert resp.status_code == 403
    assert resp.json()["error"]["type"] == "permission_error"


def test_path_not_in_allowlist_403(client):
    """AC-03。"""
    assert client.post("/async/v1/chat/completions",
                       json=BODY, headers=AUTH).status_code == 403


@pytest.mark.parametrize("method", ["patch", "options"])
def test_method_not_allowed_405(client, method):
    """AC-04：仅 POST/PUT/GET/DELETE。"""
    assert getattr(client, method)(PATH, headers=AUTH).status_code == 405


def test_put_is_accepted(client, task_store):
    resp = client.put(PATH, json=BODY, headers=AUTH)
    assert resp.status_code == 202


def test_bad_upstream_header_400(client):
    """AC-10：allowlist 之外的 upstream → 400，且**在鉴权之前**就拒绝。"""
    resp = client.post(PATH, json=BODY,
                       headers={**AUTH, "X-Upstream-Base-Url": "http://evil.com"})
    assert resp.status_code == 400


def test_upstream_userinfo_400(client):
    """AC-11。"""
    resp = client.post(PATH, json=BODY,
                       headers={**AUTH, "X-Upstream-Base-Url": "http://a:b@newapi:3000"})
    assert resp.status_code == 400


def test_idempotent_replay(client, task_store, queue_events):
    """AC-05：同键第二次返回同一 task_id，不重建、不重复入队。"""
    headers = {**AUTH, "Idempotency-Key": "same-key"}
    first = client.post(PATH, json=BODY, headers=headers).json()
    second = client.post(PATH, json=BODY, headers=headers).json()

    assert first["task_id"] == second["task_id"]
    assert second["replayed"] is True
    assert len(task_store.rows) == 1
    assert len(queue_events.execute) == 1


def test_concurrent_same_key_409(client, patch_redis, task_store):
    """AC-06：占位存在但未回填（模拟真并发在飞）→ 409，绝不重建。"""
    from app.services import idem
    from app.services.identity import token_hash

    th = token_hash("sk-test-token")
    # 手工写一个 pending 占位，模拟另一个请求正在创建链路中
    patch_redis._data[f"st:idem:{th}:inflight"] = idem.PENDING

    resp = client.post(PATH, json=BODY,
                       headers={**AUTH, "Idempotency-Key": "inflight"})
    assert resp.status_code == 409
    assert task_store.rows == {}


def test_slot_exhausted_429_with_retry_after(client, monkeypatch, test_settings):
    """AC-08：余额只够 1 槽时，第二个并发提交返 429 + Retry-After。"""
    monkeypatch.setattr(test_settings, "ref_price_default", 10.0)   # balance 10 → 1 槽

    assert client.post(PATH, json=BODY, headers=AUTH).status_code == 202
    resp = client.post(PATH, json=BODY, headers=AUTH)
    assert resp.status_code == 429
    assert int(resp.headers["retry-after"]) >= 1
    assert resp.json()["error"]["type"] == "rate_limit_error"


def test_rate_limit_429(client, monkeypatch, test_settings):
    monkeypatch.setattr(test_settings, "rate_limit", 2)
    codes = [client.post(PATH, json=BODY, headers=AUTH).status_code for _ in range(4)]
    assert codes[:2] == [202, 202]
    assert 429 in codes[2:]


def test_body_too_large_413(client, monkeypatch, test_settings):
    monkeypatch.setattr(test_settings, "body_max_bytes", 64)
    resp = client.post(PATH, json={"model": "x", "prompt": "y" * 500}, headers=AUTH)
    assert resp.status_code == 413


def test_enqueue_failure_rolls_back(client, monkeypatch, patch_redis, task_store):
    """AC-09：入队失败必须归还槽，并把已落库的行判死。

    否则用户会被一次失败的提交永久扣掉一个槽，且状态分布里留一条
    永远不会被执行的僵尸 SUBMITTED。

    幂等键**有意保留**（指向那条 FAILURE）：入队是「响应可能丢失」的
    操作——broker 收下了但确认没回来时任务其实在跑，此时归还占位会让
    客户端重试重建第二个任务，上游被调两次。让重试回放到 FAILURE 更安全。
    """
    import app.queue as queue_mod

    async def boom(_task_id: str) -> None:
        raise RuntimeError("broker down")

    monkeypatch.setattr(queue_mod, "publish_execute", boom)

    resp = client.post(PATH, json=BODY,
                       headers={**AUTH, "Idempotency-Key": "rollback-key"})
    assert resp.status_code == 500

    from app.services.identity import token_hash

    th = token_hash("sk-test-token")
    assert patch_redis.dump().get(f"st:slot:{th}", "0") in ("0", None)

    row = next(iter(task_store.rows.values()))
    assert row["status"] == "FAILURE"
    assert "aborted" in row["fail_reason"]
    # 幂等键保留且指向该任务，重试回放到失败而非重建
    assert patch_redis.dump()[f"st:idem:{th}:rollback-key"] == row["task_id"]


def test_rollback_before_backfill_releases_idem(client, monkeypatch, patch_redis,
                                                task_store):
    """回填**之前**失败（落库崩了）时，占位必须归还——此时任务不存在，
    重试重建是安全且正确的。"""
    from app.services import submit as submit_mod

    async def boom(**_kwargs):
        raise RuntimeError("db down")

    monkeypatch.setattr(submit_mod.taskstore, "create", boom)

    resp = client.post(PATH, json=BODY,
                       headers={**AUTH, "Idempotency-Key": "early-fail"})
    assert resp.status_code == 500

    from app.services.identity import token_hash

    th = token_hash("sk-test-token")
    assert f"st:idem:{th}:early-fail" not in patch_redis.dump()
    assert patch_redis.dump().get(f"st:slot:{th}", "0") in ("0", None)


def test_model_missing_falls_back(client, task_store):
    """body 无 model / 非 JSON 都不该拒绝提交——只影响 task_id 前缀。"""
    task_id = client.post(PATH, json={"prompt": "no model"},
                          headers=AUTH).json()["task_id"]
    assert task_id.startswith("task_")

    raw_id = client.post(PATH, content=b"\x00\x01binary",
                         headers={**AUTH, "Content-Type": "application/octet-stream"},
                         ).json()["task_id"]
    assert raw_id.startswith("task_")


def test_query_string_preserved(client, task_store):
    """path/query/body 原文存储原样转发。"""
    task_id = client.post(f"{PATH}?debug=1&x=2", json=BODY,
                          headers=AUTH).json()["task_id"]
    assert task_store.rows[task_id]["data"]["request_query"] == "debug=1&x=2"


def test_callback_url_recorded(client, task_store):
    task_id = client.post(PATH, json=BODY,
                          headers={**AUTH, "X-Callback-Url": "http://cb.example/hook"},
                          ).json()["task_id"]
    assert task_store.rows[task_id]["data"]["callback_url"] == "http://cb.example/hook"


def test_balance_failure_degrades_to_one_slot(client, fake_billing, task_store):
    """billing 抖动时余额未知 → 回落 1 槽，而不是拒绝服务。"""
    fake_billing.balance_error = True
    assert client.post(PATH, json=BODY, headers=AUTH).status_code == 202
    assert client.post(PATH, json=BODY, headers=AUTH).status_code == 429
