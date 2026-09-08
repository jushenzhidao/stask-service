"""提交端点端到端（经 ASGI 客户端）。

覆盖：202 契约 / 自动幂等 / 并发槽 / 限流 / 准入 / 回滚。
"""

from __future__ import annotations

import pytest

from tests.conftest import AUTH

PATH = "/async/v1/images/generations"
BODY = {"model": "dall-e-3", "prompt": "a red cube", "n": 1}


def test_submit_returns_202_with_location(client, task_store, queue_events):
    """202 + task_id + Location 头，且已入队。"""
    resp = client.post(PATH, json=BODY, headers=AUTH)
    assert resp.status_code == 202

    payload = resp.json()
    task_id = payload["task_id"]
    assert payload["status"] == "QUEUED"
    assert task_id.startswith("dall_e_3_")
    assert resp.headers["location"] == f"{PATH}/{task_id}"

    row = task_store.rows[task_id]
    assert row["status"] == "QUEUED"
    assert row["progress"] == "0%"
    assert queue_events.execute == [task_id]


def test_newapi_coexistence_contract(client, task_store):
    """ADR-006 共存契约：独立 channel_id / quota=0 / 原生状态枚举。"""
    task_id = client.post(PATH, json=BODY, headers=AUTH).json()["task_id"]
    row = task_store.rows[task_id]

    assert row["platform"] == "stask"          # 非 suno/mj → adaptor 为 nil
    assert row["channel_id"] == 990            # 独立渠道（test_settings）
    assert row["quota"] == 0                   # 零资金记账
    assert row["status"] == "QUEUED"           # 原生枚举
    assert row["progress"] == "0%"


def test_submitted_data_contract(client, task_store):
    """data JSON 契约（SPEC §6）。"""
    task_id = client.post(PATH, json=BODY, headers=AUTH).json()["task_id"]
    data = task_store.rows[task_id]["data"]

    assert data["source"] == "stask"
    assert data["model"] == "dall-e-3"
    assert data["request_method"] == "POST"
    assert data["request_path"] == "/v1/images/generations"
    assert data["upstream_base_url"] == "http://newapi:3000"


def test_sk_never_persisted(client, task_store):
    """红线：用户令牌绝不进 tasks 表。"""
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


def test_denied_path_403(client):
    resp = client.post("/async/api/user/self", json={}, headers=AUTH)
    assert resp.status_code == 403
    assert resp.json()["error"]["type"] == "permission_error"


def test_path_not_in_allowlist_403(client):
    assert client.post("/async/v1/chat/completions",
                       json=BODY, headers=AUTH).status_code == 403


@pytest.mark.parametrize("method", ["patch", "options"])
def test_method_not_allowed_405(client, method):
    assert getattr(client, method)(PATH, headers=AUTH).status_code == 405


def test_put_is_accepted(client, task_store):
    resp = client.put(PATH, json=BODY, headers=AUTH)
    assert resp.status_code == 202


def test_bad_upstream_header_400(client):
    """allowlist 之外的 upstream → 400，且**在鉴权之前**就拒绝。"""
    resp = client.post(PATH, json=BODY,
                       headers={**AUTH, "X-Upstream-Base-Url": "http://evil.com"})
    assert resp.status_code == 400


def test_upstream_userinfo_400(client):
    resp = client.post(PATH, json=BODY,
                       headers={**AUTH, "X-Upstream-Base-Url": "http://a:b@newapi:3000"})
    assert resp.status_code == 400


# ---------------------------------------------------------------------------
# 显式幂等（Idempotency-Key）
# ---------------------------------------------------------------------------


def test_no_key_creates_new_task_each_time(client, task_store, queue_events):
    """默认（不带头）不去重：同请求体两次提交是两个任务。"""
    first = client.post(PATH, json=BODY, headers=AUTH).json()
    second = client.post(PATH, json=BODY, headers=AUTH).json()

    assert first["task_id"] != second["task_id"]
    assert first["replayed"] is False and second["replayed"] is False
    assert len(task_store.rows) == 2
    assert len(queue_events.execute) == 2


def test_idempotency_key_replays(client, task_store, queue_events):
    """带同一 Idempotency-Key 的第二次提交返回同一 task_id，
    不重建、不重复入队。"""
    headers = {**AUTH, "Idempotency-Key": "order-1"}
    first = client.post(PATH, json=BODY, headers=headers).json()
    second = client.post(PATH, json=BODY, headers=headers).json()

    assert first["task_id"] == second["task_id"]
    assert first["replayed"] is False
    assert second["replayed"] is True
    assert len(task_store.rows) == 1
    assert len(queue_events.execute) == 1


def test_different_body_creates_new_task(client, task_store):
    """不带 key：不同请求体自然是不同任务。"""
    a = client.post(PATH, json=BODY, headers=AUTH).json()["task_id"]
    b = client.post(PATH, json={**BODY, "prompt": "a blue cube"},
                    headers=AUTH).json()["task_id"]
    assert a != b
    assert len(task_store.rows) == 2


def test_different_key_creates_new_task(client, task_store):
    """换 Idempotency-Key = 对同一请求体强制创建新任务。"""
    a = client.post(PATH, json=BODY,
                    headers={**AUTH, "Idempotency-Key": "run-1"}).json()["task_id"]
    b = client.post(PATH, json=BODY,
                    headers={**AUTH, "Idempotency-Key": "run-2"}).json()["task_id"]
    c = client.post(PATH, json=BODY,
                    headers={**AUTH, "Idempotency-Key": "run-2"}).json()
    assert a != b
    assert c["task_id"] == b and c["replayed"] is True
    assert len(task_store.rows) == 2


def test_different_token_different_task(client, task_store):
    """幂等键含 token_hash：不同用户的相同 key 互不干扰。"""
    a = client.post(PATH, json=BODY,
                    headers={**AUTH, "Idempotency-Key": "k"}).json()["task_id"]
    b = client.post(PATH, json=BODY,
                    headers={"Authorization": "Bearer sk-other",
                             "Idempotency-Key": "k"}).json()["task_id"]
    assert a != b


def test_concurrent_same_key_409(client, patch_redis, task_store):
    """占位存在但行未落库（同 key 真并发在飞）→ 409，绝不重建。"""
    from app.services import idem

    headers = {**AUTH, "Idempotency-Key": "conc-1"}
    resp1 = client.post(PATH, json=BODY, headers=headers)
    tid = resp1.json()["task_id"]
    # 删掉行、保留占位，模拟另一个请求正在创建链路中
    del task_store.rows[tid]
    patch_redis._data[f"st:idem:{tid}"] = idem.PENDING

    resp = client.post(PATH, json=BODY, headers=headers)
    assert resp.status_code == 409
    assert task_store.rows == {}


# ---------------------------------------------------------------------------
# 并发槽 / 限流 / 体量
# ---------------------------------------------------------------------------


def test_slot_exhausted_429_with_retry_after(client, monkeypatch, test_settings):
    """槽满时第二个提交返 429 + Retry-After（不同 body 不吃幂等）。"""
    monkeypatch.setattr(test_settings, "max_slots", 1)

    assert client.post(PATH, json=BODY, headers=AUTH).status_code == 202
    resp = client.post(PATH, json={**BODY, "prompt": "another"}, headers=AUTH)
    assert resp.status_code == 429
    assert int(resp.headers["retry-after"]) >= 1
    assert resp.json()["error"]["type"] == "rate_limit_error"


def test_rate_limit_429(client, monkeypatch, test_settings):
    monkeypatch.setattr(test_settings, "rate_limit", 2)
    codes = [
        client.post(PATH, json={**BODY, "n": i}, headers=AUTH).status_code
        for i in range(4)
    ]
    assert codes[:2] == [202, 202]
    assert 429 in codes[2:]


def test_body_too_large_413(client, monkeypatch, test_settings):
    monkeypatch.setattr(test_settings, "body_max_bytes", 64)
    resp = client.post(PATH, json={"model": "x", "prompt": "y" * 500}, headers=AUTH)
    assert resp.status_code == 413


# ---------------------------------------------------------------------------
# 回滚
# ---------------------------------------------------------------------------


def test_enqueue_failure_rolls_back(client, monkeypatch, patch_redis, task_store):
    """入队失败必须归还槽，并把已落库的行判死。

    行**有意保留**（FAILURE）：入队是「响应可能丢失」的操作——broker
    收下了但确认没回来时任务其实在跑，此时删行会让重试重建第二个任务、
    上游被调两次。带 Idempotency-Key 的重试会回放到 FAILURE 更安全。
    """
    import app.queue as queue_mod

    async def boom(_task_id: str) -> None:
        raise RuntimeError("broker down")

    monkeypatch.setattr(queue_mod, "publish_execute", boom)

    headers = {**AUTH, "Idempotency-Key": "rb-1"}
    resp = client.post(PATH, json=BODY, headers=headers)
    assert resp.status_code == 500

    from app.deps.auth import token_hash

    th = token_hash("sk-test-token")
    assert patch_redis.dump().get(f"st:slot:{th}", "0") in ("0", None)

    row = next(iter(task_store.rows.values()))
    assert row["status"] == "FAILURE"
    assert "aborted" in row["fail_reason"]

    # 同 key 重试回放到该 FAILURE 而非重建
    resp2 = client.post(PATH, json=BODY, headers=headers)
    assert resp2.json()["replayed"] is True
    assert resp2.json()["task_id"] == row["task_id"]


def test_rollback_before_create_releases_placeholder(client, monkeypatch,
                                                     patch_redis, task_store):
    """落库**之前**失败时，占位必须归还——此时行不存在，重试重建是安全的。"""
    from app.services import submit as submit_mod

    async def boom(**_kwargs):
        raise RuntimeError("db down")

    monkeypatch.setattr(submit_mod.taskstore, "create", boom)

    resp = client.post(PATH, json=BODY, headers=AUTH)
    assert resp.status_code == 500

    from app.deps.auth import token_hash

    th = token_hash("sk-test-token")
    assert not any(k.startswith("st:idem:") for k in patch_redis.dump())
    assert patch_redis.dump().get(f"st:slot:{th}", "0") in ("0", None)


# ---------------------------------------------------------------------------
# 其他
# ---------------------------------------------------------------------------


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
    """path/query/body 原文存储原样转发；query 参与幂等指纹。"""
    a = client.post(f"{PATH}?debug=1&x=2", json=BODY, headers=AUTH).json()["task_id"]
    assert task_store.rows[a]["data"]["request_query"] == "debug=1&x=2"
    b = client.post(f"{PATH}?debug=1&x=3", json=BODY, headers=AUTH).json()["task_id"]
    assert a != b


def test_callback_url_recorded(client, task_store):
    task_id = client.post(PATH, json=BODY,
                          headers={**AUTH, "X-Callback-Url": "http://cb.example/hook"},
                          ).json()["task_id"]
    assert task_store.rows[task_id]["data"]["callback_url"] == "http://cb.example/hook"
