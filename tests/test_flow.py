"""查询 / 字节级回放 / 长轮询 / 取消。

对应 AC-19 ~ AC-25、AC-22（410）。字节级回放是产品定位的关键承诺：
客户端把 `/async` 去掉后拿到的东西必须和直接调同步接口完全一致。
"""

from __future__ import annotations

import threading
import time

import pytest

from app.services import codec, slots, tokensession

BASE = "/async/v1/images/generations"
TASK = "dall_e_3_" + "b" * 32
TH = "tokenhash0000000000000000000000"


async def _seed(task_store, status="QUEUED", **data_over) -> str:
    await task_store.create(TASK, "/v1/images/generations", {
        "source": "stask", "model": "dall-e-3", "token_hash": TH,
        "upstream_response": "",
        "upstream_content_type": "", "upstream_status": 0,
        **data_over,
    })
    task_store.rows[TASK]["status"] = status
    if status in ("SUCCESS", "FAILURE", "CANCELED"):
        task_store.rows[TASK]["finish_time"] = task_store.now()
    return TASK


# ---------------------------------------------------------------------------
# 查询三态
# ---------------------------------------------------------------------------


async def test_pending_returns_202(client, task_store):
    """AC-19。"""
    await _seed(task_store)
    resp = client.get(f"{BASE}/{TASK}")
    assert resp.status_code == 202
    body = resp.json()
    assert body["task_id"] == TASK
    assert body["status"] == "QUEUED"
    assert "created_at" in body


async def test_success_replays_bytes_exactly(client, task_store):
    """AC-20：字节级回放 + Content-Type 原样。

    连 JSON 的空白与键序都必须一致——客户端可能在做签名校验。
    """
    payload = b'{"created":1,   "data":[{"b64_json":"AAAA"}]}'
    await _seed(task_store, "SUCCESS",
                upstream_response=codec.encode(payload),
                upstream_content_type="application/json; charset=utf-8",
                upstream_status=200)

    resp = client.get(f"{BASE}/{TASK}")
    assert resp.status_code == 200
    assert resp.content == payload
    assert resp.headers["content-type"] == "application/json; charset=utf-8"


async def test_binary_response_replayed(client, task_store):
    """TTS 返回 audio/mpeg 二进制——回放不得被 JSON 化。"""
    audio = bytes(range(256)) * 4
    await _seed(task_store, "SUCCESS",
                upstream_response=codec.encode(audio),
                upstream_content_type="audio/mpeg", upstream_status=200)

    resp = client.get(f"{BASE}/{TASK}")
    assert resp.content == audio
    assert resp.headers["content-type"] == "audio/mpeg"


@pytest.mark.parametrize("upstream_status", [400, 401, 402, 429, 500])
async def test_failure_replays_upstream_status_and_body(client, task_store,
                                                        upstream_status):
    """AC-21：重放上游状态码 + 原文（不包装成本地 error 形制）。"""
    body = b'{"error":{"message":"quota exceeded","type":"insufficient_quota"}}'
    await _seed(task_store, "FAILURE",
                upstream_response=codec.encode(body),
                upstream_content_type="application/json",
                upstream_status=upstream_status)

    resp = client.get(f"{BASE}/{TASK}")
    assert resp.status_code == upstream_status
    assert resp.content == body


async def test_canceled_returns_status_view(client, task_store):
    """CANCELED 从没调过上游，没有原文可回放——返回 200 + 状态视图。

    它不是「失败」（用户自己取消的），不该被塞进 502 的 error 形制。
    """
    await _seed(task_store, "CANCELED")
    resp = client.get(f"{BASE}/{TASK}")
    assert resp.status_code == 200
    assert resp.json()["status"] == "CANCELED"


async def test_local_failure_uses_error_envelope(client, task_store):
    """无上游原文的本地失败 → 统一 {"error": {...}} 形制。"""
    await _seed(task_store, "FAILURE")
    task_store.rows[TASK]["fail_reason"] = "token session missing"

    resp = client.get(f"{BASE}/{TASK}")
    assert resp.status_code == 502
    assert resp.json()["error"]["code"] == "upstream_no_body"
    assert "token session missing" in resp.json()["error"]["message"]


async def test_purged_result_returns_410(client, task_store):
    """AC-22：结果已 TTL 清理 → 410（不是 404）。

    404 会让客户端以为 task_id 写错了；410 明确表达「存在过、已过期」。
    """
    await _seed(task_store, "SUCCESS", result_purged=True, upstream_status=200)
    resp = client.get(f"{BASE}/{TASK}")
    assert resp.status_code == 410
    assert resp.json()["error"]["code"] == "result_expired"


async def test_corrupted_payload_returns_500(client, task_store):
    await _seed(task_store, "SUCCESS",
                upstream_response="!!!not-base64!!!", upstream_status=200)
    assert client.get(f"{BASE}/{TASK}").status_code == 500


def test_unknown_task_404(client):
    assert client.get(f"{BASE}/dall_e_3_{'f' * 32}").status_code == 404


def test_path_without_task_id_404(client):
    """形态预筛：纯路径不该被当成 task_id 去查库。"""
    assert client.get(BASE).status_code == 404


# ---------------------------------------------------------------------------
# 长轮询
# ---------------------------------------------------------------------------


async def test_long_poll_returns_on_terminal(client, task_store, test_settings):
    """AC-23：窗口内转终态 → 立即返回终态响应。

    TestClient 的请求跑在独立线程的事件循环里，所以用真线程延迟翻转
    状态（asyncio.create_task 在这里跨不过循环边界）。
    """
    await _seed(task_store)
    payload = b'{"done":true}'

    def flip_after_delay() -> None:
        time.sleep(0.05)
        task_store.rows[TASK]["status"] = "SUCCESS"
        task_store.rows[TASK]["data"].update({
            "upstream_response": codec.encode(payload),
            "upstream_content_type": "application/json",
            "upstream_status": 200,
        })

    threading.Thread(target=flip_after_delay, daemon=True).start()
    resp = client.get(f"{BASE}/{TASK}?wait=3")
    assert resp.status_code == 200
    assert resp.content == payload


async def test_long_poll_timeout_returns_202(client, task_store, test_settings):
    await _seed(task_store)
    resp = client.get(f"{BASE}/{TASK}?wait=1")
    assert resp.status_code == 202


def test_wait_exceeding_max_is_rejected(client, test_settings):
    """?wait 上限受 POLL_WAIT_MAX_SECONDS 约束（必须 < nginx read timeout）。"""
    assert client.get(f"{BASE}/{TASK}?wait=99999").status_code == 422


# ---------------------------------------------------------------------------
# 取消
# ---------------------------------------------------------------------------


async def test_cancel_pending_task(client, task_store, patch_redis):
    """AC-24：排队中 → CANCELED，释放槽，零资金动作。"""
    await _seed(task_store)
    await slots.acquire(TH, 10)
    await tokensession.store(TASK, "sk-test-token")

    resp = client.delete(f"{BASE}/{TASK}")
    assert resp.status_code == 200
    assert resp.json()["status"] == "CANCELED"

    row = task_store.rows[TASK]
    assert row["status"] == "CANCELED"
    assert row["progress"] == "100%"
    assert await slots.current(TH) == 0
    assert await tokensession.get(TASK) is None


async def test_cancel_in_progress_409(client, task_store):
    """AC-25：执行中不可取消——上游是同步调用，HTTP 没有中止语义。"""
    await _seed(task_store, "IN_PROGRESS")
    resp = client.delete(f"{BASE}/{TASK}")
    assert resp.status_code == 409
    assert "in progress" in resp.json()["error"]["message"]


@pytest.mark.parametrize("status", ["SUCCESS", "FAILURE", "CANCELED"])
async def test_cancel_terminal_409(client, task_store, status):
    await _seed(task_store, status)
    assert client.delete(f"{BASE}/{TASK}").status_code == 409


def test_cancel_unknown_404(client):
    assert client.delete(f"{BASE}/dall_e_3_{'e' * 32}").status_code == 404
