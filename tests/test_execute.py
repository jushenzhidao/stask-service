"""worker 执行链路：派发锁防重投 + 分流。

上游调用可能有副作用，多调一次就多一次——派发锁是全服务最关键的不变式。
"""

from __future__ import annotations

import httpx
import pytest

from app.services import codec, execute, slots, tokensession

TASK = "dall_e_3_" + "a" * 32
TH = "tokenhash0000000000000000000000"
URL = "http://newapi:3000/v1/images/generations"


async def _seed(task_store, patch_redis, *, status="QUEUED", callback_url="",
                body=b'{"model":"dall-e-3"}') -> str:
    await task_store.create(TASK, "/v1/images/generations", {
        "source": "stask", "model": "dall-e-3", "token_hash": TH,
        "callback_url": callback_url,
        "request_method": "POST", "request_path": "/v1/images/generations",
        "request_query": "", "request_headers": {"Content-Type": "application/json"},
        "request_body": codec.encode(body),
        "upstream_base_url": "http://newapi:3000",
        "upstream_response": "", "upstream_content_type": "", "upstream_status": 0,
        "dispatch_epoch": 0,
    })
    task_store.rows[TASK]["status"] = status
    await tokensession.store(TASK, "sk-test-token")
    await slots.acquire(TH, 10)
    return TASK


# ---------------------------------------------------------------------------
# 成功路径
# ---------------------------------------------------------------------------


async def test_success_stores_replayable_response(task_store, patch_redis,
                                                  test_settings, respx_router,
                                                  queue_events):
    """2xx → SUCCESS，原文 gzip 落库，Content-Type 保留。"""
    await _seed(task_store, patch_redis)
    payload = b'{"created":1,"data":[{"url":"https://cdn/x.png"}]}'
    respx_router.post(URL).mock(
        return_value=httpx.Response(200, content=payload,
                                    headers={"Content-Type": "application/json"})
    )

    await execute.run(TASK)

    row = task_store.rows[TASK]
    assert row["status"] == "SUCCESS"
    assert row["progress"] == "100%"
    assert codec.decode(row["data"]["upstream_response"]) == payload
    assert row["data"]["upstream_content_type"] == "application/json"


async def test_success_releases_slot_and_session(task_store, patch_redis,
                                                 test_settings, respx_router):
    """终态释放槽 + 清会话。"""
    await _seed(task_store, patch_redis)
    respx_router.post(URL).mock(return_value=httpx.Response(200, json={"ok": True}))

    assert await slots.current(TH) == 1
    await execute.run(TASK)

    assert await slots.current(TH) == 0
    assert await tokensession.get(TASK) is None


async def test_upstream_request_carries_token_and_task_id(task_store, patch_redis,
                                                          test_settings, respx_router):
    """上游调用必须带用户令牌与 X-Task-Id（排障反查依据）。"""
    await _seed(task_store, patch_redis)
    route = respx_router.post(URL).mock(return_value=httpx.Response(200, json={}))

    await execute.run(TASK)

    sent = route.calls[0].request
    assert sent.headers["authorization"] == "Bearer sk-test-token"
    assert sent.headers["x-task-id"] == TASK
    assert sent.content == b'{"model":"dall-e-3"}'      # body 原文转发


# ---------------------------------------------------------------------------
# 失败分流
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("status", [400, 401, 402, 429])
async def test_4xx_becomes_failure_with_replayable_body(task_store, patch_redis,
                                                        test_settings, respx_router,
                                                        status):
    """4xx → FAILURE，原文与状态码保留供重放（401 = 上游判定令牌无效）。"""
    await _seed(task_store, patch_redis)
    body = b'{"error":{"message":"invalid token"}}'
    respx_router.post(URL).mock(return_value=httpx.Response(status, content=body))

    await execute.run(TASK)

    row = task_store.rows[TASK]
    assert row["status"] == "FAILURE"
    assert row["data"]["upstream_status"] == status
    assert codec.decode(row["data"]["upstream_response"]) == body


async def test_5xx_no_retry_by_default(task_store, patch_redis, test_settings,
                                       respx_router):
    """RETRY_MAX=0 时 5xx 直接判 FAILURE，只调一次上游（副作用保守）。"""
    await _seed(task_store, patch_redis)
    route = respx_router.post(URL).mock(return_value=httpx.Response(502, text="bad gw"))

    await execute.run(TASK)

    assert route.call_count == 1
    assert task_store.rows[TASK]["status"] == "FAILURE"


async def test_5xx_retries_when_configured(task_store, patch_redis, monkeypatch,
                                           test_settings, respx_router):
    monkeypatch.setattr(test_settings, "retry_max", 2)
    monkeypatch.setattr(test_settings, "retry_backoff_base", 0.0)
    await _seed(task_store, patch_redis)
    route = respx_router.post(URL).mock(return_value=httpx.Response(500))

    await execute.run(TASK)

    assert route.call_count == 3                        # 首次 + 2 次重试
    assert task_store.rows[TASK]["status"] == "FAILURE"


async def test_timeout_fails_without_retry(task_store, patch_redis, test_settings,
                                           respx_router):
    """超时 → FAILURE 且**绝不重试**（请求已发出，可能已有副作用）。

    结果拿不回来，留挂着毫无意义；终态释放槽。
    """
    await _seed(task_store, patch_redis)
    route = respx_router.post(URL).mock(side_effect=httpx.ReadTimeout("timeout"))

    await execute.run(TASK)

    row = task_store.rows[TASK]
    assert row["status"] == "FAILURE"
    assert route.call_count == 1
    assert "timeout" in row["fail_reason"]
    assert await slots.current(TH) == 0                 # 终态释放槽


async def test_connect_error_is_safe_to_fail(task_store, patch_redis, monkeypatch,
                                             test_settings, respx_router):
    """连接层失败 = 请求未到达上游 = 零副作用 → 判死安全。"""
    await _seed(task_store, patch_redis)
    respx_router.post(URL).mock(side_effect=httpx.ConnectError("refused"))

    await execute.run(TASK)

    row = task_store.rows[TASK]
    assert row["status"] == "FAILURE"
    assert "unreachable" in row["fail_reason"]


async def test_connect_error_retries_when_configured(task_store, patch_redis,
                                                     monkeypatch, test_settings,
                                                     respx_router):
    monkeypatch.setattr(test_settings, "retry_max_connect", 2)
    monkeypatch.setattr(test_settings, "retry_backoff_base", 0.0)
    await _seed(task_store, patch_redis)
    route = respx_router.post(URL).mock(side_effect=httpx.ConnectError("refused"))

    await execute.run(TASK)

    assert route.call_count == 3


async def test_oversized_response_fails_loudly(task_store, patch_redis, monkeypatch,
                                               test_settings, respx_router):
    monkeypatch.setattr(test_settings, "response_max_bytes", 32)
    await _seed(task_store, patch_redis)
    respx_router.post(URL).mock(return_value=httpx.Response(200, content=b"x" * 100))

    await execute.run(TASK)

    row = task_store.rows[TASK]
    assert row["status"] == "FAILURE"
    assert "too large" in row["fail_reason"]
    assert row["data"]["upstream_response"] == ""       # 不落超限内容


# ---------------------------------------------------------------------------
# 派发锁（防重复调用的核心）
# ---------------------------------------------------------------------------


async def test_dispatch_lock_blocks_redelivery(task_store, patch_redis,
                                               test_settings, respx_router):
    """队列重投时锁已在 → 绝不再调上游。

    「锁在 = 一次调用已发出」。这条测试守的是整个服务最贵的不变式。
    """
    await _seed(task_store, patch_redis)
    route = respx_router.post(URL).mock(return_value=httpx.Response(200, json={}))

    await execute.run(TASK)                             # 第一次：正常执行
    assert route.call_count == 1

    # 模拟崩溃重投：把状态改回 QUEUED，锁仍在（锁不主动释放）
    task_store.rows[TASK]["status"] = "QUEUED"
    await execute.run(TASK)

    assert route.call_count == 1                        # 上游没被第二次调用
    assert task_store.rows[TASK]["status"] == "IN_PROGRESS"  # 等 sweeper 收敛


async def test_redis_down_refuses_dispatch(task_store, patch_redis, monkeypatch,
                                           test_settings, respx_router):
    """锁机制失效时保守拒绝派发——宁可等下一轮兜底。"""
    await _seed(task_store, patch_redis)
    route = respx_router.post(URL).mock(return_value=httpx.Response(200, json={}))

    async def boom(*_a, **_kw):
        raise ConnectionError("redis down")

    monkeypatch.setattr(patch_redis, "set", boom)

    await execute.run(TASK)
    assert route.call_count == 0


# ---------------------------------------------------------------------------
# 边界
# ---------------------------------------------------------------------------


async def test_missing_token_session_fails_without_calling_upstream(
    task_store, patch_redis, test_settings, respx_router
):
    """会话丢失：绝不用别的凭证代打。此时还没调过上游，零副作用。"""
    await _seed(task_store, patch_redis)
    await tokensession.clear(TASK)

    await execute.run(TASK)

    row = task_store.rows[TASK]
    assert row["status"] == "FAILURE"
    assert "token session missing" in row["fail_reason"]


async def test_terminal_task_is_skipped(task_store, patch_redis, test_settings,
                                        respx_router):
    await _seed(task_store, patch_redis, status="SUCCESS")
    await execute.run(TASK)
    assert task_store.rows[TASK]["status"] == "SUCCESS"


async def test_unknown_task_is_noop(task_store, patch_redis, test_settings):
    await execute.run("nope_" + "0" * 32)               # 不抛异常即通过


async def test_callback_enqueued_on_terminal(task_store, patch_redis, test_settings,
                                             respx_router, queue_events):
    """终态且配了回调 → 入队推送。"""
    await _seed(task_store, patch_redis, callback_url="http://cb.example/hook")
    respx_router.post(URL).mock(return_value=httpx.Response(200, json={}))

    await execute.run(TASK)

    assert queue_events.notify == [(TASK, 1, 0)]


async def test_no_callback_no_enqueue(task_store, patch_redis, test_settings,
                                      respx_router, queue_events):
    await _seed(task_store, patch_redis)
    respx_router.post(URL).mock(return_value=httpx.Response(200, json={}))
    await execute.run(TASK)
    assert queue_events.notify == []


async def test_query_string_forwarded(task_store, patch_redis, test_settings,
                                      respx_router):
    await _seed(task_store, patch_redis)
    task_store.rows[TASK]["data"]["request_query"] = "a=1&b=2"
    route = respx_router.post(f"{URL}?a=1&b=2").mock(
        return_value=httpx.Response(200, json={})
    )
    await execute.run(TASK)
    assert route.call_count == 1
