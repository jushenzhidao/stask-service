"""worker 执行链路：派发锁防重投 + 分流。

上游调用可能有副作用，多调一次就多一次——派发锁是全服务最关键的不变式。
"""

from __future__ import annotations

import httpx
import pytest

from app.services import execute, slots, tokensession
from tests.conftest import read_body, stored_body

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
        **stored_body("request_body", body),
        "upstream_base_url": "http://newapi:3000",
        "upstream_response": "", "upstream_response_encoding": "",
        "upstream_content_type": "", "upstream_status": 0,
        "dispatch_epoch": 0,
        "slot_flags": slots.FLAG_TOKEN, "slot_model": "dall-e-3",
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
    """2xx → SUCCESS，原文落库可回放，Content-Type 保留。

    小体 JSON 走**明文**分支：``upstream_response`` 直接就是原文，
    ``SELECT data ->> '$.upstream_response'`` 一眼可读（这是本轮改造的目的）。
    """
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
    assert read_body(row["data"], "upstream_response") == payload
    # 明文落库：存的就是原文本身，不是 base64
    assert row["data"]["upstream_response_encoding"] == "plain"
    assert row["data"]["upstream_response"] == payload.decode()
    assert row["data"]["upstream_content_type"] == "application/json"


async def test_large_response_falls_back_to_gzip(task_store, patch_redis, monkeypatch,
                                                  test_settings, respx_router):
    """超 ``plain_max_bytes`` → gzip+base64，并显式标记编码。"""
    monkeypatch.setattr(test_settings, "plain_max_bytes", 64)
    await _seed(task_store, patch_redis)
    payload = b'{"data":"' + b"x" * 500 + b'"}'
    respx_router.post(URL).mock(return_value=httpx.Response(200, content=payload))

    await execute.run(TASK)

    data = task_store.rows[TASK]["data"]
    assert data["upstream_response_encoding"] == "gzip+b64"
    assert data["upstream_response"] != payload.decode()      # 确实被编码了
    assert read_body(data, "upstream_response") == payload     # 但可完整还原


async def test_binary_response_always_gzips(task_store, patch_redis, test_settings,
                                             respx_router):
    """二进制体（非 UTF-8）无条件走 gzip——JSON 列存不了非法 UTF-8 字节。"""
    await _seed(task_store, patch_redis)
    payload = bytes(range(256)) * 4               # 含 0x80-0xFF，不是合法 UTF-8
    respx_router.post(URL).mock(
        return_value=httpx.Response(200, content=payload,
                                    headers={"Content-Type": "audio/mpeg"})
    )

    await execute.run(TASK)

    data = task_store.rows[TASK]["data"]
    assert data["upstream_response_encoding"] == "gzip+b64"
    assert read_body(data, "upstream_response") == payload


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
    """4xx → FAILURE，原文与状态码保留供重放（401 = 上游判定令牌无效）。

    ``fail_reason`` 必须带上游的**具体消息**，不能只有 ``upstream 401``——
    否则看板与回调都只能告诉用户"上游拒了"，真实原因还得回 DB 解原文。
    """
    await _seed(task_store, patch_redis)
    body = b'{"error":{"message":"invalid token"}}'
    respx_router.post(URL).mock(return_value=httpx.Response(status, content=body))

    await execute.run(TASK)

    row = task_store.rows[TASK]
    assert row["status"] == "FAILURE"
    assert row["data"]["upstream_status"] == status
    assert read_body(row["data"], "upstream_response") == body
    assert row["fail_reason"] == f"upstream {status}: invalid token"


# ---------------------------------------------------------------------------
# fail_reason 的上游错误消息提取
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(("body", "expected"), [
    # OpenAI / Ark 风格信封 + code
    (b'{"error":{"message":"model not found","code":"model_not_found"}}',
     "upstream 400: model not found (model_not_found)"),
    # 扁平 message
    (b'{"message":"\\u5185\\u5bb9\\u5ba1\\u6838\\u672a\\u901a\\u8fc7","code":1002}',
     "upstream 400: 内容审核未通过 (1002)"),
    # error 是字符串
    (b'{"error":"no available channel"}', "upstream 400: no available channel"),
    # 非 JSON：回落原文预览
    (b"<html><body>502 Bad Gateway</body></html>",
     "upstream 400: <html><body>502 Bad Gateway</body></html>"),
    # 空体：保持原样，不拼冒号
    (b"", "upstream 400"),
    # JSON 但没有任何可识别的消息字段：回落原文预览
    (b'{"foo":1}', 'upstream 400: {"foo":1}'),
])
async def test_fail_reason_carries_upstream_message(task_store, patch_redis,
                                                    test_settings, respx_router,
                                                    body, expected):
    await _seed(task_store, patch_redis)
    respx_router.post(URL).mock(return_value=httpx.Response(400, content=body))

    await execute.run(TASK)

    assert task_store.rows[TASK]["fail_reason"] == expected


async def test_fail_reason_is_bounded(task_store, patch_redis, test_settings,
                                      respx_router):
    """超长错误消息必须截断——fail_reason 是看板 Top10 的 GROUP BY 键，
    不截断会让每条失败都自成一组，排行榜彻底失效。"""
    await _seed(task_store, patch_redis)
    long_message = "x" * 5000
    respx_router.post(URL).mock(
        return_value=httpx.Response(400, json={"error": {"message": long_message}})
    )

    await execute.run(TASK)

    reason = task_store.rows[TASK]["fail_reason"]
    assert len(reason) <= 320                    # 300 详情 + "upstream 400: "
    assert reason.startswith("upstream 400: xxx")


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
# private_data.result_url（new-api 原生列契约）
# ---------------------------------------------------------------------------


async def test_success_writes_private_result_url(task_store, patch_redis,
                                                  test_settings, respx_router):
    """SUCCESS 必须把主产出 URL 写进 ``private_data.result_url``。

    宿主 new-api 的 ``Task.GetResultURL()`` 先读该键，为空才回落
    ``fail_reason``（它的历史兼容分支）。不写 = 宿主看板的「结果」列
    对我们的行永远是空的。
    """
    await _seed(task_store, patch_redis)
    video = "https://cdn.example/out.mp4?X-Tos-Signature=abc"
    respx_router.post(URL).mock(
        return_value=httpx.Response(200, json={"content": {"video_url": video}})
    )

    await execute.run(TASK)

    row = task_store.rows[TASK]
    assert row["status"] == "SUCCESS"
    # 两侧都要有：data 供本服务看板轻量投影，private_data 供宿主看板
    assert row["data"]["result_url"] == video
    assert row["private_data"]["result_url"] == video
    assert row["data"]["artifact_count"] == 1
    assert row["data"]["artifact_parser"] == "known"


async def test_success_without_artifacts_leaves_private_data_untouched(
    task_store, patch_redis, test_settings, respx_router
):
    """无制品的成功任务（纯文本补全类）不写 private_data。

    写一个空 ``result_url`` 会让宿主的 ``GetResultURL()`` 判定逻辑多一种
    "键存在但值为空"的状态；不写就是干净的"没有结果地址"。
    """
    await _seed(task_store, patch_redis)
    respx_router.post(URL).mock(
        return_value=httpx.Response(200, json={"text": "hello", "usage": {"tokens": 3}})
    )

    await execute.run(TASK)

    row = task_store.rows[TASK]
    assert row["status"] == "SUCCESS"
    assert row["data"]["artifact_count"] == 0
    assert row["private_data"] == {}
    assert "result_url" not in row["data"]


async def test_failure_does_not_write_private_result_url(task_store, patch_redis,
                                                          test_settings, respx_router):
    """失败任务绝不写 result_url——否则宿主看板会把错误体当成产出地址。"""
    await _seed(task_store, patch_redis)
    respx_router.post(URL).mock(
        return_value=httpx.Response(400, json={"error": {"message": "bad prompt"}})
    )

    await execute.run(TASK)

    assert task_store.rows[TASK]["private_data"] == {}


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
