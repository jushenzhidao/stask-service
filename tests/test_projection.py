"""测试 `get_meta()` 轻量投影：不返回 `upstream_response` 大字段、
布尔值归一、类型正确，且不影响 `get()` 的结果回放路径。

对应 taskstore.py:get_meta() 的设计目标——把可能达 10MB 的结果体排除在
看板详情、ops 诊断、对账扫描、回调推送这些只需要元数据的路径之外。
"""

from __future__ import annotations

from app.services import codec, taskstore
from tests.conftest import read_body, stored_body


TASK_ID = "test_projection_123456789012345678"


async def test_get_meta_excludes_upstream_response(task_store):
    """核心：轻量投影不返回 `upstream_response` 大字段。"""
    large_body = b"X" * (10 * 1024 * 1024)  # 10MB
    stored = stored_body("upstream_response", large_body)
    await task_store.create(TASK_ID, "/v1/images/generations", {
        "model": "dall-e-3",
        **stored,
        "upstream_status": 200,
    })

    # 轻量投影：不含 upstream_response 本体，但**含**编码标记
    # （标记只有十几个字节，排障时要靠它判断该怎么读原文）
    meta = await taskstore.get_meta(TASK_ID)
    assert meta is not None
    assert "upstream_response" not in meta["data"]
    assert meta["data"]["upstream_response_encoding"] == codec.GZIP_B64
    assert meta["data"]["upstream_status"] == 200
    assert meta["data"]["model"] == "dall-e-3"

    # 完整 get() 仍然能拿到原始结果体
    full = await taskstore.get(TASK_ID)
    assert full is not None
    assert read_body(full["data"], "upstream_response") == large_body


async def test_get_meta_boolean_fields_are_native_bool(task_store):
    """关键：布尔字段必须归一为 Python `bool`，不能是 `'false'` 字符串。

    `data ->> '$.x'` 返回的恒为**字符串**，布尔会变成 `'true'`/`'false'`。
    如果不归一，调用方的 `if row["data"]["reconcile_pending"]:` 会因为
    `'false'` 是真值字符串而永远成立——这是非常隐蔽的 bug。
    """
    await task_store.create(TASK_ID, "/v1/audio/speech", {
        "model": "tts-1",
        "result_purged": False,
        "body_truncated": True,
    })

    meta = await taskstore.get_meta(TASK_ID)
    assert meta is not None
    data = meta["data"]

    # 必须是原生 bool，不能是字符串
    assert data["result_purged"] is False
    assert isinstance(data["result_purged"], bool)
    assert data["body_truncated"] is True
    assert isinstance(data["body_truncated"], bool)

    # 验证反面：如果是字符串 'false'，这个判断会错误成立
    assert not data["result_purged"]
    if data["result_purged"]:
        raise AssertionError("'false' 字符串被当成真值了")


async def test_get_meta_tristate_callback_delivered(task_store):
    """三态字段 `callback_delivered`：None / True / False。"""
    await task_store.create(TASK_ID + "_none", "/v1/images", {
        "model": "dall-e-3",
    })
    meta_none = await taskstore.get_meta(TASK_ID + "_none")
    assert meta_none["data"]["callback_delivered"] is None

    await task_store.create(TASK_ID + "_true", "/v1/images", {
        "model": "dall-e-3",
        "callback_delivered": True,
    })
    meta_true = await taskstore.get_meta(TASK_ID + "_true")
    assert meta_true["data"]["callback_delivered"] is True

    await task_store.create(TASK_ID + "_false", "/v1/images", {
        "model": "dall-e-3",
        "callback_delivered": False,
    })
    meta_false = await taskstore.get_meta(TASK_ID + "_false")
    assert meta_false["data"]["callback_delivered"] is False


async def test_get_meta_int_fields_are_native_int(task_store):
    """整数字段必须归一为 Python `int`，不能是字符串。"""
    await task_store.create(TASK_ID, "/v1/images", {
        "model": "dall-e-3",
        "upstream_status": 429,
        "response_bytes": 1048576,
        "dispatch_epoch": 2,
    })

    meta = await taskstore.get_meta(TASK_ID)
    assert meta is not None
    data = meta["data"]

    assert data["upstream_status"] == 429
    assert isinstance(data["upstream_status"], int)
    assert data["response_bytes"] == 1048576
    assert isinstance(data["response_bytes"], int)
    assert data["dispatch_epoch"] == 2
    assert isinstance(data["dispatch_epoch"], int)


async def test_get_meta_string_fields_default_empty(task_store):
    """字符串字段缺失时应该是空串，不是 None。"""
    await task_store.create(TASK_ID, "/v1/images", {
        "model": "dall-e-3",
    })

    meta = await taskstore.get_meta(TASK_ID)
    assert meta is not None
    data = meta["data"]

    assert data["idempotency_key"] == ""
    assert data["callback_url"] == ""
    assert data["token_hash"] == ""


async def test_get_meta_covers_all_meta_keys(task_store):
    """确保轻量投影覆盖所有声明的元数据键。"""
    await task_store.create(TASK_ID, "/v1/audio/speech", {
        "model": "tts-1",
        "request_method": "POST",
        "request_path": "/v1/audio/speech",
        "request_query": "voice=alloy",
        "upstream_base_url": "http://newapi:3000",
        "upstream_content_type": "audio/mpeg",
        "upstream_status": 200,
        "response_bytes": 2048,
        "dispatch_epoch": 1,
        "result_purged": False,
        "body_truncated": False,
        "idempotency_key": "idem_abc",
        "callback_url": "https://example.com/hook",
        "callback_delivered": True,
        "token_hash": "hash_xyz",
    })

    meta = await taskstore.get_meta(TASK_ID)
    assert meta is not None
    data = meta["data"]

    # 字符串键
    assert data["model"] == "tts-1"
    assert data["request_method"] == "POST"
    assert data["request_path"] == "/v1/audio/speech"
    assert data["request_query"] == "voice=alloy"
    assert data["upstream_base_url"] == "http://newapi:3000"
    assert data["upstream_content_type"] == "audio/mpeg"
    assert data["idempotency_key"] == "idem_abc"
    assert data["callback_url"] == "https://example.com/hook"
    assert data["token_hash"] == "hash_xyz"

    # 整数键
    assert data["upstream_status"] == 200
    assert data["response_bytes"] == 2048
    assert data["dispatch_epoch"] == 1

    # 布尔键
    assert data["result_purged"] is False
    assert data["body_truncated"] is False

    # 三态键
    assert data["callback_delivered"] is True


async def test_result_replay_still_uses_get(task_store):
    """结果回放路径必须继续用 `get()`，确保能拿到完整的 `upstream_response`。"""
    payload = b'{"created":1234567890,"data":[{"url":"https://..."}]}'
    await task_store.create(TASK_ID, "/v1/images/generations", {
        "model": "dall-e-3",
        **stored_body("upstream_response", payload),
        "upstream_status": 200,
        "upstream_content_type": "application/json",
    })
    task_store.rows[TASK_ID]["status"] = "SUCCESS"
    task_store.rows[TASK_ID]["finish_time"] = task_store.now()

    # 轻量投影拿不到结果体
    meta = await taskstore.get_meta(TASK_ID)
    assert meta is not None
    assert "upstream_response" not in meta["data"]

    # 完整 get() 必须能拿到，否则回放路径会坏
    full = await taskstore.get(TASK_ID)
    assert full is not None
    assert full["data"]["upstream_response"] == payload.decode()   # 小体 → 明文
    assert read_body(full["data"], "upstream_response") == payload


async def test_get_meta_exposes_private_result_url(task_store):
    """``private_data`` 的白名单投影：只出 result_url / upstream_task_id。

    该列是 new-api 的 ``TaskPrivateData``，可能含渠道 ``key``（Gemini /
    Vertex 渠道会写）。这条用例守的是"绝不整列返回"——一旦有人图省事
    改成 ``SELECT private_data``，宿主哪天往里加敏感字段，我们的管理面
    就跟着泄露。
    """
    await task_store.create(TASK_ID, "/v1/videos", {"model": "seedance"})
    await taskstore.cas(
        TASK_ID, ("QUEUED",), "SUCCESS",
        patch={"result_url": "https://cdn/out.mp4"},
        private_patch={
            "result_url": "https://cdn/out.mp4",
            # 模拟宿主写入的敏感字段：绝不能出现在投影里
            "key": "sk-upstream-channel-secret",
        },
    )

    meta = await taskstore.get_meta(TASK_ID)
    assert meta is not None
    assert meta["private_data"] == {
        "result_url": "https://cdn/out.mp4",
        "upstream_task_id": "",
    }
    assert "key" not in meta["private_data"]
    # 整个响应里都不该出现渠道密钥
    assert "sk-upstream-channel-secret" not in str(meta)


async def test_get_meta_returns_none_for_missing_task(task_store):
    """不存在的 task_id 应该返回 None。"""
    meta = await taskstore.get_meta("nonexistent_task_id_999")
    assert meta is None
