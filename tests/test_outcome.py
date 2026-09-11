"""执行摘要（``Outcome``）与 taskiq-admin 可见性。

背景：任务体从不抛异常（抛 = 重投 = 重复调上游），代价是 admin 面板上
``Return Value`` 恒 null、``Error`` 恒空。这里锁死两件事：

1. 每条终止路径都返回**带 stage 的结构化摘要**，而不是 None；
2. ``OutcomeMiddleware`` 把 ``ok=False`` 的摘要翻译成 ``result.error``，
   让 admin 显示错误——且**不改变 ack 行为**（不会引发重投）。
"""

from __future__ import annotations

import httpx
import pytest
from taskiq import TaskiqResult

from app.queue import OutcomeMiddleware
from app.services import execute, slots, tokensession
from app.services.outcome import Outcome, TaskExecutionError, preview
from tests.conftest import stored_body

TASK = "dall_e_3_" + "b" * 32
TH = "tokenhash1111111111111111111111"
URL = "http://newapi:3000/v1/images/generations"


async def _seed(task_store, *, status="QUEUED", body=b'{"model":"dall-e-3"}') -> str:
    await task_store.create(TASK, "/v1/images/generations", {
        "source": "stask", "model": "dall-e-3", "token_hash": TH,
        "callback_url": "",
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
# Outcome 载荷本身
# ---------------------------------------------------------------------------


def test_as_dict_keeps_ok_false_but_drops_empties() -> None:
    """``ok=False`` 必须保留——它是面板上最关键的一列，而 ``False == 0``
    会被朴素的空值过滤误删。其余空字段剔除，避免面板全是噪声。"""
    payload = Outcome(task_id="t1", stage="timeout", ok=False).as_dict()

    assert payload["ok"] is False
    assert payload["stage"] == "timeout"
    # 未赋值的字段不该出现
    assert "upstream_preview" not in payload
    assert "artifact_urls" not in payload
    assert "result_url" not in payload


def test_as_dict_is_json_serializable() -> None:
    """摘要要进 result backend，含 bytes / 异常实例会导致结果保存失败。"""
    import json

    payload = Outcome(
        task_id="t1", stage="success", status="SUCCESS", upstream_status=200,
        artifact_types=["image"], artifact_urls=["https://cdn/x.png"],
    ).as_dict()

    assert json.loads(json.dumps(payload)) == payload


def test_summary_prefers_detail() -> None:
    with_detail = Outcome(task_id="t", stage="timeout", detail="call sent, result lost")
    assert with_detail.summary() == "call sent, result lost"

    without = Outcome(task_id="t", stage="upstream_error", status="FAILURE",
                      upstream_status=502, fail_reason="upstream 502")
    line = without.summary()
    assert "stage=upstream_error" in line
    assert "upstream=502" in line
    assert "reason=upstream 502" in line


def test_task_execution_error_repr_is_readable() -> None:
    """admin 存的是 ``repr(error)``——默认 repr 会是 <object at 0x...>，无信息量。"""
    err = TaskExecutionError(Outcome(task_id="t", stage="timeout", ok=False,
                                     detail="upstream timed out"))
    assert repr(err) == "TaskExecutionError(upstream timed out)"


def test_preview_truncates_and_marks_total() -> None:
    out = preview(b"x" * 900, limit=100)
    assert out.startswith("x" * 100)
    assert "truncated, 900 bytes total" in out


# ---------------------------------------------------------------------------
# 各 stage 的真实执行路径
# ---------------------------------------------------------------------------


async def test_success_outcome_carries_artifacts(task_store, patch_redis,
                                                 test_settings, respx_router):
    """成功摘要要能直接回答「产出是什么」——制品数、类型、URL、解析级别。"""
    await _seed(task_store)
    payload = b'{"created":1,"data":[{"url":"https://cdn.example.com/x.png"}]}'
    respx_router.post(URL).mock(
        return_value=httpx.Response(200, content=payload,
                                    headers={"Content-Type": "application/json"})
    )

    out = await execute.run(TASK)

    assert out["ok"] is True
    assert out["stage"] == "success"
    assert out["status"] == "SUCCESS"
    assert out["upstream_status"] == 200
    assert out["artifact_count"] == 1
    assert out["artifact_types"] == ["image"]
    assert out["artifact_urls"] == ["https://cdn.example.com/x.png"]
    assert out["result_url"] == "https://cdn.example.com/x.png"
    assert out["artifact_parser"] == "known"
    assert out["model"] == "dall-e-3"


async def test_upstream_error_outcome_has_preview(task_store, patch_redis,
                                                  test_settings, respx_router):
    """上游 4xx：摘要必须带状态码与响应体预览，免得还要回 DB 捞原文。"""
    await _seed(task_store)
    body = b'{"error":{"message":"invalid api key"}}'
    respx_router.post(URL).mock(return_value=httpx.Response(401, content=body))

    out = await execute.run(TASK)

    assert out["ok"] is False
    assert out["stage"] == "upstream_error"
    assert out["status"] == "FAILURE"
    assert out["upstream_status"] == 401
    assert "invalid api key" in out["upstream_preview"]
    # 本轮改造：fail_reason 携带上游具体消息，不再只是 "upstream 401"
    assert out["fail_reason"] == "upstream 401: invalid api key"


async def test_unreachable_outcome_reports_attempts(task_store, patch_redis,
                                                    test_settings, respx_router):
    """连接层失败：摘要要能说清「重试了几次都没连上」。"""
    await _seed(task_store)
    respx_router.post(URL).mock(side_effect=httpx.ConnectError("refused"))

    out = await execute.run(TASK)

    assert out["ok"] is False
    assert out["stage"] == "unreachable"
    assert out["status"] == "FAILURE"
    assert out["attempts"] >= 1
    assert "ConnectError" in out["fail_reason"]


async def test_timeout_outcome_marks_not_retried(task_store, patch_redis,
                                                 test_settings, respx_router):
    """超时：请求已发出、结果不可得，摘要要写明「没重试」的原因。"""
    await _seed(task_store)
    respx_router.post(URL).mock(side_effect=httpx.ReadTimeout("timed out"))

    out = await execute.run(TASK)

    assert out["ok"] is False
    assert out["stage"] == "timeout"
    assert "side effects" in out["detail"]


async def test_token_missing_outcome(task_store, patch_redis, test_settings):
    """令牌会话丢失：调用前判死，摘要要区别于「上游拒绝」。"""
    await _seed(task_store)
    await tokensession.clear(TASK)

    out = await execute.run(TASK)

    assert out["ok"] is False
    assert out["stage"] == "token_missing"
    assert out["status"] == "FAILURE"
    assert "token session" in out["fail_reason"]


async def test_not_found_outcome(task_store, patch_redis, test_settings):
    out = await execute.run("stask_missing_task_00000000000000")

    assert out["ok"] is False
    assert out["stage"] == "not_found"


async def test_already_terminal_outcome_is_ok(task_store, patch_redis, test_settings):
    """已终态属于正常跳过（重投的预期结果），不该在面板上标红。"""
    await _seed(task_store, status="SUCCESS")

    out = await execute.run(TASK)

    assert out["ok"] is True
    assert out["stage"] == "already_terminal"
    assert out["status"] == "SUCCESS"


async def test_lock_held_outcome_is_ok(task_store, patch_redis, test_settings,
                                       respx_router):
    """派发锁在 = 防重投生效，是**正常**结果：ok=True，不该报错。"""
    await _seed(task_store)
    respx_router.post(URL).mock(return_value=httpx.Response(200, json={"ok": True}))
    await execute.run(TASK)                      # 第一次执行，锁留下

    task_store.rows[TASK]["status"] = "QUEUED"   # 模拟重投
    out = await execute.run(TASK)

    assert out["ok"] is True
    assert out["stage"] == "lock_held"


async def test_crashed_outcome_when_store_explodes(task_store, patch_redis,
                                                   test_settings, monkeypatch):
    """未预期异常也要有摘要——否则 admin 上只剩一个空白 success。"""
    async def _boom(*_a, **_kw):
        raise RuntimeError("db exploded")

    monkeypatch.setattr(execute.taskstore, "get", _boom)

    out = await execute.run(TASK)

    assert out["ok"] is False
    assert out["stage"] == "crashed"
    assert "RuntimeError" in out["fail_reason"]
    assert "db exploded" in out["fail_reason"]


# ---------------------------------------------------------------------------
# OutcomeMiddleware：把摘要翻译成 admin 的 Error 列
# ---------------------------------------------------------------------------


def _result(return_value, *, error=None) -> TaskiqResult:
    return TaskiqResult(is_err=error is not None, return_value=return_value,
                        execution_time=0.1, error=error)


@pytest.mark.parametrize("stage", ["upstream_error", "timeout", "crashed"])
async def test_middleware_sets_error_for_failed_outcome(stage) -> None:
    payload = Outcome(task_id="t1", stage=stage, ok=False,
                      detail=f"{stage} happened").as_dict()
    result = _result(payload)

    await OutcomeMiddleware().post_execute(None, result)  # type: ignore[arg-type]

    assert isinstance(result.error, TaskExecutionError)
    assert result.is_err is True
    assert f"{stage} happened" in repr(result.error)


async def test_middleware_leaves_successful_outcome_alone() -> None:
    result = _result(Outcome(task_id="t1", stage="success").as_dict())

    await OutcomeMiddleware().post_execute(None, result)  # type: ignore[arg-type]

    assert result.error is None
    assert result.is_err is False


async def test_middleware_preserves_real_exception() -> None:
    """真异常的堆栈信息比我们合成的摘要更有价值，绝不能覆盖。"""
    real = ValueError("the real cause")
    result = _result(None, error=real)

    await OutcomeMiddleware().post_execute(None, result)  # type: ignore[arg-type]

    assert result.error is real


@pytest.mark.parametrize("value", [None, "plain-string", 42, [], {"no_ok_key": 1}])
async def test_middleware_ignores_non_outcome_returns(value) -> None:
    """非摘要返回值（其它任务、旧版本 worker）不得触发误报。"""
    result = _result(value)

    await OutcomeMiddleware().post_execute(None, result)  # type: ignore[arg-type]

    assert result.error is None


async def test_middleware_tolerates_unknown_fields() -> None:
    """滚动升级窗口：新版 worker 写入的新字段不能让旧版 middleware 崩。"""
    payload = Outcome(task_id="t1", stage="timeout", ok=False).as_dict()
    payload["field_from_the_future"] = "surprise"
    result = _result(payload)

    await OutcomeMiddleware().post_execute(None, result)  # type: ignore[arg-type]

    assert isinstance(result.error, TaskExecutionError)


async def test_failed_outcome_does_not_trigger_redelivery(task_store, patch_redis,
                                                          test_settings, respx_router):
    """**最关键的不变式**：失败摘要只影响面板显示，绝不能让任务被重投。

    填 ``result.error`` 后若 taskiq 改变 ack 行为，就会重复调用上游——
    这正是派发锁存在的理由。这里验证：即便摘要标记失败，上游也只被调一次。
    """
    await _seed(task_store)
    route = respx_router.post(URL).mock(return_value=httpx.Response(500, json={"e": 1}))

    out = await execute.run(TASK)
    assert out["ok"] is False

    result = _result(out)
    await OutcomeMiddleware().post_execute(None, result)  # type: ignore[arg-type]
    assert result.error is not None

    # 模拟重投：锁仍在 → 跳过，绝不二次调用上游
    task_store.rows[TASK]["status"] = "QUEUED"
    again = await execute.run(TASK)

    assert again["stage"] == "lock_held"
    assert len(route.calls) == 1
