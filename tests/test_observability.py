"""logfire 上报链路门禁：体量闸门、SDK 环境映射、批次/管线默认值、停机 flush。

对应 2026-09-12 的口径：**不脱敏，但不上报大文件**。四类契约都必须机械守住，
因为它们**回退了功能上完全正常**——日志照打、trace 照出，没有任何功能性
用例会变红，只有在别人账单/配额爆掉或看板上出现一坨 base64 时才被发现：

1. 体量三层闸门（业务摘要 / SDK 属性上限 / 管道 body 闸门）；
2. 配置字段 → ``OTEL_*`` 环境变量的映射必须完整（漏一个 = 那层闸门静默失效）；
3. metrics 默认关、``inspect_arguments`` 关（前者省钱，后者是令牌红线）；
4. 停机 flush（不做则「最后几秒的日志在看板上缺席」）。

第 1 类里的 body 闸门用**真实 OTel 管道**验证（LoggerProvider + 内存导出器），
不是只断言配置：这条闸门的第一版实现挂在 loguru sink 上，配置层看着全对，
端到端一跑才发现 body 里仍是原始的 30005 字符。
"""

from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import ValidationError

from app import observability
from app.config import Settings, settings

ROOT = Path(__file__).resolve().parents[1]


# ---------------------------------------------------------------------------
# 一、管道 body 体量闸门（SDK 属性上限管不到的那一层）
# ---------------------------------------------------------------------------


def _emit_through_real_pipeline(message: str, limit: int) -> str:
    """把一条日志走真实 OTel 管道（cap 处理器 → 内存导出器），返回导出的 body。

    用真管道而不是直接调 ``on_emit``：要一并验证处理器**被排在导出器之前**、
    **就地改的是导出用的那个对象**。

    走 ``provider.get_logger().emit(LogRecord(...))`` 而不走已废弃的
    ``opentelemetry.sdk._logs.LoggingHandler``（它会打 DeprecationWarning）。
    """
    from opentelemetry._logs import LogRecord
    from opentelemetry.sdk._logs import LoggerProvider
    from opentelemetry.sdk._logs.export import (
        InMemoryLogRecordExporter,
        SimpleLogRecordProcessor,
    )

    provider = LoggerProvider()
    exporter = InMemoryLogRecordExporter()
    provider.add_log_record_processor(observability._BodyCapProcessor(limit))
    provider.add_log_record_processor(SimpleLogRecordProcessor(exporter))

    provider.get_logger("stask-cap-probe").emit(
        LogRecord(severity_text="WARNING", body=message),
    )

    finished = exporter.get_finished_logs()
    assert len(finished) == 1, "管道没把日志交给导出器"
    return str(finished[0].log_record.body)


def test_body_cap_truncates_oversized_body_and_marks_the_real_length():
    """超限截断且标注原始长度：一眼能看出被裁过、裁了多少。"""
    body = _emit_through_real_pipeline("x" * 5_000, limit=100)

    assert body.startswith("x" * 100)
    assert "truncated, 5000 chars total" in body
    assert len(body) < 200


def test_body_cap_leaves_normal_body_untouched():
    """正常体量必须原样透传（不得出现任何截断标记）。"""
    body = _emit_through_real_pipeline("task success: task_id=t-1", limit=16_000)

    assert body == "task success: task_id=t-1"
    assert "truncated" not in body


def test_body_cap_processor_is_a_real_log_record_processor():
    """必须是 ``LogRecordProcessor`` 子类：logfire 靠它把处理器挂进管道。"""
    from opentelemetry.sdk._logs import LogRecordProcessor

    assert issubclass(observability._BodyCapProcessor, LogRecordProcessor)
    proc = observability._BodyCapProcessor(123)
    assert proc.force_flush() is True
    proc.shutdown()


# ---------------------------------------------------------------------------
# 二、配置 → OTel 环境变量映射（漏一个 = 那层闸门静默失效）
# ---------------------------------------------------------------------------


def test_sdk_env_mapping_names_real_settings_fields():
    """映射表里的字段名必须真实存在（改名/删字段时立刻报错）。"""
    fields = set(Settings.model_fields)
    unknown = set(observability._SDK_ENV.values()) - fields
    assert not unknown, f"映射表引用了不存在的配置字段：{unknown}"


def test_apply_sdk_env_writes_configured_values(monkeypatch):
    """默认值必须真的落到 SDK 读的那些 env 上。"""
    import os

    monkeypatch.setattr(settings, "logfire_log_queue_size", 1234)
    monkeypatch.setattr(settings, "logfire_span_schedule_delay_ms", 4321)
    for env_name in observability._SDK_ENV:
        monkeypatch.delenv(env_name, raising=False)

    observability._apply_sdk_env()

    assert os.environ["OTEL_BLRP_MAX_QUEUE_SIZE"] == "1234"
    assert os.environ["OTEL_BSP_SCHEDULE_DELAY"] == "4321"


def test_apply_sdk_env_does_not_override_explicit_env(monkeypatch):
    """运维显式设的同名变量优先——出事时要求能「只改 env、不改镜像」。"""
    import os

    monkeypatch.setenv("OTEL_BLRP_MAX_QUEUE_SIZE", "9999")
    monkeypatch.setattr(settings, "logfire_log_queue_size", 1234)

    observability._apply_sdk_env()

    assert os.environ["OTEL_BLRP_MAX_QUEUE_SIZE"] == "9999"


def test_zero_limits_are_rejected_at_startup():
    """``0`` 会让 SDK 静默退化成「什么都不上报」，故启动即拒绝。

    （与「写坏即报错」同哲学：静默失效比启动失败难查得多。）
    """
    with pytest.raises(ValidationError, match="logfire_log_queue_size"):
        Settings(logfire_log_queue_size=0)
    with pytest.raises(ValidationError, match="logfire_log_attribute_value_limit"):
        Settings(logfire_log_attribute_value_limit=0)


# ---------------------------------------------------------------------------
# 三、装配（metrics 管线 / 参数快照 / body 闸门接线）
# ---------------------------------------------------------------------------


class _Recorder:
    """假 instrumentor：记录是否被调用，不碰真实 broker。"""

    def __init__(self) -> None:
        self.instrumented = False

    def instrument(self) -> None:
        self.instrumented = True

    def instrument_broker(self, _broker: object) -> None:
        return None


def test_setup_wires_the_caps_and_keeps_optional_pipelines_off(monkeypatch):
    """装配参数门禁（一次覆盖四条默认值）。

    - ``metrics=False``：本服务没有 metrics 消费方，白养一条导出管线；
    - ``inspect_arguments=False``：**红线**——默认在 py3.11+ 是 True，
      一旦有人用 ``@logfire.instrument`` 装饰带 token 的函数，参数快照
      就会连着 raw_token 一起上报；
    - ``advanced.log_record_processors`` 必须含 body 闸门，且上限取自配置
      （漏了就是「大文件照样上报」，而日志一切正常）。
    """
    import logfire
    import taskiq.instrumentation as taskiq_inst

    captured: dict = {}

    def fake_configure(**kwargs: object) -> None:
        captured.update(kwargs)

    monkeypatch.setattr(settings, "logfire_enabled", True)
    monkeypatch.setattr(observability, "_configured", False)
    monkeypatch.setattr(logfire, "configure", fake_configure)
    monkeypatch.setattr(logfire, "instrument_httpx", lambda: None)
    monkeypatch.setattr(taskiq_inst, "TaskiqInstrumentor", _Recorder)

    try:
        observability.setup("test")
    finally:
        observability._configured = False
        from app.logging import setup_logging

        setup_logging()          # logger.remove() 是全局状态，跑完复原

    assert captured["metrics"] is False
    assert captured["inspect_arguments"] is False
    assert captured["console"] is False
    assert captured["service_name"] == "stask-test"

    processors = captured["advanced"].log_record_processors
    caps = [p for p in processors if isinstance(p, observability._BodyCapProcessor)]
    assert len(caps) == 1, "body 闸门没接进 logfire 的日志管道"
    assert caps[0]._limit == settings.logfire_log_char_limit


# ---------------------------------------------------------------------------
# 四、停机 flush
# ---------------------------------------------------------------------------


def test_flush_is_a_noop_when_logfire_is_off(monkeypatch):
    """未启用时必须是纯 no-op：调用方（lifespan / broker.shutdown）不必判断开关。"""
    import logfire

    def boom(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("未启用 logfire 时不该碰 force_flush")

    monkeypatch.setattr(observability, "_configured", False)
    monkeypatch.setattr(logfire, "force_flush", boom)

    observability.flush()


def test_flush_forces_the_exporter_when_configured(monkeypatch):
    """启用后必须真的 flush：否则最后一批（最多 5s）随进程一起消失。"""
    import logfire

    called: list[bool] = []
    monkeypatch.setattr(observability, "_configured", True)
    monkeypatch.setattr(logfire, "force_flush", lambda: called.append(True))

    observability.flush()

    assert called == [True]


def test_shutdown_paths_call_flush():
    """收尾点门禁：web 的 lifespan 与 worker 的 broker.shutdown 都要 flush。

    这两个调用点删掉后**没有任何症状**——直到某天发现「停机前最后几秒的
    日志在 logfire 上没有」。
    """
    lifespan_src = (ROOT / "app" / "main.py").read_text("utf-8")
    queue_src = (ROOT / "app" / "queue.py").read_text("utf-8")
    assert "flush_observability()" in lifespan_src
    assert "async def shutdown" in queue_src
    assert "flush_observability()" in queue_src
