"""可观测性装配单点（logfire / OpenTelemetry，可选启用）。

设计：
- ``LOGFIRE_ENABLED=1`` 才生效；默认关闭，不初始化任何 SDK（本模块只导入
  类定义，import 无副作用）。
- 凭证走 logfire 官方的 ``LOGFIRE_TOKEN`` 环境变量；
  ``send_to_logfire="if-token-present"`` 保证没配 token 时静默降级——
  本地/单测不需要任何额外配置。
- ``setup()`` 幂等且按进程调用：web 在 ``create_app``、worker 在
  ``ObservabilityMiddleware.startup``。standalone 两者同进程，先到先配。
- taskiq 任务链路 trace 用官方 ``TaskiqInstrumentor``（taskiq 0.12 内置，
  logfire **没有** instrument_taskiq）：它把 OpenTelemetryMiddleware 插到
  broker middleware 链头部，经 message labels 注入/提取 W3C traceparent——
  web 侧 kiq 的 PRODUCER span 与 worker 侧 execute 的 CONSUMER span
  自动串成一条 trace。middleware 拿的是全局 ProxyTracer，
  logfire.configure 先于 instrument 调用即可正确接上。

安全纪律（与 app.logging 一致）：
- loguru → logfire 桥接只转发 message 与显式字段，loguru 本身
  ``diagnose=False`` 已保证异常回溯不带局部变量，raw token 不会外泄；
- ``instrument_httpx()`` 默认不采集请求/响应体与 headers，只记方法/URL/
  状态码——绝不能开 capture_headers（Authorization 会直接透传）；
- ``inspect_arguments=False``：见 ``setup()`` 内注释（防参数快照带出令牌）。

噪音控制：
- FastAPI 侧 ``excluded_urls`` 排除 ``/healthz``（liveness/readiness 探针
  每秒级轮询，全量采集只会淹没业务 trace）。该参数是**子串包含匹配**，
  业务路径都在 ``/async``/``/admin``/``/ops`` 下，不会误伤；
- taskiq-admin middleware 的上报走 httpx——instrument_httpx 会给它产
  span，但上报是低频 fire-and-forget，噪音可接受，不做排除。

上报体量（2026-09-12 定：**不脱敏，但不上报大文件**）：
内部审计口径下日志内容原样上报（含签名 URL，见 ``execute._digest``），
代价由「体量闸门」独立承担，分三层：

1. **业务侧**：``execute._digest`` 把请求/响应体摘要压在 4KB 内，且超
   256KB 的体根本不做 JSON 解析（i2v 的内联 b64 是唯一的规模问题）；
2. **SDK 侧**：``_apply_sdk_env`` 写 ``OTEL_*_ATTRIBUTE_VALUE_LENGTH_LIMIT``，
   OTel 会截断每个属性值（含数组元素的逐个截断）——故 ``logfire.msg`` 与
   ``logfire.logging_args`` 都被覆盖；
3. **管道侧**：``_BodyCapProcessor`` 在导出前裁 **body**。这层必须存在：
   OTel 的属性上限**不管 body**，而 loguru 桥接把「消息模板」放进 body，
   无位置参数的日志其模板就是整条消息。

第 3 层的必要性是实测出来的，不是推演：先在 loguru sink 上挂了一层消息
截断，端到端跑一遍发现 ``record.msg`` 确实裁到了 16K，但
``logfire.msg_template`` 仍带着**原始 30005 字符**进 body——因为模板是从
loguru 的栈帧里取的，与 ``LogRecord.msg`` 无关。所以闸门必须落在管道里，
也顺带覆盖了 stdlib 桥接与直接 ``logfire.info()`` 的调用。

批次参数（日志队列容量、span 滞留）与 metrics 管线开关同样在
``_apply_sdk_env`` / ``setup()`` 收敛；metrics 默认关闭（本服务无消费方）。
"""

from __future__ import annotations

import os
from typing import Any

from opentelemetry.sdk._logs import LogRecordProcessor, ReadWriteLogRecord

from app.config import settings

_configured = False

#: OTel SDK 环境变量 → ``Settings`` 字段名。
#:
#: 为什么不直接写死 env 而要绕一层配置：SDK 只在**构造导出器/处理器时**读这些
#: 变量（时机在 ``logfire.configure`` 内部，我们插不进去），只能借 env 传递；
#: 但把取值留在 ``config.py`` 才能有单一事实源，也才能被测试断言。
_SDK_ENV: dict[str, str] = {
    "OTEL_ATTRIBUTE_VALUE_LENGTH_LIMIT": "logfire_attribute_value_limit",
    "OTEL_LOGRECORD_ATTRIBUTE_VALUE_LENGTH_LIMIT": "logfire_log_attribute_value_limit",
    "OTEL_BLRP_MAX_QUEUE_SIZE": "logfire_log_queue_size",
    "OTEL_BSP_SCHEDULE_DELAY": "logfire_span_schedule_delay_ms",
}


def _apply_sdk_env() -> None:
    """把调优配置映射成 OTel SDK 环境变量（setdefault：显式 env 优先）。

    这是全仓**唯一**允许动 ``os.environ`` 的地方，且它是「写」不是「读」——
    项目禁止的是散读 env 取配置（绕开 Settings），这里是给第三方 SDK 递契约。

    用 ``setdefault`` 而不是赋值：运维在 compose / 宿主机上显式设的同名变量
    永远优先，出问题时可以不改镜像、只改 env 覆盖。
    """
    for env_name, field in _SDK_ENV.items():
        os.environ.setdefault(env_name, str(getattr(settings, field)))


class _BodyCapProcessor(LogRecordProcessor):
    """导出前把日志 **body** 裁到字符上限（截断不丢弃）。

    OTel 的 ``OTEL_*_ATTRIBUTE_VALUE_LENGTH_LIMIT`` 只约束属性（含数组元素
    逐个截断），不约束 body；而 loguru 桥接会把消息模板放进 body，无位置
    参数的日志其模板就是整条消息。故这是「不上报大文件」三层闸门里唯一
    管得到 body 的一层。

    截断而非丢弃：丢掉整条日志会变成「有故障但看板上什么都没有」，比截断
    更难排查；截断时带上原始字符数，一眼能看出被裁过。

    放管道里而不是包 loguru sink，还有一个结构上的理由：它覆盖所有来源
    （loguru 桥接 / stdlib 桥接 / 直接的 ``logfire.info()``），只有一份实现。
    """

    def __init__(self, limit: int) -> None:
        self._limit = limit

    def on_emit(self, log_record: ReadWriteLogRecord) -> None:
        body = log_record.log_record.body
        if isinstance(body, str) and len(body) > self._limit:
            log_record.log_record.body = (
                f"{body[:self._limit]}…<truncated, {len(body)} chars total>"
            )

    def shutdown(self) -> None:
        """无自有资源（不持有导出器），故无需处理。"""

    def force_flush(self, timeout_millis: int = 30_000) -> bool:
        """无内部缓冲，恒视为已刷完。"""
        return True


def is_configured() -> bool:
    """logfire 是否已装配（``app.logging.setup_logging`` 重挂桥接 sink 用）。"""
    return _configured


def setup(component: str) -> None:
    """装配 logfire（幂等）。``component`` 进 service_name，区分 web/worker。"""
    global _configured
    if _configured or not settings.logfire_enabled:
        return

    # 必须在 logfire.configure 之前：SDK 只在构造导出器/处理器时读这些 env。
    _apply_sdk_env()

    import logfire
    from logfire import AdvancedOptions

    logfire.configure(
        service_name=f"stask-{component}",
        service_version=settings.app_version,
        send_to_logfire="if-token-present",
        environment=settings.app_env,
        console=False,          # 控制台输出仍由 loguru 负责，不重复打
        # metrics 只接受 MetricsOptions 或 False：None = 用 SDK 默认（开），
        # False = 整条管线不建（连 PeriodicExportingMetricReader 线程都不起）。
        metrics=None if settings.logfire_metrics_enabled else False,
        # 不采集被 @logfire.instrument 装饰函数的**参数值**。本服务目前不用该
        # 装饰器，但一旦有人加上，参数快照会连同 raw_token 一起上报；默认在
        # py3.11+ 是 True，故显式关掉（与 backtrace/diagnose=False 同一条红线）。
        inspect_arguments=False,
        # body 体量闸门（属性那层由 OTEL_*_ATTRIBUTE_VALUE_LENGTH_LIMIT 管）。
        advanced=AdvancedOptions(
            log_record_processors=[_BodyCapProcessor(settings.logfire_log_char_limit)],
        ),
    )
    # 上游调用 / 回调推送的出站 HTTP span（不采集 headers/body——含令牌）
    logfire.instrument_httpx()

    # taskiq 链路：官方 instrumentor（logfire 无 instrument_taskiq）。
    # instrument() 只 hook **之后创建**的 broker；本服务的 broker 在
    # ``app.queue`` import 时就已构造，必须显式 instrument_broker
    # （幂等：内部有 _is_instrumented_by_opentelemetry 防重）。
    import sys

    from taskiq.instrumentation import TaskiqInstrumentor

    instrumentor = TaskiqInstrumentor()
    instrumentor.instrument()
    queue_mod = sys.modules.get("app.queue")
    if queue_mod is not None:
        instrumentor.instrument_broker(queue_mod.broker)

    _configured = True
    # loguru → logfire：业务日志作为 log record 汇入同一条 trace。
    # 此后每次 setup_logging()（它以 logger.remove() 开头）都会按
    # is_configured() 重挂本 sink。
    from app.logging import log

    log.add(**logfire.loguru_handler())


def flush() -> None:
    """立刻导出队列里的 span / log（进程退出前调用）。

    OTel 的批次处理器最多滞留 ``schedule_delay``（span 2s、log 5s），进程被
    SIGTERM 终止时队列里的记录会一起消失——表现为「停机前最后几秒的日志在
    logfire 上缺失」，而本地终端明明打过。web 的 lifespan 收尾与 worker 的
    ``broker.shutdown`` 各调一次。

    未启用 logfire 时是纯 no-op，调用方不必判断开关。
    """
    if not _configured:
        return
    import logfire

    logfire.force_flush()


def instrument_fastapi(app: Any) -> None:
    """web 进程专用：FastAPI 请求级 span。须在 ``setup("web")`` 之后调用。

    ``excluded_urls`` 过滤探针噪音（子串匹配，见模块 docstring）。
    """
    if not settings.logfire_enabled:
        return
    import logfire

    logfire.instrument_fastapi(app, excluded_urls="/healthz")
