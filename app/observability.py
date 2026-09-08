"""可观测性装配单点（logfire / OpenTelemetry，可选启用）。

设计：
- ``LOGFIRE_ENABLED=1`` 才生效；默认关闭，零开销、零新依赖初始化。
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
- ``instrument_httpx()`` 默认**不采集**请求/响应体与 headers，
  只记方法/URL/状态码——绝不能开 capture_headers（Authorization 透传）。

噪音控制：
- FastAPI 侧 ``excluded_urls`` 排除 ``/healthz``（liveness/readiness 探针
  每秒级轮询，全量采集只会淹没业务 trace）。该参数是**子串包含匹配**，
  业务路径都在 ``/async``/``/admin``/``/ops`` 下，不会误伤；
- taskiq-admin middleware 的上报走 httpx——instrument_httpx 会给它产
  span，但上报是低频 fire-and-forget，噪音可接受，不做排除。
"""

from __future__ import annotations

from app.config import settings

_configured = False


def is_configured() -> bool:
    """logfire 是否已装配（``app.logging.setup_logging`` 重挂桥接 sink 用）。"""
    return _configured


def setup(component: str) -> None:
    """装配 logfire（幂等）。``component`` 进 service_name，区分 web/worker。"""
    global _configured
    if _configured or not settings.logfire_enabled:
        return

    import logfire

    logfire.configure(
        service_name=f"stask-{component}",
        service_version=settings.app_version,
        send_to_logfire="if-token-present",
        environment=settings.app_env,
        console=False,          # 控制台输出仍由 loguru 负责，不重复打
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


def instrument_fastapi(app: object) -> None:
    """web 进程专用：FastAPI 请求级 span。须在 ``setup("web")`` 之后调用。

    ``excluded_urls`` 过滤探针噪音（子串匹配，见模块 docstring）。
    """
    if not settings.logfire_enabled:
        return
    import logfire

    logfire.instrument_fastapi(
        app,  # type: ignore[arg-type]
        excluded_urls="/healthz",
    )
