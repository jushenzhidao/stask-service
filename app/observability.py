"""可观测性装配单点（logfire / OpenTelemetry，可选启用）。

设计：
- ``ST_LOGFIRE_ENABLED=1`` 才生效；默认关闭，零开销、零新依赖初始化。
- 凭证走 logfire 官方的 ``LOGFIRE_TOKEN`` 环境变量（**无 ST_ 前缀**，
  这是 logfire SDK 自己读的）；``send_to_logfire="if-token-present"``
  保证没配 token 时静默降级——本地/单测不需要任何额外配置。
- ``setup()`` 幂等且按进程调用：web 在 ``create_app``、worker 在
  ``ObservabilityMiddleware.startup``。standalone 两者同进程，先到先配。
- taskiq 任务链路的 trace **不在这里**：由 ``app.queue`` 挂
  ``OpenTelemetryMiddleware``（taskiq 0.12 内置）。它通过 message labels
  注入/提取 W3C traceparent——web 侧 kiq 的 PRODUCER span 与 worker 侧
  execute 的 CONSUMER span 自动串成一条 trace。middleware 拿的是全局
  ProxyTracer，logfire.configure 晚于 middleware 构造也能正确接上。

安全纪律（与 app.logging 一致）：
- loguru → logfire 桥接只转发 message 与显式字段，loguru 本身
  ``diagnose=False`` 已保证异常回溯不带局部变量，raw token 不会外泄；
- ``instrument_httpx()`` 默认**不采集**请求/响应体与 headers，
  只记方法/URL/状态码——绝不能开 capture_headers（Authorization 透传）。
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
        service_name=f"{settings.logfire_service_name}-{component}",
        service_version=settings.app_version,
        send_to_logfire="if-token-present",
        environment=settings.app_env,
        console=False,          # 控制台输出仍由 loguru 负责，不重复打
    )
    # 上游调用 / 回调推送的出站 HTTP span（不采集 headers/body——含令牌）
    logfire.instrument_httpx()

    _configured = True
    # loguru → logfire：业务日志作为 log record 汇入同一条 trace。
    # 此后每次 setup_logging()（它以 logger.remove() 开头）都会按
    # is_configured() 重挂本 sink。
    from app.logging import log

    log.add(**logfire.loguru_handler())


def instrument_fastapi(app: object) -> None:
    """web 进程专用：FastAPI 请求级 span。须在 ``setup("web")`` 之后调用。"""
    if not settings.logfire_enabled:
        return
    import logfire

    logfire.instrument_fastapi(app)  # type: ignore[arg-type]
