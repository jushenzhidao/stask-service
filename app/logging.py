"""日志装配：loguru 统一入口（web 与 taskiq worker 进程共用）。

- 业务模块一律 ``from app.logging import log``；
- ``setup_logging()`` 在 web（``app.main.create_app``）与 worker
  （``app.queue.ObservabilityMiddleware.startup``）入口各调用一次；
- 级别由 ``LOG_LEVEL`` 控制。

安全纪律（红线：用户 sk 不进日志）：
``backtrace=False, diagnose=False``——异常回溯绝不带帧局部变量值。
worker 任务参数里就有 raw_token（从令牌会话取出后传给上游调用），
开 diagnose 会把它直接打进日志。业务日志只打 task_id / user_id /
token_hash / 状态码，绝不打 raw token 与响应体。唯一豁免：worker 失败
路径的上游**错误响应体预览**（``execute._error_preview``，截断 300 字符
且原文本就落库可见），错误页/错误 JSON 是排障关键线索且不含令牌。

结构化口径：给 logfire 用的维度字段（task_id / status / fail_reason /
model 等）一律 ``log.bind(...)`` 挂 extra——logfire 的 loguru 桥接会把
extra 转成顶层可检索属性；位置 ``{}`` 参数只会落进 logfire.logging_args
数组，无法按字段过滤。
"""

from __future__ import annotations

import logging
import sys
from types import FrameType

from loguru import logger

from app.config import settings

#: 业务模块统一入口
log = logger

_FORMAT = (
    "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
    "<level>{level: <8}</level> | "
    "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - "
    "<level>{message}</level>"
)


class InterceptHandler(logging.Handler):
    """stdlib logging → loguru 桥接（uvicorn/sqlalchemy/taskiq 收编为同一格式）。"""

    def emit(self, record: logging.LogRecord) -> None:
        try:
            level: str | int = logger.level(record.levelname).name
        except ValueError:
            level = record.levelno
        # depth=2：跳过 logging 帧，让 loguru 记录真实调用位置
        frame: FrameType | None = logging.currentframe()
        depth = 2
        while frame and frame.f_code.co_filename == logging.__file__:
            frame = frame.f_back
            depth += 1
        logger.opt(depth=depth, exception=record.exc_info).log(level, record.getMessage())


def setup_logging() -> None:
    """装配 loguru（幂等）：stderr sink + stdlib 桥接 + logfire 桥接（若已配）。

    logfire sink 在这里挂而不在 ``app.observability``：本函数以
    ``logger.remove()`` 开头，任何一次重装配（standalone 下 web 与 worker
    先后各调一次）都会冲掉旧 sink——桥接必须跟着每次装配重挂。
    """
    logger.remove()
    logger.add(
        sys.stderr,
        level=settings.log_level.upper(),
        format=_FORMAT,
        backtrace=False,
        diagnose=False,
    )
    from app import observability

    if observability.is_configured():
        import logfire

        # level 必须显式给：logfire.loguru_handler() 只返回
        # ``{'sink':…, 'format':'{message}'}``，**不带 level**，而 loguru 的
        # ``add()`` 默认 level=0 —— 等于把 DEBUG 全量送去 logfire。实测后果：
        # redis-py 每次建连的 ``Failed to enable maintenance notifications``
        # （DEBUG）会出现在 logfire 看板上，而 stderr 那边被 LOG_LEVEL 挡着
        # 看不见，形成"同一行日志两个地方不一样"的错觉。
        # 对齐 stderr sink 的级别：要 DEBUG 就一起调 LOG_LEVEL。
        logger.add(**logfire.loguru_handler(), level=settings.log_level.upper())
    logging.basicConfig(handlers=[InterceptHandler()], level=0, force=True)

    logging.getLogger("httpcore").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.INFO)
    logging.getLogger("taskiq").setLevel(logging.WARNING)
    # redis-py 8.x：协议默认 RESP3（utils.DEFAULT_RESP_VERSION = 3），于是每次
    # 新建连接都会尝试 ``CLIENT MAINT_NOTIFICATIONS ON``；redis:7 不认识这个
    # 子命令 → 抛 ResponseError → 以 DEBUG 记一行"失败但不影响连接"。
    # 功能无害，但会在看板上伪装成故障，直接压到 WARNING。
    logging.getLogger("redis").setLevel(logging.WARNING)
