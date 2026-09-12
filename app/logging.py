"""日志装配：loguru 统一入口（web 与 taskiq worker 进程共用）。

- 业务模块一律 ``from app.logging import log``；
- ``setup_logging()`` 在 web（``app.main.create_app``）与 worker
  （``app.queue.ObservabilityMiddleware.startup``）入口各调用一次；
- 级别由 ``LOG_LEVEL`` 控制，**默认 DEBUG（全量）**；WARNING 是硬地板，
  见 ``_effective_level``。

**内容口径（2026-09-12 定）：不脱敏，只截断。**

内部审计用途，故不做字段级脱敏——带签名的 URL、上游错误页、请求/响应体
都原样进日志（``execute._digest`` 只按长度截断，并保留 JSON 结构）。
事实源仍是 ``tasks.data``（原文落库），日志承担「一条 trace 里看懂发了什么、
回来什么」的职责。代价是日志量与敏感面上升，这是明确接受的取舍。

**体量三层闸门**（口径同上：不脱敏，但不上报大文件）：① 本模块的调用方
（``execute._digest``）把体摘要压在 4KB；② ``observability._apply_sdk_env``
用 ``OTEL_*_ATTRIBUTE_VALUE_LENGTH_LIMIT`` 截断每个属性值；③ 导出管道里的
``observability._BodyCapProcessor`` 裁 body —— 属性上限管不到的那一层。

安全纪律（红线 AC-30：用户 sk 不进日志、不进库、不出响应）：
``backtrace=False, diagnose=False``——异常回溯绝不带帧局部变量值。
worker 任务参数里就有 raw_token（从令牌会话取出后传给上游调用），
开 diagnose 会把它直接打进日志。落库的 ``request_headers`` 在准入层已被
``admission.clean_headers`` 摘掉 Authorization/Cookie，worker 真正发出的
``Authorization`` 是当场注入的、只存在于内存变量里，**绝不进日志**
（``execute._log_headers`` 另有一道按头名兜底的防线）。

结构化口径：给 logfire 用的维度字段（task_id / status / fail_reason /
model 等）一律 ``log.bind(...)`` 挂 extra——logfire 的 loguru 桥接会把
extra 转成顶层可检索属性；位置 ``{}`` 参数只会落进 logfire.logging_args
数组，无法按字段过滤。注意 stderr 的 format 只打印 ``{message}``，
**extra 在终端不可见**，故给本地看的完整内容必须写进 message。
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

#: 级别硬地板：无论 ``LOG_LEVEL`` 调到多高（应急降噪），WARN 及以上仍必然
#: 出现在**本地终端**与 logfire 上。内部审计要求「告警不许只在远程或只在
#: 本地」，这条地板就是它的机械保证。
_LEVEL_FLOOR = "WARNING"


def _effective_level() -> str:
    """``LOG_LEVEL`` 与 ``_LEVEL_FLOOR`` 取「更啰嗦」的那个（级别数值更小者胜）。

    口径（2026-09-12 定）：

    - **本地 stderr 与 logfire 共用同一级别**，不存在「某级别只在其中一侧
      可见」的错位。历史上正是这种错位：logfire sink 默认 level=0 全量、
      stderr 被 ``LOG_LEVEL`` 挡着，于是 redis-py 那行 DEBUG 只在看板上
      出现，形成「同一行日志两个地方不一样」的错觉；
    - ``LOG_LEVEL=DEBUG``（默认）= 全量，两个 sink 都从 DEBUG 起收，
      ``debug全部落，本地+远程``；
    - 地板不可压：调到 ERROR/CRITICAL 时，降噪只降到 WARNING 为止。
    """
    try:
        want = logger.level(settings.log_level.upper())
    except ValueError:
        # env 手敲错（LOG_LEVEL=vrebose 之类）：按地板走，不让一次拼写错误
        # 把装配直接打崩（与 gunicorn.conf._env_int 的「错值回落默认」同哲学）
        return _LEVEL_FLOOR
    floor = logger.level(_LEVEL_FLOOR)
    return want.name if want.no <= floor.no else floor.name


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
    level = _effective_level()
    logger.remove()
    logger.add(
        sys.stderr,
        level=level,
        format=_FORMAT,
        backtrace=False,
        diagnose=False,
    )
    from app import observability

    if observability.is_configured():
        import logfire

        # level 必须显式给：loguru_handler() 只返回 ``{'sink':…, 'format':…}``，
        # **不带 level**，而 loguru 的 ``add()`` 默认 level=0 —— 等于绕过
        # ``_effective_level`` 把 DEBUG 无条件全量送去 logfire（本地反而被挡着，
        # 两侧不一致）。与 stderr sink 同源取级：要降噪就调 LOG_LEVEL，两边一起动。
        #
        # 这里**不**做体量截断：body 的长度闸门在导出管道里
        # （``observability._BodyCapProcessor``）——包 sink 只能裁
        # ``LogRecord.msg``，而 logfire 取的是 loguru 栈帧里的消息模板，裁不到。
        logger.add(**logfire.loguru_handler(), level=level)
    logging.basicConfig(handlers=[InterceptHandler()], level=0, force=True)

    logging.getLogger("httpcore").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.INFO)
    logging.getLogger("taskiq").setLevel(logging.WARNING)
    # redis-py 8.x：协议默认 RESP3（utils.DEFAULT_RESP_VERSION = 3），于是每次
    # 新建连接都会尝试 ``CLIENT MAINT_NOTIFICATIONS ON``；redis:7 不认识这个
    # 子命令 → 抛 ResponseError → 以 DEBUG 记一行"失败但不影响连接"。
    # 功能无害，但会在看板上伪装成故障，直接压到 WARNING。
    logging.getLogger("redis").setLevel(logging.WARNING)
    # 第三方库的 DEBUG 一律在源头掐掉：业务 DEBUG 全量放行的前提是
    # 「噪音不来自依赖」。sqlalchemy 的 echo 若被打开会打全量 SQL（含参数值），
    # 这里把引擎日志压到 WARNING，避免 DEBUG 档位把 SQL 洪流带进 logfire。
    logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)
