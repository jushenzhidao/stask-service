"""配置单例（pydantic-settings，**无 env 前缀**）。

环境变量名 = 字段名的大写形式（``max_slots`` → ``MAX_SLOTS``）。没有前缀——
本服务的键名本身已经够独特（``sk_session_ttl_seconds`` / ``queue_stream_maxlen``），
前缀防不住任何真实碰撞，却让每一处文档、每一条启动命令都要多敲几个字符。
``gunicorn.conf.py`` 直读 os.environ，用的也是这套无前缀名，改名要同步。

**不保留任何历史别名**：旧名字一旦留下，新旧两套都要维护，而旧的那套永远
测不到。改名就改到底——没有兼容层，也就没有"看起来兼容、实际静默回落"这回事。

纪律：
- 业务模块一律 ``from app.config import settings``，**禁止散读 os.environ**；
  唯一例外是 ``gunicorn.conf.py``（master 进程早于 app 加载）。
- 序列/映射型字段用 ``NoDecode`` + ``field_validator(mode="before")`` 手工解析，
  兼容「逗号分隔」与「JSON 数组」两种写法（运维手写 .env 时不必记 JSON 语法）。
- 测试重载：``get_settings.cache_clear()``；单测更常用
  ``monkeypatch.setattr(settings, "xxx", ...)`` 直接改单例字段。

定位：通用 HTTP 异步任务服务。本服务**不做计费、不做 key 管理**，
Authorization 只透传给上游，有效性由上游判定。tasks 表复用 new-api 实例，
靠 ``platform`` 列划分自有行（ADR-001 / ADR-006）。
"""

from __future__ import annotations

import json
import re
from functools import lru_cache
from typing import Annotated, Literal
from urllib.parse import quote, urlencode

from pydantic import AliasChoices, Field, field_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict

#: 本服务只用 asyncmy，缺了它 SQLAlchemy 会用默认的同步驱动
_ASYNC_SCHEME = "mysql+asyncmy://"
#: new-api（上游）的 ``SQL_DSN`` 是 Go 风格：``user:pass@tcp(host:3306)/db?params``。
#: 以 ``@tcp(`` 为锚点切，而不是「第一个 @」——密码里带 @ 是常态。
_GO_MARKER = "@tcp("
#: 混种写法里残留的 Go 风格 host 片段：``...pwd@tcp(host:3306)/db``
_TCP_FRAGMENT = re.compile(r"@tcp\((?P<host>[^:)]+)(?::(?P<port>\d+))?\)")
#: Go 驱动的参数 asyncmy 不认，放进去会在 connect 时抛未知 kwargs
_KEEP_QUERY = ("charset", "collation", "unix_socket")


def _keep_query(raw: str | None) -> str:
    """只留 asyncmy 认得的查询参数，默认补 ``charset=utf8mb4``。"""
    kept: dict[str, str] = {}
    for part in (raw or "").lstrip("?").split("&"):
        if "=" in part:
            key, _, value = part.partition("=")
            if key.lower() in _KEEP_QUERY:
                kept[key.lower()] = value
    kept.setdefault("charset", "utf8mb4")
    return "?" + urlencode(kept)


def normalize_database_url(value: str) -> str:
    """把各种写法统一成 asyncmy 的 SQLAlchemy URL。

    支持三种输入，都是真实踩过的坑：

    1. **new-api 的 ``SQL_DSN``**（Go DSN）：``root:pwd@tcp(127.0.0.1:3306)/newapi``
       ——无 scheme，逐段解析并对账号密码做百分号编码（密码里有 ``@`` ``/``
       不会把 host 解析带歪）；Go 专有参数（``parseTime`` / ``loc`` / ...）丢弃。
    2. **混种**：``mysql+asyncmy://root:pwd@tcp(host:3306)/db??charset=utf8mb4``
       ——SQLAlchemy 的壳 + Go 的 host 写法 + 重复问号。SQLAlchemy 遇到它会直接抛
       ``ValueError: invalid literal for int(): '3306)'``，服务起不来。
    3. 标准写法照原样返回；``mysql://`` 补成 ``mysql+asyncmy://``。
    """
    raw = (value or "").strip()
    if not raw:
        return raw

    if "://" not in raw and (marker := raw.find(_GO_MARKER)) >= 0:
        userinfo, _, tail = raw[:marker], "", raw[marker + len(_GO_MARKER):]
        hostport, close, remainder = tail.partition(")/")
        if close and remainder:
            host, _, port = hostport.partition(":")
            db, _, query = remainder.partition("?")
            user, _, password = userinfo.partition(":")
            secret = ""
            if user or password:
                secret = f"{quote(user, safe='')}:{quote(password, safe='')}@"
            return (
                f"{_ASYNC_SCHEME}{secret}{host}:{port or '3306'}"
                f"/{db.strip('/')}{_keep_query(query)}"
            )
        return f"{_ASYNC_SCHEME}{raw}"
    if "://" not in raw:
        return f"{_ASYNC_SCHEME}{raw}"

    fixed = re.sub(r"\?\?+", "?", raw)
    fixed = _TCP_FRAGMENT.sub(
        lambda m: f"@{m.group('host')}:{m.group('port') or 3306}", fixed
    )
    if fixed.startswith("mysql://"):
        fixed = _ASYNC_SCHEME + fixed[len("mysql://"):]
    return fixed


def _parse_str_tuple(value: object) -> object:
    """``"a,b,c"`` 或 ``'["a","b"]'`` → ``("a", "b", "c")``；已是序列则原样。"""
    if value is None or isinstance(value, list | tuple):
        return tuple(value or ())
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return ()
        if raw.startswith("["):
            try:
                return tuple(str(x).strip() for x in json.loads(raw) if str(x).strip())
            except (ValueError, TypeError):
                pass
        return tuple(part.strip() for part in raw.split(",") if part.strip())
    return value


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # ---- 应用 ----
    app_env: str = "dev"
    app_version: str = "0.2.0"
    log_level: str = "INFO"

    #: 管理面密钥（看板 + 动态配置写入）。**空 = 整个管理面 404**，
    #: 默认不开启，忘配密钥不等于裸奔。与终端用户 sk 完全分离——
    #: 用 sk 鉴权会让任何普通用户都能改全局配置（提权）。
    admin_key: str = ""

    # ---- 数据层（与 new-api 共享 MySQL 实例，零建表）----
    #: 也认 new-api 的 ``SQL_DSN``（Go 格式 ``root:pwd@tcp(host:3306)/db``）——
    #: 两边共用同一个库，直接复用上游那一份，省掉一处要同步的配置。
    #: 格式转换见 ``normalize_database_url``。
    database_url: str = Field(
        default="mysql+asyncmy://root:root@127.0.0.1:3306/newapi?charset=utf8mb4",
        validation_alias=AliasChoices("DATABASE_URL", "SQL_DSN"),
    )
    db_pool_size: int = 20
    db_max_overflow: int = 10
    db_pool_recycle: int = 1800          # 必须 < MySQL wait_timeout
    db_pool_pre_ping: bool = True

    #: tasks 表 platform 列取值——本服务自有行的唯一标识（写侧 WHERE 必带）。
    #: 必须避开 new-api 内置平台值（suno/mj/...），使 GetTaskAdaptorFunc
    #: 返回 nil、原生任务轮询天然跳过本服务的行（ADR-006）。
    gateway_platform: str = "stask"

    #: tasks 表 channel_id 列取值——外部任务专用的独立渠道号（ADR-006）。
    #: **必须是 new-api 中真实存在的渠道 id**（禁用状态也行，缓存含禁用渠道）。
    #: 原因：new-api 轮询 updateVideoTasks 里 CacheGetChannel 在 adaptor nil
    #: 检查**之前**执行——渠道不存在时该渠道下全部任务会被无 CAS 批量强制
    #: FAILURE（"Failed to get channel info"），adaptor 为 nil 救不了。
    #: 0 = 未配置（启动时打告警；若上游轮询开启，在途任务会被误杀）。
    channel_id: int = 0

    # ---- Redis（独立实例，ADR-004）----
    redis_url: str = "redis://127.0.0.1:6381/0"
    #: 全部键的统一前缀（即便误连别家实例也不会撞键）
    redis_key_prefix: str = "st"

    # ---- 上游寻址（§7）----
    #: 默认 upstream（请求未带 X-Upstream-Base-Url 头时的回落值）。
    #: 上游对本项目而言**只是一个 HTTP 服务**：new-api 是默认实现，换任何
    #: 同步生成接口只要加进 allowlist 即可，本服务不感知它是谁。
    upstream_base_url: str = "http://127.0.0.1:3000"
    #: 允许的 upstream host 白名单（含端口按 host:port 比对；不含端口只比 host）。
    #: **空 = 不限制**，与 ``callback_allowlist`` 同一套语义：不配即放行，配了
    #: 才按条目卡。默认值必须是空而不是"几个常见地址"——后者是隐藏配置，
    #: 没显式配过的人会以为自己在放行，实际被一份看不见的名单挡着。
    #: 放行时 ``X-Upstream-Base-Url`` 头能决定令牌发往哪里，只在反向代理
    #: 无条件覆盖该头（见 deploy/nginx.conf）时才安全；启动时会打 warning。
    upstream_allowlist: Annotated[tuple[str, ...], NoDecode] = ()

    # ---- 路径准入（§2）----
    async_allow_prefixes: Annotated[tuple[str, ...], NoDecode] = (
        "/v1/images", "/v1/audio", "/v1/videos",
    )
    #: 硬拒前缀（deny 优先于 allow）：管理面绝不允许被任务化转发
    async_deny_prefixes: Annotated[tuple[str, ...], NoDecode] = ("/api/", "/console/")

    # ---- 并发保护（本服务不做资金判定，纯固定闸门）----
    max_slots: int = 10                  # 单 token 在途上限
    slot_ttl_seconds: int = 3600         # 槽键 TTL 兜底（进程崩溃不永久泄漏）

    # ---- 限流 ----
    rate_limit: int = 60                 # 每窗口提交次数
    rate_limit_window_seconds: int = 60

    # ---- 鉴权 ----
    #: 鉴权模式：
    #: - ``generic``（默认）：通用上游——不做提交前鉴权/余额预检，
    #:   Authorization 原样透传，任务有效性由上游在执行时判定
    #:   （无效 key = 上游 401 = 任务 FAILURE 回放），user_id 落 0；
    #: - ``newapi``：上游是 new-api 且与本服务共库——提交前直查
    #:   tokens ⋈ users 做鉴权+余额预检（401/402 不建任务），user_id 落表。
    #: 用 Literal 而非 str：拼错（如 ``new-api``）会在启动时报错，
    #: 而不是静默回落到 generic —— 那等于悄悄关掉了鉴权。
    auth_mode: Literal["generic", "newapi"] = "generic"
    #: 鉴权正向结果的 Redis 缓存 TTL（秒，仅 newapi 模式）。窗口内 key
    #: 被禁用/余额耗尽仍可提交，但任务执行时会被上游 relay 拒绝。
    auth_cache_ttl_seconds: int = 300

    # ---- worker 执行（§5、§8）----
    worker_timeout: int = 120            # 上游调用超时秒
    retry_max: int = 0                   # 5xx 重试次数（上游可能有副作用，默认不重试）
    #: 连接层错误（请求未到达上游，重试零副作用）的独立重试次数
    retry_max_connect: int = 2
    retry_backoff_base: float = 1.0
    dispatch_lock_margin_seconds: int = 30   # 派发锁 TTL = worker_timeout + margin
    queue_concurrency: int = 64          # worker 并发度（taskiq --max-async-tasks）
    sk_session_ttl_seconds: int = 7200   # 令牌会话 TTL（2h，终态即清）

    # ---- 提交/响应体上限 ----
    body_max_bytes: int = 2 * 1024 * 1024        # 提交体落库上限（2MB）
    response_max_bytes: int = 10 * 1024 * 1024   # 响应体落库上限（10MB）

    # ---- 查询长轮询（§2）----
    poll_wait_max_seconds: int = 60      # ?wait= 的上限（须 < nginx proxy_read_timeout）
    poll_interval_seconds: float = 0.5

    # ---- 卡死任务收敛（§8）----
    #: 每轮兜底扫描条数
    sweep_batch_limit: int = 200
    #: 任务最大生命期（秒）：超过即判死 FAILURE。必须 < new-api 的 24h
    #: 超时清理线（否则会被上游 sweepTimedOutTasks 抢先动我们的行）。
    task_max_lifetime_seconds: int = 6 * 3600

    # ---- 结果存储（§9）----
    result_ttl_seconds: int = 86400      # 结果保留秒，到期清空 upstream_response
    result_purge_batch_limit: int = 200

    # ---- 回调 ----
    callback_secret: str = ""            # HMAC-SHA256 签名密钥（空 = 不签名）
    callback_timeout: float = 10.0
    callback_max_attempts: int = 5
    #: 回调 URL 的 host 白名单（空 = 不限制；生产强烈建议配置）
    callback_allowlist: Annotated[tuple[str, ...], NoDecode] = ()

    # ---- 队列（Redis Stream，at-least-once）----
    #: 流的近似最大长度（XADD MAXLEN ~）。已 ack 的条目 XACK 并不会删除，
    #: 必须 trim 防无界增长。取值要 >> 峰值积压——trim 是从最旧端裁剪，
    #: 积压超过它才可能裁到未处理消息。0 = 不裁剪（不建议，内存无界）。
    queue_stream_maxlen: int = 100_000

    # ---- 可观测性（logfire，可选）----
    #: 1 = 启用 logfire（trace + metrics + loguru 桥接）。
    #: 凭证走 logfire 自己的 LOGFIRE_TOKEN 环境变量；
    #: 未配 token 时 send_to_logfire="if-token-present" 会静默降级为不发送。
    #: service_name 固定 ``stask-web`` / ``stask-worker``，不做配置项。
    logfire_enabled: bool = False

    # ---- taskiq-admin 任务看板（必选）----
    #: taskiq-admin 实例地址（compose 里写死 http://taskiq-admin:3000）。
    #: 与 ``taskiq_admin_api_token`` **都非空**才挂 middleware；空 = 不上报。
    taskiq_admin_url: str = ""
    taskiq_admin_api_token: str = ""

    # ---- 定时任务开关 ----
    sweep_enabled: bool = True

    @field_validator(
        "upstream_allowlist", "async_allow_prefixes",
        "async_deny_prefixes", "callback_allowlist",
        mode="before",
    )
    @classmethod
    def _tuple_fields(cls, value: object) -> object:
        return _parse_str_tuple(value)

    @field_validator("database_url", mode="before")
    @classmethod
    def _normalize_database_url(cls, value: object) -> object:
        return normalize_database_url(value) if isinstance(value, str) else value


@lru_cache
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
