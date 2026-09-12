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

from app import __version__

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
    #: 服务版本。**默认值直接引用 `app/__init__.py` 的 `__version__`（唯一事实源）**，
    #: 不在此处写字面量——曾经 pyproject / 本文件 / 镜像 tag 各写一个数字，
    #: 于是 `/healthz`、OpenAPI `version`、logfire `service_version` 报的是哪个
    #: 全凭运气。仍可用 `APP_VERSION` env 覆盖（生产通常由镜像 tag 注入）。
    #: 约束由 `tests/test_spec_contract.py::test_service_version_has_single_source` 守护。
    app_version: str = __version__
    #: 日志级别（本地终端与 logfire **共用**）。默认 DEBUG = 全量，
    #: 内部审计口径下排查优先；WARNING 是硬地板——调到 ERROR 应急降噪时，
    #: WARN 及以上仍必然落两侧（见 ``app.logging._effective_level``）。
    log_level: str = "DEBUG"

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
    max_slots: int = 10                  # 单 token 在途上限（第三层闸门的最后回落值）
    slot_ttl_seconds: int = 3600         # 槽键 TTL 兜底（进程崩溃不永久泄漏）

    # ---- 攒批与模型策略（按模型控制下发节奏与并发上限）----
    #: 攒批总开关。关闭 = 所有模型收到即发（忽略策略里的 batch 声明）。
    #: 灰度起步与线上止血用：出问题时一个开关回到改造前行为。
    batch_enabled: bool = True
    #: 整批放行时的有界并发（与 sweeper 同量级，避免放行瞬间打满连接池）
    batch_release_concurrency: int = 8
    #: 放行时占不到槽的退避上限（秒）。指数退避：30/60/120/240/…封顶本值，
    #: 带 ±10% 抖动防整批同相重试形成惊群。
    batch_backoff_max_seconds: int = 300
    #: ``X-Batch-Wait`` 的上限（秒），同时是「客户端只给 N 不给 T」时的兜底等待
    #: （R-17/AC-57：客户端的 N 声明不能变成无限等待）。
    max_batch_wait_seconds: int = 300
    #: 攒批的**归组维度**（结构性开关，故只读 env 不放热改白名单）：
    #: - ``model``：按归一化模型名归组。**默认**，跨 token 合并——批次更大、
    #:   N 更容易触发、削峰效果更好，且语义简单（「同模型的一批」）。
    #: - ``token_model``：按 ``token_hash + model`` 归组，与并发维度严格对齐
    #:   （ARCH Q5 / PRD R-15 / AC-58 的原裁决口径）；代价是每个 token 各自
    #:   成批、批次显著变小，更依赖 T 触发兜底。
    #: 两者都不会改变「放行时各自占各自 token 的槽」这一事实，区别只在
    #: **谁和谁算同一批**。``X-Batch-Key`` 可逐请求覆盖本项。
    #:
    #: **2026-09-11 产品决定取 ``model``**：优先「简单 + 批次大」，
    #: 与 ARCH Q5 的原始裁决不同，属**有意的偏离**（已在 ARCH §6 Q5 注记录）。
    #: 改回 ``token_model`` 只需改这个值，无需动逻辑——但那会改变所有既有
    #: 接入方的批次数与放行节奏，属行为变更。
    batch_group_by: str = "model"
    #: 允许的最大延迟（秒）。PRD 默认 6h。
    #: **实际生效值是它与「令牌 TTL 容量」的较小值**（见 services/schedule）：
    #: 延迟 + 执行 + 余量 超过令牌会话 TTL 时，任务到点必然取不到令牌而判死，
    #: 所以那一段区间必须在提交时就拒掉，而不是放进来看它必然失败。
    max_delay_seconds: int = 21600
    #: 模型策略表（json）。键 = 模型名（小写）/ 端点前缀 / ``__default__``；
    #: 字段 batch / batch_wait / limit_per_token / limit_global。
    #: 默认空 = 不启用任何策略，行为与改造前逐字节一致。
    #: 通常不写 env，在管理看板上热改。
    model_policies: dict[str, dict[str, int]] = Field(default_factory=dict)

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
    #: 令牌会话 TTL（终态即清）。**必须 > task_max_lifetime_seconds（6h）**：
    #: 否则任务还在生命期内、令牌却已过期，重投/长排队后执行必然取不到令牌
    #: （token_missing）判死，`task_max_lifetime_seconds` 形同虚设。
    #: 7h = 6h + 1h 余量。它同时是**延迟上限的天花板**（见 services/schedule）：
    #: 延迟 D + 执行 + 余量 ≤ 本值，所以 7h 给出约 6.8h 的延迟容量。
    sk_session_ttl_seconds: int = 25200

    # ---- 提交/响应体上限 ----
    body_max_bytes: int = 2 * 1024 * 1024        # 提交体落库上限（2MB）
    response_max_bytes: int = 10 * 1024 * 1024   # 响应体落库上限（10MB）
    #: 明文落库的体量上限（字节）。≤ 本值且是合法 UTF-8 的请求体/响应体
    #: **原样存明文**，超过或含二进制字节才 gzip+base64（见 services/codec）。
    #: 默认 32KB：覆盖绝大多数生成类接口的 JSON 响应（URL + 元数据），
    #: 让 `SELECT data ->> '$.upstream_response'` 直接可读，排障不必解码。
    plain_max_bytes: int = 32 * 1024

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
    #: HMAC-SHA256 签名密钥。空 = 回调不带 ``X-Stask-Signature`` 头发出，
    #: 而 SPEC AC-29 要求「必须推送签名」——所以**严格环境下空值会阻断启动**
    #: （见 `main._check_callback_secret`），宽松环境只告警。生成：openssl rand -base64 32
    callback_secret: str = ""
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
    #: 1 = 启用 logfire（trace + loguru 日志汇入；metrics 默认关，见下）。
    #: 凭证走 logfire 自己的 LOGFIRE_TOKEN 环境变量；
    #: 未配 token 时 send_to_logfire="if-token-present" 会静默降级为不发送。
    #: service_name 固定 ``stask-web`` / ``stask-worker``，不做配置项。
    logfire_enabled: bool = False

    # ---- logfire 上报调优 ----
    # 下面这些**不是**给业务代码读的，而是由 ``observability._apply_sdk_env``
    # 映射成 OTel SDK 的环境变量（SDK 只在构造导出器时读 env，故必须在
    # ``logfire.configure`` 之前写好）。做成配置项而不是直接写死 env，是为了
    # 单一事实源 + 可被测试断言 —— 直接写 `os.environ` 的常量没法测。
    #
    #: 1 = 上报 OpenTelemetry metrics（FastAPI/httpx 请求直方图 + SDK 自省指标）。
    #: 默认 0：本服务**没有任何 metrics 消费方**（管理看板的数字全走 DB 聚合
    #: 查询，见 admin.py），开着等于白养一条 PeriodicExportingMetricReader
    #: 导出管线（常驻线程 + 每 60s 一个请求）。
    logfire_metrics_enabled: bool = False
    #: 单个属性值的字符上限（span / metric 共用的全局值）。超出由 SDK 原地
    #: 截断，**不是**丢弃整条记录。SDK 默认 None = 无限制，一个几 MB 的属性
    #: 能把整批撑到后端 413（logfire 的 5MB 检查只覆盖 span，见下）。
    logfire_attribute_value_limit: int = 8_000
    #: 单条**日志记录**属性值的字符上限（比 span 那档更宽松但仍是硬顶）。
    #: 日志的实际内容 ``logfire.msg`` 与位置参数 ``logfire.logging_args``
    #: 都是属性，所以这条是「大文件不上报」的声明式闸门。
    #: 取 16K 是因为正常最坏情形（请求体摘要 4K + 响应体摘要 4K + 签名 URL）
    #: 约 5~10K，留一倍余量以免**正常日志被静默截掉尾巴**。
    logfire_log_attribute_value_limit: int = 16_000
    #: 单条日志送入 logfire 的 message 字符硬闸（出口兜底，超出截断不丢弃）。
    #: 为什么 SDK 的属性上限不够：无位置参数的日志其**模板就是整条消息**，
    #: 而模板进的是 OTel 的 body（不受属性上限约束）。业务侧
    #: ``execute._digest`` 已把体摘要压在 4KB 内，这条防的是将来新增日志点
    #: 漏了截断，把整段响应体当消息发出去。
    logfire_log_char_limit: int = 16_000
    #: 日志批次队列条数上限。满时 OTel 丢**最老**的并打一行
    #: "Queue full, dropping logs."（那行会经 stdlib→loguru 在本地可见）。
    #: SDK 默认 2048；调试口径下每任务约 3 行，4096 给批次突发放行留一倍余量。
    logfire_log_queue_size: int = 4_096
    #: span 批次的最长滞留毫秒数。logfire 的动态批处理器默认 500ms，调大到
    #: 2000ms 让每个请求装更多 span —— trace 是上报体积的大头，请求数约降
    #: 四倍；前 10 个 span 仍走 100ms 快通道，首屏可见性不受影响。
    logfire_span_schedule_delay_ms: int = 2_000

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

    @field_validator("model_policies", mode="before")
    @classmethod
    def _parse_model_policies(cls, value: object) -> object:
        """env 里手写 JSON 字符串 → dict（``.env`` 里只能写一行文本）。

        与 ``_parse_str_tuple`` 同一套宽容策略：空串 = 空表，非法 JSON
        在启动时报错而不是静默忽略——一份写坏的策略表会让下发节奏出错，
        静默忽略等于让人以为配置生效了。
        """
        if value is None or isinstance(value, dict):
            return value or {}
        if isinstance(value, str):
            raw = value.strip()
            if not raw:
                return {}
            return json.loads(raw)
        return value

    @field_validator(
        "logfire_attribute_value_limit",
        "logfire_log_attribute_value_limit",
        "logfire_log_char_limit",
        "logfire_log_queue_size",
        "logfire_span_schedule_delay_ms",
    )
    @classmethod
    def _positive_limit(cls, value: int) -> int:
        """上限类字段拒绝 ``<= 0``。

        ``0`` 的后果是**静默退化而非报错**：属性上限 0 会把每条属性截成空串
        （logfire 上只剩时间戳），批次队列 0 会让 SDK 直接把每条日志丢掉。
        两者都表现为「配了 logfire 却什么都看不到」，属于最难查的那类故障，
        故在启动时就拒绝（与 ``_parse_model_policies`` 的「写坏即报错」同哲学）。
        """
        if value < 1:
            raise ValueError("必须 >= 1：0 会让 SDK 静默退化为不上报")
        return value


@lru_cache
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
