"""配置单例（pydantic-settings，env 前缀 ``ST_``）。

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
from functools import lru_cache
from typing import Annotated

from pydantic import AliasChoices, Field, field_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict


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
    model_config = SettingsConfigDict(env_prefix="ST_", env_file=".env", extra="ignore")

    # ---- 应用 ----
    app_env: str = "dev"
    app_version: str = "0.2.0"
    log_level: str = "INFO"

    #: 管理面密钥（看板 + 动态配置写入）。**空 = 整个管理面 404**，
    #: 默认不开启，忘配密钥不等于裸奔。与终端用户 sk 完全分离——
    #: 用 sk 鉴权会让任何普通用户都能改全局配置（提权）。
    admin_key: str = ""

    # ---- 数据层（与 new-api 共享 MySQL 实例，零建表）----
    database_url: str = "mysql+asyncmy://root:root@127.0.0.1:3306/newapi?charset=utf8mb4"
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
    upstream_base_url: str = Field(
        default="http://127.0.0.1:3000",
        validation_alias=AliasChoices("ST_UPSTREAM_BASE_URL", "ST_NEWAPI_BASE_URL"),
    )
    #: 允许的 upstream host 白名单（含端口按 host:port 比对；不含端口只比 host）
    upstream_allowlist: Annotated[tuple[str, ...], NoDecode] = ("127.0.0.1:3000", "newapi:3000")

    # ---- 路径准入（§2）----
    async_allow_prefixes: Annotated[tuple[str, ...], NoDecode] = (
        "/v1/images", "/v1/audio", "/v1/videos",
    )
    #: 硬拒前缀（deny 优先于 allow）：管理面绝不允许被任务化转发
    async_deny_prefixes: Annotated[tuple[str, ...], NoDecode] = ("/api/", "/console/")

    # ---- 并发保护（本服务不做资金判定，纯固定闸门）----
    max_slots: int = 10                  # 单 token 在途上限
    slot_ttl_seconds: int = 3600         # 槽键 TTL 兜底（进程崩溃不永久泄漏）

    # ---- 限流与幂等 ----
    rate_limit: int = 60                 # 每窗口提交次数
    rate_limit_window_seconds: int = 60
    #: 自动幂等窗口：同一 token 的字节级相同请求在此窗口内只创建一个任务
    idem_ttl: int = 86400
    idem_pending_ttl_seconds: int = 30   # 占位标记 TTL（创建链路在飞窗口）
    idem_replay_wait_seconds: float = 3.0  # 同键真并发的短轮询等待上限

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


@lru_cache
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
