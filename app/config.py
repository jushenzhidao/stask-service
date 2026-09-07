"""配置单例（pydantic-settings，env 前缀 ``ST_``）。

纪律：
- 业务模块一律 ``from app.config import settings``，**禁止散读 os.environ**；
  唯一例外是 ``gunicorn.conf.py``（master 进程早于 app 加载）。
- 序列/映射型字段用 ``NoDecode`` + ``field_validator(mode="before")`` 手工解析，
  兼容「逗号分隔」与「JSON 数组」两种写法（运维手写 .env 时不必记 JSON 语法）。
- 测试重载：``get_settings.cache_clear()``；单测更常用
  ``monkeypatch.setattr(settings, "xxx", ...)`` 直接改单例字段。

与 atask-service 的区别：env 前缀 ``ST_``（atask 是 ``GW_``），
Redis 独立实例 + 键前缀 ``st:``（ADR-004），tasks 表 platform 值独立
（``ST_GATEWAY_PLATFORM``，默认 ``stask``）——两个网关共享 tasks 表，
靠 platform 列划分自有行，绝不互相踩踏。
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
    app_version: str = "0.1.0"
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

    #: tasks 表 platform 列取值——本网关自有行的唯一标识（写侧 WHERE 必带）
    gateway_platform: str = "stask"

    # ---- Redis（独立实例，ADR-004）----
    redis_url: str = "redis://127.0.0.1:6381/0"
    #: 全部键的统一前缀（即便误连 atask 实例也不会撞键）
    redis_key_prefix: str = "st"

    # ---- 上游寻址（§7）----
    #: 默认 upstream（nginx 未注入 X-Upstream-Base-Url 头时的回落值）。
    #: 上游对本项目而言**只是一个 HTTP 服务**：new-api 是默认实现，换任何
    #: 同步生成接口只要加进 allowlist 即可，本服务不感知它是谁。
    #: 别名里保留旧名 ``ST_NEWAPI_BASE_URL``：存量 .env 不改名也能起来，
    #: 否则改名会让已部署实例静默回落默认值、把请求打到错的上游。
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

    # ---- 计费服务（余额 / 身份内省 / 消费日志，全部 HTTP，红线：禁止跨服务读库）----
    billing_svc_url: str = "http://127.0.0.1:8080"
    http_timeout: float = 10.0
    balance_cache_ttl: int = 30          # 余额缓存秒（§6）
    inspect_cache_ttl: int = 30          # 身份内省缓存秒

    # ---- 余额并发额度（§6）----
    #: 参考单价兜底值（USD/次）——按模型覆盖用 ``ST_REF_PRICE_{MODEL}``，
    #: 模型名中的 ``-``/``.``/``/`` 全部替换为 ``_`` 再大写（见 pricing.ref_price）
    ref_price_default: float = 0.04
    max_slots: int = 10                  # 单用户在途上限
    slot_ttl_seconds: int = 3600         # 槽键 TTL 兜底（进程崩溃不永久泄漏）

    # ---- 限流与幂等 ----
    rate_limit: int = 60                 # 每窗口提交次数
    rate_limit_window_seconds: int = 60
    idem_ttl: int = 86400                # 幂等键回填后的保留秒
    idem_pending_ttl_seconds: int = 30   # 占位标记 TTL（创建链路在飞窗口）
    idem_replay_wait_seconds: float = 3.0  # 同键真并发的短轮询等待上限

    # ---- worker 执行（§5、§8）----
    worker_timeout: int = 120            # 上游调用超时秒
    #: 5xx 重试次数。**默认 0（ADR-002 保守决策）**：上游的 5xx
    #: 是否确定回滚预扣配额尚未确认，重试可能造成双扣。确认后改这个值即可开。
    retry_max: int = 0
    #: 连接层错误（请求未到达上游，重试零资金风险）的独立重试次数
    retry_max_connect: int = 2
    retry_backoff_base: float = 1.0
    dispatch_lock_margin_seconds: int = 30   # 派发锁 TTL = worker_timeout + margin
    queue_concurrency: int = 64          # worker 并发度（taskiq --max-async-tasks）
    sk_session_ttl_seconds: int = 172800  # 令牌会话 TTL（48h，终态即清）

    # ---- 提交/响应体上限 ----
    body_max_bytes: int = 2 * 1024 * 1024        # 提交体落库上限（2MB）
    response_max_bytes: int = 10 * 1024 * 1024   # 响应体落库上限（10MB）

    # ---- 查询长轮询（§2）----
    poll_wait_max_seconds: int = 60      # ?wait= 的上限（须 < nginx proxy_read_timeout）
    poll_interval_seconds: float = 0.5

    # ---- 超时对账（§8）----
    reconcile_ttl: int = 86400           # 挂起转人工秒
    reconcile_batch_limit: int = 50
    reconcile_log_window_seconds: int = 3600   # 查消费日志的时间窗
    reconcile_biz_type: str = ""         # 消费日志 biz_type 过滤（空 = 不过滤）

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
