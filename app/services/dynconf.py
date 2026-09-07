"""动态配置：运行时可热改的**安全子集**。

## 分层规则（安全边界，不是便利性设计）

| 层 | 内容 | 可否运行时改 |
|---|---|---|
| 启动项 | 连接串、平台标识、渠道号、键前缀 | **永不** —— 改了等于换一个服务 |
| 安全项 | upstream 白名单、回调白名单、回调密钥、管理密钥 | **永不** —— 白名单可写 = 把防凭证泄露的防线挂到网上 |
| 运营项 | 槽上限、限流、重试、超时、TTL、开关 | 可 —— 见 `MUTABLE` |

`MUTABLE` 是**白名单**而非黑名单：新增配置项默认不可热改，要开必须显式
登记。反过来（黑名单）时新增一个敏感项忘了加进黑名单就直接暴露了。

## 读取优先级

Redis 覆盖值 > env / `.env` > 代码默认值。

Redis 不可用时**自动回落 env**——动态配置是增强，绝不能成为可用性单点。
本地缓存 TTL 5s，避免热路径被 Redis RTT 拖慢。
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from typing import Any, Literal, cast

from app.config import settings
from app.logging import log
from app.redis import _k, r

#: Redis 里存放覆盖值的键（单个 Hash，一次 HGETALL 拿全量）
_KEY = _k("dynconf")

#: 本地缓存 TTL（秒）
_CACHE_TTL = 5.0

Kind = Literal["int", "float", "bool", "str"]


class Spec:
    """一个可热改配置项的元数据（看板据此渲染表单并做前端校验）。"""

    __slots__ = ("group", "key", "kind", "label", "maximum", "minimum", "note")

    def __init__(self, key: str, kind: Kind, label: str, group: str, *,
                 minimum: float | None = None, maximum: float | None = None,
                 note: str = "") -> None:
        self.key = key
        self.kind = kind
        self.label = label
        self.group = group
        self.minimum = minimum
        self.maximum = maximum
        self.note = note

    def coerce(self, raw: Any) -> Any:
        """字符串 → 目标类型，并做区间钳制。非法值抛 ValueError。"""
        if self.kind == "bool":
            if isinstance(raw, bool):
                return raw
            text = str(raw).strip().lower()
            if text in ("1", "true", "yes", "on"):
                return True
            if text in ("0", "false", "no", "off"):
                return False
            raise ValueError(f"{self.key}: expected boolean, got {raw!r}")
        if self.kind == "str":
            return str(raw)
        number = float(raw) if self.kind == "float" else int(float(raw))
        if self.minimum is not None and number < self.minimum:
            raise ValueError(f"{self.key}: below minimum {self.minimum}")
        if self.maximum is not None and number > self.maximum:
            raise ValueError(f"{self.key}: above maximum {self.maximum}")
        return number

    def to_dict(self, current: Any, overridden: bool) -> dict:
        return {
            "key": self.key, "kind": self.kind, "label": self.label,
            "group": self.group, "value": current, "overridden": overridden,
            "min": self.minimum, "max": self.maximum, "note": self.note,
        }


#: **白名单**：只有登记在此的配置项可以运行时热改。
MUTABLE: dict[str, Spec] = {
    s.key: s for s in (
        # ---- 并发 ----
        Spec("max_slots", "int", "单 token 在途上限", "并发",
             minimum=1, maximum=10000,
             note="纯并发保护，不做任何资金判定"),
        Spec("slot_ttl_seconds", "int", "槽键 TTL (秒)", "并发",
             minimum=60, maximum=86400,
             note="进程崩溃时槽位不永久泄漏的兜底"),

        # ---- 限流 ----
        Spec("rate_limit", "int", "提交速率上限", "限流",
             minimum=0, maximum=100000, note="0 = 关闭限流"),
        Spec("rate_limit_window_seconds", "int", "限流窗口 (秒)", "限流",
             minimum=1, maximum=3600),

        # ---- worker ----
        Spec("worker_timeout", "int", "上游调用超时 (秒)", "执行",
             minimum=5, maximum=1800,
             note="改大要同步调 nginx proxy_read_timeout"),
        Spec("retry_max", "int", "5xx 重试次数", "执行",
             minimum=0, maximum=10,
             note="上游调用可能有副作用，默认 0"),
        Spec("retry_max_connect", "int", "连接层错误重试次数", "执行",
             minimum=0, maximum=10,
             note="请求未到达上游，重试零副作用"),
        Spec("dispatch_lock_margin_seconds", "int", "派发锁余量 (秒)", "执行",
             minimum=5, maximum=600, note="锁 TTL = 调用超时 + 本值"),

        # ---- 体量 ----
        Spec("body_max_bytes", "int", "提交体上限 (字节)", "体量",
             minimum=1024, maximum=100 * 1024 * 1024),
        Spec("response_max_bytes", "int", "响应体上限 (字节)", "体量",
             minimum=1024, maximum=100 * 1024 * 1024),

        # ---- 查询 ----
        Spec("poll_wait_max_seconds", "int", "长轮询上限 (秒)", "查询",
             minimum=0, maximum=300,
             note="必须小于 nginx proxy_read_timeout，否则客户端看到 504"),

        # ---- 幂等 ----
        Spec("idem_ttl", "int", "自动幂等窗口 (秒)", "幂等",
             minimum=60, maximum=7 * 86400,
             note="窗口内同请求指纹只创建一个任务"),

        # ---- 生命期与清理 ----
        Spec("task_max_lifetime_seconds", "int", "任务最大生命期 (秒)", "清理",
             minimum=300, maximum=20 * 3600,
             note="超龄非终态直接判死；必须小于上游 24h 清理线"),
        Spec("sweep_batch_limit", "int", "每轮兜底扫描条数", "清理",
             minimum=1, maximum=1000),
        Spec("result_ttl_seconds", "int", "结果保留 (秒)", "清理",
             minimum=60, maximum=30 * 86400),

        # ---- 开关 ----
        Spec("sweep_enabled", "bool", "定时任务总开关", "开关",
             note="关闭后卡死收敛、槽位校准、结果清理全停"),
    )
}

#: 显式声明**永不可改**的项（仅用于看板展示与审计说明，代码不读它做判定
#: ——判定靠 MUTABLE 白名单）
IMMUTABLE_REASONS: dict[str, str] = {
    "database_url": "启动项：改了等于换一个服务",
    "redis_url": "启动项：动态配置自身就存在这里",
    "redis_key_prefix": "启动项：改了会让在途任务的键全部失联",
    "gateway_platform": "启动项：改了会让在途任务全部失联",
    "channel_id": "启动项：共享表渠道划分依据，改了会与上游任务混行",
    "upstream_allowlist": "安全项：可写等于把防凭证泄露的防线挂到网上",
    "callback_allowlist": "安全项：可写等于开放 SSRF 出口",
    "callback_secret": "安全项：密钥不经 HTTP 传输",
    "async_allow_prefixes": "安全项：路径准入是攻击面收口点",
    "async_deny_prefixes": "安全项：同上",
}

_cache: dict[str, Any] = {}
_cache_at: float = 0.0


@dataclass(frozen=True, slots=True)
class RuntimeConfig:
    """一次读取后可安全复用的动态配置快照。"""

    max_slots: int
    slot_ttl_seconds: int
    rate_limit: int
    rate_limit_window_seconds: int
    worker_timeout: int
    retry_max: int
    retry_max_connect: int
    dispatch_lock_margin_seconds: int
    body_max_bytes: int
    response_max_bytes: int
    poll_wait_max_seconds: int
    idem_ttl: int
    task_max_lifetime_seconds: int
    sweep_batch_limit: int
    result_ttl_seconds: int
    sweep_enabled: bool

    @classmethod
    def from_values(cls, values: dict[str, Any]) -> RuntimeConfig:
        return cls(**{key: values[key] for key in cls.__dataclass_fields__})


_SNAPSHOT_KEYS = tuple(RuntimeConfig.__dataclass_fields__)


async def _load() -> dict[str, Any]:
    """读取 Redis 覆盖值（带本地缓存）。Redis 不可用返回空 dict（回落 env）。"""
    global _cache, _cache_at
    if time.monotonic() - _cache_at < _CACHE_TTL:
        return _cache
    try:
        raw = await r.hgetall(_KEY)
    except Exception:
        log.opt(exception=True).debug("dynconf read failed, falling back to env")
        _cache = {}
        _cache_at = time.monotonic()
        return _cache
    parsed: dict[str, Any] = {}
    for key, value in (raw or {}).items():
        spec = MUTABLE.get(str(key))
        if spec is None:
            continue                     # 白名单外的残留键直接忽略
        try:
            parsed[str(key)] = spec.coerce(json.loads(value))
        except Exception:
            log.warning("dynconf value invalid, ignoring: key={}", key)
    _cache = parsed
    _cache_at = time.monotonic()
    return _cache


async def get(key: str) -> Any:
    """取配置值。优先 Redis 覆盖，回落 ``settings``。"""
    if key not in MUTABLE:
        return getattr(settings, key)
    overrides = await _load()
    if key in overrides:
        return overrides[key]
    return getattr(settings, key)


async def get_runtime_config() -> RuntimeConfig:
    """读取一次并返回不可变运行时配置快照。

    调用方应在一次请求/任务开始时取得快照，并在后续逻辑中复用。
    """
    overrides = await _load()
    values = {
        key: overrides.get(key, getattr(settings, key))
        for key in _SNAPSHOT_KEYS
    }
    return RuntimeConfig.from_values(values)


async def get_int(key: str) -> int:
    return int(await get(key))


async def get_bool(key: str) -> bool:
    return bool(await get(key))


async def set_many(updates: dict[str, Any]) -> dict[str, Any]:
    """批量写覆盖值。返回生效后的全量视图。

    校验失败**整批拒绝**（不做部分成功）——半套配置比旧配置更危险。
    """
    if not updates:
        return await snapshot()
    payload: dict[str, str] = {}
    for key, raw in updates.items():
        spec = MUTABLE.get(key)
        if spec is None:
            reason = IMMUTABLE_REASONS.get(key, "not in mutable allowlist")
            raise ValueError(f"{key} cannot be changed at runtime ({reason})")
        payload[key] = json.dumps(spec.coerce(raw))

    # redis 8 的 hset 注解要求键类型是宽 union 且 Mapping 键 invariant，
    # dict[str, str] 无法直接匹配——语义没变，cast(Any) 过桥
    await r.hset(_KEY, mapping=cast(Any, payload))
    _invalidate()
    log.info("dynconf updated: keys={}", sorted(payload))
    return await snapshot()


async def reset(keys: list[str] | None = None) -> dict[str, Any]:
    """删除覆盖值，回落 env。``keys=None`` 清空全部覆盖。"""
    if keys:
        unknown = [k for k in keys if k not in MUTABLE]
        if unknown:
            raise ValueError(f"unknown keys: {unknown}")
        await r.hdel(_KEY, *keys)
        log.info("dynconf reset: keys={}", sorted(keys))
    else:
        await r.delete(_KEY)
        log.info("dynconf reset: all")
    _invalidate()
    return await snapshot()


async def snapshot() -> dict[str, Any]:
    """看板用的全量视图：可改项（含当前值与是否被覆盖）+ 只读项说明。"""
    overrides = await _load()
    groups: dict[str, list[dict]] = {}
    for key, spec in MUTABLE.items():
        current = overrides.get(key, getattr(settings, key))
        groups.setdefault(spec.group, []).append(
            spec.to_dict(current, key in overrides)
        )
    return {
        "groups": [{"name": name, "items": items} for name, items in groups.items()],
        "override_count": len(overrides),
        "immutable": [{"key": k, "reason": v} for k, v in IMMUTABLE_REASONS.items()],
    }


def _invalidate() -> None:
    """写后立即失效本地缓存（只清本进程；多副本最多再用 5s 旧值，有意取舍）。"""
    global _cache_at
    _cache_at = 0.0
