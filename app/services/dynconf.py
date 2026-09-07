"""动态配置：运行时可热改的**安全子集**。

## 为什么需要分层

`app/config.py` 的纪律是「配置单例 + 禁止散读 env」，改配置要重启。但有些
旋钮是运营性质的——某个模型涨价了要调 ref_price、某用户压测要临时放宽
max_slots、确认上游回滚语义后要开重试。为这些事重启一次网关不合理。

## 分层规则（这是安全边界，不是便利性设计）

| 层 | 内容 | 可否运行时改 |
|---|---|---|
| 启动项 | 连接串、监听地址、平台标识、键前缀 | **永不** —— 改了等于换一个服务 |
| 安全项 | upstream 白名单、回调白名单、回调密钥、管理密钥 | **永不** —— 白名单可写 = 把防 sk 泄露的防线挂到网上 |
| 运营项 | 单价、槽上限、限流、重试、超时、TTL、开关 | 可 —— 见 `MUTABLE` |

`MUTABLE` 是**白名单**而非黑名单：新增配置项默认不可热改，要开必须显式
登记。反过来（黑名单）时新增一个敏感项忘了加进黑名单就直接暴露了。

## 读取优先级

Redis 覆盖值 > env / `.env` > 代码默认值。

Redis 不可用时**自动回落 env**——动态配置是增强，绝不能成为可用性单点。
本地缓存 TTL 5s，避免热路径（每次提交都要读 ref_price 和 max_slots）
被 Redis RTT 拖慢。
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from typing import Any, Literal

from app.config import settings
from app.logging import log
from app.redis import _k, r

#: Redis 里存放覆盖值的键（单个 Hash，一次 HGETALL 拿全量）
_KEY = _k("dynconf")

#: 本地缓存 TTL（秒）——热路径每次提交要读 2~3 个键，不能每次都打 Redis
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
#: 新增配置项默认不可改——要开必须显式加进来并想清楚安全影响。
MUTABLE: dict[str, Spec] = {
    s.key: s for s in (
        # ---- 并发额度 ----
        Spec("max_slots", "int", "单用户在途上限", "额度",
             minimum=1, maximum=1000,
             note="slots = clamp(floor(余额 / 参考单价), 1, 本值)"),
        Spec("ref_price_default", "float", "参考单价兜底 (USD)", "额度",
             minimum=0.000001, maximum=1000,
             note="只影响并发闸门松紧，不参与任何资金计算"),
        Spec("slot_ttl_seconds", "int", "槽键 TTL (秒)", "额度",
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
             note="ADR-002 默认 0：上游 5xx 回滚语义未确认，重试可能双扣"),
        Spec("retry_max_connect", "int", "连接层错误重试次数", "执行",
             minimum=0, maximum=10,
             note="请求未到达上游，重试零资金风险"),
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

        # ---- 对账与清理 ----
        Spec("reconcile_ttl", "int", "挂起转人工阈值 (秒)", "对账",
             minimum=60, maximum=30 * 86400),
        Spec("reconcile_batch_limit", "int", "每轮对账条数", "对账",
             minimum=1, maximum=1000),
        Spec("reconcile_log_window_seconds", "int", "消费日志查询窗口 (秒)", "对账",
             minimum=60, maximum=7 * 86400),
        Spec("result_ttl_seconds", "int", "结果保留 (秒)", "清理",
             minimum=60, maximum=30 * 86400),

        # ---- 缓存 ----
        Spec("balance_cache_ttl", "int", "余额缓存 (秒)", "缓存",
             minimum=0, maximum=3600),
        Spec("inspect_cache_ttl", "int", "身份内省缓存 (秒)", "缓存",
             minimum=0, maximum=3600),

        # ---- 开关 ----
        Spec("sweep_enabled", "bool", "定时任务总开关", "开关",
             note="关闭后对账、槽位校准、卡死扫描、结果清理全停"),
    )
}

#: 显式声明**永不可改**的项（仅用于看板展示与审计说明，代码不读它做判定
#: ——判定靠 MUTABLE 白名单）
IMMUTABLE_REASONS: dict[str, str] = {
    "database_url": "启动项：改了等于换一个服务",
    "redis_url": "启动项：动态配置自身就存在这里",
    "redis_key_prefix": "启动项：改了会让在途任务的键全部失联",
    "gateway_platform": "启动项：改了会让在途任务全部失联",
    "upstream_allowlist": "安全项：可写等于把防 sk 泄露的防线挂到网上",
    "callback_allowlist": "安全项：可写等于开放 SSRF 出口",
    "callback_secret": "安全项：密钥不经 HTTP 传输",
    "billing_svc_url": "安全项：可写等于把用户 sk 引到任意地址",
    "async_allow_prefixes": "安全项：路径准入是攻击面收口点",
    "async_deny_prefixes": "安全项：同上",
}

_cache: dict[str, Any] = {}
_cache_at: float = 0.0


@dataclass(frozen=True, slots=True)
class RuntimeConfig:
    """一次读取后可安全复用的动态配置快照。"""

    max_slots: int
    ref_price_default: float
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
    reconcile_ttl: int
    reconcile_batch_limit: int
    reconcile_log_window_seconds: int
    result_ttl_seconds: int
    balance_cache_ttl: int
    inspect_cache_ttl: int
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

    调用方应在一次请求/任务开始时取得快照，并在后续逻辑中复用，避免
    对同一批动态配置重复执行异步 getter。
    """
    overrides = await _load()
    values = {
        key: overrides.get(key, getattr(settings, key))
        for key in _SNAPSHOT_KEYS
    }
    return RuntimeConfig.from_values(values)


async def get_int(key: str) -> int:
    return int(await get(key))


async def get_float(key: str) -> float:
    return float(await get(key))


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

    await r.hset(_KEY, mapping=payload)
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
    """写后立即失效本地缓存。

    注意：这只清**本进程**的缓存。多进程/多副本部署时其余进程最多再用
    5s 的旧值（`_CACHE_TTL`）。这是有意的取舍——为强一致去做 pub/sub
    失效广播，复杂度远超收益（这些都是运营旋钮，不是资金判定）。
    """
    global _cache_at
    _cache_at = 0.0
