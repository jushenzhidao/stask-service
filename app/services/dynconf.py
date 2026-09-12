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
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Literal, cast

from app.config import settings
from app.logging import log
from app.redis import _k, r

#: Redis 里存放覆盖值的键（单个 Hash，一次 HGETALL 拿全量）
_KEY = _k("dynconf")

#: 本地缓存 TTL（秒）
_CACHE_TTL = 5.0

Kind = Literal["int", "float", "bool", "str", "json"]


def _validate_policies(raw: Any) -> dict[str, dict[str, int]]:
    """``model_policies`` 的校验器（延迟 import 打破循环依赖）。

    ``modelpolicy`` 会 import 本模块的 ``RuntimeConfig``，所以这里不能在
    模块顶部 import 它——放到调用点。
    """
    from app.services.modelpolicy import validate

    return validate(raw)


class Spec:
    """一个可热改配置项的元数据（看板据此渲染表单并做前端校验）。

    ``kind="json"`` 用于映射型配置（如 ``model_policies``）。它**必须**带
    ``validator``：映射型配置的合法性（字段名、值域、条目间一致性）不是
    ``coerce`` 能表达的，没有校验就等于给了一个能搞垮服务下发节奏的后门。
    """

    __slots__ = ("group", "key", "kind", "label", "maximum", "minimum", "note",
                 "validator")

    def __init__(self, key: str, kind: Kind, label: str, group: str, *,
                 minimum: float | None = None, maximum: float | None = None,
                 note: str = "",
                 validator: Callable[[Any], Any] | None = None) -> None:
        self.key = key
        self.kind = kind
        self.label = label
        self.group = group
        self.minimum = minimum
        self.maximum = maximum
        self.note = note
        self.validator = validator

    def coerce(self, raw: Any) -> Any:
        """字符串/原始值 → 目标类型，并做区间钳制。非法值抛 ValueError。

        ``json`` 接受三种输入：已是 dict/list（管理面 JSON 体）、JSON 文本
        （前端表单提交的字符串）、空串（= 清空）。解析后交给 ``validator``。
        """
        if self.kind == "json":
            if raw is None or raw == "":
                return {}
            if isinstance(raw, str):
                try:
                    raw = json.loads(raw)
                except ValueError as exc:
                    raise ValueError(f"{self.key}: not valid JSON ({exc})") from exc
            if self.validator is not None:
                return self.validator(raw)
            return raw
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

    def to_dict(self, current: Any, overridden: bool) -> dict[str, Any]:
        value = current
        if self.kind == "json":
            # 看板表单是纯文本输入框：给一份缩进 JSON，人可直接编辑
            value = json.dumps(current if current is not None else {},
                               ensure_ascii=False, indent=2, sort_keys=True)
        return {
            "key": self.key, "kind": self.kind, "label": self.label,
            "group": self.group, "value": value, "overridden": overridden,
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
        Spec("plain_max_bytes", "int", "明文落库上限 (字节)", "体量",
             minimum=0, maximum=16 * 1024 * 1024,
             note="≤ 本值且为合法 UTF-8 则原样存明文，超过才 gzip+base64；0 = 全部压缩"),

        # ---- 查询 ----
        Spec("poll_wait_max_seconds", "int", "长轮询上限 (秒)", "查询",
             minimum=0, maximum=300,
             note="必须小于 nginx proxy_read_timeout，否则客户端看到 504"),

        # ---- 生命期与清理 ----
        Spec("task_max_lifetime_seconds", "int", "任务最大生命期 (秒)", "清理",
             minimum=300, maximum=20 * 3600,
             note="超龄非终态直接判死；必须小于上游 24h 清理线"),
        Spec("sweep_batch_limit", "int", "每轮兜底扫描条数", "清理",
             minimum=1, maximum=1000),
        Spec("result_ttl_seconds", "int", "结果保留 (秒)", "清理",
             minimum=60, maximum=30 * 86400),

        # ---- 攒批 ----
        Spec("batch_release_concurrency", "int", "批次放行并发度", "攒批",
             minimum=1, maximum=64,
             note="整批放行时的有界并发，与 sweeper 的 8 同量级"),
        Spec("batch_backoff_max_seconds", "int", "占槽失败退避上限 (秒)", "攒批",
             minimum=10, maximum=3600,
             note="放行时占不到槽则指数退避重排，抖动 ±10% 防整批惊群"),
        # X-Batch-Wait 的上限，同时是「只给 N 不给 T」时的兜底等待（R-17/AC-57）
        Spec("max_batch_wait_seconds", "int", "批次最长等待 (秒)", "攒批",
             minimum=1, maximum=3600,
             note="PRD 硬上限 1h；客户端 X-Batch-Wait 超此值 → 400 "
                  "batch_wait_too_long。注意还要满足 B1："
                  "本值 + 执行 + 余量 ≤ 令牌会话 TTL"),
        # 模型策略表：条目数 = 模型数（可枚举），故留在 dynconf 白名单内；
        # 校验失败整批拒绝，绝不半套生效。
        Spec("model_policies", "json", "模型策略表", "攒批",
             note=(
                 "键：模型名（小写归一，精确）> 端点前缀（如 /v1/images，最长匹配）"
                 "> __default__（兜底）。"
                 "字段：batch 0-1000（>=2 才是攒批开关，0/1 = 收到即发）；"
                 "batch_wait 1-3600（batch>=2 时必须显式给）；limit_per_token / "
                 "limit_model_token / limit_global 均 0-10000（>0 即启用该层，"
                 "limit_global 是「多 key 合计不超发」的唯一保证）；"
                 "reject_when_full 0/1（分层上限满了怎么办：0=默认排队，提交即 202、"
                 "永不 429；1=提交时原子占三层，满则 429 + Retry-After 由客户端退避"
                 "重试，不再排队、也不依赖 tick 进程。1 不可与 batch>=2 同用，"
                 "且必须至少声明一个分层上限）。"
                 "写入语义：默认「整表替换」（写入即覆盖整张表）；保存时选「合并」"
                 "或加 ?mode=merge，则只覆盖写到的模型条目、未写到的保持原值。"
                 "整表替换时必须先读出现值、把已有条目一起带上再写回，"
                 "漏掉的模型会被静默清掉（不再攒批、也不受并发上限约束，"
                 "而请求侧毫无异常）。"
                 "硬约束（违反即 400）：batch>=2 必须同时给 batch_wait；"
                 "batch_wait + 执行时长 + 余量 ≤ SK_SESSION_TTL_SECONDS。"
                 "放行延迟 ≈ batch_wait + 0~15s（tick 每分钟 1 次、内部自旋 4 轮）"
                 " + 约 3s，仅排队语义；reject_when_full=1 无此延迟，"
                 "热改只影响新提交，在途任务按创建时那套走完。"
                 "客户端可用 X-Batch-Size / X-Batch-Wait / X-Batch-Key 逐请求覆盖。"
                 "示例：{\"doubao-seedream-5-0-pro-260628\": {\"batch\": 3, "
                 "\"batch_wait\": 30}}；单条串行示例："
                 "{\"doubao-seedream-5-0-pro-260628\": {\"limit_model_token\": 1, "
                 "\"reject_when_full\": 1}}"
             ),
             validator=_validate_policies),

        # ---- 延迟/定时下发 ----
        # 上界由令牌 TTL 反推（services/schedule.capacity_seconds），此处
        # maximum 只是运营侧的表单上界；真实生效值是两者取小。
        Spec("max_delay_seconds", "int", "允许的最大延迟 (秒)", "延迟",
             minimum=0, maximum=12 * 3600,
             note="延迟 D + 执行 + 余量 必须 ≤ 令牌会话 TTL，否则到点必然"
                  " token_missing；故真实上限还会被 TTL 容量钳制，见 services/schedule"),

        # ---- 开关 ----
        Spec("sweep_enabled", "bool", "定时任务总开关", "开关",
             note="关闭后卡死收敛、槽位校准、结果清理全停"),
        Spec("batch_enabled", "bool", "攒批总开关", "开关",
             note="关闭后所有模型立即下发（即便策略里声明了 batch）"),
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
    "auth_mode": "安全项：可写等于能在线关掉提交前鉴权与余额预检",
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
    plain_max_bytes: int
    poll_wait_max_seconds: int
    task_max_lifetime_seconds: int
    sweep_batch_limit: int
    result_ttl_seconds: int
    sweep_enabled: bool
    batch_enabled: bool
    batch_release_concurrency: int
    batch_backoff_max_seconds: int
    max_batch_wait_seconds: int
    max_delay_seconds: int

    @classmethod
    def from_values(cls, values: dict[str, Any]) -> RuntimeConfig:
        return cls(**{key: values[key] for key in cls.__dataclass_fields__})


#: 映射型配置**不进** ``RuntimeConfig``：它不是标量，塞进 frozen dataclass
#: 会让每次取快照都做一次深拷贝+哈希；且策略解析要按模型名逐条查，
#: 走 :func:`get_model_policies` 单独读更直接。
_SNAPSHOT_KEYS = tuple(
    key for key, spec in MUTABLE.items() if spec.kind != "json"
)


async def get_model_policies() -> dict[str, dict[str, int]]:
    """取模型策略表（已校验）。Redis 覆盖 > env 默认（env 里恒为空表）。"""
    overrides = await _load()
    if "model_policies" in overrides:
        return cast(dict[str, dict[str, int]], overrides["model_policies"])
    return cast(dict[str, dict[str, int]], getattr(settings, "model_policies", {}) or {})


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


async def _effective_value(key: str) -> Any:
    """读某项的**当前生效值**（绕过本地缓存），供合并写入用。

    合并必须基于 Redis 里的最新值：``_load()`` 的 5s 进程内缓存可能落后于
    其他副本刚写入的条目，拿它做合并会把别人新加的模型条目覆盖掉。
    读不到或值损坏时回落 ``settings``（env），与普通读取路径同口径。
    """
    spec = MUTABLE[key]
    try:
        raw = await r.hgetall(_KEY)
    except Exception:
        log.opt(exception=True).debug("dynconf merge read failed, use env value")
        raw = None
    value = (raw or {}).get(key)
    if value is None:
        return getattr(settings, key, None)
    try:
        return spec.coerce(json.loads(value))
    except Exception:
        log.warning("dynconf value invalid, ignoring: key={}", key)
        return getattr(settings, key, None)


async def _merge_mapping(key: str, new: Any) -> Any:
    """顶层键合并，同名键**整条替换**。非映射型或类型不符时原样返回新值。"""
    current = await _effective_value(key)
    if not isinstance(current, dict) or not isinstance(new, dict):
        return new
    return {**current, **new}


async def set_many(
    updates: dict[str, Any], *, mode: str = "replace"
) -> dict[str, Any]:
    """批量写覆盖值。返回生效后的全量视图。

    校验失败**整批拒绝**（不做部分成功）——半套配置比旧配置更危险。

    ``mode`` 决定**映射型配置项**（``kind="json"``，目前只有 ``model_policies``）
    的写入语义：

    - ``replace``（默认，保持既有行为）：**整表替换**——没带上的模型条目会被
      清掉。「给第二个模型加策略」时必须把已有条目一起抄上，漏抄即静默清空。
    - ``merge``：以当前生效值为基础**只覆盖/新增本次写到的顶层键**，没写到的
      条目保持原值——上面那个坑随之消失，不必再「先读全表 → 整表写回」。

    合并只到**顶层键（模型名）**为止：同名条目整条替换，不做字段级深合并。
    字段级合并无法区分「改一个字段」与「删一个字段」，排障时也说不清
    「这条任务到底生效了哪套参数」。

    读-改-写不是原子操作：管理面是低频人工操作，两处真并发写同一份映射的
    概率极低；要严格串行请用 ``replace`` 并自行先读后写。
    """
    if not updates:
        return await snapshot()
    if mode not in ("replace", "merge"):
        raise ValueError(f"unknown mode: {mode!r} (valid: replace, merge)")

    payload: dict[str, str] = {}
    for key, raw in updates.items():
        spec = MUTABLE.get(key)
        if spec is None:
            reason = IMMUTABLE_REASONS.get(key, "not in mutable allowlist")
            raise ValueError(f"{key} cannot be changed at runtime ({reason})")
        value = spec.coerce(raw)
        if mode == "merge" and spec.kind == "json":
            value = await _merge_mapping(key, value)
        payload[key] = json.dumps(value)

    # redis 8 的 hset 注解要求键类型是宽 union 且 Mapping 键 invariant，
    # dict[str, str] 无法直接匹配——语义没变，cast(Any) 过桥
    await r.hset(_KEY, mapping=cast(Any, payload))
    _invalidate()
    log.info("dynconf updated: keys={} mode={}", sorted(payload), mode)
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
    groups: dict[str, list[dict[str, Any]]] = {}
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
