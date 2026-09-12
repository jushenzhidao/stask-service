"""模型策略：按**模型**（或端点前缀）声明「攒批节奏」与「三层并发上限」。

## 为什么控制轴是模型

本服务的三个旋钮本质上是同一件事的三个侧面——**上游渠道的真实承载能力**：

- 上游某模型的渠道只有 3 路并发 → 单发立即 429/5xx，必须排队（`limit_global`）；
- 某模型单条要跑 120s、突发提交几百条 → 攒批对齐，削掉提交突发（`batch` / `batch_wait`）；
- 某模型快且交互式、上游容量充足 → 什么都不配，保持收到即发。

所以配置以模型名为键。端点前缀（`/v1/videos`）只是给「body 里读不出 model」的
形态兜底，不是主轴。

## 解析优先级

    精确模型名  →  端点前缀（最长匹配）  →  ``__default__``  →  全局 env 标量

命中哪层就整块用哪层，未声明的字段回落下一层。**不做字段级深合并**：
排障时「这条任务到底生效了哪套参数」必须一眼可答，深合并会让这个问题
需要拿三份配置对着推。

## 分层上限满额时的两种行为（`reject_when_full`）

`limit_model_token` / `limit_global` 声明的是「在途上限」，但**满了之后怎么办**
是另一个独立决策，默认不写死其中一种：

| `reject_when_full` | 满额行为 | 客户端看到的 |
|---|---|---|
| `0`（默认） | **排队**：提交即 202，任务挂进 `st:due`，由 tick 逐条等槽 | 202 + `batch_state=waiting`，**永不 429** |
| `1` | **拒绝**：提交时就用 `acquire_layered` 原子占三层，满则 429 | 429 + `Retry-After`，客户端自行退避重试 |

两者不是「严格程度」的差别，而是**背压信号放在哪一侧**：

- 默认的排队语义把背压交给服务端（任务不会丢，但队列长度无上限，客户端看不到
  任何「该停一停」的信号，必须自己读 `/admin/api/slots`）；
- `reject_when_full=1` 把背压交回客户端（队列不积压，429 就是信号），代价是
  客户端**必须有重试逻辑**，否则任务直接丢失。

`reject_when_full=1` 时提交即占槽，因此**不再依赖 tick 进程**、也没有
「等 tick 的 0~15s」额外延迟；这是它对单条串行场景（`limit_model_token=1`）更
合适的原因。它也**不受 `batch_enabled` 连坐**——分层并发限制不是攒批的一部分，
拿攒批止血开关去关它是错的。

与 `batch >= 2` 互斥（写侧拒绝该组合）：攒批必须有等待窗口，而攒批期间占着
并发槽会把用户的配额锁死整个窗口。攒批优先，要拒绝就别攒批。

## 存储

整份策略是 **dynconf 的一个 json 类型配置项**（`model_policies`），
沿用 ADR-005 的安全模型：白名单内、写入时整批校验、校验失败整批回退、
5 秒内热生效。条目数有上限（模型名可枚举，不是运营数据），
所以留在配置体系内，而不是另开一套存储。

**热改只影响新提交**：生效参数在提交时算定并落进 `tasks.data`
（`slot_flags` / `batch_state` / `due_at`），在途任务按创建时那一套走完
生命周期。这是必需的——按新配置去释放旧任务会还掉别人的槽。
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from app.config import settings
from app.logging import log

#: 空模型名归入的固定组（与 taskstore 的 `__unknown__` 口径一致）
UNKNOWN_MODEL = "__unknown__"

#: 缺省策略的保留键
DEFAULT_KEY = "__default__"

#: 校验边界（写侧拒绝越界，避免一份配置让服务不可用）
MAX_BATCH = 1000
MAX_BATCH_WAIT = 3600
MAX_LIMIT = 10000
MAX_ENTRIES = 256

#: 策略条目的合法字段（多一个都拒绝：防止把拼错的键静默忽略）
_FIELDS = ("batch", "batch_wait", "limit_per_token", "limit_model_token",
           "limit_global", "reject_when_full")

#: 攒批等待之外必须留出的余量：单条任务的执行时长（`worker_timeout` 上界）
#: 加一点收尾空间。等待 + 执行 + 余量 必须 ≤ 令牌会话 TTL。
_TOKENCEILING_MARGIN = 600


def _tokenceiling_margin() -> int:
    """令牌 TTL 约束里的「执行 + 收尾」预留量。

    **任何「等待」都受令牌会话 TTL 约束**：攒批等 T、延迟等到点、放行失败
    退避重排，三者都在等，而令牌只存 Redis 且绝不落库。等待总时长加上执行
    时长若越过 TTL，到点取不到令牌就是 100% ``token_missing`` 判死。
    所以写侧必须把 T 卡在 TTL 之内（PRD 的 B1 阻塞项）。
    """
    return _TOKENCEILING_MARGIN


def normalize_model(model: str) -> str:
    """模型名归一：去空白、转小写、空则归入 ``__unknown__``。

    归一化结果会**落库**（``data.slot_model``）——占槽与释放必须用同一个
    字符串，否则一旦归一化规则变化，在途任务会「占 A 释放 B」造成永久漂移。
    """
    return (model or "").strip().lower() or UNKNOWN_MODEL


@dataclass(frozen=True, slots=True)
class ResolvedPolicy:
    """解析后的**生效**参数（每个字段都是确定的数值，无「未声明」态）。

    ``source`` 是命中的层（``model:<名>`` / ``path:<前缀>`` / ``default`` /
    ``settings``）——它是排障的第一落点：接入方问「我这批为什么在攒」，
    看这一个字段就知道是哪条配置在起作用。
    """

    batch: int
    batch_wait: int
    limit_per_token: int
    limit_model_token: int
    limit_global: int
    source: str
    #: 满额行为开关（见模块 docstring「分层上限满额时的两种行为」）。
    #: 放在 ``source`` 之后并带默认值：既有构造点（含测试）用关键字传参，
    #: 不给默认值会让所有 ResolvedPolicy(...) 直接 TypeError。
    reject_when_full: int = 0

    @property
    def layered_at_submit(self) -> bool:
        """提交时就**用三层原子占槽**（占不到即 429），而不是排队等槽。

        三个条件同时成立才开启：

        - ``reject_when_full > 0``：运营显式选择了「满则拒」；
        - ``batch < 2``：攒批必须有等待窗口，提交时占槽会把用户配额锁死整个
          窗口（写侧已拒绝「攒批 + 满则拒」的组合，这里是最后一道防御）；
        - 至少声明了一层分层上限（第二/三层）：没有上限就没有「满」可言，
          写侧同样已拒绝这种无效配置。

        与 ``queues`` 互斥：本属性为真时 ``queues`` 必为假。这样提交链路
        走「立即路径 + 分层占槽」，也就不再依赖 ``batch_enabled`` 这个攒批
        止血开关——分层并发限制不是攒批的一部分。
        """
        return (
            self.reject_when_full > 0
            and self.batch < 2
            and (self.limit_model_token > 0 or self.limit_global > 0)
        )

    @property
    def queues(self) -> bool:
        """是否需要走「排队放行」路径（而非提交时立即占槽入队）。

        三个条件任一成立就要排队：

        - ``limit_global > 0`` / ``limit_model_token > 0``：这两层只有
          ``dispatch.release`` 的**分层占槽**（``acquire_layered``）才会判定。
          提交时的立即路径走的是单层 ``slots.acquire``，**只认第一层**——所以
          只要声明了第二/三层，就必须把任务交给放行通道，否则该层形同虚设。
        - ``batch >= 2``：攒够 N 或等够 T 再整批放行，削掉提交突发。

        三者都不成立时（``batch<=1`` 且两个分层上限均为 0）走提交时立即占槽
        的旧路径——那条路径的行为与改造前逐字节一致。

        **例外**：``layered_at_submit`` 为真（``reject_when_full=1`` 且不攒批）
        时**不排队**——那条路径在提交侧就已经用 ``acquire_layered`` 占了
        第二/三层，判定点只是从放行前移到了提交，不需要放行通道。
        """
        if self.batch >= 2:
            return True
        if self.layered_at_submit:
            return False
        return self.limit_model_token > 0 or self.limit_global > 0


def validate(raw: Any) -> dict[str, dict[str, int]]:
    """校验并规范化策略表。非法即抛 ``ValueError``（调用方整批拒绝）。

    这一层是**写侧闸门**：策略会影响全服务的下发节奏，一份写坏的配置
    （比如 batch_wait 填 0 导致批次永远不超时、或 limit_global 填成
    天文数字形同关闭）比不配置危险得多，所以宁可拒写也不静默纠正。
    """
    if raw is None or raw == "":
        return {}
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except ValueError as exc:
            raise ValueError(f"model_policies: not valid JSON ({exc})") from exc
    if not isinstance(raw, dict):
        raise ValueError("model_policies: expected an object keyed by model name")
    if len(raw) > MAX_ENTRIES:
        raise ValueError(f"model_policies: too many entries (max {MAX_ENTRIES})")

    out: dict[str, dict[str, int]] = {}
    for key, node in raw.items():
        name = str(key).strip()
        if not name:
            raise ValueError("model_policies: empty key")
        if len(name) > 128:
            raise ValueError(f"model_policies[{name}]: key too long (max 128)")
        if not isinstance(node, dict):
            raise ValueError(f"model_policies[{name}]: expected an object")
        unknown = [f for f in node if f not in _FIELDS]
        if unknown:
            raise ValueError(
                f"model_policies[{name}]: unknown field(s) {sorted(unknown)} "
                f"(valid: {list(_FIELDS)})"
            )
        entry: dict[str, int] = {}
        for field in _FIELDS:
            if field not in node:
                continue
            value = node[field]
            if isinstance(value, bool) or not isinstance(value, int | str):
                raise ValueError(f"model_policies[{name}].{field}: expected an integer")
            try:
                number = int(value)
            except (TypeError, ValueError) as exc:
                raise ValueError(
                    f"model_policies[{name}].{field}: expected an integer, got {value!r}"
                ) from exc
            limit = {
                "batch": (0, MAX_BATCH, "batch"),
                "batch_wait": (1, MAX_BATCH_WAIT, "batch_wait"),
                "limit_per_token": (0, MAX_LIMIT, "limit_per_token"),
                "limit_model_token": (0, MAX_LIMIT, "limit_model_token"),
                "limit_global": (0, MAX_LIMIT, "limit_global"),
                "reject_when_full": (0, 1, "reject_when_full"),
            }[field]
            low, high, label = limit
            if not low <= number <= high:
                raise ValueError(
                    f"model_policies[{name}].{label}: out of range [{low}, {high}], got {number}"
                )
            entry[field] = number
        if entry.get("batch", 0) >= 2 and not entry.get("batch_wait"):
            # 攒批却没有超时上限 = 可能永远等不到放行（只发了 3 条声明 N=100）
            raise ValueError(
                f"model_policies[{name}]: batch >= 2 requires an explicit batch_wait "
                "(otherwise the batch may wait forever)"
            )
        if entry.get("reject_when_full"):
            # 「满则拒」与攒批互斥：攒批要先凑够 N 或等够 T，那段时间必须不占槽
            # （否则一批还没放行就先把自己的并发额度耗光）。两者同时声明时
            # 语义无解，宁可拒写也不静默选一个丢一个。
            if entry.get("batch", 0) >= 2:
                raise ValueError(
                    f"model_policies[{name}]: reject_when_full is incompatible with "
                    "batch >= 2 (a batch must not hold concurrent slots while waiting)"
                )
            # 开了开关却一层分层上限都没声明 = 没有任何东西会「满」，这个配置
            # 只会让人以为自己在背压而实际没有任何效果（静默无效配置）。
            if not (entry.get("limit_model_token") or entry.get("limit_global")):
                raise ValueError(
                    f"model_policies[{name}]: reject_when_full requires "
                    "limit_model_token or limit_global to be > 0"
                )
        wait = entry.get("batch_wait", 0)
        if wait and wait + _tokenceiling_margin() > settings.sk_session_ttl_seconds:
            # 令牌会话 TTL 是等待时长的真天花板：令牌只在 Redis 且绝不落库，
            # 等过头 = 到点取不到令牌 = token_missing 判死，100% 失败。
            # 这条约束比「任务最大生命期」更紧，必须显式拦在写侧。
            raise ValueError(
                f"model_policies[{name}].batch_wait: {wait}s + execution is too long; "
                f"token session TTL is {settings.sk_session_ttl_seconds}s "
                "(waited-out tasks fail 100% with token_missing)"
            )
        out[name] = entry
    return out


def _parse(policies: dict[str, Any] | None) -> dict[str, dict[str, int]]:
    """读侧容错解析：**丢弃坏条目并告警**，绝不因此让提交链路 500。

    写侧已校验（``validate``），这里是防御 db 里的历史残留值——读侧抛异常
    等于一次误写就让全服务提交不可用，代价远大于丢掉一条策略。
    """
    if not policies:
        return {}
    try:
        return validate(policies)
    except ValueError as exc:
        log.warning("model_policies invalid, falling back to no policies: {}", exc)
        return {}


def _match_path(policies: dict[str, dict[str, int]], path: str) -> tuple[str, dict[str, Any]] | None:
    """端点前缀最长匹配（与 ``async_allow_prefixes`` 同一套前缀语义）。"""
    best: tuple[str, dict[str, Any]] | None = None
    for key, entry in policies.items():
        if not key.startswith("/"):
            continue
        if path == key or path.startswith(key.rstrip("/") + "/") or path.startswith(key):
            if best is None or len(key) > len(best[0]):
                best = (key, entry)
    return best


def resolve(
    *,
    model: str,
    path: str = "",
    policies: dict[str, Any] | None = None,
    default_limit_per_token: int = 0,
) -> ResolvedPolicy:
    """算出这条任务生效的策略。

    ``default_limit_per_token`` 是全局 env 标量（``MAX_SLOTS``）——保留它作为
    最后一层回落，是为了让**不配置任何策略的部署**行为与改造前完全一致。
    """
    table = _parse(policies)
    name = normalize_model(model)

    chosen: dict[str, int] | None = None
    source = "settings"
    if name in table:
        chosen, source = table[name], f"model:{name}"
    else:
        hit = _match_path(table, path or "")
        if hit is not None:
            chosen, source = hit[1], f"path:{hit[0]}"
        elif DEFAULT_KEY in table:
            chosen, source = table[DEFAULT_KEY], "default"

    chosen = chosen or {}
    return ResolvedPolicy(
        batch=int(chosen.get("batch", 0)),
        batch_wait=int(chosen.get("batch_wait", 0)),
        limit_per_token=int(chosen.get("limit_per_token", default_limit_per_token)),
        limit_model_token=int(chosen.get("limit_model_token", 0)),
        limit_global=int(chosen.get("limit_global", 0)),
        source=source,
        # 未声明回落 0 = 「排队」旧语义。这个回落方向是刻意的：热改一张表时
        # 漏写该字段不该把已有模型的满额行为从排队静默翻成拒绝。
        reject_when_full=int(chosen.get("reject_when_full", 0)),
    )
