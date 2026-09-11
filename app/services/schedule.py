"""延迟 / 定时下发：任务压到指定时刻才准入，等待期不占并发槽。

## 两个入口，同一个到期通道

    POST 带 X-Delay-Seconds: N   →  scheduled_at = now + N     （相对）
    POST 带 X-Execute-After: ...  →  scheduled_at = 绝对时刻     （绝对）

两者互斥（同时出现 → 400）。``scheduled_at == 0`` 表示无延迟，走原有的
「立即/攒批」路径，行为逐字节不变。

到期后**复用攒批的到期通道**：``st:due`` ZSET（member=task_id，
score=该任务下次可放行的时刻）+ ``tick_batches`` cron。延迟任务的 score 是
``scheduled_at``，占槽失败退避重排的任务 score 是重试时刻——两者语义相同
（「何时该再尝试放行这条 task_id」），所以共用一个索引不会互相干扰。

## 为什么延迟上限由令牌 TTL 反推，而不是独立选一个数

用户令牌只存在 Redis 会话里，**绝不落库**（红线）。任务要延迟 N 小时执行，
令牌就必须在 Redis 里活 N 小时。所以：

    延迟 + 执行 + 余量  ≤  令牌会话 TTL

等过头 = 到点取不到令牌 = ``token_missing`` = 100% 失败。这不是性能问题，
是功能彻底失效。因此**上限不是一个可以随便填的运营参数**，它的上界由 TTL
决定；想放开更长延迟，只能先拉长 TTL（那属于安全暴露面扩大，需安全签字）。
``capacity_seconds()`` 就是这个换算，``resolve_max_delay()`` 取它与运营配置
的较小值。

## 等待期为什么不占槽

延迟 4 小时的任务若提交时就占住并发槽，用户提交 10 条就把自己的配额锁死
4 小时，正常请求全部 429。所以延迟任务在提交时**不占任何槽**，占槽发生在
到期放行的那一刻（``dispatch.release``）；等待期被取消也**不得**释放槽
（掩码为 0 时 ``release_for_task`` 什么都不做，天然满足）。
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Mapping

#: 令牌 TTL 里必须为「执行 + 收尾」留出的余量（秒）。
#: 与 ``modelpolicy._TOKENCEILING_MARGIN`` 同源同义：等待类机制都不能把
#: 令牌 TTL 吃干，否则最后一段等待必然以 token_missing 收场。
_MARGIN_SECONDS = 600

#: PRD 给的运营硬上限（12h）。它只是**外上界**，实际还受 TTL 容量钳制。
HARD_CAP_SECONDS = 12 * 3600

HEADER_DELAY = "x-delay-seconds"
HEADER_EXECUTE_AFTER = "x-execute-after"


class ScheduleError(Exception):
    """调度头非法 → 400。字段与错误码对齐 PRD §4.3 的统一错误体。"""

    def __init__(self, message: str, code: str, param: str = "") -> None:
        super().__init__(message)
        self.status = 400
        self.message = message
        self.code = code
        self.param = param


def capacity_seconds() -> int:
    """令牌 TTL 允许的**最大**延迟秒数（延迟 + 执行 + 余量 ≤ TTL）。

    下限钳到 0：若 TTL 被配得比执行时间还短（配置错误），延迟容量为 0，
    此时任何非零延迟都应被拒——宁可拒绝也不能放一个必然失败的延迟进来。
    """
    from app.config import settings

    return max(
        0,
        settings.sk_session_ttl_seconds
        - settings.worker_timeout
        - _MARGIN_SECONDS,
    )


def resolve_max_delay(configured: int) -> int:
    """运营配置的 ``max_delay_seconds`` 与 TTL 容量的较小值，并受硬上限约束。

    运营可以调小（更保守），但**调不大过物理容量**——那是任务必然失败的
    区间，放进来只会制造 100% 的 token_missing。
    """
    return max(0, min(int(configured), HARD_CAP_SECONDS, capacity_seconds()))


def _parse_int(raw: str) -> int:
    text = raw.strip()
    if not text:
        raise ScheduleError(
            "X-Delay-Seconds must be a non-negative integer",
            "invalid_delay", HEADER_DELAY,
        )
    try:
        value = int(text, 10)
    except ValueError as exc:
        raise ScheduleError(
            f"X-Delay-Seconds must be a non-negative integer, got {raw!r}",
            "invalid_delay", HEADER_DELAY,
        ) from exc
    if value < 0:
        raise ScheduleError(
            f"X-Delay-Seconds must be non-negative, got {value}",
            "invalid_delay", HEADER_DELAY,
        )
    return value


def _parse_after(raw: str, now: int) -> int:
    """``X-Execute-After``：unix 秒整数 或 RFC3339。过去时刻视为立即（0）。"""
    text = raw.strip()
    if not text:
        raise ScheduleError(
            "X-Execute-After must be a unix timestamp or RFC3339 datetime",
            "invalid_execute_after", HEADER_EXECUTE_AFTER,
        )

    # 先试 unix 秒（纯数字）
    stamp: int | None = None
    if text.lstrip("-").isdigit():
        stamp = int(text, 10)
    else:
        # RFC3339 / ISO8601。'Z' 是 UTC 后缀，fromisoformat 在 3.11 起认它，
        # 但为兼容更早解释器显式替换。无时区信息时按 UTC 解释——本地时区
        # 会让同一串在不同机器上算出不同时刻，那是运维事故的温床。
        candidate = text[:-1] + "+00:00" if text.endswith(("Z", "z")) else text
        try:
            parsed = dt.datetime.fromisoformat(candidate)
        except ValueError as exc:
            raise ScheduleError(
                f"X-Execute-After is not a valid unix timestamp or RFC3339 "
                f"datetime: {raw!r}",
                "invalid_execute_after", HEADER_EXECUTE_AFTER,
            ) from exc
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=dt.UTC)
        stamp = int(parsed.timestamp())

    # 过去时刻 = 立即执行，不报错（PRD R-02）
    return stamp if stamp > now else 0


def parse(
    headers: Mapping[str, str], *, now: int, max_delay: int
) -> int:
    """把调度头解析成 ``scheduled_at``（unix 秒；``0`` = 无延迟）。

    非法值一律抛 :class:`ScheduleError`（路由层转 400）。之所以在这里就
    拒绝而不是落库后再判：超限的延迟是**必然失败**的任务（令牌等不到那么
    久），提前拒绝才不会留下一条注定判死的行。
    """
    delay_raw = headers.get(HEADER_DELAY)
    after_raw = headers.get(HEADER_EXECUTE_AFTER)

    if delay_raw is not None and after_raw is not None:
        raise ScheduleError(
            "X-Delay-Seconds and X-Execute-After are mutually exclusive",
            "conflicting_schedule_headers", HEADER_DELAY,
        )

    if delay_raw is not None:
        seconds = _parse_int(delay_raw)
        if seconds == 0:
            return 0
        if seconds > max_delay:
            raise ScheduleError(
                f"delay {seconds}s exceeds max_delay_seconds ({max_delay}s)",
                "delay_too_long", HEADER_DELAY,
            )
        return now + seconds

    if after_raw is not None:
        scheduled_at = _parse_after(after_raw, now)
        if scheduled_at == 0:
            return 0
        delta = scheduled_at - now
        if delta > max_delay:
            raise ScheduleError(
                f"X-Execute-After is {delta}s in the future, which exceeds "
                f"max_delay_seconds ({max_delay}s)",
                "delay_too_long", HEADER_EXECUTE_AFTER,
            )
        return scheduled_at

    return 0
