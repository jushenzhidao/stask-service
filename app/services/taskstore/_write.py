"""写侧：单行 INSERT、CAS 状态迁移、JSON_MERGE_PATCH、批次成员的原子摘取与归还。

切片自原 `app/services/taskstore.py`；由 `app/services/taskstore/__init__.py`
统一再导出，对外契约（`taskstore.<name>`）不变。
"""

from __future__ import annotations

import json

from typing import Any
from sqlalchemy import CursorResult
from sqlalchemy import bindparam
from typing import cast
from sqlalchemy import text

from app.schemas import QUEUED
from app.schemas import TERMINAL
from app.db import get_session_factory
from app.config import settings
from ._base import _log_write_error, now


async def create(task_id: str, action: str, data: dict[str, Any], user_id: int = 0) -> None:
    """落库 QUEUED（ADR-006 契约行）。

    - 直接落 ``QUEUED`` 而不是先 SUBMITTED 再 CAS：省掉每次提交的第二次
      DB UPDATE。「入队未确认」窗口的两种失败都有兜底——入队调用失败时
      提交链路当场 CAS 判死；进程在入队前崩溃时行停在 QUEUED 且
      dispatch_epoch=0，sweep_stale 按「锁不在 + 从未派发」重投；
    - ``channel_id`` = 独立渠道号（``CHANNEL_ID``）；
    - ``quota`` 恒 0 —— 零资金记账，计费由上游 relay 自理，上游即便
      误动本行退款也是 0；
    - ``user_id`` = 鉴权直查值（查不到落 0，仅归属信息）。

    ``private_data`` 不在 INSERT 里写：它是 new-api 的原生列，允许 NULL
    （宿主自己的 ``Value()`` 在结构体全空时也返回 nil）。本服务只在
    SUCCESS 落终态时按需 ``JSON_MERGE_PATCH`` 写入 ``result_url``——
    提交时无任何可写内容，先占一个 ``{}`` 只是多一次无意义的列写入。
    """
    ts = now()
    try:
        async with get_session_factory()() as db:
            await db.execute(
                text(
                    """
                    INSERT INTO tasks
                      (task_id, platform, action, status, progress, data,
                       user_id, channel_id, quota, submit_time, start_time,
                       created_at, updated_at)
                    VALUES
                      (:task_id, :platform, :action, 'QUEUED', '0%', CAST(:data AS JSON),
                       :user_id, :channel_id, 0, :now, 0, :now, :now)
                    """
                ),
                {
                    "task_id": task_id,
                    "platform": settings.gateway_platform,
                    "action": action,
                    "data": json.dumps(data, ensure_ascii=False),
                    "user_id": user_id,
                    "channel_id": settings.channel_id,
                    "now": ts,
                },
            )
            await db.commit()
    except Exception:
        _log_write_error("create", task_id)
        raise
    # write-through：长轮询读侧优先命中缓存（失败静默，见 statuscache）
    from app.services import statuscache

    await statuscache.set(task_id, "QUEUED")


async def cas(
    task_id: str,
    from_statuses: tuple[str, ...],
    to_status: str,
    patch: dict[str, Any] | None = None,
    fail_reason: str = "",
    private_patch: dict[str, Any] | None = None,
) -> bool:
    """CAS 状态迁移。返回 True = 本调用者抢到推进权（负责释放槽/清会话/回调）。

    - 终态一律把 ``progress`` 置 ``100%``（不只 SUCCESS）——失败/取消停在
      ``0%`` 会让看板与客户端以为任务还在跑；
    - 终态一律用**秒**刷 ``finish_time``；``IN_PROGRESS`` 顺带刷 ``start_time``；
    - ``fail_reason`` 截断到 500 字符。

    ``private_patch`` 合并进 ``private_data`` —— new-api 的原生列
    （``TaskPrivateData``，``json:"-"`` 永不出现在它的 API 响应里）。
    本服务只写 ``result_url`` 一个键：宿主的 ``Task.GetResultURL()``
    先读它、为空才回落 ``fail_reason``（历史兼容分支），不写则宿主看板
    对我们的行显示不出结果地址。

    ``COALESCE(private_data, JSON_OBJECT())``：该列可为 NULL（new-api 的
    ``Value()`` 在结构体全空时返回 nil），直接 ``JSON_MERGE_PATCH(NULL, ...)``
    的结果是 NULL —— 补丁会被静默吞掉。
    """
    ts = now()
    terminal = 1 if to_status in TERMINAL else 0
    running = 1 if to_status == "IN_PROGRESS" else 0
    private_sql = (
        """,
            private_data = JSON_MERGE_PATCH(
                COALESCE(private_data, JSON_OBJECT()), CAST(:private_patch AS JSON))"""
        if private_patch
        else ""
    )
    stmt = text(
        f"""
        UPDATE tasks
        SET status = :to,
            updated_at = :now,
            start_time = IF(:running = 1, :now, start_time),
            finish_time = IF(:terminal = 1, :now, finish_time),
            progress = IF(:terminal = 1, '100%', progress),
            fail_reason = :reason,
            data = JSON_MERGE_PATCH(COALESCE(data, JSON_OBJECT()), CAST(:patch AS JSON)){private_sql}
        WHERE task_id = :tid AND platform = :p AND status IN :froms
        """
    ).bindparams(bindparam("froms", expanding=True))
    params: dict[str, Any] = {
        "to": to_status,
        "now": ts,
        "terminal": terminal,
        "running": running,
        "reason": (fail_reason or "")[:500],
        "patch": json.dumps(patch or {}, ensure_ascii=False),
        "tid": task_id,
        "p": settings.gateway_platform,
        "froms": from_statuses,
    }
    if private_patch:
        params["private_patch"] = json.dumps(private_patch, ensure_ascii=False)
    try:
        async with get_session_factory()() as db:
            res = cast("CursorResult[Any]", await db.execute(stmt, params))
            await db.commit()
            won = res.rowcount == 1
    except Exception:
        _log_write_error(f"cas:{from_statuses}->{to_status}", task_id)
        raise
    if won:
        # write-through：先 DB 后缓存，长轮询秒级看见终态
        from app.services import statuscache

        await statuscache.set(task_id, to_status)
    return won


async def patch_data(task_id: str, patch: dict[str, Any]) -> None:
    """非迁移性的数据合并（观测字段回填等）。"""
    try:
        async with get_session_factory()() as db:
            await db.execute(
                text(
                    """
                    UPDATE tasks
                    SET updated_at = :now,
                        data = JSON_MERGE_PATCH(COALESCE(data, JSON_OBJECT()), CAST(:patch AS JSON))
                    WHERE task_id = :tid AND platform = :p
                    """
                ),
                {
                    "now": now(),
                    "patch": json.dumps(patch, ensure_ascii=False),
                    "tid": task_id,
                    "p": settings.gateway_platform,
                },
            )
            await db.commit()
    except Exception:
        _log_write_error("patch_data", task_id)
        raise


async def claim_for_release(task_id: str) -> bool:
    """抢占「放行权」：条件更新，一行只可能被一方 claim 成功。

    ``batch_state`` 从 ``waiting`` 翻到 ``releasing`` 的同时校验状态仍是
    QUEUED。受影响行数 0 → 别人已放过、或任务已被取消/终态。这是「N 触发
    与 T 触发同时命中同一条任务」时**唯一**能挡住重复下发的地方——
    Redis 侧的 claim 只保证批次级互斥，救不了跨批次重投的成员。

    JSON 数值比较必带 ``+ 0``（不变式 13）：``data ->> '$.x'`` 返回的是
    字符串，字符串比较下 '10' < '9'。这里比较的是字符串枚举，但仍用
    ``COALESCE`` 兜住老行（无该字段时视为可放行）。
    """
    try:
        async with get_session_factory()() as db:
            result = cast(
                "CursorResult[Any]",
                await db.execute(
                text(
                    """
                    UPDATE tasks
                    SET updated_at = :now,
                        data = JSON_MERGE_PATCH(
                            COALESCE(data, JSON_OBJECT()),
                            CAST(:patch AS JSON)
                        )
                    WHERE task_id = :tid AND platform = :p
                      AND status = :queued
                      AND COALESCE(data ->> '$.batch_state', 'waiting')
                          IN ('waiting', 'scheduled')
                    """
                ),
                {
                    "now": now(),
                    "patch": json.dumps({"batch_state": "releasing"}),
                    "tid": task_id,
                    "p": settings.gateway_platform,
                    "queued": QUEUED,
                },
                ),
            )
            await db.commit()
            return bool(result.rowcount)
    except Exception:
        _log_write_error("claim_for_release", task_id)
        raise


async def unclaim_for_release(task_id: str, *, restore: str = "waiting") -> None:
    """回退放行权（占槽失败时）：``releasing`` 退回**抢占前那个等待态**。

    不回退的话这条任务在 DB 里永远是 releasing，下一轮退避重排时
    ``claim_for_release`` 必然失败 → 任务永久卡在等待期。

    ``restore`` 必须由调用方传入抢占前读到的真实值，**不能靠
    ``scheduled_at`` 反推**：延迟任务一旦入批，它就同时是「已到点的计划任务」
    （``scheduled_at`` 仍 > 0）和「批次成员」（``batch_state='waiting'``）。
    靠 scheduled_at 反推会把它误标回 ``scheduled``——``batch_waiting()``
    于是不再认为它是批次成员，索引重建会把它漏掉，整批永远凑不满 N。
    """
    await patch_data(task_id, {"batch_state": restore})
