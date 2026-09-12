"""批次与计划任务的只读查询（放行通道 `dispatch.release` 的事实源）。

切片自原 `app/services/taskstore.py`；由 `app/services/taskstore/__init__.py`
统一再导出，对外契约（`taskstore.<name>`）不变。
"""

from __future__ import annotations

from typing import Any

from sqlalchemy import text

from app.schemas import QUEUED
from app.db import get_session_factory
from app.config import settings
from ._base import _row_to_dict


async def batch_waiting(limit: int = 500) -> list[dict[str, Any]]:
    """等待放行的批次成员（Redis 索引重建 + 超期兜底放行用）。

    看板类查询不得 ``SELECT *``（不变式 6：``data`` 里可能有 10MB 结果体），
    逐字段取。``batch_due_at`` 用 ``+ 0`` 转成数值再比较。
    """
    async with get_session_factory()() as db:
        rows = (
            await db.execute(
                text(
                    """
                    SELECT task_id,
                           data ->> '$.model' AS model,
                           data ->> '$.batch_key' AS batch_key,
                           (data ->> '$.batch_due_at') + 0 AS batch_due_at,
                           (data ->> '$.batch_size') + 0 AS batch_size
                    FROM tasks
                    WHERE platform = :p AND status = :queued
                      AND data ->> '$.batch_state' = 'waiting'
                    ORDER BY submit_time ASC
                    LIMIT :lim
                    """
                ),
                {"p": settings.gateway_platform, "queued": QUEUED, "lim": limit},
            )
        ).fetchall()
    return [_row_to_dict(row) for row in rows]


async def pending_scheduled(now: int, limit: int = 500) -> list[dict[str, Any]]:
    """尚未到点的计划任务（``st:due`` 索引丢失后的回补事实源）。

    为什么需要它：延迟任务在等待期**被 sweeper 豁免**（不能重投、不能判死），
    这本身是对的；但如果 Redis 丢了这个索引（重启无持久化 / FLUSHDB /
    键被逐出），就没有任何东西会再把它们放回去——任务会一直卡到超龄被判
    FAILURE。**执行的恢复路径有 `sweep_stale`，延迟的却没有**，这个不对称
    正是本查询要补的。

    架构文档 §9 风险登记里承诺过「`sweep_stale` 从 DB 事实回补，最迟 2min
    恢复，功能不失效」——本函数与 :func:`sweeper._rearm_due_index` 是它的落地。

    只取 ``scheduled_at > now`` 的：已到点的任务由 ``admit_due`` 处理，
    不属于「等待期回补」的范围。
    """
    async with get_session_factory()() as db:
        rows = (
            await db.execute(
                text(
                    """
                    SELECT task_id,
                           (data ->> '$.scheduled_at') + 0 AS scheduled_at
                    FROM tasks
                    WHERE platform = :p AND status = :queued
                      AND data ->> '$.batch_state' = 'scheduled'
                      AND (data ->> '$.scheduled_at') + 0 > :now
                    ORDER BY submit_time ASC
                    LIMIT :lim
                    """
                ),
                {"p": settings.gateway_platform, "queued": QUEUED,
                 "now": now, "lim": limit},
            )
        ).fetchall()
    return [{"task_id": str(r[0]), "scheduled_at": int(r[1] or 0)} for r in rows]


async def scheduled_overview(now: int, limit: int = 500) -> list[dict[str, Any]]:
    """计划中任务按小时分桶的计数（管理看板「调度视图」，PRD R-21）。

    只统计 ``batch_state='scheduled'`` 且尚未到点的行——已到点但还没占到槽的
    任务不在「计划中」的语义里，它们归重排通道。

    **括号不可省**：``(data ->> '$.scheduled_at') + 0 / 3600`` 里 ``/`` 的
    优先级高于 ``+``，会算成 ``x + 0``，分桶全部落回同一桶且数值错误。
    这是本项目 JSON 列运算的通用陷阱（与 ``&`` / ``+`` 同类）。
    """
    async with get_session_factory()() as db:
        rows = (
            await db.execute(
                text(
                    """
                    SELECT FLOOR(((data ->> '$.scheduled_at') + 0) / 3600) * 3600
                               AS bucket,
                           COUNT(*) AS n
                    FROM tasks
                    WHERE platform = :p AND status = :queued
                      AND data ->> '$.batch_state' = 'scheduled'
                      AND (data ->> '$.scheduled_at') + 0 > :now
                    GROUP BY bucket
                    ORDER BY bucket ASC
                    LIMIT :lim
                    """
                ),
                {"p": settings.gateway_platform, "queued": QUEUED,
                 "now": now, "lim": limit},
            )
        ).fetchall()
    return [{"bucket": int(r[0] or 0), "count": int(r[1] or 0)} for r in rows]


async def batch_counts_by_model() -> dict[str, int]:
    """按模型统计等待中的成员数。

    **当前没有生产调用方**，是为 PRD R-21/R-22（管理看板的「等待中批次列表」
    与并发水位视图）预留的读取入口；那两个需求尚未实现。保留它是有意的：
    口径（只数 ``batch_state='waiting'``）已经与 ``batch_waiting()`` 对齐，
    看板落地时直接接上即可。已在 ``test_misc.test_no_orphan_service_functions``
    的白名单里登记，避免它被误当成遗忘的死代码。
    """
    async with get_session_factory()() as db:
        rows = (
            await db.execute(
                text(
                    """
                    SELECT data ->> '$.model' AS model, COUNT(*) AS n
                    FROM tasks
                    WHERE platform = :p AND status = :queued
                      AND data ->> '$.batch_state' = 'waiting'
                    GROUP BY data ->> '$.model'
                    """
                ),
                {"p": settings.gateway_platform, "queued": QUEUED},
            )
        ).fetchall()
    return {str(row[0] or ""): int(row[1] or 0) for row in rows}
