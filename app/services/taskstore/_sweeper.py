"""卡死收敛与结果清理的扫描入口（判据谓词与 LIMIT 都在这里）。

切片自原 `app/services/taskstore.py`；由 `app/services/taskstore/__init__.py`
统一再导出，对外契约（`taskstore.<name>`）不变。
"""

from __future__ import annotations

from typing import Any
from sqlalchemy import CursorResult
from sqlalchemy import bindparam
from typing import cast
from sqlalchemy import text

from app.schemas import ACTIVE
from app.db import get_session_factory
from app.config import settings
from ._base import now
from ._projection import SQL_SCHEDULED_DUE_BEFORE, SQL_SCHEDULED_NOT_FUTURE, _META_SELECT, _meta_row_to_dict


async def stale_active(stale_seconds: int, limit: int = 200) -> list[dict[str, Any]]:
    """长时间未更新的非终态任务（消息丢失/worker 崩溃/超期的兜底扫描）。

    直接回**元数据行**而不是 task_id 列表：sweeper 判死/重投都要读
    ``status`` 与 ``data.dispatch_epoch``，回 id 会让它每条再补一次
    ``get_meta``（200 条批 = 200 条额外查询）。投影口径与 get_meta 一致，
    不碰 ``upstream_response`` / ``request_body`` 这些大字段。

    **计划任务豁免**（AC-44）：``scheduled_at`` 还没到的任务「长时间无进展」
    是正常的——它本来就在等。不加这条谓词，兜底扫描会把延迟 3h 的任务当僵尸
    立刻重投执行，延迟语义当场失效。

    ``+ 0`` 不可省：``data ->> '$.x'`` 回 LONGTEXT，直接与整数比较会退化成
    字符串比较（``'900' < '1000'`` 为假）。
    """
    cutoff = now() - stale_seconds
    async with get_session_factory()() as db:
        rows = (
            await db.execute(
                text(
                    f"""
                    SELECT {_META_SELECT} FROM tasks
                    WHERE platform = :p AND status IN :acts
                      AND updated_at < :cutoff
                      AND {SQL_SCHEDULED_NOT_FUTURE}
                    LIMIT :lim
                    """
                ).bindparams(bindparam("acts", expanding=True)),
                {
                    "p": settings.gateway_platform,
                    "acts": ACTIVE,
                    "cutoff": cutoff,
                    "lim": limit,
                },
            )
        ).mappings().all()
    return [_meta_row_to_dict(row) for row in rows]


async def overdue_active(lifetime_seconds: int, limit: int = 200) -> list[dict[str, Any]]:
    """超过最大生命期仍非终态的任务（无条件判死对象）。

    同 ``stale_active``：回元数据行，省掉 sweeper 侧的逐条回查。

    **生命期计时起点是 ``max(created_at, scheduled_at)``**（R-06/AC-45）——
    延迟 5h 的任务若从 ``created_at`` 起算 6h，会在它真正开始执行前就被判死。

    实现上**不写成 ``max(...)`` 表达式**：``created_at`` 有索引，而 JSON
    表达式不可索引，套上 ``max()`` 会让 MySQL 对与 new-api 共用的 ``tasks``
    表做全表扫。改为两个谓词的合取：

        max(created_at, scheduled_at) < cutoff
        ⟺ created_at < cutoff AND scheduled_at < cutoff

    这是**等价变换不是近似**：第一个谓词单独就是原语义、继续吃索引缩候选集，
    第二个只在候选行上做残差过滤。``+ 0`` 不可省（JSON 取出来是 LONGTEXT，
    字符串比较下 ``'900' < '1000'`` 为假）。
    """
    cutoff = now() - lifetime_seconds
    async with get_session_factory()() as db:
        rows = (
            await db.execute(
                text(
                    f"""
                    SELECT {_META_SELECT} FROM tasks
                    WHERE platform = :p AND status IN :acts
                      AND created_at < :cutoff
                      AND {SQL_SCHEDULED_DUE_BEFORE}
                    LIMIT :lim
                    """
                ).bindparams(bindparam("acts", expanding=True)),
                {
                    "p": settings.gateway_platform,
                    "acts": ACTIVE,
                    "cutoff": cutoff,
                    "lim": limit,
                },
            )
        ).mappings().all()
    return [_meta_row_to_dict(row) for row in rows]



async def purge_expired_results(ttl_seconds: int, limit: int = 200) -> int:
    """清空超期结果体（设计 §9）：只置空 ``upstream_response``，状态行保留。

    ``JSON_SET`` 而非 ``JSON_REMOVE``：留一个空串 + ``result_purged`` 标记，
    查询端据此返回 410（"结果已过期"）而不是 404（"任务不存在"）。

    编码标记一并置空——留着 ``gzip+b64`` 而体是空串，会让读侧对着空串
    走解压分支（虽然 flow 先判空短路了，但留一个自相矛盾的状态迟早咬人）。
    """
    cutoff = now() - ttl_seconds
    async with get_session_factory()() as db:
        res = cast(
            "CursorResult[Any]",
            await db.execute(
                text(
                    """
                    UPDATE tasks
                    SET data = JSON_SET(data, '$.upstream_response', '',
                                              '$.upstream_response_encoding', '',
                                              '$.result_purged', true)
                    WHERE platform = :p
                      AND COALESCE(data ->> '$.result_purged', 'false') <> 'true'
                      AND COALESCE(data ->> '$.upstream_response', '') <> ''
                      AND finish_time BETWEEN 1 AND :cutoff
                    LIMIT :lim
                    """
                ),
                {"p": settings.gateway_platform, "cutoff": cutoff, "lim": limit},
            ),
        )
        await db.commit()
        return int(res.rowcount)
