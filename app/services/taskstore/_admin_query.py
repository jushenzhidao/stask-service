"""管理看板查询：分页检索与概览聚合（只读，绝不 SELECT *）。

切片自原 `app/services/taskstore.py`；由 `app/services/taskstore/__init__.py`
统一再导出，对外契约（`taskstore.<name>`）不变。
"""

from __future__ import annotations

from typing import Any
from sqlalchemy import bindparam
from sqlalchemy import text

from app.schemas import ACTIVE
from app.db import get_session_factory
from app.config import settings
from ._base import _ADMIN_MAX_WINDOW_SECONDS, _escape_like_prefix, _validate_search_params, as_unix_seconds, now
from ._projection import _meta_int, _meta_scalar


async def search(
    *,
    status: str = "",
    model: str = "",
    task_id: str = "",
    task_id_prefix: str = "",
    since_seconds: int = 0,
    limit: int = 50,
    offset: int = 0,
) -> dict[str, Any]:
    """看板任务列表（分页 + 筛选）。

    ``task_id`` 只支持精确匹配；需要按前缀检索时使用 ``task_id_prefix``，
    生成可使用 task_id 索引的 ``LIKE 'prefix%'``，禁止任意片段模糊搜索。
    """
    _validate_search_params(
        task_id=task_id, task_id_prefix=task_id_prefix,
        since_seconds=since_seconds, limit=limit, offset=offset,
    )
    where = ["platform = :p"]
    params: dict[str, Any] = {"p": settings.gateway_platform}

    if status:
        where.append("status = :status")
        params["status"] = status
    if model:
        where.append("data ->> '$.model' = :model")
        params["model"] = model
    if task_id:
        where.append("task_id = :tid")
        params["tid"] = task_id
    elif task_id_prefix:
        where.append("task_id LIKE :tid_prefix")
        params["tid_prefix"] = f"{_escape_like_prefix(task_id_prefix)}%"
    if since_seconds > 0:
        where.append("created_at > :since")
        params["since"] = now() - since_seconds

    clause = " AND ".join(where)

    import asyncio

    async def _count() -> int:
        async with get_session_factory()() as db:
            return (
                await db.execute(
                    text(f"SELECT COUNT(*) FROM tasks WHERE {clause}"), params
                )
            ).scalar() or 0

    async def _select() -> list[Any]:
        async with get_session_factory()() as db:
            rows = (
                await db.execute(
                    text(
                        f"""
                        SELECT task_id, status, fail_reason, channel_id, user_id,
                               created_at, start_time, finish_time,
                               data ->> '$.model'            AS model,
                               data ->> '$.request_path'     AS request_path,
                               data ->> '$.upstream_status'  AS upstream_status,
                               data ->> '$.response_bytes'   AS response_bytes,
                               data ->> '$.result_purged'    AS result_purged,
                               data ->> '$.artifact_count'   AS artifact_count,
                               data ->> '$.result_url'       AS result_url
                        FROM tasks WHERE {clause}
                        ORDER BY id DESC LIMIT :lim OFFSET :off
                        """
                    ),
                    {**params, "lim": limit, "off": offset},
                )
            ).mappings().all()
            return list(rows)

    total, rows = await asyncio.gather(_count(), _select())

    items = []
    for row in rows:
        item = dict(row)
        for col in ("created_at", "start_time", "finish_time"):
            item[col] = as_unix_seconds(item.get(col))
        finish, start = item["finish_time"], item["start_time"]
        item["duration"] = (finish - start) if (finish and start and finish >= start) else 0
        item["result_purged"] = str(item.get("result_purged")) == "true"
        # ``->>`` 恒回字符串（缺失为 None）——不归一的话看板会把 'null'
        # 当成有制品，或者在 JS 里对字符串做数值比较
        item["artifact_count"] = _meta_int(item.get("artifact_count"))
        item["result_url"] = str(_meta_scalar(item.get("result_url")) or "")
        items.append(item)

    return {"total": int(total), "items": items,
            "limit": limit, "offset": offset}


async def metrics(window_seconds: int = 3600) -> dict[str, Any]:
    """看板概览指标：窗口内状态分布、失败原因 TopN、模型分布、耗时分位。"""
    if not 60 <= window_seconds <= _ADMIN_MAX_WINDOW_SECONDS:
        raise ValueError("window must be between 60 and 604800 seconds")
    since = now() - window_seconds
    p = settings.gateway_platform

    async def _query_status() -> list[Any]:
        async with get_session_factory()() as db:
            rows = (
                await db.execute(
                    text(
                        """
                        SELECT status, COUNT(*) AS n FROM tasks
                        WHERE platform = :p AND created_at > :since
                        GROUP BY status
                        """
                    ),
                    {"p": p, "since": since},
                )
            ).all()
            return list(rows)

    async def _query_failures() -> list[Any]:
        async with get_session_factory()() as db:
            rows = (
                await db.execute(
                    text(
                        """
                        SELECT COALESCE(NULLIF(fail_reason, ''), 'unknown') AS reason,
                               COUNT(*) AS n
                        FROM tasks
                        WHERE platform = :p AND status = 'FAILURE'
                          AND created_at > :since
                        GROUP BY reason ORDER BY n DESC LIMIT 10
                        """
                    ),
                    {"p": p, "since": since},
                )
            ).all()
            return list(rows)

    async def _query_models() -> list[Any]:
        async with get_session_factory()() as db:
            rows = (
                await db.execute(
                    text(
                        """
                        SELECT COALESCE(NULLIF(data ->> '$.model', ''), 'unknown') AS model,
                               COUNT(*) AS n
                        FROM tasks
                        WHERE platform = :p AND created_at > :since
                        GROUP BY model ORDER BY n DESC LIMIT 10
                        """
                    ),
                    {"p": p, "since": since},
                )
            ).all()
            return list(rows)

    async def _query_durations() -> dict[str, int]:
        """分位数在 SQL 侧算完，只回 4 个标量。

        原实现把窗口内**全部** SUCCESS 行的 duration 拉回 Python 排序，
        任务量大时内存和网络往返都随行数线性涨。改用 MySQL 8 窗口函数
        单趟扫描定位分位行，语义与原 Python 取法完全一致（0 基下标
        ``min(n-1, floor(n*q))`` ⇔ 1 基 ``LEAST(n, FLOOR(n*q)+1)``）。
        """
        async with get_session_factory()() as db:
            row = (
                await db.execute(
                    text(
                        """
                        WITH d AS (
                            SELECT finish_time - start_time AS v,
                                   ROW_NUMBER() OVER (
                                       ORDER BY finish_time - start_time
                                   ) AS rn,
                                   COUNT(*) OVER () AS n
                            FROM tasks
                            WHERE platform = :p AND status = 'SUCCESS'
                              AND created_at > :since
                              AND start_time > 0
                              AND finish_time >= start_time
                        )
                        SELECT n,
                               MAX(IF(rn = LEAST(n, FLOOR(n * 0.50) + 1), v, NULL)) AS p50,
                               MAX(IF(rn = LEAST(n, FLOOR(n * 0.95) + 1), v, NULL)) AS p95,
                               MAX(IF(rn = LEAST(n, FLOOR(n * 0.99) + 1), v, NULL)) AS p99,
                               MAX(v) AS mx
                        FROM d GROUP BY n
                        """
                    ),
                    {"p": p, "since": since},
                )
            ).mappings().first()
        if row is None:
            return {"count": 0, "p50": 0, "p95": 0, "p99": 0, "max": 0}
        return {
            "count": int(row["n"] or 0),
            "p50": int(row["p50"] or 0),
            "p95": int(row["p95"] or 0),
            "p99": int(row["p99"] or 0),
            "max": int(row["mx"] or 0),
        }

    async def _query_active_total() -> int:
        async with get_session_factory()() as db:
            return (
                await db.execute(
                    text(
                        "SELECT COUNT(*) FROM tasks WHERE platform = :p "
                        "AND status IN :acts"
                    ).bindparams(bindparam("acts", expanding=True)),
                    {"p": p, "acts": ACTIVE},
                )
            ).scalar() or 0

    import asyncio
    status_rows, fail_rows, model_rows, durations, active_total = \
        await asyncio.gather(
            _query_status(),
            _query_failures(),
            _query_models(),
            _query_durations(),
            _query_active_total(),
        )

    counts = {str(row[0]): int(row[1]) for row in status_rows}
    done = counts.get("SUCCESS", 0) + counts.get("FAILURE", 0)

    return {
        "window_seconds": window_seconds,
        "status_counts": counts,
        "total": sum(counts.values()),
        "success_rate": round(counts.get("SUCCESS", 0) / done, 4) if done else None,
        "active_total": int(active_total),
        "duration_seconds": durations,
        "top_failures": [{"reason": str(r[0])[:160], "count": int(r[1])}
                         for r in fail_rows],
        "top_models": [{"model": str(r[0]), "count": int(r[1])} for r in model_rows],
    }
