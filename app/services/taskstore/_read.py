"""读侧：单行查询、状态查询，以及各层并发槽的活跃计数（校准事实源）。

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
from ._base import _row_to_dict
from ._projection import SQL_HOLDS_LAYER1, SQL_HOLDS_LAYER2, SQL_HOLDS_LAYER3, _META_SELECT, _meta_row_to_dict


async def exists(task_id: str) -> bool:
    """自动幂等的事实源检查：行在 = 已创建过（idem.wait_row 的回调）。"""
    async with get_session_factory()() as db:
        row = (
            await db.execute(
                text("SELECT 1 FROM tasks WHERE task_id = :t AND platform = :p LIMIT 1"),
                {"t": task_id, "p": settings.gateway_platform},
            )
        ).scalar()
    return row is not None


async def get(task_id: str) -> dict[str, Any] | None:
    """按 task_id 取**整行**（含 ``upstream_response`` 大字段）。

    只给**必须拿到原始结果体**的路径用：查询端点的字节级回放
    (`flow._replay`) 与 worker 执行前取请求体 (`execute.run`)。
    其余只看元数据的路径一律走 :func:`get_meta`。

    读侧必须加 platform 过滤：new-api 的 tasks 表在 task_id 上只有普通
    索引、无唯一约束，不加过滤可能读到别家的行——越权读取。
    """
    async with get_session_factory()() as db:
        row = (
            await db.execute(
                text("SELECT * FROM tasks WHERE task_id = :t AND platform = :p LIMIT 1"),
                {"t": task_id, "p": settings.gateway_platform},
            )
        ).mappings().first()
    return _row_to_dict(row) if row else None



async def get_meta(task_id: str) -> dict[str, Any] | None:
    """单行**元数据**投影：显式列 + 逐键 ``data ->> '$.x'``，不碰大字段。

    ``upstream_response`` / ``request_body`` 单行可达 10MB。看板详情、
    ops 诊断、回调推送这些路径一个字节都用不到，``SELECT *`` 会把它们
    全量拉过 DB 连接——并发几个就能打满带宽。
    """
    async with get_session_factory()() as db:
        row = (
            await db.execute(
                text(f"SELECT {_META_SELECT} FROM tasks WHERE task_id = :t AND platform = :p LIMIT 1"),
                {"t": task_id, "p": settings.gateway_platform},
            )
        ).mappings().first()
    return _meta_row_to_dict(row) if row else None


async def get_status(task_id: str) -> str | None:
    """轻量状态查询（长轮询每 0.5s 一次，不必拉整行含 10MB 结果体）。"""
    async with get_session_factory()() as db:
        row = (
            await db.execute(
                text("SELECT status FROM tasks WHERE task_id = :t AND platform = :p LIMIT 1"),
                {"t": task_id, "p": settings.gateway_platform},
            )
        ).scalars().first()
    return str(row) if row else None


async def counts_by_status() -> dict[str, int]:
    """状态分布（ops 观测）。"""
    async with get_session_factory()() as db:
        rows = (
            await db.execute(
                text(
                    "SELECT status, COUNT(*) AS n FROM tasks "
                    "WHERE platform = :p GROUP BY status"
                ),
                {"p": settings.gateway_platform},
            )
        ).all()
    return {str(row[0]): int(row[1]) for row in rows}


async def active_counts_by_token() -> dict[str, int]:
    """活跃任务数按 token_hash 分布——**第一层**并发槽校准的事实源。

    Redis 计数会因进程崩溃/键过期漂移，定时任务拿这张表的真值回写。

    **只数真正占了第一层的任务**（``slot_flags`` 的 bit1）。这一条不可省：
    批次成员与计划任务在放行前 ``slot_flags=0``，但它们同样是
    ``status=QUEUED`` 的活跃行。按「活跃行数」统计会让一次校准把闸门计数
    拉到远超真实占用——用户提交 20 条延迟任务后计数变成 20，正常请求
    全部 429。校准本意是修漂移，口径错了反而**自造漂移**。

    ``+ 0`` 不可省（``data ->> '$.x'`` 回 LONGTEXT，字符串比较下
    ``'900' < '1000'`` 为假）；用 ``MOD`` 而非 ``&`` 是为了避开位运算符
    与 ``+`` 的优先级歧义。
    """
    async with get_session_factory()() as db:
        rows = (
            await db.execute(
                text(
                    f"""
                    SELECT data ->> '$.token_hash' AS th, COUNT(*) AS n FROM tasks
                    WHERE platform = :p AND status IN :acts
                      AND COALESCE(data ->> '$.token_hash', '') <> ''
                      AND {SQL_HOLDS_LAYER1}
                    GROUP BY th
                    """
                ).bindparams(bindparam("acts", expanding=True)),
                {"p": settings.gateway_platform, "acts": ACTIVE},
            )
        ).all()
    return {str(row[0]): int(row[1]) for row in rows}


async def active_counts_by_model_token() -> dict[tuple[str, str], int]:
    """**第二层** (模型, token) 的活跃占用——校准事实源。

    第二层自 2026-09-11 起真正被占用（此前 dispatch 硬编码
    ``limit_model_token=0``，该层从未生效）。占用意味着它也会有崩溃泄漏，
    所以必须有对应的校准口径，否则泄漏只能等 ``slot_ttl_seconds``（6h）
    自然过期——这 6 小时内该 (模型, token) 组合被判定为满而持续排队。
    """
    async with get_session_factory()() as db:
        rows = (
            await db.execute(
                text(
                    f"""
                    SELECT data ->> '$.token_hash' AS th,
                           data ->> '$.slot_model' AS m, COUNT(*) AS n FROM tasks
                    WHERE platform = :p AND status IN :acts
                      AND COALESCE(data ->> '$.token_hash', '') <> ''
                      AND COALESCE(data ->> '$.slot_model', '') <> ''
                      AND {SQL_HOLDS_LAYER2}
                    GROUP BY th, m
                    """
                ).bindparams(bindparam("acts", expanding=True)),
                {"p": settings.gateway_platform, "acts": ACTIVE},
            )
        ).all()
    return {(str(row[0]), str(row[1])): int(row[2]) for row in rows}


async def active_counts_by_model() -> dict[str, int]:
    """**第三层** 模型全局的活跃占用——校准事实源。

    这一层才是「多 key 合计不超发」的保证，泄漏后该模型在在途为 0 时仍被
    判定为满（永久卡死），所以校准不能只管第一层。
    """
    async with get_session_factory()() as db:
        rows = (
            await db.execute(
                text(
                    f"""
                    SELECT data ->> '$.slot_model' AS m, COUNT(*) AS n FROM tasks
                    WHERE platform = :p AND status IN :acts
                      AND COALESCE(data ->> '$.slot_model', '') <> ''
                      AND {SQL_HOLDS_LAYER3}
                    GROUP BY m
                    """
                ).bindparams(bindparam("acts", expanding=True)),
                {"p": settings.gateway_platform, "acts": ACTIVE},
            )
        ).all()
    return {str(row[0]): int(row[1]) for row in rows}
