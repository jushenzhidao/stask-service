"""tasks 表读写（复用 new-api 现有表，本服务零建表）。

三条铁律：

1. **扩展字段全部在 ``data`` JSON 列**，用 ``JSON_MERGE_PATCH`` 合并，
   绝不 ALTER 表结构。
2. **状态迁移一律 CAS**：``rowcount == 1`` 才算抢到推进权（恰好一次语义）。
3. **读写侧 WHERE 必带 ``platform``**：tasks 是共享表（new-api 原生任务 +
   本服务 ``stask``），绝不动别人的行。

与 new-api 的共存契约（ADR-006）：
- ``platform`` = 自定义值（非 suno/mj）→ ``GetTaskAdaptorFunc`` 返回 nil，
  原生任务轮询天然跳过本服务的行；
- ``channel_id`` = 独立渠道号（``CHANNEL_ID``），不与上游任务混用；
- ``quota`` 恒 0 —— 本服务不做计费，上游超时清理即便动到我们的行，
  退款金额也是 0，零资金影响；
- ``status``/``progress`` 用 new-api 原生枚举（``QUEUED``/``0%``/``100%``），
  共享表里的行对上游工具（看板、SQL 巡检）保持可读；
- 任务生命期（``TASK_MAX_LIFETIME_SECONDS``，默认 6h）必须远小于
  new-api 的 24h 超时清理线——我们先于上游收敛自己的行。

时间口径（v0.3.2 收紧）：**本服务写入的时间列恒为 unix 秒**，且所有
SQL 的 WHERE 恒带 ``platform = :p``（只扫自家行），因此 SQL 侧时间谓词
一律**裸列比较**——原先的 ``IF(col > 1e11, col DIV 1000, col)`` 包裹会
让 sweeper / 看板的 range 条件吃不到索引，退化为按 platform 过滤后的
全量扫描。毫秒值只可能出现在 new-api 自己写的行里，那些行我们碰不到。
读侧仍保留 ``as_unix_seconds`` 归一，用于兜底展示上游写入的历史行。
"""

from __future__ import annotations

import json
import time
from typing import Any, cast

from sqlalchemy import CursorResult, bindparam, text

from app.config import settings
from app.db import get_session_factory
from app.logging import log
from app.schemas import ACTIVE, TERMINAL


def _log_write_error(op: str, task_id: str) -> None:
    """落表异常在源头记录（含堆栈与 SQL 操作名）后原样上抛。

    tasks 表写失败（连接池耗尽/锁等待超时/约束冲突）曾经完全无日志——
    上层只看得到 500 或任务卡死，排障无从下手。这里统一补齐：
    调用方只需记录控制流后果，不必重复打异常堆栈。
    """
    log.bind(taskstore_op=op, task_id=task_id).opt(exception=True).error(
        "taskstore write failed: op={} task_id={}", op, task_id,
    )


def now() -> int:
    return int(time.time())


#: 超过该阈值（1e11 秒 ≈ 5138 年）视为混入的毫秒时间戳
_UNIX_MS_THRESHOLD = 100_000_000_000

#: 管理看板查询边界，避免无界分页和无法使用索引的模糊 task_id 查询。
_ADMIN_MAX_WINDOW_SECONDS = 7 * 86400
_ADMIN_MAX_LIMIT = 200
_ADMIN_MAX_OFFSET = 10_000
_ADMIN_MAX_TASK_ID_LENGTH = 64
_ADMIN_MAX_PREFIX_LENGTH = 64


def _escape_like_prefix(value: str) -> str:
    """转义 LIKE 元字符，同时保留 task_id 常见的下划线前缀。"""
    return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


def _validate_search_params(
    *,
    task_id: str,
    task_id_prefix: str,
    since_seconds: int,
    limit: int,
    offset: int,
) -> None:
    if task_id and task_id_prefix:
        raise ValueError("task_id and task_id_prefix cannot be used together")
    if len(task_id) > _ADMIN_MAX_TASK_ID_LENGTH:
        raise ValueError("task_id exceeds 64 characters")
    if len(task_id_prefix) > _ADMIN_MAX_PREFIX_LENGTH:
        raise ValueError("task_id_prefix exceeds 64 characters")
    if any(char in task_id for char in ("%", "\\")):
        raise ValueError("task_id must be an exact identifier")
    if any(char in task_id_prefix for char in ("%", "\\")):
        raise ValueError("task_id_prefix must not contain wildcard characters")
    if not 0 <= since_seconds <= _ADMIN_MAX_WINDOW_SECONDS:
        raise ValueError("since must be between 0 and 604800 seconds")
    if not 1 <= limit <= _ADMIN_MAX_LIMIT:
        raise ValueError("limit must be between 1 and 200")
    if not 0 <= offset <= _ADMIN_MAX_OFFSET:
        raise ValueError("offset must be between 0 and 10000")

#: tasks 表的全部时间列（读侧归一的作用面）
_TIME_COLUMNS = ("submit_time", "start_time", "finish_time", "created_at", "updated_at")


def as_unix_seconds(value: Any) -> int:
    """时间值归一为 unix 秒：毫秒时间戳折算，缺失/非法 → 0。"""
    try:
        ts = int(value or 0)
    except (TypeError, ValueError):
        return 0
    if ts > _UNIX_MS_THRESHOLD:
        ts //= 1000
    return ts


def _row_to_dict(row: Any) -> dict:
    result = dict(row)
    data = result.get("data")
    if isinstance(data, dict):
        result["data"] = data
    elif isinstance(data, str):
        try:
            result["data"] = json.loads(data)
        except ValueError:
            result["data"] = {}
    elif data is None:
        result["data"] = {}
    for col in _TIME_COLUMNS:
        if col in result:
            result[col] = as_unix_seconds(result[col])
    return result


# ---------------------------------------------------------------------------
# 写
# ---------------------------------------------------------------------------


async def create(task_id: str, action: str, data: dict, user_id: int = 0) -> None:
    """落库 QUEUED（ADR-006 契约行）。

    - 直接落 ``QUEUED`` 而不是先 SUBMITTED 再 CAS：省掉每次提交的第二次
      DB UPDATE。「入队未确认」窗口的两种失败都有兜底——入队调用失败时
      提交链路当场 CAS 判死；进程在入队前崩溃时行停在 QUEUED 且
      dispatch_epoch=0，sweep_stale 按「锁不在 + 从未派发」重投；
    - ``channel_id`` = 独立渠道号（``CHANNEL_ID``）；
    - ``quota`` 恒 0 —— 零资金记账，计费由上游 relay 自理，上游即便
      误动本行退款也是 0；
    - ``user_id`` = 鉴权直查值（查不到落 0，仅归属信息）。
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
    patch: dict | None = None,
    fail_reason: str = "",
) -> bool:
    """CAS 状态迁移。返回 True = 本调用者抢到推进权（负责释放槽/清会话/回调）。

    - 终态一律把 ``progress`` 置 ``100%``（不只 SUCCESS）——失败/取消停在
      ``0%`` 会让看板与客户端以为任务还在跑；
    - 终态一律用**秒**刷 ``finish_time``；``IN_PROGRESS`` 顺带刷 ``start_time``；
    - ``fail_reason`` 截断到 500 字符。
    """
    ts = now()
    terminal = 1 if to_status in TERMINAL else 0
    running = 1 if to_status == "IN_PROGRESS" else 0
    stmt = text(
        """
        UPDATE tasks
        SET status = :to,
            updated_at = :now,
            start_time = IF(:running = 1, :now, start_time),
            finish_time = IF(:terminal = 1, :now, finish_time),
            progress = IF(:terminal = 1, '100%', progress),
            fail_reason = :reason,
            data = JSON_MERGE_PATCH(COALESCE(data, JSON_OBJECT()), CAST(:patch AS JSON))
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


async def patch_data(task_id: str, patch: dict) -> None:
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


# ---------------------------------------------------------------------------
# 读
# ---------------------------------------------------------------------------


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


async def get(task_id: str) -> dict | None:
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


#: 轻量投影的表列清单（显式列出，绝不 ``SELECT *``）
_META_COLUMNS = (
    "task_id", "status", "fail_reason", "progress", "channel_id", "user_id",
    "submit_time", "start_time", "finish_time", "created_at", "updated_at",
)

#: 轻量投影从 ``data`` 取的键，按目标类型分组。
#: ``data ->> '$.x'`` 恒返回**字符串**，每一组都必须显式归一，否则
#: ``'false'`` 这种真值字符串会让调用方的布尔判断永远成立。
_META_STR_KEYS = (
    "model", "request_method", "request_path", "request_query",
    "upstream_base_url", "upstream_content_type",
    "idempotency_key", "callback_url", "token_hash",
    "result_url", "artifact_parser",
)
_META_INT_KEYS = ("upstream_status", "response_bytes", "dispatch_epoch", "artifact_count")
_META_BOOL_KEYS = ("result_purged", "body_truncated")
#: JSON 数组键：``data ->> '$.artifacts'`` 回的是 JSON **文本**，
#: 必须再 loads 一次才是列表，否则看板拿到的是一串转义字符串。
_META_JSON_KEYS = ("artifacts",)
#: 三态（None = 从未回调过 / True = 已送达 / False = 重试耗尽）
_META_TRISTATE_KEYS = ("callback_delivered",)

_META_DATA_KEYS = (
    _META_STR_KEYS
    + _META_INT_KEYS
    + _META_BOOL_KEYS
    + _META_TRISTATE_KEYS
    + _META_JSON_KEYS
)

#: ``JSON_UNQUOTE`` 对 JSON null 返回字面量字符串 'null'，等价于缺失
_JSON_NULL = "null"

_META_SELECT = ", ".join(
    [*_META_COLUMNS]
    + [f"data ->> '$.{key}' AS {key}" for key in _META_DATA_KEYS]
)


def _meta_scalar(value: Any) -> Any:
    return None if value is None or value == _JSON_NULL else value


def _meta_int(value: Any) -> int:
    try:
        return int(str(_meta_scalar(value) or 0))
    except (TypeError, ValueError):
        return 0


def _meta_json_list(value: Any) -> list[dict[str, Any]]:
    """JSON 数组键的读侧归一：JSON 文本 → 元素为对象的列表。

    ``data ->> '$.artifacts'`` 回的是序列化文本（缺失时为 None 或字面量
    ``'null'``）。解析失败/非数组/元素非对象一律回空列表——看板「制品」列
    宁可显示"无"，也不能因为一行脏数据把整页查询打成 500。
    """
    scalar = _meta_scalar(value)
    if scalar is None:
        return []
    if isinstance(scalar, list):
        return [item for item in scalar if isinstance(item, dict)]
    try:
        parsed = json.loads(str(scalar))
    except ValueError:
        return []
    if not isinstance(parsed, list):
        return []
    return [item for item in parsed if isinstance(item, dict)]


def _meta_row_to_dict(row: Any) -> dict:
    raw = dict(row)
    result: dict[str, Any] = {col: raw.get(col) for col in _META_COLUMNS}
    for col in _TIME_COLUMNS:
        result[col] = as_unix_seconds(result.get(col))
    result["task_id"] = str(result.get("task_id") or "")
    result["status"] = str(result.get("status") or "")
    result["fail_reason"] = str(result.get("fail_reason") or "")
    result["progress"] = str(result.get("progress") or "")
    result["channel_id"] = _meta_int(result.get("channel_id"))
    result["user_id"] = _meta_int(result.get("user_id"))

    data: dict[str, Any] = {}
    for key in _META_STR_KEYS:
        data[key] = str(_meta_scalar(raw.get(key)) or "")
    for key in _META_INT_KEYS:
        data[key] = _meta_int(raw.get(key))
    for key in _META_BOOL_KEYS:
        data[key] = _meta_scalar(raw.get(key)) == "true"
    for key in _META_TRISTATE_KEYS:
        value = _meta_scalar(raw.get(key))
        data[key] = None if value is None else value == "true"
    for key in _META_JSON_KEYS:
        data[key] = _meta_json_list(raw.get(key))
    result["data"] = data
    return result


async def get_meta(task_id: str) -> dict | None:
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
    """活跃任务数按 token_hash 分布——并发槽校准的**事实源**。

    Redis 计数会因进程崩溃/键过期漂移，定时任务拿这张表的真值回写。
    """
    async with get_session_factory()() as db:
        rows = (
            await db.execute(
                text(
                    """
                    SELECT data ->> '$.token_hash' AS th, COUNT(*) AS n FROM tasks
                    WHERE platform = :p AND status IN :acts
                      AND COALESCE(data ->> '$.token_hash', '') <> ''
                    GROUP BY th
                    """
                ).bindparams(bindparam("acts", expanding=True)),
                {"p": settings.gateway_platform, "acts": ACTIVE},
            )
        ).all()
    return {str(row[0]): int(row[1]) for row in rows}


async def stale_active(stale_seconds: int, limit: int = 200) -> list[dict]:
    """长时间未更新的非终态任务（消息丢失/worker 崩溃/超期的兜底扫描）。

    直接回**元数据行**而不是 task_id 列表：sweeper 判死/重投都要读
    ``status`` 与 ``data.dispatch_epoch``，回 id 会让它每条再补一次
    ``get_meta``（200 条批 = 200 条额外查询）。投影口径与 get_meta 一致，
    不碰 ``upstream_response`` / ``request_body`` 这些大字段。
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


async def overdue_active(lifetime_seconds: int, limit: int = 200) -> list[dict]:
    """超过最大生命期仍非终态的任务（按 created_at 判定，无条件判死对象）。

    同 ``stale_active``：回元数据行，省掉 sweeper 侧的逐条回查。
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


async def search(
    *,
    status: str = "",
    model: str = "",
    task_id: str = "",
    task_id_prefix: str = "",
    since_seconds: int = 0,
    limit: int = 50,
    offset: int = 0,
) -> dict:
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

    async def _select() -> list:
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
                               data ->> '$.result_purged'    AS result_purged
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
        items.append(item)

    return {"total": int(total), "items": items,
            "limit": limit, "offset": offset}


async def metrics(window_seconds: int = 3600) -> dict:
    """看板概览指标：窗口内状态分布、失败原因 TopN、模型分布、耗时分位。"""
    if not 60 <= window_seconds <= _ADMIN_MAX_WINDOW_SECONDS:
        raise ValueError("window must be between 60 and 604800 seconds")
    since = now() - window_seconds
    p = settings.gateway_platform

    async def _query_status() -> list:
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

    async def _query_failures() -> list:
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

    async def _query_models() -> list:
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


async def purge_expired_results(ttl_seconds: int, limit: int = 200) -> int:
    """清空超期结果体（设计 §9）：只置空 ``upstream_response``，状态行保留。

    ``JSON_SET`` 而非 ``JSON_REMOVE``：留一个空串 + ``result_purged`` 标记，
    查询端据此返回 410（"结果已过期"）而不是 404（"任务不存在"）。
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
