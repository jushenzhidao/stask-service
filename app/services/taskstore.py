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
from app.schemas import ACTIVE, QUEUED, TERMINAL


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
    patch: dict | None = None,
    fail_reason: str = "",
    private_patch: dict | None = None,
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


async def batch_waiting(limit: int = 500) -> list[dict]:
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


async def pending_scheduled(now: int, limit: int = 500) -> list[dict]:
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


async def scheduled_overview(now: int, limit: int = 500) -> list[dict]:
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
    #: 体的落库形态（plain / gzip+b64）——排障时决定该怎么读原文
    "request_body_encoding", "upstream_response_encoding",
    #: 占槽/攒批/延迟状态。**必须投影**：sweeper 判死与 flow 取消拿到的都是
    #: 元数据行，缺这几项它们就没法按掩码释放（一层不还 = 槽永久泄漏），
    #: 也认不出「攒批等待期」而把还在等放行的任务当僵尸判死。
    "slot_model", "batch_state", "batch_key",
)
_META_INT_KEYS = ("upstream_status", "response_bytes", "dispatch_epoch", "artifact_count",
                  #: 占位掩码（1=token / 2=(模型,token) / 4=模型全局）。
                  #: ``->>`` 恒回字符串，必须走 int 组归一，否则终态释放拿到
                  #: ``'1'`` 这种字符串做位运算会静默算错层。
                  "slot_flags",
                  #: 计划执行时刻。sweeper 的豁免谓词与 `dispatch.release` 的
                  #: 「未到点不放行」判定都要读它；缺了就退化成「所有任务都算
                  #: 已到点」，延迟语义失效。
                  "scheduled_at",
                  #: 退避次数。`dispatch.requeue` 每次都要读它来算下次退避时长，
                  #: 而 requeue 在闸门饱和时是每轮每任务都跑的热路径。
                  "requeue_attempts")
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

#: ``private_data`` 里允许被读出并展示的键（**白名单**）。#: 该列是 new-api 的 ``TaskPrivateData``，宿主自己都标了 ``json:"-"``
#: ——里面可能有渠道 ``key``（Gemini/Vertex 渠道会写）。所以这里逐键取，
#: 绝不 ``SELECT private_data``：一旦整列返回，宿主哪天往里加个敏感字段，
#: 我们的管理面就跟着泄露，而且没人会想起来这里有个出口。
_PRIVATE_STR_KEYS = ("result_url", "upstream_task_id")

#: ``JSON_UNQUOTE`` 对 JSON null 返回字面量字符串 'null'，等价于缺失
_JSON_NULL = "null"

# ---- 计划任务豁免谓词（stale_active / overdue_active 共用）----
#
# 这两条谓词只活在 SQL 里，单测跑不到（测试用手写 InMemoryTaskStore，它有一份
# 独立的等价实现）。所以把它们提成常量：
#   1. 避免两处 SQL 各自写歪；
#   2. 让 test_schedule 能用一条**结构断言**守住「豁免被误删」——那是一个
#      会让延迟语义当场失效、且单测完全看不见的改动。
#
# 语义：``scheduled_at`` 尚未到达 = 这条任务还在正常等待，不算「卡死/超龄」。
# ``+ 0`` 不可省：``data ->> '$.x'`` 回 LONGTEXT，直接与整数比较会退化成
# 字符串比较（``'900' < '1000'`` 为假）。
SQL_SCHEDULED_NOT_FUTURE = "COALESCE(data ->> '$.scheduled_at', 0) + 0 <= :cutoff"
#: 生命期判断：计时起点是 ``max(created_at, scheduled_at)``，等价于两个谓词
#: 合取（``max(a,b) < c ⟺ a < c AND b < c``，是等价变换不是近似）。这里是与
#: ``created_at < :cutoff`` 合取的那一半。
SQL_SCHEDULED_DUE_BEFORE = "COALESCE(data ->> '$.scheduled_at', 0) + 0 < :cutoff"

# ---- 槽校准的「是否真持该层」谓词（掩码位判定）----
#
# 掩码 1=第一层 / 2=第二层 / 4=第三层。用 MOD 而不是 `&`：MySQL 里位运算符与
# `+` 的优先级容易读错，MOD 是函数调用，语义无歧义。
# 等待期任务（批次成员、计划任务）掩码为 0，必须被这三条谓词全部排除——
# 否则校准会把「没占槽的任务」算成占用，把闸门往紧里拉（自造漂移）。
SQL_HOLDS_LAYER1 = "MOD(COALESCE(data ->> '$.slot_flags', 0) + 0, 2) = 1"
SQL_HOLDS_LAYER2 = (
    "MOD(FLOOR((COALESCE(data ->> '$.slot_flags', 0) + 0) / 2), 2) = 1"
)
SQL_HOLDS_LAYER3 = "COALESCE(data ->> '$.slot_flags', 0) + 0 >= 4"

_META_SELECT = ", ".join(
    [*_META_COLUMNS]
    + [f"data ->> '$.{key}' AS {key}" for key in _META_DATA_KEYS]
    + [
        f"private_data ->> '$.{key}' AS private_{key}"
        for key in _PRIVATE_STR_KEYS
    ]
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
    #: 与 ``data`` 平级的独立键：``private_data`` 的白名单投影。
    #: 放在单独的 dict 里而不是混进 ``data``，是为了让调用方看得出
    #: 「这个值来自 new-api 的原生列，不是我们自己的扩展字段」。
    result["private_data"] = {
        key: str(_meta_scalar(raw.get(f"private_{key}")) or "")
        for key in _PRIVATE_STR_KEYS
    }
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


async def stale_active(stale_seconds: int, limit: int = 200) -> list[dict]:
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


async def overdue_active(lifetime_seconds: int, limit: int = 200) -> list[dict]:
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
