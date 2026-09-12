"""会话原语与时间/行解码：本包所有模块的共同底座（不碰业务语义）。

切片自原 `app/services/taskstore.py`；由 `app/services/taskstore/__init__.py`
统一再导出，对外契约（`taskstore.<name>`）不变。
"""

from __future__ import annotations

import json
import time

from typing import Any

from app.logging import log


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


def _row_to_dict(row: Any) -> dict[str, Any]:
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
