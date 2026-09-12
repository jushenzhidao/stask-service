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

from ._projection import (
    SQL_HOLDS_LAYER1,
    SQL_HOLDS_LAYER2,
    SQL_HOLDS_LAYER3,
    SQL_SCHEDULED_DUE_BEFORE,
    SQL_SCHEDULED_NOT_FUTURE,
    _JSON_NULL,
    _META_BOOL_KEYS,
    _META_COLUMNS,
    _META_DATA_KEYS,
    _META_INT_KEYS,
    _META_JSON_KEYS,
    _META_SELECT,
    _META_STR_KEYS,
    _META_TRISTATE_KEYS,
    _PRIVATE_STR_KEYS,
    _meta_int,
    _meta_json_list,
    _meta_row_to_dict,
    _meta_scalar,
)

from ._base import (
    _ADMIN_MAX_LIMIT,
    _ADMIN_MAX_OFFSET,
    _ADMIN_MAX_PREFIX_LENGTH,
    _ADMIN_MAX_TASK_ID_LENGTH,
    _ADMIN_MAX_WINDOW_SECONDS,
    _TIME_COLUMNS,
    _UNIX_MS_THRESHOLD,
    _escape_like_prefix,
    _log_write_error,
    _row_to_dict,
    _validate_search_params,
    as_unix_seconds,
    now,
)

from ._read import (
    active_counts_by_model,
    active_counts_by_model_token,
    active_counts_by_token,
    counts_by_status,
    exists,
    get,
    get_meta,
    get_status,
)

from ._batch import (
    batch_counts_by_model,
    batch_waiting,
    pending_scheduled,
    scheduled_overview,
)

from ._write import (
    cas,
    claim_for_release,
    create,
    patch_data,
    unclaim_for_release,
)

from ._admin_query import (
    metrics,
    search,
)

from ._sweeper import (
    overdue_active,
    purge_expired_results,
    stale_active,
)

__all__ = [
    "SQL_HOLDS_LAYER1",
    "SQL_HOLDS_LAYER2",
    "SQL_HOLDS_LAYER3",
    "SQL_SCHEDULED_DUE_BEFORE",
    "SQL_SCHEDULED_NOT_FUTURE",
    "_ADMIN_MAX_LIMIT",
    "_ADMIN_MAX_OFFSET",
    "_ADMIN_MAX_PREFIX_LENGTH",
    "_ADMIN_MAX_TASK_ID_LENGTH",
    "_ADMIN_MAX_WINDOW_SECONDS",
    "_JSON_NULL",
    "_META_BOOL_KEYS",
    "_META_COLUMNS",
    "_META_DATA_KEYS",
    "_META_INT_KEYS",
    "_META_JSON_KEYS",
    "_META_SELECT",
    "_META_STR_KEYS",
    "_META_TRISTATE_KEYS",
    "_PRIVATE_STR_KEYS",
    "_TIME_COLUMNS",
    "_UNIX_MS_THRESHOLD",
    "_escape_like_prefix",
    "_log_write_error",
    "_meta_int",
    "_meta_json_list",
    "_meta_row_to_dict",
    "_meta_scalar",
    "_row_to_dict",
    "_validate_search_params",
    "active_counts_by_model",
    "active_counts_by_model_token",
    "active_counts_by_token",
    "as_unix_seconds",
    "batch_counts_by_model",
    "batch_waiting",
    "cas",
    "claim_for_release",
    "counts_by_status",
    "create",
    "exists",
    "get",
    "get_meta",
    "get_status",
    "metrics",
    "now",
    "overdue_active",
    "patch_data",
    "pending_scheduled",
    "purge_expired_results",
    "scheduled_overview",
    "search",
    "stale_active",
    "unclaim_for_release",
]
