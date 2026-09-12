"""轻量投影白名单、`data` JSON 解码，以及 sweeper / 槽校准用的 SQL 谓词常量。

切片自原 `app/services/taskstore.py`；由 `app/services/taskstore/__init__.py`
统一再导出，对外契约（`taskstore.<name>`）不变。
"""

from __future__ import annotations

import json

from typing import Any

from ._base import _TIME_COLUMNS, as_unix_seconds


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


def _meta_row_to_dict(row: Any) -> dict[str, Any]:
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
