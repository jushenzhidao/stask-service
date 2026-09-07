"""共享契约：状态常量与内部数据模型。

状态机：``NOT_START → IN_PROGRESS → SUCCESS / FAILURE / CANCELED``。

状态值与 new-api tasks.status 的枚举完全对齐（ADR-006）：初始态用
``NOT_START`` 而不是自造值——共享表里的行对上游工具（看板、SQL 巡检）
也应该是可读的。
"""

from __future__ import annotations

from pydantic import BaseModel

# ---------------------------------------------------------------------------
# 状态常量（与 new-api tasks.status 口径对齐；大写）
# ---------------------------------------------------------------------------

NOT_START = "NOT_START"
IN_PROGRESS = "IN_PROGRESS"
SUCCESS = "SUCCESS"
FAILURE = "FAILURE"
CANCELED = "CANCELED"

#: 活跃（非终态）——CAS 迁移的合法起点
ACTIVE: tuple[str, ...] = (NOT_START, IN_PROGRESS)
#: 终态——不可逆
TERMINAL: tuple[str, ...] = (SUCCESS, FAILURE, CANCELED)


class SubmitPlan(BaseModel):
    """提交链路的预检产物：所有校验通过后交给落库的完整参数。

    ``raw_token`` **不在**本模型内——它只经函数参数传递并写入 Redis
    令牌会话，绝不进入任何会被序列化/记录的结构（红线：sk 不落库不进日志）。
    """

    task_id: str
    token_hash: str
    model: str
    method: str
    path: str
    query: str
    headers: dict[str, str]
    body_b64: str
    body_truncated: bool = False
    upstream_base_url: str
    idempotency_key: str = ""
    callback_url: str = ""


class TaskView(BaseModel):
    """对外的非终态任务视图（202 响应体）。"""

    task_id: str
    status: str
    created_at: int = 0
