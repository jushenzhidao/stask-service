"""共享契约：状态常量与内部数据模型。

状态机（设计 §1）：``SUBMITTED → IN_PROGRESS → SUCCESS / FAILURE / CANCELED``。
比 atask 少了 QUEUED 与 HELD——本服务的上游是**同步接口**，没有"已提交上游、
等待推进"的中间态，也没有冻结因而没有挂起收口需求。
"""

from __future__ import annotations

from pydantic import BaseModel

# ---------------------------------------------------------------------------
# 状态常量（与 new-api tasks.status 口径对齐；大写）
# ---------------------------------------------------------------------------

SUBMITTED = "SUBMITTED"
IN_PROGRESS = "IN_PROGRESS"
SUCCESS = "SUCCESS"
FAILURE = "FAILURE"
CANCELED = "CANCELED"

#: 活跃（非终态）——CAS 迁移的合法起点
ACTIVE: tuple[str, ...] = (SUBMITTED, IN_PROGRESS)
#: 终态——不可逆
TERMINAL: tuple[str, ...] = (SUCCESS, FAILURE, CANCELED)


class UserIdentity(BaseModel):
    """billing ``/api/v1/auth/inspect`` 的解析结果（令牌即身份）。"""

    user_id: int
    token_id: int = 0


class SubmitPlan(BaseModel):
    """提交链路的预检产物：所有校验通过后交给落库的完整参数。

    ``raw_token`` **不在**本模型内——它只经函数参数传递并写入 Redis
    令牌会话，绝不进入任何会被序列化/记录的结构（红线：sk 不落库不进日志）。
    """

    task_id: str
    user_id: int
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
