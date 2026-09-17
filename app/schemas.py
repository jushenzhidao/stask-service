"""共享契约：状态常量与内部数据模型。

状态机（全部取自 new-api 原生 ``TaskStatus`` 枚举，零自造值）：

    QUEUED → IN_PROGRESS → SUCCESS / FAILURE / CANCELED

与外部系统语义的对照：

    外部系统: 已提交/排队中 → 处理中       → 完成/失败/取消
    本服务:   QUEUED       → IN_PROGRESS  → SUCCESS/FAILURE/CANCELED

- 落库即 ``QUEUED``（提交链路单次 INSERT，不做 SUBMITTED→QUEUED 二段
  写——省一次 UPDATE）。「入队未确认」窗口由 dispatch_epoch=0 +
  sweep_stale 兜底重投覆盖，不需要独立状态表达；
- new-api 的 ``ToVideoStatus`` 把 QUEUED 归为 queued，共享表里的行对
  上游工具（看板、SQL 巡检）保持可读（ADR-006）。

**大小写分层（两套形态、一个事实源）**：库内与内部传递恒为大写原生枚举；
客户端与回调接收方看到的恒为小写，由 ``public_status()`` 单点转换。
"""

from __future__ import annotations

from pydantic import BaseModel

# ---------------------------------------------------------------------------
# 状态常量（new-api model.TaskStatus 原生枚举；大写）
# ---------------------------------------------------------------------------

QUEUED = "QUEUED"
IN_PROGRESS = "IN_PROGRESS"
SUCCESS = "SUCCESS"
FAILURE = "FAILURE"
CANCELED = "CANCELED"

#: 活跃（非终态）——CAS 迁移的合法起点
ACTIVE: tuple[str, ...] = (QUEUED, IN_PROGRESS)
#: worker 领取前的可取消状态（语义是「可取消」而非「待处理」——它正是 cancel
#: 与 execute 两条 CAS 路径的分界）
PENDING: tuple[str, ...] = (QUEUED,)
#: 终态——不可逆
TERMINAL: tuple[str, ...] = (SUCCESS, FAILURE, CANCELED)

#: 库内大写原生枚举 → **客户端小写形态**的**唯一**映射（勿在别处另写一份）。
#:
#: 分界很关键：``tasks`` 表与 new-api 共写，落库值与 Redis/回调无关的一切内部
#: 传递恒为大写原生枚举（ADR-006：上游看板与 SQL 巡检按大写读）；小写只发生在
#: **对外塑形**这一步，即 HTTP 响应体与回调体。因此本映射**绝不可**用于写库、
#: 写 ``statuscache`` 或参与 CAS——``status IN :froms`` 是大小写敏感的，
#: 一旦有值以小写写回，CAS 会静默匹配不到任何行（不报错，表现为状态卡死）。
_PUBLIC_STATUS: dict[str, str] = {
    QUEUED: "queued",
    IN_PROGRESS: "in_progress",
    SUCCESS: "success",
    FAILURE: "failure",
    CANCELED: "canceled",
}


def public_status(status: str) -> str:
    """对外状态形态：小写（客户端与回调接收方看到的值）。

    未知值退化为 ``lower()`` 而非报错：``tasks`` 表与 new-api 共写，上游工具
    可能写入本服务枚举之外的值，此时把原值小写返回，比让查询路径抛 500 更合适。
    """
    return _PUBLIC_STATUS.get(status, status.lower())


class SubmitPlan(BaseModel):
    """提交链路的预检产物：所有校验通过后交给落库的完整参数。

    ``raw_token`` **不在**本模型内——它只经函数参数传递并写入 Redis
    令牌会话，绝不进入任何会被序列化/记录的结构（红线：sk 不落库不进日志）。
    """

    task_id: str
    token_hash: str
    user_id: int = 0
    model: str
    method: str
    path: str
    query: str
    headers: dict[str, str]
    body: str
    """落库形态的请求体。``body_encoding`` 说明它是明文还是 gzip+base64。"""
    body_encoding: str = ""
    """``plain`` / ``gzip+b64``；空体时为空串（见 services/codec）。"""
    body_truncated: bool = False
    upstream_base_url: str
    idempotency_key: str = ""
    callback_url: str = ""
    #: 计划执行时刻（unix 秒）。``0`` = 无延迟（默认，与改造前一致）。
    #: 由路由层解析调度头算出并落库；等待期任务保持 ``QUEUED``，不引入新状态。
    scheduled_at: int = 0
    #: 客户端分批参数（R-14~R-17）。``None`` = 本次请求未声明，交服务端策略。
    #: 声明了就在这里带着走，落库与入批都用**生效值**而不是原始头。
    batch_size: int | None = None
    batch_wait: int | None = None
    batch_key: str | None = None
