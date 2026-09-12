"""new-api 现有 ``tasks`` 表的映射说明（**仅供参考，运行时不使用**）。

本服务不建任何表。实际读写全部走 ``app/services/taskstore/`` 包里的原生 SQL
（``JSON_MERGE_PATCH`` 等 MySQL 特有能力，ORM 表达不出来）。

与 new-api 的共存契约（ADR-006）：
- ``platform`` = 自定义值 ``stask``（非 suno/mj）→ GetTaskAdaptorFunc 返回 nil，
  原生任务轮询天然跳过；
- ``channel_id`` = 独立渠道号（``CHANNEL_ID``），不与上游任务混用；
- ``quota`` 恒 0 —— 零资金记账（计费由上游 relay 自理）；
- ``user_id`` = 上游鉴权回查值（tokens 表共库直查，查不到落 0）；
- ``status``/``progress`` 用 new-api 原生枚举（QUEUED/
  IN_PROGRESS/SUCCESS/FAILURE/CANCELED + 0%/100%）；
- 任务生命期（默认 6h）< new-api 的 24h 超时清理线。

时间字段是 int64 **unix 秒**。本服务写侧恒写秒（``taskstore.now()``），
所以 SQL 的时间谓词直接裸比较列以吃到索引；毫秒只可能出现在 new-api
原生任务自己的行里，而本服务的 WHERE 恒带 ``platform='stask'`` 把它们
排除在外。读侧仍经 ``taskstore.as_unix_seconds`` 归一（防手工改库）。

本服务对 MySQL 的依赖：``tasks`` 表读写 + ``AUTH_MODE=newapi`` 时
``tokens``/``users`` 只读（``upstream._fetch_credential``）。不做计费。
"""

from sqlalchemy import JSON, BigInteger, Column, String, Text
from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    ...


class Task(Base):
    __tablename__ = "tasks"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    task_id = Column(String(64), index=True)   # {model_slug}_{32hex} ≤53
    platform = Column(String(30), default="stask", index=True)
    action = Column(String(40), default="")
    status = Column(String(20), default="QUEUED", index=True)
    fail_reason = Column(Text, default="")
    progress = Column(String(20), default="0%")
    submit_time = Column(BigInteger, default=0)
    start_time = Column(BigInteger, default=0)
    finish_time = Column(BigInteger, default=0)
    created_at = Column(BigInteger, default=0)
    updated_at = Column(BigInteger, default=0)
    data = Column(JSON)
    user_id = Column(BigInteger, default=0)
    channel_id = Column(BigInteger, default=0)
    quota = Column(BigInteger, default=0)
