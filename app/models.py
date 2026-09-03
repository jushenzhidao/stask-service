"""new-api 现有 ``tasks`` 表的映射说明（**仅供参考，运行时不使用**）。

本服务不建任何表。实际读写全部走 ``app/services/taskstore.py`` 的原生 SQL
（``JSON_MERGE_PATCH`` 等 MySQL 特有能力，ORM 表达不出来）。

关键口径：
- 时间字段是 int64 **unix 秒**（``created_at/updated_at/submit_time/
  start_time/finish_time``）——但表被 new-api 原生任务模块用 UnixMilli 写过，
  所以读侧一律经 ``taskstore.as_unix_seconds`` 归一，SQL 侧一律套
  ``taskstore._secs(col)``；
- ``progress`` 是字符串（如 ``"0%"`` / ``"100%"``）；
- ``data`` 是 JSON 列——本服务的全部扩展字段都在这里（契约见 SPEC §6）；
- ``platform`` 是三方共享表的分区依据：new-api 原生任务、atask（``gateway``）、
  本服务（``stask``）各写各的行，**写侧 WHERE 必带 platform**。

本服务对 MySQL 只有这一张表的读写依赖；``tokens`` / ``users`` 完全不碰
（身份与余额由 billing 服务的 HTTP 接口提供——红线：禁止跨服务读库）。
"""

from sqlalchemy import JSON, BigInteger, Column, String, Text
from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    ...


class Task(Base):
    __tablename__ = "tasks"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    task_id = Column(String(64), unique=True, index=True)   # {model_slug}_{uuid4hex} ≤53
    platform = Column(String(32), default="stask", index=True)
    action = Column(String(32), default="")
    status = Column(String(32), default="SUBMITTED", index=True)
    fail_reason = Column(Text, default="")
    progress = Column(String(16), default="0%")
    submit_time = Column(BigInteger, default=0)
    start_time = Column(BigInteger, default=0)
    finish_time = Column(BigInteger, default=0)
    created_at = Column(BigInteger, default=0)
    updated_at = Column(BigInteger, default=0)
    data = Column(JSON)
    user_id = Column(BigInteger, index=True)
    channel_id = Column(BigInteger, default=0)
    quota = Column(BigInteger, default=0)
