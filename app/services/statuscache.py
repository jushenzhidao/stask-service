"""任务状态的 Redis write-through 缓存——长轮询卸载 DB 的核心。

问题：``flow._long_poll`` 是「每个等待客户端 × 每 1~5s 一条
``SELECT status``」，几百个客户端挂长轮询时 DB QPS 随人数线性上涨。

方案：状态每次落库（create / cas 成功）时顺手写进 Redis
``st:status:{task_id}``（TTL 兜底防泄漏）。长轮询先查 Redis，未命中
（键过期/Redis 刚重启）回落 DB 一次并**回填**——同一任务的 N 个等待
客户端里只有第一个打 DB，其余都命中回填后的缓存。

一致性：Redis 只是 tasks 表的影子，**写侧永远先 DB 后缓存**；缓存写
失败静默忽略（读侧回落 DB，正确性不依赖缓存）。Redis 整体不可用时
长轮询退化回 v0.3 的 DB 轮询行为，不影响可用性。
"""

from __future__ import annotations

from app.redis import K_STATUS, r

#: 缓存 TTL（秒）。活跃任务的状态每次迁移都会刷新；终态写入后，
#: 等待中的长轮询在秒级内消费掉，1h 足够覆盖并防键泄漏。
_TTL = 3600


async def set(task_id: str, status: str) -> None:
    """写状态（先 DB 后调用；失败静默——缓存不承载正确性）。"""
    try:
        await r.set(K_STATUS.format(task_id=task_id), status, ex=_TTL)
    except Exception:
        pass


async def get(task_id: str) -> str | None:
    """读状态。None = 未命中或 Redis 不可用（调用方回落 DB）。"""
    try:
        value = await r.get(K_STATUS.format(task_id=task_id))
    except Exception:
        return None
    return None if value is None else str(value)
