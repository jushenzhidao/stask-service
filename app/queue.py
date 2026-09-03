"""taskiq 装配：broker / scheduler / 任务定义 / 发布门面。

约定（踩过的坑，别改）：
- **延迟任务必须走 ``schedule_by_time``**：``with_labels(delay=...)`` 对
  ListQueueBroker 不生效，任务会立刻执行；
- **请求路径只依赖发布门面函数**（``publish_execute`` / ``publish_notify``），
  不直接碰 broker——测试里 monkeypatch 这两个函数就能拦全部入队；
- **任务体内延迟 import** 业务模块，打破 queue ↔ services 的循环依赖；
- worker 侧的日志装配点在 ``ObservabilityMiddleware.startup``（web 侧在
  ``create_app``）——两个进程各装配一次，格式统一。

定时任务（cron）三只：对账、槽位校准、结果清理。全部带重入锁——
worker 扩副本后 scheduler 若误起多份，锁保证同一轮只有一个在跑。
"""

from __future__ import annotations

import datetime as dt

from taskiq import Context, TaskiqDepends, TaskiqMessage, TaskiqResult
from taskiq.abc.middleware import TaskiqMiddleware
from taskiq.schedule_sources import LabelScheduleSource
from taskiq_redis import ListQueueBroker, RedisAsyncResultBackend, RedisScheduleSource
from taskiq import TaskiqScheduler

from app.config import settings
from app.logging import log, setup_logging

QUEUE_NAME = f"{settings.redis_key_prefix}:taskiq"
SCHED_PREFIX = f"{settings.redis_key_prefix}:sched"

broker = ListQueueBroker(settings.redis_url, queue_name=QUEUE_NAME)
broker = broker.with_result_backend(
    RedisAsyncResultBackend(settings.redis_url, result_ex_time=3600)
)
schedule_source = RedisScheduleSource(settings.redis_url, prefix=SCHED_PREFIX)
scheduler = TaskiqScheduler(broker, sources=[LabelScheduleSource(broker), schedule_source])


class ObservabilityMiddleware(TaskiqMiddleware):
    """worker 进程的日志装配点 + 任务级异常可见性。"""

    async def startup(self) -> None:
        setup_logging()
        log.info("stask worker started: queue={} concurrency={}",
                 QUEUE_NAME, settings.queue_concurrency)

    async def on_error(
        self, message: TaskiqMessage, result: TaskiqResult, exception: BaseException
    ) -> None:
        log.opt(exception=exception).error(
            "task failed: name={} args={}", message.task_name, message.args
        )


broker.add_middlewares(ObservabilityMiddleware())


# ---------------------------------------------------------------------------
# 任务定义
# ---------------------------------------------------------------------------


@broker.task
async def execute_task(task_id: str, _context: Context = TaskiqDepends()) -> None:
    """执行一个任务（设计 §5）。

    队列是 at-least-once：本函数可能被同一个 task_id 调用多次（崩溃重投、
    可见性超时）。防重的责任**全在 execute 内部的派发锁**上，不靠队列。
    """
    from app.services.execute import run

    await run(task_id)


@broker.task
async def notify_task(task_id: str, attempt: int = 1,
                      _context: Context = TaskiqDepends()) -> None:
    """终态回调推送（失败按指数退避重投，上限 ``ST_CALLBACK_MAX_ATTEMPTS``）。"""
    from app.services.notify import deliver

    await deliver(task_id, attempt)


@broker.task(schedule=[{"cron": "*/1 * * * *"}])
async def sweep_reconcile(_context: Context = TaskiqDepends()) -> None:
    """每分钟：超时挂起任务对账（设计 §8）。"""
    if not settings.sweep_enabled:
        return
    from app.services.reconcile import run_reconcile

    await run_reconcile()


@broker.task(schedule=[{"cron": "*/5 * * * *"}])
async def sweep_slots(_context: Context = TaskiqDepends()) -> None:
    """每 5 分钟：并发槽计数按 tasks 表事实校准。"""
    if not settings.sweep_enabled:
        return
    from app.services.reconcile import recalibrate_slots

    await recalibrate_slots()


@broker.task(schedule=[{"cron": "*/2 * * * *"}])
async def sweep_stale(_context: Context = TaskiqDepends()) -> None:
    """每 2 分钟：卡死任务兜底扫描。

    补的是「入队消息丢失」这条路径——任务落库了但队列消息没了，worker
    永不执行，而对账只扫被标记 ``reconcile_pending`` 的行，扫不到它。
    没有这只 sweeper，那行会永久停在 SUBMITTED 并吃掉一个并发额度。
    """
    if not settings.sweep_enabled:
        return
    from app.services.reconcile import sweep_stale as run

    await run()


@broker.task(schedule=[{"cron": "17 * * * *"}])
async def sweep_results(_context: Context = TaskiqDepends()) -> None:
    """每小时第 17 分：清理超期结果体（设计 §9）。

    错开整点：整点是各类定时任务的高峰，DB 上再叠一个批量 UPDATE 不划算。
    """
    if not settings.sweep_enabled:
        return
    from app.services.reconcile import purge_results

    await purge_results()


# ---------------------------------------------------------------------------
# 发布门面（请求路径与业务层的唯一入队入口）
# ---------------------------------------------------------------------------


async def publish_execute(task_id: str) -> None:
    await execute_task.kiq(task_id)


async def publish_notify(task_id: str, attempt: int = 1, delay_seconds: int = 0) -> None:
    """回调推送。``delay_seconds > 0`` 时走 schedule_by_time（labels delay 无效）。"""
    if delay_seconds > 0:
        when = dt.datetime.now(dt.UTC) + dt.timedelta(seconds=delay_seconds)
        await notify_task.schedule_by_time(schedule_source, when, task_id, attempt)
        return
    await notify_task.kiq(task_id, attempt)
