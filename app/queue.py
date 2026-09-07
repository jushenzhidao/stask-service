"""taskiq 装配：broker / scheduler / 任务定义 / 发布门面。

broker = **RedisStreamBroker**（Redis Stream + consumer group，at-least-once）：
- ListQueueBroker 的 BRPOP 取走即删——worker 崩溃时在飞消息直接蒸发，
  只能靠 sweep_stale 兜底重投（最长 2min 盲区）。Stream 版 XREADGROUP 后
  消息留在 PEL，直到执行完成才 XACK；worker 崩溃后消息经 XAUTOCLAIM
  被其他 consumer 认领重投，**队列层不丢任务**；
- ``ack_type`` 用默认 WHEN_SAVED（结果落 backend 后才 ack）——崩溃窗口内
  未 ack 的消息必然重投。重复执行无害：防重责任在 execute 的派发锁上；
- ``idle_timeout`` 必须 > 派发锁 TTL（worker_timeout + margin）：确保消息
  被 XAUTOCLAIM 重投时，上一个持锁执行要么已终态、要么锁已到期，
  重投的那次执行不会与原执行并发双跑；
- ``maxlen`` 近似裁剪防流无界增长（XACK 不删条目）——取值 >> 峰值积压。

约定（踩过的坑，别改）：
- **延迟任务必须走 ``schedule_by_time``**：``with_labels(delay=...)`` 对
  redis broker 不生效，任务会立刻执行；
- **请求路径只依赖发布门面函数**（``publish_execute`` / ``publish_notify``），
  不直接碰 broker——测试里 monkeypatch 这两个函数就能拦全部入队；
- **任务体内延迟 import** 业务模块，打破 queue ↔ services 的循环依赖；
- worker 侧的日志/observability 装配点在 ``ObservabilityMiddleware.startup``
  （web 侧在 ``create_app``）——两个进程各装配一次，格式统一。

可观测性（均可选，默认关）：
- logfire：``OpenTelemetryMiddleware``（taskiq 0.12 内置）经 message labels
  传播 traceparent，web 的 kiq span 与 worker 的 execute span 串成一条 trace；
- taskiq-admin：``TaskiqAdminMiddleware``（taskiq 0.12 内置）把 queued /
  started / executed 事件推给 admin 面板，fire-and-forget 不阻塞任务执行。

定时任务（cron）四只：卡死收敛、超龄判死、槽位校准、结果清理。
全部带重入锁——worker 扩副本后 scheduler 若误起多份，锁保证同一轮只有
一个在跑。
"""

from __future__ import annotations

import datetime as dt

from taskiq import Context, TaskiqDepends, TaskiqMessage, TaskiqResult, TaskiqScheduler
from taskiq.abc.middleware import TaskiqMiddleware
from taskiq.schedule_sources import LabelScheduleSource
from taskiq_redis import ListRedisScheduleSource, RedisAsyncResultBackend, RedisStreamBroker

from app.config import settings
from app.logging import log, setup_logging

QUEUE_NAME = f"{settings.redis_key_prefix}:taskiq"
SCHED_PREFIX = f"{settings.redis_key_prefix}:sched"

#: XAUTOCLAIM 认领阈值（毫秒）：必须 > 派发锁 TTL，见模块 docstring
_IDLE_TIMEOUT_MS = (settings.worker_timeout + settings.dispatch_lock_margin_seconds + 60) * 1000

broker = RedisStreamBroker(
    settings.redis_url,
    queue_name=QUEUE_NAME,
    consumer_group_name=f"{settings.redis_key_prefix}:workers",
    # 消费掉 worker 下线期间积压的消息："$" 只看新消息，重建 group 时会漏
    consumer_id="0",
    idle_timeout=_IDLE_TIMEOUT_MS,
    unacknowledged_batch_size=100,
    maxlen=settings.queue_stream_maxlen or None,
    approximate=True,
).with_result_backend(
    RedisAsyncResultBackend(settings.redis_url, result_ex_time=3600)
)
schedule_source = ListRedisScheduleSource(settings.redis_url, prefix=SCHED_PREFIX)
scheduler = TaskiqScheduler(broker, sources=[LabelScheduleSource(broker), schedule_source])


class ObservabilityMiddleware(TaskiqMiddleware):
    """worker 进程的日志/observability 装配点 + 任务级异常可见性。"""

    async def startup(self) -> None:
        setup_logging()
        from app.observability import setup as setup_observability

        setup_observability("worker")
        log.info("stask worker started: queue={} concurrency={}",
                 QUEUE_NAME, settings.queue_concurrency)

    async def on_error(
        self, message: TaskiqMessage, result: TaskiqResult, exception: BaseException
    ) -> None:
        log.opt(exception=exception).error(
            "task failed: name={} args={}", message.task_name, message.args
        )


def _build_middlewares() -> list[TaskiqMiddleware]:
    """按配置装配 middleware 链。顺序：observability → otel → admin。"""
    chain: list[TaskiqMiddleware] = [ObservabilityMiddleware()]

    if settings.logfire_enabled:
        # taskiq 0.12 内置；依赖 taskiq[opentelemetry] extra（otel-api + psutil）。
        # tracer 不显式传——用全局 ProxyTracer，logfire.configure 后自动生效。
        from taskiq.middlewares.opentelemetry_middleware import OpenTelemetryMiddleware

        chain.append(OpenTelemetryMiddleware())

    if settings.taskiq_admin_url and settings.taskiq_admin_api_token:
        # taskiq 0.12 内置；上报是 fire-and-forget（asyncio.create_task），
        # admin 面板挂掉只会打 warning，不影响任务执行
        from taskiq.middlewares.taskiq_admin_middleware import TaskiqAdminMiddleware

        chain.append(TaskiqAdminMiddleware(
            url=settings.taskiq_admin_url,
            api_token=settings.taskiq_admin_api_token,
            taskiq_broker_name="stask",
        ))
    return chain


broker.add_middlewares(*_build_middlewares())


# ---------------------------------------------------------------------------
# 任务定义
# ---------------------------------------------------------------------------


@broker.task
async def execute_task(task_id: str, _context: Context = TaskiqDepends()) -> None:
    """执行一个任务（设计 §5）。

    队列是 at-least-once：本函数可能被同一个 task_id 调用多次（崩溃后
    XAUTOCLAIM 重投、可见性超时）。防重的责任**全在 execute 内部的
    派发锁**上，不靠队列。
    """
    from app.services.execute import run

    await run(task_id)


@broker.task
async def notify_task(task_id: str, attempt: int = 1,
                      _context: Context = TaskiqDepends()) -> None:
    """终态回调推送（失败按指数退避重投，上限 ``ST_CALLBACK_MAX_ATTEMPTS``）。"""
    from app.services.notify import deliver

    await deliver(task_id, attempt)


@broker.task(schedule=[{"cron": "*/2 * * * *"}])
async def sweep_stale(_context: Context = TaskiqDepends()) -> None:
    """每 2 分钟：卡死任务收敛（消息丢失重投 / 派发后失联判死）。

    Stream broker 下队列层已不丢消息，本任务退化为**第二道保险**：
    覆盖「入队调用本身失败但行已建」「stream 被人工清空」等队列外场景。
    """
    if not settings.sweep_enabled:
        return
    from app.services.sweeper import sweep_stale as run

    await run()


@broker.task(schedule=[{"cron": "*/5 * * * *"}])
async def sweep_overdue(_context: Context = TaskiqDepends()) -> None:
    """每 5 分钟：超龄任务判死（必须先于 new-api 的 24h 清理线收敛）。"""
    if not settings.sweep_enabled:
        return
    from app.services.sweeper import sweep_overdue as run

    await run()


@broker.task(schedule=[{"cron": "*/5 * * * *"}])
async def sweep_slots(_context: Context = TaskiqDepends()) -> None:
    """每 5 分钟：并发槽计数按 tasks 表事实校准。"""
    if not settings.sweep_enabled:
        return
    from app.services.sweeper import recalibrate_slots

    await recalibrate_slots()


@broker.task(schedule=[{"cron": "17 * * * *"}])
async def sweep_results(_context: Context = TaskiqDepends()) -> None:
    """每小时第 17 分：清理超期结果体（设计 §9）。

    错开整点：整点是各类定时任务的高峰，DB 上再叠一个批量 UPDATE 不划算。
    """
    if not settings.sweep_enabled:
        return
    from app.services.sweeper import purge_results

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
