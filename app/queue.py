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
- logfire：由 ``app.observability.setup`` 统一装配——官方
  ``TaskiqInstrumentor`` 把 OpenTelemetryMiddleware 插到本 broker 链头，
  经 message labels 传播 traceparent，web 的 kiq span 与 worker 的
  execute span 串成一条 trace。本模块**不再手动挂** otel middleware；
- taskiq-admin：``TaskiqAdminMiddleware``（taskiq 0.12 内置）把 queued /
  started / executed 事件推给 admin 面板，fire-and-forget 不阻塞任务执行。

定时任务（cron）四只：卡死收敛、超龄判死、槽位校准、结果清理。
全部带重入锁——worker 扩副本后 scheduler 若误起多份，锁保证同一轮只有
一个在跑。
"""

from __future__ import annotations

import dataclasses
import datetime as dt

from taskiq import Context, TaskiqDepends, TaskiqMessage, TaskiqResult, TaskiqScheduler
from taskiq.abc.middleware import TaskiqMiddleware
from taskiq.schedule_sources import LabelScheduleSource
from taskiq_redis import ListRedisScheduleSource, RedisAsyncResultBackend, RedisStreamBroker

from app.config import settings
from app.logging import log, setup_logging
from app.services.outcome import Outcome, TaskExecutionError

#: ``Outcome`` 的字段名集合——从 admin 回填的 dict 重建对象时过滤未知键，
#: 避免任务体与本模块版本不一致（滚动升级窗口）时 TypeError。
_OUTCOME_FIELDS = frozenset(f.name for f in dataclasses.fields(Outcome))

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
    # XAUTOCLAIM 扫描锁的自动释放时间：不设的话 worker 在认领扫描中途
    # 崩溃会让锁永不过期，pending 消息永久卡死（taskiq-redis 已知坑）
    unacknowledged_lock_timeout=60.0,
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


class OutcomeMiddleware(TaskiqMiddleware):
    """把任务返回的执行摘要翻译成 taskiq-admin 能显示的错误。

    背景：我们的任务体**从不抛异常**（抛 = taskiq 判失败 = 按 ack 策略重投，
    而重投正是派发锁要防的）。代价是 admin 的 ``Error`` 列恒为空、
    ``State`` 恒 success——上游 500、令牌丢失、响应超限这些真失败在面板上
    与正常成功毫无区别，排障只能回 DB 捞。

    这里在 ``post_execute`` 里读任务返回的 ``Outcome``：``ok=False`` 时给
    ``result.error`` 赋一个**不抛出**的 ``TaskExecutionError``，于是 admin
    的 Error 列有内容、State 显示 failure。

    为什么安全（已核对 taskiq 0.12 receiver 源码）：
    - ack 时机由 ``AcknowledgeType`` 决定，**不看** ``result.error``
      （``WHEN_SAVED`` 在 ``post_execute`` 之后无条件 ack）——填 error 不会重投；
    - ``post_execute`` 按 ``reversed(broker.middlewares)`` 调用，所以本
      middleware 必须**排在 admin 之后**入链，反转后才先于 admin 执行。

    ``on_error`` 不会因此被触发：它只在任务真抛异常时由 receiver 调用。
    """

    async def post_execute(self, message: TaskiqMessage, result: TaskiqResult) -> None:
        if result.error is not None:
            return  # 真异常已有错误对象，别覆盖真实堆栈
        value = result.return_value
        if not isinstance(value, dict) or value.get("ok") is not False:
            return
        outcome = Outcome(**{k: v for k, v in value.items() if k in _OUTCOME_FIELDS})
        result.error = TaskExecutionError(outcome)
        result.is_err = True


def _build_middlewares() -> list[TaskiqMiddleware]:
    """按配置装配 middleware 链。顺序：observability → admin。

    otel middleware 不在这里挂——``app.observability.setup`` 里的
    ``TaskiqInstrumentor`` 会把它插到链头（幂等防重）。
    """
    chain: list[TaskiqMiddleware] = [ObservabilityMiddleware()]

    if settings.taskiq_admin_url and settings.taskiq_admin_api_token:
        # taskiq 0.12 内置；上报是 fire-and-forget（asyncio.create_task），
        # admin 面板挂掉只会打 warning，不影响任务执行
        from taskiq.middlewares.taskiq_admin_middleware import TaskiqAdminMiddleware

        chain.append(TaskiqAdminMiddleware(
            url=settings.taskiq_admin_url,
            api_token=settings.taskiq_admin_api_token,
            taskiq_broker_name="stask",
        ))
    # 必须排在 admin 之后：post_execute 按 reversed(middlewares) 调用，
    # 本 middleware 要先于 admin 跑才能把 error 填好再被上报
    chain.append(OutcomeMiddleware())
    return chain


broker.add_middlewares(*_build_middlewares())


# ---------------------------------------------------------------------------
# 任务定义
# ---------------------------------------------------------------------------


@broker.task
async def execute_task(task_id: str, _context: Context = TaskiqDepends()) -> dict:
    """执行一个任务（设计 §5）。

    队列是 at-least-once：本函数可能被同一个 task_id 调用多次（崩溃后
    XAUTOCLAIM 重投、可见性超时）。防重的责任**全在 execute 内部的
    派发锁**上，不靠队列。

    返回执行摘要（``Outcome.as_dict()``）——taskiq 落进 result backend，
    taskiq-admin 的 ``Return Value`` 直接显示：终态、上游状态码、失败原因、
    制品数与 URL、命中的解析级别。失败摘要另由 ``OutcomeMiddleware``
    转成 admin 的 ``Error`` 列。
    """
    from app.services.execute import run

    return await run(task_id)


@broker.task
async def notify_task(task_id: str, attempt: int = 1,
                      _context: Context = TaskiqDepends()) -> dict:
    """终态回调推送（失败按指数退避重投，上限 ``CALLBACK_MAX_ATTEMPTS``）。

    返回投递摘要（delivered / rejected+HTTP 码 / transport_error / exhausted），
    admin 上可直接看出回调是被对端拒了还是根本没送到。
    """
    from app.services.notify import deliver

    return await deliver(task_id, attempt)


@broker.task
async def release_batch(model: str, source: str = "batch",
                        _context: Context = TaskiqDepends()) -> dict:
    """放行一个模型的整批任务（N 触发 / T 触发 / 人工放行共用）。

    N 触发时 web 侧只 ``kiq`` 这个任务就返回 202——**绝不在提交响应里同步
    放行整批**：一批 500 条的放行要做 500 次 DB 条件更新加 500 次占槽 EVAL，
    压在第 500 个提交者的响应延迟里，等于让最后一个提交的人替所有人付账。

    重复投递无害：``batching.claim`` 的原子摘取保证只有一方拿到成员，
    后到的那次拿到空列表直接返回。
    """
    from app.services.batching import release_model

    return await release_model(model, source=source)


@broker.task(schedule=[{"cron": "* * * * *"}])
async def tick_batches(_context: Context = TaskiqDepends()) -> dict:
    """每分钟：T 触发 + 占槽失败重排的到期扫描。

    cron 的最小粒度是 1 分钟，而 ``batch_wait`` 允许配到秒级——所以本任务
    内部**自旋多轮**（每轮间隔 ``_TICK_INTERVAL`` 秒），把有效扫描频率提到
    亚分钟级。这是为了让 ``batch_wait=30`` 这类配置的实际放行延迟不被
    cron 粒度放大到「最坏 +60s」。

    自旋总时长略短于一分钟，避免与下一轮 cron 叠在一起跑。

    **不在这里判 ``batch_enabled``**：本任务同时驱动「批次 T 触发」与「到期
    通道（计划任务到点 / 占槽失败重排）」，后者与攒批无关。在这个入口一刀
    切掉等于「关攒批止血」会连延迟任务与重排一起停掉。止血开关的去处在
    ``batching.tick_once`` 内部，那里只精确地跳过批次放行那一段。
    """
    from app.services.batching import tick

    return await tick()


@broker.task(schedule=[{"cron": "*/2 * * * *"}])
async def sweep_stale(_context: Context = TaskiqDepends()) -> dict:
    """每 2 分钟：卡死任务收敛（消息丢失重投 / 派发后失联判死）。

    Stream broker 下队列层已不丢消息，本任务退化为**第二道保险**：
    覆盖「入队调用本身失败但行已建」「stream 被人工清空」等队列外场景。

    返回本轮统计（扫描数 / 重投数 / 判死数），让 admin 上能直接看出
    「这一轮到底动了什么」——恒 0 才是健康态。
    """
    if not settings.sweep_enabled:
        return {"skipped": "sweep_disabled"}
    from app.services.sweeper import sweep_stale as run

    return await run()


@broker.task(schedule=[{"cron": "*/5 * * * *"}])
async def sweep_overdue(_context: Context = TaskiqDepends()) -> dict:
    """每 5 分钟：超龄任务判死（必须先于 new-api 的 24h 清理线收敛）。"""
    if not settings.sweep_enabled:
        return {"skipped": "sweep_disabled"}
    from app.services.sweeper import sweep_overdue as run

    return await run()


@broker.task(schedule=[{"cron": "*/5 * * * *"}])
async def sweep_slots(_context: Context = TaskiqDepends()) -> dict:
    """每 5 分钟：并发槽计数按 tasks 表事实校准。"""
    if not settings.sweep_enabled:
        return {"skipped": "sweep_disabled"}
    from app.services.sweeper import recalibrate_slots

    return await recalibrate_slots()


@broker.task(schedule=[{"cron": "17 * * * *"}])
async def sweep_results(_context: Context = TaskiqDepends()) -> dict:
    """每小时第 17 分：清理超期结果体（设计 §9）。

    错开整点：整点是各类定时任务的高峰，DB 上再叠一个批量 UPDATE 不划算。
    """
    if not settings.sweep_enabled:
        return {"skipped": "sweep_disabled"}
    from app.services.sweeper import purge_results

    return await purge_results()


# ---------------------------------------------------------------------------
# 发布门面（请求路径与业务层的唯一入队入口）
# ---------------------------------------------------------------------------


async def publish_execute(task_id: str) -> None:
    await execute_task.kiq(task_id)


async def publish_release_batch(model: str, source: str = "batch") -> None:
    """N 触发的放行入口：只投递，不在请求线程里做整批放行。"""
    await release_batch.kiq(model, source)


async def publish_notify(task_id: str, attempt: int = 1, delay_seconds: int = 0) -> None:
    """回调推送。``delay_seconds > 0`` 时走 schedule_by_time（labels delay 无效）。"""
    if delay_seconds > 0:
        when = dt.datetime.now(dt.UTC) + dt.timedelta(seconds=delay_seconds)
        await notify_task.schedule_by_time(schedule_source, when, task_id, attempt)
        return
    await notify_task.kiq(task_id, attempt)
