"""单进程模式：web + worker + scheduler 跑在同一个事件循环里。

用途：单机试用、小流量生产、本地开发。一条 `docker run` 或
`python -m app.standalone` 就能起全套，不必拉 compose、不必开三个终端。

为什么可以合在一起：worker 的活儿是 `await` 上游 HTTP（IO 密集），
和 web 请求共享事件循环不会互相饿死。真正的限制是**无法独立水平扩展**
——流量上来后 web 和 worker 需要按不同倍数扩副本，那时必须拆回 compose。

scheduler 必须单副本（多份会重复触发 cron），单进程模式天然满足。

拆分标准（到了就该换 compose）：
- 提交 QPS > 200，或
- 在途任务常态 > 500，或
- 需要滚动重启 web 而不中断在途任务的执行。
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import signal

import uvicorn

from app.config import settings
from app.logging import log, setup_logging


async def _run_worker() -> None:
    """在当前事件循环里跑 taskiq worker。

    用 receiver + broker.listen 而不是 spawn `taskiq worker` 子进程：
    子进程要各自建 DB/Redis 连接池，单机模式下白白多一倍连接。
    """
    from taskiq.api import run_receiver_task

    from app.queue import broker

    await broker.startup()
    try:
        await run_receiver_task(broker, max_async_tasks=settings.queue_concurrency)
    finally:
        with contextlib.suppress(Exception):
            await broker.shutdown()


async def _run_scheduler() -> None:
    """在当前事件循环里跑 taskiq scheduler（cron 触发源）。"""
    from taskiq.api import run_scheduler_task

    from app.queue import scheduler

    await run_scheduler_task(scheduler)


async def _run_web(stop: asyncio.Event) -> None:
    config = uvicorn.Config(
        "app.main:app",
        host=os.environ.get("HOST", "0.0.0.0"),
        port=int(os.environ.get("PORT", "8000")),
        log_config=None,                 # 日志统一走 loguru（setup_logging 已装配）
        access_log=False,
        timeout_keep_alive=15,
    )
    server = uvicorn.Server(config)
    # 信号由本模块统一处理——让 uvicorn 也装一套会互相抢，出现「Ctrl-C 一次
    # 只停了 web、worker 还在跑」的半死状态
    setattr(server, "install_signal_handlers", lambda: None)  # noqa: B010
    task = asyncio.create_task(server.serve())
    await stop.wait()
    server.should_exit = True
    await task


async def main() -> None:
    setup_logging()
    log.info(
        "stask standalone starting: env={} platform={} upstream={} admin={}",
        settings.app_env, settings.gateway_platform, settings.upstream_base_url,
        "on" if settings.admin_key else "off",
    )

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, stop.set)

    tasks = [
        asyncio.create_task(_run_web(stop), name="web"),
        asyncio.create_task(_run_worker(), name="worker"),
    ]
    if settings.sweep_enabled:
        tasks.append(asyncio.create_task(_run_scheduler(), name="scheduler"))

    done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)

    # 任一组件退出即整体收摊——半死状态（web 活着但 worker 没了）比直接
    # 退出更糟：请求照收、任务永远不执行，客户端只能看到 202 然后超时
    for task in done:
        if (exc := task.exception()) is not None:
            log.opt(exception=exc).error("component crashed: {}", task.get_name())
    stop.set()
    for task in pending:
        task.cancel()
    await asyncio.gather(*pending, return_exceptions=True)
    log.info("stask standalone stopped")


if __name__ == "__main__":
    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(main())
