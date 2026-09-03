"""gunicorn 配置。

这是**唯一**允许直读 os.environ 的地方——master 进程在 app 模块加载之前
就要拿到 workers/bind，此时 pydantic-settings 单例还不存在。

``preload_app = True`` 配合 app 里的惰性单例：fork 前只加载代码不建连接，
子进程各自在自己的事件循环里创建 DB/Redis/HTTP 连接池。反过来（预热连接
后 fork）会让多个进程共享同一批 socket，行为不可预测。
"""

import multiprocessing
import os

bind = os.environ.get("BIND", "0.0.0.0:8000")
worker_class = "uvicorn.workers.UvicornWorker"
workers = int(os.environ.get("GUNICORN_WORKERS",
                             min(16, multiprocessing.cpu_count() * 2 + 1)))
preload_app = True

# /dev/shm 上的心跳文件：容器里 /tmp 可能是慢速 overlay，会导致 worker 被误杀
worker_tmp_dir = "/dev/shm"

timeout = int(os.environ.get("GUNICORN_TIMEOUT", "180"))    # 需 > 最长长轮询窗口
graceful_timeout = 30                                        # 与 compose stop_grace_period 对齐
keepalive = 5

# 周期性重启 worker，兜住任何未发现的内存增长；jitter 防止同时重启造成流量凹陷
max_requests = 5000
max_requests_jitter = 500

limit_request_line = 4094
limit_request_fields = 100
limit_request_field_size = 8190

accesslog = "-" if os.environ.get("GUNICORN_ACCESS_LOG", "0") == "1" else None
errorlog = "-"
loglevel = os.environ.get("ST_LOG_LEVEL", "info").lower()


def on_starting(server):
    server.log.info("stask-service master starting: workers=%s bind=%s", workers, bind)


def worker_exit(server, worker):
    server.log.info("worker exited: pid=%s", worker.pid)
