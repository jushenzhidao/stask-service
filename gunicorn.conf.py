"""gunicorn 配置。

这是**唯一**允许直读 os.environ 的地方——master 进程在 app 模块加载之前
就要拿到 workers/bind，此时 pydantic-settings 单例还不存在。
代价是：本机 ``make run`` 不会加载 .env，这里读到的 ST_* 全是默认值，
只有容器里（compose 的 env_file）才是真实值——推导结果以容器为准。

``preload_app = True`` 配合 app 里的惰性单例：fork 前只加载代码不建连接，
子进程各自在自己的事件循环里创建 DB/Redis/HTTP 连接池。反过来（预热连接
后 fork）会让多个进程共享同一批 socket，行为不可预测。

高可用三条硬约束（下面每一项都由它推导，别写死数字）：
1. **worker 数由 DB 连接预算反推**——tasks 表与 new-api 共享同一个 MySQL，
   打爆 max_connections 会连累上游，属于级联故障。CPU 核心数只是上限不是依据；
2. **timeout > 最长合法请求**（长轮询 ``?wait=``），否则正常请求会被
   当成卡死 worker 杀掉；
3. **graceful_timeout ≥ 最长合法请求**，否则滚动重启/缩容时在飞的长轮询
   被硬截断。它还必须 < compose 的 stop_grace_period，不然 Docker 先 SIGKILL。
"""

import multiprocessing
import os


def _env_int(name: str, default: int) -> int:
    """读整型 env；非法值回落默认（运维手敲错了不至于直接起不来）。"""
    try:
        return int(os.environ.get(name, default))
    except (TypeError, ValueError):
        return default


bind = os.environ.get("BIND", "0.0.0.0:8000")
worker_class = "uvicorn.workers.UvicornWorker"
preload_app = True
proc_name = os.environ.get("GUNICORN_PROC_NAME", "stask-service")

# ---- 约束 1：worker 数由 DB 连接预算反推 ----
# 每个 worker 独占一个连接池（preload 只加载代码，连接池在 fork 后各自创建）。
_DB_PER_WORKER = max(1, _env_int("ST_DB_POOL_SIZE", 10) + _env_int("ST_DB_MAX_OVERFLOW", 10))
# MySQL 实例实际的 max_connections；共享实例上按低的那个填
_DB_BUDGET = _env_int("ST_DB_MAX_CONNECTIONS", 151)
# 分给 web 的份额，剩下的留给 worker 进程、new-api 自己和管理连接
_DB_WEB_SHARE = float(os.environ.get("ST_DB_WEB_CONNECTION_SHARE", "0.5"))
_BY_BUDGET = max(1, int(_DB_BUDGET * _DB_WEB_SHARE) // _DB_PER_WORKER)
_BY_CPU = min(16, multiprocessing.cpu_count() * 2 + 1)
# 最少 2 个：单 worker 在 max_requests 回收或崩溃重启的窗口里就是单点
workers = _env_int("GUNICORN_WORKERS", 0) or max(2, min(_BY_CPU, _BY_BUDGET))

# ---- 约束 2 / 3：超时由长轮询窗口推导 ----
_POLL_WAIT = _env_int("ST_POLL_WAIT_MAX_SECONDS", 60)
# +120 给上游/DB 抖动留余量：60s 轮询 → 180s；调到 120s 轮询 → 240s
timeout = _env_int("GUNICORN_TIMEOUT", 0) or max(180, _POLL_WAIT + 120)
# +15 让在飞的长轮询走完再退；再夹一层 timeout-5 防止两者倒挂
graceful_timeout = min(
    _env_int("GUNICORN_GRACEFUL_TIMEOUT", 0) or (_POLL_WAIT + 15),
    max(5, timeout - 5),
)

# 突发排队的接纳队列；nginx 那层也要接得住，否则排队发生在内核而非这里
backlog = _env_int("GUNICORN_BACKLOG", 2048)
keepalive = _env_int("GUNICORN_KEEPALIVE", 5)

# /dev/shm 上的心跳文件：容器里 /tmp 可能是慢速 overlay，会导致 worker 被误杀。
# 兜底：/dev/shm 不存在或不可写时退回 /tmp，别让目录探测直接崩掉 master。
_tmp_dir = "/dev/shm"
if not (os.path.isdir(_tmp_dir) and os.access(_tmp_dir, os.W_OK)):
    _tmp_dir = "/tmp"
worker_tmp_dir = _tmp_dir

# 周期性重启 worker，兜住任何未发现的内存增长；jitter 防同时重启造成流量凹陷
max_requests = _env_int("GUNICORN_MAX_REQUESTS", 5000)
max_requests_jitter = _env_int("GUNICORN_MAX_REQUESTS_JITTER", 500)

limit_request_line = 4094
limit_request_fields = 100
limit_request_field_size = 8190

# 反向代理后的真实客户端 IP（只影响访问日志；业务按 token_hash 走，不依赖它）
forwarded_allow_ips = os.environ.get("FORWARDED_ALLOW_IPS", "127.0.0.1")

accesslog = "-" if os.environ.get("GUNICORN_ACCESS_LOG", "0") == "1" else None
errorlog = "-"
loglevel = os.environ.get("ST_LOG_LEVEL", "info").lower()


def on_starting(server):
    server.log.info(
        "stask-service master starting: workers=%s (cpu上限=%s db预算上限=%s "
        "每worker连接=%s) bind=%s timeout=%s graceful=%s",
        workers, _BY_CPU, _BY_BUDGET, _DB_PER_WORKER, bind, timeout, graceful_timeout,
    )
    if graceful_timeout + 5 > timeout:
        server.log.warning(
            "graceful_timeout(%s) 逼近 timeout(%s)：慢请求可能还没走完就被强杀",
            graceful_timeout, timeout,
        )


def worker_exit(server, worker):
    server.log.info("worker exited: pid=%s", worker.pid)
