"""HTTP 客户端工厂：进程级共享 ``AsyncClient``（连接池 keep-alive 复用）。

为什么必须共享：提交链路一次就有 inspect + balance 两个控制面调用，
worker 侧还有上游调用。每次新建 AsyncClient 等于每个请求都付完整
TCP + TLS 握手，握手开销会成为提交链路 P99 的主要来源。

调用方**不得** ``aclose()`` 返回值；进程退出由 ``close_all()``
（lifespan / worker shutdown）统一释放。
"""

from __future__ import annotations

import httpx

from app.logging import log

#: key = 用途名（每个用途恒一个实例，构造参数只在首次生效）
_shared: dict[str, httpx.AsyncClient] = {}


def shared_client(name: str, **kwargs) -> httpx.AsyncClient:
    """按用途名取共享客户端；不存在或已关闭时按 ``kwargs`` 新建。

    key **只用 name**：可变的 timeout 之类参数绝不能进 key，否则运行时改
    一次配置就多出一个实例，旧实例连接一直占着直到进程退出。会随配置变的
    超时请走请求级 ``client.request(..., timeout=...)``。
    """
    client = _shared.get(name)
    if client is None or client.is_closed:
        client = httpx.AsyncClient(**kwargs)
        _shared[name] = client
    return client


async def close_all() -> None:
    """释放全部共享客户端（幂等）。

    先清表再逐个关闭：关闭期间新进的调用拿到的是新建实例；单个 aclose
    失败只告警，不阻塞其余释放（lifespan 里后面还有 close_db）。
    """
    clients = list(_shared.values())
    _shared.clear()
    for client in clients:
        try:
            await client.aclose()
        except Exception:
            log.opt(exception=True).warning("shared http client close failed")
