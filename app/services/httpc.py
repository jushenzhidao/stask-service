"""HTTP 客户端工厂：进程级共享 ``AsyncClient``（连接池 keep-alive 复用）。

为什么必须共享：提交链路一次就有 inspect + balance 两个控制面调用，
worker 侧还有 relay 调用。每次新建 AsyncClient 等于每个请求都付完整
TCP + TLS 握手，握手开销会成为提交链路 P99 的主要来源。

调用方**不得** ``aclose()`` 返回值；进程退出由 ``close_all()``
（lifespan / worker shutdown）统一释放。
"""

from __future__ import annotations

import httpx

from app.logging import log

#: key = 构造参数组合（同参数返回同一实例）
_shared: dict[tuple, httpx.AsyncClient] = {}


def shared_client(**kwargs) -> httpx.AsyncClient:
    key = tuple(sorted((name, repr(value)) for name, value in kwargs.items()))
    client = _shared.get(key)
    if client is None or client.is_closed:
        client = httpx.AsyncClient(**kwargs)
        _shared[key] = client
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
