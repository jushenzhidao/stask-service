"""提交链路压测（SPEC §10 目标：P99 < 80ms，不含 billing RTT）。

只压**提交**，不等结果——提交是毫秒级同步链路，是本服务唯一有延迟 SLO
的路径。worker 执行的耗时由上游决定，压它没有意义。

用法：

    .venv/bin/python scripts/bench_submit.py \\
        --url http://127.0.0.1:8000 --token sk-xxx --concurrency 50 --requests 2000

注意事项：
- 每个请求带唯一 Idempotency-Key，否则会全部命中幂等回放，测出来是假数据；
- `ST_MAX_SLOTS` 会限制在途任务数，压测前把它调大（或让 worker 跑起来消费），
  否则大量请求会撞 429——那测的是限流器不是提交链路；
- 建议同时观察 `/ops/stats` 的状态分布确认任务确实在落库。
"""

from __future__ import annotations

import argparse
import asyncio
import statistics
import time
import uuid

import httpx

PAYLOAD = {"model": "dall-e-3", "prompt": "benchmark", "n": 1}


async def _worker(client: httpx.AsyncClient, path: str, token: str,
                  queue: asyncio.Queue, latencies: list[float],
                  codes: dict[int, int]) -> None:
    while True:
        try:
            queue.get_nowait()
        except asyncio.QueueEmpty:
            return
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "Idempotency-Key": uuid.uuid4().hex,
        }
        started = time.perf_counter()
        try:
            resp = await client.post(path, json=PAYLOAD, headers=headers)
            code = resp.status_code
        except httpx.HTTPError:
            code = 0
        latencies.append((time.perf_counter() - started) * 1000)
        codes[code] = codes.get(code, 0) + 1
        queue.task_done()


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="http://127.0.0.1:8000")
    parser.add_argument("--path", default="/async/v1/images/generations")
    parser.add_argument("--token", required=True)
    parser.add_argument("--concurrency", type=int, default=50)
    parser.add_argument("--requests", type=int, default=1000)
    args = parser.parse_args()

    queue: asyncio.Queue = asyncio.Queue()
    for _ in range(args.requests):
        queue.put_nowait(1)

    latencies: list[float] = []
    codes: dict[int, int] = {}

    limits = httpx.Limits(max_connections=args.concurrency * 2,
                          max_keepalive_connections=args.concurrency)
    async with httpx.AsyncClient(base_url=args.url, timeout=30.0,
                                 limits=limits) as client:
        started = time.perf_counter()
        await asyncio.gather(*[
            _worker(client, args.path, args.token, queue, latencies, codes)
            for _ in range(args.concurrency)
        ])
        elapsed = time.perf_counter() - started

    latencies.sort()
    n = len(latencies)

    def pct(p: float) -> float:
        return latencies[min(n - 1, int(n * p))] if n else 0.0

    print(f"requests   : {n}")
    print(f"elapsed    : {elapsed:.2f}s")
    print(f"throughput : {n / elapsed:.1f} req/s")
    print(f"mean       : {statistics.mean(latencies):.1f} ms" if n else "")
    print(f"p50        : {pct(0.50):.1f} ms")
    print(f"p95        : {pct(0.95):.1f} ms")
    print(f"p99        : {pct(0.99):.1f} ms   (SLO: < 80 ms)")
    print(f"max        : {latencies[-1]:.1f} ms" if n else "")
    print(f"status     : {dict(sorted(codes.items()))}")
    if codes.get(429):
        print("\n注意：出现 429——并发槽被打满。压测前调大 ST_MAX_SLOTS，"
              "或让 worker 跑起来消费在途任务，否则测的是限流器不是提交链路。")


if __name__ == "__main__":
    asyncio.run(main())
