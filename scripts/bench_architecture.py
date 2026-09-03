"""真实提交路径基准工具：预热、多轮、正确性守卫与双侧 CPU 采样。

此工具只调用运行中的 HTTP 服务，不调用内部函数；因此结果只能用于
当前环境下的同环境 A/B 对比，不能直接外推生产容量。服务器 PID 必须
指向实际处理请求的 web 进程；容器部署请在外部同时记录 ``docker stats``。
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import resource
import statistics
import subprocess
import time
import uuid
from dataclasses import dataclass
from typing import Any

import httpx

PAYLOAD = {"model": "dall-e-3", "prompt": "benchmark", "n": 1}


@dataclass
class RoundResult:
    latencies_ms: list[float]
    statuses: dict[int, int]
    elapsed_s: float
    client_cpu_pct: float
    server_cpu_max_pct: float | None


def _server_cpu(pid: int) -> float | None:
    """读取指定服务进程的瞬时 CPU 百分比；进程消失时返回 None。"""
    try:
        output = subprocess.check_output(
            ["ps", "-p", str(pid), "-o", "%cpu="],
            text=True,
            stderr=subprocess.DEVNULL,
            timeout=2,
        ).strip()
        return float(output) if output else None
    except (OSError, ValueError, subprocess.SubprocessError):
        return None


def _percentile(values: list[float], percentile: float) -> float:
    if not values:
        return 0.0
    return values[min(len(values) - 1, int(len(values) * percentile))]


async def _sample_server(pid: int, samples: list[float], stop: asyncio.Event) -> None:
    while not stop.is_set():
        value = _server_cpu(pid)
        if value is not None:
            samples.append(value)
        try:
            await asyncio.wait_for(stop.wait(), timeout=0.25)
        except TimeoutError:
            pass


async def _request(
    client: httpx.AsyncClient,
    path: str,
    token: str,
    latencies: list[float],
    statuses: dict[int, int],
    correctness_errors: list[str],
) -> None:
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Idempotency-Key": f"bench-{uuid.uuid4().hex}",
    }
    started = time.perf_counter()
    try:
        response = await client.post(path, json=PAYLOAD, headers=headers)
        status = response.status_code
        statuses[status] = statuses.get(status, 0) + 1
        if status == 202:
            try:
                body: Any = response.json()
            except (ValueError, json.JSONDecodeError):
                correctness_errors.append("202 response is not JSON")
            else:
                if not isinstance(body, dict) or not body.get("task_id"):
                    correctness_errors.append("202 response has no task_id")
                if not response.headers.get("location"):
                    correctness_errors.append("202 response has no Location")
        latencies.append((time.perf_counter() - started) * 1000)
    except httpx.HTTPError as exc:
        statuses[0] = statuses.get(0, 0) + 1
        correctness_errors.append(f"HTTP error: {type(exc).__name__}")
        latencies.append((time.perf_counter() - started) * 1000)


async def _run_round(
    client: httpx.AsyncClient,
    path: str,
    token: str,
    concurrency: int,
    requests: int,
    server_pid: int | None,
) -> RoundResult:
    latencies: list[float] = []
    statuses: dict[int, int] = {}
    correctness_errors: list[str] = []
    server_samples: list[float] = []
    stop = asyncio.Event()
    sampler = (
        asyncio.create_task(_sample_server(server_pid, server_samples, stop))
        if server_pid is not None
        else None
    )
    semaphore = asyncio.Semaphore(concurrency)

    async def one() -> None:
        async with semaphore:
            await _request(client, path, token, latencies, statuses, correctness_errors)

    started = time.perf_counter()
    client_cpu_before = resource.getrusage(resource.RUSAGE_SELF)
    await asyncio.gather(*(one() for _ in range(requests)))
    elapsed = time.perf_counter() - started
    client_cpu_after = resource.getrusage(resource.RUSAGE_SELF)
    stop.set()
    if sampler is not None:
        await sampler
    if not server_samples:
        raise RuntimeError(f"无法采集服务端 CPU：PID {server_pid} 不存在或不可读")
    if correctness_errors:
        preview = "; ".join(correctness_errors[:3])
        raise RuntimeError(f"正确性断言失败（{len(correctness_errors)}）：{preview}")
    client_cpu_seconds = (
        client_cpu_after.ru_utime + client_cpu_after.ru_stime
        - client_cpu_before.ru_utime - client_cpu_before.ru_stime
    )
    client_cpu_pct = client_cpu_seconds / elapsed * 100
    return RoundResult(
        latencies,
        statuses,
        elapsed,
        client_cpu_pct,
        max(server_samples) if server_samples else None,
    )


async def main() -> int:
    parser = argparse.ArgumentParser(description="真实提交链路性能回归")
    parser.add_argument("--url", default="http://127.0.0.1:8000")
    parser.add_argument("--path", default="/async/v1/images/generations")
    parser.add_argument("--token", required=True)
    parser.add_argument("--concurrency", type=int, default=50)
    parser.add_argument("--requests", type=int, default=1000)
    parser.add_argument("--warmup", type=int, default=30)
    parser.add_argument("--rounds", type=int, default=5)
    parser.add_argument("--server-pid", type=int, default=None)
    parser.add_argument("--slo-ms", type=float, default=80.0)
    args = parser.parse_args()
    if args.concurrency < 1 or args.requests < 1 or args.rounds < 1:
        parser.error("concurrency、requests、rounds 必须为正数")
    if args.server_pid is None:
        parser.error("必须提供 --server-pid 以采集服务端 CPU；容器请另行采集 docker stats")

    limits = httpx.Limits(
        max_connections=args.concurrency * 2,
        max_keepalive_connections=args.concurrency,
    )
    async with httpx.AsyncClient(base_url=args.url, timeout=30.0, limits=limits) as client:
        ready = await client.get("/healthz/ready")
        if ready.status_code >= 300:
            raise RuntimeError(f"ready 检查失败：HTTP {ready.status_code} {ready.text[:200]}")
        print(f"ready      : HTTP {ready.status_code}")
        for _ in range(args.warmup):
            await _request(client, args.path, args.token, [], {}, [])
        print(f"warmup     : {args.warmup}（不计入结果）")
        results = []
        for index in range(args.rounds):
            result = await _run_round(
                client, args.path, args.token, args.concurrency, args.requests, args.server_pid
            )
            results.append(result)
            total = sum(result.statuses.values())
            success = result.statuses.get(202, 0) / total if total else 0.0
            p99 = _percentile(sorted(result.latencies_ms), 0.99)
            print(
                f"round {index + 1}: p50={_percentile(sorted(result.latencies_ms), .50):.1f}ms "
                f"p99={p99:.1f}ms success={success:.1%} "
                f"client_cpu={result.client_cpu_pct:.1f}% "
                f"server_cpu_max={result.server_cpu_max_pct!s} statuses={result.statuses}"
            )
            if success < 0.95:
                raise RuntimeError("成功路径占比低于 95%，本轮测到的是拒绝/错误路径")
            if p99 >= args.slo_ms:
                raise RuntimeError(f"P99 {p99:.1f}ms 未达到 < {args.slo_ms:.1f}ms")

    p99s = [_percentile(sorted(item.latencies_ms), 0.99) for item in results]
    print(f"median_p99 : {statistics.median(p99s):.1f}ms（{args.rounds} 轮）")
    print(f"code        : {os.environ.get('GIT_COMMIT', '未提供；请记录 git commit')}")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
