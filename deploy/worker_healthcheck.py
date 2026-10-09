"""worker 容器 healthcheck：区分「真的没活干」与「消费者僵死还在堆积」。

背景（2026-10-09 事故）：旧探活只测「能连上 Redis 的 TCP」，taskiq 消费者
停摆 8 小时而容器恒 healthy，任务被另一台机器上的残留部署整批判死。

判定顺序（第一个命中即返回）：

1. **心跳新鲜**（< 300s）→ healthy。``app.queue._touch_heartbeat`` 在每个
   任务执行后刷新，而 cron 任务（tick 每 30s 一条）让低流量期心跳也持续
   新鲜——心跳断只可能是 receiver 真的停了；
2. **旧镜像无心跳文件** → 回退**进程检查**：/proc 里能找到 taskiq worker /
   scheduler 进程即 healthy（升级窗口内不误杀）；
3. **心跳陈旧但队列没有积压**（lag < 100）→ healthy，保守放行：避免
   重启风暴（worker 重启无害但无意义，还会短暂中断 scheduler）；
4. **心跳陈旧且积压** → unhealthy：有活干却不动 = 消费者僵死，交给
   autoheal 重启。

退出码：0 = healthy，1 = unhealthy（docker convention）。
"""

from __future__ import annotations

import os
import sys
import time

HEARTBEAT_PATH = "/dev/shm/stask_worker_heartbeat"
HEARTBEAT_MAX_AGE = 300  # 秒；tick 间隔 30s，容忍偶发暂停
LAG_THRESHOLD = 100  # 条；超过视为「有活干却不动」


def _heartbeat_age() -> float | None:
    """心跳年龄（秒）；文件缺失或内容不可解析返回 None。"""
    try:
        with open(HEARTBEAT_PATH, encoding="ascii") as fh:
            return max(0.0, time.time() - float(fh.read().strip() or 0))
    except (OSError, ValueError):
        return None


def _has_taskiq_process() -> bool:
    """/proc 里能找到 taskiq worker / scheduler 进程（slim 镜像无 ps/pgrep）。"""
    for pid in os.listdir("/proc"):
        if not pid.isdigit():
            continue
        try:
            with open(f"/proc/{pid}/cmdline", "rb") as fh:
                cmd = fh.read().decode("utf-8", errors="replace")
        except OSError:
            continue
        if "taskiq" in cmd and ("worker" in cmd or "scheduler" in cmd):
            return True
    return False


def _stream_lag() -> int | None:
    """主队列（``{prefix}:taskiq``）的未投递消息数；查询失败返回 None。"""
    try:
        import redis
    except ImportError:  # 理论不可达：taskiq broker 依赖 redis
        return None
    try:
        prefix = os.environ.get("REDIS_KEY_PREFIX", "st")
        client = redis.Redis.from_url(  # type: ignore[attr-defined]
            os.environ.get("REDIS_URL", "redis://redis:6379/0"),
            socket_timeout=3,
        )
        groups = client.xinfo_groups(f"{prefix}:taskiq")
    except Exception:
        return None
    for group in groups:
        name = group.get("name")
        if isinstance(name, bytes):
            name = name.decode("utf-8", errors="replace")
        if name == f"{prefix}:workers":
            lag = group.get("lag")
            return int(lag) if isinstance(lag, (int, float)) else None
    return None


def main() -> int:
    age = _heartbeat_age()
    if age is not None and age < HEARTBEAT_MAX_AGE:
        return 0
    if age is None:
        # 旧镜像未升级（应用侧尚未写心跳）：回退进程检查，绝不因升级顺序误杀
        return 0 if _has_taskiq_process() else 1
    # 心跳断了：有积压 = 消费僵死；没积压 = 真没活干，保守放行
    lag = _stream_lag()
    if lag is None:
        return 0  # Redis 查询失败不误杀；TCP 级故障另有进程退出兜底
    return 1 if lag > LAG_THRESHOLD else 0


if __name__ == "__main__":
    sys.exit(main())
