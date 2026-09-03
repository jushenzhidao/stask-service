# ADR-004: Redis 使用独立实例，键统一前缀 `st:`

## Status

Accepted (2026-08-21) —— 对应设计文档开放问题⑥（文档倾向隔离）

## Background

atask-service 已有一个 Redis 实例（compose 映射 6380，AOF everysec，键前缀 `gw:`）。stask 可以共用它，也可以起独立实例。

stask 的 Redis 承载：幂等占位、并发槽计数、限流窗口、身份/余额缓存、**用户 sk 令牌会话**、taskiq 队列与调度源。其中令牌会话是安全敏感数据，队列是可用性关键路径。

## Decision

**独立实例**（compose 映射 6381），同时**所有键仍加 `st:` 前缀**（`ST_REDIS_KEY_PREFIX`）。

前缀是第二道防线：即便运维把 `ST_REDIS_URL` 误配成 atask 的实例，两边键空间也不会碰撞（`gw:*` vs `st:*`），最坏结果只是共享内存与故障域，不会数据串台。

## Consequences

**正面**：故障域隔离——stask 的队列积压/内存打满不会拖垮 atask 的探测与结算链路；令牌会话不与另一个服务的敏感数据同库；可独立调 `maxmemory` 与持久化策略。

**负面**：多一个容器、多一份运维成本（备份、监控、告警各一套）。单机 compose 起步阶段成本可忽略。

**AOF 要求**：`--appendonly yes --appendfsync everysec`。丢失窗口 ≤1s；丢失的最坏后果是并发槽计数漂移（由定时校准从 tasks 表事实重建）与令牌会话丢失（导致回调无法带签名，任务本身不受影响，因为 stask 无资金动作）。

## Related

SPEC §10；`docker-compose.yml`；`app/redis.py`。
