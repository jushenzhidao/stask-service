# ADR-001: 复用 new-api `tasks` 表，零建表

## Status

Accepted (2026-08-21)

## Background

stask-service 需要持久化任务状态。可选：(a) 自建 `stask_tasks` 表；(b) 复用 new-api 已有的 `tasks` 表；(c) 只用 Redis。

约束：atask-service 已经在复用 `tasks` 表（`platform='gateway'`），它有一个 sweeper 会扫描"终态但未结算"的任务做兜底重发；new-api 自己的任务模块也写这张表。

## Decision

复用 `tasks` 表，`platform = 'stask'` 划分自有行。零建表、零 migration。

三条配套纪律：

1. **所有 UPDATE / 扫描类 SELECT 的 WHERE 必带 `platform = :p`** —— 共享表，绝不动别人的行。
2. **扩展字段全部塞 `data` JSON 列**，用 `JSON_MERGE_PATCH` 合并，不加任何列。
3. **`data` 恒写 `freeze_amount: 0` 与 `settled: true`** —— 让 atask 的 sweeper 天然跳过 stask 的行（它按 `settled != 'true'` 找候选）。这是跨服务共存的关键防撞设计。

时间列统一 unix **秒**：写侧恒写秒，读侧 `as_unix_seconds()` 兜底归一（new-api 原生模块用 UnixMilli 写法，列里会混入毫秒值）。SQL 时间谓词直接裸列比较——查询恒带 `platform='stask'`，命中的只有本服务写的秒值行，不必再包裹归一表达式（包裹会使索引失效，见 ADR-008）。

## Consequences

**正面**：查询/看板天然与 new-api 打通；无 migration 负担；atask 的运维经验直接复用。

**负面**：
- 表 schema 由 new-api 掌控，它 AutoMigrate 改列会波及本服务 —— 缓解：只依赖 `task_id/platform/status/data/user_id/channel_id` 等稳定列。
- `data ->> '$.xxx'` 无索引，扫描类查询是全表扫 —— 缓解：所有扫描都带 `platform` + 状态 + 时间窗三重收敛，且有 LIMIT。
- 三方共写一张表，任何一方漏加 `platform` 过滤都是生产事故 —— 缓解：`taskstore.py` 是唯一数据访问点，纪律集中在一个文件里可审。

## Related

ADR-004（Redis 隔离）；SPEC §6。
