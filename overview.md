# stask-service 优化专项交付概览

## 已完成
- 增加 `RuntimeConfig` 不可变动态配置快照，pricing 与限流读取合并为单次缓存加载；Redis 故障时清除旧覆盖并回落启动配置。
- 管理查询收紧为 task_id 精确匹配或 task_id_prefix 前缀匹配，拒绝 leading-wildcard 模糊搜索；增加查询时间窗、分页边界和 LIKE 元字符防护，保留 stask platform 隔离与轻量字段投影。
- 新增真实 HTTP 性能回归工具和验收文档，包含 ready fail-fast、预热、多轮离散度口径、唯一幂等键、202/Location 正确性断言、成功率守卫和双侧 CPU 采样。

## 验证结果
- Ruff：All checks passed
- mypy app/：Success，无问题（38 个源文件）
- pytest：208 passed
- 未运行真实 MySQL/Redis/billing/new-api 全链路压测，因此没有虚构 P99/QPS 或收益百分比。

## 重要契约变化
- 管理任务搜索参数：`task_id` 为精确匹配；前缀搜索使用 `task_id_prefix`；两者不可同时使用。
- `since` 最大 7 天，`limit` 最大 200，`offset` 最大 10000。

## 后续事项
- 当前发现并已记录的动态配置边界：部分已列入 MUTABLE 的配置仍有旧的静态读取路径（例如 body/poll/slot TTL 等），需要后续专门修复并补充真实装配测试；本轮未宣称这些项已完全热生效。
- 正式性能验收需在隔离的真实依赖环境执行脚本，并归档代码版本、每轮原始输出及依赖侧 CPU 数据。
- relay 5xx 回滚、超时退款、X-Task-Id billing attrs 等外部决策仍在 OPEN-DECISIONS 中。
