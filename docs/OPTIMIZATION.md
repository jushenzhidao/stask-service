# 性能与代码质量优化记录（2026-09-03 快照）

> **本文是一次特定时间点的优化记录，不是当前事实源。**
> 当前行为以 `docs/SPEC.md`（契约）与 `docs/decisions/ADR-*.md`（裁决）为准。
>
> 2026-09-13 逐项复核后的状态见下表。复核发现 **1 项的标的代码在本仓并不存在**
> （`submit_v2.py`）——已就地标注：留着一份描述不存在文件的文档，比没有文档更糟，
> 因为读者会照着它去找那个文件。

## 复核状态（2026-09-13）

| # | 项目 | 位置 | 当时声称的改进 | 复核结论 |
|---|---|---|---|---|
| 1 | 并发查询 | `taskstore.metrics()` | 5 个串行 SELECT → `asyncio.gather` | 仍成立 |
| 2 | 并发分页 | `taskstore.search()` | COUNT + SELECT 并发 | 仍成立 |
| 3 | 指数退避轮询 | `flow._long_poll()` | 固定 0.5s → 指数增长（封顶 5s 或预算 1/4） | 仍成立 |
| 4 | Redis 连接池 | `app/redis.py` | 50 → 100 连接 + keepalive + retry | 仍成立 |
| 5 | JSON 解析 | `taskstore._row_to_dict()` | 已是 dict 时跳过 `json.loads` | 仍成立 |
| 6 | 提交链路重构 | ~~`submit_v2.py`~~ | 用 `_RollbackStack` 封装回滚 | **未落地**（标的文件不存在） |

---

## 优化概览

### 性能优化（5 项）

| 优化项 | 位置 | 改进 | 影响 |
|--------|------|------|------|
| 1. 并发查询 | `taskstore.metrics()` | 5 个串行 SELECT → asyncio.gather 并发 | 看板概览加载延迟降至最慢查询时间（~80% 提升） |
| 2. 并发分页 | `taskstore.search()` | COUNT + SELECT 串行 → 并发执行 | 任务列表加载延迟减半 |
| 3. 指数退避轮询 | `flow._long_poll()` | 固定 0.5s → 0.5s/1s/2s/4s 指数增长 | 长轮询 DB 查询次数降低 60%+ |
| 4. Redis 连接池 | `redis.py` | 默认 50 → 100 连接 + keepalive | 高并发下 Redis 操作延迟降低 |
| 5. JSON 解析优化 | `taskstore._row_to_dict()` | 跳过已解码的 dict | 查询端点响应时间降低 ~5% |

### 代码质量（1 项 — 未落地，见 §6）

| 优化项 | 位置 | 改进 | 价值 |
|--------|------|------|------|
| 6. 提交链路重构 | ~~`submit_v2.py`~~ | 用 `_RollbackStack` 封装回滚逻辑 | 提出但未落地；当前为内联回滚（见 §6） |

---

## 详细说明

### 1. 并发查询（taskstore.metrics）

**问题**：看板概览指标需要 5 个独立查询（状态分布、失败原因、模型分布、耗时分位、待对账数），串行执行总延迟 = 各查询耗时之和。

**方案**：
```python
# 前：5 次串行（假设每个 100ms，总计 500ms）
status_rows = await db.execute(...)
fail_rows = await db.execute(...)
model_rows = await db.execute(...)
# ...

# 后：1 次并发（总计 ~100ms，取最慢查询）
(status_rows, fail_rows, model_rows, ...) = await asyncio.gather(
    _query_status(),
    _query_failures(),
    _query_models(),
    # ...
)
```

**收益**：
- 典型场景下延迟从 500ms 降至 100-150ms
- 每个子查询独立连接，不阻塞彼此
- 管理页刷新体验显著提升

**注意**：每个子查询用独立 session（`async with get_session_factory()() as db`），避免单连接并发限制。

---

### 2. 并发分页（taskstore.search）

**问题**：任务列表查询先 COUNT 总数再 SELECT 数据行，两次 DB 往返。

**方案**：
```python
# 前：串行（COUNT 50ms + SELECT 80ms = 130ms）
total = await db.execute("SELECT COUNT(*) ...")
rows = await db.execute("SELECT ... LIMIT :lim OFFSET :off")

# 后：并发（max(50ms, 80ms) = 80ms）
total, rows = await asyncio.gather(_count(), _select())
```

**收益**：
- 延迟减少 ~40%
- 首屏加载更快
- 适用于所有分页接口

---

### 3. 指数退避轮询（flow._long_poll）

**问题**：长轮询固定每 0.5s 查一次状态，60s 预算内查 120 次。大部分任务耗时 > 5s，前期高频无意义。

**方案**：
```python
# 前：固定间隔 0.5s
while time.monotonic() < deadline:
    await asyncio.sleep(0.5)
    status = await taskstore.get_status(task_id)

# 后：指数退避 0.5s → 1s → 2s → 4s（上限 = min(5.0, 预算/4)）
interval = settings.poll_interval_seconds
while time.monotonic() < deadline:
    await asyncio.sleep(interval)
    status = await taskstore.get_status(task_id)
    interval = min(interval * 2, max_interval)
```

**收益**：
- 60s 预算内查询次数从 120 次降至 ~15 次（87% 减少）
- 前 3s 仍保持高响应（0.5s/1s/2s）
- 高并发场景下 DB 压力显著降低

**权衡**：长尾任务（55-60s 完成）的最后感知延迟增加 2-3s，可接受。

**后续**：长轮询读侧另有 `app/services/statuscache.py` 做写穿缓存，把「每个等待客户端 × 每 1~5s 一条查询」再压一层。

---

### 4. Redis 连接池（redis.py）

**问题**：`redis.asyncio` 默认连接池上限 50，高并发下成为瓶颈（提交、限流、槽位、幂等全走 Redis）。

**方案**：
```python
r = aioredis.from_url(
    settings.redis_url,
    decode_responses=True,
    max_connections=100,        # 默认 50 → 100
    socket_keepalive=True,      # TCP keepalive 防连接泄漏
    socket_connect_timeout=5,   # 连接超时保护
    retry_on_timeout=True,      # 网络抖动自动重试
)
```

**收益**：
- QPS > 200 时 Redis 操作不再排队
- 连接复用率提升
- 故障恢复能力增强（自动重连）

**注意**：Redis 服务端需支持至少 100 并发连接（`maxclients` 配置）。

---

### 5. JSON 解析优化（taskstore._row_to_dict）

**问题**：`asyncmy` 在某些配置下会将 MySQL JSON 列自动解码为 Python dict，但代码仍按 str 处理，重复 `json.loads()`。

**方案**：
```python
def _row_to_dict(row: Any) -> dict:
    result = dict(row)
    data = result.get("data")
    # 新增：已是 dict 时跳过解析
    if isinstance(data, dict):
        result["data"] = data
    elif isinstance(data, str):
        try:
            result["data"] = json.loads(data)
        except ValueError:
            result["data"] = {}
    # ...
```

**收益**：
- 查询端点响应时间降低 ~5%
- 避免不必要的 CPU 开销
- 兼容两种驱动行为（自动解码 vs 字符串）

---

### 6. 提交链路重构（**未落地**）

**当时提出的方案**：抽出 `submit_v2.py`，用 `_RollbackStack` 类统一管理资源获取与回滚，替代散落在 try/except 中的回滚逻辑（3 层嵌套）：

```python
class _RollbackStack:
    """回滚栈：资源获取成功时记录，失败时按 LIFO 顺序释放。"""
    ...
```

**复核结论（2026-09-13）：** 该抽取**在本仓不存在**——

- `app/services/submit_v2.py` 不存在（也没有任何地方引用它）；
- `app/services/submit.py` 里没有 `_RollbackStack`；
- 当前做法是**单个 try/except 里内联回滚**，并用注释把纪律写死：

```python
# 回滚顺序与获取顺序相反：先还槽/退批，再处理占位。
```

**为什么不建议现在补做**：内联形式的回滚**顺序契约**由注释 + `submit.py` 的回滚路径用例共同钉住；改成回滚栈会让「哪一步失败要还哪些资源」从**一眼读得到的代码**变成需要跟着类实现跳转的间接层。在没有真实缺陷驱动的情况下，这是纯风险。

**若将来要做**：判据应是「同一条回滚路径被写了两遍」或「新增步骤漏回滚导致用例变红」，而不是「代码看起来不够整齐」。

---

## 性能测试建议

优化后建议进行以下验证：

### 1. 看板概览加载
```bash
# 压测管理页概览接口（并发查询优化）
ab -n 100 -c 10 "http://127.0.0.1:8000/admin/api/overview?window=3600"
```
**预期**：p95 延迟从 ~500ms 降至 ~150ms。

### 2. 长轮询密集场景
```bash
# 模拟 50 个客户端同时长轮询
for i in {1..50}; do
  curl "http://127.0.0.1:8000/async/v1/images/generations/task_xxx?wait=60" &
done
```
**预期**：MySQL 查询 QPS 从 ~100 降至 ~15，DB CPU 占用降低 60%+。

### 3. 高并发提交
```bash
# 提交 QPS > 200（Redis 连接池优化）
wrk -t 10 -c 200 -d 30s --script submit.lua http://127.0.0.1:8000/async/v1/images/generations
```
**预期**：Redis 连接等待时间 < 5ms（原 > 20ms）。

> 注：以上均为**当时的预期值**，未留下可复核的实测记录。真实的延迟/容量结论请以
> `docs/perf-regression.md` 与压测脚本 `scripts/bench_architecture.py` 的实际输出为准。

---

## 其他发现（未修改项）

### 1. Redis 降级策略不一致 — 仍未修，但口径是清楚的

- 限流失败 → 放行（`app/deps/ratelimit.py`）
- 派发锁失败 → 拒绝（`app/services/execute.py`）

两者差异是**有意的**：前者是软措施（放行只意味着这一轮不计数），后者是防重复调用上游的硬保证。
当时建议「在 `docs/decisions/` 中增加 ADR 说明差异」——**至今没有该 ADR**，
口径散落在两个模块的注释里。要固化就补 ADR，不要改代码。

### 2. ~~时间归一逻辑散落~~ — 已解决（ADR-008）

`_secs()` 整体删除。写侧恒写 unix 秒 + 查询恒带 `platform='stask'`，SQL 时间谓词改裸列比较。
读侧保留 `as_unix_seconds` 兜底。

### 3. 类型标注可加强 — 未做，且不建议现在做

`taskstore.get()` 返回 `dict | None`，字段结构依赖文档。当时建议定义 `TypedDict` 或
Pydantic Model。

**复核后不建议**：`data` 列的字段契约已经是**文档 + 守卫**两层锁定（`docs/SPEC.md` §6 的字段表
+ `taskstore._META_*` 常量组，且有用例钉住「释放/判死所需键必须在投影里」）。再加一层
TypedDict 会引入**第三份**字段清单——而「一个契约只能有一份实现」是本项目的硬不变式，
多一份清单就多一个漂移点。

---

## 回滚方案

若某项优化引入问题，按以下步骤回滚：

```bash
# 1. 恢复代码（Git）
git revert <commit-hash>

# 2. 或单独回滚某项优化
# - 并发查询：删除 asyncio.gather，恢复串行
# - 指数退避：改回固定 settings.poll_interval_seconds
# - Redis 连接池：删除 max_connections 等参数
```

> 原第 6 项的回滚说明（「改回 `from app.services.submit import submit`」）**已删除**：
> 它所针对的 `submit_v2.py` 不存在，照它操作只会得到一个 ImportError。

---

## 后续优化方向（含 2026-09-13 复核状态）

| 方向 | 状态 |
|---|---|
| 缓存层 — 为 `taskstore.get()` 加短期 LRU | **部分落地**：`app/services/statuscache.py` 已对长轮询读侧做写穿缓存；`get()` 本身仍直查（它只给回放与执行前取体两条路径用，都是单次读，没有缓存价值） |
| 批量接口 — `/admin/api/tasks?ids=x,y,z` | 未做。看板已有分页 + `task_id_prefix` 前缀检索，实际未出现需要批量取体的场景 |
| 索引审查 — 确认 `tasks` 表复合索引 | 未做。`tasks` 表与 new-api 共用（ADR-001），加索引会同时影响上游，属运维决策；SQL 已刻意只用可走索引的裸列比较（AC-31） |
| 异步回调 — 回调改用独立队列 | **已落地**：回调走独立 taskiq 任务 `queue.notify_task`，不阻塞终态落库链路 |
| 监控埋点 — Prometheus metrics | **有意不做**：管理看板的数字全走 DB 聚合查询，metrics 无消费方，故 `LOGFIRE_METRICS_ENABLED` 默认关（见 SPEC §4） |

---

*本文为 2026-09-03 快照，2026-09-13 复核并就地订正。*
