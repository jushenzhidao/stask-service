# ADR-008: 上游中立化（AUTH_MODE）与六处热点路径优化（v0.4）

日期：2026-09-08 ｜ 状态：已采纳

## 背景

ADR-007 把「上游鉴权前置 + user_id 落表」做进了提交路径，但实现直接假定
上游是 new-api：账单端点路径、tokens 表回查、余额语义全是 new-api 特有的。
本服务的定位是**通用 HTTP 异步任务服务**（给任意同步接口加 `/async` 前缀），
把 new-api 的假设烧死在主链路上是定位错误——换上游即不可用。

同时压测暴露了六处热点开销，共性是「等待期和 sweeper 的固定成本随规模
线性上涨」，而非单次请求慢。

## 决策一：上游中立，new-api 特性降级为可选模式

新增 `AUTH_MODE`，取值 `Literal["generic", "newapi"]`，**默认 `generic`**。

| | generic（默认） | newapi |
|---|---|---|
| 提交前鉴权 | 不做 | 直查共享库 tokens ⋈ users |
| 余额预检 | 不做 | 余额 ≤ 0 → 402 |
| 无效 key 表现 | 任务执行时 FAILURE | 提交即 401，不建任务 |
| user_id | 恒 0 | tokens 表回查值 |

- 类型用 `Literal` 而非 `str`：拼错（如 `AUTH_MODE=new-api`）在启动时
  Pydantic 直接报错。这一项若静默回落默认值，等于**静默关闭鉴权**——
  安全开关的失败方向必须是「起不来」，不能是「放行」。
- 启动日志明写当前模式与其准入行为（`main._log_auth_mode`）：排障第一
  个要确认的就是哪个闸门生效，不该靠翻 .env 猜。
- generic 模式下 Authorization 原样透传，key 有效性由上游在执行时判定。
  这是有意的取舍：通用上游没有标准的「查余额」协议，猜一个反而更糟。

**净配置变化**：+1（`AUTH_MODE`）。ADR-007 引入的 new-api 专用项全部保留
但仅在 `newapi` 模式下生效，未启用时不产生任何上游调用。

## 决策二：六处热点路径优化

1. **长轮询卸载到 Redis**（高收益）
   原实现是「每个等待客户端 × 每 1~5s 一条 SELECT status」，DB QPS 随
   等待人数线性上涨。改为 `cas` 落终态时 write-through 写 Redis（带 TTL），
   长轮询先查 Redis，未命中或 Redis 不可用再回落 DB。等待期查询几乎全部
   离开 DB，且 Redis 故障时行为退化为原实现（可用性不降级）。

2. **`flow.view` 首次探测不再拉整行**
   活跃任务走 202 只需 task_id/status/created_at 三个字段，原先 `get()`
   会把 `data` 里最大 2MB 的 base64 request_body 整列拉回。改为先取状态，
   命中终态才拉整行。

3. **删除 `_secs()` 包裹，时间谓词走索引**
   `IF(col > 1e11, col DIV 1000, col)` 包裹列使 range 条件无法用索引，
   sweeper 每 2 分钟对本服务全部行做过滤扫描。写侧恒写 unix 秒 + 查询恒带
   `platform='stask'`（只命中本服务的行），故可裸列比较。毫秒值只存在于
   new-api 自己写的行，那些行我们碰不到。读侧保留 `as_unix_seconds` 兜底。

4. **sweeper 去重查询 + 有界并发**
   `_kill` 内重复的 `get_meta` 改为复用调用方已取到的 dict；循环改
   `asyncio.gather` 并按信号量限并发，避免 200 条任务串行往返。

5. **分位数下推 SQL**
   原先把窗口内（最长 7 天）全部 SUCCESS 行的 duration 拉回 Python 排序，
   内存与往返都随任务量涨。改为 SQL 侧计算，只回传 4 个标量。

6. **消除 httpc 孤儿客户端**
   `_dispatch` 曾用 `Timeout(worker_timeout)` 做缓存 key——dynconf 改一次
   该值就产生一个新 client，旧实例连接一直占着直到进程退出。改为 worker
   侧复用固定 client、timeout 在请求级传入。

## 影响

- 不兼容旧版本（按要求）：默认模式变为 generic，原 new-api 行为需显式
  `AUTH_MODE=newapi` 才恢复。
- 不变式新增：时间谓词禁止包裹列表达式（见 README 约定 3）。
- 长轮询新增一条 Redis 依赖路径，但为软依赖——失败即回落 DB。
