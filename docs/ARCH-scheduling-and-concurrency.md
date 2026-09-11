# 架构设计 — 调度下发与细粒度并发控制

> 版本：v0.1（架构评审稿）
> 输入：`docs/PRD-scheduling-and-concurrency.md` v0.1
> 基线：当前 main 代码事实（已逐处核对，出处见文内行号）
> 本文只做架构裁决与机制设计，不含生产代码。

---

## 0. 结论摘要

三项需求（延迟/定时下发、批量聚合、model×token 并发）**架构上可行**，落地为两条
新机制，不新增 MySQL 表、不新增状态枚举、不改动既有 Lua 脚本：

| 机制 | 实质 | 复用/新增 |
|---|---|---|
| **到期通道**（timer wheel） | 一个 Redis ZSET 做定时索引 + 一个 ticker 驱动放行；`tasks.data` 是事实源，sweeper 从事实重建 | 新增，**不复用** taskiq `schedule_by_time`（理由见 §3.1） |
| **两层并发闸门** | token 总量（外层，语义重定义）+ (model,token) 细粒度（内层），**单条 Lua 一次原子占用** | 新增 2 个 Lua 脚本，原脚本不动 |

但**有三个阻塞项必须先解决，否则功能上线即回归**。前两项 PRD 未识别：

| # | 阻塞项 | 后果 | 裁决 |
|---|---|---|---|
| **B1** | 用户令牌会话 TTL 固定 2h（`sk_session_ttl_seconds=7200`，`config.py:213`），而 PRD 的延迟上限默认 6h、硬上限 12h | 任何延迟 > 2h 的任务，到点时令牌已过期 → `execute.py:240` 走 `token_missing` → **100% FAILURE**。这是功能性彻底失效，不是性能问题 | 令牌会话 TTL 改为**按任务计算**，并把延迟上限**从 TTL 上限反推**（§2.1）——不可反过来独立选一个延迟上限 |
| **B2** | `sweep_stale` 的重投路径直接 `publish_execute`（`sweeper.py:140-142`），**绕过占槽** | 放行必须占槽（AC-43），但兜底重投不占——同一个任务两条入队路径两套账，槽计数必然漂移；且等待期任务被提前执行 | 放行动作收敛为**唯一策略点** `release()`，兜底扫描必须调它而非 `publish_execute`（§3.4） |
| **B3** | PRD Q3 提出把 `sweep_overdue` 计时起点改为 `max(created_at, scheduled_at)` | `overdue_active()` 现在靠 `created_at < :cutoff` 命中索引（`taskstore.py:561`），改成 JSON 表达式取 max 会退化为全表扫 | 无需改起点：用**索引友好的双谓词**改写，粗筛仍走 `created_at` 索引，精筛做残差过滤（§5.1）——原谓词恰好是新语义的超集，可证 |

---

## 1. 设计原则（沿用既有架构，不另立一套）

本次设计不引入新的架构范式，严格沿用四条既有原则：

1. **事实源恒在 `tasks` 表，Redis 只放"丢了能重建"的索引**（`app/redis.py` 模块约定）。
   到期时刻、批次归属一律落 `tasks.data`；ZSET 与批次列表都是**可重建的加速索引**，
   丢失由 sweeper 从 DB 事实重建。这与并发槽「Redis 计数 + 定时校准」完全同构。
2. **新增字段一律落 `tasks.data` JSON 列**（SPEC §3 铁律，ADR-001）。零建表。
3. **不新增状态枚举**（ADR-006 共存约束）。等待期任务保持 `QUEUED`，
   客户端靠 `scheduled_at` 字段区分——PRD §4.5 的决策正确，架构侧确认。
4. **不改既有 Lua 脚本与参数顺序**（占槽正确性依赖其原子性）。新语义用新脚本。

---

## 2. 阻塞项裁决

### 2.1 B1 — 令牌会话 TTL 是延迟上限的真实天花板（PRD Q3 需重做）

事实链（三处代码，缺一不成立）：

- `tokensession.store()` 无条件用 `settings.sk_session_ttl_seconds`（=7200s）写 TTL；
- 令牌**只在 Redis**，红线禁止落库（`app/redis.py` docstring、不变式 3），
  所以到点时 TTL 已过 = 令牌永久不可得，**没有任何补救手段**；
- `execute.run` 取不到令牌时明确「拒绝替代」，直接判 FAILURE（`execute.py:238-245`）。

PRD Q3 的推导只考虑了「最大延迟 D + 生命期 L < 上游 24h 清理线」，
漏掉了 D + L 还必须 ≤ 令牌 TTL。补上后的完整约束是：

```
D + L + margin  ≤  SK_SESSION_TTL_MAX     （令牌可得性，B1 新增，最紧的一条）
D + L           <   24h                    （上游清理线，PRD 已识别）
```

**裁决：TTL 按任务计算，延迟上限从 TTL 上限反推。**

- 提交时 `tokensession.store(task_id, token, ttl=<computed>)`，
  `computed = delay + task_max_lifetime_seconds + margin`，**上限钳到
  `sk_session_ttl_max_seconds`**；`delay=0`（即时任务）时结果恒为现值 2h，
  **即时任务的令牌驻留时长逐秒不变**（这是 AC-63 向后兼容的一部分）；
- 有效延迟上限 = `min(max_delay_seconds, sk_session_ttl_max_seconds - L - margin)`，
  提交时按这个**推导值**校验并返 `delay_too_long`，而不是按裸配置值；
- `sk_session_ttl_max_seconds` 是**安全项，永久只读 env**（进 `IMMUTABLE_REASONS`）；
  `max_delay_seconds` 是运营项，可热改，但**改不动上面那个钳位**。

**这是一个必须显式承认的安全取舍**：延迟功能把用户令牌在 Redis 的驻留窗口从
2h 拉长到最坏 12h。取舍成立的前提是「只有用了延迟的任务才延长」，
且上限由只读 env 封顶——运营热改配置无法扩大暴露面。若安全侧不接受 12h 驻留，
则延迟上限必须相应下调，**两者是同一个旋钮的两端，不能分开定**。

推荐取值（待安全 + 运营确认）：

| 配置 | 值 | 层 |
|---|---|---|
| `sk_session_ttl_seconds` | 7200（现值不动，即时任务用） | 启动项 |
| `sk_session_ttl_max_seconds` | 43200（12h，延迟任务钳位上限） | **安全项，只读** |
| `task_max_lifetime_seconds` (L) | 21600（6h，现值不动） | 运营项 |
| `max_delay_seconds` (D) | 18000（5h）→ 有效上限 = min(5h, 12h−6h−margin) ≈ 5h | 运营项 |

即：默认给到 5h 延迟，D+L=11h < 12h TTL，且 11h << 24h 上游清理线，两条约束同时满足。
若运营要 6h 延迟，则 `sk_session_ttl_max_seconds` 需提到 13h 以上，**需安全侧重新签字**。

### 2.2 B2 — 放行必须是唯一策略点（PRD R-19/AC-43 的实现前提）

当前有**两条**入队路径：提交链路 `submit.submit()` 的 `enqueue` 回调，和兜底
`sweeper.sweep_stale` 的 `publish_execute`（`sweeper.py:140-142`）。前者占槽，后者不占——
今天无害（占槽发生在提交时，重投的任务槽还在），但引入「等待期不占槽 + 放行时占槽」后，
后者会变成一条**绕过占槽的后门**：等待期任务被 `sweep_stale` 捞到就直接执行，
既破坏延迟语义（AC-44），又让槽账目失衡（AC-42/AC-53）。

**裁决：新增 `app/services/dispatch.py`，`release(task_id)` 作为放行的唯一入口。**

```
release(task_id):
  1. 读 meta（status / data.scheduled_at / data.model / data.token_hash）
  2. 非 QUEUED  → 丢弃（已取消或已在跑，幂等静默返回）
  3. 未到 scheduled_at → 丢弃（不该被放行，交回到期通道）
  4. 两层占槽（§4.2 单条 Lua）
       占不到 → 退避重排（§3.5），不判死、不报错、不跳过占槽
  5. patch data.inflight_slot=true / slot_model=<归组模型>
  6. publish_execute(task_id)
```

三条调用方全部改为走它：到期通道 ticker、批次放行、`sweep_stale` 重投。
`submit` 链路的**即时任务**保持现状（提交时同步占槽 + 直接入队），
理由是它要在占不到槽时同步返 429——这是 AC-08 语义，不能改成异步重排。

第 5 步的 `inflight_slot` 标记是 PRD §4.6 修订 AC-18/AC-24 的落地依据：
**释放槽的唯一判据是这个标记，不是状态**。它必须与占槽在同一步之后立刻写，
且释放侧（`execute` 终态、`flow.cancel`、`sweeper._kill`）三处统一读它。

> 已知失败窗口：占槽成功后、`patch_data` 落库前进程崩溃 → 槽被占但无标记 →
> 该槽泄漏，直到 `recalibrate_slots` 按 DB 事实回写（≤5min）或键 TTL 过期。
> 与现有「占槽后落库前崩溃」窗口同性质、同收敛手段，不新增风险类别。

### 2.3 B3 — 超龄扫描不改计时起点，改谓词（PRD Q3 的性能风险可消除）

PRD 担心的是对的：`overdue_active()` 现在是 `created_at < :cutoff`（`taskstore.py:561`），
`created_at` 有索引；若改成 `max(created_at, JSON_EXTRACT(data,'$.scheduled_at')) < :cutoff`，
JSON 表达式不可索引，MySQL 必然全表扫 `tasks`（与 new-api 共表，行数量级是全平台任务）。

**裁决：不改计时起点表达式，改成两个谓词。**

```sql
WHERE platform = :p AND status IN :acts
  AND created_at < :cutoff                                   -- 粗筛，走索引，恒不变
  AND COALESCE(data ->> '$.scheduled_at', 0) + 0 < :cutoff    -- 精筛，残差过滤
LIMIT :lim
```

**正确性可证**：目标语义是 `max(created_at, scheduled_at) < cutoff`，而
`max(a,b) < c  ⟺  a < c AND b < c`。两个谓词的合取**恰好等价**于目标语义，
不是近似。第一个谓词单独就是现状语义，MySQL 优化器会优先用它的索引缩小候选集，
第二个只在候选行上做残差计算——候选集是「超龄非终态」，正常态下应为 0 条，
最坏受 `LIMIT` 保护。零索引风险，零语义损失。

同理适用于 `stale_active()`：加 `AND COALESCE(data ->> '$.scheduled_at', 0) + 0 <= :now`
即可豁免等待期任务（AC-44），不影响其 `updated_at` 索引利用。

**注意 `+ 0`**：`data ->> '$.x'` 返回的是 LONGTEXT，与整数比较会走字符串比较
（`'900' < '1000'` 为假）。必须显式转数值。这是本项目 JSON 列比较的通用陷阱，
应写进不变式。

---

## 3. 到期通道设计

### 3.1 为什么不复用 taskiq `schedule_by_time`

`publish_notify` 已在用它（`queue.py:254-259`），复用看起来最省事。但读完
`ListRedisScheduleSource` 与 scheduler 主循环的实现后，**结论是不能用**：

| 事实（已核对源码） | 出处 | 后果 |
|---|---|---|
| 时间键按**分钟**分桶：`{prefix}:time:%Y-%m-%dT%H:%M` | `list_schedule_source.py:77-82` | 精度上限就是分钟，够用（AC-41 要 30s），这条不是问题 |
| `get_schedules()` **每个 tick（默认 1s）** 都 `lrange` 当前分钟桶，再 `mget` 全部 data 键 | 同上 188-235；`run.py:305` | 一个分钟桶里攒 500 条计划任务 = 每秒 500 次 mget 往返。用于回调重试（稀疏）没问题，用于批量跑批（500 条挤同一分钟）是**每秒放大 500 倍的读放大** |
| 首次运行 `scan_iter("{prefix}:time:*")` 扫全部历史时间键 | 同上 131 | scheduler 重启时对 Redis 做一次 SCAN 全表；键数 = 计划任务涉及的分钟数 |
| 调度数据用 **Pickle** 序列化（默认 serializer） | 同上 47-49 | 存的是 taskiq 内部对象。我们要在管理看板上查询「还有哪些任务在等、还剩多久」，得反序列化 pickle 才能读——不可查询、不可运维 |
| `post_send` 后 `delete_schedule` 才清理 | 同上 183-186 | 取消一个计划任务需要拿到 `schedule_id` 反查，我们只有 `task_id`，得再存一层映射 |

核心矛盾：taskiq 的调度源是为**稀疏、少量、内部**的定时任务设计的（cron sweeper、
回调重试），而本需求是**密集、批量、需要被运维查询和取消**的业务对象。
硬套会同时踩上读放大和不可观测两个坑。

**裁决：自建到期通道，ZSET + ticker。** `publish_notify` 继续用 taskiq
（它确实稀疏），两者并存不冲突。

### 3.2 数据结构

```
事实源（MySQL，tasks.data）——丢了要能重建全部索引
  scheduled_at   : int   到期 unix 秒；0 = 即时任务（老任务读出来就是 0，兼容）
  batch_key      : str   批次归组键；"" = 不参与批次
  batch_state    : str   "waiting" / "released" / ""
  inflight_slot  : bool  是否实际占用过槽（释放槽的唯一判据，§2.2）
  slot_model     : str   占槽时用的归组模型（释放要用同一个值，见下）

索引（Redis，可重建）
  st:due            ZSET  member=task_id, score=scheduled_at    到期索引
  st:batch:{key}    ZSET  member=task_id, score=enqueued_at     批次成员
  st:batch:meta:{key} HASH size/wait/deadline/model/token_hash  批次参数
  st:tick           STRING ticker 重入锁
```

`slot_model` 必须落库，不能执行时重算：`data.model` 理论上不变，但**归组模型
是经过归一化的**（空模型 → `__unknown__`，见 §4.1）。占用时算一次、落库、
释放时读同一个值——否则一旦归一化规则变更（比如将来加别名合并），
在途任务会「占 A 释放 B」，造成永久漂移。这类 bug 靠校准也修不干净
（校准口径同样依赖归一化规则）。

### 3.3 ticker

新增一只 cron 任务，`FREQ = */1 min` 是不够的（AC-41 要 30s 精度），
所以设计为**自驱动循环任务**而非 cron：

```
tick():                                   # 每 15s 一轮，重入锁 TTL 30s
  1. lock st:tick nx ex=30 → 抢不到直接返回（多副本安全）
  2. ZRANGEBYSCORE st:due -inf now LIMIT batch   → 到期 task_id 列表
  3. 逐条（有界并发 8，与 _SWEEP_CONCURRENCY 一致）：
       dispatch.release(task_id)
       ZREM st:due task_id            ← 成功放行 / 已取消 / 已终态 都要移除
  4. 批次超时检查：扫 st:batch:meta:* 的 deadline，到期整批放行
```

15s 轮询 + 一轮内处理完 = 最坏延迟 15s，满足 AC-41 的 30s。
ZSET 单键 `ZRANGEBYSCORE ... LIMIT` 是 O(log N + M)，
一轮只取 `sweep_batch_limit`（200）条，与积压量无关——**这是不用 taskiq 的直接收益**：
500 条挤在同一秒，也只是一次 ZSET range，不是 500 次 mget。

**cron 语法只能到分钟**，所以 ticker 用 `*/1 * * * *` 起，内部循环 4 次
×15s（跑满一分钟即退出，下一分钟由新的 cron 实例接手）。这样不引入常驻线程，
沿用现有 scheduler 装配，也不会因为一次崩溃就永久停摆。

> 为什么不做成「一次投递、精确到点执行」：那等于把 500 条任务的定时职责
> 交给 Redis 的过期通知或 broker 的可见性超时，两者都不保证时序精度，
> 且都会绕过 §2.2 的唯一放行点。轮询是这里的正确解——它慢，但账目干净。

### 3.4 与 sweeper 的关系（B2 的落地）

| sweeper | 改动 |
|---|---|
| `sweep_stale` | ① SQL 加 `scheduled_at <= now` 豁免（§2.3）；② 重投路径 `publish_execute` → `dispatch.release`（B2）；③ 重投前顺带 `ZADD st:due` 回补索引——**这是 ZSET 丢失后的重建路径**，Redis 掉数据时任务最迟 2min 后被捞回，功能不失效只是延迟 |
| `sweep_overdue` | SQL 换双谓词（§2.3）。等待期任务因此天然豁免 |
| `recalibrate_slots` | 事实源扩为**两个维度**且都排除等待期（§4.3） |
| `purge_results` | 无影响 |

`sweep_stale` 的 ③ 是整个设计的**兜底闭环**：Redis 里的 ZSET 是索引，
DB 的 `scheduled_at` 是事实，两者不一致时以 DB 为准并回补。这与
「Redis 槽计数 + 定时校准」是同一个模式，不是新发明。

### 3.5 放行时占不到槽：退避重排（R-19/AC-43）

客户端早已拿到 202 离开，此时不能报错，也不能跳过占槽直接执行（会击穿水位）。

```
占槽失败 → attempt += 1（落 data.release_attempts）
         → ZADD st:due  score = now + backoff(attempt)
         → backoff: 30s, 60s, 120s, 240s, 300s(封顶)，带 ±10% 抖动
         → 直到 max(created_at, scheduled_at) 超 L → 交给 sweep_overdue 判死
```

抖动是必需的：整批 200 条同时占槽失败，无抖动会导致它们永远同相重试，
形成稳定的惊群。判死不由 ticker 做——**判死权只在 `sweep_overdue` 一处**，
ticker 只负责重排，避免两处都能判死时的竞态。

---

## 4. 两层并发闸门

### 4.1 归组模型的归一化（AC-51）

```
normalize(model) = "__unknown__"            if model 为空/空白
                 = model.strip().lower()    否则
```

不做别名合并、不做前缀截断——`data.model` 是浅解析原值（`proxy.py:_extract_model`），
上限已由 `model_slug` 的 16 字符截断证明够用。归一化结果落 `data.slot_model`（§3.2）。

`__unknown__` 用双下划线包裹，与任何真实模型名不可能碰撞（模型名不含空格但可能含
下划线，`__unknown__` 这个具体串在实践中不存在；若担心，可改用 Redis 不合法字符，
但会牺牲可读性——**倾向保留可读性**，碰撞后果仅是「未知模型与某个真名模型共享水位」，
非安全问题）。

### 4.2 单条 Lua 原子占用（回答 PRD Q1 的架构风险）

PRD Q1 明确要求架构侧评估「外层占成功、内层失败时必须归还外层」这个
跨两键复合操作的原子性，并指出它直接影响 AC-50 可实现性。

**裁决：做成一条 Lua 脚本，两个键在同一次 EVAL 内判定与回滚，不存在失败窗口。**

Redis 单实例（ADR-004：本服务独占 6381）下 Lua 是原子的，且两个键
`st:slot:{th}` 与 `st:mslot:{th}:{model}` 天然同实例（无 cluster），
`KEYS[1]/KEYS[2]` 一次传入即可：

```lua
-- KEYS = [token 槽键, (model,token) 槽键]
-- ARGV = [token 上限, model 上限, ttl]
local n1 = redis.call('INCR', KEYS[1])
if n1 > tonumber(ARGV[1]) then
  redis.call('DECR', KEYS[1])
  return 0                        -- 外层超限
end
local n2 = redis.call('INCR', KEYS[2])
if n2 > tonumber(ARGV[2]) then
  redis.call('DECR', KEYS[2])
  redis.call('DECR', KEYS[1])     -- 同一原子块内归还外层，无窗口
  return -1                       -- 内层超限
end
redis.call('EXPIRE', KEYS[1], ARGV[3])
redis.call('EXPIRE', KEYS[2], ARGV[3])
return 1
```

返回码区分内外层超限，让 429 的 message 能指明是哪一层——运营排查
「为什么我被限」时这是关键信息，PRD B2 要的可观测性一半由此满足。

**AC-50 的「归还外层」要求由此变成不可能失败的分支**，不需要补偿逻辑。
释放同理做成一条双 DECR 脚本（带下溢保护），确保两层账目同生同死。

`LUA_SLOT_ACQUIRE` / `LUA_SLOT_RELEASE` **原样保留不动**：
即时任务与延迟任务都走新脚本，旧脚本在过渡期内仍被引用（滚动升级窗口内
可能有老版本 web 进程在跑），下一个版本再删。

### 4.3 三级配置解析与校准口径

上限解析（AC-49，优先级从高到低）：

```
(model,token) 专项  →  model 级  →  全局默认 max_slots
```

专项配置是**运营数据**（key 维度、条目数不可预测），不进 dynconf；
model 级是**配置**（条目数 = 模型数，可枚举），进 dynconf。见 §6 对 Q4 的裁决。

校准（AC-53）：`active_counts_by_token()` 旁边新增
`active_counts_by_model_token()`，且**两者都必须加等待期排除条件**：

```sql
-- 两个函数共用的新增谓词
AND COALESCE(data ->> '$.inflight_slot', 'false') = 'true'
```

注意这里用 `inflight_slot` 而**不是** `scheduled_at <= now`：
「已到点但正在退避重排」的任务也没占槽（§3.5），只有 `inflight_slot=true`
才是「真占了槽」的事实。校准口径必须与占槽/释放的判据**完全同源**，
否则校准本身会制造漂移——这是不变式级别的要求。

> `recalibrate_slots` 现在只处理 truth 里存在的 key（`sweeper.py:186-193`）。
> 新增维度后有个既有缺陷会被放大：**truth 里没有、但 Redis 里有残留计数的键
> 不会被清零**（只靠键 TTL 兜底）。model 维度的键基数是 token×model，
> 比原来大一个数量级，泄漏的绝对量会上升。建议同期修：校准时按
> `SCAN st:mslot:*` 与 truth 做差集清零。这条独立于本需求，但被本需求放大。

### 4.4 MAX_SLOTS 默认值上调（回答 Q1 的配套动作）

架构侧确认 PRD 的推理成立：外层维持 10 则内层永不触发，功能等于没做。
但**默认值不该是 30 这个凭空的数**，应从服务实际容量反推：

```
单 worker 并发 queue_concurrency = 64（config.py:212）
外层默认值 = 64 / 预期并发活跃 key 数
```

- 若预期同时有 ≥6 个活跃 key，外层 10 就是合理的（6×10=60 ≈ 64）；
- 取 30 意味着**2 个 key 就能吃满整个 worker**（2×30=60）。

**架构建议：外层默认取 30 可以接受，但必须同期把内层默认值定得足够小
（建议 5），并明确 `queue_concurrency` 是最终物理上限。** 否则「30」只是
把超卖从提交层挪到了 worker 队列层——任务不再被 429，而是在 stream 里排队
到超龄判死，用户体验更差（等 6h 拿到 FAILURE，不如立刻拿到 429）。

**这是需要运营 + SRE 签字的容量决策，不是架构可以单方面拍的数。**
架构侧只给出约束式：`外层默认 × 预期活跃key数 ≲ queue_concurrency × 副本数`。

---

## 5. 对既有查询与不变式的影响

### 5.1 SQL 改动清单（全部保持索引友好）

| 函数 | 改动 | 索引影响 |
|---|---|---|
| `stale_active` | 加 `scheduled_at <= now` 残差谓词 | 无（`updated_at` 粗筛不变） |
| `overdue_active` | 加 `scheduled_at < cutoff` 残差谓词（§2.3） | 无（`created_at` 粗筛不变） |
| `active_counts_by_token` | 加 `inflight_slot='true'` | 无（本就是全量 GROUP BY） |
| `active_counts_by_model_token` | 新增，`GROUP BY slot_model, token_hash` | 同上，同一次扫描可 |
| `search` | 可选：暴露 `scheduled_at` / `batch_key` 列 | 逐字段投影，遵守不变式 6 |

**`active_counts_by_token` 与 `active_counts_by_model_token` 应合并为一次查询**
（`GROUP BY th, slot_model` 后在 Python 侧聚合出两个维度），避免校准任务
对共享 `tasks` 表做两次全量 GROUP BY。这是 §4.3 的实现细节但值得写进设计：
共表扫描的代价由 new-api 一起承担。

### 5.2 新增/修订不变式

现有 12 条不变式（`MEMORY.md`）需新增 4 条：

13. **JSON 数值比较必须 `+ 0` 转型**：`data ->> '$.x'` 是 LONGTEXT，
    直接与整数比会退化为字符串比较（`'900' < '1000'` 为假）。
14. **释放槽的唯一判据是 `data.inflight_slot`，不是状态**。
    等待期任务从未占槽，按状态释放会还掉别人的槽（`LUA_SLOT_RELEASE`
    的下溢保护挡不住「有余额时的误扣」）。
15. **占槽与释放必须用同一个 `data.slot_model`**（占用时落库，释放时读库，
    绝不重算）。
16. **放行只能经 `dispatch.release`**，任何新增的入队路径都必须走它；
    直接 `publish_execute` 只允许出现在 `dispatch.release` 内部。

### 5.3 测试基建同步（不变式 7）

- `tests/conftest.py` 的 `_TASKSTORE_FUNCS` 与 `InMemoryTaskStore`
  必须加 `active_counts_by_model_token`，否则测试静默打真 MySQL；
- `_REDIS_CONSUMERS` 加到期通道与批次的键消费方；
- FakeRedis 需支持 `ZADD/ZRANGEBYSCORE/ZREM/ZCARD` 与**双键 Lua 的求值**。
  当前 FakeRedis 的 `eval` 是按脚本内容硬编码分派的，新增两条脚本要同步；
  这是本次工作量中容易被低估的一块。

---

## 6. 对 PRD 待确认问题的架构裁决

| 问题 | 裁决 |
|---|---|
| **Q1** 全局槽保留还是替换 | **保留，双层**。PRD 的四条理由成立。原子性风险已由 §4.2 单条 Lua 消除，AC-50 可实现。默认值见 §4.4——数值需运营/SRE 签字 |
| **Q2** 超限 429 硬拒 | **同意维持**。转排队是重大语义变化，且延迟头已提供等价能力 |
| **Q3** 延迟上限与生命期口径 | **PRD 的推导需重做**：漏了令牌 TTL 这条更紧的约束（B1/§2.1）。有效上限 ≈ 5h，非 6h。计时口径**不改起点、改谓词**（B3/§2.3），性能风险消除 |
| **Q4** 模型级上限存储机制 | **拆两半**：model 级上限（条目可枚举）扩 dynconf 加 `json` 类型 Spec，带条目数与值域校验，留在 ADR-005 白名单体系内；(model,token) 专项（条目不可枚举、按 key 维度）是**运营数据**，独立 Redis Hash + 独立管理端点，不进 dynconf。理由：把无界的 key 维度数据塞进「整批校验、整批回退」的配置模型会让一次误操作影响全部 key |
| **Q5** 批次默认归组键 | **同意 `token_hash + model`**，与并发维度对齐，语义自洽 |

> **实现现状（2026-09-11）：默认取 `model`，与本裁决不同，属有意偏离。**
> 归组键可配置（`Settings.batch_group_by`），两个取值：
>
> - `model`（**当前默认**）：按归一化模型名归组、跨 token 合并。
>   产品决定优先「简单 + 批次大」——批次越大 N 越容易触发，削峰效果越好，
>   且语义直白（「同模型的一批」）。代价是批内成员来自不同 token，
>   与本裁决「凑批填满同一个并发窗口」的论证不完全吻合。
> - `token_model`：本裁决口径。与并发维度严格对齐，但每个 token 各自
>   成批，批次显著变小、更依赖 T 触发兜底。
>
> 两者都不改变「放行时各自占各自 token 的槽」这一事实。`X-Batch-Key`
> 可逐请求覆盖本项（AC-58 的「显式优先」已完整实现）。
> **改回 `token_model` 只需改配置值**，但会改变所有既有接入方的批次数与
> 放行节奏，属行为变更。
>
> 运维注意：`batch_key` 在提交时即落库，`admit_due` 优先复用它，所以
> **切换维度只影响之后新提交的任务**，已在等待的成员仍按原键放行。
| **Q6** 限流与突发 500 条冲突 | **架构侧确认这是真阻塞**，但不该改默认值。建议：批量提交场景走**单独的限流档位**（按 token 配额，运营数据），而非全局上调 `rate_limit`——全局上调会让所有 key 都能高速提交，把风险从上游渠道挪到本服务的 DB 连接池 |
| **Q7** 批次保序 | **同意不承诺**。64 并发下入队顺序不决定完成顺序 |
| **Q8** SPEC 的 `SUBMITTED` 勘误 | **同意一并勘误**为 `QUEUED` |

---

## 7. 实施顺序（依赖关系强制）

分四批，**顺序不可调整**——每一批都是下一批的前置：

| 批次 | 内容 | 为什么必须在此位置 |
|---|---|---|
| **0. 前置修复** | B1 令牌 TTL 按任务计算；B2 `dispatch.release` 唯一放行点（先把 `sweep_stale` 切过去，此时行为等价）；B3 两个 sweeper 的谓词改写 | 这三项**不改变任何对外行为**，可独立上线验证。不先做，后续任何延迟任务都是 100% 失败 |
| **1. 两层并发** | 新 Lua、三级配置解析、校准双维度、`inflight_slot`/`slot_model` 字段 | 放行必须占槽（AC-43），所以占槽机制要先就位 |
| **2. 延迟/定时** | 调度头解析、`scheduled_at` 落库、ZSET + ticker、退避重排、查询视图 | 依赖批 1 的占槽与批 0 的放行点 |
| **3. 批量聚合** | 批次归组、N/T 触发、批次管理端点与看板 | 复用批 2 的 ticker 与放行点，只是触发条件不同 |

批 0 是**纯技术债偿还**，对外零变化，风险最低收益最高——建议单独一个 PR 先合。

看板（R-21/R-22）跟随各批增量交付，不单独排批。

---

## 8. 明确不做（避免范围蔓延）

- 不做上游 batch API 合并调用（PRD §7.1 已确认）。但 §2.2 的
  `dispatch.release` 恰好满足 PRD 要求的「放行是可替换策略点」——
  未来替换只需改它内部，不动任何调用方；
- 不做优先级抢占、跨 key 公平调度；
- 不做周期性重复执行（cron 语义）；
- 不做自适应水位（所有上限保持人工静态配置）。

---

## 9. 遗留风险登记

| 风险 | 影响 | 现有缓解 |
|---|---|---|
| 令牌 Redis 驻留窗口延长至最坏 12h | 安全暴露面扩大 | 只影响用了延迟的任务；上限由只读 env 封顶（§2.1）。**需安全侧签字** |
| Redis 丢 ZSET | 计划任务索引丢失 | `sweep_stale` 从 DB 事实回补（§3.4），最迟 2min 恢复，功能不失效 |
| model 维度槽键基数上升 | Redis 键残留泄漏放大 | 建议同期修校准的差集清零（§4.3 注） |
| 外层默认值 30 与 `queue_concurrency=64` 的比例 | 超卖从提交层转移到队列层 | 需运营/SRE 按 §4.4 约束式定容量 |
| FakeRedis 需支持 ZSET 与新 Lua | 测试基建工作量被低估 | 已登记（§5.3） |


