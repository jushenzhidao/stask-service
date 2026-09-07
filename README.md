# stask-service — 通用 HTTP 异步任务服务

把一个**同步 HTTP 接口**变成长任务：给目标路径加 `/async` 前缀提交，毫秒返回
本地 `task_id`，结果异步取回。除此之外没有别的职责——**不做计费、不做 key
管理**，只做队列化 worker。Authorization 原样透传给上游，有效性由上游判定。

**上游只是一个 HTTP 服务**（new-api 是默认实现，换任何同步生成接口只要进
allowlist 即可）。鉴权、渠道选择、配额扣费、限流全部在上游内部闭环。

```
nginx（同一域名）
├─ /async/ → stask-service          其余 → 上游 HTTP
│
stask web    提交：请求指纹自动幂等 → 占槽 → 落库 NOT_START → 令牌会话 → 入队 → 202
stask worker 执行：派发锁 → 用户令牌调上游同步接口 → 响应即终态落库 → 释放槽/清会话/回调
上游          鉴权 / 渠道 / 扣费 / 限流，零改动
```

状态机：`NOT_START → IN_PROGRESS → SUCCESS / FAILURE / CANCELED`
（与 new-api tasks 表原生枚举对齐，ADR-006）。

> **已知耦合（有意保留）**：任务行落在与 new-api 同实例的 `tasks` 表上，靠
> `platform='stask'` + 独立 `channel_id` + `quota=0` 与上游任务零冲突共存
> （契约见 ADR-006）。彻底解耦需独立存储 + 迁移方案。

---

## 快速开始

```bash
make setup       # 建 venv、装依赖、生成 .env
make check       # 三项门禁：ruff + mypy + pytest（不需要 MySQL/Redis）
make up          # 起全套（web + worker + redis）
```

`make` 无参数列出所有命令。两种运行形态按需选：

| 形态 | 命令 | 适用 |
|---|---|---|
| 三容器 | `make up` | 标准部署，worker 可独立扩副本 |
| 本机单进程 | `make standalone` | 本地开发、单机试用，一条命令起全套且**免 .env** |

接真实环境时 `.env` 至少改三项：`ST_UPSTREAM_BASE_URL`（上游地址）、
`ST_DATABASE_URL`（指向 tasks 表所在库）、`ST_UPSTREAM_ALLOWLIST`。
**必须配 `ST_CHANNEL_ID`**：在 new-api 建一个占位渠道（可禁用）并填其 id——渠道不存在会被上游轮询批量误判 FAILURE（详见 ADR-006）。

---

## API

| Method | Path | 说明 |
|---|---|---|
| POST/PUT | `/async/{path}` | 提交。202 + `{task_id,status,replayed}` + `Location` 头，**自动幂等** |
| GET | `/async/{path}/{task_id}` | 查询。202 进行中 / 200 原文回放 / 重放上游错误码；`?wait=60` 长轮询 |
| DELETE | `/async/{path}/{task_id}` | 取消。排队中 → CANCELED；执行中 → 409 |
| GET | `/healthz/live` `/healthz/ready` | 探针 |
| GET | `/ops/stats` `/ops/tasks/{id}` | 观测（带 Bearer 即可，脱敏） |
| POST | `/ops/sweep/stale` `/ops/slots/recalibrate` | 手工触发兜底任务 |

提交示例：

```bash
curl -X POST https://api.example.com/async/v1/images/generations \
  -H "Authorization: Bearer sk-xxx" \
  -H "Content-Type: application/json" \
  -H "X-Callback-Url: https://myapp.com/hook" \
  -H "X-Upstream-Base-Url: https://newapi.com" \
  -d '{"model":"dall-e-3","prompt":"a red cube","n":1}'
# → 202 {"task_id":"dall_e_3_ab12...","status":"NOT_START","replayed":false}
#   Location: /async/v1/images/generations/dall_e_3_ab12...

curl "https://api.example.com/async/v1/images/generations/dall_e_3_ab12...?wait=60"
# → 200 + 与直接调同步接口**字节级一致**的响应体
```

- `X-Upstream-Base-Url` 可选（默认读 env `ST_UPSTREAM_BASE_URL`），host 必须
  在 allowlist 内。
- **自动幂等**：`task_id = {model}_{sha256(token|method|path|query|body|盐)[:32]}`。
  同一 token 的字节级相同请求在幂等窗口内恒返回同一任务，客户端重试**不需要
  带任何头**；想强制重跑同一请求，带 `Idempotency-Key: 任意新值` 作盐即可。

管理面（`X-Admin-Key` 鉴权，未配置密钥时全部 404）：

| Method | Path | 说明 |
|---|---|---|
| GET | `/admin` | 看板页面 |
| GET | `/admin/api/overview?window=` | 概览指标 |
| GET | `/admin/api/tasks?status=&model=&task_id=&task_id_prefix=&limit=&offset=` | 任务列表 |
| GET | `/admin/api/tasks/{id}` | 任务详情（脱敏） |
| POST | `/admin/api/tasks/{id}/requeue` | 重投队列（不清派发锁） |
| GET/PUT | `/admin/api/config` | 读写运行时配置 |
| POST | `/admin/api/config/reset` | 重置回落 env |
| POST | `/admin/api/jobs/{stale\|overdue\|slots\|purge}` | 手工触发定时任务 |

---

## 与 new-api 的共存契约（ADR-006）

外部任务复用最新版 new-api `tasks` 表，与其内部任务零冲突：

| 条件 | 效果 |
|---|---|
| `platform='stask'`（非 suno/mj） | `GetTaskAdaptorFunc` 返回 nil，原生任务轮询天然跳过 |
| `channel_id` = **new-api 中真实存在的渠道**（`ST_CHANNEL_ID`，占位渠道可禁用） | 轮询按渠道分组；渠道必须存在——CacheGetChannel 失败先于 adaptor nil 检查，会把整组任务批量误判 FAILURE |
| `quota = 0` | 上游 24h 超时清理即便动到本行，退款也是 0，零资金影响 |
| `status`/`progress` 原生枚举 | 共享表对上游看板/巡检工具可读 |
| 生命期 6h（`ST_TASK_MAX_LIFETIME_SECONDS`）| 先于上游 24h 清理线自行收敛，正常情况上游永远碰不到我们的活跃行 |

写读侧 WHERE 恒带 `platform`，绝不动别人的行。

---

## 设计红线

1. **零资金动作**——不冻结、不结算、不对账；扣费成败是上游和用户之间的事。
2. **令牌不落库不进日志**——只放 Redis 会话（TTL 2h），终态即清。
   loguru 固定 `backtrace=False, diagnose=False`。
3. **tasks 表时间列必须归一**——读侧 `as_unix_seconds`，SQL 侧 `_secs(col)`。
   表被 new-api 原生模块用 UnixMilli 写过，两侧都不能省。
4. **写读侧 WHERE 必带 `platform`**——共享表，绝不动别人的行。
5. **派发锁在 = 一次调用已发出 = 绝不再调上游**——队列 at-least-once，防重全靠锁。
6. **超时/传输断判 FAILURE 且不重试**——请求可能已产生副作用，重发才是事故；
   结果拿不回来，留挂着毫无意义。
7. **安全项不可运行时改**——白名单类配置只读 env，管理面拒绝写入（ADR-005）。

---

## 关键决策

| 决策 | 取值 | 依据 |
|---|---|---|
| 幂等 | **自动**：task_id = 请求指纹 | 客户端零心智负担；`Idempotency-Key` 降级为可选盐 |
| 共存 | platform + 独立渠道 + quota=0 + 6h 生命期 | ADR-006：外部任务不影响 new-api 内部任务 |
| 5xx 重试 | 默认关（`ST_RETRY_MAX=0`） | ADR-002：上游调用可能有副作用 |
| Redis | 独立实例 + `st:` 键前缀 | ADR-004：故障域隔离，前缀是误配时的第二道防线 |
| 建表 | 零 | ADR-001：复用上游同实例 `tasks` 表 |
| 动态配置 | 白名单子集可热改 | ADR-005：安全项开放等于把防线挂到网上 |

未决事项见 [`docs/decisions/OPEN-DECISIONS.md`](docs/decisions/OPEN-DECISIONS.md)。

---

## 目录结构

```
app/
├── config.py         配置单例（ST_ 前缀，禁止散读 os.environ）
├── db.py             MySQL 惰性引擎（零建表）
├── redis.py          键规范 + Lua 脚本（全部集中在此）
├── errors.py         {"error":{...}} 统一形制
├── queue.py          taskiq broker/scheduler/发布门面
├── main.py           应用装配
├── standalone.py     单进程模式（web + worker + scheduler 同事件循环）
├── static/           admin.html 单文件看板（零构建）
├── deps/             auth（Bearer 提取，不外呼）/ ratelimit（滑窗）/ admin（管理密钥）
├── routers/          proxy（/async 通配）/ ops / admin（看板 + 配置）
└── services/
    ├── admission.py    路径准入 + upstream 三防线 + 头清洗
    ├── idem.py         自动幂等（请求指纹 task_id + 创建窗口占位）
    ├── submit.py       提交链路（失败即回滚）
    ├── execute.py      worker 执行（派发锁 + 分流）
    ├── flow.py         查询 / 字节级回放 / 长轮询 / 取消
    ├── sweeper.py      卡死收敛 / 超龄判死 / 槽位校准 / 结果清理
    ├── taskstore.py    tasks 表原生 SQL（数据访问单点）
    ├── dynconf.py      运行时配置白名单（Redis > env > 默认）
    ├── slots.py        并发槽（固定上限）
    ├── tokensession.py 用户令牌会话（唯一查询处）
    ├── codec.py        gzip + b64
    └── notify.py       HMAC 签名回调
```

## 兜底扫描（四只 sweeper）

| 任务 | 频率 | 作用 |
|---|---|---|
| 卡死收敛 | 每 2 分钟 | 锁过期 + 从未派发（消息丢失）→ 重投；锁过期 + 派发过（结果不可得）→ 判死 |
| 超龄判死 | 每 5 分钟 | 超过最大生命期（6h）仍非终态 → FAILURE，先于 new-api 24h 清理线收敛 |
| 槽位校准 | 每 5 分钟 | Redis 计数按 tasks 表事实双向修正 |
| 结果清理 | 每小时 | 清空超期 `upstream_response`，状态行保留（查询返回 410）|

## 测试

216 个用例，不依赖真实 MySQL / Redis / 上游：
Redis 用手写 FakeRedis（Lua 按 `app/redis.py` 常量做等价 Python 实现），
tasks 表用 InMemoryTaskStore（保留 CAS 与 data 合并语义），
出站 HTTP 用 respx 拦截（未声明的请求立即失败），
`mypy` 与 `ruff` 各做成一个 pytest 用例。
