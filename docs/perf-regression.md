# 性能回归与验收报告

> 建立日期：2026-09-03
> 结论状态：**待真实环境重测；当前没有可采信的收益数字**

## 1. 当前基线门禁

在工作区 `/Users/betterme/PycharmProjects/AI/stask-service` 执行：

| 门禁 | 结果 | 说明 |
|---|---|---|
| `ruff check .` | 通过 | 默认环境的 Ruff 通过 |
| `mypy app tests` | 失败 | `tests/conftest.py:156` 将 `Any \| list[int] \| None` 赋给 `list[int]` |
| `.venv/bin/python -m pytest -q` | 失败 | 203 passed，1 failed；`tests/test_misc.py::test_ruff_clean` 检出 `app/services/dynconf.py:33` 未使用的 `dataclass` 导入 |
| 系统 `pytest -q` | 无法收集 | 系统环境缺少 `respx` |

上述是当前代码状态的事实记录，不把静态检查或测试结果当成性能收益。性能验收前必须先修复门禁，或由变更负责人明确记录对应基线。

## 2. 真实路径与脚本

提交链路的唯一目标是 Spec §10 的 `POST/PUT /async/{path}` P99 < 80ms（不含 billing RTT）。真实代码路径为：

1. HTTP 请求进入提交路由；
2. 幂等占位；
3. 身份/余额信息；
4. upstream 与路径准入；
5. Redis Lua 占槽；
6. `tasks` 表落库；
7. token session 写入；
8. 幂等回填；
9. 入队并返回 `202 + task_id + Location`。

`/Users/betterme/PycharmProjects/AI/stask-service/scripts/bench_submit.py` 是已有单轮提交压测器，但只报告一次请求批次，缺少预热、多轮离散度、强制健康检查、正确性守卫和 CPU 采样，不能单独作为回归验收依据。

新增 `/Users/betterme/PycharmProjects/AI/stask-service/scripts/bench_architecture.py`，仅通过 HTTP 调用真实服务路径，并提供：

- 启动前 `GET /healthz/ready` fail-fast；
- 显式预热（默认 30 次，结果丢弃）；
- 多轮测量（默认 5 轮），每轮独立报告 p50/p99、状态码、吞吐相关耗时；
- 每个请求唯一 `Idempotency-Key`，避免幂等回放污染；
- 正确性断言：`202` 必须有 JSON `task_id` 与 `Location`；
- 成功路径占比守卫：`202` 占比低于 95% 直接 FAIL，避免把 429/错误拒绝路径当成吞吐；
- `--server-pid` 采样实际 web 进程 CPU，同时报告压测客户端 CPU；
- p99 必须严格低于可配置 SLO（默认 80ms），否则退出失败；
- 输出代码版本字段（优先 `GIT_COMMIT`，未提供时明确提示）。

示例（必须使用真实服务 token 与 web worker PID）：

```bash
.venv/bin/python scripts/bench_architecture.py \
  --url http://127.0.0.1:8000 \
  --token sk-xxx \
  --server-pid 12345 \
  --concurrency 50 \
  --requests 1000 \
  --warmup 30 \
  --rounds 5
```

该脚本不是简化函数探针。它的 HTTP、Redis、数据库、身份服务和队列依赖均属于真实运行时路径。因而没有服务与依赖时，不应改用内部函数计时来替代端到端结论。

## 3. 压测资格与可比性

### 环境资格

- 当前运行环境为 macOS 开发机；未启动 MySQL/Redis/new-api/billing 全链路，也没有真实 HTTP 压测数字。
- 开发机、Docker Desktop、共享 CI 的绝对吞吐不能外推生产容量；最多用于**同一环境、同一版本、同一配置的 A/B 相对比较**。
- 必须记录 Python、依赖版本、服务启动方式、worker 数/并发、CPU/内存限制、MySQL/Redis/new-api/billing 地址与版本。敏感值只记是否配置，不记录 token、密码或连接串。

### 每轮固定流程

1. 记录 `git rev-parse HEAD`（当前仓库尚无 commit，正式验收前必须记录可追溯版本标识）。
2. 启动并确认 `/healthz/ready` 返回 2xx；任何依赖不 ready 立即退出，不产出数据。
3. 每个版本只改一项配置；先清理上一轮测试任务/状态与幂等键，避免历史任务令槽位耗尽。不得在生产数据上 `FLUSHDB`。
4. 预热至少 30 秒或等价请求量，丢弃爬坡数据；脚本默认 30 次请求只是最低可执行门槛，容量评估建议按时间预热。
5. 正式测量至少 5 轮，记录每轮 p50/p95/p99、状态码、成功占比、客户端 CPU、服务端 CPU；报告中取中位数，不能挑最好一轮。
6. 测量期间外部采集数据库、Redis、billing 和 relay 的 CPU/连接池/错误率；`--server-pid` 只覆盖服务进程，不覆盖容器内依赖。
7. 一次只比较一个改动；同一轮的请求体、并发、配额、超时、连接池和上游响应必须一致。

### 双侧 CPU 归因

无双侧 CPU 数据时，不得宣称服务端饱和或容量提升：

| 服务端 CPU | 客户端 CPU | 判定 |
|---|---|---|
| 高/接近单核饱和 | 未饱和 | 才能初步支持服务端 CPU 瓶颈 |
| 未饱和 | 高/接近饱和 | 客户端瓶颈，数据不可作服务容量结论 |
| 都未饱和 | 都未饱和 | 可能是网络、锁、数据库或其他依赖瓶颈，不能臆测 |

容器部署时，除 `--server-pid` 外必须在宿主机记录全局 `docker stats --no-stream` 快照（web、worker、Redis、数据库及压测客户端），并注明采样时间窗口。

## 4. 正确性闸门

性能数据仅在以下条件全部满足时有效：

- ready 检查通过；
- 每个请求使用唯一幂等键；
- `202` 响应均带 `task_id` 和 `Location`；
- 正式轮 `202` 成功占比至少 95%；
- 没有因 `ST_MAX_SLOTS`、rate limit、上游 mock 或 billing 错误而主要走拒绝路径；
- 核对 `/ops/stats`，确认任务真实落库且状态分布符合场景；
- 端到端正确性（幂等重放、并发冲突、槽位上限、失败回滚）仍由现有功能测试覆盖，性能脚本不以吞吐掩盖这些回归。

如果要验收 `limit=50` 的并发槽正确性，应另行使用受控场景断言：恰好 50 个请求通过、其余返回 429、无槽泄漏；该正确性场景与吞吐场景分开报告。

## 5. 收益体现位置与阈值

- 减少一次 Redis/HTTP 往返等 I/O 优化：优先看真实提交 P99/p95 与服务端 CPU；不使用纳秒级内部探针收益替代端到端收益。
- Python 对象/解析/锁等 CPU 优化：优先看单 worker 吞吐和 CPU 占用，只有其成本足够大时才期待 P99 变化。
- 当前没有可信优化前基线，因此**不预设好看的百分比阈值，也不捏造提升数字**。应先在优化前版本按本方案采集 5 轮，再由两侧结果和离散度共同确定阈值。
- 多轮相对离散度超过约 20% 时，信号不足，任何小于该离散度的提升均判为需重测；必须先降低噪声。
- 任何容量结论都必须限定为“在记录的环境/配置/负载下”；开发机结果不得写成“可承载 X QPS”。

## 6. 当前验收结论

```text
数字可信度: 无数字，需重测
口径确认: 已提供真实 HTTP 提交路径脚本；尚未执行真实服务压测
瓶颈归属: 未知；尚无双侧 CPU 采样
环境资格: 仅允许未来同环境 A/B；无生产容量资格
离散度: 未采集
基线有效性: 静态/功能基线未全绿，且尚无优化前性能基线
收益体现位置: 依优化类型分别看 P99、吞吐或 CPU；不得混用
```

正式验收报告必须把每轮原始输出、代码版本、环境声明和双侧 CPU 快照作为附件或归档，而不是只保留汇总百分比。
