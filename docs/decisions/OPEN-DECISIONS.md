# 悬而未决登记册（OPEN-DECISIONS）

> 只追加、就地关闭。每次进入新阶段前先复现本表，逐条判断能否关闭。
> 当前：**1 未决 / 7 已决**

| Date | Source | Open Item | Related Constraints | Current Leaning | Blocked By | Resolves When | Status |
|---|---|---|---|---|---|---|---|
| 2026-08-21 | 设计 §12 ① | 上游 5xx 是否有副作用（决定重试安全性） | 本服务不做计费，副作用归上游自理 | 默认关重试（ADR-002），确认后在管理页开启 | 需与上游维护者确认 | 拿到上游 5xx 路径的副作用证据 | RESOLVED（保守方案落地，改为纯配置问题） |
| 2026-08-21 | 设计 §12 ② | 超时误判的退款通道 | — | — | — | — | RESOLVED（v0.2 去计费化：本服务零资金动作，不存在退款问题；超时直接判死 FAILURE） |
| 2026-08-21 | 设计 §12 ③ | ref_price 来源 | — | — | — | — | RESOLVED（v0.2 去计费化：删除 ref_price/余额槽位，改为固定并发上限 `ST_MAX_SLOTS`） |
| 2026-08-21 | 设计 §12 ④ | channel_id 回填来源 | — | — | — | — | RESOLVED（v0.2 / ADR-006：channel_id 改为**本服务写入的独立渠道号** `ST_CHANNEL_ID`，不再从响应头回填） |
| 2026-08-21 | 设计 §12 ⑤ | 超大响应是否外置对象存储 | 10MB 上限 + gzip 已覆盖出图/TTS 的常见体量 | 本期不做；超限直接落 FAILURE + `response_too_large` 并打点 | 需要真实流量分布数据 | 出现 `response_too_large` 告警累计超阈值 | OPEN（`design-decision-to-evaluate`） |
| 2026-08-21 | 设计 §12 ⑥ | 是否与 atask 共用 Redis 实例 | 故障域、内存配额、敏感数据同库 | 独立实例 + `st:` 键前缀双保险 | - | - | RESOLVED → ADR-004 |
| 2026-08-21 | 实现 §8 | 消费日志按 X-Task-Id 反查（对账精确匹配） | — | — | — | — | RESOLVED（v0.2 去计费化：对账机制整体删除；X-Task-Id 头保留仅作上游日志反查排障用） |
| 2026-08-21 | 实现 §4 | 入队失败时任务行**保留为 FAILURE** 而非删除 | 幂等语义 vs 上游被重复调用 | 入队是「响应可能丢失」的操作：broker 已收下但确认没回来时任务其实在跑，删行会让重试重建第二个任务、上游被调两次。让自动幂等把重试回放到 FAILURE 更安全 | - | - | RESOLVED（见 `submit.py` 回滚注释与 `test_enqueue_failure_rolls_back`） |

## 三类固定 slug 说明

- `waiting-on-external-condition`：等外部条件（他方确认/第三方接口）
- `design-decision-to-evaluate`：设计待评估（需 POC 或真实数据）
