# 悬而未决登记册（OPEN-DECISIONS）

> 只追加、就地关闭。每次进入新阶段前先复现本表，逐条判断能否关闭。
> 当前：**4 未决 / 4 已决**

| Date | Source | Open Item | Related Constraints | Current Leaning | Blocked By | Resolves When | Status |
|---|---|---|---|---|---|---|---|
| 2026-08-21 | 设计 §12 ① | new-api relay 返回 5xx 时预扣配额是否确定回滚 | 决定重试安全性；本服务无退款通道 | 默认关重试（ADR-002），确认后改配置开启 | 需读 new-api relay 源码或与其维护者确认 | 拿到 relay 5xx 路径的回滚证据 | RESOLVED（保守方案落地，语义待确认） |
| 2026-08-21 | 设计 §12 ② | 超时误判的退款通道：new-api 管理 API 还是人工台账 | 对账补记 SUCCESS 后若发现实际未产出结果，钱已扣但用户无交付 | 先做人工台账：对账挂起超 TTL 打 `reconcile_manual` 告警日志，运维介入 | 需确认 new-api 是否有管理侧退款端点 | new-api 管理 API 清单确认后 | OPEN（`waiting-on-external-condition`） |
| 2026-08-21 | 设计 §12 ③ | ref_price 来源：配置 vs new-api 定价接口 | 只影响并发闸门松紧，不影响资金正确性 | 配置为主 + 兜底默认值 | - | - | RESOLVED → ADR-003 |
| 2026-08-21 | 设计 §12 ④ | channel_id 回填来源：响应头 vs 消费日志 | 仅影响看板归因，非功能性 | 尽力从响应头 `X-Channel-Id` / `X-Oneapi-Channel-Id` 取；取不到留 0，不为它去查日志 | new-api 是否稳定回吐该头未验证 | 抓一次真实响应头确认 | OPEN（`design-decision-to-evaluate`） |
| 2026-08-21 | 设计 §12 ⑤ | 超大响应是否外置对象存储 | 10MB 上限 + gzip 已覆盖出图/TTS 的常见体量 | 本期不做；超限直接落 FAILURE + `response_too_large` 并打点 | 需要真实流量分布数据 | 出现 `response_too_large` 告警累计超阈值 | OPEN（`design-decision-to-evaluate`） |
| 2026-08-21 | 设计 §12 ⑥ | 是否与 atask 共用 Redis 实例 | 故障域、内存配额、敏感数据同库 | 独立实例 + `st:` 键前缀双保险 | - | - | RESOLVED → ADR-004 |
| 2026-08-21 | 实现 §8 | 消费日志按 `X-Task-Id` 反查依赖 billing `attr_filter`，而该头需 new-api 侧把它写进 attrs | 对账精确度 | **只做精确匹配，有意不实现模糊降级**：token+时间窗模糊匹配会把用户同期的其他调用误判为本任务的扣费，补记出假 SUCCESS。宁可查不到（保持挂起转人工）也不要假阳性 | new-api 是否记录该头 | 抓一次真实 billing_logs 记录确认 | OPEN（`waiting-on-external-condition`） |
| 2026-08-21 | 实现 §4 | 入队失败时幂等键**保留**而非归还（回填在入队之前） | 幂等语义 vs 双扣风险 | 入队是「响应可能丢失」的操作：broker 已收下但确认没回来时任务其实在跑，归还占位会让重试重建第二个任务、上游被调两次。让重试回放到那条被判死的 FAILURE 更安全 | - | - | RESOLVED（见 `submit.py` 第 5 步注释与 `test_enqueue_failure_rolls_back`） |

## 三类固定 slug 说明

- `waiting-on-external-condition`：等外部条件（他方确认/第三方接口）
- `design-decision-to-evaluate`：设计待评估（需 POC 或真实数据）
- `existing-design-boundary`：现有设计边界约束

## 关闭规则

OPEN → RESOLVED 时就地补 Resolution 列内容，并在够格时升格为 `ADR-XXX.md`。禁止删除历史行。
