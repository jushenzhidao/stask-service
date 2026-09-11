"""任务执行结果摘要（taskiq ``return_value`` 的载荷）。

为什么需要它：taskiq-admin 的任务详情页只有三个数据源——
``args``、``Return Value``（= ``TaskiqResult.return_value``）、
``Error``（= ``repr(TaskiqResult.error)``）。我们的任务体原先声明
``-> None`` 且内部吞掉所有异常，于是面板上恒显示 ``null`` + 空 Error，
排障只能回 DB / logfire 捞——admin 沦为「只能看状态和耗时」。

本模块把执行链路已经掌握的事实（终态、上游状态码、失败原因、制品数、
命中的解析级别、耗时…）收拢成一个可 JSON 序列化的扁平 dict，交给
taskiq 落进 result backend。约束：

- **必须可 JSON 序列化**：result backend 走 json/pickle，放 bytes、
  httpx 对象、异常实例都会让结果保存失败（那会连状态都看不到）；
- **绝不含令牌**：不放 Authorization、raw_token、完整 request_headers。
  ``token_hash`` 是哈希值（已落库字段），可放；
- **体积可控**：``upstream_preview`` 截断，绝不放完整响应体——
  原文已在 DB 的 ``upstream_response`` 里，admin 不是回放工具；
- **恒不抛**：构造过程不做任何可能失败的操作（见 ``as_dict``）。

``ok`` 语义 = 「本次执行是否把任务推进到了预期终态」，不等于 HTTP 2xx：
上游 4xx 时任务被正确判为 FAILURE，执行链路本身工作正常，但对使用者
而言这是一次失败任务——``ok=False`` 让它在 admin 上带红色 Error 显现，
这正是主人要的「错误信息尽可能全面」。
"""

from __future__ import annotations

import dataclasses as dc
from typing import Any

#: 摘要里 upstream 预览的最大字符数（DB 存完整原文，这里只留线索）
PREVIEW_LIMIT = 300


@dc.dataclass(slots=True)
class Outcome:
    """一次 ``execute_task`` 的执行摘要。

    ``stage`` 是排障的第一落点，标明执行走到哪一步就结束了：

    ============================ ==================================================
    stage                        含义
    ============================ ==================================================
    ``success``                  上游 2xx，落 SUCCESS（附制品清单）
    ``upstream_error``           上游非 2xx，落 FAILURE，原文已存库可回放
    ``response_too_large``       上游 2xx 但体积超限，判死且**不存**响应体
    ``unreachable``              连接层失败，请求从未到达上游（重试过）
    ``timeout``                  请求已发出但结果拿不回来，绝不重试
    ``token_missing``            令牌会话丢失，调用前判死
    ``bad_request_body``         落库的请求体解不开，调用前判死
    ``lock_held``                派发锁在，跳过（重投防护生效）
    ``not_found``                任务行不存在
    ``already_terminal``         已是终态，跳过
    ``crashed``                  未预期异常，任务留给 sweeper 收敛
    ============================ ==================================================
    """

    task_id: str
    stage: str
    ok: bool = True
    status: str = ""
    """任务终态（SUCCESS/FAILURE），未落终态时为空。"""
    upstream_status: int = 0
    fail_reason: str = ""
    attempts: int = 1
    model: str = ""
    request_path: str = ""
    response_bytes: int = 0
    content_type: str = ""
    response_encoding: str = ""
    """落库的响应体编码：``plain`` / ``gzip+b64``。排障时一眼看出该按哪种方式读 ``upstream_response``。"""
    artifact_count: int = 0
    artifact_parser: str = ""
    """命中的解析级别（known/walk/inline/none），区分「三级全空」与「首级误命中」。"""
    artifact_types: list[str] = dc.field(default_factory=list)
    artifact_urls: list[str] = dc.field(default_factory=list)
    """成功结果直给：前若干条制品 URL，admin 上可直接点开验证产物。"""
    result_url: str = ""
    upstream_preview: str = ""
    detail: str = ""
    """人类可读的一句话结论，admin 的 Error 列直接用它。"""

    def as_dict(self) -> dict[str, Any]:
        """转 JSON 友好 dict。空值剔除，让面板上只留有信息量的字段。

        ``ok`` 必须显式保留：``False == 0`` 会被空值规则误删，而它恰是
        面板上最需要的一列。
        """
        raw = dc.asdict(self)
        return {k: v for k, v in raw.items() if k == "ok" or v}

    def summary(self) -> str:
        """一行结论（middleware 用它填 ``result.error``）。"""
        if self.detail:
            return self.detail
        bits = [f"stage={self.stage}"]
        if self.status:
            bits.append(f"status={self.status}")
        if self.upstream_status:
            bits.append(f"upstream={self.upstream_status}")
        if self.fail_reason:
            bits.append(f"reason={self.fail_reason}")
        return " ".join(bits)


class TaskExecutionError(Exception):
    """携带完整摘要的「非抛出型」错误对象。

    **从不 raise**——只由 ``OutcomeMiddleware`` 实例化后赋给
    ``TaskiqResult.error``，让 taskiq-admin 的 Error 列有内容可显示。
    真 raise 会让 taskiq 认定任务失败并按 ack 策略重投，而重投正是
    派发锁要防的事（见 ``app.services.execute`` 模块 docstring）。
    """

    def __init__(self, outcome: Outcome) -> None:
        super().__init__(outcome.summary())
        self.outcome = outcome

    def __repr__(self) -> str:
        # admin 存的是 repr()，这里让它可读而非 <...object at 0x...>
        return f"TaskExecutionError({self.args[0]})"


def preview(raw: bytes, limit: int = PREVIEW_LIMIT) -> str:
    """响应体预览（截断 + 安全解码）。失败路径专用。"""
    head = raw[:limit].decode("utf-8", errors="replace").strip()
    if len(raw) > limit:
        head += f" ...(truncated, {len(raw)} bytes total)"
    return head


#: ``fail_reason`` 落 tasks 表的 TEXT 列，写侧另有 500 字符截断（``taskstore.cas``）。
#: 这里先截到更短的长度：``fail_reason`` 是看板「失败原因 Top 10」的 GROUP BY 键，
#: 太长会让每条失败都成为独立分组，排行榜彻底失去聚合意义。
REASON_DETAIL_LIMIT = 300

#: 上游错误消息的常见字段名，按「越具体越先试」排列。
#: OpenAI 系 ``{"error":{"message":...}}`` 最常见；国产厂商多用扁平
#: ``message`` / ``msg`` / ``error_msg``；Ark 用 ``{"error":{"message","code"}}``。
_MESSAGE_KEYS = ("message", "msg", "error_msg", "errorMessage", "detail", "reason",
                 "error_message", "description")
_CODE_KEYS = ("code", "error_code", "errorCode", "type", "status")


def _first_str(node: dict[str, Any], keys: tuple[str, ...]) -> str:
    for key in keys:
        value = node.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
        # 有些厂商的 code 是整数（HTTP 风格错误码）
        if isinstance(value, int) and not isinstance(value, bool):
            return str(value)
    return ""


def upstream_error_detail(raw: bytes, content_type: str = "") -> str:
    """从上游非 2xx 响应体里提取**人类可读的错误消息**。

    存在的理由：原先 ``fail_reason`` 只写 ``"upstream 400"``——看板、
    回调、``/async`` 查询三处都只能告诉用户"上游返回了 400"，真正的原因
    （模型不存在 / 参数非法 / 内容审核不通过 / 渠道无可用 key）全都躺在
    ``upstream_response`` 里等人手工解码。

    解析策略（逐级退化，任何一级失败都回落到下一级，**绝不抛异常**）：

    1. JSON 且是 dict → 试 ``error``/``data``/``result`` 信封，逐层找
       ``message`` 类字段；找到后拼上 ``code``（若有）；
    2. JSON 但形状不认识 → 回落到截断的原文预览；
    3. 非 JSON（HTML 错误页、纯文本）→ 截断的原文预览。

    返回空串表示"没有任何可读信息"（响应体为空），调用方保持原有的
    ``upstream {status}`` 即可。
    """
    if not raw:
        return ""
    text = _extract_from_json(raw)
    if not text:
        # 非 JSON 或未识别形状：原文截断。HTML 错误页会被压成一行，
        # 信息密度低但至少能看出是不是网关拦截（"502 Bad Gateway" 之类）。
        text = " ".join(preview(raw, REASON_DETAIL_LIMIT).split())
    return text[:REASON_DETAIL_LIMIT]


def _extract_from_json(raw: bytes) -> str:
    """JSON 体 → ``"message (code)"``；解析失败或形状不认识返回空串。"""
    import json

    try:
        parsed = json.loads(raw)
    except (ValueError, UnicodeDecodeError):
        return ""
    if isinstance(parsed, str):
        return parsed.strip()
    if not isinstance(parsed, dict):
        return ""

    # 候选节点：根 + 常见错误信封（一层足够——没见过嵌两层的错误体）
    candidates: list[dict[str, Any]] = [parsed]
    for key in ("error", "data", "result", "response", "body"):
        child = parsed.get(key)
        if isinstance(child, dict):
            candidates.append(child)
        elif isinstance(child, str) and child.strip() and key == "error":
            # ``{"error": "invalid model"}`` 这种扁平写法
            return child.strip()

    for node in candidates:
        message = _first_str(node, _MESSAGE_KEYS)
        if message:
            code = _first_str(node, _CODE_KEYS)
            return f"{message} ({code})" if code and code not in message else message
    return ""
