"""请求/响应体 → 日志摘要（体量闸门：**截断但不脱敏**）。

切片自 `app/services/execute.py`（2026-09-13）。抽出理由：它是一整块自成一体的
纯函数（体量上限常量 + ``_clip`` / ``_log_headers`` / ``_shrink`` / ``_digest``），
与执行流水线毫无耦合，留在大文件里只是让它超长。

**唯一的屏蔽项是凭证头名**（AC-30）：``_log_headers`` 摘掉 Authorization / Cookie 等，
其余内容（含签名 URL）按内部审计口径原样进日志。
"""

from __future__ import annotations

import json
from typing import Any


# ---- 请求/响应体日志摘要（2026-09-12）--------------------------------------
# 事实源仍是 ``tasks.data``（原文落库）；日志只承担「一条 trace 里看懂发了
# 什么、回来什么」。**不脱敏**（内部审计口径，含带签名的 URL），只按长度
# 截断——内联 b64（i2v 的图、内联音频）是这里唯一的规模问题。
#
# 唯一豁免脱敏的是**凭证头名**（见 ``_CREDENTIAL_HEADERS``）：AC-30 是红线
# （用户 sk 不进日志），与「日志内容不脱敏」不冲突。

#: 一条日志里 body 摘要的总字符上限
_LOG_BODY_LIMIT = 4000
#: 结构化字段里单个字符串的上限（超过按「前缀 + 实际长度」截断）
_LOG_STR_LIMIT = 500
#: 单个数组最多保留几项
_LOG_LIST_LIMIT = 20
#: 超过此字节数就不再 JSON 解析：大体内联 b64，解析纯属给日志白花 CPU 与
#: 峰值内存（请求体本就在内存里，再 parse 一遍等于翻倍）
_LOG_PARSE_LIMIT = 256 * 1024
#: 不解析的大体只留头部这么多字节（JSON 的 model / prompt 一般在前部）
_LOG_HEAD_BYTES = 800

#: 携带凭证的请求头名（小写）。落库的 ``request_headers`` 已由
#: ``admission.clean_headers`` 摘掉这些，这里再兜一道：AC-30 是红线，
#: 不能依赖调用链上某个环节永远不出错。除这些之外一律原样进日志。
_CREDENTIAL_HEADERS = frozenset({
    "authorization", "cookie", "set-cookie", "proxy-authorization",
    "x-api-key", "api-key",
})


def _clip(text: str, limit: int) -> str:
    """超长即截断，并标注**原始长度**（不脱敏，只控体量）。"""
    if len(text) <= limit:
        return text
    return f"{text[:limit]}…<truncated, {len(text)} chars total>"


def _log_headers(headers: dict[str, Any]) -> dict[str, Any]:
    """日志用请求头：只剔除携带凭证的头名，其余（含自定义头）原样保留。"""
    return {k: v for k, v in headers.items() if k.lower() not in _CREDENTIAL_HEADERS}


def _shrink(node: Any, depth: int = 0) -> Any:
    """JSON 结构瘦身：长字符串/长数组截断，``data:…;base64`` 只留 MIME 与长度。

    内联 b64 保留 500 字符前缀毫无信息量（同一字符表重复），换成
    ``<inline data:image/png;base64 ~1.8M chars>`` 才能一眼看出「发了张图」。
    """
    if depth >= 4:
        return "…"
    if isinstance(node, dict):
        return {str(k): _shrink(v, depth + 1) for k, v in node.items()}
    if isinstance(node, list):
        head = [_shrink(v, depth + 1) for v in node[:_LOG_LIST_LIMIT]]
        rest = len(node) - _LOG_LIST_LIMIT
        return [*head, f"…<{rest} more>"] if rest > 0 else head
    if isinstance(node, str):
        marker = node.find(";base64,")
        if node.startswith("data:") and 0 < marker < 64:
            return f"<inline {node[:marker]};base64 ~{len(node)} chars>"
        return _clip(node, _LOG_STR_LIMIT)
    return node


def _digest(raw: bytes) -> str:
    """请求/响应体 → 日志摘要（**截断但不脱敏**，恒不抛）。

    - 空体 → 空串；
    - 超 ``_LOG_PARSE_LIMIT`` → 不解析，只留头部 + 总字节数；
    - JSON dict/list → 逐层瘦身后序列化，再按 ``_LOG_BODY_LIMIT`` 裁一刀；
    - 其余（HTML 错误页、二进制、非法 UTF-8）→ 容错解码后截断。

    恒不抛的理由与执行链路一致：摘要只服务观测，构造失败绝不能影响任务。
    """
    if not raw:
        return ""
    if len(raw) > _LOG_PARSE_LIMIT:
        head = raw[:_LOG_HEAD_BYTES].decode("utf-8", errors="replace")
        return f"{head}…<not parsed, {len(raw)} bytes total>"
    try:
        parsed = json.loads(raw)
    except (ValueError, UnicodeDecodeError):
        return _clip(raw.decode("utf-8", errors="replace"), _LOG_BODY_LIMIT)
    if not isinstance(parsed, dict | list):
        return _clip(raw.decode("utf-8", errors="replace"), _LOG_BODY_LIMIT)
    return _clip(json.dumps(_shrink(parsed), ensure_ascii=False), _LOG_BODY_LIMIT)
