"""上游响应 → 制品清单（多路逐级退化解析）。

## 为什么需要这一层

本服务是**通用**任务网关：给任意同步接口加 `/async` 前缀即任务化，
上游可能是 Ark（豆包）、Kling、MiniMax、fal、acedata……每家的成功响应
形状都不同，且同一厂商图片/视频/音频任务的字段名也不同。看板要显示
「制品」列、客户端要拿可下载 URL，就必须有一层与厂商无关的归一化。

## 三级退化（严格按顺序，命中即停止下钻）

1. **精确路径**：`_KNOWN_PATHS` 里逐条试已知厂商形状
   （Ark 图片 `data[].url`、Ark 视频 `content.video_url`、
   fal `images[].url`、MiniMax `file.download_url` ……）；
2. **键名递归**：全树遍历，靠**键名**判定媒介类型
   （`*_url` / `url` / `file` / `images` ...），深度上限 8、带环检测；
3. **内联字节**：`b64_json` / `data:` URI / 裸 base64 —— 无 URL 可回源时
   把字节自身作为制品（`inline=True`），由读侧直接吐给客户端。

三级都空 → 返回空清单（**不是错误**）：纯文本补全类任务本就没有制品，
看板显示「—」即可，绝不能因此把成功任务标记失败。

## 契约（对齐 new-api-plugins 插件的 artifact 协议）

产出 `Artifact`：`key` 稳定且同任务内唯一（`image` / `image_2` / `video`），
`type` ∈ `image|video|audio|file`，`mime_type` 尽力推断，
`url` 与 `inline_b64` 二者其一。`credentialless` 恒 True —— 厂商返回的
都是预签名 CDN 地址，回源**绝不能**带渠道 Authorization（会 403，
且等于把渠道密钥漏给第三方 CDN）。
"""

from __future__ import annotations

import base64
import binascii
import json
import re
from dataclasses import dataclass, field
from typing import Any

from app.logging import log

_MAX_DEPTH = 8
_MAX_ARTIFACTS = 32
_URL_RE = re.compile(r"^https?://", re.IGNORECASE)
_DATA_URI_RE = re.compile(r"^data:([\w.+-]+/[\w.+-]+);base64,(.+)$", re.DOTALL | re.IGNORECASE)
# 裸 base64 最短判定长度：短字符串（如 "ok"、id）也能通过 b64 字符集校验，
# 没有长度下限会把普通字段误判成图片字节。
_MIN_B64_LEN = 512

_KIND_IMAGE = "image"
_KIND_VIDEO = "video"
_KIND_AUDIO = "audio"
_KIND_FILE = "file"

_EXT_MIME: dict[str, str] = {
    "png": "image/png",
    "jpg": "image/jpeg",
    "jpeg": "image/jpeg",
    "webp": "image/webp",
    "gif": "image/gif",
    "bmp": "image/bmp",
    "svg": "image/svg+xml",
    "mp4": "video/mp4",
    "webm": "video/webm",
    "mov": "video/quicktime",
    "mp3": "audio/mpeg",
    "wav": "audio/wav",
    "flac": "audio/flac",
    "ogg": "audio/ogg",
    "m4a": "audio/mp4",
    "aac": "audio/aac",
    "pdf": "application/pdf",
    "zip": "application/zip",
    "glb": "model/gltf-binary",
    "gltf": "model/gltf+json",
    "obj": "model/obj",
    "fbx": "application/octet-stream",
}

_MIME_KIND_DEFAULT: dict[str, str] = {
    _KIND_IMAGE: "image/png",
    _KIND_VIDEO: "video/mp4",
    _KIND_AUDIO: "audio/mpeg",
    _KIND_FILE: "application/octet-stream",
}

# 键名 → 媒介类型。按「更具体优先」匹配，故用有序元组而非 dict。
_KEY_HINTS: tuple[tuple[str, str], ...] = (
    ("image", _KIND_IMAGE),
    ("img", _KIND_IMAGE),
    ("thumbnail", _KIND_IMAGE),
    ("cover", _KIND_IMAGE),
    ("last_frame", _KIND_IMAGE),
    ("first_frame", _KIND_IMAGE),
    ("picture", _KIND_IMAGE),
    ("photo", _KIND_IMAGE),
    ("video", _KIND_VIDEO),
    ("movie", _KIND_VIDEO),
    ("audio", _KIND_AUDIO),
    ("voice", _KIND_AUDIO),
    ("speech", _KIND_AUDIO),
    ("music", _KIND_AUDIO),
    ("song", _KIND_AUDIO),
    ("sound", _KIND_AUDIO),
    ("tts", _KIND_AUDIO),
    ("model", _KIND_FILE),
    ("mesh", _KIND_FILE),
    ("3d", _KIND_FILE),
    ("file", _KIND_FILE),
    ("attachment", _KIND_FILE),
    ("document", _KIND_FILE),
)

# 只在这些键上继续下钻（容器键），避免把请求回显、日志、元数据也扫成制品。
_CONTAINER_KEYS = frozenset(
    {
        "data",
        "result",
        "results",
        "output",
        "outputs",
        "content",
        "contents",
        "response",
        "artifacts",
        "files",
        "items",
        "parts",
        "list",
        "choices",
        "media",
        "assets",
        "task_result",
        "generations",
    }
)

# 明确排除：这些子树是请求侧回显，含用户传入的 image_url（输入图）。
# 把输入图当产出制品是最常见的误报源。
_INPUT_KEYS = frozenset(
    {
        "request",
        "input",
        "inputs",
        "parameters",
        "params",
        "payload",
        "prompt",
        "messages",
        "body",
        "config",
        "options",
        "usage",
        "metadata",
        "meta",
    }
)


@dataclass(slots=True)
class Artifact:
    """单个制品。``url`` 与 ``inline_b64`` 互斥，至少一个非空。"""

    key: str
    type: str
    mime_type: str
    url: str = ""
    inline_b64: str = ""
    credentialless: bool = True

    @property
    def inline(self) -> bool:
        return not self.url and bool(self.inline_b64)

    def to_dict(self) -> dict[str, Any]:
        """落库/响应形态。``inline_b64`` 不落库——体量大且已在
        ``upstream_response`` 里存过一份，只标记 ``inline``。"""
        out: dict[str, Any] = {"key": self.key, "type": self.type}
        if self.mime_type:
            out["mime_type"] = self.mime_type
        if self.url:
            out["url"] = self.url
        if self.inline_b64:
            out["inline"] = True
        return out


@dataclass(slots=True)
class _Collector:
    """收集期状态：按 URL/字节去重，按类型分配稳定序号。"""

    seen: set[str] = field(default_factory=set)
    counters: dict[str, int] = field(default_factory=dict)
    items: list[Artifact] = field(default_factory=list)
    #: 命中级别（``known`` / ``walk`` / ``inline`` / ``none``）。落库进
    #: ``data.artifact_parser``：线上出现"该有制品却是空"时，这一个字段
    #: 就能区分"三级全空"与"第一级误命中了错字段"，不必回捞原始响应。
    tier: str = "none"

    def add(self, kind: str, *, url: str = "", inline_b64: str = "", mime: str = "") -> None:
        if len(self.items) >= _MAX_ARTIFACTS:
            return
        fingerprint = url or inline_b64[:128]
        if not fingerprint or fingerprint in self.seen:
            return
        self.seen.add(fingerprint)
        n = self.counters.get(kind, 0) + 1
        self.counters[kind] = n
        self.items.append(
            Artifact(
                key=kind if n == 1 else f"{kind}_{n}",
                type=kind,
                mime_type=mime or _mime_from_url(url) or _MIME_KIND_DEFAULT[kind],
                url=url,
                inline_b64=inline_b64,
            )
        )

    @property
    def found(self) -> bool:
        return bool(self.items)


def _valid_url(value: Any) -> str:
    return value.strip() if isinstance(value, str) and _URL_RE.match(value.strip()) else ""


def _mime_from_url(url: str) -> str:
    """靠扩展名推断 MIME。预签名 URL 带一大串 query，先截掉 ``?``/``#``。"""
    if not url:
        return ""
    path = url.split("?", 1)[0].split("#", 1)[0]
    dot = path.rfind(".")
    return _EXT_MIME.get(path[dot + 1 :].lower(), "") if dot >= 0 else ""


def _kind_from_url(url: str, fallback: str = _KIND_FILE) -> str:
    mime = _mime_from_url(url)
    for kind in (_KIND_IMAGE, _KIND_VIDEO, _KIND_AUDIO):
        if mime.startswith(f"{kind}/"):
            return kind
    return fallback


def _kind_from_key(key: str) -> str:
    """键名 → 媒介类型。``url``/``*_url`` 这类无语义键归 file（后续按扩展名细化）。"""
    name = key.strip().lower()
    if not name:
        return ""
    for token, kind in _KEY_HINTS:
        if token in name:
            return kind
    if name in {"url", "uri", "link", "download_url", "content_url"} or name.endswith(
        ("_url", "_uri", "_link")
    ):
        return _KIND_FILE
    return ""


# ── 第一级：已知厂商精确路径 ────────────────────────────────────────────
# (容器路径, 字段名, 媒介类型)；类型留空 = 按 URL 扩展名自动判定。
_KNOWN_PATHS: tuple[tuple[tuple[str, ...], str, str], ...] = (
    # Ark / OpenAI 图片：{"data":[{"url":...}]}
    ((), "url", ""),
    (("data",), "url", ""),
    (("data",), "image_url", _KIND_IMAGE),
    (("data",), "video_url", _KIND_VIDEO),
    (("data",), "audio_url", _KIND_AUDIO),
    # Ark 视频（Seedance）：{"content":{"video_url":..., "last_frame_url":...}}
    (("content",), "video_url", _KIND_VIDEO),
    (("content",), "last_frame_url", _KIND_IMAGE),
    (("content",), "audio_url", _KIND_AUDIO),
    # 根级扁平字段（多数国产厂商）
    ((), "image_url", _KIND_IMAGE),
    ((), "video_url", _KIND_VIDEO),
    ((), "audio_url", _KIND_AUDIO),
    ((), "file_url", _KIND_FILE),
    # fal / replicate 风格
    (("images",), "url", _KIND_IMAGE),
    (("image",), "url", _KIND_IMAGE),
    (("video",), "url", _KIND_VIDEO),
    (("audio",), "url", _KIND_AUDIO),
    (("output",), "url", ""),
    (("outputs",), "url", ""),
    # Kling
    (("task_result", "images"), "url", _KIND_IMAGE),
    (("task_result", "videos"), "url", _KIND_VIDEO),
    # MiniMax
    (("file",), "download_url", ""),
    (("files",), "download_url", ""),
    # 通用 result 容器
    (("result",), "url", ""),
    (("results",), "url", ""),
    (("artifacts",), "url", ""),
)


def _envelopes(root: Any) -> list[dict[str, Any]]:
    """候选 envelope，按「越具体越先试」排列。

    上游成功体可能是裸结果，也可能被包一层 ``data``/``response``——
    宿主 new-api 插件的 ``persistedEnvelope`` 也是同样的退化思路。
    注意 ``data`` 为 list 时（Ark 图片）不能当 envelope，仍要留在父级由
    ``_KNOWN_PATHS`` 的 ``("data",)`` 路径消费。
    """
    if not isinstance(root, dict):
        return []
    wrappers = ("response", "result", "data", "output", "body")
    out: list[dict[str, Any]] = [root]
    frontier = [root]
    # 逐层剥壳（最多 3 层）：真实上游常见 data.response.data 这类三重包装，
    # 只展开一层会漏。深度设上限避免病态深嵌套拖慢解析。
    for _ in range(3):
        nxt: list[dict[str, Any]] = []
        for node in frontier:
            for key in wrappers:
                child = node.get(key)
                if isinstance(child, dict) and not any(child is seen for seen in out):
                    out.append(child)
                    nxt.append(child)
        if not nxt:
            break
        frontier = nxt
    return out


def _walk_path(node: Any, path: tuple[str, ...]) -> list[dict[str, Any]]:
    """按路径下钻，遇 list 自动展开，返回末端所有 dict 节点。"""
    current: list[Any] = [node]
    for key in path:
        nxt: list[Any] = []
        for item in current:
            if isinstance(item, dict):
                child = item.get(key)
                nxt.extend(child if isinstance(child, list) else [child])
        current = nxt
    out: list[dict[str, Any]] = []
    for item in current:
        if isinstance(item, list):
            out.extend(x for x in item if isinstance(x, dict))
        elif isinstance(item, dict):
            out.append(item)
    return out


def _tier_known(root: Any, col: _Collector) -> None:
    for env in _envelopes(root):
        for path, field_name, kind in _KNOWN_PATHS:
            for node in _walk_path(env, path):
                url = _valid_url(node.get(field_name))
                if url:
                    # 扩展名优先、声明兜底：与第二级判定规则保持一致。
                    # 厂商把视频挂在 image_url 上并不罕见（图生视频接口复用
                    # 了图片响应体），此时 .mp4 比字段名更可信；URL 无扩展名
                    # 时才回落到路径表声明的类型。
                    col.add(_kind_from_url(url, kind or _KIND_FILE), url=url)
        if col.found:
            return


# ── 第二级：键名递归 ────────────────────────────────────────────────────
def _tier_walk(root: Any, col: _Collector) -> None:
    """全树遍历，靠键名判定类型。深度上限 + 环检测 + 输入子树排除。"""
    visited: set[int] = set()

    def walk(node: Any, hint: str, depth: int) -> None:
        if depth > _MAX_DEPTH or len(col.items) >= _MAX_ARTIFACTS:
            return
        if isinstance(node, str):
            url = _valid_url(node)
            if url:
                # 扩展名优先（最可靠），认不出来再回落键名提示
                hinted = _kind_from_key(hint)
                col.add(_kind_from_url(url, hinted or _KIND_FILE), url=url)
            return
        if isinstance(node, list):
            if id(node) in visited:
                return
            visited.add(id(node))
            for item in node:
                walk(item, hint, depth + 1)
            return
        if not isinstance(node, dict) or id(node) in visited:
            return
        visited.add(id(node))
        for key, child in node.items():
            name = key.strip().lower()
            if name in _INPUT_KEYS:
                continue
            kind = _kind_from_key(key)
            if kind:
                walk(child, key, depth + 1)
            elif name in _CONTAINER_KEYS or isinstance(child, list | dict):
                walk(child, hint, depth + 1)

    walk(root, "", 0)


# ── 第三级：内联字节 ────────────────────────────────────────────────────
def _looks_base64(value: str, *, min_len: int = _MIN_B64_LEN) -> bool:
    """是否像 base64 字节流。

    ``min_len`` 由调用方按键名可信度给：``b64_json`` 这类显式键名本身即是
    强证据，用长度门槛反而会误杀小图（几十字节的占位图/1x1 png）；键名不
    带线索时才需要长度兜底，否则任意 id、hash 都会被当成图片字节。
    """
    if len(value) < min_len:
        return False
    try:
        base64.b64decode(value, validate=True)
    except (binascii.Error, ValueError):
        return False
    return True


def _tier_inline(root: Any, col: _Collector) -> None:
    """``b64_json`` / ``data:`` URI / 裸 base64 —— 无可回源 URL 时的最后兜底。"""
    visited: set[int] = set()

    def walk(node: Any, hint: str, depth: int) -> None:
        if depth > _MAX_DEPTH or len(col.items) >= _MAX_ARTIFACTS:
            return
        if isinstance(node, str):
            matched = _DATA_URI_RE.match(node.strip())
            if matched:
                mime = matched.group(1).lower()
                col.add(_kind_from_mime(mime), inline_b64=matched.group(2), mime=mime)
                return
            name = hint.strip().lower()
            explicit = "b64" in name or "base64" in name
            if explicit or name in {"image", "audio", "video"}:
                # 显式 b64 键名免长度门槛；靠 image/audio/video 这种弱线索
                # 命中的仍要够长，否则 {"image": "none"} 之类会被误收。
                text = node.strip()
                if _looks_base64(text, min_len=1 if explicit else _MIN_B64_LEN):
                    col.add(_kind_from_key(hint) or _KIND_IMAGE, inline_b64=text)
            return
        if isinstance(node, list):
            if id(node) in visited:
                return
            visited.add(id(node))
            for item in node:
                walk(item, hint, depth + 1)
            return
        if not isinstance(node, dict) or id(node) in visited:
            return
        visited.add(id(node))
        for key, child in node.items():
            if key.strip().lower() in _INPUT_KEYS:
                continue
            walk(child, key, depth + 1)

    walk(root, "", 0)


def _kind_from_mime(mime: str) -> str:
    for kind in (_KIND_IMAGE, _KIND_VIDEO, _KIND_AUDIO):
        if mime.startswith(f"{kind}/"):
            return kind
    return _KIND_FILE


def parse(payload: Any) -> list[Artifact]:
    """上游成功响应 → 制品清单（只要清单，忽略命中级别）。"""
    return parse_result(payload).items


@dataclass(slots=True)
class ParseResult:
    """解析产出：制品清单 + 命中级别（可观测性用）。"""

    items: list[Artifact]
    tier: str

    @property
    def found(self) -> bool:
        return bool(self.items)


def parse_result(payload: Any) -> ParseResult:
    """上游成功响应 → 制品清单。空清单是合法结果（纯文本任务无制品）。

    三级严格短路：精确路径命中就不再递归，避免第二级把同一 URL 的缩略图、
    水印图等次要字段一并扫进来，污染 ``key`` 序号的稳定性。
    """
    if isinstance(payload, bytes | bytearray):
        payload = _loads(bytes(payload))
    elif isinstance(payload, str):
        payload = _loads(payload.encode("utf-8"))
    if payload is None:
        return ParseResult(items=[], tier="none")

    col = _Collector()
    try:
        _tier_known(payload, col)
        if col.found:
            col.tier = "known"
        else:
            _tier_walk(payload, col)
            if col.found:
                col.tier = "walk"
            else:
                _tier_inline(payload, col)
                if col.found:
                    col.tier = "inline"
    except Exception:
        # 制品解析是**增强**而非任务成败的判据：上游 2xx 就是成功。
        # 畸形/超预期响应导致解析崩溃时，宁可交回已收集的部分（可能为空），
        # 绝不能把异常抛给 _settle_response 让成功任务变 FAILURE。
        log.opt(exception=True).warning(
            "artifact parse failed, degrading to partial result: collected={}",
            len(col.items),
        )
        if col.found and col.tier == "none":
            col.tier = "partial"
    return ParseResult(items=col.items, tier=col.tier)


def _loads(raw: bytes) -> Any:
    try:
        return json.loads(raw)
    except (ValueError, UnicodeDecodeError):
        return None


def parse_to_dicts(payload: Any) -> list[dict[str, Any]]:
    """``parse`` 的落库形态（供 ``data.artifacts`` 直接写入）。"""
    return [a.to_dict() for a in parse(payload)]


def parse_for_store(payload: Any) -> dict[str, Any]:
    """一次解析产出全部落库字段（``_settle_response`` 的唯一入口）。

    合成 ``data`` 的四个键，一次解析全部算完——避免落库侧为了拿
    URL/计数分别再调一次 ``parse``（同一响应解析三遍，大响应体白烧 CPU）。

    - ``artifacts``：清单（``JSON_MERGE_PATCH`` 里数组是整体覆盖，安全）
    - ``result_url``：主产出 URL，宿主看板「结果」列直读
    - ``artifact_count``：计数，看板列表页不必反序列化清单即可判有无
    - ``artifact_parser``：命中级别，排查"该有制品却为空"时的唯一线索
    """
    result = parse_result(payload)
    return {
        "artifacts": [a.to_dict() for a in result.items],
        "result_url": primary_url(result.items),
        "artifact_count": len(result.items),
        "artifact_parser": result.tier,
    }


def primary_url(items: list[Artifact]) -> str:
    """首个可回源 URL —— 宿主 new-api 看板的「结果」列读这个值。

    优先 video/image（用户真正想看的产出），再退到任意带 URL 的制品。
    """
    for kind in (_KIND_VIDEO, _KIND_IMAGE, _KIND_AUDIO):
        for item in items:
            if item.type == kind and item.url:
                return item.url
    for item in items:
        if item.url:
            return item.url
    return ""
