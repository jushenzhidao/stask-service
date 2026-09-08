"""准入校验：路径白名单、upstream 寻址与三防线、task_id 形态、请求头清洗。

这是本服务**唯一的攻击面收口点**——提交端点是通配路由（``/async/{path:path}``），
携带的是用户真实 sk，一旦把 sk 打到野地址就是凭证泄露。设计 §7 的三防线
在这里落地：

1. nginx ``proxy_set_header`` 无条件覆盖客户端同名头（部署侧，见 deploy/nginx.conf）；
2. host 必须命中 ``UPSTREAM_ALLOWLIST``（本模块 ``resolve_upstream``）；
3. 仅 http(s)、拒绝 URL userinfo（同上）。

第 1 道在 nginx，但**不能只靠它**：直连 8000 端口的流量绕过 nginx，
第 2/3 道必须在应用内独立成立。
"""

from __future__ import annotations

import re
from urllib.parse import urlsplit

from app.config import settings

#: task_id 形态：{model_slug}_{uuid4hex}。查询/取消端点从 path 末段取
#: 候选串时先用它预筛——避免把 ``/async/v1/images/generations`` 这种
#: 纯路径误当成 task_id 去查库。
TASK_ID_RE = re.compile(r"^[a-z0-9_]{1,20}_[0-9a-f]{32}$")

#: 提交时必须剔除的请求头（逐跳头 + 会被 httpx 重算的头 + 凭证）。
#: Authorization 单独处理：它不落库，worker 从令牌会话取 sk 重新组装。
_DROP_HEADERS = frozenset({
    "host", "content-length", "connection", "keep-alive", "transfer-encoding",
    "upgrade", "proxy-authenticate", "proxy-authorization", "te", "trailer",
    "authorization", "cookie", "accept-encoding",
    # 本服务自有的控制头，不应转发给上游
    "idempotency-key", "x-callback-url", "x-upstream-base-url",
    # nginx / 代理链注入的头，转发无意义
    "x-forwarded-for", "x-forwarded-proto", "x-forwarded-host", "x-real-ip",
})


class AdmissionError(Exception):
    """准入失败。``status`` 直接作为 HTTP 状态码抛给客户端。"""

    def __init__(self, status: int, message: str, code: str = "") -> None:
        super().__init__(message)
        self.status = status
        self.message = message
        self.code = code


def check_path(path: str) -> None:
    """路径准入：deny 优先于 allow（AC-02/AC-03）。

    ``path`` 是**剥掉 /async 前缀后**的上游路径，恒以 ``/`` 开头。
    deny 优先的原因：allow 前缀写宽了（比如有人配 ``/``）时，deny 仍能
    挡住管理面。两者顺序颠倒等于给了配置失误一条直达 ``/api/user/self``
    的路。
    """
    for deny in settings.async_deny_prefixes:
        if deny and path.startswith(deny):
            raise AdmissionError(403, f"path not allowed: {deny}", "path_denied")
    allows = settings.async_allow_prefixes
    if not allows:
        raise AdmissionError(403, "no async path configured", "path_not_allowed")
    for allow in allows:
        if allow and path.startswith(allow):
            return
    raise AdmissionError(403, "path not in allowlist", "path_not_allowed")


def _host_matches(host: str) -> bool:
    """allowlist 比对：条目含端口则按 ``host:port`` 全等，不含端口只比 host。

    **空 = 不限制**（与 ``CALLBACK_ALLOWLIST`` 同一套语义）：不配即放行，
    配了就按条目卡。启动时会打 warning 提醒——放行意味着 ``X-Upstream-Base-Url``
    头能决定用户令牌往哪儿发，只在前面有 nginx 无条件覆盖该头时才安全。

    不做通配/后缀匹配——``*.example.com`` 这类规则一旦写错就是开放重定向。
    要加就显式列全。
    """
    if not settings.upstream_allowlist:
        return True
    hostname = host.split(":")[0]
    for entry in settings.upstream_allowlist:
        item = entry.strip().lower()
        if not item:
            continue
        if ":" in item:
            if host == item:
                return True
        elif hostname == item:
            return True
    return False


def resolve_upstream(header_value: str | None) -> str:
    """确定并校验 upstream base url（防线 2 + 防线 3）。

    优先级：``X-Upstream-Base-Url`` 头（nginx 注入）> ``UPSTREAM_BASE_URL``。
    返回值是**规整后**的 base（去尾斜杠），提交时随任务落库，worker 只认
    落库值——中途改配置不影响在途任务的目标地址。
    """
    raw = (header_value or "").strip() or settings.upstream_base_url
    parts = urlsplit(raw)

    if parts.scheme not in ("http", "https"):
        raise AdmissionError(400, "upstream scheme must be http or https",
                             "upstream_invalid_scheme")
    # userinfo（user:pass@host）会让 host 解析产生歧义，也是 SSRF 常用绕过手法
    if parts.username or parts.password or "@" in parts.netloc:
        raise AdmissionError(400, "upstream must not contain userinfo",
                             "upstream_userinfo")
    if not parts.hostname:
        raise AdmissionError(400, "upstream host missing", "upstream_invalid")
    if parts.query or parts.fragment:
        raise AdmissionError(400, "upstream must not contain query or fragment",
                             "upstream_invalid")

    host = parts.netloc.lower()
    if not _host_matches(host):
        raise AdmissionError(400, f"upstream host not in allowlist: {host}",
                             "upstream_not_allowed")

    return f"{parts.scheme}://{host}{parts.path.rstrip('/')}"


def clean_headers(headers: dict[str, str]) -> dict[str, str]:
    """请求头清洗：剔除逐跳头、凭证与控制头，其余原样保留转发。

    保留 ``content-type``、``accept``、以及上游可能识别的自定义头
    （比如渠道侧的 ``x-request-id``）——设计要求「原文存储原样转发」。
    """
    return {
        k: v for k, v in headers.items()
        if k.lower() not in _DROP_HEADERS
    }


def extract_task_id(path: str) -> str | None:
    """从查询/取消请求的路径末段提取 task_id（形态预筛，AC-19）。"""
    segment = path.rstrip("/").rsplit("/", 1)[-1]
    return segment if TASK_ID_RE.match(segment) else None
