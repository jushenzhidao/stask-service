"""请求体 / 响应体的落库编解码：**明文优先，超阈值才 gzip+base64**。

## 为什么默认存明文

``data`` 是 MySQL 的 JSON 列，人和工具都会直接读它：

    SELECT data ->> '$.upstream_response' FROM tasks WHERE task_id = '...';

旧实现无条件 gzip+base64，上面这条查询回的是一串 ``H4sIAAAA...``——
排障时必须把它拷进 Python 里解一次才能看内容，看板也只能显示字节数。
绝大多数任务的响应体是几百字节到几 KB 的 JSON（URL + 元数据），
压缩省下的空间还不如一次人工解码的时间值钱。

所以规则反过来：

| 条件 | 存储形态 | ``encoding`` |
|---|---|---|
| 原始字节 ≤ 阈值 且 是合法 UTF-8 | **明文原样** | ``"plain"`` |
| 超阈值 或 含非 UTF-8 字节（二进制） | gzip → base64 | ``"gzip+b64"`` |

阈值 = ``PLAIN_MAX_BYTES``（默认 32 KB，已登记进 dynconf 白名单，
可在管理页热改）。二进制无条件走 gzip 分支——JSON 列只能存合法
UTF-8，音频/图片字节直接塞进去会被 MySQL 拒绝。

## 编码标记是**显式列**而非猜测

每个体都配一个 ``*_encoding`` 兄弟字段（``request_body_encoding`` /
``upstream_response_encoding``）。不靠"试着 b64 解一下，失败就当明文"
——那种嗅探对恰好是合法 base64 的明文 JSON 会判错，属于必然踩的坑。

无兼容层（本轮明确不兼容旧版本）：只有 ``plain`` / ``gzip+b64`` / 空串
三种取值合法，其余一律抛错。旧的 gzip 行标记为空 → 会按明文读出一串
base64，这是有意的：升级即换代，宁可看到明显错误的内容，也不要一个
"看起来像在工作"的兼容层。

## 体量账

``RESPONSE_MAX_BYTES``（10MB）判定的恒是**原始字节数**，与存储形态无关。
gzip 分支落库实际占用 ≈ gzip 后 ×4/3（base64 膨胀）；明文分支就是原样。
"""

from __future__ import annotations

import base64
import gzip

#: 编码标记的两个合法取值（落进 ``data.*_encoding``）
PLAIN = "plain"
GZIP_B64 = "gzip+b64"

#: 与 ``Settings.plain_max_bytes`` 同值。**仅供测试与独立脚本**使用——
#: 业务代码一律从运行时配置快照取（阈值可在管理页热改，读常量会拿到旧值）。
PLAIN_MAX_DEFAULT = 32 * 1024


def encode(raw: bytes, plain_max_bytes: int) -> tuple[str, str]:
    """原始字节 → ``(stored, encoding)``。

    ``plain_max_bytes`` 由调用方从运行时配置快照传入——本模块不读配置，
    保持纯函数（测试里可以逐个阈值断言，不必构造 Redis）。

    ``mtime=0``：gzip header 默认嵌当前时间戳，同一输入跨秒两次 encode
    会产出不同字节串——既让输出不可复现（测试 flaky），也没有任何用处。
    """
    if len(raw) <= plain_max_bytes:
        try:
            return raw.decode("utf-8"), PLAIN
        except UnicodeDecodeError:
            pass                      # 二进制体：JSON 列存不了，落 gzip 分支
    return base64.b64encode(gzip.compress(raw, compresslevel=6, mtime=0)).decode("ascii"), GZIP_B64


def decode(stored: str, encoding: str) -> bytes:
    """``encode`` 的逆操作。非法输入抛 ValueError，由调用方转 500/410。

    - ``plain`` 或**空串** → 原样 UTF-8 编码回字节。空串的容忍面很窄：
      只为手工插入/外部写入的行，不是为了兼容旧版本（本轮明确不兼容）；
    - ``gzip+b64`` → base64 解码后解压；
    - **其他任何值 → 抛错**。绝不"不认识就当明文"：真按明文回放，客户端
      拿到的会是一串 base64 而不是它要的图片/JSON，且没有任何报错提示——
      这类静默污染比当场 500 难查一个量级。
    """
    if encoding == GZIP_B64:
        try:
            return gzip.decompress(base64.b64decode(stored))
        except Exception as exc:      # 统一收敛为 ValueError，调用方只处理一种
            raise ValueError(f"corrupted stored response: {type(exc).__name__}") from exc
    if encoding in (PLAIN, ""):
        return stored.encode("utf-8")
    raise ValueError(f"unknown stored encoding: {encoding!r}")
