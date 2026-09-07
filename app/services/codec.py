"""响应体编解码：gzip + base64（设计 §9）。

为什么 b64 而不是直接存 bytes：``data`` 是 MySQL JSON 列，只能存合法
UTF-8 字符串。gzip 后的字节流不是合法 UTF-8，必须再套一层 b64。

体量账：出图接口的典型响应是 JSON 包 URL（几百字节）或 b64 图片
（1~3MB）。JSON 型 gzip 压缩率 70%+；b64 图片本身已是压缩后的 PNG/JPEG
再编码，gzip 收益小但也不会膨胀（gzip 对不可压缩数据的开销约 0.03%）。
b64 会让体积 ×4/3，所以 ``ST_RESPONSE_MAX_BYTES``（10MB）判定的是**原始
字节数**，落库实际占用约 gzip 后 ×4/3。
"""

from __future__ import annotations

import base64
import gzip


def encode(raw: bytes) -> str:
    """原始响应字节 → gzip → base64 字符串（可直接进 JSON 列）。

    ``mtime=0``：gzip header 默认嵌当前时间戳，同一输入跨秒两次 encode
    会产出不同字节串——既让输出不可复现（测试 flaky），也没有任何用处。
    """
    return base64.b64encode(gzip.compress(raw, compresslevel=6, mtime=0)).decode("ascii")


def decode(encoded: str) -> bytes:
    """``encode`` 的逆操作。非法输入抛 ValueError，由调用方转 500/410。"""
    try:
        return gzip.decompress(base64.b64decode(encoded))
    except Exception as exc:                     # 统一收敛为 ValueError，调用方只处理一种
        raise ValueError(f"corrupted stored response: {type(exc).__name__}") from exc
