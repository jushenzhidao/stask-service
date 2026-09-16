"""把服务版本写进两处「事实源」文件。

存在理由：``app/__init__.py`` 的 ``__version__`` 是全仓唯一手写的版本号，而
``tests/test_spec_contract.py`` 要求 ``.env.example`` 的 ``APP_VERSION`` 与它
逐字一致。于是「push main 即自增 patch」这件事不可能只打一个 tag —— tag 与代码
一旦漂移，``/healthz`` 与 OpenAPI 的 ``version`` 就会报旧值（而守卫还会在下一次
改动时变红）。

CI 的发版 job 在**构建镜像之前**调用本脚本，把版本落到文件里、随提交推回主干，
所以镜像内自报的版本与 tag 是同一个数。

用法::

    python scripts/bump_version.py 0.4.1          # 改写两处
    python scripts/bump_version.py 0.4.1 --check  # 只校验（两处都已是该版本才 0）

两条纪律：命中数必须恰好 1（命中 0 说明契约写法变了，命中多处说明冒出了第二处
手写版本号，两种都当场报错而不是猜）；不在 ``.env`` 上做任何事 —— 它是
gitignored 的本地文件，由使用方按镜像 tag 自行同步。
"""

from __future__ import annotations

import argparse
import pathlib
import re
import sys

_REPO = pathlib.Path(__file__).resolve().parents[1]

#: (相对路径, 匹配正则, 替换模板, 人读标签)
_TARGETS: tuple[tuple[str, str, str, str], ...] = (
    (
        "app/__init__.py",
        r'^__version__ = "[^"]+"',
        '__version__ = "{version}"',
        "app/__init__.py 的 __version__",
    ),
    (
        ".env.example",
        r"^APP_VERSION=\S+",
        "APP_VERSION={version}",
        ".env.example 的 APP_VERSION",
    ),
)

_SEMVER = re.compile(r"^\d+\.\d+\.\d+$")


def bump(version: str, *, check: bool = False) -> list[str]:
    """把 ``version`` 写进各目标文件，返回「与目标版本不符」的文件列表。

    ``check=True`` 时不落盘，只回答「哪些文件还不是这个版本」——空列表即通过。
    """
    if not _SEMVER.match(version):
        raise SystemExit(f"版本必须形如 X.Y.Z（不带 v 前缀），收到 {version!r}")

    changed: list[str] = []
    for rel, pattern, template, label in _TARGETS:
        path = _REPO / rel
        if not path.is_file():
            raise SystemExit(f"{rel} 不存在 —— 仓库结构变了？")
        text = path.read_text()
        new, hits = re.subn(
            pattern, template.format(version=version), text, count=1, flags=re.M
        )
        if hits != 1:
            raise SystemExit(f"{label}：期望命中 1 处，实际 {hits} 处 —— 契约变了？")
        if new == text:
            continue
        if not check:
            path.write_text(new)
        changed.append(rel)
    return changed


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="把版本写进 __version__ 与 .env.example（两处必须一致）"
    )
    parser.add_argument("version", help="形如 0.4.1，不带 v 前缀")
    parser.add_argument("--check", action="store_true", help="只校验，不写文件")
    args = parser.parse_args(argv)

    changed = bump(args.version, check=args.check)
    if args.check:
        if changed:
            raise SystemExit(f"以下文件还不是 {args.version}：{', '.join(changed)}")
        print(f"OK：两处版本均已是 {args.version}")
        return 0
    print(f"版本已写为 {args.version}：" + (", ".join(changed) or "（本来就一致，无变化）"))
    return 0


if __name__ == "__main__":
    sys.exit(main())
