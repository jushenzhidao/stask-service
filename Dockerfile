# ---- 构建阶段：只为「目标平台没有 wheel」的依赖准备编译工具链 -----------------
#
# 事实（2026-09-13 实测）：`asyncmy` 在 **linux/aarch64 上没有 wheel**
# （0.2.10 / 0.2.11 / 0.2.12 都没有，0.2.9 与 0.2.13+ 才有），而它是构建期从
# sdist 就地编译的 Cython 扩展。`python:3.12-slim` 不带编译器，于是 arm64 构建
# 直接失败：`Failed building wheel for asyncmy`（本机原生复现过）。
#
# amd64 走 wheel，这一步不会真的编译（只多装几个 apt 包）；arm64 才会编译。
# 编译工具链**只留在本阶段**，运行镜像里一个字节都没有。
FROM python:3.12-slim AS builder

RUN apt-get update \
    && apt-get install -y --no-install-recommends gcc python3-dev libc6-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /srv
# 依赖钉版唯一处是 pyproject.toml（SPEC §4 版本纪律，不用 requirements.txt）
COPY . .
# 装到独立前缀，便于整目录拷进运行阶段
RUN pip install --no-cache-dir --prefix=/install .

# ---- 运行阶段 ---------------------------------------------------------------
FROM python:3.12-slim

# 生产镜像**默认声明 prod**：`app/main.py` 的致命启动校验（CHANNEL_ID /
# CALLBACK_SECRET / taskiq-admin token）只在严格环境生效，而镜像本身就是生产产物。
# 不写这一行的话，容器的 APP_ENV 会落到配置默认值 `dev`——即「校验全部静默跳过」，
# 恰是这些校验要防的场景。要跑本地/预发，运行时显式覆盖即可
# （compose 的 env_file，或 docker run -e APP_ENV=dev）。
ENV APP_ENV=prod

WORKDIR /srv
# 先拷已装好的依赖：/usr/local 下的 site-packages 与 bin 对官方 python 镜像即在 PATH 上
COPY --from=builder /install /usr/local
# 再拷源码：gunicorn.conf.py 必须在工作目录里（`.dockerignore` 已排除
# tests/ docs/ .env，不会把测试与本地配置带进镜像）
COPY . .

EXPOSE 8000

CMD ["gunicorn", "-c", "gunicorn.conf.py", "app.main:app"]
