FROM python:3.12-slim

WORKDIR /srv

# 生产镜像**默认声明 prod**：`app/main.py` 的致命启动校验（CHANNEL_ID /
# CALLBACK_SECRET / taskiq-admin token）只在严格环境生效，而镜像本身就是生产产物。
# 不写这一行的话，容器的 APP_ENV 会落到配置默认值 `dev`——即「校验全部静默跳过」，
# 恰是这些校验要防的场景。要跑本地/预发，运行时显式覆盖即可
# （compose 的 env_file，或 docker run -e APP_ENV=dev）。
ENV APP_ENV=prod

# 依赖钉版唯一处是 pyproject.toml（SPEC §4 版本纪律，不用 requirements.txt）
COPY . .
RUN pip install --no-cache-dir .

EXPOSE 8000

CMD ["gunicorn", "-c", "gunicorn.conf.py", "app.main:app"]
