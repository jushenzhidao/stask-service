.DEFAULT_GOAL := help
PY := .venv/bin/python
SHELL := /bin/bash

.PHONY: help setup check test lint type run worker scheduler standalone \
        up down logs build clean bench admin-key

help:  ## 显示可用命令
	@grep -hE '^[a-z-]+:.*?## ' $(MAKEFILE_LIST) \
		| awk -F':.*?## ' '{printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

setup:  ## 建虚拟环境并安装依赖（含开发依赖）
	@command -v uv >/dev/null 2>&1 || { echo "需要 uv：curl -LsSf https://astral.sh/uv/install.sh | sh"; exit 1; }
	uv venv --python 3.12 .venv
	uv pip install -e ".[dev]" --python $(PY)
	@test -f .env || { cp .env.example .env && echo "已生成 .env，请检查 SQL_DSN 与 REDIS_URL"; }

check: lint type test  ## 三项门禁全跑（提交前必须绿）

lint:  ## ruff 静态检查
	$(PY) -m ruff check app tests scripts gunicorn.conf.py

type:  ## mypy 类型检查
	$(PY) -m mypy app/

test:  ## 运行测试套件（不依赖 MySQL/Redis）
	$(PY) -m pytest tests/ -q

fix:  ## ruff 自动修复
	$(PY) -m ruff check app tests scripts --fix

run:  ## 本地起 web（需另开 worker）
	$(PY) -m gunicorn -c gunicorn.conf.py app.main:app

worker:  ## 本地起 worker
	.venv/bin/taskiq worker app.queue:broker --max-async-tasks 64

scheduler:  ## 本地起 scheduler（必须单副本）
	.venv/bin/taskiq scheduler app.queue:scheduler

standalone:  ## 本地单进程起全套（web + worker + scheduler）
	$(PY) -m app.standalone

up:  ## compose 起全套（web + worker + redis）
	docker compose up -d --build
	@echo "等待就绪..." && sleep 3
	@curl -sf http://127.0.0.1:8000/healthz/ready | head -c 400 || echo "尚未就绪，看 make logs"

down:  ## 停止并移除容器
	docker compose down

logs:  ## 跟随日志
	docker compose logs -f --tail=100

build:  ## 构建镜像
	docker build -t stask-service:local .

admin-key:  ## 生成一个管理密钥（写入 .env 的 ADMIN_KEY）
	@key=$$($(PY) -c "import secrets;print(secrets.token_urlsafe(32))"); \
	if grep -q '^ADMIN_KEY=' .env 2>/dev/null; then \
		sed -i.bak "s|^ADMIN_KEY=.*|ADMIN_KEY=$$key|" .env && rm -f .env.bak; \
	else echo "ADMIN_KEY=$$key" >> .env; fi; \
	echo "ADMIN_KEY=$$key"; echo "看板：http://127.0.0.1:8000/admin"

bench:  ## 压测提交链路（需 TOKEN=sk-xxx）
	@test -n "$(TOKEN)" || { echo "用法：make bench TOKEN=sk-xxx"; exit 1; }
	$(PY) scripts/bench_submit.py --token $(TOKEN) --concurrency 50 --requests 1000

clean:  ## 清理构建与缓存产物
	rm -rf .pytest_cache .mypy_cache .ruff_cache build dist *.egg-info
	find . -name __pycache__ -type d -prune -exec rm -rf {} +
