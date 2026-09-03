FROM python:3.12-slim

WORKDIR /srv

# 依赖钉版唯一处是 pyproject.toml（SPEC §4 版本纪律，不用 requirements.txt）
COPY . .
RUN pip install --no-cache-dir .

EXPOSE 8000

CMD ["gunicorn", "-c", "gunicorn.conf.py", "app.main:app"]
