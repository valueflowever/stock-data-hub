FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    TZ=Asia/Shanghai \
    HOME=/home/appuser

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates tzdata \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml README.md ./
COPY src ./src

RUN python -m pip install --retries 5 --timeout 120 ".[service,sources,indicators]"

RUN useradd --create-home --shell /usr/sbin/nologin appuser \
    && site_packages="$(python -c 'import sysconfig; print(sysconfig.get_path("purelib"))')" \
    && install -d -o appuser -g appuser "$site_packages/efinance/data" \
    && chown -R appuser:appuser /app

USER appuser

EXPOSE 8000

CMD ["stock-data-hub", "serve", "--host", "0.0.0.0", "--port", "8000"]
