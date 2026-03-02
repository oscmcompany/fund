FROM python:3.12.10-slim AS builder

COPY --from=ghcr.io/astral-sh/uv:0.7.2 /uv /bin/uv

RUN apt-get update && \
    apt-get install -y --no-install-recommends build-essential clang && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY pyproject.toml uv.lock ./

COPY tools/ tools/

COPY applications/equitypricemodel/ applications/equitypricemodel/

COPY libraries/python/ libraries/python/

RUN uv sync --no-dev

FROM python:3.12.10-slim AS worker

WORKDIR /app

RUN groupadd --system worker && useradd --system --gid worker worker

COPY --from=ghcr.io/astral-sh/uv:0.7.2 /uv /bin/uv

COPY --from=builder /app /app

ENV PYTHONPATH=/app/tools/src:/app/applications/equitypricemodel/src:/app/libraries/python/src

USER worker

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD ["python", "-c", "import sys; sys.exit(0)"]

ENTRYPOINT ["uv", "run", "--package", "tools", "prefect", "worker", "start", "--pool", "training-pool", "--type", "process"]
