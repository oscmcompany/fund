FROM python:3.12.10-slim AS builder

COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

WORKDIR /app

COPY pyproject.toml uv.lock ./

COPY tools/ tools/

COPY applications/equitypricemodel/ applications/equitypricemodel/

COPY libraries/python/ libraries/python/

RUN uv sync --no-dev

FROM python:3.12.10-slim AS worker

RUN apt-get update && \
    apt-get install -y --no-install-recommends build-essential clang && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

COPY --from=builder /app /app

ENV PYTHONPATH=/app/tools/src:/app/applications/equitypricemodel/src:/app/libraries/python/src

ENTRYPOINT ["uv", "run", "--package", "tools", "prefect", "worker", "start", "--pool", "training-pool", "--type", "process"]
