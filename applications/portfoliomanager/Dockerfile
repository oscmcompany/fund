FROM python:3.12.10 AS builder

COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

WORKDIR /app

COPY pyproject.toml uv.lock ./

COPY applications/portfoliomanager/ applications/portfoliomanager/

COPY libraries/python/ libraries/python/

RUN uv sync --no-dev 

FROM python:3.12.10-slim AS server

RUN apt-get update && \
    apt-get install -y --no-install-recommends build-essential clang curl && \
    rm -rf /var/lib/apt/lists/*

ENV PYTHONPATH=/app/applications/portfoliomanager/src

WORKDIR /app

COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

COPY --from=builder /app /app

EXPOSE 8080

ENTRYPOINT ["uv", "run", "--package", "portfoliomanager", "uvicorn", "portfoliomanager.server:application", "--host", "0.0.0.0", "--port", "8080", "--app-dir", "applications/portfoliomanager/src"]

