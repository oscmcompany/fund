FROM python:3.12.10-slim AS builder

COPY --from=ghcr.io/astral-sh/uv:0.7.2 /uv /bin/uv

WORKDIR /app

COPY pyproject.toml uv.lock ./

COPY applications/ensemble_manager/ applications/ensemble_manager/

COPY libraries/python/ libraries/python/

COPY models/tide/ models/tide/
COPY tools/ tools/

RUN uv sync --no-dev

FROM python:3.12.10-slim AS server

RUN apt-get update && \
    apt-get install -y --no-install-recommends build-essential clang && \
    rm -rf /var/lib/apt/lists/*

ENV PYTHONPATH=/app/applications/ensemble_manager/src

WORKDIR /app

COPY --from=ghcr.io/astral-sh/uv:0.7.2 /uv /bin/uv

COPY --from=builder /app /app

RUN useradd --system --uid 10001 --create-home appuser && \
    chown -R appuser:appuser /app

USER appuser

EXPOSE 8080

ENTRYPOINT ["uv", "run", "--package", "ensemble_manager", "uvicorn", "ensemble_manager.server:application", "--host", "0.0.0.0", "--port", "8080", "--app-dir", "applications/ensemble_manager/src"]
