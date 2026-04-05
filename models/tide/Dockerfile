FROM python:3.12.10-slim AS builder

COPY --from=ghcr.io/astral-sh/uv:0.7.2 /uv /bin/uv

WORKDIR /app

COPY pyproject.toml uv.lock ./

COPY models/tide/ models/tide/

COPY libraries/python/ libraries/python/

COPY tools/ tools/

RUN uv sync --no-dev --package tide

FROM nvidia/cuda:12.4.1-devel-ubuntu22.04 AS model-runner

ENV DEBIAN_FRONTEND=noninteractive
ENV TZ=UTC

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    software-properties-common && \
    add-apt-repository -y ppa:deadsnakes/ppa && \
    apt-get update && \
    apt-get install -y --no-install-recommends \
    python3.12 python3.12-venv python3.12-dev \
    build-essential clang curl && \
    rm -rf /var/lib/apt/lists/* && \
    ln -sf /usr/bin/python3.12 /usr/bin/python

ENV CUDA=1

WORKDIR /app

COPY --from=ghcr.io/astral-sh/uv:0.7.2 /uv /bin/uv

COPY --from=builder /app /app

ENV PYTHONPATH=/app/models/tide/src

RUN useradd --system --uid 10001 --create-home appuser && \
    chown -R appuser:appuser /app

USER appuser

ENTRYPOINT ["uv", "run", "--package", "tide", "python", "-m", "tide.workflow"]
