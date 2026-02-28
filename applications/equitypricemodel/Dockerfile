FROM python:3.12.10-slim AS builder 

COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv  

WORKDIR /app

COPY pyproject.toml uv.lock ./

COPY applications/equitypricemodel/ applications/equitypricemodel/

COPY libraries/python/ libraries/python/

RUN uv sync --no-dev 

FROM nvidia/cuda:12.4.1-devel-ubuntu22.04 AS trainer

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

COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

COPY --from=builder /app /app

ENV PYTHONPATH=/app/applications/equitypricemodel/src
ENV TRAINING_DATA_PATH=/opt/ml/input/data/train/filtered_tide_training_data.parquet
ENV MODEL_OUTPUT_PATH=/opt/ml/model

ENTRYPOINT ["uv", "run", "--package", "equitypricemodel", "python", "-m", "equitypricemodel.trainer"]

FROM python:3.12.10-slim AS server

RUN apt-get update && \
    apt-get install -y --no-install-recommends build-essential clang && \
    rm -rf /var/lib/apt/lists/*

ENV PYTHONPATH=/app/applications/equitypricemodel/src

WORKDIR /app

COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

COPY --from=builder /app /app

EXPOSE 8080

ENTRYPOINT ["uv", "run", "--package", "equitypricemodel", "uvicorn", "equitypricemodel.server:application", "--host", "0.0.0.0", "--port", "8080", "--app-dir", "applications/equitypricemodel/src"]

