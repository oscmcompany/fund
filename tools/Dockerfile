FROM python:3.12.10-slim AS builder

COPY --from=ghcr.io/astral-sh/uv:0.7.2 /uv /bin/uv

RUN apt-get update && \
    apt-get install -y --no-install-recommends build-essential clang && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY pyproject.toml uv.lock ./

COPY tools/ tools/

COPY applications/ensemble_manager/ applications/ensemble_manager/

COPY libraries/python/ libraries/python/

RUN uv sync --no-dev

FROM python:3.12.10-slim AS worker

WORKDIR /app

RUN apt-get update && \
    apt-get install -y --no-install-recommends build-essential clang && \
    rm -rf /var/lib/apt/lists/*

RUN groupadd --system worker && useradd --system --gid worker worker

COPY --from=ghcr.io/astral-sh/uv:0.7.2 /uv /bin/uv

COPY --from=builder /app /app

ENV PYTHONPATH=/app/tools/src:/app/applications/ensemble_manager/src:/app/libraries/python/src
ENV HOME=/home/worker

RUN mkdir -p /home/worker/.aws && \
    printf '[default]\ns3 =\n    addressing_style = path\n' > /home/worker/.aws/config && \
    chown -R worker:worker /home/worker /app && \
    printf '%s\n' \
      '#!/usr/bin/env python3' \
      'import os' \
      'import sys' \
      '' \
      'for process_id in os.listdir("/proc"):' \
      '    if not process_id.isdigit():' \
      '        continue' \
      '    command_path = f"/proc/{process_id}/cmdline"' \
      '    try:' \
      '        with open(command_path, "rb") as command_file:' \
      '            command = command_file.read().replace(b"\\x00", b" ").decode("utf-8", errors="ignore")' \
      '    except OSError:' \
      '        continue' \
      '    if "prefect worker start" in command and "training-pool" in command:' \
      '        sys.exit(0)' \
      '' \
      'sys.exit(1)' \
      > /usr/local/bin/prefect_worker_healthcheck.py && \
    chmod +x /usr/local/bin/prefect_worker_healthcheck.py

USER worker

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD ["python", "/usr/local/bin/prefect_worker_healthcheck.py"]

ENTRYPOINT ["uv", "run", "--package", "tools", "prefect", "worker", "start", "--pool", "training-pool", "--type", "process"]
