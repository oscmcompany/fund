---
name: train
description: >
  Autonomous model training optimization loop using autoresearch methodology.
  Invoked as `/train <model-name>` (e.g., `/train tide`). Iteratively modifies model
  architecture code, runs training via the existing Prefect pipeline in Docker Compose,
  evaluates a target metric, and keeps or reverts changes to find the best model.
  Use when the user wants to optimize a model, run autoresearch, find the best architecture,
  or improve training metrics. Optionally accepts `--metric <name>` to specify the
  optimization target.
---

# Autoresearch Training Loop

Autonomous model optimization via iterative architecture search. See
[references/autoresearch.md](references/autoresearch.md) for methodology details.

## Invocation

```
/train <model-name>                         # infer metric from model
/train <model-name> --metric <metric-name>  # explicit metric
```

## Local Infrastructure

Training uses **MinIO** as a local S3-compatible store instead of AWS S3. Data persists
in a Docker volume (`minio_data`) between runs.

### Services (docker-compose.yaml at project root)

| Service | Purpose | Port |
|---------|---------|------|
| `minio` | S3-compatible object store | 9000 (API), 9001 (console) |
| `minio-init` | Creates buckets on first start | - |
| `postgres` | Prefect metadata DB | 5432 |
| `prefect-server` | Workflow orchestration | 4200 |
| `prefect-worker-1` | Executes training flows | - |
| `datamanager` | Equity data sync API | 8080 |

### MinIO Buckets

| Bucket | Env Var | Contents |
|--------|---------|----------|
| `fund-data` | `AWS_S3_DATA_BUCKET_NAME` | Equity bars (hive-partitioned parquet), details CSV |
| `fund-model-artifacts` | `AWS_S3_MODEL_ARTIFACTS_BUCKET_NAME` | Training parquet, model artifacts |

### Key Patterns

```
fund-data/equity/bars/daily/year={YYYY}/month={MM}/day={DD}/data.parquet
fund-data/equity/details/details.csv
fund-model-artifacts/training/filtered_tide_training_data.parquet
fund-model-artifacts/artifacts/equitypricemodel-trainer-{TIMESTAMP}/output/model.tar.gz
```

### Seeding Data

On first use (or after `docker compose down -v`), seed MinIO with synthetic data:

```bash
docker compose exec prefect-worker-1 uv run --package tools python -m tools.seed_minio
```

This generates 365 days of synthetic equity bars for 20 tickers. Data persists in
the `minio_data` volume across container restarts. Re-seeding is only needed after
volume removal.

### Environment

All services use MinIO credentials and endpoint:
- `AWS_ACCESS_KEY_ID=minioadmin`
- `AWS_SECRET_ACCESS_KEY=minioadmin`
- `AWS_ENDPOINT_URL=http://minio:9000`
- `SKIP_DATA_SYNC=true` (skips Massive API calls, uses seeded data)

The Rust datamanager auto-detects `AWS_ENDPOINT_URL` and enables path-style S3.
Python boto3 path-style is configured via `~/.aws/config` in Docker images.

## Startup Procedure

1. Parse `<model-name>` from args. Resolve the model directory under `applications/`.
   Models live at `applications/<model-name>model/` (e.g., `applications/equitypricemodel/`
   for `/train tide`). Known mappings:
   - `tide` -> `applications/equitypricemodel/`
   - `equitypricemodel` -> `applications/equitypricemodel/`

2. **Run preflight checks (fail fast).** Stop immediately and report if any check fails:
   ```bash
   # 1. Docker available
   docker compose version

   # 2. Model directory exists
   ls applications/<model-name>model/src/

   # 3. Start MinIO and verify buckets
   docker compose up -d minio minio-init
   # Wait for minio-init to exit successfully (creates buckets)
   docker compose ps minio-init  # should show "Exited (0)"

   # 4. Check if MinIO has data (seed if empty)
   docker compose exec minio mc ls local/fund-data/equity/bars/daily/ | head -1
   # If empty, seed: docker compose exec prefect-worker-1 uv run --package tools python -m tools.seed_minio
   ```
   Use short timeouts (10-15s max) for each check. If any fails, report the specific
   error and stop. Do NOT proceed to Docker Compose with broken prerequisites.

3. If `--metric` not provided, read the model's training code to infer available metrics
   (loss functions, validation methods). Present options to the user:
   ```
   Detected metrics for tide:
     1. quantile_loss (default, minimize)
     2. validation_loss (minimize)
   Which metric to optimize? [1]:
   ```

4. Determine metric direction (minimize or maximize) from
   [references/autoresearch.md](references/autoresearch.md).

5. Read existing `results/<model-name>/results.tsv` if present to understand prior runs.

6. Create a dated branch: `autoresearch/<model-name>/<YYYY-MM-DD>`

7. Confirm with user before starting the loop:
   ```
   Ready to optimize tide model targeting quantile_loss (minimize).
   Training runs via Prefect in Docker Compose (MinIO local storage).
   Continue? [Y/n]:
   ```

## Main Loop

Run indefinitely until the user interrupts. Each iteration:

### 1. Analyze and Hypothesize

- Read current model architecture code and training configuration
- Review `results.tsv` for patterns (what worked, what didn't)
- Form a hypothesis for a single, focused change
- Prioritize changes by category: architecture > hyperparameters > loss > data > optimizer

### 2. Modify

- Make exactly one change to the model code
- Keep changes small and attributable
- Target files: model definition (e.g., `tide_model.py`), trainer config (e.g., `trainer.py`)
- Do NOT modify the Prefect flow, data pipeline, or evaluation logic unless fixing a bug

### 3. Commit

```bash
git add -A && git commit -m "autoresearch: <description of change>"
```

### 4. Run Training

Execute the training pipeline via Docker Compose. The `docker-compose.yaml` is at the
project root (not in `tools/`).

Start only the services needed for training (skip equitypricemodel server and
portfoliomanager - they are for serving predictions, not training):

```bash
# Start core services (detached) - MinIO persists data between runs
docker compose up -d --build postgres minio minio-init prefect-server datamanager prefect-worker-1

# Wait for health checks (max 60s, fail fast if any container exits)
docker compose ps --format "table {{.Name}}\t{{.Status}}"

# Deploy the flow (only needed once per session, or after code changes)
docker compose exec prefect-worker-1 uv run --package tools python -m tools.deploy_training_flow

# Trigger a run
docker compose exec prefect-worker-1 uv run --package tools prefect deployment run 'tide-training-pipeline/daily-training'
```

Monitor Prefect worker logs for the target metric. Use short polling intervals (10s)
and fail fast if the flow run enters a Failed state:

```bash
# Check flow run status (fail fast on errors)
docker compose logs prefect-worker-1 --since 10s 2>&1 | grep -E "(Training complete|Failed|Error|final_loss|epoch_loss)"
```

Do NOT use `--follow` with long timeouts. Instead, poll with `--since` in short intervals.

### 5. Extract Metric

Parse training logs for the target metric value. For the tide model, look for:
- `final_loss` in trainer completion logs
- `epoch_loss` from the last completed epoch
- Validation loss from `model.validate()` if available

### 6. Decide and Log

Compare metric against previous best:

- **Improved**: Log as KEEP in `results.tsv`, this becomes the new baseline
- **Not improved**: Log as DISCARD, revert: `git reset --hard HEAD~1`
- **Crashed**: Log as CRASH, diagnose and fix the error (this is a bug fix, not an
  experiment), commit the fix, and re-run the same experiment

Append to `results.tsv`:
```
<commit-hash>\t<metric-value>\t<KEEP|DISCARD|CRASH>\t<description>
```

### 7. Report

After each iteration, briefly report:
```
Experiment #N: <description>
  Result: <metric_value> (previous best: <prev_best>)
  Status: KEEP/DISCARD
  Running best: <best_value> over N experiments
```

### 8. Repeat

Return to step 1. Build on kept changes. Never ask for permission to continue.

## Error Handling

**Fail fast principle**: Surface errors immediately. Never wait with long timeouts when
something is broken. Use 10-15s timeouts for health checks, 30s for container startup.

- **MinIO not starting**: Check `docker compose logs minio`. Verify port 9000 is free.
  Console available at `http://localhost:9001` (minioadmin/minioadmin).
- **Empty buckets after restart**: If `docker compose down -v` was used, re-seed:
  `docker compose exec prefect-worker-1 uv run --package tools python -m tools.seed_minio`
- **Container crash on startup**: Check `docker compose ps` and logs within 15s. If a
  container exited, report the error and stop. Do not wait for health check timeouts.
- **Build failure**: Read error logs, fix the code, commit fix, retry
- **Runtime crash**: Check stack trace, fix bug in model/pipeline code, commit fix, retry
- **Prefect flow failure**: Poll `docker compose logs` with `--since 10s` in short
  intervals. If the flow enters Failed state, extract the error and report immediately.
- **Timeout**: If training exceeds reasonable time, consider reducing epochs or batch size
  as the next experiment

## Files Modified During Loop

Primarily:
- `applications/<model>model/src/<model>model/*_model.py` (model architecture)
- `applications/<model>model/src/<model>model/trainer.py` (configuration/hyperparams)

Only when fixing bugs:
- `tools/src/tools/flows/training_flow.py`
- `applications/<model>model/src/<model>model/*_data.py`
- `tools/Dockerfile`

## Results File

Created at `results/<model-name>/results.tsv` to track all experiments.
