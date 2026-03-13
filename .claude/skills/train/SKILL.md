---
name: train
description: >
  Autonomous model training optimization loop using autoresearch methodology.
  Invoked as `/train <model-name>` (e.g., `/train tide`). Iteratively modifies model
  architecture code, runs training via parallel local execution, evaluates a target
  metric, and keeps or reverts changes to find the best model. Uses parallel Agent
  workers for hyperparameter search and sequential iterations for architecture changes.
  Use when the user wants to optimize a model, run autoresearch, find the best
  architecture, or improve training metrics. Optionally accepts `--metric <name>` to
  specify the optimization target.
---

# Autoresearch Training Loop

Autonomous model optimization via iterative architecture search. See
[references/autoresearch.md](references/autoresearch.md) for methodology details.

## Invocation

```
/train <model-name>                         # infer metric from model
/train <model-name> --metric <metric-name>  # explicit metric
```

## Training Infrastructure

Training runs **locally** via `uv run` for fast iteration. MinIO provides the initial
training data but it is cached to local disk for speed.

### Data Setup (one-time)

MinIO must be running for initial data seeding/caching:

```bash
# Start MinIO
docker compose up -d minio minio-init

# Seed synthetic data (only if empty / after volume removal)
docker compose up -d postgres prefect-server datamanager prefect-worker-1
docker compose exec prefect-worker-1 uv run --package tools python -m tools.seed_minio

# Cache training data locally (reads from MinIO, writes to local parquet)
AWS_ENDPOINT_URL=http://localhost:9000 AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin \
  uv run --package tools python -m tools.cache_training_data
```

This creates `results/equitypricemodel/training_data.parquet`. Subsequent training
runs read this file directly -- no Docker or network needed.

### Local Training CLI

```bash
uv run --package tools python -m tools.run_training \
  --data-path results/equitypricemodel/training_data.parquet \
  --config '{"learning_rate": 0.003, "hidden_size": 64, "epoch_count": 1}'
```

Outputs a single JSON line to **stdout** (logs go to stderr):
```json
{"config": {...}, "quantile_loss": 0.2541, "all_losses": [0.2541], "status": "OK"}
```

## Startup Procedure

1. Parse `<model-name>` from args. Resolve the model directory under `applications/`.
   Known mappings:
   - `tide` -> `applications/equitypricemodel/`
   - `equitypricemodel` -> `applications/equitypricemodel/`

2. **Run preflight checks (fail fast):**
   ```bash
   # Model directory exists
   ls applications/<model-name>model/src/

   # Training data is cached locally
   ls results/equitypricemodel/training_data.parquet
   # If missing, run cache_training_data (requires MinIO running)

   # Local training works
   uv run --package tools python -m tools.run_training \
     --data-path results/equitypricemodel/training_data.parquet \
     --config '{"epoch_count": 1}' 2>/dev/null
   ```

3. If `--metric` not provided, read the model's training code to infer available metrics.
   Default: `quantile_loss` (minimize).

4. Read existing `results/<model-name>/results.jsonl` if present to understand prior runs.

5. Create a dated branch: `autoresearch/<model-name>/<YYYY-MM-DD>`

6. Confirm with user before starting the loop.

## Main Loop

Run indefinitely until the user interrupts. Two modes of operation:

### Mode A: Parallel Hyperparameter Search

For config-only changes (learning rate, hidden size, dropout, batch size, etc.),
run **multiple experiments in parallel** using background Agent workers.

**Orchestrator pattern:**

1. **Design a batch** of 3-4 config variants to test, each exploring a different
   hypothesis. Include the reasoning for each.

2. **Spawn parallel agents** using the Agent tool with `run_in_background: true`.
   Each agent gets a prompt like:

   ```
   Run this training command and return the full JSON output:

   cd /path/to/repo && uv run --package tools python -m tools.run_training \
     --data-path results/equitypricemodel/training_data.parquet \
     --config '<JSON config>'

   Return the stdout JSON result.
   ```

3. **Collect results** as agents complete (you'll be notified automatically).
   Do NOT poll or sleep -- continue with other work or wait for notifications.

4. **Log all results** to `results.jsonl` and identify the best performer.

5. **Apply the winning config** to `DEFAULT_CONFIGURATION` in trainer.py if it beat
   the previous best. Commit with description.

6. **Design the next batch** based on what worked: narrow the search around winning
   values, or explore a new dimension.

### Mode B: Sequential Architecture Changes

For code changes (model architecture, loss function, layer structure), run
**one experiment at a time** since each requires different code:

1. **Hypothesize** a single architecture change
2. **Modify** the model code (tide_model.py or trainer.py)
3. **Commit**: `git add -A && git commit -m "autoresearch: <description>"`
4. **Run training** locally:
   ```bash
   uv run --package tools python -m tools.run_training \
     --data-path results/equitypricemodel/training_data.parquet \
     --config '{}' 2>/dev/null
   ```
5. **Parse stdout JSON** for `quantile_loss`
6. **Decide**: KEEP (new baseline) or DISCARD (`git reset --hard HEAD~1`)
7. **Log to results.jsonl**

### Prioritization

Alternate between modes based on what the results suggest:
- Start with Mode A to find good hyperparameters quickly
- Switch to Mode B when architecture changes seem more promising
- After an architecture KEEP, run Mode A again to re-tune hyperparameters

Priority order: architecture > hyperparameters > loss function > data pipeline

## Results Format

Append JSON lines to `results/<model-name>/results.jsonl`:

```json
{
  "experiment": 4,
  "commit": "abc1234",
  "quantile_loss": 0.2541,
  "status": "KEEP",
  "description": "increase learning rate to 0.01",
  "config": {"learning_rate": 0.01, "hidden_size": 64, "...": "..."},
  "changes": "changed learning_rate from 0.003 to 0.01",
  "reasoning": "with 1 epoch, higher LR extracts more from single pass. result confirmed hypothesis"
}
```

Every experiment gets logged, including DISCARDs and CRASHes, so the full search
history is preserved for analysis.

## Error Handling

**Fail fast principle**: Surface errors immediately.

- **Training crash**: Check stderr output from `run_training.py`. Log as CRASH,
  fix the bug (this is a bug fix, not an experiment), commit fix, retry.
- **Missing training data**: Run `cache_training_data.py` to re-download from MinIO.
- **Import errors**: Ensure `uv sync` has been run for the workspace.
- **tinygrad CPU codegen bug**: The model avoids 3D Linear operations (see commit
  b692d6a). If new architecture changes trigger it, ensure all Linear layers
  operate on 2D tensors only.

## Files Modified During Loop

Primarily:
- `applications/<model>model/src/<model>model/*_model.py` (model architecture)
- `applications/<model>model/src/<model>model/trainer.py` (configuration/hyperparams)

Only when fixing bugs:
- `tools/src/tools/flows/training_flow.py`
- `tools/src/tools/run_training.py`
- `applications/<model>model/src/<model>model/*_data.py`

## Key Files

- **Training CLI**: `tools/src/tools/run_training.py` -- lightweight local runner
- **Data cache**: `tools/src/tools/cache_training_data.py` -- downloads from MinIO
- **Cached data**: `results/equitypricemodel/training_data.parquet`
- **Results log**: `results/equitypricemodel/results.jsonl`
