---
name: train
description: >
  Autonomous model training optimization loop using autoresearch methodology.
  Invoked as `/train <model-name>` (e.g., `/train tide`). Iteratively modifies model
  architecture code, runs training via parallel local execution, evaluates a target
  metric, and keeps or reverts changes to find the best model. Uses parallel Agent
  workers for hyperparameter search and sequential iterations for architecture changes.
  Use when the user wants to optimize a model, run autoresearch, find the best
  architecture, or improve training metrics. Includes epoch escalation (Mode C) for
  pushing past plateaus. Optionally accepts `--metric <name>` to specify the
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
{"config": {...}, "quantile_loss": 0.0206, "final_epoch_loss": 0.0298, "all_losses": [0.05, 0.03, 0.02, 0.03], "status": "OK"}
```

**Important**: `quantile_loss` is `min(all_losses)` -- the best epoch's loss. This matches
the actual model state because `run_training.py` uses checkpointing: the model saves its
best weights during training and restores them before returning. `final_epoch_loss` is
provided for reference but should NOT be used for KEEP/DISCARD decisions.

Use `quantile_loss` for all KEEP/DISCARD comparisons.

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

4. Check MLflow for prior runs in the experiment to understand baseline performance.
   All training runs are automatically logged to MLflow when `MLFLOW_TRACKING_URI` is set.
   Local runs are tagged with `source=cli, task=autoresearch, environment=development`.

5. Create a dated branch: `autoresearch/<model-name>/<YYYY-MM-DD>`

6. Confirm with user before starting the loop.

## Main Loop

Run indefinitely until the user interrupts. Two modes of operation:

### Mode A: Parallel Hyperparameter Search

For config-only changes (learning rate, hidden size, dropout, batch size, etc.),
run **multiple experiments in parallel** using Bash background jobs.

**Orchestrator pattern:**

1. **Design a batch** of 3-4 config variants to test, each exploring a different
   hypothesis. Include the reasoning for each.

2. **Spawn parallel training runs** using the Bash tool with `run_in_background: true`:

   ```bash
   uv run --package tools python -m tools.run_training \
     --data-path results/equitypricemodel/training_data.parquet \
     --config '<JSON config>' 2>/dev/null
   ```

3. **Collect results** using `TaskOutput` with `block: true` on each task ID.
   All tasks can be awaited in parallel.

4. **Collect results** and identify the best performer. All runs are automatically
   logged to MLflow with params, epoch-by-epoch loss curves, and best_quantile_loss.

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
   Results are automatically tracked in MLflow.

### Mode C: Epoch Escalation

When improvements plateau at the current epoch count, **escalate training depth**
before concluding that the search is exhausted:

1. **Detect plateau**: If 2+ consecutive batches produce no new KEEP at the current
   epoch count, the search is likely exhausted at this training depth.

2. **Sweep epochs**: Run parallel experiments at 2x, 3x, 5x, and 10x the current
   epoch count to find the next useful training depth.

3. **Re-tune hyperparameters**: After increasing epochs, LR and dropout must be
   re-tuned because their optimal values shift:
   - **LR decreases** with more epochs (e.g., 0.003 at 1 epoch -> 0.001 at 10 epochs)
   - **Dropout increases** with more epochs (e.g., 0.0 at 1 epoch -> 0.05 at 10 epochs)
   - Always run Mode A after epoch escalation to find the new optimal LR/dropout.

4. **Repeat**: Continue the Mode A -> Mode B -> Mode C cycle. Each epoch escalation
   unlocks a new region of the loss landscape to explore.

### Prioritization

Alternate between modes based on what the results suggest:
- Start with Mode A to find good hyperparameters quickly
- Switch to Mode B when architecture changes seem more promising
- After an architecture KEEP, run Mode A again to re-tune hyperparameters
- **When Mode A and B both plateau, use Mode C to escalate epochs**
- After epoch escalation, return to Mode A to re-tune for the new depth

Priority order: architecture > hyperparameters > epoch depth > loss function > data pipeline

## Experiment Tracking

All experiments are automatically tracked in MLflow (centralized on AWS at
`http://<alb-dns>:5000`). Each training run logs:

- **Parameters**: Full configuration dict (learning_rate, hidden_size, etc.)
- **Metrics**: Per-epoch `quantile_loss`, `best_quantile_loss`, `final_quantile_loss`, `total_epochs`
- **Tags**: `environment` (development/production), `source` (cli/prefect), `host`, `task`

The MLflow experiment name is `tide`. Use the MLflow UI to compare runs, view loss
curves, and track the full search history.

stdout JSON output from `run_training.py` is still used for KEEP/DISCARD decisions
in the autoresearch loop (fast, no network dependency).

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
- **MLflow tracking**: `models/tide/src/tide/tracking.py` -- MLflow integration module
- **MLflow UI**: `http://<alb-dns>:5000` -- centralized experiment tracking (AWS ALB)
