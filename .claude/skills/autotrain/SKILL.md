---
name: autotrain
description: >
  Autonomous model training optimization loop using autoresearch methodology.
  Invoked as `/autotrain <model-name>` (e.g., `/autotrain tide`). Iteratively modifies model
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
/autotrain <model-name>                         # infer metric from model
/autotrain <model-name> --metric <metric-name>  # explicit metric
```

## Training Infrastructure

Training runs **locally** via `uv run` for fast iteration. S3 data is accessed via
Prefect Cloud S3Bucket blocks (`data-bucket` and `artifact-bucket`).

### Local Training

```bash
# Run training locally (uses Prefect blocks for S3 access)
devenv tasks run models:tide:train:local

# Or directly
uv run python models/tide/src/tide/workflow.py
```

### Production Training

```bash
# Deploy to Prefect Cloud ECS work pool
devenv tasks run models:tide:deploy

# Trigger a run on ECS
devenv tasks run models:tide:train
```

## Startup Procedure

1. Parse `<model-name>` from args. Resolve the model directory under `models/`.
   Known mappings:
   - `tide` -> `models/tide/`

2. **Run preflight checks (fail fast):**
   ```bash
   # Model directory exists
   ls models/<model-name>/src/

   # Prefect Cloud connection works
   uv run prefect config view

   # Quick smoke test (1 epoch) -- override epoch_count in trainer.py DEFAULT_CONFIGURATION
   uv run python -c "from tide.trainer import train_model; ..."
   ```

3. If `--metric` not provided, read the model's training code to infer available metrics.
   Default: `quantile_loss` (minimize).

4. Create a dated branch: `autoresearch/<model-name>/<YYYY-MM-DD>`

5. Confirm with user before starting the loop.

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
   uv run python models/tide/src/tide/workflow.py
   ```

3. **Collect results** using `TaskOutput` with `block: true` on each task ID.

4. **Identify the best performer**.

5. **Apply the winning config** to `DEFAULT_CONFIGURATION` in `models/tide/src/tide/trainer.py`
   if it beat the previous best. Commit with description.

6. **Log result** to `autotrain/<model-name>/experiments.jsonl` with fields:
   `commit`, `metric_value`, `status`, `change`, `hypothesis`, `rationale`.

7. **Design the next batch** based on what worked.

### Mode B: Sequential Architecture Changes

For code changes (model architecture, loss function, layer structure), run
**one experiment at a time**:

1. **Hypothesize** a single architecture change
2. **Modify** the model code (model.py or data.py)
3. **Commit**: `git add -A && git commit -m "autoresearch: <description>"`
4. **Run training**:
   ```bash
   uv run python models/tide/src/tide/workflow.py
   ```
5. **Parse output** for `quantile_loss`
6. **Decide**: KEEP (new baseline) or DISCARD (`git reset --hard HEAD~1`)
7. **Log result** to `autotrain/<model-name>/experiments.jsonl` with fields:
   `commit`, `metric_value`, `status`, `change`, `hypothesis`, `rationale`

### Mode C: Epoch Escalation

When improvements plateau at the current epoch count, **escalate training depth**:

1. **Detect plateau**: 2+ consecutive batches with no KEEP.
2. **Sweep epochs**: Run parallel experiments at 2x, 3x, 5x, 10x current epochs.
3. **Re-tune hyperparameters**: LR decreases and dropout increases with more epochs.
4. **Repeat**: Continue Mode A -> B -> C cycle.

### Prioritization

- Start with Mode A for quick hyperparameter wins
- Mode B when architecture changes seem promising
- After architecture KEEP, re-tune with Mode A
- When A and B plateau, use Mode C to escalate epochs
- After epoch escalation, return to Mode A

Priority order: architecture > hyperparameters > epoch depth > loss function > data pipeline

## Error Handling

**Fail fast**: Surface errors immediately.

- **Training crash**: Log as CRASH, fix the bug, commit fix, retry.
- **Import errors**: Run `uv sync` for the workspace.
- **tinygrad CPU codegen bug**: Ensure all Linear layers operate on 2D tensors only.

## Files Modified During Loop

Primarily:
- `models/tide/src/tide/model.py` (model architecture)
- `models/tide/src/tide/data.py` (data processing)
- `models/tide/src/tide/trainer.py` (DEFAULT_CONFIGURATION, training hyperparameters)

## Key Files

- **Trainer**: `models/tide/src/tide/trainer.py` (DEFAULT_CONFIGURATION lives here)
- **Workflow**: `models/tide/src/tide/workflow.py`
- **Model**: `models/tide/src/tide/model.py`
- **Data**: `models/tide/src/tide/data.py`
- **Experiment log**: `autotrain/<model-name>/experiments.jsonl`
- **Prefect config**: `prefect.yaml`
- **devenv tasks**: `devenv.nix`
