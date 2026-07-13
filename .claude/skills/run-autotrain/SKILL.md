---
name: run-autotrain
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

Training runs **locally** via `cargo run --release --bin tide_model_trainer`. S3 data
and model artifacts are accessed via AWS SDK using the `AWS_S3_BUCKET_NAME`
environment variable.

### Local Training

```bash
# Run training locally (uses secretspec for AWS/Alpaca credentials)
secretspec run -- cargo run --release --features train --bin tide_model_trainer

# Override epoch count or lookback window
FUND_EPOCHS=50 FUND_LOOKBACK_DAYS=730 secretspec run -- cargo run --release --features train --bin tide_model_trainer
```

## Startup Procedure

1. Parse `<model-name>` from args. Resolve the model source under `src/models/`.
   Known mappings:
   - `tide` -> `src/models/tide/` (source modules) + `src/bin/tide_model_trainer.rs` (entrypoint)

2. **Run preflight checks (fail fast):**
   ```bash
   # Model source exists
   ls src/models/<model-name>/

   # Binary compiles
   cargo build --release --features train --bin tide_model_trainer

   # Quick smoke test (1 epoch)
   FUND_EPOCHS=1 FUND_LOOKBACK_DAYS=30 secretspec run -- cargo run --release --features train --bin tide_model_trainer
   ```

3. If `--metric` not provided, read the model's evaluation code to infer available metrics.
   Default: `crps` (continuous ranked probability score, minimize).

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

2. **Spawn parallel training runs** using the Bash tool with `run_in_background: true`.
   Each variant needs its own working copy to avoid conflicting source edits:

   - For **config-only sweeps** that can be expressed as environment variables
     (`FUND_EPOCHS`, `FUND_LOOKBACK_DAYS`), run directly — no source edits needed.
   - For **source-level sweeps** (hidden size, dropout, layer counts), create a
     `git worktree` per variant so each build is isolated:

   ```bash
   git worktree add /tmp/autotrain-variant-1 HEAD
   # edit config.rs in /tmp/autotrain-variant-1, then:
   cd /tmp/autotrain-variant-1 && secretspec run -- cargo run --release --features train --bin tide_model_trainer
   ```

3. **Collect results** using `TaskOutput` with `block: true` on each task ID.

4. **Identify the best performer**.

5. **Apply the winning config** to the `Default` impl in `src/models/tide/config.rs`
   (for `ModelParameters`) or `src/models/tide/train.rs` (for `TrainConfig`)
   if it beat the previous best. Commit with description.

6. **Log result** to `autotrain/<model-name>/experiments.jsonl` with fields:
   `commit`, `metric_value`, `status`, `change`, `hypothesis`, `rationale`.

7. **Design the next batch** based on what worked.

### Mode B: Sequential Architecture Changes

For code changes (model architecture, loss function, layer structure), run
**one experiment at a time**:

1. **Hypothesize** a single architecture change
2. **Modify** the model code (model.rs, data.rs, or loss.rs)
3. **Commit**: `git add -A && git commit -m "autoresearch: <description>"`
4. **Run training**:
   ```bash
   secretspec run -- cargo run --release --features train --bin tide_model_trainer
   ```
5. **Parse output** for `crps` (logged by tracing as `crps = <value>`)
6. **Decide**: KEEP (new baseline) or DISCARD (`git reset --hard HEAD~1`)
7. **Log result** to `autotrain/<model-name>/experiments.jsonl` with fields:
   `commit`, `metric_value`, `status`, `change`, `hypothesis`, `rationale`

### Mode C: Epoch Escalation

When improvements plateau at the current epoch count, **escalate training depth**:

1. **Detect plateau**: 2+ consecutive batches with no KEEP.
2. **Sweep epochs**: Run parallel experiments at 2x, 3x, 5x, 10x current epochs
   (override via `FUND_EPOCHS` environment variable).
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
- **Compilation errors**: Fix the Rust code, ensure `cargo build --release --features train` passes.
- **Missing data**: Check `AWS_S3_BUCKET_NAME` and S3 connectivity via `secretspec run -- aws s3 ls`.

## Files Modified During Loop

Primarily:
- `src/models/tide/model.rs` (model architecture — TideModel, encoder/decoder layers)
- `src/models/tide/data.rs` (data processing — feature engineering, dataset construction)
- `src/models/tide/config.rs` (ModelParameters — architecture hyperparameters)
- `src/models/tide/train.rs` (TrainConfig — learning rate, epochs, batch size, early stopping)
- `src/models/tide/loss.rs` (loss function — quantile loss, huber delta)

## Key Files

- **Entrypoint**: `src/bin/tide_model_trainer.rs` (loads data, trains, evaluates, uploads artifact)
- **Model**: `src/models/tide/model.rs` (Burn TideModel with encoder/decoder)
- **Config**: `src/models/tide/config.rs` (ModelParameters with Default impl)
- **Training**: `src/models/tide/train.rs` (TrainConfig, train loop, early stopping)
- **Data**: `src/models/tide/data.rs` (TrainingDataset, feature engineering)
- **Loss**: `src/models/tide/loss.rs` (quantile loss with huber delta)
- **Evaluation**: `src/models/tide/evaluate.rs` (CRPS, directional accuracy, quantile coverage)
- **Drift**: `src/models/tide/drift.rs` (drift detection against prior runs)
- **Artifacts**: `src/models/tide/artifact.rs` (tar.gz packaging, S3 upload)
- **Experiment log**: `autotrain/<model-name>/experiments.jsonl`
- **devenv config**: `devenv.nix`
