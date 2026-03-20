# Autoresearch Methodology Reference

## Core Concept

Autonomous model optimization loop inspired by Karpathy's autoresearch. An LLM agent
iteratively modifies model architecture code, runs training via the existing Prefect pipeline,
evaluates a target metric, and keeps or reverts changes based on improvement.

## Loop Steps

1. **Analyze** current model code and results history
2. **Hypothesize** a change (architecture, hyperparameters, loss function, etc.)
3. **Modify** the model code in a focused, single-variable way
4. **Commit** the change to a dated branch
5. **Run** training via Docker Compose Prefect pipeline
6. **Extract** the target metric from training logs
7. **Decide**: if metric improved, KEEP; otherwise REVERT via `git reset`
8. **Log** result to `experiments.jsonl`
9. **Repeat**

## Decision Rules

- **KEEP**: Target metric improved (lower loss, higher accuracy, etc. depending on direction)
- **DISCARD**: Target metric did not improve - revert to last known good state
- **CRASH**: Training failed - debug and fix pipeline errors, then retry
- Never accumulate failed changes; always revert to last good state before trying next idea

## Change Categories (ordered by typical impact)

1. **Architecture**: layer counts, hidden sizes, attention mechanisms, skip connections, activation functions, normalization
2. **Hyperparameters**: learning rate, batch size, dropout rate, weight decay
3. **Loss function**: alternative losses, loss weighting, auxiliary losses
4. **Data processing**: feature engineering changes in the pipeline stages
5. **Optimizer**: optimizer type, scheduler, warmup

## Results Logging Format

Log to `autotrain/<model-name>/experiments.jsonl` (one JSON object per line):

```jsonl
{"commit": "abc1234", "metric_value": 0.0342, "status": "KEEP", "change": "Increased hidden_size from 64 to 128", "hypothesis": "Larger hidden dim captures more complex feature interactions", "rationale": "Loss improved 3.2% -- wider layers help at this data scale"}
{"commit": "def5678", "metric_value": 0.0351, "status": "DISCARD", "change": "Added third encoder layer", "hypothesis": "Deeper encoder extracts richer temporal representations", "rationale": "Loss regressed 2.6% -- model likely overfitting with added depth at current data size"}
{"commit": "ghi9012", "metric_value": null, "status": "CRASH", "change": "Switched to GeLU activation", "hypothesis": "Smoother gradient flow improves convergence", "rationale": "TypeError in forward pass -- tinygrad GeLU not compatible with cast pattern, fixed"}
```

Fields:
- `commit`: git commit hash
- `metric_value`: measured metric (null if crash)
- `status`: KEEP, DISCARD, or CRASH
- `change`: what was changed
- `hypothesis`: why this change was expected to improve the model
- `rationale`: why the change was kept or rejected (post-experiment reasoning)

## Key Constraints

- One change per experiment for clear attribution
- Always commit before running so changes are traceable
- Keep changes small and testable
- If a crash occurs, fix the bug (don't count as an architecture experiment), commit the fix, and re-run
- Build incrementally on previous wins (greedy hill-climbing)

## Metric Direction

- **Minimize**: loss, quantile_loss, mse, mae, bpb
- **Maximize**: accuracy, r2, sharpe, precision, recall
