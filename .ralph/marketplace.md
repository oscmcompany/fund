# Ralph Marketplace State

This directory contains the runtime state for the Ralph marketplace competition system.

## Directory Structure

```
.ralph/
├── config.json              # Marketplace configuration (tracked in git)
├── marketplace.json         # Cached state (gitignored, regenerated from events)
├── .state_version           # Event count for cache invalidation (gitignored)
└── events/                  # Append-only event log (tracked in git)
    └── [timestamp]-[bot_id]-[outcome].json
```

## Files

### config.json

Configuration for the marketplace:
- `num_bots`: Number of competing smart bots (default: 3)
- `base_budget_per_bot`: Base iteration budget per bot (default: 10)
- `scoring_weights`: Weight for each scoring dimension (sum to 1.0)
- `weight_constraints`: Min/max weight bounds for bots

**Tracked in git:** Yes - configuration is part of the codebase

### marketplace.json

Cached marketplace state computed from event log:
- Bot weights, efficiency, success/failure counts
- Total budget pool
- Rounds completed
- Last updated timestamp

**Tracked in git:** No (gitignored) - regenerated from events automatically

### .state_version

Simple counter tracking the number of events processed to determine if cache is stale.

**Tracked in git:** No (gitignored)

### events/

Append-only log of marketplace events. Each event is a JSON file named:
```
[timestamp]-[bot_id]-[outcome].json
```

Example: `2026-01-29T10-15-00-123456Z-smart_bot_2-success.json`

Event schema:
```json
{
  "timestamp": "2026-01-29T10:15:00.123456Z",
  "issue_number": 123,
  "bot_id": "smart_bot_2",
  "outcome": "success",
  "proposal_score": 0.87,
  "implementation_score": 0.85,
  "accuracy": 0.98,
  "weight_delta": 0.15,
  "iteration_count": 3,
  "metrics": {
    "tests_passed": true,
    "code_quality_passed": true,
    "coverage_delta": 2.5,
    "lines_changed": 45,
    "files_affected": 3
  }
}
```

**Tracked in git:** Yes - events are the source of truth for marketplace learning

## State Management

The marketplace uses an append-only event log for state management:

1. **Events are immutable** - once written, never modified
2. **State is computed** - marketplace.json is derived from events
3. **Cache invalidation** - .state_version tracks when recomputation is needed
4. **Conflict resolution** - multiple branches can add different events; they merge cleanly

## Concurrency

Multiple developers can run marketplace loops concurrently:

1. Each loop appends new event files (unique timestamps prevent conflicts)
2. marketplace.json might conflict on merge, but it's gitignored
3. After pulling, state is recomputed from merged event log
4. All developers converge on same state (events are source of truth)

## Commands

Initialize marketplace:
```bash
mask ralph marketplace setup
```

View current state:
```bash
mask ralph marketplace status
```

Run marketplace loop:
```bash
mask ralph marketplace loop <issue_number>
```

Reset to initial state (erase history):
```bash
mask ralph marketplace reset
```

For simple (single-agent) workflow, use:
```bash
mask ralph simple setup
mask ralph simple loop <issue_number>
```

## Learning Persistence

Bot weights and efficiency evolve over time based on events:
- Successful implementations increase bot weight
- Failed implementations decrease bot weight
- Accuracy bonuses for good prediction
- Efficiency = success_rate affects budget allocation

This creates a competitive marketplace where high-performing bots get more opportunities.

## Backup and Recovery

To backup marketplace state:
```bash
cp -r .ralph .ralph.backup
```

To recover:
```bash
cp -r .ralph.backup .ralph
```

Events are tracked in git, so full history is preserved in version control.

## Troubleshooting

**Cache out of sync:**
```bash
# Delete cache, will regenerate from events
rm .ralph/marketplace.json .ralph/.state_version
mask ralph marketplace status
```

**Corrupted event:**
```bash
# Find and remove bad event file
ls -la .ralph/events/
rm .ralph/events/[bad-event-file].json
```

**Reset everything:**
```bash
mask ralph marketplace reset
```
