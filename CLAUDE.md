# claude

> Agentic tooling context

## Notes

- Rust and Python are the primary project languages
- [Flox](https://flox.dev/) manages project environment and packages
- [Mask](https://github.com/jacobdeichert/mask) is used for command management
- [Pulumi](https://www.pulumi.com/) manages cloud infrastructure via the Python SDK
- Python code follows [uv](https://github.com/astral-sh/uv) workspace conventions
- Rust code follows Cargo workspace conventions
- AWS is the target cloud provider
- Models are primarily built using [tinygrad](https://docs.tinygrad.org/)
- Python servers primarily use [FastAPI](https://fastapi.tiangolo.com/)
- Use `mask development python all` for comprehensive Python checks
- Use `mask development rust all` for comprehensive Rust checks
- Add in-line code comments only where necessary for clarity
- Use full word variables in code whenever possible
- Follow Rust and Python recommended casing conventions
- Strictly use Python version 3.12.10
- Scan and remove unused dependencies from `pyproject.toml` files
- Move duplicate dependencies into root workspace `pyproject.toml`
- Introduce new dependencies only after approval
- Include type hints on all Python function parameters and return types
- Rust servers primarily use [Axum](https://docs.rs/axum/latest/axum/)
- Use Polars for [Python](https://docs.pola.rs/api/python/stable/reference/index.html) and
  [Rust](https://docs.rs/polars/latest/polars/) dataframes
- Use `typing` module `cast` function for `tinygrad` method outputs when necessary with union types
- Write `pytest` functions for Python tests
- Ensure Rust and Python automated test suites achieve at least 90% line or statement coverage per service or library
- Exclude generated code, third-party code, tooling boilerplate, and anything explicitly excluded in this repository
  from test coverage calculations
- Check that DataFrame definitions in both Python and Rust match expected schemas defined in `pandera` resources
- When adding `ValueError` exceptions, create a separate variable called `message` to hold the error string before raising
- When logging after an exception, use `logger.exception()` to capture stack trace with the `structlog` package
- Structured log messages should be short sentences with sentence case (e.g., "Starting data sync" not "STARTING DATA SYNC")
- When debugging or fixing bugs, check Sentry errors, ECS logs, and Alpaca account status to understand what happened
- After fixing a bug, create a git commit with a detailed summary of the root cause and fix in the commit message
- When creating GitHub issues or pull requests, use the templates provided in the `.github/` directory
- Only use labels already available on the GitHub repository for issues and pull requests
- When naming branches, use an all-lowercase, hyphenated, and concise summary of the work being done
- `tools/` folder contains development utilities and scripts
- `applications/` folder contains deployable services and training workflows
- `libraries/` folder contains shared code resources
- `infrastructure/` folder contains Pulumi infrastructure as code
- See `README.md` "Principles" section for developer philosophy

## Ralph Simple Workflow

Ralph is an autonomous development loop for implementing GitHub issue specs.

### Commands

- `mask ralph simple setup` - Create required labels (run once before first use)
- `mask ralph simple spec [issue_number]` - Interactive spec refinement (creates new issue if no number provided)
- `mask ralph simple ready <issue_number>` - Mark a spec as ready for implementation
- `mask ralph simple loop <issue_number>` - Run autonomous loop on a ready spec
- `mask ralph simple backlog` - Review open issues for duplicates, overlaps, and implementation status
- `mask ralph simple pull-request [pull_request_number]` - Process pull request review feedback interactively

### Labels

**Status labels:**

- `in-refinement` - Spec being built or discussed
- `ready` - Spec complete, ready for implementation
- `in-progress` - Work actively in progress
- `attention-needed` - Blocked or needs human intervention
- `backlog-review` - Backlog review tracking issue

**Actor label:**

- `ralph` - Ralph is actively working on this (remove to hand off to human)

### Workflow

1. Create or refine spec: `mask ralph simple spec` or `mask ralph simple spec <issue_number>`
2. When spec is complete, mark as ready: `mask ralph simple ready <issue_number>`
3. Run autonomous loop: `mask ralph simple loop <issue_number>`
4. Loop assigns the issue and resulting pull request to the current GitHub user
5. Loop creates pull request with `Closes #<issue_number>` on completion
6. Pull request merge auto-closes issue

### Context Rotation

- Complete logically related requirements together (same files, same concepts)
- Exit after meaningful progress to allow fresh context on next iteration
- Judgment factors: relatedness, complexity, context size, dependencies

### Completion Signal

Output `<promise>COMPLETE</promise>` when all requirement checkboxes are checked to signal task completion.

### Commit as Verification

After implementing requirements, ALWAYS attempt `git commit`. The commit triggers pre-commit hooks which
run all tests/linting. This IS the verification step:

- If commit fails → fix issues and retry
- If commit succeeds → requirement is verified, check it off in issue
- Do not skip this step or run tests separately

### Ralph Learnings

Document failure patterns here after Ralph loops to prevent recurrence. Periodically compact this section
by merging similar learnings and removing entries that have been incorporated into the workflow or specs above.

#### 2026-01-26: #723 (spec: commit-as-verification not explicit)

**Issue:** Loop implemented requirements but didn't attempt git commit to verify.

**Root cause:** Spec said "commit is the verification gate" but didn't explicitly say to always attempt commit after implementing.

**Fix:** Added explicit "Commit-as-Verification" section requiring commit attempt after every implementation.


## Ralph Marketplace Workflow

The marketplace is an advanced Ralph workflow where multiple bidders compete to provide the best solution.

### Architecture

**Actors:**
- **Broker** - Orchestrates competition, evaluates proposals, implements winner
- **Bidders (3)** - Submit lightweight proposals, compete for selection. Each bidder has deep expertise across languages (Rust, Python), tools (Axum, FastAPI, Polars), infrastructure (Pulumi, AWS), and risk assessment.

**Agent Definitions:**
- Agent system prompts in `.claude/agents/`
- Runtime state in `.ralph/`
- Orchestration code in `tools/ralph_marketplace_*.py`

### Commands

- `mask ralph marketplace setup` - Initialize marketplace state and bidder configurations
- `mask ralph marketplace spec [issue_number]` - Interactive spec refinement (creates new issue if no number provided)
- `mask ralph marketplace ready <issue_number>` - Mark a spec as ready for implementation
- `mask ralph marketplace loop <issue_number>` - Run marketplace competition on a ready spec
- `mask ralph marketplace backlog` - Review open issues for duplicates, overlaps, and implementation status
- `mask ralph marketplace pull-request [pull_request_number]` - Process pull request review feedback interactively
- `mask ralph marketplace status` - Show bidder weights, efficiency, recent rounds
- `mask ralph marketplace reset` - Reset bidder weights to equal (erases learning history)

### Workflow

1. Issue marked "ready" → `mask ralph marketplace loop <issue_number>`
2. Broker spawned, reads spec, extracts requirements
3. Broker spawns 3 bidders in parallel (identities hidden)
4. Bidders apply domain expertise directly, submit lightweight proposals
5. Broker scores proposals on 5 dimensions (spec alignment, technical quality, innovation, risk, efficiency)
6. Broker ranks proposals, selects top scorer (tie-break: earlier timestamp wins)
7. Broker implements ONLY top proposal
8. Broker runs comprehensive checks (format, lint, type-check, dead-code, complex, tests individually)
9. **Success:** Update weights (+), check completeness or rotate context after logical groupings
10. **Failure:** Replan round (all bidders see failure, submit new proposals)
11. Repeat until complete or max iterations → pull request creation or attention-needed

### Budget Model

**Fixed pool with efficiency rewards:**
- Total pool = 10 iterations × number of bidders (default: 30)
- Allocation = (bidder_weight × bidder_efficiency) / sum(all bidder scores) × total_pool
- Zero-sum competition: high performers take budget from low performers
- Mathematical guarantee: allocations always sum to exactly total_pool

### Weight Updates

Immediate updates after each round:

**Initial Round:**
- Ranked #1, implementation succeeds: **+0.10**
- Ranked #1, implementation fails: **-0.15**
- Ranked #2+, tried after #1 failed, succeeds: **+0.08**
- Ranked #2+, tried after #1 failed, also fails: **-0.18**
- Ranked but not tried (another succeeded): **-0.02**

**Replan Round:**
- New proposal succeeds: **+0.12** (bonus for learning from failure)
- Failed again: **-0.20** (heavy penalty)
- Resubmitted same proposal: **-0.05** (not adapting)

**Accuracy Bonus:**
- If absolute difference between proposal and implementation score ≤ 0.15: **+0.05** bonus
- Rewards accurate prediction (applies only to success outcomes)

**Constraints:**
- Weights normalized to sum to 1.0 after each update
- Min weight: 0.05 (maintains diversity)
- Max weight: 0.60 (prevents monopoly)
- Constraints enforced iteratively to maintain normalized sum

### Scoring Dimensions

Proposals and implementations scored on 5 unified dimensions:

1. **Spec Alignment (32%)** - Checkbox coverage, component coverage, implicit requirements
2. **Technical Quality (22%)** - Pattern conformance, code quality checks pass
3. **Innovation (15%)** - Novel approach, elegance, simplicity
4. **Risk (21%)** - Files affected, breaking changes, test coverage
5. **Efficiency (10%)** - Estimated vs. actual complexity, diff size

### State Management

**Append-only event log pattern:**
- Events stored in `.ralph/events/` as immutable JSON files
- State computed from events (cached in `.ralph/marketplace.json`)
- Handles concurrency: different branches add different events, merge cleanly
- Source of truth: event log (tracked in git)
- Cache: regenerated automatically when stale (gitignored)

**Event schema:**

```json
{
  "timestamp": "2026-01-29T10:15:00Z",
  "issue_number": 123,
  "bot_id": "bidder_2",
  "outcome": "success",
  "proposal_score": 0.87,
  "implementation_score": 0.85,
  "accuracy": 0.98,
  "weight_delta": 0.15,
  "iteration_count": 3,
  "metrics": { ... }
}
```

### Replan Rounds

Triggered when top proposal's implementation fails:

1. Post failure context to all bidders (failed proposal, error details)
2. Failed bidder MUST submit new proposal (cannot resubmit)
3. Other bidders CAN resubmit or submit new proposals
4. Return to evaluation phase with new proposals
5. If replan fails → human intervention (attention-needed label)

### Learning Over Time

**Efficiency tracking:**
- Efficiency = implementations_succeeded / (succeeded + failed)
- High efficiency → larger budget allocation
- Low efficiency → reduced budget

**Weight evolution:**
- Successful bidders gain weight over time
- Failed predictions lose weight
- Accurate predictions get bonus

**Long-term outcome:**
- Bidders specialize based on success patterns
- High performers dominate budget allocation
- Maintains minimum diversity (5% min weight)

### Future Enhancements

See `proposal_followups.md` for detailed future considerations:
- Meta-broker for dynamic weight tuning (Phase 2)
- Post-merge health tracking (Phase 3)
- Bidder hobbies and token rewards (Phase 4)
- External system access for bidders (Phase 5)
- Multi-user concurrency improvements (Phase 6)

### Comparison to Standard Ralph Loop

**Standard Ralph (`mask ralph simple loop`):**
- Single agent implements entire issue
- Iterative: plan → implement → commit → repeat
- Max 10 iterations
- Context rotation after logical groupings
- No competition, no learning

**Marketplace Ralph (`mask ralph marketplace loop`):**
- 3 bidders compete with proposals
- Broker selects and implements best proposal
- Budget allocated by weight × efficiency
- Bidders learn and improve over time
- Zero-sum competition for resources
- Context rotation after logical groupings

**When to use:**
- **Standard:** Simple issues, single approach obvious, quick iteration
- **Marketplace:** Complex issues, multiple approaches possible, quality-critical, learning important
