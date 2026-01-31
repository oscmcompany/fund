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

## Ralph Workflow

Ralph is an autonomous development loop for implementing GitHub issue specs.

### Commands

- `mask ralph setup` - Create required labels (run once before first use)
- `mask ralph spec [issue_number]` - Interactive spec refinement (creates new issue if no number provided)
- `mask ralph ready <issue_number>` - Mark a spec as ready for implementation
- `mask ralph loop <issue_number>` - Run autonomous loop on a ready spec
- `mask ralph backlog` - Review open issues for duplicates, overlaps, and implementation status
- `mask ralph pull-request [pull_request_number]` - Process pull request review feedback interactively

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

1. Create or refine spec: `mask ralph spec` or `mask ralph spec <issue_number>`
2. When spec is complete, mark as ready: `mask ralph ready <issue_number>`
3. Run autonomous loop: `mask ralph loop <issue_number>`
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


## Ralph Marketplace

The marketplace is an advanced Ralph workflow where multiple smart bots compete to provide the best solution.

### Architecture

**Actors:**
- **Arbiter** - Orchestrates competition, evaluates proposals, implements winner
- **Smart Bots (3)** - Submit lightweight proposals, compete for selection
- **Dumb Bots** - Specialists consulted by smart bots (Rust, Python, Infrastructure, Risk, Codebase Explorer)

**Agent Definitions:**
- Agent system prompts in `.claude/agents/`
- Specialist bots in `.claude/agents/ralph_specialists/`
- Runtime state in `.ralph/`
- Orchestration code in `tools/ralph_marketplace_*.py`

### Commands

- `mask ralph marketplace setup` - Initialize marketplace state and bot configurations
- `mask ralph marketplace loop <issue_number>` - Run marketplace competition on a ready spec
- `mask ralph marketplace status` - Show bot weights, efficiency, recent rounds
- `mask ralph marketplace reset` - Reset bot weights to equal (erases learning history)

### Workflow

1. Issue marked "ready" → `mask ralph marketplace loop <issue_number>`
2. Arbiter spawned, reads spec, extracts requirements
3. Arbiter spawns 3 smart bots in parallel (identities hidden)
4. Smart bots consult specialist dumb bots, submit lightweight proposals
5. Arbiter scores proposals on 6 dimensions (spec alignment, technical quality, innovation, risk, efficiency, specialist validation)
6. Arbiter ranks proposals, selects top scorer (tie-break: earlier timestamp wins)
7. Arbiter implements ONLY top proposal
8. Arbiter runs comprehensive checks (format, lint, type-check, dead-code, tests individually)
9. **Success:** Update weights (+), check completeness, continue or finish
10. **Failure:** Replan round (all bots see failure, submit new proposals)
11. Repeat until complete or max iterations → PR creation or attention-needed

### Budget Model

**Fixed pool with efficiency rewards:**
- Total pool = 10 iterations × number of bots (default: 30)
- Allocation = (bot_weight × bot_efficiency) / sum(all bot scores) × total_pool
- Zero-sum competition: high performers take budget from low performers
- Mathematical guarantee: allocations always sum to exactly total_pool

### Weight Updates

Immediate updates after each round:

**Initial Round:**
- Ranked #1, implementation succeeds: **+0.10**
- Ranked #1, implementation fails: **-0.15**
- Ranked #2+, tried after #1 failed, succeeds: **+0.08**
- Ranked but not tried (another succeeded): **-0.02**

**Replan Round:**
- New proposal succeeds: **+0.12** (bonus for learning from failure)
- Failed again: **-0.20** (heavy penalty)
- Resubmitted same proposal: **-0.05** (not adapting)

**Accuracy Bonus:**
- If proposal score matches implementation score (±0.15): **+0.05** bonus
- Rewards accurate prediction

**Constraints:**
- Weights normalized to sum to 1.0 after each update
- Min weight: 0.05 (maintains diversity)
- Max weight: 0.60 (prevents monopoly)

### Scoring Dimensions

Proposals and implementations scored on 6 unified dimensions:

1. **Spec Alignment (30%)** - Checkbox coverage, component coverage, implicit requirements
2. **Technical Quality (20%)** - Pattern conformance, code quality checks pass
3. **Innovation (15%)** - Novel approach, elegance, simplicity
4. **Risk (20%)** - Files affected, breaking changes, test coverage
5. **Efficiency (10%)** - Estimated vs. actual complexity, diff size
6. **Specialist Validation (5%)** - Quality of specialist consultations

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
  "bot_id": "smart_bot_2",
  "outcome": "success",
  "proposal_score": 0.87,
  "implementation_score": 0.85,
  "accuracy": 0.98,
  "weight_delta": 0.15,
  "iteration_count": 3,
  "metrics": { ... }
}
```

### Specialist Bots

**Rust Specialist:**
- Idiomatic Rust patterns, Axum framework, Polars usage
- Consult for: Rust implementations, error handling, middleware patterns

**Python Specialist:**
- Python 3.12.10, FastAPI, type hints, Polars, pytest
- Consult for: Python implementations, endpoint patterns, testing strategies

**Infrastructure Specialist:**
- Pulumi, AWS (ECS, ECR, S3, IAM), Docker, deployment processes
- Consult for: Infrastructure impacts, deployment requirements, IAM permissions

**Risk Specialist:**
- Security (OWASP Top 10), test coverage, failure modes, breaking changes
- Consult for: Security implications, required tests, risk assessment

**Codebase Explorer:**
- Finding files, searching patterns, understanding structure
- Consult for: Existing implementations, pattern discovery, dependencies

### Replan Rounds

Triggered when top proposal's implementation fails:

1. Post failure context to all smart bots (failed proposal, error details)
2. Failed bot MUST submit new proposal (cannot resubmit)
3. Other bots CAN resubmit or submit new proposals
4. Return to evaluation phase with new proposals
5. If replan fails → human intervention (attention-needed label)

### Learning Over Time

**Efficiency tracking:**
- Efficiency = implementations_succeeded / (succeeded + failed)
- High efficiency → larger budget allocation
- Low efficiency → reduced budget

**Weight evolution:**
- Successful bots gain weight over time
- Failed predictions lose weight
- Accurate predictions get bonus

**Long-term outcome:**
- Bots specialize based on success patterns
- High performers dominate budget allocation
- Maintains minimum diversity (5% min weight)

### Future Enhancements

See `proposal_followups.md` for detailed future considerations:
- Meta-arbiter for dynamic weight tuning (Phase 2)
- Post-merge health tracking (Phase 3)
- Smart bot hobbies and token rewards (Phase 4)
- External system access for dumb bots (Phase 5)
- Multi-user concurrency improvements (Phase 6)

### Comparison to Standard Ralph Loop

**Standard Ralph (`mask ralph loop`):**
- Single agent implements entire issue
- Iterative: plan → implement → commit → repeat
- Max 10 iterations
- Context rotation after logical groupings
- No competition, no learning

**Marketplace Ralph (`mask ralph marketplace loop`):**
- 3 smart bots compete with proposals
- Arbiter selects and implements best proposal
- Budget allocated by weight × efficiency
- Bots learn and improve over time
- Zero-sum competition for resources

**When to use:**
- **Standard:** Simple issues, single approach obvious, quick iteration
- **Marketplace:** Complex issues, multiple approaches possible, quality-critical, learning important
