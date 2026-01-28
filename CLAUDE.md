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
- Use Polars for [Python](https://docs.pola.rs/api/python/stable/reference/index.html) and [Rust](https://docs.rs/polars/latest/polars/) dataframes
- Use `typing` module `cast` function for `tinygrad` method outputs when necessary with union types
- Write `pytest` functions for Python tests
- Ensure Rust and Python automated test suites achieve at least 90% line or statement coverage per service or library
- Exclude generated code, third‑party code, tooling boilerplate, and anything explicitly excluded in this repository from test coverage calculations
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
- `mask ralph pr [--pr <number>]` - Process PR review feedback interactively

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
4. Loop assigns the issue and resulting PR to the current GitHub user
5. Loop creates PR with `Closes #<issue_number>` on completion
6. PR merge auto-closes issue

### Context Rotation

- Complete logically related requirements together (same files, same concepts)
- Exit after meaningful progress to allow fresh context on next iteration
- Judgment factors: relatedness, complexity, context size, dependencies

### Completion Signal

Output `<promise>COMPLETE</promise>` when all requirement checkboxes are checked to signal task completion.

### Commit as Verification

After implementing requirements, ALWAYS attempt `git commit`. The commit triggers pre-commit hooks which run all tests/linting. This IS the verification step:
- If commit fails → fix issues and retry
- If commit succeeds → requirement is verified, check it off in issue
- Do not skip this step or run tests separately

### Ralph Learnings

Document failure patterns here after Ralph loops to prevent recurrence. Periodically compact this section by merging similar learnings and removing entries that have been incorporated into the workflow or specs above.

#### 2026-01-26: #723 (spec: commit-as-verification not explicit)

**Issue:** Loop implemented requirements but didn't attempt git commit to verify.

**Root cause:** Spec said "commit is the verification gate" but didn't explicitly say to always attempt commit after implementing.

**Fix:** Added explicit "Commit-as-Verification" section requiring commit attempt after every implementation.
