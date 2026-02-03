# claude

> Agentic tooling context

## Notes

This is a collection of guidelines and references.

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
- Folder names under the `applications/` directory should end with `model` for machine learning services
  and end with `manager` for all others
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

## Workflow Orchestration

This is a set of instructions for tasks and projects.

### Specifics

1. Plan Mode Default

- Enter plan mode for any task with more than one step or architectural decisions
- If something goes wrong, stop and re-plan immediately - don't continue working
- Use plan mode for verification steps, not just building steps
- Write detailed specifications upfront to reduce ambiguity

2. Subagent Strategy

- Use subagents liberally to keep main context window clean
- Offload research, exploration, and parallel analysis to subagents
- For complex problems, deploy additional subagents
- One task per subagent for focused execution

3. Self-Improvement Loop

- After any correction from the user: update `.claude/tasks/lessons.md` with the pattern
- Write rules for yourself that prevent the same mistake and include a timestamp when added
- Iterate on these rules until mistake rate drops
- Review rules at session start for relevant project

4. Verification Before Done

- Do not mark a task complete without proving it works
- Compare behavior between the `master` branch and your changes when relevant
- Ask yourself: "Would these changes be approved to merge to `master`?"
- Run `mask development python/rust all` commands, check logs, demonstrate correctness

5. Demand Elegance

- For non-trivial changes: pause and ask "Is there a more elegant way?"
- If a fix feels hacky: "Knowing everything I know now, implement the elegant solution"
- Skip this for simple fixes - don't over-engineer solutions
- Challenge your own work before presenting it

### Task Management

Use `.claude/tasks/todos.md` for general task tracking when working on non-command tasks. For command-specific workflows (like `/update-pull-request`), plan mode handles organization.

When using todos.md:
1. Plan First: Write plan to `.claude/tasks/todos.md` with checkable items
2. Track Progress: Mark items complete as you go
3. Document Results: Add review section to `.claude/tasks/todos.md`
4. Capture Lessons: Update `.claude/tasks/lessons.md` after corrections

Note: Read the file before writing if it already exists to avoid write errors.

### Core Principles

- Simplicity First: Make every change as simple as possible. Impact minimal code.
- No Laziness: Find root causes. No temporary fixes. Senior developer standards.
- Minimal Impact: Changes should only touch what's necessary. Avoid introducing bugs.
