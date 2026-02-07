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
- Write `pytest` functions for Python tests (use plain functions, not class-based test organization)
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
- If something goes wrong during a task, stop immediately and re-plan rather than continuing
- Use subagents to keep main context window clean and offload research, exploration, and analysis work
- After user corrections, update `.claude/tasks/lessons.md` with timestamp to prevent repeating mistakes
- Prove changes work before marking tasks complete - run `mask` checks, compare behavior, demonstrate correctness
- For non-trivial changes, pause and ask "Is there a more elegant way?" before implementing
- Make every change as simple as possible and impact minimal code
- Find root causes and avoid temporary fixes - maintain high standards
- Changes should only touch what's necessary to avoid introducing bugs
