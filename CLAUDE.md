# claude

> Agentic tooling context

## Notes

This is a collection of guidelines and references.

- Rust and Python are the primary project languages
- [devenv](https://devenv.sh/) manages the development environment, tasks, and local services
- Services run on a single exe.dev VM via `devenv --profile apps up` in both dev and production
- Secrets are managed via [secretspec](https://secretspec.dev/) with the `awssm` provider
- Python code follows [uv](https://github.com/astral-sh/uv) workspace conventions
- Rust code follows Cargo workspace conventions
- AWS S3 is used for blob storage (data bucket and model artifacts bucket)
- AWS Secrets Manager stores secrets in secretspec format (`secretspec/{project}/{profile}/{key}`)
- Models are primarily built using [tinygrad](https://docs.tinygrad.org/)
- Use `devenv tasks run checks:python` for comprehensive Python checks
- Use `devenv tasks run checks:rust` for comprehensive Rust checks
- Add in-line code comments only where necessary for clarity; include language-appropriate docstrings for functions and
  modules
- Spell identifiers out fully in code (variables, functions, fields, modules): prefer `dataframe` over
  `df`, `message` over `msg`, `error_message` over `err_msg`, `quantity` over `qty`, `index` over `idx`,
  `column` over `col`, `value` over `val`, `volatility` over `vol`, `profit_and_loss` over `pnl`,
  `timezone` over `tz`, and `aggregate` over `agg`
- Expand metric and domain acronyms in identifiers too: `root_mean_squared_error` (not `rmse`),
  `mean_absolute_error` (not `mae`), `mean_squared_error` (not `mse`), `r_squared` (not `r2`),
  `continuous_ranked_probability_score` (not `crps`), `end_of_day` (not `eod`), and `postgres` (not `pg`)
- Keep only universally-understood acronyms as-is: formats, protocols, and identity (`csv`, `json`,
  `sql`, `http`, `url`, `uri`, `io`, `id`, `uuid`, `api`) and established domain or proper-noun terms
  (`aws`, `utc`, `etf`, `ohlcv`, `guc`); case acronyms as ordinary words per language convention
  (Rust `HttpClient`/`Uuid`, Python `api_key`/`http_client`), not all-caps runs
- Never rename fixed external identifiers: devenv profile names (`apps`, `ml`), the `awssm` secretspec
  provider, the `tide` model and package name, environment variables and secret keys (e.g.
  `DATABASE_URL`, `ALPACA_API_KEY_ID`), library import aliases (`np`, `pl`, `pa`), linter directives
  (`noqa`), and external library fields and parameters (e.g. the Alpaca/Massive `vw` deserialization
  field, datetime `tz=`/`tzinfo`)
- Apply the spell-it-out rule to new code, to identifiers you touch, and to dedicated cleanup passes;
  already-shipped database and schema identifiers (index, constraint, and column names) and stored
  values are effectively fixed and change only via an explicit migration
- Follow Rust and Python recommended casing conventions
- Strictly use Python version 3.12.10
- Scan and remove unused dependencies from `pyproject.toml` files; move duplicate dependencies into root workspace `pyproject.toml`
- Introduce new dependencies only after approval
- Include type hints on all Python function parameters and return types
- Use Polars for [Python](https://docs.pola.rs/api/python/stable/reference/index.html) and
  [Rust](https://docs.rs/polars/latest/polars/) dataframes
- Use `typing` module `cast` function for `tinygrad` method outputs when necessary with union types
- Write `pytest` functions for Python tests using plain functions, not class-based test organization
- Ensure Rust and Python automated test suites achieve at least 90% line or statement coverage per service or library
- Exclude generated code, third-party code, tooling boilerplate, and anything explicitly excluded in this repository
  from test coverage calculations
- Check that DataFrame definitions in both Python and Rust match expected schemas defined in `pandera` resources
- When adding `ValueError` exceptions, create a separate variable called `message` to hold the error string before raising
- When logging after an exception, use `logger.exception()` to capture stack trace with the `structlog` package
- Structured log messages should be short sentences with sentence case (e.g., "Starting data sync" not "STARTING DATA SYNC")
- When debugging or fixing bugs, check structured logs and error log files in `/var/log/fund/` to understand what happened
- After fixing a bug, create a git commit with a detailed summary of the root cause and fix in the commit message
- When creating GitHub issues or pull requests, use the templates in the `.github/` directory and follow commented instructions
- When naming branches, use an all-lowercase, hyphenated, and concise summary of the work being done
- `tools/` folder contains development utilities and scripts
- `applications/` folder contains deployable services
- `libraries/` folder contains shared code resources
- `models/` folder contains model definitions and training code
- See `README.md` "Principles" section for developer philosophy
- If something goes wrong during a task, stop immediately and re-plan rather than continuing
- Use subagents to keep main context window clean and offload research, exploration, and analysis work
- After user corrections, update `.claude/tasks/lessons.md` with timestamp to prevent repeating mistakes
- Prove changes work before marking tasks complete - run `devenv tasks run` checks, compare behavior, demonstrate correctness
- For non-trivial changes, pause and ask "Is there a more elegant way?" before implementing
- Make every change as simple as possible and impact minimal code
- Find root causes and avoid temporary fixes - maintain high standards
- Changes should only touch what's necessary to avoid introducing bugs
- If uncertainty arises, ask for help or input rather than guessing
- Do not introduce abstractions for single-use code
- Always match existing styles and patterns in the codebase for consistency
- When fixing a bug, write tests that reproduce the bug before fixing it, then verify the tests pass after the fix
- Do not use emojis in commit messages, GitHub issues, or pull requests - maintain a professional tone
- When possible, use GitHub's GraphQL API directly for scripts and tools where token efficiency matters
- When the user indicates pull request review bots have provided feedback, suggest running `/update-pull-request`
- Invoke skills and suggest commands based on conversational context rather than waiting for explicit slash commands
- Guard against division by zero when computing ratios or percentages from DataFrame aggregations
- When Polars `Series.sum()` is used on a potentially empty or all-null series, handle the `None` return case
- `devenv tasks run` supports prefix group execution: `checks:python` runs all `checks:python:*` subtasks
- Prefer validated constructors with private fields over public struct literals — a value in scope should be proof of its
  own validity, not a candidate for re-checking downstream
- Model data flows as typed morphisms: each function that accepts external data and returns an internal type is a
  boundary morphism that enforces validity at the edge of the system
- Place boundary morphisms at every external system boundary (API responses, S3 reads, database rows, WebSocket frames)
  so that untrusted data never reaches domain logic
- Prefer compile-time type errors over runtime errors: use enums and newtype wrappers so that invalid states cannot be
  represented
