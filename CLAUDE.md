# claude

> Agentic tooling context

## Notes

This is a collection of guidelines and references.

- Rust is the primary project language
- [devenv](https://devenv.sh/) manages the development environment, tasks, and local services
- Services run on a single exe.dev VM via `devenv --profile applications up` in both dev and production
- Secrets are managed via [secretspec](https://secretspec.dev/) with the `awssm` provider
- Rust code follows Cargo workspace conventions
- AWS S3 is used for blob storage
- AWS Secrets Manager stores secrets in secretspec format (`secretspec/{project}/{profile}/{key}`)
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
- Never rename fixed external identifiers: devenv profile names (`applications`, `ml`), the `awssm` secretspec
  provider, the `tide` model and package name, environment variables and secret keys (e.g.
  `DATABASE_URL`, `ALPACA_API_KEY_ID`), library import aliases (`np`, `pl`, `pa`), linter directives
  (`noqa`), and external library fields and parameters (e.g. the Alpaca/Massive `vw` deserialization
  field, datetime `tz=`/`tzinfo`)
- Apply the spell-it-out rule to new code, to identifiers you touch, and to dedicated cleanup passes;
  already-shipped database and schema identifiers (index, constraint, and column names) and stored
  values are effectively fixed and change only via an explicit migration
- Follow Rust recommended casing conventions
- Introduce new dependencies only after approval
- Use Polars for [Rust](https://docs.rs/polars/latest/polars/) dataframes
- Ensure Rust automated test suites achieve at least 75% line or statement coverage
- Exclude generated code, third-party code, tooling boilerplate, and anything explicitly excluded in this repository
  from test coverage calculations
- Structured log messages should be short sentences with sentence case (e.g., "Starting data sync" not "STARTING DATA SYNC")
- When debugging or fixing bugs, check structured logs and error log files in `/var/log/fund/` to understand what happened
- After fixing a bug, create a Git commit with a detailed summary of the root cause and fix in the commit message
- When creating GitHub issues or pull requests, use the templates in the `.github/` directory and follow commented instructions
- When naming branches, use an all-lowercase, hyphenated, and concise summary of the work being done
- `models/` folder contains model definitions and training code and `src/` contains application code
- See `README.md` "Principles" section for developer philosophy
- If something goes wrong during a task, stop immediately and re-plan rather than continuing
- Use subagents to keep main context window clean and offload research, exploration, and analysis work
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
- `devenv tasks run` supports prefix group execution: `checks:rust` runs all `checks:rust:*` subtasks
- Prefer validated constructors with private fields over public struct literals — a value in scope should be proof of its
  own validity, not a candidate for re-checking downstream
- `schema.sql` is the single source of truth for the database schema; all DDL must use idempotent forms
  (`CREATE TABLE IF NOT EXISTS`, `CREATE INDEX IF NOT EXISTS`, `CREATE OR REPLACE FUNCTION`,
  `DO` block checking `cron.job` for existence before calling `cron.schedule*(...)`, etc.)
  so the file can be safely re-run against any populated database — never add migration-style
  `ALTER TABLE`, `DROP TABLE`, or bare `UPDATE` blocks
- Utilize category theory when designing data transformations and model architectures - functors, monads, and natural
  transformations; this leads to more composable, reusable, and maintainable code
- Only use existing repository labels for GitHub issues and pull requests
- After any change to `schema.sql` or a `query!` macro, run `cargo sqlx prepare -- --all-features` to
  regenerate the `.sqlx/` offline cache; if Postgres is not already running, start it first with
  `devenv up -d --no-tui` then poll `pg_isready` until it accepts connections; use
  `cargo sqlx prepare --check -- --all-features` to verify without updating the cache; the
  `dashboard_service` uses raw `sqlx::query()` (no macros) and has no offline cache entries
- Encode domain constraints in the type system: use enums with per-variant data to make invalid states
  unrepresentable at compile time rather than checking validity at runtime
- Use `match` (not `if let` chains) when handling enum variants — exhaustive matching ensures every variant is
  handled and the compiler flags missing cases when variants change
- Wrap primitive types in tuple structs to enforce domain type safety (e.g., `struct Price(f64)`); never accept
  a raw `f64` or `String` where a specific domain value is required
- Model state machines with two enums (states and transitions) matched as a tuple:
  `match (current_state, transition) { ... }` — keeps business logic exhaustive and legible
- Design structs to be flat and normalized: each struct represents one concept with only its own fields;
  avoid deep nesting or struct embedding as a substitute for inheritance
