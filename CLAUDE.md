# claude

> Agentic tooling context

## Notes

This is a collection of guidelines and references.

- [devenv](https://devenv.sh/) manages the development environment, tasks, and local services
- Services run on a single exe.dev VM via `devenv --profile application up` in both dev and production
- Secrets are managed via [secretspec](https://secretspec.dev/) with the `awssm` provider, stored in AWS
  Secrets Manager as `secretspec/{project}/{profile}/{key}`
- Use `devenv tasks run checks:rust` for comprehensive Rust checks; `devenv tasks run` supports prefix group
  execution, so `checks:rust` runs every `checks:rust:*` subtask
- Introduce new dependencies only after approval
- Use Polars for [Rust](https://docs.rs/polars/latest/polars/) dataframes
- See `README.md` "Principles" section for developer philosophy
- Docstrings are one sentence on what the item is for, plus at most two more for a caveat, invariant, or
  assumption a caller cannot read off the signature; module documentation is three lines maximum, and an
  in-line comment is one line where a step is genuinely surprising, with two the hard ceiling
- Rationale belongs in the commit message rather than the source — why this approach and not the obvious
  one, what broke before, what the alternative would cost; the source states the constraint and the commit
  argues it
- Spell identifiers out fully in code (variables, functions, fields, modules): prefer `dataframe` over
  `df`, `message` over `msg`, `quantity` over `qty`, `index` over `idx`, `value` over `val`, `column` over
  `col`, and `volatility` over `vol`
- Expand metric and domain acronyms in identifiers too: `root_mean_squared_error` (not `rmse`),
  `mean_absolute_error` (not `mae`), `continuous_ranked_probability_score` (not `crps`), and `end_of_day`
  (not `eod`)
- Keep only universally-understood acronyms as-is: formats, protocols, and identity (`csv`, `json`, `sql`,
  `http`, `url`, `uri`, `io`, `id`, `uuid`, `api`) and established domain or proper-noun terms (`aws`,
  `utc`, `etf`, `ohlcv`); case acronyms as ordinary words per language convention
- Never rename fixed external identifiers: devenv profile names (`application`, `trainer`), the `awssm`
  secretspec provider, the `tide` model and package name, environment variables and secret keys, library
  import aliases, linter directives, and external library fields and parameters
- Three hosts do the work and each has one name, used for the instance, its tags, its scripts, and the
  `producer=` partition its journals and logs are written under: `archiver` collects data, `trader`
  executes, and `researcher` runs experiments and model training
- The host name is the anchor and a module name is never a host name — `researcher` runs every
  `laboratory_*` binary and would run training beside them, so the module appears in the finer key
  (`service=` for a log, `experiment_type=` for an experiment) rather than in the host's name
- The devenv profiles still read `application` and `trainer`, which is a recorded mismatch rather than a
  defect: they are renamed to `trader` and `researcher` when the exe.dev dependency is retired, and until
  then the line above governs everything except those two profile names
- `fund-role` and `fund-profile` are different axes and neither absorbs the other: the role is which kind
  of host (`archiver`), the profile is which environment (`production`), and provisioning filters on both
  so a second environment cannot adopt the first's box
- "Profile" already means a role in devenv and an environment in secretspec, so never introduce a third
  sense of it; a record's host partition is `producer=`
- Apply the spell-it-out rule to new code and to identifiers you touch; already-shipped schema identifiers
  and stored values are effectively fixed and change only via an explicit migration
- Always match existing styles and patterns in the codebase for consistency
- Structured log messages should be short sentences with sentence case (e.g., "Starting data sync" not
  "STARTING DATA SYNC")
- Encode domain constraints in the type system: use enums with per-variant data to make invalid states
  unrepresentable at compile time rather than checking validity at runtime
- Wrap primitive types in tuple structs to enforce domain type safety (e.g., `struct Price(f64)`); never
  accept a raw `f64` or `String` where a specific domain value is required
- Prefer validated constructors with private fields over public struct literals — a value in scope should
  be proof of its own validity, not a candidate for re-checking downstream
- Use `match` (not `if let` chains) when handling enum variants, and never write a wildcard arm — the
  compiler flags every site that needs attention when a variant is added, but only if no arm swallows the rest
- Model state machines with two enums (states and transitions) matched as a tuple:
  `match (current_state, transition) { ... }` — keeps business logic exhaustive and legible
- `Option` means unmeasurable, never zero — zero is a measurement and absence is not
- An absence carries its cause: when something is missing or refused, the type records why, along with the
  number that produced the refusal
- Two fields that must agree will eventually not; if one is a function of the other, derive it rather than
  storing both
- Write a comparison in one place — four call sites checking against the same constant are four rules that
  agree only by coincidence
- Design structs to be flat and normalized: each struct represents one concept with only its own fields;
  avoid deep nesting or struct embedding as a substitute for inheritance
- Prefer transformations that compose: a mapping that preserves structure, an operation with an identity
  and an associative combine, and a round trip that returns the original are the shapes worth reaching for
- Time has exactly two kinds and they never mix: an instant is a moment on the timeline and is always UTC
  (`DateTime<Utc>`, `TIMESTAMPTZ`), while a session is a trading day and is always an `America/New_York`
  calendar date — that is what the exchange's day is, not a display preference
- A session is `data::calendar::SessionDate`, never a bare `NaiveDate`: derive one from an instant with
  `SessionDate::at` and convert back with `.midnight()`/`.bounds()`, never via `Utc::now().date_naive()`
  or a hardcoded offset
- `SessionDate::from_date` is for dates already expressed in Eastern terms, so transport modules keep
  `NaiveDate` and the wrap happens at the domain boundary; `.midnight()` is for genuinely date-only values
  and never for a daily bar, which is stamped at the 16:00 Eastern close
- `SessionDate` guarantees the timezone, not tradability: weekends and holidays are representable and
  `plus_calendar_days` will land on them, so only `TradingCalendar::is_trading_day` answers whether a date
  trades — never infer it from the type
- Write `Eastern` or `America/New_York`, never `EST` or `EDT` — each names only half the year; elapsed
  durations and timeouts carry no zone, so measure them on a monotonic clock (`tokio::time::Instant`), and
  schedule trading jobs with a UTC cron expression gated on the Eastern wall clock, a pairing that
  `tests/test_schedules.rs` enforces
- Guard against division by zero when computing ratios or percentages from DataFrame aggregations, and
  handle the `None` that Polars `Series.sum()` returns on an empty or all-null series
- Ensure Rust automated test suites achieve at least 75% line or statement coverage, excluding generated
  code, third-party code, tooling boilerplate, and anything explicitly excluded in this repository;
  coverage is a proxy for the real question, so judge new code on whether a mutation to it fails a test
  rather than on the line rate alone
- When fixing a bug, write tests that reproduce the bug before fixing it, then verify the tests pass after
  the fix
- Pin test expectations to literals, never to the constant or list the test is checking — an expectation
  derived from the value under test moves with it and can never fail
- Prove a new assertion is load-bearing before trusting it: break the code it covers, watch the test fail,
  restore
- Assert the key set before comparing values — "for every element of the result" is vacuously true when the
  result is empty
- Never build a fixture from the belief under test; probe the live payload before typing a struct against
  it, because a mock only proves the parser agrees with the fixture
- Test the seam between components — two units that are each correct and each individually tested can still
  be jointly broken
- Assert mathematical properties the output must have as an object (mass integrates to one, no negative
  probabilities, a mean that lands on the forward) rather than agreement with a fixture
- Before believing a result, actively attempt to produce it from nothing — permute the labels, shuffle the
  target, or feed the pipeline noise, and confirm the effect disappears
- Run the identical pipeline on an input that cannot contain the effect; whatever it reports is the
  machinery's own bias, and every real reading has to clear it
- A control must differ from the treatment in exactly one respect, and it must be able to vary — a control
  that cannot fail is not a control
- A baseline's free choices are fixed by an external specification or swept across a declared range, never
  chosen by us, and every arm carries its own parameters in its name
- Prefer the instrument that can lower your own number — a screen that costs a fifth of the panel and
  changes no answer is a result, and an instrument that can only flatter is not a measurement
- Before trusting a measurement that reports nothing, confirm the instrument reports something when
  something is there
- State the trivial baseline before reading any metric, and quote skill against a named baseline rather
  than a raw score
- Start a study as simply as you can provided the main effects are in, get the insights, then evolve
  it to the fully detailed run — a full-scale simulation disguises the system
- Volume of output is a poor substitute for an intimate feeling for the situation being simulated —
  prefer fewer runs read carefully over many runs summarized
- Unusually low variance across seeds is a warning rather than a quality signal — it means the metric is
  pinned by the shape of the data rather than by anything the model learned
- Magnitude before significance: ask whether the effect could pay for itself at measured cost before asking
  whether it is real
- State a new direction's admission criteria before opening it — a returns bar, a reachable size, an
  underserved gap, and a capability we actually hold — and record which of them it fails, because failing
  three of four is a legitimate answer
- Admit a dataset on the same questions: does it raise the average information, is it re-derivable, and
  does it carry a dimension nothing else does
- Split the sample and test the difference between the halves, never whether both halves point the same way
- Fix every free choice before looking at the result, and record each one with the number it produced
- Scope a kill precisely — say exactly what was refuted and under what conditions, because over-claiming a
  refutation closes doors that are still open and under-claiming reopens ones that are shut
- Correcting an instance does not generalise the lesson; run the check deliberately against each new
  measurement rather than remembering it as a past mistake
- Standard errors assume independent observations, which financial data never is — aggregate to the level
  that actually varies and report effective sample size rather than row count
- Always report the undefined share alongside an estimate; an estimator silently defined on 60% of its
  inputs reads exactly like a complete measurement
- Print the population and the boundary values beside any difference — `1254 / 1254 / 0 / 0` is a proof and
  a bare `0` is not
- Screen on the quantity being paid for rather than a correlate of it, and check a screen's units before
  checking its threshold
- Levels drift and ranks do not — a threshold fixed in absolute units is a moving quantile, so scale
  windows and cutoffs to the distribution's own width
- Screen first, then average: a subset's cost read off the whole distribution's deciles is the wrong
  number, and so is costing a strategy at an average over buckets it does not trade
- Never quantile-bin an unordered category — nominal is not ordinal, and binning one merges unrelated
  groups on the basis of the order they happened to arrive in
- Compare mutual information as a share of the target's entropy rather than in raw bits, and subtract a
  permutation-null baseline before reading the table
- The universe is part of the answer, so journal it beside the result — the set of instruments collected
  and the set measured over are different questions with different answers
- Buy what gets more expensive with time and rent what gets cheaper — a lapsing tick subscription and a
  session never captured are purchases, while compute, storage and throughput are rentals
- Before filtering a provider feed by date, check whether the filter matches the event's own date or the
  provider's record time — Alpaca's activity `date=`/`after=` match record time, so a date-only row dated
  D is returned by a query for D+1
- If something goes wrong during a task, stop immediately and re-plan rather than continuing
- For non-trivial changes, pause and ask "Is there a more elegant way?" before implementing
- Make every change as simple as possible, touching only what is necessary to avoid introducing bugs
- Find root causes and avoid temporary fixes - maintain high standards
- Do not introduce abstractions for single-use code
- Use subagents to keep main context window clean and offload research, exploration, and analysis work
- Invoke skills and suggest commands based on conversational context rather than waiting for explicit slash
  commands
- Prove changes work before marking tasks complete - run `devenv tasks run` checks, compare behavior,
  demonstrate correctness
- Verify against real data before reporting done, and say which route was used: `secretspec run -- curl`
  against a provider, DuckDB over the S3 parquet, or a trainer rehearsal read back from `/var/log/fund/`;
  when only fixtures were exercised, say so plainly, and ask to be pointed at real data rather than
  inferring
- When debugging or fixing bugs, check structured logs and error log files in `/var/log/fund/` to
  understand what happened
- A bug-fix commit message states the root cause and the fix rather than restating the diff
- When creating GitHub issues or pull requests, use the templates in the `.github/` directory and follow
  commented instructions
- When naming branches, use an all-lowercase, hyphenated, and concise summary of the work being done
- Only use existing repository labels for GitHub issues and pull requests
- Do not use emojis in commit messages, GitHub issues, or pull requests - maintain a professional tone
- When possible, use GitHub's GraphQL API directly for scripts and tools where token efficiency matters
