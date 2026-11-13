@THESIS.md

# claude

> Agentic tooling context

## Notes

This is a collection of guidelines and references.

- Every host runs on AWS EC2: the `archiver` and `trader` sync to `master` and build their binary on every boot,
  while the `researcher` has no schedule and builds nothing from `master` — `host/researcher` holds it, pushes the
  laptop's working tree to it and builds and runs one study there
- Secrets are managed via [secretspec](https://secretspec.dev/) with the `awsps` provider, stored in AWS
  Systems Manager Parameter Store as standard-tier `SecureString` parameters at `/secretspec/{project}/{profile}/{key}`
- `src/common/` holds the crate's shared pure code as `module.rs` beside `module/`; its modules and the pure
  `decide`/`solve` take no clients, are not `async`, read no clock or randomness, and import no `tokio`, `reqwest`, `aws_sdk_*`,
  `std::fs` or `std::net`, and name only allowlisted crates, which `tests/test_purity.rs` enforces
- Plans live in `.scratchpad/`, local and deliberately untracked (it is gitignored because plans carry private strategy
  and capital details); the user names the plan a conversation works from
- Machinery is public and state is private: playbooks, journals, vendor data, fitted weights, capital and plans stay
  out of git, and fixtures are synthetic because vendor licences forbid redistributing their data;
  `checks/private-files` refuses only the patterns it names, so a new private format needs adding there
- Read the `experiments` and `verdicts` views (`start-duckdb`) before starting a study to see what has already been
  tried and concluded, under each profile or with `SET VARIABLE profile_bucket` set to the other bucket, and journal
  every experiment through a `Study`; the catalog is a log of the work and gates none of it
- Studies are throwaway binaries in the gitignored `studies/src/bin/`, run with `cargo run -p studies --bin <name>`,
  or scripts in the gitignored `studies/jobs/`, and a `Study` journals into the gitignored `studies/journal/`, which
  `finish` ships to the profile bucket with each file written to `Study::notes`, kept under the run's source
- Introduce new dependencies only after approval
- See `README.md` "Principles" section for developer philosophy
- Docstrings are one sentence on what the item is for, plus at most two more for a caveat, invariant, or
  assumption a caller cannot read off the signature; module documentation is three lines maximum, and an
  in-line comment is one line where a step is genuinely surprising, with two the hard ceiling
- A docstring or comment claims only what the code enforces — a promise that two sites agree, with nothing
  making them agree, reads as a bug
- Rationale belongs in the commit message rather than the source — why this approach and not the obvious
  one, what broke before, what the alternative would cost; the source states the constraint and the commit
  argues it
- Name things in our own vocabulary rather than a vendor's, and keep sibling names consistent (`Bar` beside
  `BarInterval`, not `Ohlc`)
- Spell identifiers out fully in code (variables, functions, fields, modules), metric and domain acronyms included:
  `dataframe` not `df`, `message` not `msg`, `quantity` not `qty`, `index` not `idx`, `value` not `val`, `column`
  not `col`, `volatility` not `vol`, `root_mean_squared_error` not `rmse`, `mean_absolute_error` not `mae`,
  `continuous_ranked_probability_score` not `crps`, and `end_of_day` not `eod`; apply it to new code and to
  identifiers you touch
- Keep only universally-understood acronyms as-is, cased as ordinary words per language convention: formats,
  protocols, and identity (`csv`, `json`, `sql`, `http`, `url`, `uri`, `io`, `id`, `uuid`, `api`) and established
  domain or proper-noun terms (`aws`, `utc`, `etf`, `ohlcv`); never rename fixed external identifiers (the `awsps`
  secretspec provider, environment variables and secret keys, library import aliases, linter directives, external
  library fields and parameters), and treat already-shipped schema identifiers and stored values as fixed, changed
  only via an explicit migration
- Three EC2 hosts do the work and each has one name, used for the instance, its tags, its scripts, and the
  `producer=` partition its journals and logs are written under: `archiver` collects data, `trader` executes, and
  `researcher` runs experiments and model training; `producer=operator` marks a change a person makes by hand (a
  playbook upload), which no host makes and which never names an instance
- The host name is the anchor and a binary's name is never a host name — the `archiver` runs `archive_nightly` and
  the `researcher` runs whatever study `host/researcher` sends it, so the binary appears in the finer key
  (`service=` for a log) rather than in the host's name
- `fund-role` and `fund-profile` are different axes and neither absorbs the other: the role is which kind of host
  (`archiver`), the profile is which environment (secretspec's `production`, `development/<name>`), and
  provisioning filters on both so a second environment cannot adopt the first's box; "profile" means an
  environment and nothing else, so never give it a second sense
- Structured log messages should be short sentences with sentence case (e.g., "Starting data sync" not
  "STARTING DATA SYNC")
- Encode domain constraints in the type system: use enums with per-variant data to make invalid states
  unrepresentable at compile time rather than checking validity at runtime
- Derive a string-backed enum's names with strum (`Display`, `EnumString`, `IntoStaticStr`, `EnumIter`)
  from its first line, with a round-trip test wherever serde's casing must agree
- Wrap primitive types in tuple structs to enforce domain type safety (e.g., `struct Price(f64)`); never
  accept a raw `f64` or `String` where a specific domain value is required
- Money and share counts are integers so every sum is exact: a `Price` counts millionths of a dollar
  (`PRICE_SCALE`, 1,000,000), `Shares` counts millionths of a share (`SHARE_SCALE`, 1,000,000), and a `DollarVolume`
  is their product; a float appears only to present a value (a log line, a report) and is never combined
- Prefer validated constructors with private fields over public struct literals — a value in scope should
  be proof of its own validity, not a candidate for re-checking downstream
- Use `match` (not `if let` chains) when handling enum variants, and never write a wildcard arm — the
  compiler flags every site that needs attention when a variant is added, but only if no arm swallows the rest
- Model state machines with two enums (states and transitions) matched as a tuple:
  `match (current_state, transition) { ... }` — keeps business logic exhaustive and legible
- Dispatch statically: take a generic or `impl Trait`, and when the type varies at runtime reach for an enum over
  the closed set; use `dyn` only where a type must be erased at runtime: an open set, a stored callback, or a
  parameter of a closure, which cannot itself be generic
- `Option` means unmeasurable, never zero — zero is a measurement and absence is not
- An absence carries its cause: when something is missing or refused, the type records why, along with the
  number that produced the refusal
- Two fields that must agree will eventually not; if one is a function of the other, derive it rather than
  storing both
- Write a comparison in one place — four call sites checking against the same constant are four rules that
  agree only by coincidence
- Design structs to be flat and normalized: each struct represents one concept with only its own fields;
  avoid deep nesting or struct embedding as a substitute for inheritance
- Prefer transformations that compose — a mapping that preserves structure, an operation with an identity and an
  associative combine, a round trip that returns the original — and test those laws with proptest rather than
  hand-picked examples
- Time has exactly two kinds and they never mix: an instant is a moment on the timeline and is always UTC
  (`DateTime<Utc>`, and a UTC timestamp wherever it is stored), while a session is a trading day, always an
  `America/New_York` calendar date held as a `SessionDate` and never a bare `NaiveDate`, because that is the
  exchange's day rather than a display preference; cross between them with `SessionDate::at` and
  `.midnight()`/`.bounds()`, never via `Utc::now().date_naive()` or a hardcoded offset, and write `Eastern` or
  `America/New_York`, never `EST` or `EDT`, which each name only half the year
- `SessionDate` guarantees the timezone and nothing more: `SessionDate::from_date` is for dates already in Eastern
  terms, so transport modules keep `NaiveDate` and the wrap happens at the domain boundary; `.midnight()` is for
  genuinely date-only values and never for a daily bar, which is stamped at the 16:00 Eastern close; weekends and
  holidays are representable and `plus_calendar_days` will land on them, so only
  `TradingCalendar::is_trading_day` answers whether a date trades; elapsed durations and timeouts carry no zone,
  so measure them on a monotonic clock (`tokio::time::Instant`); and schedule host starts and stops in
  `America/New_York` outside the 01:00-02:59 a transition skips or repeats, with the run itself gating on the
  published calendar
- Judge new code on whether a mutation to it fails a test rather than on the coverage line rate alone; CI does
  not run mutations, so run `mutate-rust <diff>` locally over a slice's diff when it adds logic worth guarding,
  and expect a function that only wraps a network call to survive
- When fixing a bug, write tests that reproduce the bug before fixing it, then verify the tests pass after
  the fix
- Pin test expectations to literals, never to the constant or list the test is checking — an expectation
  derived from the value under test moves with it and can never fail
- Assert the key set before comparing values — "for every element of the result" is vacuously true when the
  result is empty
- Never build a fixture from the belief under test; probe the live payload before typing a struct against
  it, because a mock only proves the parser agrees with the fixture
- Test the seam between components — two units that are each correct and each individually tested can still
  be jointly broken
- Assert mathematical properties the output must have as an object (mass integrates to one, no negative
  probabilities, a mean that lands on the forward) rather than agreement with a fixture
- Prove a test can see before trusting it: break the code a new assertion covers, watch the test fail and
  restore it; a measurement's version of the rule, with the rest of the research method, is the `study` skill,
  loaded whenever a study is designed, written, run or read
- A tooling trap or a recurring review comment that a check could detect is owed a check, not a memory — a
  memory is read after the trap fires; only a hazard of a typed command that no check can observe stays a note
- Buy what gets more expensive with time and rent what gets cheaper — a lapsing tick subscription and a
  session never captured are purchases, while compute, storage and throughput are rentals
- A data source stays a study's own until a study shows it adds value: fetch it raw (from the provider, or a restored
  archive object) into a gitignored directory or the profile bucket's `scratch/` prefix, clean it as often as the
  study needs, and write nothing to the archive bucket
- Promoting a source makes it permanent in the archive bucket under its own `Key` variant: its raw bytes are kept in
  Deep Archive unless a licence forbids it or they are re-derivable from what we hold, and its parsed form is a fixed
  transform of those bytes by code in the repo, so it can be rebuilt from raw at any time; every archive family has
  exactly one writer, the `archiver`, the `researcher` or an `operator`, granted only that family's prefix, and a
  development profile never writes the archive bucket
- At promotion, record the source's facts (recurring or fixed, perishable or still on sale, whether rows carry a
  publication time for `known_at`, and its consumer) and let them set the rest: schedule a fetch only for a recurring
  source that is perishable or read at decision time, parse at write time only what execution reads, and parse at
  read time otherwise; the consumer is execution (risk, the guard, fills), which must be exact and typed,
  decision-time features, which must be available live and point-in-time, or training-only inputs, which shape a
  model's parameters but are never read when deciding, and which must be reproducible but never live
- Before building, ask what the result will be used for and check the request serves it — the problem as stated is
  rarely the real problem
- When moving a process onto a new tool, define equivalence by what its consumer reads, then redesign the process for
  the tool rather than reproduce the old one step by step
- For work larger than one slice, agree the types, signatures and module layout before writing any bodies
- Do not introduce abstractions for single-use code
- Prove changes work before marking tasks complete — run targeted `cargo test` while iterating and
  `devenv tasks run checks:all` once before each push, compare behavior, demonstrate correctness; `devenv tasks run`
  executes prefix groups, so `checks:rust` runs every `checks:rust:*` subtask, and the pre-commit hook runs
  `checks:rust` and each lint only when a commit stages a file it covers, so `checks:all` is the whole-tree pass
- The ignored `live_` tests reach real providers, the S3 buckets or the Alpaca paper account and never run in CI;
  run one deliberately when a change touches its seam, confirming `ALPACA_IS_PAPER=true` before any that trades
- Verify against real data before reporting done, and say which route was used: `secretspec run -- curl`
  against a provider, DuckDB over the S3 parquet, or a researcher run read back from `/var/log/fund/`, whose
  structured and error logs are also the first stop when debugging; when only fixtures were exercised, say so
  plainly, and ask to be pointed at real data rather than inferring
- A bug-fix commit message states the root cause and the fix rather than restating the diff
- When creating GitHub issues or pull requests, use the templates in the `.github/` directory and follow
  commented instructions
- Keep a pull request to roughly 250–500 lines of production code, excluding tests and lockfiles; when a plan
  or a diff grows past that, propose splitting it into stacked slices
- Before opening a pull request or pushing to one, spawn the `reviewer` subagent over the diff and let it fix
  what it finds, without waiting on the user, then record its fixes in the commit message that carries them
- When reviewers disagree (the `reviewer` subagent, Greptile, another bot), the pull request's author judges each
  finding on its merits, never by majority or by splitting the difference
- When naming branches, use an all-lowercase, hyphenated, and concise summary of the work being done
- Only use existing repository labels for GitHub issues and pull requests
- Do not use emojis in commit messages, GitHub issues, or pull requests - maintain a professional tone
- When possible, use GitHub's GraphQL API directly for scripts and tools where token efficiency matters
