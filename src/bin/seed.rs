//! Seeds and repairs every archive the fund reads from, one subcommand per target.
//!
//! No subcommand writes to both PostgreSQL and S3: the application never reads the archive.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use chrono::{NaiveDate, Utc};
use clap::{Args, Parser, Subcommand, ValueEnum};
use tracing::{error, info, warn};

use fund::common::alpaca::{AlpacaCredentials, DataFeed, MarketDataClient, TradingClient};
use fund::common::database::connect_pool;
use fund::common::flatfiles;
use fund::common::log::init_tracing;
use fund::common::massive::MassiveClient;
use fund::common::types::{
    BarInterval, IntradayCadence, LiquidityFloor, QuoteSummary, SessionDate, Ticker,
};
use fund::data::archive::{self, NameSelection, Scope, SessionSelection};
use fund::data::calendar::TradingCalendar;
use fund::data::{attribution, bars, details, quotes};

/// One file for the whole seeder, since it is one process however it was invoked.
///
/// The `service` field still names the target, so the split the six binaries had by filename
/// survives as a field a log query can filter on.
const LOG_FILE: &str = "seed.log";

/// Calendar days the S3 bar archive covers when no start date is given.
///
/// Two years rather than the trainer's one: this is the floor the archive is built to, and the
/// training window is what gets read out of it. Widening `FUND_LOOKBACK_DAYS` later should not also
/// require a backfill, so the seed deliberately reaches further back than any run needs today.
const DEFAULT_ARCHIVE_LOOKBACK_DAYS: i64 = 730;

/// Calendar days fetched into PostgreSQL before the rows are written and the buffer released.
///
/// A grouped response is the whole market — on the order of ten thousand rows per session — so a
/// year fetched before the first write would hold roughly two and a half million bars in memory and
/// lose all of them to one failure. Thirty days is about twenty-one sessions.
const CHUNK_DAYS: i64 = 30;

/// Sessions between quote samples unless told otherwise, which is every session.
const DEFAULT_STRIDE: usize = 1;

/// Missing names printed per session by a scan before the line is truncated.
///
/// A session short over a thousand names produced a multi-kilobyte line that buried the counts above
/// it. The full count is always printed; the union at the end is the actionable list.
const NAMES_SHOWN_PER_SESSION: usize = 12;

// --- The argument surface -------------------------------------------------------------------

#[derive(Debug, Parser)]
#[command(
    name = "seed",
    about = "Seeds and repairs the archives the fund reads from",
    disable_help_subcommand = true
)]
struct Arguments {
    #[command(subcommand)]
    command: Command,
}

/// The nouns, named for the prefixes they write: `data/derived/equity/{bars,details,quotes}`.
#[derive(Debug, Subcommand)]
enum Command {
    /// Bars from Massive, at whichever cadence the route beneath answers for.
    EquityBars {
        #[command(subcommand)]
        route: BarRoute,
    },
    /// Ticker metadata, from the CSV compiled into this binary.
    EquityDetails {
        #[command(subcommand)]
        target: DetailsTarget,
    },
    /// Quoted spreads from Alpaca, into the S3 archive. Every session is written at both cadences,
    /// so there is no interval to choose.
    EquityQuotes {
        #[command(subcommand)]
        action: QuoteAction,
    },
    /// The printed tape from Massive's flat files, into the S3 archive. Every session is written at
    /// all three cadences, so there is no interval to choose.
    EquityTrades {
        #[command(subcommand)]
        action: TradeAction,
    },
    /// Which vendor and subscription built each archived partition.
    ArchiveProvenance {
        #[command(subcommand)]
        action: ProvenanceAction,
    },
}

/// What to do about provenance the archive does not yet record.
#[derive(Debug, Subcommand)]
enum ProvenanceAction {
    /// Write sidecars for partitions that have none, reading the attribution from pass logs.
    ///
    /// The logs are one configuration source. A partition already carrying a sidecar is left
    /// alone: a record written at the time of the write beats one reconstructed afterwards.
    Backfill(ProvenanceArguments),
    /// Report partitions carrying no sidecar, writing nothing.
    Sweep,
}

#[derive(Debug, Args)]
#[group(required = true, multiple = true)]
struct ProvenanceArguments {
    /// Directory of pass logs to attribute from, searched recursively for `*.log`.
    ///
    /// What a pass observed. Only speaks for sessions a pass actually logged.
    #[arg(long)]
    from_logs: Option<std::path::PathBuf>,
    /// JSON file declaring routes for whole prefixes.
    ///
    /// What is true by construction: Massive's REST bar cadences, and every raw flat file. Logs win
    /// where both answer, because observed beats declared.
    #[arg(long)]
    from_configuration: Option<std::path::PathBuf>,
    /// Count what would be written without writing it.
    #[arg(long)]
    dry_run: bool,
}

impl Command {
    /// The `service` every line of this run carries.
    ///
    /// Splits the two bar routes, which is the one place inside a noun where a log query wants the
    /// halves apart: they call different endpoints and answer for different cadences.
    fn service(&self) -> &'static str {
        match self {
            Command::EquityBars { route } => match route {
                BarRoute::Daily { .. } => "seed-equity-bars-daily",
                BarRoute::Intraday { .. } => "seed-equity-bars-intraday",
                BarRoute::FlatFile { .. } => "seed-equity-bars-flat-file",
            },
            Command::EquityDetails { .. } => "seed-equity-details",
            Command::EquityQuotes { .. } => "seed-equity-quotes",
            Command::EquityTrades { .. } => "seed-equity-trades",
            Command::ArchiveProvenance { .. } => "seed-archive-provenance",
        }
    }
}

/// Which Massive endpoint answers, which is also which cadences it can answer for.
///
/// A subcommand rather than an `--interval` flag because the two differ in more than the value:
/// only the daily route has a PostgreSQL target, only the intraday one has a scan, and the aggregates
/// route stamps a daily bar sixteen hours from where the grouped route stamps it.
#[derive(Debug, Subcommand)]
enum BarRoute {
    /// Whole-market daily bars, one request per session off the grouped endpoint.
    Daily {
        #[command(subcommand)]
        target: DailyTarget,
    },
    /// Per-symbol intraday bars off the aggregates endpoint, into the S3 archive.
    Intraday {
        #[command(subcommand)]
        action: IntradayAction,
    },
    /// Whole-market one-minute bars off Massive's flat files, into the S3 archive.
    ///
    /// A third route rather than a cadence flag on `intraday`, for the reason the two above already
    /// differ: this one reads a whole session from one object instead of 12,000 per-symbol
    /// requests, carries no `vw`, and stamps its rows in nanoseconds.
    FlatFile {
        #[command(subcommand)]
        action: BarFlatFileAction,
    },
}

/// What a flat-file bar pass does about a session that already has a partition.
#[derive(Debug, Subcommand)]
enum BarFlatFileAction {
    /// Write the sampled sessions that have no one-minute partition yet.
    Archive(QuoteArguments),
    /// Re-fold every sampled session and merge the result into what is already there.
    ///
    /// Merge, not replace. A row the re-fold produces wins its key, so a corrected value does land;
    /// a row the re-fold no longer produces is not matched, and survives. Shrinking a partition --
    /// dropping a name that should never have been stored -- therefore means deleting it and running
    /// `archive` over the gap, since no merge can remove what it does not overwrite.
    Widen(QuoteArguments),
}

impl BarFlatFileAction {
    /// The scope this action writes under. Both are whole-market and differ only in sessions.
    fn scope(&self) -> Result<Scope, SeedError> {
        whole_market(match self {
            BarFlatFileAction::Archive(_) => SessionSelection::Absent,
            BarFlatFileAction::Widen(_) => SessionSelection::Every,
        })
    }

    /// The window and stride this action runs over.
    fn arguments(&self) -> &QuoteArguments {
        match self {
            BarFlatFileAction::Archive(arguments) | BarFlatFileAction::Widen(arguments) => {
                arguments
            }
        }
    }
}

#[derive(Debug, Subcommand)]
enum DailyTarget {
    /// Into `equity_bars`, which the application trades from. Needs a database and Massive.
    Postgres(DatabaseBarsArguments),
    /// Into `data/derived/equity/bars/interval=one_day/`, which the trainer trains from. Needs AWS, Massive
    /// and Alpaca.
    S3(ArchiveBarsArguments),
}

#[derive(Debug, Subcommand)]
enum DetailsTarget {
    /// Into `equity_details`, which the pair screen's per-sector cap reads.
    Postgres,
    /// Into `data/derived/equity/details/details.csv`, which DuckDB's `training_details` view resolves to.
    S3,
}

#[derive(Debug, Args)]
struct DatabaseBarsArguments {
    /// First session to fetch, inclusive: an Eastern calendar date, YYYY-MM-DD.
    #[arg(long, value_parser = session_date)]
    start: SessionDate,
    /// Last session to fetch, inclusive. Defaults to today.
    #[arg(long, value_parser = session_date)]
    end: Option<SessionDate>,
}

#[derive(Debug, Args)]
struct ArchiveBarsArguments {
    /// First session to repair, inclusive. Defaults to two years before the end.
    #[arg(long, value_parser = session_date)]
    start: Option<SessionDate>,
    /// Last session to repair, inclusive. Defaults to the session before today.
    #[arg(long, value_parser = session_date)]
    end: Option<SessionDate>,
}

#[derive(Debug, Subcommand)]
enum IntradayAction {
    /// Fetch the whole market into the sessions that have no partition yet.
    Fill(IntradayArguments),
    /// Refetch the whole market into every session in the window, widening ones already written.
    Widen(IntradayArguments),
    /// Report which names each partition is short of the screen, and write nothing.
    Scan(IntradayArguments),
    /// Fetch named symbols into the sessions that already have a partition. Given no symbol set,
    /// scans first and repairs whatever the scan reported missing.
    Repair(IntradayRepairArguments),
}

#[derive(Debug, Args)]
struct IntradayArguments {
    #[command(flatten)]
    window: WindowArguments,
    /// Cadence to read and write, which is also the partition it lands in.
    #[arg(long, value_enum, default_value = "five_minute")]
    cadence: Cadence,
}

#[derive(Debug, Args)]
struct IntradayRepairArguments {
    #[command(flatten)]
    intraday: IntradayArguments,
    #[command(flatten)]
    symbols: SymbolArguments,
}

#[derive(Debug, Subcommand)]
enum QuoteAction {
    /// Fold every name the daily archive holds into the sampled sessions that have no partition
    /// yet.
    Archive(QuoteArguments),
    /// Fold every name the daily archive holds into every sampled session, widening ones already
    /// summarized.
    Widen(QuoteArguments),
    /// Fold named symbols and print what they read, touching no partition.
    Measure(QuoteSymbolArguments),
    /// Fold named symbols into the sampled sessions that already have a partition.
    Repair(QuoteSymbolArguments),
    /// Read one day of Massive's flat files and report what is in it, writing nothing.
    ///
    /// What the backfill needs measured before it is written: the row order, which decides whether
    /// a fold holds one name's ticks or every name's, and the throughput, which the published
    /// download estimate leaves out because it counts bandwidth only.
    Probe(ProbeArguments),
}

impl QuoteAction {
    /// The scope an action that derives its universe from the archive folds under, and `None` for
    /// the actions that name their own symbols or write nothing.
    ///
    /// Both universe actions fold the whole market and differ only in their session set. Returned
    /// as one value so a test asserts the scope the pass will actually use rather than a
    /// reconstruction of it beside it.
    fn universe_scope(&self) -> Option<Result<Scope, SeedError>> {
        let sessions = match self {
            QuoteAction::Archive(_) => SessionSelection::Absent,
            QuoteAction::Widen(_) => SessionSelection::Every,
            QuoteAction::Measure(_) | QuoteAction::Repair(_) | QuoteAction::Probe(_) => {
                return None;
            }
        };
        Some(whole_market(sessions))
    }
}

/// What a trade pass does with the sessions it is given.
///
/// Two actions where quotes have five: there is no per-name repair, because a trade file *is* the
/// session and a name missing from it is missing from the tape rather than from a retryable fetch.
#[derive(Debug, Subcommand)]
enum TradeAction {
    /// Fold every name the daily archive holds into the sampled sessions that have no partition yet.
    Archive(QuoteArguments),
    /// Fold every name the daily archive holds into every sampled session, widening ones already
    /// summarized.
    Widen(QuoteArguments),
}

impl TradeAction {
    /// The scope this action folds under. Both fold the whole market and differ only in sessions.
    fn universe_scope(&self) -> Result<Scope, SeedError> {
        whole_market(match self {
            TradeAction::Archive(_) => SessionSelection::Absent,
            TradeAction::Widen(_) => SessionSelection::Every,
        })
    }

    /// The window and stride this action runs over.
    fn arguments(&self) -> &QuoteArguments {
        match self {
            TradeAction::Archive(arguments) | TradeAction::Widen(arguments) => arguments,
        }
    }
}

#[derive(Debug, Args)]
struct ProbeArguments {
    /// The session to read, as an Eastern calendar date: YYYY-MM-DD.
    #[arg(long, value_parser = session_date)]
    date: SessionDate,
    /// Fold only these names and print their session summaries, rather than counting the file.
    /// This is how a flat-file fold is checked against the same session through Alpaca.
    #[command(flatten)]
    symbols: SymbolArguments,
}

#[derive(Debug, Args)]
struct QuoteArguments {
    #[command(flatten)]
    window: WindowArguments,
    /// Sample every Nth published session, anchored at the start. A multiple of 5 samples one
    /// weekday forever; 21 does not.
    #[arg(long, default_value_t = DEFAULT_STRIDE, value_parser = stride)]
    stride: usize,
    /// Keep the vendor's own bytes under `data/raw/`, as Deep Archive, as this pass reads them.
    /// Every cadence the archive stores is a lossy read of them, so this is what makes a later
    /// cadence a compute cost rather than another subscription month.
    #[arg(long)]
    tee_raw: bool,
    /// Where a raw object waits between the download finishing and the upload starting. Needs room
    /// for one object: the largest session measured is 9.0 GB of quotes.
    #[arg(long, default_value = DEFAULT_STAGING_DIRECTORY)]
    staging_directory: std::path::PathBuf,
}

/// Where `--tee-raw` stages by default. Deliberately not `/tmp`, which is a tmpfs on the backfill
/// box and would put a 9 GB object in memory.
const DEFAULT_STAGING_DIRECTORY: &str = "/var/tmp/fund-flat-files";

#[derive(Debug, Args)]
struct QuoteSymbolArguments {
    #[command(flatten)]
    quotes: QuoteArguments,
    #[command(flatten)]
    symbols: SymbolArguments,
}

/// The window an archive pass runs over, as the arguments give it.
#[derive(Debug, Args)]
struct WindowArguments {
    /// First session to touch, inclusive: an Eastern calendar date, YYYY-MM-DD.
    #[arg(long, value_parser = session_date)]
    start: SessionDate,
    /// Last session to touch, inclusive: an Eastern calendar date, YYYY-MM-DD.
    #[arg(long, value_parser = session_date)]
    end: SessionDate,
}

impl WindowArguments {
    fn window(&self) -> Result<Window, String> {
        Window::new(self.start, self.end)
    }
}

/// Where a named symbol set comes from.
///
/// A file as well as a list, and mutually exclusive with it. The spread-capped universe this
/// unblocks is eleven thousand names, which is not something a command line can hold — while the
/// repair that actually keeps happening names one.
#[derive(Debug, Args)]
#[group(multiple = false)]
struct SymbolArguments {
    /// Comma-separated tickers, for a handful named by hand.
    #[arg(long, value_delimiter = ',', value_parser = ticker)]
    symbols: Vec<Ticker>,
    /// A file of tickers, one per line.
    #[arg(long, value_name = "PATH")]
    symbols_file: Option<PathBuf>,
}

impl SymbolArguments {
    /// The named set, or `None` when neither argument was given.
    fn names(&self) -> Result<Option<BTreeSet<Ticker>>, String> {
        match &self.symbols_file {
            Some(path) => read_symbols(path).map(Some),
            None if self.symbols.is_empty() => Ok(None),
            None => Ok(Some(self.symbols.iter().cloned().collect())),
        }
    }

    /// The named set, refusing its absence.
    ///
    /// For the quote actions, which act on a list and have no scan to derive one from the way the
    /// intraday path does.
    fn required_names(&self) -> Result<BTreeSet<Ticker>, String> {
        self.names()?
            .ok_or_else(|| "--symbols or --symbols-file is required".to_string())
    }
}

/// The cadences the aggregates route answers for.
///
/// Daily is absent rather than refused: it is what `equity-bars daily` is for, off the grouped route,
/// and taken from here it would be stamped sixteen hours from the archive it landed beside. Spelled
/// as the partition value it writes, so the argument names the path.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum Cadence {
    #[value(name = "five_minute")]
    FiveMinute,
    #[value(name = "one_minute")]
    OneMinute,
}

impl Cadence {
    fn interval(self) -> BarInterval {
        match self {
            Cadence::FiveMinute => BarInterval::FiveMinute,
            Cadence::OneMinute => BarInterval::OneMinute,
        }
    }
}

/// Parses an Eastern calendar date, which is what a session is.
fn session_date(raw: &str) -> Result<SessionDate, String> {
    NaiveDate::parse_from_str(raw.trim(), "%Y-%m-%d")
        .map(SessionDate::from_date)
        .map_err(|_| format!("expected an Eastern calendar date as YYYY-MM-DD, got {raw:?}"))
}

/// Parses one ticker, refusing anything unusable including an empty component.
///
/// Refused rather than skipped: a list that silently loses a name produces a partial repair the run
/// then reports as success, so an empty component is a typo rather than a separator.
fn ticker(raw: &str) -> Result<Ticker, String> {
    Ticker::new(raw.trim()).ok_or_else(|| format!("unusable ticker: {raw:?}"))
}

/// Parses a sampling stride, refusing one that would sample nothing.
fn stride(raw: &str) -> Result<usize, String> {
    raw.trim()
        .parse::<usize>()
        .ok()
        .filter(|stride| *stride > 0)
        .ok_or_else(|| format!("expected a positive whole number, got {raw:?}"))
}

/// Reads a ticker per line, refusing the whole file if any line is unusable.
///
/// A blank line is skipped rather than refused — a file ends in a newline and that is not a typo,
/// which is the one respect in which a file differs from a comma-separated argument.
fn read_symbols(path: &Path) -> Result<BTreeSet<Ticker>, String> {
    let contents = std::fs::read_to_string(path)
        .map_err(|error| format!("cannot read {}: {error}", path.display()))?;
    let mut symbols = BTreeSet::new();
    for line in contents
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
    {
        symbols.insert(ticker(line).map_err(|error| format!("{}: {error}", path.display()))?);
    }
    if symbols.is_empty() {
        return Err(format!("{} contains no tickers", path.display()));
    }
    Ok(symbols)
}

/// Inclusive session window, validated on construction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Window {
    start: SessionDate,
    end: SessionDate,
}

impl Window {
    /// Rejects an inverted window, so a `Window` in scope is proof the range is orderable.
    ///
    /// Checked here rather than by clap, which validates one argument at a time and so cannot see a
    /// rule that spans two.
    fn new(start: SessionDate, end: SessionDate) -> Result<Self, String> {
        if start > end {
            return Err(format!("--start {start} must be on or before --end {end}"));
        }
        Ok(Self { start, end })
    }

    /// Splits into consecutive inclusive windows of at most [`CHUNK_DAYS`].
    ///
    /// Windows abut rather than overlap: each begins the day after the previous one ended. The bars
    /// upsert is idempotent, so an overlap would be harmless — it would just refetch.
    fn chunks(&self) -> Vec<Window> {
        let mut chunks = Vec::new();
        let mut window_start = self.start;
        while window_start <= self.end {
            let window_end = window_start
                .plus_calendar_days(CHUNK_DAYS - 1)
                .min(self.end);
            chunks.push(Window {
                start: window_start,
                end: window_end,
            });
            window_start = window_end.plus_calendar_days(1);
        }
        chunks
    }

    /// Every calendar day in the window.
    ///
    /// Calendar days, not trading sessions: the database path exists for the case where the database
    /// is empty, and the published calendar is one of the things that is not there yet. A weekend
    /// costs one request and answers with nothing.
    fn dates(&self) -> Vec<SessionDate> {
        let mut dates = Vec::new();
        let mut date = self.start;
        while date <= self.end {
            dates.push(date);
            date = date.plus_calendar_days(1);
        }
        dates
    }
}

/// The most recent session whose daily bar can already exist.
///
/// A daily bar is stamped at the close, so the current session has none until 16:00 Eastern.
/// Defaulting to today made a pre-close run request a session with no data, which the
/// calendar-filtered pass reports as a fault rather than as a holiday. An explicitly named date is
/// left alone: an operator who asks for today is asking for a session with no data.
fn last_final_session(today: SessionDate) -> SessionDate {
    today.plus_calendar_days(-1)
}

impl DatabaseBarsArguments {
    /// Ends on today rather than the session before it, unlike the archive path.
    ///
    /// This path has no calendar, so a date with no bars is an empty answer rather than a fault —
    /// there is nothing for a pre-close run to trip over.
    fn window(&self, today: SessionDate) -> Result<Window, String> {
        Window::new(self.start, self.end.unwrap_or(today))
    }
}

impl ArchiveBarsArguments {
    /// Defaults both ends, so no arguments means "make the last two years right".
    fn window(&self, today: SessionDate) -> Result<Window, String> {
        let end = self.end.unwrap_or_else(|| last_final_session(today));
        let start = self
            .start
            .unwrap_or_else(|| end.plus_calendar_days(-DEFAULT_ARCHIVE_LOOKBACK_DAYS));
        Window::new(start, end)
    }
}

// --- What a run produced --------------------------------------------------------------------

/// Why a run stopped, and therefore what it exits with.
#[derive(Debug, thiserror::Error)]
enum SeedError {
    /// A rule spanning two arguments, which clap cannot express and so is checked after parsing.
    /// Exits 2, the code clap's own parse failures use.
    #[error("{0}")]
    Usage(String),
    /// The run started and something it needed failed.
    #[error("{0}")]
    Failed(Box<dyn std::error::Error>),
}

impl From<String> for SeedError {
    fn from(message: String) -> Self {
        SeedError::Usage(message)
    }
}

impl From<Box<dyn std::error::Error>> for SeedError {
    fn from(error: Box<dyn std::error::Error>) -> Self {
        SeedError::Failed(error)
    }
}

/// What a chunked PostgreSQL backfill did, and the two ways it can silently do less.
#[derive(Debug, Default, PartialEq, Eq)]
struct ChunkedSummary {
    rows_stored: u64,
    /// Sessions the fetch could not retrieve. A gap in the history, not a failure to store.
    dates_failed: usize,
    /// Windows whose store failed after a successful fetch.
    chunks_failed: usize,
}

impl ChunkedSummary {
    /// A run that stepped over anything is incomplete, so it must not look successful.
    ///
    /// Both counters, because a fetch that quietly skipped eleven sessions leaves exactly the hole
    /// that surfaces later as a correlation computed across a gap.
    fn is_complete(&self) -> bool {
        self.chunks_failed == 0 && self.dates_failed == 0
    }
}

impl std::fmt::Display for ChunkedSummary {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "stored {} rows, {} sessions unfetched, {} windows failed",
            self.rows_stored, self.dates_failed, self.chunks_failed
        )
    }
}

/// What a run accomplished, and therefore what it exits with.
///
/// One variant per shape of work rather than one per subcommand, because what the exit code turns on
/// is how a run can step over work, and there are only three answers to that.
enum Outcome {
    /// An S3 archive pass, which carries its own rule for whether it finished.
    Pass(archive::PassSummary),
    /// A chunked backfill into PostgreSQL, which steps over a failed window rather than aborting.
    Chunked(ChunkedSummary),
    /// A run with nothing to step over: it did all of its work, or returned an error instead of it.
    Complete,
}

impl Outcome {
    /// The operator's one line, or `None` for a run whose output already was the report.
    fn report(&self) -> Option<String> {
        match self {
            Outcome::Pass(summary) => Some(summary.to_string()),
            Outcome::Chunked(summary) => Some(summary.to_string()),
            Outcome::Complete => None,
        }
    }

    /// Records the counts as fields, so a log query can ask which runs stepped over something.
    fn log(&self) {
        match self {
            Outcome::Pass(summary) => info!(
                sessions_requested = summary.sessions_requested(),
                sessions_written = summary.sessions_written(),
                sessions_without_data = summary.sessions_without_data(),
                sessions_failed = summary.sessions_failed().len(),
                symbols_failed = summary.symbols_failed(),
                rows_written = summary.output().rows_written(),
                complete = summary.is_complete(),
                "Archive pass finished"
            ),
            Outcome::Chunked(summary) => info!(
                rows_stored = summary.rows_stored,
                dates_failed = summary.dates_failed,
                chunks_failed = summary.chunks_failed,
                complete = summary.is_complete(),
                "Backfill finished"
            ),
            Outcome::Complete => {}
        }
    }

    /// Non-zero on an incomplete run, because a partition it wrote reads as complete to everything
    /// downstream and the exit code is the only signal automation sees.
    fn exit_code(&self) -> i32 {
        let complete = match self {
            Outcome::Pass(summary) => summary.is_complete(),
            Outcome::Chunked(summary) => summary.is_complete(),
            Outcome::Complete => true,
        };
        if complete {
            0
        } else {
            1
        }
    }
}

#[tokio::main]
async fn main() {
    fund::common::crypto::install_default_crypto_provider();

    // Parsed before tracing is installed, because clap prints its own usage and exits — a guard
    // taken first would never be dropped and its buffered lines would go nowhere.
    let arguments = Arguments::parse();
    let tracing_guard = init_tracing(LOG_FILE, Some("info"), arguments.command.service());

    let code = match run(&arguments.command, SessionDate::at(Utc::now())).await {
        Ok(outcome) => {
            outcome.log();
            if let Some(report) = outcome.report() {
                println!("{report}");
            }
            outcome.exit_code()
        }
        Err(SeedError::Usage(message)) => {
            eprintln!("{message}");
            2
        }
        Err(SeedError::Failed(error)) => {
            error!(%error, "Seeding failed");
            eprintln!("Seeding failed: {error}");
            1
        }
    };

    // `std::process::exit` runs no destructors, so the non-blocking appender's guard would never
    // drop and its buffered lines would be lost — exactly when the failure log matters.
    drop(tracing_guard);
    std::process::exit(code);
}

/// Runs the subcommand the arguments name.
///
/// `today` is a parameter rather than read from the clock here, because a function that reads the
/// wall clock cannot be tested across the hours where the Eastern date and the UTC date disagree.
async fn run(command: &Command, today: SessionDate) -> Result<Outcome, SeedError> {
    match command {
        Command::EquityBars { route } => match route {
            BarRoute::Daily { target } => match target {
                DailyTarget::Postgres(arguments) => Ok(Outcome::Chunked(
                    seed_database_bars(&arguments.window(today)?).await?,
                )),
                DailyTarget::S3(arguments) => Ok(Outcome::Pass(
                    seed_archive_bars(&arguments.window(today)?).await?,
                )),
            },
            BarRoute::Intraday { action } => seed_intraday_bars(action).await,
            BarRoute::FlatFile { action } => seed_flat_file_bars(action).await,
        },
        Command::EquityDetails { target } => {
            match target {
                DetailsTarget::Postgres => seed_database_details().await?,
                DetailsTarget::S3 => seed_archive_details().await?,
            }
            Ok(Outcome::Complete)
        }
        Command::EquityQuotes { action } => seed_quotes(action).await,
        Command::EquityTrades { action } => seed_trades(action).await,
        Command::ArchiveProvenance { action } => seed_provenance(action).await,
    }
}

// --- Daily bars -----------------------------------------------------------------------------

/// Backfills daily bars into PostgreSQL over the window, a chunk at a time.
///
/// Massive is the only source, because its grouped endpoint takes a **date** rather than a symbol
/// list. Asking Alpaca means asking for its *current* tradable set, so every symbol delisted since
/// the start date would be missing from its own history.
async fn seed_database_bars(window: &Window) -> Result<ChunkedSummary, Box<dyn std::error::Error>> {
    let client = MassiveClient::from_env()?;
    let pool = connect_pool().await?;

    // No symbol list, and no universe. The grouped endpoint is asked for a date and answers with
    // every stock that traded on it, which is exactly what a bootstrap wants: the liquidity screen
    // downstream selects from what is stored, so storing a pre-filtered subset would decide the
    // universe here rather than there.
    let chunks = window.chunks();
    info!(
        destination = "postgres",
        start = %window.start,
        end = %window.end,
        chunks = chunks.len(),
        "Seeding equity bars from Massive"
    );

    let mut summary = ChunkedSummary::default();
    for chunk in &chunks {
        match seed_database_chunk(&client, &pool, chunk).await {
            Ok(chunk_summary) => {
                summary.rows_stored += chunk_summary.rows_stored;
                summary.dates_failed += chunk_summary.dates_failed;
            }
            Err(error) => {
                // A store failure, as distinct from a fetch failure — `fetch_daily_bars` already
                // steps over the dates it could not retrieve. Stepped over for the same reason:
                // a seed spans months, aborting costs every window after this one, and the upsert
                // makes re-running the whole range cheap.
                summary.chunks_failed += 1;
                error!(start = %chunk.start, end = %chunk.end, %error, "Chunk failed, continuing");
            }
        }
    }

    Ok(summary)
}

async fn seed_database_chunk(
    client: &MassiveClient,
    pool: &sqlx::PgPool,
    chunk: &Window,
) -> Result<ChunkedSummary, Box<dyn std::error::Error>> {
    let dates = chunk.dates();
    let fetched = bars::fetch_daily_bars(client, &dates).await;

    if fetched.bars.is_empty() {
        // Expected for a window that is entirely weekend or holiday, so not an error — but a
        // silent zero over a window that should hold sessions is worth being able to see.
        warn!(start = %chunk.start, end = %chunk.end, "Chunk returned no bars");
        return Ok(ChunkedSummary {
            rows_stored: 0,
            dates_failed: fetched.dates_failed.len(),
            chunks_failed: 0,
        });
    }

    let stored = bars::store_bars(pool, &fetched.bars).await?;
    info!(
        start = %chunk.start,
        end = %chunk.end,
        fetched = fetched.bars.len(),
        dates_failed = fetched.dates_failed.len(),
        stored,
        "Chunk seeded"
    );
    Ok(ChunkedSummary {
        rows_stored: stored,
        dates_failed: fetched.dates_failed.len(),
        chunks_failed: 0,
    })
}

/// Repairs the S3 bar archive over the window and returns what the pass accomplished.
///
/// Only the sessions the bucket is missing are fetched, so the cost of a run is the size of the gap
/// rather than the size of the range. Alpaca answers only for the trading calendar: without it the
/// pass would request holidays, which answer empty forever and cannot be told apart from a session
/// Massive is missing.
async fn seed_archive_bars(
    window: &Window,
) -> Result<archive::PassSummary, Box<dyn std::error::Error>> {
    let bucket = bucket_name()?;
    let massive = MassiveClient::from_env()?;
    let s3_client = fund::common::aws::s3_client().await;
    let calendar = trading_calendar(window).await?;

    info!(
        destination = "s3",
        bucket,
        start = %window.start,
        end = %window.end,
        sessions = calendar.len(),
        "Seeding the equity bar archive from Massive"
    );

    Ok(archive::archive_missing_sessions(
        &s3_client,
        &massive,
        &bucket,
        window.start,
        window.end,
        Some(&calendar),
    )
    .await?)
}

// --- Details --------------------------------------------------------------------------------

/// Seeds ticker metadata into the database from the embedded CSV.
///
/// Nothing in the running service writes `equity_details`: Alpaca does not publish sector or
/// industry, so the metadata has one source and it is compiled into this binary.
async fn seed_database_details() -> Result<(), Box<dyn std::error::Error>> {
    let details = details::parse_embedded_details()?;
    info!(tickers = details.len(), "Parsed embedded ticker metadata");

    let pool = connect_pool().await?;
    let stored = details::store_details(&pool, &details).await?;
    info!(
        destination = "postgres",
        rows = stored,
        "Equity details seeded"
    );
    Ok(())
}

/// Uploads the embedded ticker metadata beside the S3 bar archive.
///
/// Training does not read this object — the trainer parses the embedded CSV directly, so a model run
/// cannot be broken by its absence. It exists for readers outside the process.
async fn seed_archive_details() -> Result<(), Box<dyn std::error::Error>> {
    let bucket = bucket_name()?;
    let s3_client = fund::common::aws::s3_client().await;

    // Parsed before it is uploaded, so a malformed embedded CSV fails here rather than becoming an
    // object every downstream reader has to discover is unusable.
    let parsed = details::parse_embedded_details()?;
    let csv = details::embedded_csv();
    info!(bucket, tickers = parsed.len(), "Archiving equity details");

    archive::archive_details(&s3_client, &bucket, csv).await?;
    info!(
        destination = "s3",
        bytes = csv.len(),
        "Equity details archived"
    );
    Ok(())
}

// --- Intraday bars --------------------------------------------------------------------------

/// Fills, widens, scans or repairs the intraday archive over the requested window.
///
/// One function per shape of run, because they disagree about what they need: a scan builds no vendor
/// client at all, and a repair has to build its clients before the scan that derives its symbol set.
async fn seed_intraday_bars(action: &IntradayAction) -> Result<Outcome, SeedError> {
    match action {
        IntradayAction::Fill(arguments) => {
            fold_intraday(arguments, whole_market(SessionSelection::Absent)?).await
        }
        IntradayAction::Widen(arguments) => {
            fold_intraday(arguments, whole_market(SessionSelection::Every)?).await
        }
        IntradayAction::Scan(arguments) => scan_intraday_coverage(arguments).await,
        IntradayAction::Repair(repair) => repair_intraday(repair).await,
    }
}

/// The whole market over the given sessions, which only a pass that may create a partition may ask.
fn whole_market(sessions: SessionSelection) -> Result<Scope, SeedError> {
    Scope::new(NameSelection::WholeMarket, sessions)
        .map_err(|error| SeedError::Usage(error.to_string()))
}

/// Reports which names each partition is short of the screen, writing nothing.
///
/// Builds no vendor client: this reads the archive against itself, so a credential it will never use
/// must not be demanded of it.
async fn scan_intraday_coverage(arguments: &IntradayArguments) -> Result<Outcome, SeedError> {
    let window = arguments.window.window()?;
    let bucket = bucket_name()?;
    let s3_client = fund::common::aws::s3_client().await;

    let scan = scan_intraday(&s3_client, &bucket, arguments.cadence.interval(), &window).await?;
    report(&scan);
    require_whole_window(scan.failed())?;
    Ok(Outcome::Complete)
}

/// Fetches the whole market into the archive over the window.
async fn fold_intraday(arguments: &IntradayArguments, scope: Scope) -> Result<Outcome, SeedError> {
    let window = arguments.window.window()?;
    let interval = arguments.cadence.interval();
    let bucket = bucket_name()?;
    let s3_client = fund::common::aws::s3_client().await;
    let massive = MassiveClient::from_env().map_err(box_error)?;
    let calendar = trading_calendar(&window).await?;

    fold(
        &s3_client, &massive, &calendar, &bucket, interval, &window, &scope,
    )
    .await
}

/// Fetches named symbols into the sessions that already have a partition.
///
/// Given no symbol set the scan supplies one, which is why the clients are built before it rather
/// than after: that scan is thousands of reads, and an unset credential must fail in the first second
/// rather than at the end of them.
async fn repair_intraday(repair: &IntradayRepairArguments) -> Result<Outcome, SeedError> {
    let arguments = &repair.intraday;
    // Resolved first of all, so an unusable symbol file is refused before a single request.
    let given = repair.symbols.names()?;
    let window = arguments.window.window()?;
    let interval = arguments.cadence.interval();
    let bucket = bucket_name()?;
    let s3_client = fund::common::aws::s3_client().await;
    let massive = MassiveClient::from_env().map_err(box_error)?;
    let calendar = trading_calendar(&window).await?;

    let named = match given {
        Some(named) => named,
        None => {
            let scan = scan_intraday(&s3_client, &bucket, interval, &window).await?;
            report(&scan);
            require_whole_window(scan.failed())?;
            let missing = scan.missing_symbols();
            if missing.is_empty() {
                println!("Nothing to repair.");
                return Ok(Outcome::Complete);
            }
            missing
        }
    };

    let scope = Scope::new(NameSelection::Named(named), SessionSelection::Present)
        .map_err(|error| SeedError::Usage(error.to_string()))?;
    fold(
        &s3_client, &massive, &calendar, &bucket, interval, &window, &scope,
    )
    .await
}

/// The half every fetching action shares, once its scope and its clients are settled.
async fn fold(
    s3_client: &aws_sdk_s3::Client,
    massive: &MassiveClient,
    calendar: &TradingCalendar,
    bucket: &str,
    interval: BarInterval,
    window: &Window,
    scope: &Scope,
) -> Result<Outcome, SeedError> {
    info!(
        bucket,
        start = %window.start,
        end = %window.end,
        interval = %interval,
        %scope,
        sessions = calendar.len(),
        "Seeding the intraday bar archive from Massive"
    );

    Ok(Outcome::Pass(
        archive::archive_intraday_sessions(
            s3_client,
            massive,
            bucket,
            interval,
            window.start,
            window.end,
            scope,
            Some(calendar),
        )
        .await
        .map_err(box_error)?,
    ))
}

/// Refuses a scan that could not read every session it was asked about.
///
/// The names it found come only from the sessions it could read, so both the report and any repair
/// driven by it are narrower than the window — and that is the one failure neither the scan line nor
/// a pass summary can show. Fatal rather than carried, on the same terms as a calendar that does not
/// cover its window.
fn require_whole_window(unreadable: &BTreeSet<SessionDate>) -> Result<(), SeedError> {
    if unreadable.is_empty() {
        return Ok(());
    }
    Err(SeedError::Failed(
        format!(
            "could not read {} of the window's sessions, so this scan covers less than it was given",
            unreadable.len()
        )
        .into(),
    ))
}

async fn scan_intraday(
    s3_client: &aws_sdk_s3::Client,
    bucket: &str,
    interval: BarInterval,
    window: &Window,
) -> Result<archive::SymbolScan, Box<dyn std::error::Error>> {
    Ok(archive::scan_intraday_symbols(
        s3_client,
        bucket,
        interval,
        window.start,
        window.end,
        LiquidityFloor::CURRENT,
    )
    .await?)
}

/// Prints the scan: the counts, then every session that is short a name.
///
/// Per-session as well as the union, because the two answer different questions — the union is what
/// the repair takes, while one session short fifty names and fifty sessions short one are the same
/// union and very different faults.
fn report(scan: &archive::SymbolScan) {
    let counts = scan.counts();
    println!(
        "scanned {} sessions: {} complete, {} partial, {} absent, {} undescribed",
        counts.complete + counts.partial + counts.absent + counts.undescribed,
        counts.complete,
        counts.partial,
        counts.absent,
        counts.undescribed
    );

    for (session, coverage) in scan.coverage() {
        match coverage {
            archive::SessionCoverage::Partial(missing) => {
                let shown: Vec<&str> = missing
                    .iter()
                    .take(NAMES_SHOWN_PER_SESSION)
                    .map(Ticker::as_str)
                    .collect();
                let elided = missing.len().saturating_sub(shown.len());
                let suffix = if elided > 0 {
                    format!(" and {elided} more")
                } else {
                    String::new()
                };
                println!(
                    "  {session}: short {} — {}{suffix}",
                    missing.len(),
                    shown.join(",")
                );
            }
            archive::SessionCoverage::Absent => println!("  {session}: no partition"),
            archive::SessionCoverage::Undescribed => {
                println!("  {session}: no daily partition to screen against")
            }
            archive::SessionCoverage::Complete => {}
        }
    }

    if !scan.failed().is_empty() {
        // Loud, because a repair driven by this scan repairs only what the readable sessions named.
        println!(
            "WARNING: {} sessions could not be read; this picture is incomplete",
            scan.failed().len()
        );
    }

    let missing = scan.missing_symbols();
    println!(
        "{} distinct names missing from at least one session",
        missing.len()
    );
}

// --- Quotes ---------------------------------------------------------------------------------

/// Folds the sampled sessions, measures named symbols across them, or probes a vendor file.
///
/// One function per action, as on the intraday path and for the same reason: a probe reads a
/// different vendor over a different transport and needs no Alpaca credential at all, so it must not
/// be reachable only after one has been demanded.
async fn seed_quotes(action: &QuoteAction) -> Result<Outcome, SeedError> {
    match action {
        QuoteAction::Probe(arguments) => match arguments.symbols.names()? {
            None => probe_flat_file(arguments.date).await,
            Some(named) => fold_named_from_flat_file(arguments.date, named).await,
        },
        // One arm, so the universe is written once and the two actions cannot disagree about it.
        QuoteAction::Archive(arguments) | QuoteAction::Widen(arguments) => {
            // Unreachable: this arm names the two actions that answer. Returned rather than
            // panicked so that adding a variant here without adding it to `universe_scope`
            // degrades to a usage error instead of killing the process mid-pass.
            let scope = action
                .universe_scope()
                .ok_or_else(|| SeedError::Usage(format!("{action:?} folds no universe")))??;
            fold_sampled(arguments, scope, QuoteProvider::WholeSession).await
        }
        QuoteAction::Measure(symbols) => measure_sampled(symbols).await,
        QuoteAction::Repair(symbols) => {
            // Resolved before any credential is read, so a missing symbol set is refused in the
            // first millisecond rather than after a calendar fetch.
            let named = symbols.symbols.required_names()?;
            let scope = Scope::new(NameSelection::Named(named), SessionSelection::Present)
                .map_err(|error| SeedError::Usage(error.to_string()))?;
            fold_sampled(&symbols.quotes, scope, QuoteProvider::PerName).await
        }
    }
}

/// Folds the sampled sessions into the archive under the scope the action names.
///
/// Takes a built `Scope` rather than its two halves, so a caller cannot pair a universe with a
/// session set here that the constructor would have refused.
async fn fold_sampled(
    arguments: &QuoteArguments,
    scope: Scope,
    provider: QuoteProvider,
) -> Result<Outcome, SeedError> {
    let window = arguments.window.window()?;
    let (market_data, calendar) = quote_sources(&window).await?;
    let sampled = sample(&calendar, &window, arguments.stride);
    report_sample(&window, arguments.stride, &calendar, &sampled);

    // Bound before the source so it outlives the borrow, and built only where it is used: a
    // repair must not demand flat-file credentials to reach two names through Alpaca.
    let flat_files;
    let source = match provider {
        QuoteProvider::WholeSession => {
            flat_files = flat_file_client(arguments).await?;
            archive::QuoteSource::WholeSession(&flat_files)
        }
        QuoteProvider::PerName => archive::QuoteSource::PerName(&market_data),
    };

    Ok(Outcome::Pass(
        fold_quotes(&source, &calendar, &sampled, &scope).await?,
    ))
}

/// The flat-file client, teeing the vendor's bytes into the archive when the pass keeps them.
///
/// The tee writes the shared archive rather than this instance's records: the raw objects are a
/// provider-derived fact, and a second copy per developer is the thing the bucket split exists to
/// prevent.
async fn flat_file_client(
    arguments: &QuoteArguments,
) -> Result<flatfiles::FlatFileClient, SeedError> {
    let client = flatfiles::FlatFileClient::from_env().map_err(box_error)?;
    if !arguments.tee_raw {
        return Ok(client);
    }
    let s3_client = fund::common::aws::s3_client().await;
    Ok(client.teeing_raw_to(flatfiles::RawTee::new(
        s3_client,
        bucket_name()?,
        arguments.staging_directory.clone(),
    )))
}

/// Folds the sampled sessions' printed tape into the archive under the scope the action names.
///
/// The calendar comes from Alpaca and the tape from Massive, which is the same split the quote
/// pass uses: only the exchange publishes its own hours, and only the flat files hold every print.
/// Records, or reports, which route built each archived partition.
///
/// Reads the archive rather than the calendar: this is about objects that exist, so a session the
/// archive never held is not a gap here.
async fn seed_provenance(action: &ProvenanceAction) -> Result<Outcome, SeedError> {
    let bucket = bucket_name()?;
    let s3_client = fund::common::aws::s3_client().await;

    let outcome = match action {
        ProvenanceAction::Backfill(arguments) => {
            let attribution = match &arguments.from_logs {
                Some(logs) => attribution::routes_from_logs(logs).map_err(box_error)?,
                None => Default::default(),
            };
            let declarations = match &arguments.from_configuration {
                Some(path) => attribution::routes_from_configuration(path).map_err(box_error)?,
                None => Vec::new(),
            };
            info!(
                observed = attribution.len(),
                declared = declarations.len(),
                dry_run = arguments.dry_run,
                "Read an attribution"
            );
            archive::stamp_partition_provenance(
                &s3_client,
                &bucket,
                &attribution,
                &declarations,
                arguments.dry_run,
            )
            .await
            .map_err(box_error)?
        }
        ProvenanceAction::Sweep => archive::sweep_partition_provenance(&s3_client, &bucket)
            .await
            .map_err(box_error)?,
    };

    // The population, not just the difference: a run that wrote nothing because everything was
    // already stamped reads identically to one that found no partitions at all.
    info!(
        partitions_seen = outcome.partitions_seen,
        sidecars_present = outcome.sidecars_present,
        sidecars_written = outcome.sidecars_written,
        unattributed = outcome.unattributed.len(),
        "Provenance pass finished"
    );
    for key in outcome.unattributed.iter().take(20) {
        warn!(
            key,
            "Partition carries no provenance and none could be attributed"
        );
    }
    println!(
        "{} partitions, {} already recorded, {} written, {} unattributed",
        outcome.partitions_seen,
        outcome.sidecars_present,
        outcome.sidecars_written,
        outcome.unattributed.len()
    );
    Ok(Outcome::Complete)
}

async fn seed_trades(action: &TradeAction) -> Result<Outcome, SeedError> {
    let arguments = action.arguments();
    let scope = action.universe_scope()?;
    let window = arguments.window.window()?;

    let credentials = AlpacaCredentials::from_env().map_err(box_error)?;
    let days = TradingClient::from_env(credentials)
        .fetch_calendar(window.start.date(), window.end.date())
        .await
        .map_err(box_error)?;
    let calendar = TradingCalendar::from_days(days);
    let sampled = sample(&calendar, &window, arguments.stride);
    report_sample(&window, arguments.stride, &calendar, &sampled);

    let flat_files = flat_file_client(arguments).await?;
    let bucket = bucket_name()?;
    let s3_client = fund::common::aws::s3_client().await;
    Ok(Outcome::Pass(
        archive::archive_trade_sessions(
            &s3_client,
            &flat_files,
            &calendar,
            &bucket,
            &sampled,
            &scope,
        )
        .await
        .map_err(box_error)?,
    ))
}

/// Writes whole-market one-minute bars from Massive's flat files.
///
/// The trade pass's shape over a different dataset. Sessions come from the trading calendar rather
/// than from the archive, because a bar file needs no universe to fold against -- the file is the
/// whole market, which is the reason to read it at all.
async fn seed_flat_file_bars(action: &BarFlatFileAction) -> Result<Outcome, SeedError> {
    let arguments = action.arguments();
    let scope = action.scope()?;
    let window = arguments.window.window()?;

    let credentials = AlpacaCredentials::from_env().map_err(box_error)?;
    let days = TradingClient::from_env(credentials)
        .fetch_calendar(window.start.date(), window.end.date())
        .await
        .map_err(box_error)?;
    let calendar = TradingCalendar::from_days(days);
    let sampled = sample(&calendar, &window, arguments.stride);
    report_sample(&window, arguments.stride, &calendar, &sampled);

    let flat_files = flat_file_client(arguments).await?;
    let bucket = bucket_name()?;
    let s3_client = fund::common::aws::s3_client().await;
    Ok(Outcome::Pass(
        archive::archive_bar_flat_file_sessions(
            &s3_client,
            &flat_files,
            &bucket,
            &sampled,
            &scope,
            Some(&calendar),
        )
        .await
        .map_err(box_error)?,
    ))
}

/// Folds named symbols across the sampled sessions and prints what they read, writing nothing.
async fn measure_sampled(symbols: &QuoteSymbolArguments) -> Result<Outcome, SeedError> {
    let named = symbols.symbols.required_names()?;
    let arguments = &symbols.quotes;
    let window = arguments.window.window()?;
    let (market_data, calendar) = quote_sources(&window).await?;
    let sampled = sample(&calendar, &window, arguments.stride);
    report_sample(&window, arguments.stride, &calendar, &sampled);

    measure(&market_data, &calendar, &sampled, &named).await;
    Ok(Outcome::Complete)
}

fn report_sample(
    window: &Window,
    stride: usize,
    calendar: &TradingCalendar,
    sampled: &[SessionDate],
) {
    info!(
        start = %window.start,
        end = %window.end,
        stride,
        published = calendar.len(),
        sampled = sampled.len(),
        "Sampled the sessions to fold"
    );
}

/// Reads one day of Massive's flat files and reports what is in it, folding nothing.
///
/// Four things cannot be known before the subscription exists and all four decide how the backfill
/// is written: the row order, the file's size, the throughput of decompressing and parsing it, and
/// how much of it is a book no spread reads off. Counting rather than folding, so the measurement
/// costs one download and almost no memory.
async fn probe_flat_file(date: SessionDate) -> Result<Outcome, SeedError> {
    let client = flatfiles::FlatFileClient::from_env().map_err(box_error)?;
    let started = tokio::time::Instant::now();
    let (summary, _) = client
        .fold_quotes(date.date(), flatfiles::ForEach(|_ticker, _tick| {}))
        .await
        .map_err(box_error)?;
    let elapsed = started.elapsed().as_secs_f64();

    println!("{}", flatfiles::quote_key(date.date()));
    println!(
        "  {} rows, {} usable, {} unusable ({:.2}%), {} tickers",
        summary.rows_read,
        summary.ticks_folded,
        summary.unusable,
        percentage(summary.unusable, summary.rows_read),
        summary.tickers
    );
    match summary.layout() {
        Some(layout) => println!(
            "  {} ticker runs, so the file is {layout}",
            summary.ticker_runs
        ),
        None => println!("  no usable rows, so the layout is unmeasured"),
    }
    if summary.split_tickers.is_empty() {
        println!("  every name's rows are contiguous");
    } else {
        println!(
            "  {} names are split across the file: {}",
            summary.split_tickers.len(),
            summary
                .split_tickers
                .names()
                .map(|ticker| ticker.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        );
    }
    println!(
        "  {:.2} GiB compressed, read in {elapsed:.0}s at {:.1} MiB/s and {:.0} rows/s",
        summary.compressed_bytes as f64 / (1024.0 * 1024.0 * 1024.0),
        rate(summary.compressed_bytes as f64 / (1024.0 * 1024.0), elapsed),
        rate(summary.rows_read as f64, elapsed)
    );
    // The number that decides whether a fold can hold every name at once. Regular hours only, and
    // the observations are what a time-weighted quantile cannot be computed without.
    const OBSERVATION_BYTES: f64 = 16.0;
    println!(
        "  holding every fold at once would keep {:.1} GiB of observations",
        summary.ticks_folded as f64 * OBSERVATION_BYTES / (1024.0 * 1024.0 * 1024.0)
    );
    Ok(Outcome::Complete)
}

/// Folds named symbols out of a session's flat file and prints what they read, writing nothing.
///
/// The counterpart of `equity-quotes measure`, which asks Alpaca the same question. Running both on
/// one session is how the two providers are checked against each other, and it costs one file rather
/// than the whole universe held in memory.
async fn fold_named_from_flat_file(
    date: SessionDate,
    named: BTreeSet<Ticker>,
) -> Result<Outcome, SeedError> {
    let client = flatfiles::FlatFileClient::from_env().map_err(box_error)?;
    let credentials = AlpacaCredentials::from_env().map_err(box_error)?;
    let days = TradingClient::from_env(credentials)
        .fetch_calendar(date.date(), date.date())
        .await
        .map_err(box_error)?;
    let calendar = TradingCalendar::from_days(days);
    let Some((open, close)) = quotes::trading_hours(&calendar, date) else {
        return Err(SeedError::Usage(format!(
            "{date} is not a published session"
        )));
    };

    let started = tokio::time::Instant::now();
    let fold = quotes::MarketFold::new(date, QUOTE_CADENCE, open, close, named);
    let (file, fold) = client
        .fold_quotes(date.date(), fold)
        .await
        .map_err(box_error)?;
    let elapsed = started.elapsed().as_secs_f64();
    let folded_ticks = fold.folded();
    let folded = fold.finish();

    println!("{}", flatfiles::quote_key(date.date()));
    println!(
        "  {} rows scanned in {elapsed:.0}s, {folded_ticks} folded for the names asked for",
        file.rows_read
    );
    if !folded.resumed.is_empty() {
        println!(
            "  resumed across runs: {}",
            folded
                .resumed
                .iter()
                .map(Ticker::as_str)
                .collect::<Vec<_>>()
                .join(", ")
        );
    }
    let mut session_rows: Vec<_> = folded
        .summaries
        .iter()
        .filter(|summary| summary.bar_interval() == BarInterval::OneDay)
        .collect();
    session_rows.sort_by_key(|summary| summary.ticker().as_str().to_string());
    for summary in session_rows {
        println!(
            "  {:<6} mean {:>8.3}bp  median {:>8.3}bp  p90 {:>9.3}bp  bid {:>10.1}  ask {:>10.1}  quotes {:>9}  covered {:>8.1}s",
            summary.ticker().as_str(),
            summary.quoted_spread_basis_points_mean().value(),
            summary.quoted_spread_basis_points_median().value(),
            summary.quoted_spread_basis_points_ninetieth_percentile().value(),
            summary.bid_size_mean(),
            summary.ask_size_mean(),
            summary.quote_count(),
            summary.covered_seconds(),
        );
    }
    Ok(Outcome::Complete)
}

/// Guards the division, because an empty file is a real answer rather than a panic.
fn percentage(part: usize, whole: usize) -> f64 {
    if whole == 0 {
        return 0.0;
    }
    part as f64 * 100.0 / whole as f64
}

fn rate(quantity: f64, seconds: f64) -> f64 {
    if seconds <= 0.0 {
        return 0.0;
    }
    quantity / seconds
}

/// The Alpaca client and the calendar every quote action needs, measuring or writing.
async fn quote_sources(
    window: &Window,
) -> Result<(MarketDataClient, TradingCalendar), Box<dyn std::error::Error>> {
    let credentials = AlpacaCredentials::from_env()?;
    // SIP is pinned, not read from `ALPACA_DATA_FEED`: IEX's best bid and offer is not the national
    // one, so an environment variable could put two incomparable series under one key.
    let market_data = MarketDataClient::new(credentials.clone(), DataFeed::Sip);
    let days = TradingClient::from_env(credentials)
        .fetch_calendar(window.start.date(), window.end.date())
        .await?;
    Ok((market_data, TradingCalendar::from_days(days)))
}

/// The cadence every quote action folds at.
///
/// One-minute is plumbed the whole way through [`IntradayCadence`] but deliberately unreachable from
/// the command line: a one-minute pass re-derives the session row, and the check that compares it
/// against the stored one rather than overwriting it does not exist yet.
const QUOTE_CADENCE: IntradayCadence = IntradayCadence::FiveMinute;

/// Which provider a fold reads from.
///
/// A backfill takes whole sessions off Massive's flat files, because five years one name at a time
/// is a hundred days of API calls. A repair takes named symbols from Alpaca, because it already
/// knows which names it wants and a whole file to reach two of them is seven gigabytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum QuoteProvider {
    WholeSession,
    PerName,
}

/// Folds the sampled sessions into the archive, which is the half a measurement skips.
async fn fold_quotes(
    source: &archive::QuoteSource<'_>,
    calendar: &TradingCalendar,
    sampled: &[SessionDate],
    scope: &Scope,
) -> Result<archive::PassSummary, Box<dyn std::error::Error>> {
    let bucket = bucket_name()?;
    let s3_client = fund::common::aws::s3_client().await;
    Ok(archive::archive_quote_sessions(
        &s3_client,
        source,
        calendar,
        &bucket,
        sampled,
        scope,
        QUOTE_CADENCE,
    )
    .await?)
}

/// Every `stride`-th published session in the window, anchored at the oldest.
///
/// Anchored at the start rather than the end so re-running the same window samples the same
/// sessions: a sample that shifts under a longer window cannot be extended without refetching what
/// is already archived.
fn sample(calendar: &TradingCalendar, window: &Window, stride: usize) -> Vec<SessionDate> {
    calendar
        .trading_days_in_range(window.start, window.end)
        .into_iter()
        .step_by(stride)
        .collect()
}

/// Folds the named symbols and prints their session figures, writing nothing.
///
/// Sequential on purpose: this is for reading a handful of numbers off real data, and a run whose
/// symbols interleave is harder to compare against a reference than one that is slower.
async fn measure(
    market_data: &MarketDataClient,
    calendar: &TradingCalendar,
    sampled: &[SessionDate],
    symbols: &BTreeSet<Ticker>,
) {
    println!(
        "{:<8}{:<12}{:>10}{:>10}{:>10}{:>10}{:>10}{:>10}{:>12}",
        "ticker",
        "session",
        "mean_bp",
        "median_bp",
        "p90_bp",
        "first_bp",
        "min_bp",
        "quotes",
        "covered_s"
    );
    for session in sampled {
        let Some((open, close)) = quotes::trading_hours(calendar, *session) else {
            println!("{session}: not a published session");
            continue;
        };
        for ticker in symbols {
            match quotes::fold_session(market_data, ticker, *session, QUOTE_CADENCE, open, close)
                .await
            {
                Ok((summaries, fetch)) => {
                    print_session_row(ticker, *session, &summaries, fetch.received)
                }
                // Padded through `as_str`/`to_string`, because both Display impls delegate to an
                // inner type that ignores the width and would run the two columns together.
                Err(error) => println!(
                    "{:<8}{:<12} failed: {error}",
                    ticker.as_str(),
                    session.to_string()
                ),
            }
        }
    }
}

/// Prints one fold: its session row, plus the two intraday buckets worth reading beside it.
///
/// A session mean hides the shape the five-minute cadence exists to expose — AAPL quotes four times
/// wider at the open than at midday. `first` is the earliest bucket that carried a book, which is
/// the opening one only for a name quoting from 09:30; `min` is the tightest anywhere in the day.
fn print_session_row(
    ticker: &Ticker,
    session: SessionDate,
    summaries: &[QuoteSummary],
    quotes_folded: usize,
) {
    let Some((row, buckets)) = summaries.split_last() else {
        println!(
            "{:<8}{:<12} no quotes",
            ticker.as_str(),
            session.to_string()
        );
        return;
    };
    let basis_points = |summary: &QuoteSummary| summary.quoted_spread_basis_points_mean().value();
    let opening = buckets.first().map(basis_points).unwrap_or(f64::NAN);
    let tightest = buckets
        .iter()
        .map(basis_points)
        .fold(f64::NAN, |narrowest, bucket| bucket.min(narrowest));
    println!(
        "{:<8}{:<12}{:>10.2}{:>10.2}{:>10.2}{:>10.2}{:>10.2}{:>10}{:>12.0}",
        ticker.as_str(),
        session.to_string(),
        basis_points(row),
        row.quoted_spread_basis_points_median().value(),
        row.quoted_spread_basis_points_ninetieth_percentile()
            .value(),
        opening,
        tightest,
        quotes_folded,
        row.covered_seconds()
    );
}

// --- Shared plumbing ------------------------------------------------------------------------

/// The bucket every S3 subcommand writes into.
fn bucket_name() -> Result<String, Box<dyn std::error::Error>> {
    std::env::var("AWS_S3_ARCHIVE_BUCKET_NAME")
        .map_err(|_| "AWS_S3_ARCHIVE_BUCKET_NAME must be set (the shared data/** archive)".into())
}

/// Boxes a concrete error, which `?` cannot reach [`SeedError`] through in one conversion.
fn box_error<E: std::error::Error + 'static>(error: E) -> SeedError {
    SeedError::Failed(Box::new(error))
}

/// Fetches the published sessions over the window, so holidays are never requested.
///
/// One request covering the whole range: `/v2/calendar` is unpaginated and answers 1990 through 2029
/// in a single call, so even a five-year seed costs one round trip.
async fn trading_calendar(window: &Window) -> Result<TradingCalendar, Box<dyn std::error::Error>> {
    let credentials = AlpacaCredentials::from_env()?;
    let days = TradingClient::from_env(credentials)
        .fetch_calendar(window.start.date(), window.end.date())
        .await?;
    Ok(TradingCalendar::covering(days, window.start, window.end))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(arguments: &[&str]) -> Result<Arguments, clap::Error> {
        Arguments::try_parse_from(std::iter::once("seed").chain(arguments.iter().copied()))
    }

    fn session(value: &str) -> SessionDate {
        SessionDate::from_date(
            NaiveDate::parse_from_str(value, "%Y-%m-%d").expect("a valid test date"),
        )
    }

    fn database_bars(arguments: &[&str]) -> DatabaseBarsArguments {
        let parsed = parse(arguments).expect("valid arguments");
        match parsed.command {
            Command::EquityBars {
                route:
                    BarRoute::Daily {
                        target: DailyTarget::Postgres(arguments),
                    },
            } => arguments,
            _ => panic!("expected the database bar subcommand"),
        }
    }

    fn archive_bars(arguments: &[&str]) -> ArchiveBarsArguments {
        let parsed = parse(arguments).expect("valid arguments");
        match parsed.command {
            Command::EquityBars {
                route:
                    BarRoute::Daily {
                        target: DailyTarget::S3(arguments),
                    },
            } => arguments,
            _ => panic!("expected the archive bar subcommand"),
        }
    }

    fn intraday(arguments: &[&str]) -> IntradayAction {
        let parsed = parse(arguments).expect("valid arguments");
        match parsed.command {
            Command::EquityBars {
                route: BarRoute::Intraday { action },
            } => action,
            _ => panic!("expected the intraday subcommand"),
        }
    }

    fn quotes(arguments: &[&str]) -> QuoteAction {
        let parsed = parse(arguments).expect("valid arguments");
        match parsed.command {
            Command::EquityQuotes { action } => action,
            _ => panic!("expected the quote subcommand"),
        }
    }

    #[test]
    fn test_the_database_path_requires_a_start_and_ends_on_today() {
        assert!(parse(&["equity-bars", "daily", "postgres"]).is_err());

        let window = database_bars(&["equity-bars", "daily", "postgres", "--start", "2026-01-05"])
            .window(session("2026-07-31"))
            .expect("a valid window");
        assert_eq!(window.start, session("2026-01-05"));
        // Today rather than the session before it: this path has no calendar, so a date with no
        // bars answers empty rather than counting as a fault.
        assert_eq!(window.end, session("2026-07-31"));
    }

    /// Today's daily bar is stamped at the close and does not exist before it, so a default ending
    /// on today makes a pre-close run request a session the calendar-filtered pass calls a fault.
    #[test]
    fn test_the_archive_path_defaults_to_the_two_years_before_the_last_final_session() {
        let window = archive_bars(&["equity-bars", "daily", "s3"])
            .window(session("2026-08-06"))
            .expect("a valid window");

        assert_eq!(window.end, session("2026-08-05"));
        // The literal rather than the constant: two years back from the end.
        assert_eq!(window.start, session("2024-08-05"));
    }

    /// Named ends are taken as given. An operator naming today is asking for a session with no data,
    /// and the run reporting that is the exit code telling the truth rather than a defect.
    #[test]
    fn test_an_explicit_archive_end_is_taken_as_given() {
        let window = archive_bars(&["equity-bars", "daily", "s3", "--end", "2026-08-06"])
            .window(session("2026-08-06"))
            .expect("a valid window");
        assert_eq!(window.end, session("2026-08-06"));
    }

    /// Flags rather than positionals, so an end alone is now expressible. Positionally it was not,
    /// and the shell task that drove this binary carried a guard block refusing the attempt because
    /// a lone end date would have been read as the start.
    #[test]
    fn test_an_end_alone_backfills_the_two_years_before_it() {
        let window = archive_bars(&["equity-bars", "daily", "s3", "--end", "2026-03-31"])
            .window(session("2026-08-06"))
            .expect("a valid window");

        assert_eq!(window.end, session("2026-03-31"));
        assert_eq!(window.start, session("2024-03-31"));
    }

    #[test]
    fn test_an_inverted_window_is_refused() {
        let error = archive_bars(&[
            "equity-bars",
            "daily",
            "s3",
            "--start",
            "2026-02-03",
            "--end",
            "2026-01-02",
        ])
        .window(session("2026-08-06"))
        .expect_err("an inverted window");
        assert!(error.contains("must be on or before"), "{error}");

        let error = intraday_window(&[
            "equity-bars",
            "intraday",
            "fill",
            "--start",
            "2026-08-20",
            "--end",
            "2026-08-01",
        ])
        .expect_err("an inverted window");
        assert!(error.contains("must be on or before"), "{error}");
    }

    fn intraday_window(arguments: &[&str]) -> Result<Window, String> {
        match intraday(arguments) {
            IntradayAction::Fill(arguments) => arguments.window.window(),
            _ => panic!("expected the fill action"),
        }
    }

    #[test]
    fn test_a_malformed_date_is_refused() {
        assert!(parse(&["equity-bars", "daily", "postgres", "--start", "not-a-date"]).is_err());
        assert!(parse(&["equity-bars", "daily", "postgres", "--start", "2026-13-02"]).is_err());
        assert!(parse(&[
            "equity-quotes",
            "archive",
            "--start",
            "2026-08-21T00:00:00Z",
            "--end",
            "2026-08-21"
        ])
        .is_err());
    }

    /// A one-day window is a window, not an error: it is how a single missing session is repaired.
    #[test]
    fn test_a_single_session_window_is_allowed() {
        let window = intraday_window(&[
            "equity-bars",
            "intraday",
            "fill",
            "--start",
            "2026-08-20",
            "--end",
            "2026-08-20",
        ])
        .expect("a one-day window");
        assert_eq!(window.start, window.end);
    }

    #[test]
    fn test_the_cadence_defaults_to_five_minutes_and_names_its_partition() {
        let window = ["--start", "2026-08-01", "--end", "2026-08-20"];
        let cadence = |arguments: &[&str]| match intraday(arguments) {
            IntradayAction::Fill(arguments) => arguments.cadence.interval(),
            _ => panic!("expected the fill action"),
        };

        let mut defaulted = vec!["equity-bars", "intraday", "fill"];
        defaulted.extend_from_slice(&window);
        assert_eq!(cadence(&defaulted), BarInterval::FiveMinute);

        // The documented spellings, passed explicitly. Without these a typo in either value name
        // sends an operator who followed the help text to an error and the default test still passes.
        let mut five = defaulted.clone();
        five.extend_from_slice(&["--cadence", "five_minute"]);
        assert_eq!(cadence(&five), BarInterval::FiveMinute);

        let mut one = defaulted.clone();
        one.extend_from_slice(&["--cadence", "one_minute"]);
        assert_eq!(cadence(&one), BarInterval::OneMinute);
    }

    /// The aggregates route stamps a daily bar sixteen hours from where the grouped route stamps it,
    /// so a backfill taken here would not line up with the archive it landed beside.
    #[test]
    fn test_a_daily_cadence_is_not_a_cadence() {
        let window = ["--start", "2026-08-01", "--end", "2026-08-20"];
        for value in ["one_day", "1Day", "five-minute"] {
            let mut arguments = vec!["equity-bars", "intraday", "fill"];
            arguments.extend_from_slice(&window);
            arguments.extend_from_slice(&["--cadence", value]);
            assert!(parse(&arguments).is_err(), "{value} must not parse");
        }
    }

    /// `scan`, `repair` and `widen` were reserved words smuggled into the symbol slot, so each was
    /// an edge case with its own parsing test and `SCAN` had to be kept a ticker by matching order.
    /// As subcommands they cannot collide with a name at all.
    #[test]
    fn test_an_action_word_is_never_confused_with_a_ticker() {
        let action = intraday(&[
            "equity-bars",
            "intraday",
            "repair",
            "--start",
            "2026-08-01",
            "--end",
            "2026-08-20",
            "--symbols",
            "SCAN,ALL,FILL",
        ]);
        let IntradayAction::Repair(repair) = action else {
            panic!("expected the repair action");
        };
        let names = repair
            .symbols
            .names()
            .expect("a valid list")
            .expect("a named set");
        assert_eq!(names.len(), 3);
        assert!(names.contains(&Ticker::new("SCAN").expect("a valid ticker")));
    }

    #[test]
    fn test_a_symbol_list_is_parsed_and_trimmed() {
        let action = intraday(&[
            "equity-bars",
            "intraday",
            "repair",
            "--start",
            "2026-08-01",
            "--end",
            "2026-08-20",
            "--symbols",
            " CBOE , CME,ICE ",
        ]);
        let IntradayAction::Repair(repair) = action else {
            panic!("expected the repair action");
        };
        let names = repair
            .symbols
            .names()
            .expect("a valid list")
            .expect("a named set");
        assert_eq!(names.len(), 3);
        assert!(names.contains(&Ticker::new("CME").expect("a valid ticker")));
    }

    /// Refused rather than skipped. A typo silently narrowing the universe looks exactly like a name
    /// the vendor has no data for, and the run would report success having fetched less than asked.
    #[test]
    fn test_an_unusable_symbol_refuses_the_whole_list() {
        for list in ["CBOE,,ICE", "CBOE,", "AAPL,TOOLONGNAME", "  ,  "] {
            assert!(
                parse(&[
                    "equity-bars",
                    "intraday",
                    "repair",
                    "--start",
                    "2026-08-01",
                    "--end",
                    "2026-08-20",
                    "--symbols",
                    list,
                ])
                .is_err(),
                "{list} must not parse"
            );
        }
    }

    /// Two sources for one set would leave the run to pick, and picking silently is how a repair
    /// covers a different universe than the operator wrote down.
    #[test]
    fn test_a_list_and_a_file_cannot_both_be_given() {
        assert!(parse(&[
            "equity-quotes",
            "repair",
            "--start",
            "2026-08-17",
            "--end",
            "2026-08-21",
            "--symbols",
            "CBOE",
            "--symbols-file",
            "names.txt",
        ])
        .is_err());
    }

    /// A file ends in a newline and that is not a typo, which is the one respect in which a file
    /// differs from a comma-separated argument.
    #[test]
    fn test_a_symbol_file_skips_blank_lines_and_refuses_an_unusable_one() {
        let directory = tempfile::tempdir().expect("a temporary directory");

        let usable = directory.path().join("usable.txt");
        std::fs::write(&usable, "CBOE\n\n  CME  \nICE\n").expect("a written file");
        let names = read_symbols(&usable).expect("a usable file");
        assert_eq!(names.len(), 3);
        assert!(names.contains(&Ticker::new("ICE").expect("a valid ticker")));

        let unusable = directory.path().join("unusable.txt");
        std::fs::write(&unusable, "CBOE\nTOOLONGNAME\n").expect("a written file");
        assert!(read_symbols(&unusable).is_err());

        let empty = directory.path().join("empty.txt");
        std::fs::write(&empty, "\n\n").expect("a written file");
        assert!(read_symbols(&empty).is_err(), "a file naming nothing");

        assert!(read_symbols(&directory.path().join("absent.txt")).is_err());
    }

    #[test]
    fn test_the_stride_defaults_to_every_session_and_refuses_sampling_nothing() {
        let QuoteAction::Archive(arguments) = quotes(&[
            "equity-quotes",
            "archive",
            "--start",
            "2026-08-03",
            "--end",
            "2026-08-21",
        ]) else {
            panic!("expected the archive action");
        };
        assert_eq!(arguments.stride, 1);

        for value in ["0", "-1", "many"] {
            assert!(
                parse(&[
                    "equity-quotes",
                    "archive",
                    "--start",
                    "2026-08-03",
                    "--end",
                    "2026-08-21",
                    "--stride",
                    value,
                ])
                .is_err(),
                "{value} must not parse"
            );
        }
    }

    /// A probe reads one vendor file rather than the archive, so it takes a date and at most a set
    /// of names to fold out of it — never a window or a stride, which only a sampled pass has.
    #[test]
    fn test_a_probe_takes_one_date_and_optionally_names() {
        let QuoteAction::Probe(arguments) =
            quotes(&["equity-quotes", "probe", "--date", "2026-03-09"])
        else {
            panic!("expected the probe action");
        };
        assert_eq!(arguments.date, session("2026-03-09"));

        // Named, it folds those names and prints their summaries against the same session read
        // through Alpaca; unnamed, it counts the file.
        let QuoteAction::Probe(arguments) = quotes(&[
            "equity-quotes",
            "probe",
            "--date",
            "2026-03-09",
            "--symbols",
            "AAPL,MSFT",
        ]) else {
            panic!("expected the probe action");
        };
        assert_eq!(
            arguments
                .symbols
                .names()
                .expect("a valid symbol list")
                .expect("names were given")
                .len(),
            2
        );

        for extra in [vec!["--stride", "21"], vec!["--start", "2026-03-09"]] {
            let mut arguments = vec!["equity-quotes", "probe", "--date", "2026-03-09"];
            arguments.extend_from_slice(&extra);
            assert!(
                parse(&arguments).is_err(),
                "{extra:?} is not a probe argument"
            );
        }
        assert!(
            parse(&["equity-quotes", "probe"]).is_err(),
            "the date is required"
        );
    }

    /// Writing is the irreversible half and measuring reads a handful of numbers, so neither can
    /// stand in for the other by omission — each names itself.
    #[test]
    fn test_a_quote_action_on_names_requires_them() {
        let window = ["--start", "2026-08-03", "--end", "2026-08-21"];
        for action in ["measure", "repair"] {
            let mut arguments = vec!["equity-quotes", action];
            arguments.extend_from_slice(&window);
            let parsed = quotes(&arguments);
            let (QuoteAction::Measure(named) | QuoteAction::Repair(named)) = parsed else {
                panic!("expected a named quote action");
            };
            assert!(named.symbols.required_names().is_err());
        }
    }

    /// Both counters gate the exit code. A fetch that skipped sessions leaves a hole in the history
    /// that nothing else reports, so it must not exit zero any more than a failed store does.
    #[test]
    fn test_any_skipped_work_makes_a_backfill_incomplete() {
        let clean = ChunkedSummary {
            rows_stored: 100,
            dates_failed: 0,
            chunks_failed: 0,
        };
        assert_eq!(Outcome::Chunked(clean).exit_code(), 0);

        let store_failed = ChunkedSummary {
            rows_stored: 100,
            dates_failed: 0,
            chunks_failed: 1,
        };
        assert_eq!(Outcome::Chunked(store_failed).exit_code(), 1);

        let fetch_skipped = ChunkedSummary {
            rows_stored: 100,
            dates_failed: 1,
            chunks_failed: 0,
        };
        assert_eq!(Outcome::Chunked(fetch_skipped).exit_code(), 1);
    }

    /// Fill only creates a partition; widen rewrites one that is already there. Confusing the two
    /// turns a gap fill into a rewrite of every session in the window, which nothing downstream
    /// reports and no re-run undoes.
    #[test]
    fn test_filling_and_widening_choose_different_sessions() {
        assert_eq!(
            whole_market(SessionSelection::Absent)
                .expect("the whole market may create a partition")
                .to_string(),
            "every name, absent sessions only"
        );
        assert_eq!(
            whole_market(SessionSelection::Every)
                .expect("the whole market may rewrite a partition")
                .to_string(),
            "every name, every session"
        );
    }

    /// `archive` folded the screened universe, so a five-year pass wrote roughly 1,200 names a
    /// session where the daily archive held 10,067. Nothing downstream reports it: the partition
    /// exists, so the next pass reads the session as present and never looks inside, and widening
    /// afterwards means re-reading vendor files a lapsed subscription no longer serves.
    #[test]
    fn test_both_universe_quote_actions_fold_the_whole_market() {
        let window = ["--start", "2021-08-26", "--end", "2026-08-25"];
        let parsed = |verb: &str| {
            let arguments = parse(&[["equity-quotes", verb].as_slice(), &window].concat())
                .expect("a quote window");
            match arguments.command {
                Command::EquityQuotes { action } => action,
                other => panic!("expected a quote action, got {other:?}"),
            }
        };

        let rendered = |verb: &str| {
            parsed(verb)
                .universe_scope()
                .expect("a universe action has a scope")
                .expect("the whole market may write a partition")
                .to_string()
        };

        // Literals, not `whole_market(..).to_string()`: an expectation built from the call under
        // test moves with it and can never fail.
        assert_eq!(rendered("archive"), "every name, absent sessions only");
        assert_eq!(rendered("widen"), "every name, every session");

        // The actions that name their own symbols must not answer here at all, or the arm above
        // would fold a universe over a repair.
        let repair = parse(&[
            "equity-quotes",
            "repair",
            "--symbols",
            "AAPL",
            "--start",
            "2021-08-26",
            "--end",
            "2026-08-25",
        ])
        .expect("a repair");
        match repair.command {
            Command::EquityQuotes { action } => assert!(action.universe_scope().is_none()),
            other => panic!("expected a quote action, got {other:?}"),
        }
    }

    /// The trade pass folds the whole market, asserted before it runs for four days.
    ///
    /// The quote backfill folded the *screened* universe for fifteen hours and 254 sessions before a
    /// count caught it, because nothing pinned the scope the pass would actually use. This is that
    /// assertion for trades, and it is worth more here: the trade pass is the last Advanced-only
    /// item, so a wrong universe cannot be re-read after the subscription lapses.
    #[test]
    fn test_both_universe_trade_actions_fold_the_whole_market() {
        let window = ["--start", "2021-08-26", "--end", "2026-08-25"];
        let rendered = |verb: &str| {
            let arguments = parse(&[["equity-trades", verb].as_slice(), &window].concat())
                .expect("a trade window");
            match arguments.command {
                Command::EquityTrades { action } => action
                    .universe_scope()
                    .expect("the whole market may write a partition")
                    .to_string(),
                other => panic!("expected a trade action, got {other:?}"),
            }
        };

        // Literals, not `whole_market(..).to_string()`: an expectation built from the call under
        // test moves with it and can never fail.
        assert_eq!(rendered("archive"), "every name, absent sessions only");
        assert_eq!(rendered("widen"), "every name, every session");
    }

    /// A scan that could not read a session found its names only in the ones it could, so both the
    /// report and any repair driven by it are narrower than the window. `report` already printed a
    /// warning about it; the exit code, which is what automation reads, ignored it.
    #[test]
    fn test_a_scan_that_could_not_read_a_session_is_refused() {
        require_whole_window(&BTreeSet::new()).expect("a scan that read its whole window");

        let unreadable = BTreeSet::from([session("2026-08-18")]);
        let error = require_whole_window(&unreadable).expect_err("an unreadable session");
        assert!(
            matches!(error, SeedError::Failed(_)),
            "a run that started and could not finish, not a usage error"
        );
        assert!(error.to_string().contains("could not read 1"), "{error}");
    }

    /// A run with nothing to step over reports through its own output, so there is no line to print
    /// and nothing to exit non-zero over.
    #[test]
    fn test_a_run_with_nothing_to_step_over_is_silent_and_succeeds() {
        assert_eq!(Outcome::Complete.exit_code(), 0);
        assert_eq!(Outcome::Complete.report(), None);
    }

    #[test]
    fn test_a_range_shorter_than_one_chunk_is_a_single_window() {
        let window = Window::new(session("2026-01-05"), session("2026-01-09")).expect("a window");
        let chunks = window.chunks();
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].start, session("2026-01-05"));
        assert_eq!(chunks[0].end, session("2026-01-09"));
    }

    /// The windows must abut exactly: a one-day gap between them would drop a session, and the
    /// upsert would never tell anyone, because a bar that was never fetched cannot conflict.
    #[test]
    fn test_chunks_abut_and_cover_the_whole_range_without_gaps() {
        let window = Window::new(session("2026-01-01"), session("2026-04-15")).expect("a window");
        let chunks = window.chunks();

        assert!(chunks.len() > 1, "expected the range to split");
        assert_eq!(chunks[0].start, session("2026-01-01"));
        assert_eq!(
            chunks.last().expect("a final chunk").end,
            session("2026-04-15")
        );

        for pair in chunks.windows(2) {
            assert_eq!(
                pair[1].start,
                pair[0].end.plus_calendar_days(1),
                "chunks must abut without a gap or an overlap"
            );
        }

        for chunk in &chunks {
            let span = (chunk.end.date() - chunk.start.date()).num_days() + 1;
            assert!(span <= 30, "chunk of {span} days exceeds the bound");
        }
    }

    /// Every calendar day, weekends included. The database path has no calendar to filter with, and
    /// a non-session simply answers with nothing.
    #[test]
    fn test_dates_covers_every_calendar_day_in_the_window() {
        let window = Window::new(session("2026-01-01"), session("2026-01-10")).expect("a window");
        let dates = window.dates();
        assert_eq!(dates.len(), 10);
        assert_eq!(dates[0], session("2026-01-01"));
        assert_eq!(dates[9], session("2026-01-10"));
        for pair in dates.windows(2) {
            assert_eq!(pair[1], pair[0].plus_calendar_days(1));
        }
    }

    /// A single-day range must still produce that day, or a one-session top-up fetches nothing.
    #[test]
    fn test_a_single_day_range_yields_that_one_date() {
        let window = Window::new(session("2026-01-05"), session("2026-01-05")).expect("a window");
        assert_eq!(window.chunks().len(), 1);
        assert_eq!(window.dates(), vec![session("2026-01-05")]);
    }
}
