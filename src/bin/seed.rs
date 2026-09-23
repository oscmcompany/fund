//! Seeds and repairs every archive the fund reads from, one subcommand per target.
//!
//! No subcommand writes to both PostgreSQL and S3: the application never reads the archive.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::time::Duration;

use chrono::{NaiveDate, Utc};
use clap::{Args, Parser, Subcommand, ValueEnum};
use tracing::{error, info, warn};

use fund::common::alpaca::{AlpacaCredentials, DataFeed, MarketDataClient, TradingClient};
use fund::common::aws::Producer;
use fund::common::database::connect_pool;
use fund::common::flatfiles;
use fund::common::journal::{ArchiveFolded, Journal, Observation};
use fund::common::log::init_tracing;
use fund::common::massive::MassiveClient;
use fund::common::types::{
    BarInterval, IntradayCadence, LiquidityFloor, QuoteSummary, SessionDate, SicCode, Ticker,
    TradeSummary,
};
use fund::data::archive::{self, ForeignProvider, NameSelection, Scope, SessionSelection};
use fund::data::cadence::CadenceTotals;
use fund::data::calendar::TradingCalendar;
use fund::data::nightly::{self, Leg, LegOutcome, NightlyReport, ReferenceOutcome};
use fund::data::{attribution, bars, details, quotes, trades};

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
    /// Point-in-time symbol reference from Massive: what each instrument was on a date.
    EquityReference {
        #[command(subcommand)]
        action: ReferenceAction,
    },
    /// Which vendor and subscription built each archived partition.
    ArchiveProvenance {
        #[command(subcommand)]
        action: ProvenanceAction,
    },
    /// Whether a family's coarser rows are the finer rows beneath them. Writes nothing.
    ArchiveCadence {
        #[command(subcommand)]
        action: CadenceAction,
    },
    /// Whatever the recent sessions are still missing, cheapest family first, under a wall clock
    /// budget. What the nightly schedule runs.
    ArchiveNightly(NightlyArguments),
    /// Ship this box's journal and logs to the records bucket, before it powers off.
    ExportRecords,
}

/// Which vendor a nightly run takes its quotes and prints from.
///
/// The flat-file arm needs Massive Advanced, which lapses 2026-10-11.
#[derive(Debug, Clone, Copy, ValueEnum)]
enum NightlyProvider {
    /// Alpaca's REST endpoint, one request per name. Needs no subscription beyond the one the
    /// account already has.
    AlpacaRest,
    /// Massive's flat files, one object per session. Requires Advanced.
    MassiveFlatFile,
}

/// What one scheduled nightly run may do.
#[derive(Debug, Args)]
struct NightlyArguments {
    /// Which vendor the quote and trade legs read. Bars come from Massive either way.
    ///
    /// Named `--provider` because `--source` already names which copy of a flat file to read.
    #[arg(long, value_enum, default_value_t = NightlyProvider::AlpacaRest)]
    provider: NightlyProvider,
    /// How many recent trading sessions to check for gaps. More than one because the run repairs
    /// by set difference, so a missed night is healed by the next rather than by anyone noticing.
    #[arg(long, default_value_t = 5)]
    lookback_sessions: u32,
    /// Stop starting new families after this many minutes. A budget rather than a session cap: a
    /// cap of N sessions would take forty nights to heal a two-hundred-session hole.
    #[arg(long, default_value_t = 240)]
    budget_minutes: u64,
    #[command(flatten)]
    files: FlatFileArguments,
}

/// Which family to fold up and compare. Both arms read the archive and write nothing.
///
/// A subcommand rather than a flag because the two families store different columns under different
/// prefixes, and a flag would let a run name the quote prefix and the trade column rules.
#[derive(Debug, Subcommand)]
enum CadenceAction {
    /// Compare the quote archive's cadences against each other.
    Quotes(CadenceArguments),
    /// Compare the trade archive's cadences against each other.
    Trades(CadenceArguments),
}

impl CadenceAction {
    /// Which prefix and column rules this action reads.
    fn family(&self) -> archive::SummaryFamily {
        match self {
            CadenceAction::Quotes(_) => archive::SummaryFamily::Quotes,
            CadenceAction::Trades(_) => archive::SummaryFamily::Trades,
        }
    }

    /// The window, stride and pair of cadences this action runs over.
    fn arguments(&self) -> &CadenceArguments {
        match self {
            CadenceAction::Quotes(arguments) | CadenceAction::Trades(arguments) => arguments,
        }
    }
}

impl CadenceArguments {
    /// The pair to fold, refusing a direction the fold cannot run in.
    ///
    /// Checked here rather than by clap, which sees one argument at a time and so cannot express a
    /// rule spanning two; `Window::new` sets the same precedent. Refused before the listing, so an
    /// inverted pair costs no request and reports as the usage error it is.
    fn intervals(&self) -> Result<(BarInterval, BarInterval), String> {
        if self.from > self.to {
            return Err(format!(
                "--from {} is coarser than --to {}; a fold runs one way",
                self.from.bar_interval(),
                self.to.bar_interval()
            ));
        }
        Ok((self.from.bar_interval(), self.to.bar_interval()))
    }
}

#[derive(Debug, Args)]
struct CadenceArguments {
    #[command(flatten)]
    window: WindowArguments,
    /// Sample every Nth session the finer prefix holds, anchored at the oldest.
    #[arg(long, default_value_t = DEFAULT_STRIDE, value_parser = stride)]
    stride: usize,
    /// The cadence to fold up from, which is also the prefix the session population is read off.
    #[arg(long, value_enum, default_value = "one_minute")]
    from: Interval,
    /// The cadence to compare against. Refused if it is finer than `--from`, since the fold only
    /// runs one way; equal to it is the degenerate fold, which compares each row against itself.
    #[arg(long, value_enum, default_value = "one_day")]
    to: Interval,
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
    /// Write the sidecars. Without it the pass reports what it would write and writes nothing.
    ///
    /// Opt-in rather than opt-out: the archive holds tens of thousands of objects, and a flag that
    /// has to be remembered in order to *avoid* writing them is the wrong default.
    #[arg(long)]
    apply: bool,
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
            Command::EquityReference { .. } => "seed-equity-reference",
            Command::ArchiveProvenance { .. } => "seed-archive-provenance",
            Command::ArchiveCadence { .. } => "seed-archive-cadence",
            Command::ArchiveNightly(_) => "seed-archive-nightly",
            Command::ExportRecords => "seed-export-records",
        }
    }
}

/// What to do with a reference sweep.
///
/// `Probe` exists because a capability with no read-only route is one nobody checks: without it the
/// only way to see what the feed says for a date would be to write a partition and read it back.
#[derive(Debug, Subcommand)]
enum ReferenceAction {
    /// Fetch every symbol that traded and write one partition per date in the window.
    Archive(ReferenceArguments),
    /// Fetch and report, writing nothing.
    Probe(ReferenceArguments),
    /// Report the quarterly grid and which of it the archive owes. Fetches nothing.
    Grid {
        /// The Eastern date to answer as of, for asking what a future night would owe.
        #[arg(long, value_name = "YYYY-MM-DD")]
        as_of: Option<NaiveDate>,
    },
}

#[derive(Debug, clap::Args)]
struct ReferenceArguments {
    #[command(flatten)]
    window: WindowArguments,
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

/// What a symbol scan takes: a window and any interval the archive stores.
///
/// Deliberately not [`IntradayArguments`], whose [`Cadence`] cannot name `one_day` by construction.
/// Quotes and trades both store a daily partition, and those are the ones a completeness question is
/// most often asked of, so a scan that could not reach them would be answering about two thirds of
/// the archive while looking like it answered about all of it.
#[derive(Debug, Args)]
struct ScanArguments {
    #[command(flatten)]
    window: WindowArguments,
    /// The interval to scan, which is the partition the names are read from.
    #[arg(long, value_enum, default_value = "one_minute")]
    interval: Interval,
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
    Archive(QuoteFoldArguments),
    /// Fold every name the daily archive holds into every sampled session, widening ones already
    /// summarized.
    Widen(QuoteFoldArguments),
    /// Fold every sampled session afresh, taking each partition over from whatever built it.
    ///
    /// The only action that discards stored rows, which is what makes a partition mixing two
    /// vendors answer for one where `widen` would merge and keep both. Its own verb rather than a
    /// flag, because the failure mode is a pass that silently deletes what it should preserve.
    Refold(QuoteFoldArguments),
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
    /// Report which names each partition is short of the daily universe, and write nothing.
    Scan(ScanArguments),
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
            // A re-fold only has work where a partition already exists, so it takes the same
            // session set as `widen` rather than a third one.
            QuoteAction::Widen(_) | QuoteAction::Refold(_) => SessionSelection::Every,
            QuoteAction::Measure(_)
            | QuoteAction::Repair(_)
            | QuoteAction::Probe(_)
            | QuoteAction::Scan(_) => {
                return None;
            }
        };
        Some(whole_market(sessions))
    }

    /// What this action does to a partition another provider built.
    ///
    /// Read off the action rather than taken as an argument, so a claim is reachable only by asking
    /// for the verb that means it. Every other action refuses, which is the archive's standing rule.
    fn foreign_provider(&self) -> ForeignProvider {
        match self {
            QuoteAction::Refold(_) => ForeignProvider::Claim,
            QuoteAction::Archive(_)
            | QuoteAction::Widen(_)
            | QuoteAction::Measure(_)
            | QuoteAction::Repair(_)
            | QuoteAction::Probe(_)
            | QuoteAction::Scan(_) => ForeignProvider::Refuse,
        }
    }
}

/// What a trade pass does with the sessions it is given.
///
/// The whole-market actions read a flat file, which is the only affordable route to five years;
/// `repair` reaches one name through Alpaca's per-name prints, and is also the only route that
/// reaches past Massive's five-year window at all.
#[derive(Debug, Subcommand)]
enum TradeAction {
    /// Fold every name the daily archive holds into the sampled sessions that have no partition yet.
    Archive(QuoteArguments),
    /// Fold every name the daily archive holds into every sampled session, widening ones already
    /// summarized.
    Widen(QuoteArguments),
    /// Fold named symbols from Alpaca into the sampled sessions that already have a partition.
    Repair(TradeSymbolArguments),
    /// Fold named symbols from Alpaca and print what they read, touching no partition.
    ///
    /// The seam between the two providers is only checkable read-only. Writing an Alpaca fold into
    /// a partition built from flat files is how the quote archive ended up carrying rows from two
    /// passes at once, and this exists so the comparison costs nothing.
    Measure(TradeSymbolArguments),
    /// Report which names each partition is short of the daily universe, and write nothing.
    Scan(ScanArguments),
}

/// What a per-name trade pass takes, which is the window, the stride and the names.
///
/// Deliberately not [`QuoteSymbolArguments`]: these routes reach Alpaca one name at a time, so
/// `--tee-raw`, `--staging-directory` and `--cadence` name nothing they can act on, and a flag the
/// CLI accepts and ignores reads as a setting that was applied.
#[derive(Debug, Args)]
struct TradeSymbolArguments {
    #[command(flatten)]
    window: WindowArguments,
    /// Sample every Nth published session, anchored at the start. A multiple of 5 samples one
    /// weekday forever; 21 does not.
    #[arg(long, default_value_t = DEFAULT_STRIDE, value_parser = stride)]
    stride: usize,
    #[command(flatten)]
    symbols: SymbolArguments,
}

impl TradeAction {
    /// The scope an action that derives its universe from the archive folds under.
    ///
    /// `None` for the repair, which names its own symbols. Returned as one value so a test asserts
    /// the scope the pass will use rather than a reconstruction of it beside it.
    fn universe_scope(&self) -> Option<Result<Scope, SeedError>> {
        let sessions = match self {
            TradeAction::Archive(_) => SessionSelection::Absent,
            TradeAction::Widen(_) => SessionSelection::Every,
            TradeAction::Repair(_) | TradeAction::Measure(_) | TradeAction::Scan(_) => return None,
        };
        Some(whole_market(sessions))
    }

    /// The window this action runs over.
    ///
    /// Read separately from the stride rather than through one arguments struct, because the two
    /// routes no longer share one: a flat-file pass also carries the raw tee, and a per-name pass
    /// has nothing to tee.
    fn window(&self) -> &WindowArguments {
        match self {
            TradeAction::Archive(arguments) | TradeAction::Widen(arguments) => &arguments.window,
            TradeAction::Repair(symbols) | TradeAction::Measure(symbols) => &symbols.window,
            TradeAction::Scan(arguments) => &arguments.window,
        }
    }

    /// How many published sessions this action steps between folds.
    fn stride(&self) -> usize {
        match self {
            TradeAction::Archive(arguments) | TradeAction::Widen(arguments) => arguments.stride,
            TradeAction::Repair(symbols) | TradeAction::Measure(symbols) => symbols.stride,
            // A scan reads whatever the archive holds, so every published session is its
            // population; striding would make a hole it skipped read as one that is not there.
            TradeAction::Scan(_) => 1,
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
    #[command(flatten)]
    files: FlatFileArguments,
}

#[derive(Debug, Args)]
struct QuoteArguments {
    #[command(flatten)]
    window: WindowArguments,
    /// Sample every Nth published session, anchored at the start. A multiple of 5 samples one
    /// weekday forever; 21 does not.
    #[arg(long, default_value_t = DEFAULT_STRIDE, value_parser = stride)]
    stride: usize,
    #[command(flatten)]
    files: FlatFileArguments,
}

/// Where a whole-session pass reads its flat files, and where it stages them.
///
/// Separate from [`QuoteArguments`] so it can be left off the per-name types, which reach Alpaca one
/// name at a time and never open a file. The same reasoning [`TradeSymbolArguments`] already
/// records: a flag the CLI accepts and ignores reads as a setting that was applied.
#[derive(Debug, Args)]
struct FlatFileArguments {
    /// Which copy of the flat files to read, and whether to keep it.
    #[arg(long, value_enum, default_value_t = FlatFileSource::Vendor)]
    source: FlatFileSource,
    /// Where a raw object waits between the download finishing and the upload starting. Needs room
    /// for one object: the largest session measured is 9.0 GB of quotes.
    #[arg(long, default_value = DEFAULT_STAGING_DIRECTORY)]
    staging_directory: std::path::PathBuf,
}

/// Which copy of a flat file a whole-session pass reads.
///
/// One flag rather than a source and a `--tee-raw` beside it, because only two of the four
/// combinations mean anything: the archive's copy is already kept, so teeing it would upload the
/// object being read back over itself. Spelling the pair as one value is what leaves that
/// unsayable instead of guarded.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
enum FlatFileSource {
    /// Massive's endpoint, reading the bytes and discarding them.
    Vendor,
    /// Massive's endpoint, keeping every object under `data/raw/` as Deep Archive. Every cadence
    /// the archive stores is a lossy read of them, so this is what makes a later cadence a compute
    /// cost rather than another subscription month.
    VendorKeepingRaw,
    /// The archive's own copy, which buys nothing from the vendor. The objects are Deep Archive, so
    /// a session must be restored before a pass can reach it.
    Archive,
}

/// Where a kept raw object stages by default. Deliberately not `/tmp`, which is a tmpfs on the
/// backfill box and would put a 9 GB object in memory.
const DEFAULT_STAGING_DIRECTORY: &str = "/var/tmp/fund-flat-files";

/// What a per-name quote pass takes, which is the window, the stride, the names and the cadence.
///
/// Deliberately not [`QuoteArguments`]: these routes reach Alpaca one name at a time, so `--source`
/// and `--staging-directory` name nothing they can act on. The window and stride are repeated here
/// rather than flattened in, which is the same trade [`TradeSymbolArguments`] makes.
#[derive(Debug, Args)]
struct QuoteSymbolArguments {
    #[command(flatten)]
    window: WindowArguments,
    /// Sample every Nth published session, anchored at the start. A multiple of 5 samples one
    /// weekday forever; 21 does not.
    #[arg(long, default_value_t = DEFAULT_STRIDE, value_parser = stride)]
    stride: usize,
    #[command(flatten)]
    symbols: SymbolArguments,
    /// Cadence of the partition being repaired, which is the cadence the fold is opened at.
    ///
    /// A session can hold a one-minute quote partition, so a repair pinned to five minutes would
    /// fold the wrong cadence, merge it into the wrong prefix and report success over an unrepaired
    /// gap.
    #[arg(long, value_enum, default_value = "five_minute")]
    cadence: Cadence,
}

/// A whole-market quote fold, which is the only quote action that chooses its cadence.
#[derive(Debug, Args)]
struct QuoteFoldArguments {
    #[command(flatten)]
    quotes: QuoteArguments,
    /// Cadence to fold the intraday rows at, which is also the partition they land in.
    ///
    /// The session row is written only by the cadence that authors it. A fold at any other cadence
    /// derives the same row, compares it against the stored one and reports — see
    /// `archive-cadence quotes` for that comparison run over the archive afterwards.
    #[arg(long, value_enum, default_value = "five_minute")]
    cadence: Cadence,
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
    /// The bucket width this names, which is what a fold is opened at.
    fn intraday(self) -> IntradayCadence {
        match self {
            Cadence::FiveMinute => IntradayCadence::FiveMinute,
            Cadence::OneMinute => IntradayCadence::OneMinute,
        }
    }

    /// The partition rows folded at this cadence land in.
    fn interval(self) -> BarInterval {
        self.intraday().bar_interval()
    }
}

/// Any interval the archive stores, which is a cadence plus the session row.
///
/// Separate from [`Cadence`], which names a bucket a fold can be opened at: a session row is the
/// merge of the buckets rather than a grid of them, so it belongs to a comparison and never to a
/// fold. The two enums are what keep `--cadence one_day` unrepresentable.
///
/// Ordered finest-first, which is the order a fold runs in: `--from` must not exceed `--to`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
enum Interval {
    #[value(name = "one_minute")]
    OneMinute,
    #[value(name = "five_minute")]
    FiveMinute,
    #[value(name = "one_day")]
    OneDay,
}

impl Interval {
    fn bar_interval(self) -> BarInterval {
        match self {
            Interval::OneMinute => BarInterval::OneMinute,
            Interval::FiveMinute => BarInterval::FiveMinute,
            Interval::OneDay => BarInterval::OneDay,
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
    /// A read-only check, which always finished and reports whether what it read agreed.
    Checked(CadenceTotals),
    /// A scheduled night, which reports every leg it owed whatever became of it.
    Nightly(NightlyReport),
    /// A run with nothing to step over: it did all of its work, or returned an error instead of it.
    Complete,
}

impl Outcome {
    /// The operator's one line, or `None` for a run whose output already was the report.
    fn report(&self) -> Option<String> {
        match self {
            Outcome::Pass(summary) => Some(summary.to_string()),
            Outcome::Chunked(summary) => Some(summary.to_string()),
            Outcome::Checked(totals) => {
                let (folded_only, stored_only) = totals.one_sided();
                // The four ways a session did not agree, named rather than summed. A window where
                // most sessions were absent reads identically to a clean one under a single count.
                let unresolved: Vec<String> = totals
                    .unresolved()
                    .iter()
                    .map(|(session, outcome)| format!("{session} {outcome}"))
                    .collect();
                Some(format!(
                    "{} sessions reached, {} agreeing, {} rows compared, {} disagreements, {folded_only} folded-only, {stored_only} stored-only{}",
                    totals.sessions(),
                    totals.sessions_agreeing(),
                    totals.compared(),
                    totals.disagreements(),
                    if unresolved.is_empty() {
                        String::new()
                    } else {
                        format!("; unresolved: {}", unresolved.join(", "))
                    }
                ))
            }
            Outcome::Nightly(report) => Some(report.to_string()),
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
            Outcome::Checked(totals) => info!(
                sessions_reached = totals.sessions(),
                sessions_agreeing = totals.sessions_agreeing(),
                rows_compared = totals.compared(),
                disagreements = totals.disagreements(),
                unresolved = ?totals.unresolved(),
                agrees = totals.agrees(),
                "Check finished"
            ),
            Outcome::Nightly(report) => info!(
                written = report.written(),
                failed = ?report.failed(),
                skipped = ?report.skipped(),
                incomplete = ?report.incomplete(),
                complete = report.is_complete(),
                "Nightly archive run finished"
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
            // A disagreement is what this run exists to find, so it is the one outcome that must
            // not exit zero: nothing downstream reads the report, and automation reads only this.
            Outcome::Checked(totals) => totals.agrees(),
            Outcome::Nightly(report) => report.is_complete(),
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
            }
            Ok(Outcome::Complete)
        }
        Command::EquityQuotes { action } => seed_quotes(action).await,
        Command::EquityTrades { action } => seed_trades(action).await,
        Command::EquityReference { action } => seed_reference(action).await,
        Command::ArchiveProvenance { action } => seed_provenance(action).await,
        Command::ArchiveNightly(arguments) => archive_nightly(arguments, today).await,
        Command::ArchiveCadence { action } => check_cadence(action).await,
        Command::ExportRecords => export_records(today).await,
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

/// Seeds ticker metadata into the database from the archive's newest reference partition.
///
/// The newest rather than a point-in-time read, because the screen that consumes `equity_details`
/// only ever runs against the current session.
async fn seed_database_details() -> Result<(), Box<dyn std::error::Error>> {
    let bucket = bucket_name()?;
    let s3_client = fund::common::aws::s3_client().await;
    let universe = archive::current_universe(&s3_client, &bucket).await?;
    let details = details::details_from_universe(universe.rows())?;
    info!(tickers = details.len(), "Read the reference universe");

    let pool = connect_pool().await?;
    let stored = details::store_details(&pool, &details).await?;
    info!(
        destination = "postgres",
        rows = stored,
        "Equity details seeded"
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

    let scan = scan_family(
        &s3_client,
        &bucket,
        archive::SessionFamily::Bars,
        arguments.cadence.interval(),
        &window,
    )
    .await?;
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
            let scan = scan_family(
                &s3_client,
                &bucket,
                archive::SessionFamily::Bars,
                interval,
                &window,
            )
            .await?;
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

/// Scans a summary family and prints what it found, reading the archive against itself.
///
/// Refuses a partial window for the same reason the bars scan does: the names it found come only
/// from the sessions it could read, so a short scan understates the gap it was run to measure.
async fn scan_summary_coverage(
    family: archive::SessionFamily,
    arguments: &ScanArguments,
) -> Result<Outcome, SeedError> {
    let window = arguments.window.window()?;
    let bucket = bucket_name()?;
    let s3_client = fund::common::aws::s3_client().await;

    let scan = scan_family(
        &s3_client,
        &bucket,
        family,
        arguments.interval.bar_interval(),
        &window,
    )
    .await?;
    report(&scan);
    require_whole_window(scan.failed())?;
    Ok(Outcome::Complete)
}

/// Scans one family, against the universe that family's own fold was written to fill.
///
/// The selection is no longer expressible here — it comes from the family inside the scan — so the
/// floor is the only thing this chooses, and it may because a scan writes nothing.
async fn scan_family(
    s3_client: &aws_sdk_s3::Client,
    bucket: &str,
    family: archive::SessionFamily,
    interval: BarInterval,
    window: &Window,
) -> Result<archive::SymbolScan, Box<dyn std::error::Error>> {
    Ok(archive::scan_session_symbols(
        s3_client,
        bucket,
        family,
        interval,
        LiquidityFloor::CURRENT,
        window.start,
        window.end,
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
            None => probe_flat_file(arguments.date, &arguments.files).await,
            Some(named) => fold_named_from_flat_file(arguments.date, named, &arguments.files).await,
        },
        QuoteAction::Scan(arguments) => {
            scan_summary_coverage(archive::SessionFamily::Quotes, arguments).await
        }
        // One arm, so the universe is written once and the three actions cannot disagree about it.
        QuoteAction::Archive(arguments)
        | QuoteAction::Widen(arguments)
        | QuoteAction::Refold(arguments) => {
            // Unreachable: this arm names the three actions that answer. Returned rather than
            // panicked so that adding a variant here without adding it to `universe_scope`
            // degrades to a usage error instead of killing the process mid-pass.
            let scope = action
                .universe_scope()
                .ok_or_else(|| SeedError::Usage(format!("{action:?} folds no universe")))??;
            fold_sampled(
                &arguments.quotes.window,
                arguments.quotes.stride,
                scope,
                QuoteProvider::WholeSession(&arguments.quotes.files),
                arguments.cadence.intraday(),
                action.foreign_provider(),
            )
            .await
        }
        QuoteAction::Measure(symbols) => measure_sampled(symbols).await,
        QuoteAction::Repair(symbols) => {
            // Resolved before any credential is read, so a missing symbol set is refused in the
            // first millisecond rather than after a calendar fetch.
            let named = symbols.symbols.required_names()?;
            let scope = Scope::new(NameSelection::Named(named), SessionSelection::Present)
                .map_err(|error| SeedError::Usage(error.to_string()))?;
            fold_sampled(
                &symbols.window,
                symbols.stride,
                scope,
                QuoteProvider::PerName,
                symbols.cadence.intraday(),
                action.foreign_provider(),
            )
            .await
        }
    }
}

/// Folds the sampled sessions into the archive under the scope the action names.
///
/// Takes a built `Scope` rather than its two halves, so a caller cannot pair a universe with a
/// session set here that the constructor would have refused. The provider carries the flat-file
/// arguments rather than taking them beside it, so the per-name route has no way to be handed
/// settings it cannot act on.
async fn fold_sampled(
    window: &WindowArguments,
    stride: usize,
    scope: Scope,
    provider: QuoteProvider<'_>,
    cadence: IntradayCadence,
    foreign: ForeignProvider,
) -> Result<Outcome, SeedError> {
    let window = window.window()?;
    let (market_data, calendar) = quote_sources(&window).await?;
    let sampled = sample(&calendar, &window, stride);
    report_sample(&window, stride, &calendar, &sampled);

    // Bound before the source so it outlives the borrow, and built only where it is used: a
    // repair must not demand flat-file credentials to reach two names through Alpaca.
    let flat_files;
    let source = match provider {
        QuoteProvider::WholeSession(files) => {
            flat_files = flat_file_client(files).await?;
            archive::QuoteSource::WholeSession(&flat_files)
        }
        QuoteProvider::PerName => archive::QuoteSource::PerName(&market_data),
    };

    Ok(Outcome::Pass(
        fold_quotes(&source, &calendar, &sampled, &scope, cadence, foreign).await?,
    ))
}

/// Folds whatever the recent sessions are still missing, cheapest family first.
///
/// Every leg repairs by set difference against the bucket, so this is safe to run nightly and safe
/// to run twice. A leg that errors is stepped over rather than aborting the night, because the
/// families are independent and losing the quote fold should not also lose the daily bars.
async fn archive_nightly(
    arguments: &NightlyArguments,
    today: SessionDate,
) -> Result<Outcome, SeedError> {
    let horizon = Window::new(
        today.plus_calendar_days(-(i64::from(arguments.lookback_sessions) * 2 + 7)),
        today,
    )
    .map_err(SeedError::Usage)?;
    let calendar = trading_calendar(&horizon)
        .await
        .map_err(SeedError::Failed)?;
    let plan = nightly::plan(today, arguments.lookback_sessions, &calendar)
        .map_err(|refusal| SeedError::Usage(refusal.to_string()))?;

    let budget = nightly::Budget::starting_now(Duration::from_secs(arguments.budget_minutes * 60));
    let mut report = NightlyReport::over(&plan);

    info!(
        window_start = %plan.window_start(),
        window_end = %plan.window_end(),
        sessions = plan.sessions().len(),
        budget_minutes = arguments.budget_minutes,
        "Starting the nightly archive run"
    );

    for leg in Leg::ALL {
        if !budget.may_start_another() {
            report.record(leg, LegOutcome::Skipped);
            continue;
        }
        let outcome = match run_leg(leg, &plan, &calendar, arguments, &budget).await {
            Ok(outcome) => outcome,
            Err(error) => {
                error!(%leg, %error, "Nightly leg failed, continuing to the next");
                LegOutcome::Failed(error.to_string())
            }
        };
        info!(%leg, remaining_seconds = budget.remaining().as_secs(), "Leg finished");
        report.record(leg, outcome);
    }

    let reference = if budget.may_start_another() {
        match run_reference(today, &budget).await {
            Ok(outcome) => outcome,
            Err(error) => {
                error!(%error, "Reference sweep failed");
                ReferenceOutcome::Failed(error.to_string())
            }
        }
    } else {
        ReferenceOutcome::Skipped
    };
    info!(%reference, "Reference sweep finished");
    report.record_reference(reference);

    // Written before the caller decides the exit code, because the box stops itself once this
    // returns: a record produced after the run is a record produced on a machine that is gone.
    record_the_fold(&report).await;

    Ok(Outcome::Nightly(report))
}

/// Writes the night into the archiver's journal, if it has one.
///
/// A missing journal is logged and stepped over rather than failing the run. The fold is the work
/// and the record is the account of it; losing the account is bad, and throwing away a completed
/// fold because the account could not be filed is worse.
async fn record_the_fold(report: &NightlyReport) {
    let journal = match Journal::from_env() {
        Ok(journal) => journal,
        Err(error) => {
            warn!(%error, "No journal on this box; the night is not recorded");
            return;
        }
    };
    let observation = Observation::ArchiveFolded(ArchiveFolded {
        window_start: report.window().0.to_string(),
        window_end: report.window().1.to_string(),
        sessions: report.sessions_planned(),
        partitions_written: report.written(),
        legs: report
            .legs()
            .iter()
            .map(|(leg, outcome)| (leg.to_string(), format!("{outcome:?}")))
            .collect(),
        reference: report.reference().map(|outcome| outcome.to_string()),
        failed: report.failed().iter().map(|leg| leg.to_string()).collect(),
        skipped: report.skipped().iter().map(|leg| leg.to_string()).collect(),
        incomplete: report
            .incomplete()
            .iter()
            .map(|leg| leg.to_string())
            .collect(),
        complete: report.is_complete(),
    });
    journal
        .record(uuid::Uuid::new_v4(), Utc::now(), observation)
        .await;
}

/// Ships this box's journal and logs to the records bucket.
///
/// Its own subcommand rather than a step inside the nightly, because a night that failed is the one
/// whose records are most worth having: chaining the export to a successful fold would lose them
/// exactly when they matter.
async fn export_records(today: SessionDate) -> Result<Outcome, SeedError> {
    let bucket = std::env::var("AWS_S3_RECORDS_BUCKET_NAME")
        .map_err(|_| SeedError::Usage("AWS_S3_RECORDS_BUCKET_NAME must be set".to_string()))?;
    let s3_client = fund::common::aws::s3_client().await;

    match Journal::from_env() {
        Ok(journal) => {
            let sessions = fund::data::export::export_journals(
                &journal,
                &s3_client,
                &bucket,
                today,
                Producer::Archiver,
            )
            .await;
            info!(
                sessions = sessions.exported.len(),
                records = sessions.total_records(),
                failed = sessions.failed.len(),
                "Journal exported"
            );
        }
        Err(error) => warn!(%error, "No journal on this box; nothing to export"),
    }

    let logs = fund::data::export::export_logs(
        &fund::common::log::log_directory(),
        &s3_client,
        &bucket,
        today,
        Producer::Archiver,
    )
    .await;
    info!(
        files = logs.exported.len(),
        lines = logs.total_lines(),
        failed = logs.failed.len(),
        unparsable = logs.unparsable_lines,
        "Logs exported"
    );
    // Reported rather than swallowed: "the directory could not be listed" and "it was empty" are
    // different answers and a count of zero cannot tell them apart.
    if let Some(error) = &logs.directory_error {
        return Err(SeedError::Failed(error.clone().into()));
    }

    Ok(Outcome::Complete)
}

/// Fills every quarterly reference observation the archive is missing.
///
/// Runs after the legs rather than beside them because it reads a session's symbol list out of the
/// bar archive: the grid point for a new quarter is a session whose bars this same run has just
/// written. Deferral is cheap here in a way it is not for a leg -- the work is defined by a set
/// difference over quarters, so a night that skips it loses a day and never the quarter.
async fn run_reference(
    today: SessionDate,
    budget: &nightly::Budget,
) -> Result<ReferenceOutcome, Box<dyn std::error::Error>> {
    let bucket = bucket_name()?;
    let s3_client = fund::common::aws::s3_client().await;
    let present = archive::reference_partition_dates(&s3_client, &bucket).await?;

    let anchor = nightly::reference_anchor(&present)
        .ok_or_else(|| nightly::ReferenceRefusal::NoObservations.to_string())?;
    let calendar = trading_calendar(&Window::new(anchor, today)?).await?;
    let plan = nightly::plan_reference(today, &present, &calendar)
        .map_err(|refusal| refusal.to_string())?;

    info!(
        %anchor,
        grid = plan.grid().len(),
        owed = plan.owed().len(),
        "Planned the quarterly reference grid"
    );

    // Before the client is built, not after: on most nights nothing is owed, and a missing Massive
    // credential must not fail a sweep that was never going to call the feed.
    if plan.owed().is_empty() {
        return Ok(ReferenceOutcome::Swept {
            written: Vec::new(),
            unwritten: Vec::new(),
        });
    }

    let massive = MassiveClient::from_env()?;
    let mut written = Vec::new();
    let mut unwritten = Vec::new();
    for as_of in plan.owed() {
        if !budget.may_start_another() {
            unwritten.push((*as_of, "the budget was spent".to_string()));
            continue;
        }
        // Stepped over rather than propagated, the way the leg loop steps over a failed leg: the
        // grid points are independent, so one historical hole must not block the current quarter.
        let tickers = match archive::session_symbols(&s3_client, &bucket, *as_of).await {
            Ok(tickers) => tickers,
            Err(error) => {
                error!(%as_of, %error, "Could not read the session's symbols");
                unwritten.push((*as_of, error.to_string()));
                continue;
            }
        };
        if tickers.is_empty() {
            unwritten.push((*as_of, "no bar partition to take symbols from".to_string()));
            continue;
        }
        let sweep =
            match archive::archive_reference(&s3_client, &massive, &bucket, *as_of, &tickers).await
            {
                Ok(sweep) => sweep,
                Err(error) => {
                    error!(%as_of, %error, "The reference sweep failed for this quarter");
                    unwritten.push((*as_of, error.to_string()));
                    continue;
                }
            };
        match sweep.refusal() {
            None => written.push(*as_of),
            Some(refusal) => unwritten.push((*as_of, refusal.to_string())),
        }
    }

    Ok(ReferenceOutcome::Swept { written, unwritten })
}

/// Runs one leg, stopping between sessions once the budget is spent.
///
/// Daily bars are the exception twice over: they run as one windowed call, and so they are the one
/// leg the budget cannot interrupt. `sessions_to_request` derives its correction window from the
/// whole window, so feeding it a session at a time would re-request every session every night
/// rather than only the recent ones Massive still restates. The overrun that buys is bounded by
/// `--lookback-sessions` grouped requests, which is seconds at any sane lookback, and it is why
/// this leg runs first.
async fn run_leg(
    leg: Leg,
    plan: &nightly::NightlyPlan,
    calendar: &TradingCalendar,
    arguments: &NightlyArguments,
    budget: &nightly::Budget,
) -> Result<LegOutcome, Box<dyn std::error::Error>> {
    let bucket = bucket_name()?;
    let s3_client = fund::common::aws::s3_client().await;
    let scope = Scope::new(NameSelection::WholeMarket, SessionSelection::Absent)?;

    if let Leg::DailyBars = leg {
        let summary = archive::archive_missing_sessions(
            &s3_client,
            &MassiveClient::from_env()?,
            &bucket,
            plan.window_start(),
            plan.window_end(),
            Some(calendar),
        )
        .await?;
        return Ok(LegOutcome::Folded {
            complete: summary.is_complete(),
            written: summary.sessions_written(),
        });
    }

    let sessions = plan.sessions();

    // Bound before the loop so one client serves every session, and so the faithfulness floor is
    // asked of the whole plan before a single print is fetched. Not built for intraday bars, which
    // read Massive's API: a missing flat-file credential must not abort a leg that never needed it.
    let flat_files;
    let market_data;
    let trade_source = match leg {
        Leg::IntradayBars(_) => None,
        _ => Some(match arguments.provider {
            NightlyProvider::AlpacaRest => {
                market_data = market_data_client().await?;
                archive::TradeSource::PerName(&market_data)
            }
            NightlyProvider::MassiveFlatFile => {
                flat_files = flat_file_client(&arguments.files).await?;
                archive::TradeSource::WholeSession(&flat_files)
            }
        }),
    };

    if let (Leg::Trades, Some(trade_source)) = (leg, &trade_source) {
        // Past the floor Alpaca folds an opening auction print the archive excludes, adding
        // 0.3-2.2% of session volume and varying by name.
        let unfaithful = trade_source.unfaithful_sessions(sessions);
        if let Some(earliest) = unfaithful.first() {
            return Err(format!(
                "{trade_source} trades are not faithful before {}: {} of {} planned sessions are \
                 earlier, from {earliest}. Writing them would add volume the archive correctly \
                 excludes; re-fold from the raw tee instead.",
                trade_source
                    .faithful_from()
                    .expect("a route that refuses a session has a floor"),
                unfaithful.len(),
                sessions.len(),
            )
            .into());
        }
    }

    let mut written = 0;
    let mut complete = true;

    for (index, session) in sessions.iter().enumerate() {
        // Asked between sessions and never inside one, so a fold cannot be cut off having written
        // a session at one cadence and not the other.
        if !budget.may_start_another() {
            warn!(
                %leg,
                unreached = sessions.len() - index,
                "Budget spent; the next run will reach the rest"
            );
            return Ok(LegOutcome::CutShort {
                written,
                unreached: sessions.len() - index,
            });
        }

        let one = [*session];
        let summary = match leg {
            Leg::DailyBars => unreachable!("handled above, before the per-session loop"),
            Leg::IntradayBars(_) => {
                archive::archive_intraday_sessions(
                    &s3_client,
                    &MassiveClient::from_env()?,
                    &bucket,
                    leg.interval(),
                    *session,
                    *session,
                    &scope,
                    Some(calendar),
                )
                .await?
            }
            Leg::Quotes(cadence) => {
                let Some(source) = &trade_source else {
                    unreachable!("only intraday bars skip the source, and this is not that arm")
                };
                let quote_source = match source {
                    archive::TradeSource::PerName(client) => archive::QuoteSource::PerName(client),
                    archive::TradeSource::WholeSession(files) => {
                        archive::QuoteSource::WholeSession(files)
                    }
                };
                archive::archive_quote_sessions(
                    &s3_client,
                    &quote_source,
                    calendar,
                    &bucket,
                    &one,
                    &scope,
                    cadence,
                    ForeignProvider::Refuse,
                )
                .await?
            }
            Leg::Trades => {
                let flat_files;
                let market_data;
                let source = match arguments.provider {
                    NightlyProvider::AlpacaRest => {
                        market_data = market_data_client().await?;
                        archive::TradeSource::PerName(&market_data)
                    }
                    NightlyProvider::MassiveFlatFile => {
                        flat_files = flat_file_client(&arguments.files).await?;
                        archive::TradeSource::WholeSession(&flat_files)
                    }
                };
                // Refused before a print is fetched: past the floor Alpaca folds an opening auction
                // print the archive excludes, adding 0.3-2.2% of session volume.
                let unfaithful = source.unfaithful_sessions(&one);
                if let Some(earliest) = unfaithful.first() {
                    return Err(format!(
                        "{source} trades are not faithful before {}: {earliest} is earlier. \
                         Writing it would add volume the archive correctly excludes; re-fold \
                         from the raw tee instead.",
                        source
                            .faithful_from()
                            .expect("a route that refuses a session has a floor"),
                    )
                    .into());
                }
                archive::archive_trade_sessions(
                    &s3_client, &source, calendar, &bucket, &one, &scope,
                )
                .await?
            }
        };
        written += summary.sessions_written();
        complete &= summary.is_complete();
    }

    Ok(LegOutcome::Folded { complete, written })
}

/// The flat-file client the pass's source names.
///
/// The tee and the archive read both address the shared archive rather than this instance's
/// records: the raw objects are a provider-derived fact, and a second copy per developer is the
/// thing the bucket split exists to prevent.
async fn flat_file_client(
    arguments: &FlatFileArguments,
) -> Result<flatfiles::FlatFileClient, SeedError> {
    match arguments.source {
        FlatFileSource::Vendor => flatfiles::FlatFileClient::from_env().map_err(box_error),
        FlatFileSource::VendorKeepingRaw => {
            let s3_client = fund::common::aws::s3_client().await;
            flatfiles::FlatFileClient::from_env_teeing_to(flatfiles::RawTee::new(
                s3_client,
                bucket_name()?,
                arguments.staging_directory.clone(),
            ))
            .map_err(box_error)
        }
        FlatFileSource::Archive => Ok(flatfiles::FlatFileClient::reading_the_archive(
            fund::common::aws::s3_client().await,
            bucket_name()?,
        )),
    }
}

/// Sweeps the reference feed over every session in the window, writing one partition per date.
///
/// The caller chooses the dates. A quarterly grid is the intended use — share counts move over
/// quarters, not sessions — but the window is not forced to one, because a sampling question is the
/// operator's and a rule here would be a second place the grid is decided.
async fn seed_reference(action: &ReferenceAction) -> Result<Outcome, SeedError> {
    let (arguments, writes) = match action {
        ReferenceAction::Archive(arguments) => (arguments, true),
        ReferenceAction::Probe(arguments) => (arguments, false),
        ReferenceAction::Grid { as_of } => return report_reference_grid(*as_of).await,
    };
    let window = arguments.window.window()?;
    let bucket = bucket_name()?;
    let s3_client = fund::common::aws::s3_client().await;
    let massive = MassiveClient::from_env().map_err(box_error)?;
    let calendar = trading_calendar(&window).await?;
    let sessions = calendar.trading_days_in_range(window.start, window.end);

    info!(
        bucket,
        start = %window.start,
        end = %window.end,
        sessions = sessions.len(),
        writes,
        "Sweeping the point-in-time reference feed"
    );

    let mut sessions_written = 0usize;
    let mut sessions_failed = 0usize;
    for session in &sessions {
        let tickers = archive::session_symbols(&s3_client, &bucket, *session)
            .await
            .map_err(box_error)?;
        if tickers.is_empty() {
            // A session the bar archive does not hold is not a market holiday, it is a gap; the
            // calendar already excluded the holidays.
            warn!(%session, "No bar partition, so no symbols to ask the reference feed about");
            sessions_failed += 1;
            continue;
        }

        if !writes {
            let sample = tickers
                .first()
                .expect("a non-empty list has a first symbol");
            let found = massive
                .fetch_reference(sample, *session)
                .await
                .map_err(box_error)?;
            println!(
                "{session}  {:>6} symbols traded  |  {} -> {}",
                tickers.len(),
                sample,
                match &found {
                    Some(reference) => format!(
                        "type={:?} sic={} shares={}",
                        reference.security_type().map(|kind| kind.as_code()),
                        reference.sic_code().map_or("-", SicCode::as_str),
                        reference
                            .shares_outstanding()
                            .map_or_else(|| "-".to_string(), |count| format!("{count:.0}"))
                    ),
                    None => "the feed has no record".to_string(),
                }
            );
            continue;
        }

        let sweep = archive::archive_reference(&s3_client, &massive, &bucket, *session, &tickers)
            .await
            .map_err(box_error)?;
        if sweep.wrote_partition() {
            sessions_written += 1;
        } else {
            sessions_failed += 1;
        }
        println!(
            "{session}  requested {:>6}  found {:>6}  absent {:>4}  failed {:>4}  coverage {}",
            sweep.requested,
            sweep.found,
            sweep.absent.len(),
            sweep.failed.len(),
            sweep.coverage().map_or_else(
                || "n/a".to_string(),
                |share| format!("{:.2}%", share * 100.0)
            )
        );
    }

    // Keyed on sessions, not symbols: an unanswerable symbol is the ordinary residual of a
    // whole-market sweep, and an earlier driver recorded no progress at all by failing on one.
    if sessions_failed > 0 {
        return Err(box_error(ReferenceSweepIncomplete {
            written: sessions_written,
            failed: sessions_failed,
        }));
    }
    Ok(Outcome::Complete)
}

/// Prints the quarterly grid and the set difference the nightly run would act on.
///
/// The read-only route to the planning half, so the question "is 2026-10-01 owed" can be asked
/// without writing a partition to find out.
///
/// `as_of` answers it for a night that has not happened yet, which is the only way to watch this
/// report name a quarter: against today's archive it prints nothing owed, and an instrument that
/// can only report nothing has not been shown to work.
async fn report_reference_grid(as_of: Option<NaiveDate>) -> Result<Outcome, SeedError> {
    let bucket = bucket_name()?;
    let s3_client = fund::common::aws::s3_client().await;
    let today = as_of.map_or_else(|| SessionDate::at(Utc::now()), SessionDate::from_date);
    let present = archive::reference_partition_dates(&s3_client, &bucket)
        .await
        .map_err(box_error)?;
    let anchor = nightly::reference_anchor(&present)
        .ok_or_else(|| SeedError::Usage(nightly::ReferenceRefusal::NoObservations.to_string()))?;
    let calendar = trading_calendar(&Window::new(anchor, today).map_err(SeedError::Usage)?)
        .await
        .map_err(SeedError::Failed)?;
    let plan = nightly::plan_reference(today, &present, &calendar)
        .map_err(|refusal| SeedError::Usage(refusal.to_string()))?;

    for as_of in plan.grid() {
        let owed = plan.owed().contains(as_of);
        println!("{as_of}  {}", if owed { "OWED" } else { "present" });
    }
    println!(
        "{} grid points from {anchor}, {} present, {} owed",
        plan.grid().len(),
        plan.grid().len() - plan.owed().len(),
        plan.owed().len()
    );
    Ok(Outcome::Complete)
}

/// A sweep that left a session unwritten, named so the exit code has a reason attached.
#[derive(Debug)]
struct ReferenceSweepIncomplete {
    written: usize,
    failed: usize,
}

impl std::fmt::Display for ReferenceSweepIncomplete {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "wrote {} reference partitions and left {} session(s) unwritten",
            self.written, self.failed
        )
    }
}

impl std::error::Error for ReferenceSweepIncomplete {}

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
                apply = arguments.apply,
                "Read an attribution"
            );
            archive::stamp_partition_provenance(
                &s3_client,
                &bucket,
                &attribution,
                &declarations,
                arguments.apply,
            )
            .await
            .map_err(box_error)?
        }
        ProvenanceAction::Sweep => archive::sweep_partition_provenance(&s3_client, &bucket)
            .await
            .map_err(box_error)?,
    };

    // The population, not just the difference: a run that wrote nothing because everything was
    // already stamped reads identically to one that found no objects at all.
    info!(
        objects_seen = outcome.objects_seen,
        sidecars_present = outcome.sidecars_present,
        sidecars_planned = outcome.sidecars_planned,
        sidecars_written = outcome.sidecars_written,
        sidecars_missing = outcome.sidecars_missing.len(),
        unattributed = outcome.unattributed.len(),
        write_failures = outcome.write_failures.len(),
        "Provenance pass finished"
    );
    for key in outcome.unattributed.iter().take(20) {
        warn!(
            key,
            "Object carries no provenance and none could be attributed"
        );
    }
    for key in outcome.sidecars_missing.iter().take(20) {
        warn!(key, "Object carries no provenance record");
    }
    match action {
        ProvenanceAction::Backfill(arguments) if arguments.apply => println!(
            "{} objects, {} already recorded, {} written, {} failed, {} unattributed",
            outcome.objects_seen,
            outcome.sidecars_present,
            outcome.sidecars_written,
            outcome.write_failures.len(),
            outcome.unattributed.len()
        ),
        // Says "would write". A reporting run that printed "written" is the output an operator would
        // act on, and it would be false.
        ProvenanceAction::Backfill(_) => println!(
            "{} objects, {} already recorded, {} would be written, {} unattributed -- pass --apply to write",
            outcome.objects_seen,
            outcome.sidecars_present,
            outcome.sidecars_planned,
            outcome.unattributed.len()
        ),
        ProvenanceAction::Sweep => println!(
            "{} objects, {} recorded, {} missing a provenance record",
            outcome.objects_seen, outcome.sidecars_present, outcome.sidecars_missing.len()
        ),
    }
    Ok(Outcome::Complete)
}

/// Folds the sampled sessions' printed tape into the archive under the scope the action names.
///
/// The calendar comes from Alpaca and the tape from Massive, which is the same split the quote
/// pass uses: only the exchange publishes its own hours, and only the flat files hold every print.
async fn seed_trades(action: &TradeAction) -> Result<Outcome, SeedError> {
    match action {
        TradeAction::Measure(symbols) => return measure_trades(symbols).await,
        // Answered here for the same reason as a measure: it reads the archive against itself, so
        // demanding a vendor credential it will never use would be the only way it could fail.
        TradeAction::Scan(arguments) => {
            return scan_summary_coverage(archive::SessionFamily::Trades, arguments).await
        }
        TradeAction::Archive(_) | TradeAction::Repair(_) | TradeAction::Widen(_) => {}
    }
    // Resolved before any credential is read, so a repair with no symbol set is refused in the first
    // millisecond rather than after a calendar fetch.
    let scope = match action {
        TradeAction::Repair(symbols) => Scope::new(
            NameSelection::Named(symbols.symbols.required_names()?),
            SessionSelection::Present,
        )
        .map_err(|error| SeedError::Usage(error.to_string()))?,
        whole_market_action => whole_market_action
            .universe_scope()
            .ok_or_else(|| SeedError::Usage(format!("{action:?} folds no universe")))??,
    };
    let window = action.window().window()?;
    let stride = action.stride();

    let credentials = AlpacaCredentials::from_env().map_err(box_error)?;
    let days = TradingClient::from_env(credentials)
        .fetch_calendar(window.start.date(), window.end.date())
        .await
        .map_err(box_error)?;
    let calendar = TradingCalendar::from_days(days);
    let sampled = sample(&calendar, &window, stride);
    report_sample(&window, stride, &calendar, &sampled);

    // Bound before the source so it outlives the borrow, and built only where it is used: a repair
    // must not demand flat-file credentials to reach two names through Alpaca.
    let flat_files;
    let market_data;
    let source =
        match action {
            TradeAction::Repair(_) => {
                market_data = sip_market_data()?;
                archive::TradeSource::PerName(&market_data)
            }
            TradeAction::Archive(arguments) | TradeAction::Widen(arguments) => {
                flat_files = flat_file_client(&arguments.files).await?;
                archive::TradeSource::WholeSession(&flat_files)
            }
            // Returned above, before any credential is read. Named rather than swept into a catch-all so
            // a new action cannot silently inherit the flat-file route.
            TradeAction::Measure(_) | TradeAction::Scan(_) => return Err(SeedError::Usage(
                "a measure or scan pass writes nothing and is answered before a source is built"
                    .to_string(),
            )),
        };

    // Refused before a single print is fetched: past the floor Alpaca folds an opening auction
    // print the archive correctly excludes, adding 0.3-2.2% of session volume and varying by name.
    let unfaithful = source.unfaithful_sessions(&sampled);
    if let Some(earliest) = unfaithful.first() {
        return Err(SeedError::Usage(format!(
            "{source} trades are not faithful before {}: {} of {} sampled sessions are earlier, \
             from {earliest}. Writing them would add volume the archive correctly excludes; \
             re-fold from the raw tee instead.",
            source
                .faithful_from()
                .expect("a route that refuses a session has a floor"),
            unfaithful.len(),
            sampled.len(),
        )));
    }

    let bucket = bucket_name()?;
    let s3_client = fund::common::aws::s3_client().await;
    Ok(Outcome::Pass(
        archive::archive_trade_sessions(&s3_client, &source, &calendar, &bucket, &sampled, &scope)
            .await
            .map_err(box_error)?,
    ))
}

/// Folds named symbols' prints and prints their session figures, writing nothing.
///
/// Sequential on purpose, as the quote measurement is: this exists to read a handful of numbers off
/// real data and compare them against what the archive already holds.
async fn measure_trades(symbols: &TradeSymbolArguments) -> Result<Outcome, SeedError> {
    let named = symbols.symbols.required_names()?;
    let window = symbols.window.window()?;
    let (market_data, calendar) = quote_sources(&window).await?;
    let sampled = sample(&calendar, &window, symbols.stride);
    report_sample(&window, symbols.stride, &calendar, &sampled);

    // The exclusion counters are printed beside the totals because they are the first thing a
    // disagreement with the flat-file archive would be explained by: the two providers spell
    // conditions differently, so they can admit different prints.
    println!(
        "{:<8}{:<12}{:>10}{:>16}{:>18}{:>11}{:>10}{:>10}",
        "ticker", "session", "trades", "volume", "dollar_volume", "vwap", "inelig", "unresolv"
    );
    for session in &sampled {
        let Some((open, close)) = quotes::trading_hours(&calendar, *session) else {
            println!("{session}: not a published session");
            continue;
        };
        for ticker in &named {
            match trades::fold_session(&market_data, ticker, *session, open, close).await {
                Ok((summaries, fetch)) => print_trade_row(ticker, *session, &summaries, fetch),
                Err(error) => println!(
                    "{:<8}{:<12} failed: {error}",
                    ticker.as_str(),
                    session.to_string()
                ),
            }
        }
    }
    Ok(Outcome::Complete)
}

/// Prints one name's session row, which is the last summary the fold returns.
fn print_trade_row(
    ticker: &Ticker,
    session: SessionDate,
    summaries: &[TradeSummary],
    fetch: fund::common::alpaca::TradeFetch,
) {
    let Some(row) = summaries
        .iter()
        .find(|row| row.bar_interval() == BarInterval::OneDay)
    else {
        println!(
            "{:<8}{:<12} no prints (received {}, rejected {}, untaped {})",
            ticker.as_str(),
            session.to_string(),
            fetch.received,
            fetch.rejected,
            fetch.untaped
        );
        return;
    };
    println!(
        "{:<8}{:<12}{:>10}{:>16.2}{:>18.2}{:>11}{:>10}{:>10}",
        ticker.as_str(),
        session.to_string(),
        row.trade_count(),
        row.volume(),
        row.dollar_volume(),
        row.volume_weighted_average_price()
            .map(|price| format!("{price:.4}"))
            .unwrap_or_else(|| "none".to_string()),
        row.exclusions().volume_ineligible_trades(),
        row.exclusions().unresolved_condition_trades(),
    );
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

    let flat_files = flat_file_client(&arguments.files).await?;
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
    let window = symbols.window.window()?;
    let (market_data, calendar) = quote_sources(&window).await?;
    let sampled = sample(&calendar, &window, symbols.stride);
    report_sample(&window, symbols.stride, &calendar, &sampled);

    measure(
        &market_data,
        &calendar,
        &sampled,
        &named,
        symbols.cadence.intraday(),
    )
    .await;
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
async fn probe_flat_file(
    date: SessionDate,
    files: &FlatFileArguments,
) -> Result<Outcome, SeedError> {
    let client = flat_file_client(files).await?;
    let started = tokio::time::Instant::now();
    let (summary, _) = client
        .fold_quotes(date.date(), flatfiles::ForEach(|_ticker, _tick| {}))
        .await
        .map_err(box_error)?;
    let elapsed = started.elapsed().as_secs_f64();

    println!(
        "{}/{}",
        client.bucket(),
        client.object_key(flatfiles::RawDataset::Quotes, date.date())
    );
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
    files: &FlatFileArguments,
) -> Result<Outcome, SeedError> {
    let client = flat_file_client(files).await?;
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

    println!(
        "{}/{}",
        client.bucket(),
        client.object_key(flatfiles::RawDataset::Quotes, date.date())
    );
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
/// The SIP market-data client a per-name fold reads through.
///
/// SIP is pinned rather than read from `ALPACA_DATA_FEED`, for the reason [`quote_sources`] pins it:
/// IEX's prints are one venue's, and an environment variable could put two incomparable series under
/// one key.
fn sip_market_data() -> Result<MarketDataClient, SeedError> {
    let credentials = AlpacaCredentials::from_env().map_err(box_error)?;
    Ok(MarketDataClient::new(credentials, DataFeed::Sip))
}

/// The market data client, on the feed the archive is allowed to fold.
///
/// SIP is pinned rather than read from `ALPACA_DATA_FEED` for the reason `quote_sources` gives:
/// IEX's best bid and offer is not the national one.
async fn market_data_client() -> Result<MarketDataClient, Box<dyn std::error::Error>> {
    Ok(MarketDataClient::new(
        AlpacaCredentials::from_env()?,
        DataFeed::Sip,
    ))
}

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

/// The cadence a probe folds at, which reports numbers and writes no partition.
///
/// The only quote action left without a flag. Every action that touches the archive takes one,
/// because a fold at the wrong cadence lands in the wrong prefix.
const QUOTE_CADENCE: IntradayCadence = IntradayCadence::FiveMinute;

/// Which provider a fold reads from.
///
/// A backfill takes whole sessions off Massive's flat files, because five years one name at a time
/// is a hundred days of API calls. A repair takes named symbols from Alpaca, because it already
/// knows which names it wants and a whole file to reach two of them is seven gigabytes.
#[derive(Debug, Clone, Copy)]
enum QuoteProvider<'a> {
    /// Whole sessions off a flat file, which is the only variant with a file to source.
    WholeSession(&'a FlatFileArguments),
    PerName,
}

/// Folds the sampled sessions into the archive, which is the half a measurement skips.
async fn fold_quotes(
    source: &archive::QuoteSource<'_>,
    calendar: &TradingCalendar,
    sampled: &[SessionDate],
    scope: &Scope,
    cadence: IntradayCadence,
    foreign: ForeignProvider,
) -> Result<archive::PassSummary, Box<dyn std::error::Error>> {
    let bucket = bucket_name()?;
    let s3_client = fund::common::aws::s3_client().await;
    Ok(archive::archive_quote_sessions(
        &s3_client, source, calendar, &bucket, sampled, scope, cadence, foreign,
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
    cadence: IntradayCadence,
) {
    println!(
        "{:<8}{:<12}{:>10}{:>10}{:>10}{:>10}{:>10}{:>10}{:>10}{:>10}{:>12}",
        "ticker",
        "session",
        "mean_bp",
        "median_bp",
        "p90_bp",
        "first_bp",
        "min_bp",
        "bid_size",
        "ask_size",
        "quotes",
        "covered_s"
    );
    for session in sampled {
        let Some((open, close)) = quotes::trading_hours(calendar, *session) else {
            println!("{session}: not a published session");
            continue;
        };
        for ticker in symbols {
            match quotes::fold_session(market_data, ticker, *session, cadence, open, close).await {
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
        "{:<8}{:<12}{:>10.2}{:>10.2}{:>10.2}{:>10.2}{:>10.2}{:>10.1}{:>10.1}{:>10}{:>12.0}",
        ticker.as_str(),
        session.to_string(),
        basis_points(row),
        row.quoted_spread_basis_points_median().value(),
        row.quoted_spread_basis_points_ninetieth_percentile()
            .value(),
        opening,
        tightest,
        row.bid_size_mean(),
        row.ask_size_mean(),
        quotes_folded,
        row.covered_seconds()
    );
}

// --- Cross-cadence check --------------------------------------------------------------------

/// Folds one stored cadence up to another and reports whether they agree, writing nothing.
///
/// Needs AWS and nothing else. The population is what the finer prefix holds rather than what a
/// calendar publishes, so this runs against the archive as it stands and asks for no vendor
/// credential — which is what makes it usable after a subscription lapses.
async fn check_cadence(action: &CadenceAction) -> Result<Outcome, SeedError> {
    let arguments = action.arguments();
    let (from, to) = arguments.intervals()?;
    let window = arguments.window.window()?;
    let bucket = bucket_name()?;
    let s3_client = fund::common::aws::s3_client().await;

    let totals = archive::check_cadence_agreement(
        &s3_client,
        &bucket,
        action.family(),
        from,
        to,
        window.start,
        window.end,
        arguments.stride,
    )
    .await
    .map_err(box_error)?;

    // Named, not counted. The medians are not decomposable, so a run that printed only agreement
    // would read as covering the row -- which is the overstatement this whole check exists against.
    println!(
        "{} {} -> {}: {} of {} sessions agree over {} rows; {} columns are not decomposable and were not checked: {}",
        action.family(),
        from,
        to,
        totals.sessions_agreeing(),
        totals.sessions(),
        totals.compared(),
        action.family().opaque_columns().len(),
        action.family().opaque_columns().join(", ")
    );
    Ok(Outcome::Checked(totals))
}

// --- Shared plumbing ------------------------------------------------------------------------

/// The bucket every S3 subcommand writes into.
fn bucket_name() -> Result<String, Box<dyn std::error::Error>> {
    Ok(fund::common::aws::archive_bucket()?)
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

    /// The nightly command's parsed arguments, or a panic naming what clap rejected.
    fn nightly(arguments: &[&str]) -> NightlyArguments {
        let parsed = parse(&[&["archive-nightly"], arguments].concat())
            .unwrap_or_else(|error| panic!("archive-nightly {arguments:?} should parse: {error}"));
        match parsed.command {
            Command::ArchiveNightly(arguments) => arguments,
            other => panic!("expected a nightly run, got {other:?}"),
        }
    }

    #[test]
    fn test_a_nightly_run_reads_alpaca_unless_told_otherwise() {
        // Pinned to the literal rather than to NightlyProvider::default(), so flipping the default
        // back to the flat files after they lapse has to be done here too.
        assert!(matches!(nightly(&[]).provider, NightlyProvider::AlpacaRest));
    }

    #[test]
    fn test_both_provider_routes_are_reachable_by_name() {
        assert!(matches!(
            nightly(&["--provider", "alpaca-rest"]).provider,
            NightlyProvider::AlpacaRest
        ));
        assert!(matches!(
            nightly(&["--provider", "massive-flat-file"]).provider,
            NightlyProvider::MassiveFlatFile
        ));
    }

    #[test]
    fn test_a_provider_that_is_not_a_route_is_refused() {
        // `alpaca` alone named the vendor rather than the route, and Alpaca also has a stream this
        // does not use. It must fail rather than resolve to the REST arm.
        assert!(parse(&["archive-nightly", "--provider", "alpaca"]).is_err());
        assert!(parse(&["archive-nightly", "--provider", "massive"]).is_err());
    }

    #[test]
    fn test_the_nightly_defaults_are_a_working_night_not_a_placeholder() {
        let arguments = nightly(&[]);
        assert_eq!(arguments.lookback_sessions, 5);
        assert_eq!(arguments.budget_minutes, 240);
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

    fn cadence_action(arguments: &[&str]) -> CadenceAction {
        let parsed = parse(arguments).expect("valid arguments");
        match parsed.command {
            Command::ArchiveCadence { action } => action,
            _ => panic!("expected the cadence subcommand"),
        }
    }

    /// A whole-market quote fold, which is the only quote action that names its own cadence.
    fn quote_fold(arguments: &[&str]) -> QuoteFoldArguments {
        match quotes(arguments) {
            QuoteAction::Archive(arguments) | QuoteAction::Widen(arguments) => arguments,
            other => panic!("expected a whole-market fold, got {other:?}"),
        }
    }

    #[test]
    fn test_a_quote_fold_defaults_to_five_minutes_and_reaches_one_minute() {
        let window = ["--start", "2026-08-03", "--end", "2026-08-21"];
        for action in ["archive", "widen"] {
            let mut defaulted = vec!["equity-quotes", action];
            defaulted.extend_from_slice(&window);
            assert_eq!(
                quote_fold(&defaulted).cadence.intraday(),
                IntradayCadence::FiveMinute
            );

            let mut one = defaulted.clone();
            one.extend_from_slice(&["--cadence", "one_minute"]);
            assert_eq!(
                quote_fold(&one).cadence.intraday(),
                IntradayCadence::OneMinute
            );
        }
    }

    #[test]
    fn test_a_session_is_not_a_cadence_a_quote_fold_can_be_opened_at() {
        // A session row is the merge of the buckets rather than a grid of them, so `one_day` names
        // no fold. Refused by the value enum rather than checked after parsing.
        assert!(parse(&[
            "equity-quotes",
            "archive",
            "--start",
            "2026-08-03",
            "--end",
            "2026-08-21",
            "--cadence",
            "one_day",
        ])
        .is_err());
    }

    #[test]
    fn test_the_cadence_check_reads_both_families_and_defaults_to_the_session_row() {
        let window = ["--start", "2021-08-23", "--end", "2026-08-21"];

        let mut quotes = vec!["archive-cadence", "quotes"];
        quotes.extend_from_slice(&window);
        let action = cadence_action(&quotes);
        assert!(matches!(action.family(), archive::SummaryFamily::Quotes));
        assert_eq!(
            action.arguments().from.bar_interval(),
            BarInterval::OneMinute
        );
        assert_eq!(action.arguments().to.bar_interval(), BarInterval::OneDay);
        assert_eq!(action.arguments().stride, 1);

        let mut trades = vec!["archive-cadence", "trades", "--to", "five_minute"];
        trades.extend_from_slice(&window);
        let action = cadence_action(&trades);
        assert!(matches!(action.family(), archive::SummaryFamily::Trades));
        assert_eq!(
            action.arguments().to.bar_interval(),
            BarInterval::FiveMinute
        );
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

    /// Writing must be asked for. The pass touches every object in the archive, so a flag that has
    /// to be remembered in order to *avoid* writing is the wrong way round.
    #[test]
    fn test_provenance_backfill_does_not_write_without_apply() {
        let parsed = parse(&["archive-provenance", "backfill", "--from-logs", "/tmp/logs"])
            .expect("valid arguments");
        let Command::ArchiveProvenance {
            action: ProvenanceAction::Backfill(arguments),
        } = parsed.command
        else {
            panic!("expected a provenance backfill");
        };
        assert!(!arguments.apply, "the default must not write");

        let parsed = parse(&[
            "archive-provenance",
            "backfill",
            "--from-logs",
            "/tmp/logs",
            "--apply",
        ])
        .expect("valid arguments");
        let Command::ArchiveProvenance {
            action: ProvenanceAction::Backfill(arguments),
        } = parsed.command
        else {
            panic!("expected a provenance backfill");
        };
        assert!(arguments.apply);
    }

    /// A backfill with neither source would silently stamp nothing.
    #[test]
    fn test_provenance_backfill_needs_a_source() {
        assert!(parse(&["archive-provenance", "backfill"]).is_err());
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
        assert_eq!(arguments.quotes.stride, 1);

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

    /// `--source` chooses which copy of a flat file to read, so only the actions that open one may
    /// take it.
    ///
    /// A per-name route reaches Alpaca a symbol at a time and never opens a file, so accepting the
    /// flag there would report a source that changed nothing about the pass. Asserted as a refusal
    /// rather than as an ignored value, because clap is what has to do the rejecting.
    #[test]
    fn test_only_the_actions_that_open_a_file_take_a_source() {
        let window = ["--start", "2026-08-03", "--end", "2026-08-21"];
        for (action, extra) in [
            ("archive", &[][..]),
            ("widen", &[][..]),
            ("repair", &["--symbols", "AAPL"][..]),
            ("measure", &["--symbols", "AAPL"][..]),
        ] {
            let mut arguments = vec!["equity-quotes", action];
            arguments.extend_from_slice(&window);
            arguments.extend_from_slice(extra);
            assert!(
                parse(&arguments).is_ok(),
                "{action} must parse without a source"
            );

            arguments.extend_from_slice(&["--source", "archive"]);
            let opens_a_file = matches!(action, "archive" | "widen");
            assert_eq!(
                parse(&arguments).is_ok(),
                opens_a_file,
                "{action} with --source"
            );
        }

        // The probe takes a date rather than a window, and it is the only route that reads an
        // archived object without writing one -- so it is how a restore is checked at all.
        let probe = ["equity-quotes", "probe", "--date", "2026-08-03"];
        assert!(parse(&probe).is_ok(), "a probe must parse without a source");
        assert!(
            parse(&[probe.as_slice(), &["--source", "archive"]].concat()).is_ok(),
            "a probe must reach the archive"
        );
    }

    /// A probe reads one file rather than a sampled range, so it takes a date and at most a set of
    /// names to fold out of it — never a window or a stride, which only a sampled pass has.
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
    fn test_every_universe_quote_action_folds_the_whole_market() {
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
        assert_eq!(rendered("refold"), "every name, every session");

        // The boundary a routing change would erase: exactly one verb discards stored rows, and
        // widening that to `archive` or `widen` turns two ordinary backfills into destructive ones.
        assert_eq!(parsed("refold").foreign_provider(), ForeignProvider::Claim);
        for verb in ["archive", "widen"] {
            assert_eq!(
                parsed(verb).foreign_provider(),
                ForeignProvider::Refuse,
                "{verb} must refuse a foreign provider"
            );
        }

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
                    .expect("a whole-market action names a universe")
                    .expect("the whole market may write a partition")
                    .to_string(),
                other => panic!("expected a trade action, got {other:?}"),
            }
        };

        // Literals, not `whole_market(..).to_string()`: an expectation built from the call under
        // test moves with it and can never fail.
        assert_eq!(rendered("archive"), "every name, absent sessions only");
        assert_eq!(rendered("widen"), "every name, every session");

        // The repair derives no universe: it names its symbols, so a scope built here would be a
        // second answer to a question the symbol set has already answered.
        let repair = parse(
            &[
                ["equity-trades", "repair", "--symbols", "AAPL"].as_slice(),
                &window,
            ]
            .concat(),
        )
        .expect("a trade repair");
        match repair.command {
            Command::EquityTrades { action } => assert!(action.universe_scope().is_none()),
            other => panic!("expected a trade action, got {other:?}"),
        }
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

    /// The read-only route exists and takes the same window as the writing one, so checking what
    /// the feed says for a date costs nothing and writes nothing.
    #[test]
    fn test_a_reference_sweep_has_a_read_only_route() {
        let probe = Arguments::try_parse_from([
            "seed",
            "equity-reference",
            "probe",
            "--start",
            "2021-10-01",
            "--end",
            "2021-10-01",
        ])
        .expect("probe must parse");
        let Command::EquityReference { action } = probe.command else {
            panic!("expected a reference command");
        };
        assert!(matches!(action, ReferenceAction::Probe(_)));

        let archive = Arguments::try_parse_from([
            "seed",
            "equity-reference",
            "archive",
            "--start",
            "2021-10-01",
            "--end",
            "2021-12-31",
        ])
        .expect("archive must parse");
        let Command::EquityReference { action } = archive.command else {
            panic!("expected a reference command");
        };
        assert!(matches!(action, ReferenceAction::Archive(_)));
    }

    #[test]
    fn test_the_grid_report_takes_no_window_and_an_optional_date() {
        // No window, because the grid is derived from the archive rather than supplied. The date is
        // what lets the report name a quarter before one is actually owed.
        let today = Arguments::try_parse_from(["seed", "equity-reference", "grid"])
            .expect("grid must parse without a window");
        let Command::EquityReference { action } = today.command else {
            panic!("expected a reference command");
        };
        assert!(matches!(action, ReferenceAction::Grid { as_of: None }));

        let dated = Arguments::try_parse_from([
            "seed",
            "equity-reference",
            "grid",
            "--as-of",
            "2026-10-02",
        ])
        .expect("grid must accept a date");
        let Command::EquityReference { action } = dated.command else {
            panic!("expected a reference command");
        };
        let ReferenceAction::Grid { as_of } = action else {
            panic!("expected the grid report");
        };
        assert_eq!(
            as_of,
            Some(chrono::NaiveDate::from_ymd_opt(2026, 10, 2).expect("a date"))
        );
    }

    /// A sweep that left a session unwritten reports the count rather than exiting quietly.
    ///
    /// Keyed on sessions and not symbols: the ordinary residual of a whole-market sweep is a handful
    /// of symbols the feed has no record for, and failing on those is what made an earlier driver
    /// record no progress across an entire five-year run.
    #[test]
    fn test_an_unwritten_session_is_named_in_the_failure() {
        let incomplete = ReferenceSweepIncomplete {
            written: 20,
            failed: 1,
        };

        let rendered = incomplete.to_string();
        assert!(rendered.contains("20"), "{rendered}");
        assert!(rendered.contains("1 session"), "{rendered}");
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
