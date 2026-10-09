//! Trades one session on the paper account: warms the market state on the previous session's archived bars, waits for
//! the open, then folds the tape and trades each decision bar until the close. Exits 0 when the session ran to the close
//! or found none, and every record shipped; 1 when it halted, stopped or did not ship; 2 when it could not start.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::io;
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Mutex;
use std::time::Duration;

use chrono::{DateTime, Utc};
use tokio::sync::mpsc;
use tokio::time::MissedTickBehavior;
use tracing::Instrument;
use tracing_subscriber::Layer;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, fmt};

use fund::archive::reference::{SnapshotError, latest_conditions};
use fund::archive::trade_bars::{DecodeRefusal, decode};
use fund::archive::{Archive, ArchiveError};
use fund::broker::{Broker, BrokerError, PaperAccount};
use fund::common::book::ValuationRefusal;
use fund::common::journal::{Commit, Observation, RunId, SessionOpened};
use fund::common::market::record::BarInterval;
use fund::common::market::state::MarketState;
use fund::common::market::{Price, Symbol};
use fund::common::playbook::{Playbook, PlaybookRead, PlaybookRefusal, Played};
use fund::common::standing::{HaltCause, SessionClosed, SessionEnding};
use fund::common::storage::{Host, Key, Origin, Provider, Service};
use fund::common::time::calendar::TradingCalendar;
use fund::common::time::{SessionDate, SessionRange};
use fund::ingest::alpaca::Alpaca;
use fund::ingest::alpaca::feed::{Feed, FeedEvent};
use fund::ingest::alpaca::stream::StreamMessage;
use fund::ingest::{FetchError, VariableRefusal};
use fund::journal::{Journal, built_commit, lock};
use fund::parameter::log_directory_from_environment;
use fund::records::{log_file_name, ship, shipped_filter};
use fund::trader::parameters::Parameters;
use fund::trader::{Session, SessionError, Standing, last_closes, warm};
use uuid::Uuid;

const SERVICE: &str = "trader";
const REFUSED_TO_START: u8 = 2;
/// Calendar days read back for the previous session, past any run of holidays and a weekend.
const CALENDAR_DAYS_BACK: i64 = 14;
/// Feed events held while the session is busy trading, so the socket keeps being read.
const EVENTS_BUFFERED: usize = 100_000;
const TICK: Duration = Duration::from_secs(1);

#[tokio::main]
async fn main() -> ExitCode {
    let today = SessionDate::at(Utc::now());
    let service = Service::new(SERVICE).expect("the service name is one path segment");
    let resolved = Parameters::from_environment();
    let log_file = log_directory_from_environment().map(|directory| {
        std::fs::create_dir_all(&directory).and_then(|()| {
            File::options()
                .create(true)
                .append(true)
                .open(directory.join(log_file_name(&service, today)))
        })
    });
    // A refused log directory is reported with the other parameters; only a directory that resolved can fail to open.
    let (log_writer, log_file_error) = match log_file {
        Ok(Ok(file)) => (Some(Mutex::new(file)), None),
        Ok(Err(error)) => (None, Some(error)),
        Err(_) => (None, None),
    };
    tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .with(
            fmt::layer()
                .json()
                .with_current_span(true)
                .with_span_list(false),
        )
        .with(log_writer.map(|writer| {
            fmt::layer()
                .json()
                .with_current_span(true)
                .with_span_list(false)
                .with_writer(writer)
                .with_filter(shipped_filter())
        }))
        .init();
    let run_id = RunId::new(Uuid::new_v4());
    let commit = built_commit();
    let span = tracing::info_span!(
        "run",
        %run_id,
        commit = commit.as_ref().map_or("unknown", Commit::as_str),
    );
    async move {
        let (parameters, configuration) = match resolved {
            Ok(resolved) => resolved,
            Err(refusal) => {
                tracing::error!(%refusal, "Parameters refused");
                return ExitCode::from(REFUSED_TO_START);
            }
        };
        if let Some(error) = log_file_error {
            tracing::error!(%error, "Log file did not open");
            return ExitCode::from(REFUSED_TO_START);
        }
        let mut journal = match Journal::open(parameters.journal_directory(), run_id) {
            Ok(journal) => journal,
            Err(error) => {
                tracing::error!(%error, "Journal did not open");
                return ExitCode::from(REFUSED_TO_START);
            }
        };
        // Held until the process exits, so two traders never send orders against one account.
        let _lock = match lock(parameters.journal_directory(), &service) {
            Ok(file) => file,
            Err(refusal) => {
                tracing::error!(%refusal, "Another run is under way or the lock is unavailable");
                return ExitCode::from(REFUSED_TO_START);
            }
        };
        if let Err(error) = journal.append(
            Utc::now(),
            Observation::ConfigurationResolved(configuration),
        ) {
            tracing::error!(%error, "Configuration was not journaled");
            return ExitCode::from(REFUSED_TO_START);
        }
        // From here every exit runs through the closing step below, so the journal always holds the session's end.
        let sdk_configuration = aws_config::load_from_env().await;
        let records = Archive::records(&sdk_configuration);
        let archive = Archive::market_data(&sdk_configuration);
        let traded = match (&records, &archive) {
            (Ok(_), Ok(archive)) => trade(&parameters, archive, &mut journal, today).await,
            (Err(refusal), _) | (_, Err(refusal)) => Err(Stopped::BeforeTheOpen(
                StartRefusal::Archive(refusal.clone()),
            )),
        };
        let closed = SessionClosed::new(today, ending(&traded));
        let journaled = journal.append(Utc::now(), Observation::SessionClosed(closed));
        let outcome = match traded {
            Ok(Ran::NoSession) => {
                tracing::info!(%today, "No session today");
                ExitCode::SUCCESS
            }
            Ok(Ran::ToTheClose) => {
                tracing::info!("Session ran to the close");
                ExitCode::SUCCESS
            }
            Ok(Ran::Halted(cause)) => {
                tracing::error!(?cause, "Session halted");
                ExitCode::FAILURE
            }
            Ok(Ran::HeldAtTheClose { positions }) => {
                tracing::error!(positions, "Positions held at the close");
                ExitCode::FAILURE
            }
            Err(Stopped::BeforeTheOpen(refusal)) => {
                tracing::error!(%refusal, "Session did not start");
                ExitCode::from(REFUSED_TO_START)
            }
            Err(Stopped::Trading(refusal)) => {
                tracing::error!(%refusal, "Session stopped");
                ExitCode::FAILURE
            }
        };
        let outcome = match journaled {
            Ok(()) => outcome,
            Err(error) => {
                tracing::error!(%error, "Session close was not journaled");
                if outcome == ExitCode::SUCCESS {
                    ExitCode::FAILURE
                } else {
                    outcome
                }
            }
        };
        let records = match records {
            Ok(records) => records,
            // The refusal was logged as the reason the session did not start.
            Err(_) => return outcome,
        };
        // Shipped whatever the session did, since a failed session's records are the ones most worth reading.
        let shipped = ship(
            &records,
            Host::Trader,
            &service,
            parameters.journal_directory(),
            parameters.log_directory(),
            SessionDate::at(Utc::now()),
        )
        .await;
        let mut all_shipped = true;
        for (key, outcome) in &shipped {
            match outcome {
                Ok(()) => tracing::info!(path = key.path(), "Records shipped"),
                Err(cause) => {
                    all_shipped = false;
                    tracing::error!(path = key.path(), %cause, "Records not shipped");
                }
            }
        }
        match (outcome == ExitCode::SUCCESS, all_shipped) {
            (true, false) => ExitCode::FAILURE,
            (true, true) | (false, true | false) => outcome,
        }
    }
    .instrument(span)
    .await
}

/// How a session ended without error.
enum Ran {
    NoSession,
    /// Ran to the close and the account held nothing there.
    ToTheClose,
    Halted(HaltCause),
    /// Ran to the close but the account still held positions, as when a tape gap kept the session from going flat.
    HeldAtTheClose {
        positions: usize,
    },
}

/// Why a session stopped, before any order could be sent or after.
enum Stopped {
    BeforeTheOpen(StartRefusal),
    Trading(TradingStop),
}

/// Why a session did not start, kept as the refused step's own cause.
enum StartRefusal {
    Archive(VariableRefusal),
    Alpaca(VariableRefusal),
    PlaybookUnread {
        path: PathBuf,
        error: io::Error,
    },
    Playbook {
        path: PathBuf,
        refusal: PlaybookRefusal,
    },
    NotPaper(BrokerError),
    Calendar(FetchError),
    AfterClose {
        close: DateTime<Utc>,
    },
    Conditions(SnapshotError),
    Book(BrokerError),
    PreviousSession(PreviousRefusal),
    Opening(ValuationRefusal),
    Journal(io::Error),
}

impl std::fmt::Display for StartRefusal {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Archive(refusal) => write!(formatter, "the archive client refused: {refusal}"),
            Self::Alpaca(refusal) => write!(formatter, "the Alpaca client refused: {refusal}"),
            Self::PlaybookUnread { path, error } => {
                write!(formatter, "playbook {} unread: {error}", path.display())
            }
            Self::Playbook { path, refusal } => {
                write!(formatter, "playbook {} refused: {refusal}", path.display())
            }
            Self::NotPaper(error) => write!(formatter, "the paper account refused: {error}"),
            Self::Calendar(error) => write!(formatter, "the calendar was not read: {error}"),
            Self::AfterClose { close } => write!(formatter, "the session closed at {close}"),
            Self::Conditions(error) => write!(formatter, "the conditions were not read: {error}"),
            Self::Book(error) => write!(formatter, "the book was not read: {error}"),
            Self::PreviousSession(refusal) => write!(formatter, "{refusal}"),
            Self::Opening(refusal) => write!(formatter, "the opening was not valued: {refusal}"),
            Self::Journal(error) => write!(formatter, "the journal refused a write: {error}"),
        }
    }
}

/// Why the previous session's bars did not warm the state.
enum PreviousRefusal {
    NoTradingDay { today: SessionDate },
    Archive(ArchiveError),
    NotArchived { key: Key },
    Decode { key: Key, refusal: DecodeRefusal },
}

impl std::fmt::Display for PreviousRefusal {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NoTradingDay { today } => write!(
                formatter,
                "no trading day in the {CALENDAR_DAYS_BACK} days before {today}"
            ),
            Self::Archive(error) => write!(formatter, "the previous session was not read: {error}"),
            Self::NotArchived { key } => write!(formatter, "{} is not archived", key.path()),
            Self::Decode { key, refusal } => {
                write!(formatter, "{} not decoded: {refusal}", key.path())
            }
        }
    }
}

/// Why a session that had opened stopped trading.
enum TradingStop {
    Session(SessionError),
    /// The feed never ends, so a closed channel means its task panicked.
    FeedEnded,
    Book(BrokerError),
}

impl std::fmt::Display for TradingStop {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Session(error) => write!(formatter, "{error}"),
            Self::FeedEnded => write!(formatter, "the feed task ended"),
            Self::Book(error) => write!(formatter, "the book was not read at the close: {error}"),
        }
    }
}

/// How a session ended, as journaled; a halt's cause is journaled when it happens, and a stop's reason is logged.
fn ending(traded: &Result<Ran, Stopped>) -> SessionEnding {
    match traded {
        Ok(Ran::NoSession) => SessionEnding::NoSession,
        Ok(Ran::ToTheClose) => SessionEnding::ToTheClose,
        Ok(Ran::Halted(_)) => SessionEnding::Halted,
        Ok(Ran::HeldAtTheClose { positions }) => SessionEnding::HeldAtTheClose {
            positions: *positions,
        },
        Err(Stopped::BeforeTheOpen(_)) => SessionEnding::StoppedBeforeTheOpen,
        Err(Stopped::Trading(_)) => SessionEnding::StoppedTrading,
    }
}

/// Reads and journals the playbook, so the session's records name the playbook it traded under.
fn read_playbook(parameters: &Parameters, journal: &mut Journal) -> Result<Played, StartRefusal> {
    let path = parameters.playbook().to_path_buf();
    let contents = match std::fs::read_to_string(&path) {
        Ok(contents) => contents,
        Err(error) => return Err(StartRefusal::PlaybookUnread { path, error }),
    };
    let playbook = match Playbook::parse(&contents) {
        Ok(playbook) => playbook,
        Err(refusal) => return Err(StartRefusal::Playbook { path, refusal }),
    };
    journal
        .append(
            Utc::now(),
            Observation::PlaybookRead(PlaybookRead::new(contents)),
        )
        .map_err(StartRefusal::Journal)?;
    Ok(playbook.play(parameters.universe().symbols()))
}

async fn trade(
    parameters: &Parameters,
    archive: &Archive,
    journal: &mut Journal,
    today: SessionDate,
) -> Result<Ran, Stopped> {
    let refused = Stopped::BeforeTheOpen;
    let strategy = read_playbook(parameters, journal).map_err(refused)?;
    let http_client = reqwest::Client::new();
    let (tape, account) = match (
        Alpaca::from_environment(http_client.clone()),
        Alpaca::from_environment(http_client),
    ) {
        (Ok(tape), Ok(account)) => (tape, account),
        (Err(refusal), _) | (_, Err(refusal)) => {
            return Err(refused(StartRefusal::Alpaca(refusal)));
        }
    };
    let broker =
        PaperAccount::new(account).map_err(|error| refused(StartRefusal::NotPaper(error)))?;
    let calendar = tape
        .calendar(
            SessionRange::single(today)
                .reaching_back_to(today.plus_calendar_days(-CALENDAR_DAYS_BACK)),
        )
        .await
        .map_err(|error| refused(StartRefusal::Calendar(error)))?;
    let Some((open, close)) = calendar.session(today).map(|session| session.hours()) else {
        return Ok(Ran::NoSession);
    };
    if Utc::now() >= close {
        return Err(refused(StartRefusal::AfterClose { close }));
    }
    let (_, conditions) = latest_conditions(archive)
        .await
        .map_err(|error| refused(StartRefusal::Conditions(error)))?;
    let book = broker
        .book()
        .await
        .map_err(|error| refused(StartRefusal::Book(error)))?;
    // Names held outside the universe are watched too, since the book cannot be valued without their prices.
    let symbols: BTreeSet<Symbol> = parameters
        .universe()
        .symbols()
        .iter()
        .chain(book.positions().keys())
        .cloned()
        .collect();
    let (previous, closes) = previous_bars(archive, &calendar, today, &symbols)
        .await
        .map_err(|refusal| refused(StartRefusal::PreviousSession(refusal)))?;
    let opening = book
        .value(|symbol| closes.get(symbol).copied())
        .map_err(|refusal| refused(StartRefusal::Opening(refusal)))?;
    journal
        .append(
            Utc::now(),
            Observation::SessionOpened(SessionOpened::new(today, &book, opening)),
        )
        .map_err(|error| refused(StartRefusal::Journal(error)))?;
    tracing::info!(
        %open,
        %close,
        cash = book.cash().dollars(),
        positions = book.positions().len(),
        opening = opening.dollars(),
        "Session opened"
    );
    sleep_until(open).await;
    let mut session = Session::new(
        strategy,
        parameters.settings(),
        calendar,
        today,
        conditions,
        previous,
        book,
        opening,
        Utc::now(),
    );
    let (sender, mut events) = mpsc::channel(EVENTS_BUFFERED);
    let mut feed = Feed::new(tape, symbols.into_iter().collect(), open);
    let reading = tokio::spawn(async move {
        loop {
            if sender.send(feed.next().await).await.is_err() {
                break;
            }
        }
    });
    let mut ticks = tokio::time::interval(TICK);
    ticks.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let ran = loop {
        tokio::select! {
            event = events.recv() => match event {
                Some(event) => {
                    report(&event);
                    if let Err(error) = session.observe(Utc::now(), &event, journal) {
                        break Err(Stopped::Trading(TradingStop::Session(error)));
                    }
                }
                None => break Err(Stopped::Trading(TradingStop::FeedEnded)),
            },
            _ = ticks.tick() => {
                let now = Utc::now();
                if now >= close {
                    break match broker.book().await {
                        Ok(held) if held.positions().is_empty() => Ok(Ran::ToTheClose),
                        Ok(held) => Ok(Ran::HeldAtTheClose { positions: held.positions().len() }),
                        Err(error) => Err(Stopped::Trading(TradingStop::Book(error))),
                    };
                }
                if let Err(error) = session.advance(now, &broker, journal).await {
                    break Err(Stopped::Trading(TradingStop::Session(error)));
                }
                match session.standing() {
                    Standing::Trading => {}
                    Standing::Halted(cause) => break Ok(Ran::Halted(cause.clone())),
                }
            }
        }
    };
    reading.abort();
    ran
}

/// The previous session's one-minute trade bars of `symbols` as the archive derived them, folded into a state, with each
/// symbol's last close among them.
async fn previous_bars(
    archive: &Archive,
    calendar: &TradingCalendar,
    today: SessionDate,
    symbols: &BTreeSet<Symbol>,
) -> Result<(MarketState, BTreeMap<Symbol, Price>), PreviousRefusal> {
    let previous = calendar
        .previous_trading_day(today)
        .ok_or(PreviousRefusal::NoTradingDay { today })?;
    let key = Key::Trades {
        provider: Provider::Alpaca,
        origin: Origin::Derived,
        interval: BarInterval::OneMinute,
        session: previous,
    };
    let bytes = match archive.get(&key).await.map_err(PreviousRefusal::Archive)? {
        Some(bytes) => bytes,
        None => return Err(PreviousRefusal::NotArchived { key }),
    };
    let (bars, _) = match decode(&key, bytes) {
        Ok(decoded) => decoded,
        Err(refusal) => return Err(PreviousRefusal::Decode { key, refusal }),
    };
    let bars: Vec<_> = bars
        .into_iter()
        .filter(|bar| symbols.contains(bar.symbol()))
        .collect();
    let closes = last_closes(&bars);
    Ok((warm(bars, symbols), closes))
}

/// Logs what the feed reports of its own gaps, which the session journals only as continuity changes, and of messages
/// it could not read, which the journal does not hold.
fn report(event: &FeedEvent) {
    match event {
        FeedEvent::Lost { cause } => tracing::warn!(%cause, "Stream lost"),
        FeedEvent::Reopened { attempts } => tracing::info!(attempts, "Stream reopened"),
        FeedEvent::Backfilled { since, fresh } => {
            tracing::info!(%since, fresh, "Tape backfilled");
        }
        FeedEvent::BackfillFailed { cause } => tracing::warn!(%cause, "Backfill failed"),
        FeedEvent::Message(StreamMessage::Refused { code, message }) => {
            tracing::warn!(code, message, "Stream refused a request");
        }
        FeedEvent::Message(StreamMessage::Unrecognized { kind, raw }) => {
            tracing::warn!(kind, raw, "Stream message unrecognized");
        }
        FeedEvent::Message(StreamMessage::Malformed { cause, raw }) => {
            tracing::warn!(%cause, raw, "Stream message malformed");
        }
        FeedEvent::Message(
            StreamMessage::Connected
            | StreamMessage::Authenticated
            | StreamMessage::Subscribed(_)
            | StreamMessage::Trade { .. }
            | StreamMessage::Quote(_),
        ) => {}
    }
}

/// Waits for `instant`, returning at once when it has passed.
async fn sleep_until(instant: DateTime<Utc>) {
    if let Ok(wait) = (instant - Utc::now()).to_std() {
        tokio::time::sleep(wait).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;
    use fund::execution::{JournalFailed, ReconcileFailed};

    fn disk_full() -> io::Error {
        io::Error::other("disk full")
    }

    fn previous_key() -> Key {
        Key::Trades {
            provider: Provider::Alpaca,
            origin: Origin::Derived,
            interval: BarInterval::OneMinute,
            session: SessionDate::from_date(NaiveDate::from_ymd_opt(2026, 10, 7).unwrap()),
        }
    }

    const PREVIOUS_PATH: &str = "data/equity/stage=parsed/trades/provider=alpaca/origin=derived/interval=one_minute/year=2026/month=10/day=07/data.parquet";

    #[test]
    fn test_each_start_refusal_displays_its_cause() {
        let close: DateTime<Utc> = "2026-10-08T20:00:00Z".parse().unwrap();
        let path = PathBuf::from("playbook.toml");
        let displayed = [
            StartRefusal::Archive(VariableRefusal::Missing { name: "BUCKET" }),
            StartRefusal::Alpaca(VariableRefusal::Missing { name: "KEY" }),
            StartRefusal::PlaybookUnread {
                path: path.clone(),
                error: disk_full(),
            },
            StartRefusal::Playbook {
                path,
                refusal: PlaybookRefusal::Empty,
            },
            StartRefusal::NotPaper(BrokerError::NotPaper),
            StartRefusal::Calendar(FetchError::Refused {
                status: 503,
                body: "busy".to_string(),
            }),
            StartRefusal::AfterClose { close },
            StartRefusal::Conditions(SnapshotError::Absent),
            StartRefusal::Book(BrokerError::NotPaper),
            StartRefusal::PreviousSession(PreviousRefusal::NotArchived {
                key: previous_key(),
            }),
            StartRefusal::Opening(ValuationRefusal::Unpriced {
                symbol: Symbol::new("AAPL").unwrap(),
            }),
            StartRefusal::Journal(disk_full()),
        ]
        .map(|refusal| refusal.to_string());
        assert_eq!(
            displayed,
            [
                "the archive client refused: BUCKET is not set",
                "the Alpaca client refused: KEY is not set",
                "playbook playbook.toml unread: disk full",
                "playbook playbook.toml refused: the playbook has no entries",
                "the paper account refused: the Alpaca keys trade live, not on paper",
                "the calendar was not read: refused with 503: busy",
                "the session closed at 2026-10-08 20:00:00 UTC",
                "the conditions were not read: no conditions table in the archive",
                "the book was not read: the Alpaca keys trade live, not on paper",
                &format!("{PREVIOUS_PATH} is not archived"),
                "the opening was not valued: the book holds AAPL with no price",
                "the journal refused a write: disk full",
            ]
        );
    }

    #[test]
    fn test_each_previous_refusal_displays_its_cause() {
        let displayed = [
            PreviousRefusal::NoTradingDay {
                today: SessionDate::from_date(NaiveDate::from_ymd_opt(2026, 10, 8).unwrap()),
            },
            PreviousRefusal::Archive(ArchiveError::Get {
                path: "a/b".to_string(),
                reason: "timed out".to_string(),
            }),
            PreviousRefusal::NotArchived {
                key: previous_key(),
            },
            PreviousRefusal::Decode {
                key: previous_key(),
                refusal: DecodeRefusal::NotATradesKey,
            },
        ]
        .map(|refusal| refusal.to_string());
        assert_eq!(
            displayed,
            [
                "no trading day in the 14 days before 2026-10-08",
                "the previous session was not read: reading a/b failed: timed out",
                &format!("{PREVIOUS_PATH} is not archived"),
                &format!("{PREVIOUS_PATH} not decoded: the key does not name trade bars"),
            ]
        );
    }

    #[test]
    fn test_each_trading_stop_displays_its_cause() {
        let displayed = [
            TradingStop::Session(SessionError::Journal(JournalFailed::before_any_order(
                disk_full(),
            ))),
            TradingStop::Session(SessionError::Reconcile(ReconcileFailed::Journal(
                JournalFailed::before_any_order(disk_full()),
            ))),
            TradingStop::Session(SessionError::Reconcile(ReconcileFailed::Unread(
                BrokerError::NotPaper,
            ))),
            TradingStop::FeedEnded,
            TradingStop::Book(BrokerError::NotPaper),
        ]
        .map(|stop| stop.to_string());
        assert_eq!(
            displayed,
            [
                "the journal refused a write after 0 orders: disk full",
                "the journal refused a write after 0 orders: disk full",
                "the broker's book was not read to reconcile: the Alpaca keys trade live, not on paper",
                "the feed task ended",
                "the book was not read at the close: the Alpaca keys trade live, not on paper",
            ]
        );
    }

    #[test]
    fn test_ending_journals_each_way_a_session_ends() {
        let close: DateTime<Utc> = "2026-10-08T20:00:00Z".parse().unwrap();
        let journaled = [
            Ok(Ran::NoSession),
            Ok(Ran::ToTheClose),
            Ok(Ran::Halted(HaltCause::Diverged)),
            Ok(Ran::HeldAtTheClose { positions: 3 }),
            Err(Stopped::BeforeTheOpen(StartRefusal::AfterClose { close })),
            Err(Stopped::Trading(TradingStop::FeedEnded)),
        ]
        .map(|traded| serde_json::to_string(&ending(&traded)).unwrap());
        assert_eq!(
            journaled,
            [
                r#""no_session""#,
                r#""to_the_close""#,
                r#""halted""#,
                r#"{"held_at_the_close":{"positions":3}}"#,
                r#""stopped_before_the_open""#,
                r#""stopped_trading""#,
            ]
        );
    }
}
