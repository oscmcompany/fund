//! Trades one session on the paper account: warms the market state on the previous session's archived bars, waits for
//! the open, then folds the tape and trades each decision bar until the close. Exits 0 when the session ran to the close
//! or found none, and every record shipped; 1 when it halted, stopped or did not ship; 2 when it could not start.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
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

use fund::archive::Archive;
use fund::archive::reference::latest_conditions;
use fund::archive::trade_bars::decode;
use fund::broker::Broker;
use fund::broker::PaperAccount;
use fund::common::journal::{Commit, Observation, RunId, SessionOpened};
use fund::common::market::record::BarInterval;
use fund::common::market::state::MarketState;
use fund::common::market::{Price, Symbol};
use fund::common::playbook::{Playbook, PlaybookRead, Played};
use fund::common::storage::{Host, Key, Origin, Provider, Service, TradesKey};
use fund::common::time::calendar::TradingCalendar;
use fund::common::time::{SessionDate, SessionRange};
use fund::ingest::alpaca::Alpaca;
use fund::ingest::alpaca::feed::{Feed, FeedEvent};
use fund::ingest::alpaca::stream::StreamMessage;
use fund::journal::{Journal, built_commit, lock};
use fund::parameter::log_directory_from_environment;
use fund::records::{log_file_name, ship, shipped_filter};
use fund::trader::parameters::Parameters;
use fund::trader::{Session, last_closes, warm};
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
        let strategy = match read_playbook(&parameters, &mut journal) {
            Ok(strategy) => strategy,
            Err(refusal) => {
                tracing::error!(%refusal, path = %parameters.playbook().display(), "Playbook refused");
                return ExitCode::from(REFUSED_TO_START);
            }
        };
        let sdk_configuration = aws_config::load_from_env().await;
        let (archive, records) = match (
            Archive::market_data(&sdk_configuration),
            Archive::records(&sdk_configuration),
        ) {
            (Ok(archive), Ok(records)) => (archive, records),
            (Err(refusal), _) | (_, Err(refusal)) => {
                tracing::error!(%refusal, "Client configuration refused");
                return ExitCode::from(REFUSED_TO_START);
            }
        };
        let traded = trade(&parameters, strategy, &archive, &mut journal, today).await;
        let outcome = match traded {
            Ok(Ran::NoSession) => {
                tracing::info!(%today, "No session today");
                ExitCode::SUCCESS
            }
            Ok(Ran::ToTheClose) => {
                tracing::info!("Session ran to the close");
                ExitCode::SUCCESS
            }
            Ok(Ran::Halted) => {
                tracing::error!("Session halted");
                ExitCode::FAILURE
            }
            Ok(Ran::HeldAtTheClose { positions }) => {
                tracing::error!(positions, "Positions held at the close");
                ExitCode::FAILURE
            }
            Err(Stopped::BeforeTheOpen(reason)) => {
                tracing::error!(reason, "Session did not start");
                ExitCode::from(REFUSED_TO_START)
            }
            Err(Stopped::Trading(reason)) => {
                tracing::error!(reason, "Session stopped");
                ExitCode::FAILURE
            }
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
    Halted,
    /// Ran to the close but the account still held positions, as when a tape gap kept the session from going flat.
    HeldAtTheClose {
        positions: usize,
    },
}

/// Why a session stopped, before any order could be sent or after.
enum Stopped {
    BeforeTheOpen(String),
    Trading(String),
}

/// Reads and journals the playbook, so the session's records name the playbook it traded under.
fn read_playbook(parameters: &Parameters, journal: &mut Journal) -> Result<Played, String> {
    let contents =
        std::fs::read_to_string(parameters.playbook()).map_err(|error| error.to_string())?;
    let playbook = Playbook::parse(&contents).map_err(|refusal| refusal.to_string())?;
    journal
        .append(
            Utc::now(),
            Observation::PlaybookRead(PlaybookRead::new(contents)),
        )
        .map_err(|error| error.to_string())?;
    Ok(playbook.play(parameters.universe().symbols()))
}

async fn trade(
    parameters: &Parameters,
    strategy: Played,
    archive: &Archive,
    journal: &mut Journal,
    today: SessionDate,
) -> Result<Ran, Stopped> {
    let refused = Stopped::BeforeTheOpen;
    let http_client = reqwest::Client::new();
    let (tape, account) = match (
        Alpaca::from_environment(http_client.clone()),
        Alpaca::from_environment(http_client),
    ) {
        (Ok(tape), Ok(account)) => (tape, account),
        (Err(refusal), _) | (_, Err(refusal)) => return Err(refused(format!("{refusal:?}"))),
    };
    let broker = PaperAccount::new(account).map_err(|error| refused(error.to_string()))?;
    let calendar = tape
        .calendar(
            SessionRange::single(today)
                .reaching_back_to(today.plus_calendar_days(-CALENDAR_DAYS_BACK)),
        )
        .await
        .map_err(|error| refused(error.to_string()))?;
    let Some((open, close)) = calendar.session(today).map(|session| session.hours()) else {
        return Ok(Ran::NoSession);
    };
    if Utc::now() >= close {
        return Err(refused(format!("the session closed at {close}")));
    }
    let (_, conditions) = latest_conditions(archive)
        .await
        .map_err(|error| refused(error.to_string()))?;
    let book = broker
        .book()
        .await
        .map_err(|error| refused(error.to_string()))?;
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
        .map_err(refused)?;
    let opening = book
        .value(|symbol| closes.get(symbol).copied())
        .map_err(|refusal| refused(format!("{refusal:?}")))?;
    journal
        .append(
            Utc::now(),
            Observation::SessionOpened(SessionOpened::new(today, &book, opening)),
        )
        .map_err(|error| refused(error.to_string()))?;
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
                    session.observe(&event);
                }
                // The feed never ends, so a closed channel means its task panicked.
                None => break Err(Stopped::Trading("the feed task ended".to_string())),
            },
            _ = ticks.tick() => {
                let now = Utc::now();
                if now >= close {
                    break match broker.book().await {
                        Ok(held) if held.positions().is_empty() => Ok(Ran::ToTheClose),
                        Ok(held) => Ok(Ran::HeldAtTheClose { positions: held.positions().len() }),
                        Err(error) => Err(Stopped::Trading(error.to_string())),
                    };
                }
                if let Err(error) = session.advance(now, &broker, journal).await {
                    break Err(Stopped::Trading(format!("{error:?}")));
                }
                if session.halted() {
                    break Ok(Ran::Halted);
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
) -> Result<(MarketState, BTreeMap<Symbol, Price>), String> {
    let previous = calendar
        .previous_trading_day(today)
        .ok_or_else(|| format!("no trading day in the {CALENDAR_DAYS_BACK} days before {today}"))?;
    let key = TradesKey::new(
        Provider::Alpaca,
        Origin::Derived,
        BarInterval::OneMinute,
        previous,
    );
    let bytes = archive
        .get(&key.into())
        .await
        .map_err(|error| error.to_string())?
        .ok_or_else(|| format!("{} is not archived", Key::from(key).path()))?;
    let (bars, _) = decode(&key, bytes).map_err(|refusal| format!("{refusal:?}"))?;
    let bars: Vec<_> = bars
        .into_iter()
        .filter(|bar| symbols.contains(bar.symbol()))
        .collect();
    let closes = last_closes(&bars);
    Ok((warm(bars, symbols), closes))
}

/// Logs what the feed reports of its own gaps and of messages it could not read, which the journal does not hold.
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
