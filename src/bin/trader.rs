//! Trades one session on the paper account: warms the state on the previous session's archived bars, then builds each
//! minute from REST trades once it has ended and trades each decision bar until the close. Exits 0 when it ran to the
//! close or found none and every record shipped; 1 when it halted, stopped or did not ship; 2 when it could not start.

use std::collections::{BTreeMap, BTreeSet};
use std::io;
use std::process::ExitCode;
use std::time::Duration;

use chrono::{DateTime, TimeDelta, Utc};
use tokio::time::MissedTickBehavior;
use tracing::Instrument;

use fund::broker::{Broker, BrokerError, PaperAccount};
use fund::common::book::{Fill, ValuationRefusal};
use fund::common::journal::{Observation, SessionOpened};
use fund::common::market::record::BarInterval;
use fund::common::market::state::MarketState;
use fund::common::market::{Price, Symbol};
use fund::common::playbook::{Playbook, PlaybookRead, PlaybookRefusal, Played};
use fund::common::standing::{HaltCause, SessionClosed, SessionEnding};
use fund::common::storage::{ConfigurationKey, Host, Key, ObjectKey, Provider, Service, TradesKey};
use fund::common::time::calendar::TradingCalendar;
use fund::common::time::{SessionDate, SessionRange};
use fund::ingest::alpaca::Alpaca;
use fund::ingest::{FetchError, VariableRefusal};
use fund::journal::Journal;
use fund::records::{RefusedToStart, resolved, ship_logged, start};
use fund::storage::reference::{SnapshotError, latest_conditions};
use fund::storage::{Bucket, BucketError, parquet, trade_bars};
use fund::trader::parameters::Parameters;
use fund::trader::{DecisionInterval, Session, SessionError, Standing, last_closes, resumed, warm};

const SERVICE: &str = "trader";
/// Calendar days read back for the previous session, past any run of holidays and a weekend.
const CALENDAR_DAYS_BACK: i64 = 14;
/// How long past the close a run waits for the last minutes to be built before it reads the book regardless.
const CLOSE_GRACE: TimeDelta = TimeDelta::minutes(5);
const TICK: Duration = Duration::from_secs(1);

#[tokio::main]
async fn main() -> ExitCode {
    let today = SessionDate::at(Utc::now());
    let service = Service::new(SERVICE).expect("the service name is one path segment");
    let parameters = Parameters::from_environment();
    let started = start(service.clone(), today);
    let span = started.span();
    async move {
        let (parameters, configuration) = match resolved(parameters) {
            Ok(resolved) => resolved,
            Err(refused) => return refused.into(),
        };
        // Held until the process exits, so two traders never send orders against one account.
        let (mut journal, _lock) = match started.open(parameters.journal_directory(), configuration)
        {
            Ok(opened) => opened,
            Err(refused) => return refused.into(),
        };
        // From here every exit runs through the closing step below, so the journal always holds the session's end.
        let sdk_configuration = aws_config::load_from_env().await;
        let profile = Bucket::profile(&sdk_configuration);
        let archive = Bucket::archive(&sdk_configuration);
        let traded = match (&profile, &archive) {
            (Ok(profile), Ok(archive)) => {
                trade(&parameters, profile, archive, &mut journal, today).await
            }
            (Err(refusal), _) | (_, Err(refusal)) => Err(Stopped::BeforeTheOpen(
                StartRefusal::Bucket(refusal.clone()),
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
                RefusedToStart.into()
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
        let profile = match profile {
            Ok(profile) => profile,
            // The refusal was logged as the reason the session did not start.
            Err(_) => return outcome,
        };
        let all_shipped = ship_logged(
            &profile,
            Host::Trader,
            &service,
            parameters.journal_directory(),
            parameters.log_directory(),
        )
        .await;
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
    /// Ran to the close but the account still held positions, as when unread minutes kept the session from going flat.
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
#[derive(Debug, thiserror::Error)]
enum StartRefusal {
    #[error("the archive client refused: {0}")]
    Bucket(VariableRefusal),
    #[error("the Alpaca client refused: {0}")]
    Alpaca(VariableRefusal),
    #[error("the playbook was not read: {0}")]
    PlaybookUnread(BucketError),
    #[error("no playbook is uploaded at {}", .key.path())]
    PlaybookAbsent { key: ConfigurationKey },
    #[error("the playbook is not UTF-8 text: {0}")]
    PlaybookNotText(std::string::FromUtf8Error),
    #[error("the playbook was refused: {0}")]
    Playbook(PlaybookRefusal),
    #[error("the paper account refused: {0}")]
    NotPaper(BrokerError),
    #[error("the calendar was not read: {0}")]
    Calendar(FetchError),
    #[error("the session closed at {close}")]
    AfterClose { close: DateTime<Utc> },
    #[error("the conditions were not read: {0}")]
    Conditions(SnapshotError),
    #[error("the book was not read: {0}")]
    Book(BrokerError),
    #[error("the session's fills were not read: {0}")]
    Fills(BrokerError),
    #[error("the session's fills moved from {before} to {after} while the book was read")]
    FillsMoved { before: usize, after: usize },
    #[error("{0}")]
    PreviousSession(PreviousRefusal),
    #[error("the opening was not valued: {0}")]
    Opening(ValuationRefusal),
    #[error("the journal refused a write: {0}")]
    Journal(io::Error),
}

/// Why the previous session's bars did not warm the state.
#[derive(Debug, thiserror::Error)]
enum PreviousRefusal {
    #[error("no trading day in the {CALENDAR_DAYS_BACK} days before {today}")]
    NoTradingDay { today: SessionDate },
    #[error("the previous session was not read: {0}")]
    Bucket(BucketError),
    #[error("{} is not archived", Key::from(*.key).path())]
    NotArchived { key: TradesKey },
    #[error("{} not decoded: {refusal}", Key::from(*.key).path())]
    Decode {
        key: TradesKey,
        refusal: parquet::DecodeRefusal,
    },
}

/// Why a session that had opened stopped trading.
#[derive(Debug, thiserror::Error)]
enum TradingStop {
    #[error("{0}")]
    Session(SessionError),
    #[error("the book was not read at the close: {0}")]
    Book(BrokerError),
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

/// Reads the profile's uploaded playbook and journals it, so the session's records name the playbook it traded under.
async fn read_playbook(
    parameters: &Parameters,
    profile: &Bucket,
    journal: &mut Journal,
) -> Result<Played, StartRefusal> {
    let key = ConfigurationKey::Playbook;
    let bytes = match profile.get(&key).await {
        Ok(Some(bytes)) => bytes,
        Ok(None) => return Err(StartRefusal::PlaybookAbsent { key }),
        Err(error) => return Err(StartRefusal::PlaybookUnread(error)),
    };
    let contents = String::from_utf8(bytes).map_err(StartRefusal::PlaybookNotText)?;
    let playbook = Playbook::parse(&contents).map_err(StartRefusal::Playbook)?;
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
    profile: &Bucket,
    archive: &Bucket,
    journal: &mut Journal,
    today: SessionDate,
) -> Result<Ran, Stopped> {
    let refused = Stopped::BeforeTheOpen;
    let strategy = read_playbook(parameters, profile, journal)
        .await
        .map_err(refused)?;
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
    // A run restarted within the session unwinds the fills an earlier run made, so its losses run from the open; read
    // on both sides of the book, so an order left open that fills between them refuses the start.
    let read_fills = || async {
        broker
            .fills(today.midnight())
            .await
            .map_err(|error| refused(StartRefusal::Fills(error)))
    };
    let fills = read_fills().await?;
    let book = broker
        .book()
        .await
        .map_err(|error| refused(StartRefusal::Book(error)))?;
    let after = read_fills().await?;
    if after != fills {
        return Err(refused(StartRefusal::FillsMoved {
            before: fills.len(),
            after: after.len(),
        }));
    }
    // Names held outside the universe, or traded earlier in the session, are watched too, since the book cannot be
    // valued without their prices.
    let symbols: BTreeSet<Symbol> = parameters
        .universe()
        .symbols()
        .iter()
        .chain(book.positions().keys())
        .chain(fills.iter().map(Fill::symbol))
        .cloned()
        .collect();
    let decision = parameters.settings().decision();
    let (previous, closes) = previous_bars(archive, &calendar, today, &symbols, decision)
        .await
        .map_err(|refusal| refused(StartRefusal::PreviousSession(refusal)))?;
    let (opened, opening, losses) = resumed(&book, &fills, |symbol| closes.get(symbol).copied())
        .map_err(|refusal| refused(StartRefusal::Opening(refusal)))?;
    journal
        .append(
            Utc::now(),
            Observation::SessionOpened(SessionOpened::new(today, &opened, opening)),
        )
        .map_err(|error| refused(StartRefusal::Journal(error)))?;
    tracing::info!(
        %open,
        %close,
        cash = book.cash().dollars(),
        positions = book.positions().len(),
        fills = fills.len(),
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
        symbols.into_iter().collect(),
        previous,
        book,
        opening,
        losses,
        open,
    );
    let mut ticks = tokio::time::interval(TICK);
    ticks.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let deadline = close + CLOSE_GRACE;
    loop {
        ticks.tick().await;
        let now = Utc::now();
        let remaining = (deadline - now)
            .to_std()
            .ok()
            .filter(|remaining| !remaining.is_zero());
        // Nothing is decided once the clock is past the close, so bounding a later advance cuts off only its reads.
        let advanced = match (remaining, now >= close) {
            (None, _) => None,
            (Some(_), false) => Some(session.advance(&Utc::now, &tape, &broker, journal).await),
            (Some(remaining), true) => tokio::time::timeout(
                remaining,
                session.advance(&Utc::now, &tape, &broker, journal),
            )
            .await
            .ok(),
        };
        let Some(advanced) = advanced else {
            tracing::warn!(%close, "Last minutes not built by the close grace");
            break;
        };
        if let Err(error) = advanced {
            return Err(Stopped::Trading(TradingStop::Session(error)));
        }
        match session.standing() {
            Standing::Trading => {}
            Standing::Halted(cause) => return Ok(Ran::Halted(cause.clone())),
        }
        if session.built_through_close() {
            break;
        }
    }
    match broker.book().await {
        Ok(held) if held.positions().is_empty() => Ok(Ran::ToTheClose),
        Ok(held) => Ok(Ran::HeldAtTheClose {
            positions: held.positions().len(),
        }),
        Err(error) => Err(Stopped::Trading(TradingStop::Book(error))),
    }
}

/// The previous session's one-minute trade bars of `symbols` as the archive derived them, folded into a state beside
/// their `decision` bars, with each symbol's last close among them.
async fn previous_bars(
    archive: &Bucket,
    calendar: &TradingCalendar,
    today: SessionDate,
    symbols: &BTreeSet<Symbol>,
    decision: DecisionInterval,
) -> Result<(MarketState, BTreeMap<Symbol, Price>), PreviousRefusal> {
    let previous = calendar
        .previous_trading_day(today)
        .ok_or(PreviousRefusal::NoTradingDay { today })?;
    let key = TradesKey::new(Provider::Alpaca, BarInterval::OneMinute, previous);
    let bytes = match archive
        .get(&Key::from(key))
        .await
        .map_err(PreviousRefusal::Bucket)?
    {
        Some(bytes) => bytes,
        None => return Err(PreviousRefusal::NotArchived { key }),
    };
    let (bars, _) = match trade_bars::decode(&key, bytes) {
        Ok(decoded) => decoded,
        Err(refusal) => return Err(PreviousRefusal::Decode { key, refusal }),
    };
    let bars: Vec<_> = bars
        .into_rows()
        .into_iter()
        .filter(|bar| symbols.contains(bar.symbol()))
        .collect();
    let closes = last_closes(&bars);
    Ok((warm(bars, symbols, decision), closes))
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
    use fund::execution::{JournalFailure, ReconcileFailure};

    fn disk_full() -> io::Error {
        io::Error::other("disk full")
    }

    fn previous_key() -> TradesKey {
        TradesKey::new(
            Provider::Alpaca,
            BarInterval::OneMinute,
            SessionDate::from_date(NaiveDate::from_ymd_opt(2026, 10, 7).unwrap()),
        )
    }

    const PREVIOUS_PATH: &str = "data/equity/stage=parsed/trades/provider=alpaca/interval=one_minute/year=2026/month=10/day=07/data.parquet";

    #[test]
    fn test_each_start_refusal_displays_its_cause() {
        let close: DateTime<Utc> = "2026-10-08T20:00:00Z".parse().unwrap();
        let displayed = [
            StartRefusal::Bucket(VariableRefusal::Missing { name: "BUCKET" }),
            StartRefusal::Alpaca(VariableRefusal::Missing { name: "KEY" }),
            StartRefusal::PlaybookUnread(BucketError::Get {
                path: "configuration/playbook.toml".to_string(),
                reason: "throttled".to_string(),
            }),
            StartRefusal::PlaybookAbsent {
                key: ConfigurationKey::Playbook,
            },
            StartRefusal::PlaybookNotText(String::from_utf8(vec![0xff]).unwrap_err()),
            StartRefusal::Playbook(PlaybookRefusal::Empty),
            StartRefusal::NotPaper(BrokerError::NotPaper),
            StartRefusal::Calendar(FetchError::Refused {
                status: 503,
                body: "busy".to_string(),
            }),
            StartRefusal::AfterClose { close },
            StartRefusal::Conditions(SnapshotError::Absent),
            StartRefusal::Book(BrokerError::NotPaper),
            StartRefusal::Fills(BrokerError::NotPaper),
            StartRefusal::FillsMoved {
                before: 3,
                after: 4,
            },
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
                "the playbook was not read: reading configuration/playbook.toml failed: throttled",
                "no playbook is uploaded at configuration/playbook.toml",
                "the playbook is not UTF-8 text: invalid utf-8 sequence of 1 bytes from index 0",
                "the playbook was refused: the playbook has no entries",
                "the paper account refused: the Alpaca keys trade live, not on paper",
                "the calendar was not read: refused with 503: busy",
                "the session closed at 2026-10-08 20:00:00 UTC",
                "the conditions were not read: no conditions table in the archive",
                "the book was not read: the Alpaca keys trade live, not on paper",
                "the session's fills were not read: the Alpaca keys trade live, not on paper",
                "the session's fills moved from 3 to 4 while the book was read",
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
            PreviousRefusal::Bucket(BucketError::Get {
                path: "a/b".to_string(),
                reason: "timed out".to_string(),
            }),
            PreviousRefusal::NotArchived {
                key: previous_key(),
            },
            PreviousRefusal::Decode {
                key: previous_key(),
                refusal: parquet::DecodeRefusal::File(parquet::ReadRefusal::Layout {
                    version: "9".to_string(),
                }),
            },
        ]
        .map(|refusal| refusal.to_string());
        assert_eq!(
            displayed,
            [
                "no trading day in the 14 days before 2026-10-08",
                "the previous session was not read: reading a/b failed: timed out",
                &format!("{PREVIOUS_PATH} is not archived"),
                &format!("{PREVIOUS_PATH} not decoded: the file is written under layout 9"),
            ]
        );
    }

    #[test]
    fn test_each_trading_stop_displays_its_cause() {
        let displayed = [
            TradingStop::Session(SessionError::Journal(JournalFailure::before_any_order(
                disk_full(),
            ))),
            TradingStop::Session(SessionError::Reconcile(ReconcileFailure::Journal(
                JournalFailure::before_any_order(disk_full()),
            ))),
            TradingStop::Session(SessionError::Reconcile(ReconcileFailure::Unread(
                BrokerError::NotPaper,
            ))),
            TradingStop::Book(BrokerError::NotPaper),
        ]
        .map(|stop| stop.to_string());
        assert_eq!(
            displayed,
            [
                "the journal refused a write after 0 orders: disk full",
                "the journal refused a write after 0 orders: disk full",
                "the broker's book was not read to reconcile: the Alpaca keys trade live, not on paper",
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
            Err(Stopped::Trading(TradingStop::Book(BrokerError::NotPaper))),
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
