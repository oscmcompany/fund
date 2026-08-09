//! Rebuilds missing `account_snapshots` rows from Alpaca's portfolio history.
//!
//! Recovery tooling. `account_snapshots` is written only by the post-close sync, so a session the
//! sync missed — a failed run, a stopped VM, a `database:reset` — leaves a hole that nothing else
//! can fill. Every other table has a path back: bars refetch from Massive, activities refetch from
//! Alpaca, `equity_pairs` restores from a dump. This table had none, which made every schema change
//! a permanent loss of equity history.
//!
//! Usage: `backfill_account_snapshots [--dry-run] <start YYYY-MM-DD> [end YYYY-MM-DD]`
//! The end date defaults to today (Eastern) when omitted.
//!
//! Reconstructed rows carry equity and no balances, because equity is all portfolio history
//! reports. That is enough for the two things the table exists for: the drawdown gate's reference
//! and the dashboard's return series.
//!
//! A dry run reports what it would write and writes nothing, which makes this tool its own gap
//! inventory — "how bad is it" and "fix it" are the same command with one flag between them.

use std::collections::BTreeMap;

use chrono::{NaiveDate, Utc};
use rust_decimal::Decimal;
use tracing::{error, info, warn};

use fund::common::alpaca::{AlpacaCredentials, TradingClient};
use fund::common::database::connect_pool;
use fund::common::observability::init_tracing;
use fund::data::calendar::{SessionDate, TradingCalendar};
use fund::portfolio::account;

const USAGE: &str =
    "Usage: backfill_account_snapshots [--dry-run] <start YYYY-MM-DD> [end YYYY-MM-DD]";

/// What a run should do, and over which sessions.
#[derive(Debug, PartialEq, Eq)]
struct BackfillRequest {
    start: SessionDate,
    end: SessionDate,
    dry_run: bool,
}

impl BackfillRequest {
    /// Rejects an inverted range, so a `BackfillRequest` in scope is proof the window is orderable.
    fn new(start: SessionDate, end: SessionDate, dry_run: bool) -> Result<Self, String> {
        if start > end {
            return Err(format!(
                "Invalid range: start date {start} must be on or before end date {end}"
            ));
        }
        Ok(Self {
            start,
            end,
            dry_run,
        })
    }
}

fn parse_date(value: &str) -> Result<SessionDate, String> {
    NaiveDate::parse_from_str(value, "%Y-%m-%d")
        .map(SessionDate::from_date)
        .map_err(|error| format!("Invalid date '{value}': expected YYYY-MM-DD ({error})"))
}

/// Parses the arguments, defaulting the end date to today in Eastern.
///
/// `today` is a parameter rather than read from the clock here, because a function that reads the
/// wall clock cannot be tested across the hours where the Eastern date and the UTC date disagree.
///
/// `--dry-run` is accepted in any position rather than only first. It is the flag that makes this
/// tool safe to point at production, and one that silently became a date argument because it
/// trailed the range would be a trap.
fn parse_arguments(arguments: &[String], today: SessionDate) -> Result<BackfillRequest, String> {
    let dry_run = arguments.iter().any(|argument| argument == "--dry-run");
    let dates: Vec<&String> = arguments
        .iter()
        .filter(|argument| *argument != "--dry-run")
        .collect();

    match dates.as_slice() {
        [] => Err(format!("Start date is required\n{USAGE}")),
        [start] => BackfillRequest::new(parse_date(start)?, today, dry_run),
        [start, end] => BackfillRequest::new(parse_date(start)?, parse_date(end)?, dry_run),
        _ => Err(format!("Too many arguments\n{USAGE}")),
    }
}

/// What a run found, and what it could not put right.
#[derive(Debug, Default, PartialEq, Eq)]
struct BackfillSummary {
    /// Sessions already present, left untouched.
    present: usize,
    /// Sessions written, or that a dry run would have written.
    filled: usize,
    /// Trading days absent from `account_snapshots` *and* from portfolio history. A hole this tool
    /// cannot close.
    unfillable: usize,
    /// Sessions where a stored equity and portfolio history disagree.
    disagreements: usize,
}

impl BackfillSummary {
    /// A run that left a gap open must not look successful.
    ///
    /// Disagreements gate the exit code too. The likeliest cause is a session-mapping error
    /// shifting the whole curve by a day, which would otherwise be reported as a clean fill of
    /// entirely wrong numbers.
    fn exit_code(&self) -> i32 {
        if self.unfillable > 0 || self.disagreements > 0 {
            1
        } else {
            0
        }
    }
}

/// Decides what to do with each trading day in the range.
///
/// Pure, and separated from both the fetch and the write so the decision can be tested without a
/// database or a network. `stored` and `reported` are what the two sources say; the calendar says
/// which days were sessions at all, so weekends and holidays are never counted as gaps.
fn plan(
    trading_days: &[SessionDate],
    stored: &BTreeMap<SessionDate, Decimal>,
    reported: &BTreeMap<SessionDate, Decimal>,
) -> (Vec<(SessionDate, Decimal)>, BackfillSummary) {
    let mut fills = Vec::new();
    let mut summary = BackfillSummary::default();

    for day in trading_days {
        match (stored.get(day), reported.get(day)) {
            (Some(held), Some(fetched)) => {
                summary.present += 1;
                // Free cross-check: the two sources should agree on a session both know about.
                // Systematic disagreement is what a one-day session-mapping skew looks like.
                if held != fetched {
                    summary.disagreements += 1;
                    warn!(
                        session = %day,
                        stored = %held,
                        reported = %fetched,
                        "Stored equity disagrees with portfolio history"
                    );
                }
            }
            (Some(_), None) => summary.present += 1,
            (None, Some(fetched)) => fills.push((*day, *fetched)),
            (None, None) => {
                summary.unfillable += 1;
                warn!(session = %day, "Trading day is missing and portfolio history has no equity for it");
            }
        }
    }

    summary.filled = fills.len();
    (fills, summary)
}

#[tokio::main]
async fn main() {
    fund::common::crypto::install_default_crypto_provider();
    let tracing_guard = init_tracing(
        "backfill-account-snapshots.log",
        Some("info"),
        "backfill-account-snapshots",
    );

    let arguments: Vec<String> = std::env::args().skip(1).collect();
    let request = match parse_arguments(&arguments, SessionDate::at(Utc::now())) {
        Ok(request) => request,
        Err(message) => {
            eprintln!("{message}");
            drop(tracing_guard);
            std::process::exit(2);
        }
    };

    let code = match run(&request).await {
        Ok(summary) => {
            info!(
                present = summary.present,
                filled = summary.filled,
                unfillable = summary.unfillable,
                disagreements = summary.disagreements,
                dry_run = request.dry_run,
                "Account snapshot backfill complete"
            );
            summary.exit_code()
        }
        Err(error) => {
            error!(%error, "Backfilling account snapshots failed");
            eprintln!("Backfilling account snapshots failed: {error}");
            1
        }
    };

    // `std::process::exit` runs no destructors, so the non-blocking appender's guard would never
    // drop and its buffered lines would be lost — exactly when the failure log matters.
    drop(tracing_guard);
    std::process::exit(code);
}

async fn run(request: &BackfillRequest) -> Result<BackfillSummary, Box<dyn std::error::Error>> {
    let client = TradingClient::from_env(AlpacaCredentials::from_env()?);
    let pool = connect_pool().await?;

    // The exchange decides which days were sessions. Deriving them from weekends alone would count
    // every holiday as a gap this tool cannot close, and report a failure on a correct database.
    let calendar = TradingCalendar::from_days(
        client
            .fetch_calendar(request.start.date(), request.end.date())
            .await?,
    );
    let trading_days = calendar.trading_days_in_range(request.start, request.end);
    if trading_days.is_empty() {
        info!(start = %request.start, end = %request.end, "No trading days in range, nothing to do");
        return Ok(BackfillSummary::default());
    }

    let stored = load_stored_equity(&pool, request.start, request.end).await?;
    let reported: BTreeMap<SessionDate, Decimal> = client
        .fetch_portfolio_history(request.start.date(), request.end.date())
        .await?
        .into_iter()
        .map(|point| (SessionDate::at(point.measured_at()), point.equity()))
        .collect();

    let (fills, mut summary) = plan(&trading_days, &stored, &reported);
    info!(
        sessions = trading_days.len(),
        present = summary.present,
        missing = fills.len(),
        unfillable = summary.unfillable,
        "Planned the backfill"
    );

    if request.dry_run {
        for (session, equity) in &fills {
            info!(session = %session, equity = %equity, "Would fill this session");
        }
        return Ok(summary);
    }

    let mut written = 0;
    for (session, equity) in &fills {
        // `DO NOTHING` means a row that appeared since the plan was made is left alone rather than
        // downgraded, so a zero here is a race that resolved correctly, not a failure.
        written += account::store_equity_snapshot(&pool, *session, *equity).await?;
    }
    summary.filled = written as usize;
    info!(written, "Filled missing account snapshots");
    Ok(summary)
}

/// Reads the equity already recorded across the range, keyed by session.
async fn load_stored_equity(
    pool: &sqlx::PgPool,
    start: SessionDate,
    end: SessionDate,
) -> Result<BTreeMap<SessionDate, Decimal>, sqlx::Error> {
    let rows = sqlx::query!(
        r#"SELECT session_date AS "session_date!", equity AS "equity!"
           FROM account_snapshots
           WHERE session_date BETWEEN $1 AND $2"#,
        start.date(),
        end.date(),
    )
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|row| (SessionDate::from_date(row.session_date), row.equity))
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn date(value: &str) -> SessionDate {
        SessionDate::from_date(
            NaiveDate::parse_from_str(value, "%Y-%m-%d").expect("a valid test date"),
        )
    }

    fn decimal(value: &str) -> Decimal {
        value.parse().expect("a valid test decimal")
    }

    fn equities(entries: &[(&str, &str)]) -> BTreeMap<SessionDate, Decimal> {
        entries
            .iter()
            .map(|(session, equity)| (date(session), decimal(equity)))
            .collect()
    }

    #[test]
    fn test_a_missing_start_date_is_rejected() {
        let error = parse_arguments(&[], date("2026-07-31")).expect_err("no start date");
        assert!(error.contains("Start date is required"), "{error}");
    }

    #[test]
    fn test_an_omitted_end_date_defaults_to_today() {
        let request = parse_arguments(&["2026-01-05".to_string()], date("2026-07-31"))
            .expect("a valid single-argument range");
        assert_eq!(request.start, date("2026-01-05"));
        assert_eq!(request.end, date("2026-07-31"));
        assert!(!request.dry_run);
    }

    #[test]
    fn test_an_inverted_range_is_rejected() {
        let error = parse_arguments(
            &["2026-07-31".to_string(), "2026-01-05".to_string()],
            date("2026-07-31"),
        )
        .expect_err("an inverted range");
        assert!(error.contains("must be on or before"), "{error}");
    }

    #[test]
    fn test_an_unparseable_date_is_rejected() {
        let error = parse_arguments(&["not-a-date".to_string()], date("2026-07-31"))
            .expect_err("an unparseable date");
        assert!(error.contains("expected YYYY-MM-DD"), "{error}");
    }

    /// The flag must not be mistaken for a date wherever it lands, or a trailing `--dry-run` would
    /// turn a dry run into a live write against the range the operator was only inspecting.
    #[test]
    fn test_the_dry_run_flag_is_recognized_in_any_position() {
        for arguments in [
            vec!["--dry-run".to_string(), "2026-01-05".to_string()],
            vec!["2026-01-05".to_string(), "--dry-run".to_string()],
        ] {
            let request =
                parse_arguments(&arguments, date("2026-07-31")).expect("a valid dry-run request");
            assert!(request.dry_run, "{arguments:?}");
            assert_eq!(request.start, date("2026-01-05"));
            assert_eq!(request.end, date("2026-07-31"));
        }
    }

    #[test]
    fn test_extra_arguments_are_rejected() {
        let arguments = vec![
            "2026-01-05".to_string(),
            "2026-01-06".to_string(),
            "2026-01-07".to_string(),
        ];
        let error =
            parse_arguments(&arguments, date("2026-07-31")).expect_err("too many arguments");
        assert!(error.contains("Too many arguments"), "{error}");
    }

    #[test]
    fn test_only_the_missing_sessions_are_filled() {
        let trading_days = vec![date("2026-05-14"), date("2026-05-15")];
        let stored = equities(&[("2026-05-14", "20100.50")]);
        let reported = equities(&[("2026-05-14", "20100.50"), ("2026-05-15", "20559.16")]);

        let (fills, summary) = plan(&trading_days, &stored, &reported);

        assert_eq!(fills, vec![(date("2026-05-15"), decimal("20559.16"))]);
        assert_eq!(summary.present, 1);
        assert_eq!(summary.filled, 1);
        assert_eq!(summary.exit_code(), 0);
    }

    /// A session already recorded is never rewritten, even though portfolio history also has it.
    /// The stored row may carry balances that portfolio history cannot supply.
    #[test]
    fn test_a_session_already_present_is_left_alone() {
        let trading_days = vec![date("2026-05-14")];
        let held = equities(&[("2026-05-14", "20100.50")]);

        let (fills, summary) = plan(&trading_days, &held, &held);

        assert!(fills.is_empty());
        assert_eq!(summary.present, 1);
        assert_eq!(summary.exit_code(), 0);
    }

    /// The cross-check that would catch a session-mapping skew. A shifted curve makes every
    /// overlapping session disagree at once, which is why disagreement fails the run.
    #[test]
    fn test_disagreeing_equity_is_reported_and_fails_the_run() {
        let trading_days = vec![date("2026-05-14")];
        let stored = equities(&[("2026-05-14", "20100.50")]);
        let reported = equities(&[("2026-05-14", "19000.00")]);

        let (fills, summary) = plan(&trading_days, &stored, &reported);

        assert!(fills.is_empty());
        assert_eq!(summary.disagreements, 1);
        assert_eq!(summary.exit_code(), 1);
    }

    /// A gap neither source can close is the failure this tool exists to surface. Counting it as
    /// "nothing to do" would report success over a hole that is still there.
    #[test]
    fn test_a_session_neither_source_has_fails_the_run() {
        let trading_days = vec![date("2026-05-14")];

        let (fills, summary) = plan(&trading_days, &BTreeMap::new(), &BTreeMap::new());

        assert!(fills.is_empty());
        assert_eq!(summary.unfillable, 1);
        assert_eq!(summary.exit_code(), 1);
    }

    /// Portfolio history returns points for non-trading days too. The calendar decides what counts,
    /// so a weekend inside the range is neither filled nor reported as a gap.
    #[test]
    fn test_days_outside_the_calendar_are_not_filled() {
        let trading_days = vec![date("2026-05-15")];
        let reported = equities(&[
            ("2026-05-15", "20559.16"),
            // 2026-05-16 is a Saturday.
            ("2026-05-16", "20559.16"),
        ]);

        let (fills, summary) = plan(&trading_days, &BTreeMap::new(), &reported);

        assert_eq!(fills, vec![(date("2026-05-15"), decimal("20559.16"))]);
        assert_eq!(summary.unfillable, 0);
        assert_eq!(summary.exit_code(), 0);
    }
}
