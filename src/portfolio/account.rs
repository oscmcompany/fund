//! The post-close account sync: what Alpaca says actually happened.
//!
//! Three things, in order. Balances land in `account_snapshots`, whose `equity` is what the *next*
//! session's drawdown gate measures against. Fills and transfers land in `account_activities`,
//! keyed by Alpaca's activity identifier so a re-run conflicts on every row and changes nothing.
//! Then the fills — and only the fills — are attributed back to their pairs.
//!
//! Attribution is a materialization, not a source: `realized_profit_and_loss` exists so the
//! dashboard need not re-join activities to pairs on every page load.
//!
//! Transfers are stored because they are read, not because they are attributed: a capital flow
//! makes every equity-derived return wrong, and the dashboard withholds those figures rather than
//! publishing them. Nothing here decides *whose* capital moved — Alpaca does not know.

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use sqlx::PgPool;
use tracing::{info, warn};
use uuid::Uuid;

use crate::common::alpaca::{
    AccountActivity, AccountSnapshot, ClientError, TradingClient, FILL_ACTIVITY_TYPE,
    TRANSFER_ACTIVITY_TYPES,
};
use crate::data::calendar::{SessionDate, TradingCalendar};
use crate::data::session_log::{AccountObserved, Observation, SessionLog};
use crate::portfolio::pairs::{self, ClosedPair, PairsError};

/// The activity types this sync fetches and stores.
///
/// Fills, because they are attributed to pairs, plus transfers, because a capital flow invalidates
/// every equity-derived return the dashboard publishes and something has to see it arrive.
///
/// `INT`, `DIV`, and `FEE` are deliberately absent. They are return rather than external flow, so
/// nothing consumes them yet, and syncing a type nothing reads is how a column full of untested
/// parsing gets discovered in production. They join when the unit ledger needs them.
pub fn synced_activity_types() -> Vec<&'static str> {
    std::iter::once(FILL_ACTIVITY_TYPE)
        .chain(TRANSFER_ACTIVITY_TYPES)
        .collect()
}

/// Errors syncing the account.
#[derive(Debug, thiserror::Error)]
pub enum AccountError {
    #[error("failed to reach Alpaca: {0}")]
    Alpaca(#[from] ClientError),
    #[error("account persistence failed: {0}")]
    Database(#[from] sqlx::Error),
    #[error("pair record access failed: {0}")]
    Pairs(#[from] PairsError),
}

/// What one post-close sync did.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct AccountSyncSummary {
    pub session_date: SessionDate,
    pub equity: Decimal,
    pub activities_stored: u64,
    pub pairs_attributed: usize,
    pub activities_unattributed: usize,
    /// The previous trading session, when it has no snapshot — a hole in the equity series that a
    /// future time-weighted return cannot chain across.
    pub previous_session_gap: Option<SessionDate>,
}

/// Runs the whole post-close sync for one session date.
pub async fn sync_account(
    pool: &PgPool,
    client: &TradingClient,
    session_log: &SessionLog,
    calendar: &TradingCalendar,
    session_date: SessionDate,
) -> Result<AccountSyncSummary, AccountError> {
    let account = client.fetch_account().await?;
    store_snapshot(pool, session_date, &account).await?;

    // Recorded in full, not just the equity. Alpaca's portfolio history can report equity for a
    // past date and nothing can report cash, buying power, or the market values, so this is the
    // only moment they are observable at all.
    session_log
        .record(
            uuid::Uuid::new_v4(),
            Utc::now(),
            Observation::AccountObserved(AccountObserved {
                // The session this describes, which a sync re-run after Eastern midnight would
                // otherwise lose: the envelope files a record under the session it *happened* in.
                session_date,
                equity: account.equity().to_f64(),
                cash: account.cash().to_f64(),
                buying_power: account.buying_power().to_f64(),
                long_market_value: account.long_market_value().to_f64(),
                short_market_value: account.short_market_value().to_f64(),
            }),
        )
        .await;
    let previous_session_gap = missing_previous_snapshot(pool, calendar, session_date).await;

    // One request per type: Alpaca puts the activity type in the URL path, and the sync already
    // tolerates several round trips.
    let mut activities = Vec::new();
    for activity_type in synced_activity_types() {
        activities.extend(
            client
                .fetch_activities(activity_type, session_date.date())
                .await?,
        );
    }
    let activities_stored = store_activities(pool, &activities).await?;

    let (start, end) = session_date.bounds();
    let closed = pairs::load_closed_between(pool, start, end).await?;
    // Only fills reach `attribute`. A transfer has no ticker and no signed cash flow, so passing
    // one in would count it as unattributed and warn on every session that received capital.
    let fills: Vec<AccountActivity> = activities
        .iter()
        .filter(|activity| activity.activity_type() == FILL_ACTIVITY_TYPE)
        .cloned()
        .collect();
    let attribution = attribute(&closed, &fills);

    let mut pairs_attributed = 0;
    for (id, amount) in &attribution.realized {
        if pairs::record_realized_profit_and_loss(pool, *id, *amount).await? {
            pairs_attributed += 1;
        }
    }

    if let Some(previous) = previous_session_gap {
        warn!(
            %previous,
            %session_date,
            "Previous trading session has no account snapshot; the equity series has a gap"
        );
    }

    info!(
        %session_date,
        equity = %account.equity(),
        activities_stored,
        pairs_attributed,
        unattributed = attribution.unattributed,
        "Account sync complete"
    );

    Ok(AccountSyncSummary {
        session_date,
        equity: account.equity(),
        activities_stored,
        pairs_attributed,
        previous_session_gap,
        activities_unattributed: attribution.unattributed,
    })
}

/// Writes one session's balances, overwriting any existing row for the date.
pub async fn store_snapshot(
    pool: &PgPool,
    session_date: SessionDate,
    account: &AccountSnapshot,
) -> Result<(), AccountError> {
    sqlx::query!(
        r#"INSERT INTO account_snapshots (
               session_date, equity, cash, buying_power,
               long_market_value, short_market_value
           )
           VALUES ($1, $2, $3, $4, $5, $6)
           ON CONFLICT (session_date) DO UPDATE SET
               equity             = EXCLUDED.equity,
               cash               = EXCLUDED.cash,
               buying_power       = EXCLUDED.buying_power,
               long_market_value  = EXCLUDED.long_market_value,
               short_market_value = EXCLUDED.short_market_value,
               fetched_at         = now()"#,
        session_date.date(),
        account.equity(),
        account.cash(),
        account.buying_power(),
        account.long_market_value(),
        account.short_market_value(),
    )
    .execute(pool)
    .await?;
    Ok(())
}

/// Writes a session's equity alone, leaving the balances NULL because portfolio history reports
/// none and zero would be a claim.
///
/// `DO NOTHING`, unlike the `DO UPDATE` in [`store_snapshot`]: this row is strictly less
/// informative, so a re-run must never downgrade a complete snapshot to an equity-only one.
pub async fn store_equity_snapshot(
    pool: &PgPool,
    session_date: SessionDate,
    equity: Decimal,
) -> Result<u64, AccountError> {
    let result = sqlx::query!(
        r#"INSERT INTO account_snapshots (session_date, equity)
           VALUES ($1, $2)
           ON CONFLICT (session_date) DO NOTHING"#,
        session_date.date(),
        equity,
    )
    .execute(pool)
    .await?;
    Ok(result.rows_affected())
}

/// Stores activities, keyed by Alpaca's identifier.
///
/// `DO NOTHING` rather than `DO UPDATE`: an activity is an immutable record of something that
/// already happened, so a conflict means the row is already correct.
pub async fn store_activities(
    pool: &PgPool,
    activities: &[AccountActivity],
) -> Result<u64, AccountError> {
    if activities.is_empty() {
        return Ok(0);
    }

    let mut rows_affected: u64 = 0;
    let mut transaction = pool.begin().await?;

    for chunk in activities.chunks(1_000) {
        let mut query_builder = sqlx::QueryBuilder::new(
            "INSERT INTO account_activities \
             (id, activity_type, transaction_time, ticker, side, quantity, price, net_amount, \
              order_id) ",
        );
        query_builder.push_values(chunk, |mut builder, activity| {
            builder
                .push_bind(activity.id().to_string())
                .push_bind(activity.activity_type().to_string())
                .push_bind(activity.transaction_time())
                .push_bind(activity.ticker().map(|ticker| ticker.as_str().to_string()))
                .push_bind(activity.side().map(str::to_string))
                .push_bind(activity.shares())
                .push_bind(activity.price())
                .push_bind(activity.net_amount())
                .push_bind(activity.order_id().map(str::to_string));
        });
        query_builder.push(" ON CONFLICT (id) DO NOTHING");

        rows_affected += query_builder
            .build()
            .execute(&mut *transaction)
            .await?
            .rows_affected();
    }

    transaction.commit().await?;
    info!(
        rows = rows_affected,
        supplied = activities.len(),
        "Account activities stored"
    );
    Ok(rows_affected)
}

/// The previous trading session, when it has no snapshot of its own.
///
/// A sync that failed at 16:15 leaves a hole nothing else notices, and the equity series is what a
/// time-weighted return will eventually be folded from — a fold cannot chain across a missing
/// session, and by the time one is discovered it may be months old.
/// `/v2/account/portfolio/history` can refill it.
///
/// Asking the calendar for the previous session, rather than testing a date against
/// [`TradingCalendar::is_trading_day`], is deliberate: it only ever returns days it actually holds,
/// so a date beyond the fetched horizon cannot pass for a holiday. `None` means either no gap or no
/// calendar reaching back that far, and neither is worth failing the sync over.
async fn missing_previous_snapshot(
    pool: &PgPool,
    calendar: &TradingCalendar,
    session_date: SessionDate,
) -> Option<SessionDate> {
    let previous = calendar.previous_trading_day(session_date)?;
    match load_equity_for(pool, previous).await {
        Ok(None) => Some(previous),
        Ok(Some(_)) => None,
        // A read failure says nothing about whether a gap exists, and the sync's real work is
        // already done. Reporting it and continuing beats failing the session over a diagnostic.
        Err(error) => {
            warn!(%error, %previous, "Could not check the previous session for a gap");
            None
        }
    }
}

/// Reads the equity recorded for a session, if one was.
///
/// `None` means no snapshot exists for that date — a first run, or a post-close sync that did not
/// complete. The drawdown gate treats that as "no reference" rather than as a breach; see
/// [`crate::portfolio::risk::RiskGate::new`].
pub async fn load_equity_for(
    pool: &PgPool,
    session_date: SessionDate,
) -> Result<Option<Decimal>, AccountError> {
    let row = sqlx::query!(
        r#"SELECT equity AS "equity!" FROM account_snapshots WHERE session_date = $1"#,
        session_date.date(),
    )
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|row| row.equity))
}

/// Realized profit and loss per pair, plus the fills that belonged to no pair.
#[derive(Debug, Clone, PartialEq)]
pub struct Attribution {
    pub realized: HashMap<Uuid, Decimal>,
    pub unattributed: usize,
}

/// Attributes fills to the pairs whose legs and window they fall inside.
///
/// A fill belongs to a pair when its symbol is one of the pair's legs and its timestamp is within
/// the pair's open window. Profit and loss is the sum of the signed cash flows: a buy is negative,
/// a sell positive, so a pair that opened and closed cleanly sums to what it made.
///
/// A fill matching more than one pair is counted against **none** of them and reported as
/// unattributed. That means the same symbol traded in two overlapping pairs, which
/// [`crate::portfolio::screen::select_disjoint`] is supposed to prevent — so splitting the fill
/// would produce two plausible numbers and hide an upstream bug.
pub fn attribute(closed: &[ClosedPair], activities: &[AccountActivity]) -> Attribution {
    let mut realized: HashMap<Uuid, Decimal> = closed
        .iter()
        .map(|pair| (pair.id(), Decimal::ZERO))
        .collect();
    let mut unattributed = 0;

    for activity in activities {
        let Some(ticker) = activity.ticker() else {
            unattributed += 1;
            continue;
        };
        let Some(cash_flow) = activity.signed_cash_flow() else {
            unattributed += 1;
            continue;
        };

        let mut matches = closed
            .iter()
            .filter(|pair| pair.covers(ticker, activity.transaction_time()));
        let (Some(pair), None) = (matches.next(), matches.next()) else {
            unattributed += 1;
            continue;
        };

        *realized.entry(pair.id()).or_insert(Decimal::ZERO) += cash_flow;
    }

    if unattributed > 0 {
        warn!(
            unattributed,
            activities = activities.len(),
            "Fills could not be attributed to exactly one closed pair"
        );
    }
    Attribution {
        realized,
        unattributed,
    }
}

/// The instant bounds of a session date, for callers that need them alongside a sync.
pub fn session_bounds(session_date: SessionDate) -> (DateTime<Utc>, DateTime<Utc>) {
    session_date.bounds()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::types::{PairID, Ticker};
    use crate::portfolio::pairs::CloseReason;
    use chrono::NaiveDate;

    fn ticker(raw: &str) -> Ticker {
        Ticker::new(raw).unwrap()
    }

    fn instant(hour: u32, minute: u32) -> DateTime<Utc> {
        NaiveDate::from_ymd_opt(2026, 7, 30)
            .unwrap()
            .and_hms_opt(hour, minute, 0)
            .unwrap()
            .and_utc()
    }

    fn closed_pair(long: &str, short: &str, open_hour: u32, close_hour: u32) -> ClosedPair {
        ClosedPair::new(
            Uuid::new_v4(),
            PairID::new(ticker(long), ticker(short)),
            instant(open_hour, 0),
            instant(close_hour, 0),
            CloseReason::Convergence,
        )
    }

    fn fill(
        id: &str,
        symbol: &str,
        side: &str,
        shares: i64,
        price: i64,
        at: DateTime<Utc>,
    ) -> AccountActivity {
        AccountActivity::new(
            id.to_string(),
            FILL_ACTIVITY_TYPE.to_string(),
            at,
            Some(ticker(symbol)),
            Some(side.to_string()),
            Some(Decimal::from(shares)),
            Some(Decimal::from(price)),
            None,
            None,
        )
    }

    /// A pair bought low and sold high nets a gain; the sign convention has to carry that through
    /// four fills across two legs without cancelling itself out.
    #[test]
    fn test_a_profitable_round_trip_sums_to_its_gain() {
        let pair = closed_pair("AAAA", "BBBB", 14, 18);
        let activities = vec![
            // Long leg: bought 10 at 100, sold 10 at 110. +100.
            fill("1", "AAAA", "buy", 10, 100, instant(14, 5)),
            fill("2", "AAAA", "sell", 10, 110, instant(17, 55)),
            // Short leg: sold 10 at 200, bought back 10 at 195. +50.
            fill("3", "BBBB", "sell_short", 10, 200, instant(14, 5)),
            fill("4", "BBBB", "buy", 10, 195, instant(17, 55)),
        ];

        let attribution = attribute(std::slice::from_ref(&pair), &activities);
        assert_eq!(attribution.unattributed, 0);
        assert_eq!(attribution.realized[&pair.id()], Decimal::from(150));
    }

    /// A pair with no fills in its window is attributed zero rather than omitted. Omitting it would
    /// leave `realized_profit_and_loss` null, which the dashboard cannot tell apart from a pair
    /// whose sync has not run yet.
    #[test]
    fn test_a_pair_with_no_fills_is_attributed_zero() {
        let pair = closed_pair("AAAA", "BBBB", 14, 18);
        let attribution = attribute(std::slice::from_ref(&pair), &[]);
        assert_eq!(attribution.realized[&pair.id()], Decimal::ZERO);
    }

    #[test]
    fn test_a_fill_outside_the_window_is_not_attributed() {
        let pair = closed_pair("AAAA", "BBBB", 14, 18);
        let activities = vec![fill("1", "AAAA", "buy", 10, 100, instant(19, 0))];
        let attribution = attribute(std::slice::from_ref(&pair), &activities);

        assert_eq!(attribution.realized[&pair.id()], Decimal::ZERO);
        assert_eq!(attribution.unattributed, 1);
    }

    #[test]
    fn test_a_fill_in_a_symbol_the_pair_does_not_hold_is_not_attributed() {
        let pair = closed_pair("AAAA", "BBBB", 14, 18);
        let activities = vec![fill("1", "CCCC", "buy", 10, 100, instant(15, 0))];
        assert_eq!(attribute(&[pair], &activities).unattributed, 1);
    }

    /// Two overlapping pairs holding the same symbol make a fill ambiguous. Splitting it would
    /// produce two plausible figures and hide the fact that the disjointness rule was violated.
    #[test]
    fn test_an_ambiguous_fill_is_attributed_to_neither_pair() {
        let first = closed_pair("AAAA", "BBBB", 14, 18);
        let second = closed_pair("AAAA", "CCCC", 15, 17);
        let activities = vec![fill("1", "AAAA", "buy", 10, 100, instant(16, 0))];

        let attribution = attribute(&[first.clone(), second.clone()], &activities);
        assert_eq!(attribution.unattributed, 1);
        assert_eq!(attribution.realized[&first.id()], Decimal::ZERO);
        assert_eq!(attribution.realized[&second.id()], Decimal::ZERO);
    }

    /// A fee has no side or price, so it has no cash flow to attribute. Folding it into a pair
    /// would move that pair's realized profit and loss by the fee amount.
    #[test]
    fn test_a_non_trade_activity_is_not_attributed() {
        let pair = closed_pair("AAAA", "BBBB", 14, 18);
        let fee = AccountActivity::new(
            "f".to_string(),
            "FEE".to_string(),
            instant(16, 0),
            Some(ticker("AAAA")),
            None,
            None,
            None,
            None,
            None,
        );
        let attribution = attribute(std::slice::from_ref(&pair), &[fee]);
        assert_eq!(attribution.unattributed, 1);
        assert_eq!(attribution.realized[&pair.id()], Decimal::ZERO);
    }

    #[test]
    fn test_session_bounds_are_half_open_across_the_eastern_day() {
        let (start, end) = session_bounds(SessionDate::from_date(
            NaiveDate::from_ymd_opt(2026, 7, 30).unwrap(),
        ));
        assert!(start < end);
        assert_eq!((end - start).num_hours(), 24);
    }
}
