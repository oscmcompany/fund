//! `equity_pairs` persistence: which long belongs with which short, and why.
//!
//! Alpaca holds the authoritative position, quantity, and fill. What it cannot answer is which
//! long belongs with which short and what signal put them there, so that — and only that — is what
//! this table stores. Nothing here is a position ledger, and nothing here should be trusted over
//! Alpaca about what is actually held.
//!
//! `realized_profit_and_loss` is written by the post-close account sync in
//! [`crate::portfolio::account`], not by the evaluation pass. The pass knows a pair closed; it does
//! not know what the fills came back at.

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use sqlx::PgPool;
use tracing::{info, warn};
use uuid::Uuid;

use crate::common::types::{PairID, Ticker};

/// Errors reading or writing the pair record.
#[derive(Debug, thiserror::Error)]
pub enum PairsError {
    #[error("equity_pairs access failed: {0}")]
    Database(#[from] sqlx::Error),
}

/// Why an open pair was closed.
///
/// These four are the `close_reason` CHECK constraint in `schema.sql`, spelled as an enum so a
/// reason the database would reject cannot be constructed in the first place.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CloseReason {
    /// The spread returned through its mean. The trade worked.
    Convergence,
    /// The spread widened past the stop, against the position. The trade did not.
    StopLoss,
    /// The pre-close fail-safe flattened the book. No opinion either way.
    EndOfDay,
    /// Alpaca no longer reports a position for one or both legs, so there is nothing left to hold.
    PositionMissing,
}

impl CloseReason {
    /// Every variant, for exhaustive iteration in tests.
    pub const ALL: [CloseReason; 4] = [
        CloseReason::Convergence,
        CloseReason::StopLoss,
        CloseReason::EndOfDay,
        CloseReason::PositionMissing,
    ];

    /// The stored form, which must match the CHECK constraint exactly.
    pub fn as_str(self) -> &'static str {
        match self {
            CloseReason::Convergence => "convergence",
            CloseReason::StopLoss => "stop_loss",
            CloseReason::EndOfDay => "end_of_day",
            CloseReason::PositionMissing => "position_missing",
        }
    }

    /// Parses the stored form. Returns `None` for anything outside the vocabulary.
    pub fn parse(raw: &str) -> Option<Self> {
        CloseReason::ALL
            .into_iter()
            .find(|reason| reason.as_str() == raw)
    }

    /// Whether this exit reflects the strategy's own opinion about the spread.
    ///
    /// A book that only ever exits at [`CloseReason::EndOfDay`] is one whose holding period is
    /// shorter than the horizon it forecasts, which is the mismatch recorded in
    /// `improvements_intraday_horizon.md`. The ratio of these two answers is the interim measure of
    /// whether the strategy is doing anything, so the distinction is worth naming rather than
    /// recomputing from a string at each call site.
    pub fn is_signal(self) -> bool {
        match self {
            CloseReason::Convergence | CloseReason::StopLoss => true,
            CloseReason::EndOfDay | CloseReason::PositionMissing => false,
        }
    }
}

impl std::fmt::Display for CloseReason {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Why a pair entry was rejected before it could be recorded.
#[derive(Debug, Clone, PartialEq)]
pub struct InvalidEntryError {
    pub field: &'static str,
    pub value: f64,
}

impl std::fmt::Display for InvalidEntryError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "pair entry field {} is not usable: {}",
            self.field, self.value
        )
    }
}

impl std::error::Error for InvalidEntryError {}

/// The signal that justified opening a pair, as it is recorded.
///
/// Validated on construction rather than on write. A non-finite hedge ratio reaches PostgreSQL as
/// a `NaN` that every later comparison against it answers `false` to, which presents as a pair that
/// simply never closes.
#[derive(Debug, Clone, PartialEq)]
pub struct PairEntry {
    pair_id: PairID,
    hedge_ratio: f64,
    entry_z_score: f64,
    signal_strength: f64,
    model_run_id: Option<String>,
}

impl PairEntry {
    /// Constructs an entry, rejecting values that cannot describe a real position.
    ///
    /// `entry_z_score` must be positive: the spread is defined as
    /// `ln(short) - hedge_ratio * ln(long)`, so a pair is entered precisely when the short leg is
    /// the expensive one and the score is above the entry threshold. A negative entry score would
    /// mean the legs were assigned the wrong way round.
    pub fn new(
        pair_id: PairID,
        hedge_ratio: f64,
        entry_z_score: f64,
        signal_strength: f64,
        model_run_id: Option<String>,
    ) -> Result<Self, InvalidEntryError> {
        if !hedge_ratio.is_finite() {
            return Err(InvalidEntryError {
                field: "hedge_ratio",
                value: hedge_ratio,
            });
        }
        if !entry_z_score.is_finite() || entry_z_score <= 0.0 {
            return Err(InvalidEntryError {
                field: "entry_z_score",
                value: entry_z_score,
            });
        }
        // Strictly positive, matching `PairCandidate::new`. Zero means the forecast is neutral
        // between the legs, which `screen::orient` already refuses — so accepting it here would let
        // a future call site build a `PairEntry` the screen would never have produced.
        if !signal_strength.is_finite() || signal_strength <= 0.0 {
            return Err(InvalidEntryError {
                field: "signal_strength",
                value: signal_strength,
            });
        }
        Ok(Self {
            pair_id,
            hedge_ratio,
            entry_z_score,
            signal_strength,
            model_run_id,
        })
    }

    pub fn pair_id(&self) -> &PairID {
        &self.pair_id
    }

    pub fn long_ticker(&self) -> &Ticker {
        self.pair_id.long()
    }

    pub fn short_ticker(&self) -> &Ticker {
        self.pair_id.short()
    }

    pub fn hedge_ratio(&self) -> f64 {
        self.hedge_ratio
    }

    pub fn entry_z_score(&self) -> f64 {
        self.entry_z_score
    }

    pub fn signal_strength(&self) -> f64 {
        self.signal_strength
    }

    pub fn model_run_id(&self) -> Option<&str> {
        self.model_run_id.as_deref()
    }
}

/// A pair currently on the book.
///
/// Carries the hedge ratio because the exit test needs it: the spread the pass measures now has to
/// be the same spread the entry was decided on, and the hedge ratio is what makes it so.
#[derive(Debug, Clone, PartialEq)]
pub struct OpenPair {
    id: Uuid,
    pair_id: PairID,
    hedge_ratio: f64,
    entry_z_score: f64,
    opened_at: DateTime<Utc>,
}

impl OpenPair {
    pub fn new(
        id: Uuid,
        pair_id: PairID,
        hedge_ratio: f64,
        entry_z_score: f64,
        opened_at: DateTime<Utc>,
    ) -> Self {
        Self {
            id,
            pair_id,
            hedge_ratio,
            entry_z_score,
            opened_at,
        }
    }

    pub fn id(&self) -> Uuid {
        self.id
    }

    pub fn pair_id(&self) -> &PairID {
        &self.pair_id
    }

    pub fn long_ticker(&self) -> &Ticker {
        self.pair_id.long()
    }

    pub fn short_ticker(&self) -> &Ticker {
        self.pair_id.short()
    }

    pub fn hedge_ratio(&self) -> f64 {
        self.hedge_ratio
    }

    pub fn entry_z_score(&self) -> f64 {
        self.entry_z_score
    }

    pub fn opened_at(&self) -> DateTime<Utc> {
        self.opened_at
    }

    /// How long the pair has been held, in whole minutes.
    pub fn minutes_held(&self, now: DateTime<Utc>) -> i64 {
        (now - self.opened_at).num_minutes()
    }
}

/// A pair that closed, with the window its fills fall inside.
///
/// The window is what the post-close attribution joins account activities against: a fill for one
/// of the legs, between the open and the close, belongs to this pair.
#[derive(Debug, Clone, PartialEq)]
pub struct ClosedPair {
    id: Uuid,
    pair_id: PairID,
    opened_at: DateTime<Utc>,
    closed_at: DateTime<Utc>,
    close_reason: CloseReason,
}

impl ClosedPair {
    pub fn new(
        id: Uuid,
        pair_id: PairID,
        opened_at: DateTime<Utc>,
        closed_at: DateTime<Utc>,
        close_reason: CloseReason,
    ) -> Self {
        Self {
            id,
            pair_id,
            opened_at,
            closed_at,
            close_reason,
        }
    }

    pub fn id(&self) -> Uuid {
        self.id
    }

    pub fn pair_id(&self) -> &PairID {
        &self.pair_id
    }

    pub fn opened_at(&self) -> DateTime<Utc> {
        self.opened_at
    }

    pub fn closed_at(&self) -> DateTime<Utc> {
        self.closed_at
    }

    pub fn close_reason(&self) -> CloseReason {
        self.close_reason
    }

    /// Whether an activity at `instant` on `ticker` belongs to this pair.
    ///
    /// The bounds are inclusive at both ends. A fill timestamped exactly at the open is the opening
    /// fill, and one timestamped exactly at the close is the closing fill; excluding either would
    /// drop the trade this pair exists to record.
    pub fn covers(&self, ticker: &Ticker, instant: DateTime<Utc>) -> bool {
        (ticker == self.pair_id.long() || ticker == self.pair_id.short())
            && instant >= self.opened_at
            && instant <= self.closed_at
    }
}

/// Loads every pair currently open.
///
/// Rows whose stored `pair_id` no longer parses are dropped with a warning rather than failing the
/// pass. They cannot be acted on — there is no way to know which symbols to close — and stopping
/// here would take the pairs that *are* readable down with them.
pub async fn load_open_pairs(pool: &PgPool) -> Result<Vec<OpenPair>, PairsError> {
    let rows = sqlx::query!(
        r#"SELECT id, pair_id, hedge_ratio, entry_z_score, opened_at
           FROM equity_pairs
           WHERE status = 'open'
           ORDER BY opened_at"#
    )
    .fetch_all(pool)
    .await?;

    let mut pairs = Vec::with_capacity(rows.len());
    for row in rows {
        let Some(pair_id) = PairID::parse(&row.pair_id) else {
            warn!(
                pair_id = %row.pair_id,
                id = %row.id,
                "Dropped an open pair whose identifier no longer parses"
            );
            continue;
        };
        pairs.push(OpenPair::new(
            row.id,
            pair_id,
            row.hedge_ratio,
            row.entry_z_score,
            row.opened_at,
        ));
    }
    Ok(pairs)
}

/// Records a newly opened pair and returns its identifier.
///
/// `long_ticker` and `short_ticker` are written alongside `pair_id` even though both are derivable
/// from it. They exist so the attribution join and the dashboard can match on a symbol without
/// parsing every identifier in the table.
pub async fn record_open(
    pool: &PgPool,
    entry: &PairEntry,
    opened_at: DateTime<Utc>,
) -> Result<Uuid, PairsError> {
    let id = Uuid::new_v4();
    sqlx::query!(
        r#"INSERT INTO equity_pairs (
               id, pair_id, long_ticker, short_ticker, hedge_ratio, entry_z_score,
               signal_strength, model_run_id, status, opened_at
           )
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'open', $9)"#,
        id,
        entry.pair_id().as_str(),
        entry.long_ticker().as_str(),
        entry.short_ticker().as_str(),
        entry.hedge_ratio(),
        entry.entry_z_score(),
        entry.signal_strength(),
        entry.model_run_id(),
        opened_at,
    )
    .execute(pool)
    .await?;

    info!(
        pair_id = %entry.pair_id(),
        entry_z_score = entry.entry_z_score(),
        "Pair opened"
    );
    Ok(id)
}

/// Marks a pair closed. Returns `false` when it was already closed.
///
/// The `status = 'open'` predicate is what makes this idempotent: a replayed liquidation, or a
/// pass that raced the pre-close fail-safe, updates nothing rather than overwriting the original
/// close reason with `end_of_day`.
pub async fn record_close(
    pool: &PgPool,
    id: Uuid,
    reason: CloseReason,
    closed_at: DateTime<Utc>,
) -> Result<bool, PairsError> {
    let result = sqlx::query!(
        r#"UPDATE equity_pairs
           SET status = 'closed', closed_at = $1, close_reason = $2
           WHERE id = $3 AND status = 'open'"#,
        closed_at,
        reason.as_str(),
        id,
    )
    .execute(pool)
    .await?;

    let closed = result.rows_affected() > 0;
    if closed {
        info!(id = %id, reason = reason.as_str(), "Pair closed");
    } else {
        warn!(id = %id, reason = reason.as_str(), "Pair was already closed; nothing updated");
    }
    Ok(closed)
}

/// Loads pairs closed within a half-open instant range, for post-close attribution.
pub async fn load_closed_between(
    pool: &PgPool,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) -> Result<Vec<ClosedPair>, PairsError> {
    let rows = sqlx::query!(
        r#"SELECT id, pair_id, opened_at, closed_at AS "closed_at!", close_reason AS "close_reason!"
           FROM equity_pairs
           WHERE status = 'closed' AND closed_at >= $1 AND closed_at < $2
           ORDER BY closed_at"#,
        start,
        end,
    )
    .fetch_all(pool)
    .await?;

    let mut pairs = Vec::with_capacity(rows.len());
    for row in rows {
        let (Some(pair_id), Some(close_reason)) = (
            PairID::parse(&row.pair_id),
            CloseReason::parse(&row.close_reason),
        ) else {
            warn!(
                pair_id = %row.pair_id,
                close_reason = %row.close_reason,
                "Dropped a closed pair with an unreadable identifier or close reason"
            );
            continue;
        };
        pairs.push(ClosedPair::new(
            row.id,
            pair_id,
            row.opened_at,
            row.closed_at,
            close_reason,
        ));
    }
    Ok(pairs)
}

/// Writes the realized profit and loss attributed to a closed pair.
///
/// Idempotent by overwrite rather than by predicate: re-running the account sync for a session
/// recomputes the same figure from the same activities, and an attribution that has been corrected
/// upstream should land rather than be refused.
pub async fn record_realized_profit_and_loss(
    pool: &PgPool,
    id: Uuid,
    amount: Decimal,
) -> Result<bool, PairsError> {
    let result = sqlx::query!(
        r#"UPDATE equity_pairs
           SET realized_profit_and_loss = $1
           WHERE id = $2 AND status = 'closed'"#,
        amount,
        id,
    )
    .execute(pool)
    .await?;
    Ok(result.rows_affected() > 0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pair() -> PairID {
        PairID::new(Ticker::new("AAPL").unwrap(), Ticker::new("MSFT").unwrap())
    }

    /// The stored strings are a database CHECK constraint. A variant whose spelling drifts from it
    /// fails at write time, in a handler, at the close — the worst place to find out.
    #[test]
    fn test_close_reason_round_trips_every_variant() {
        for reason in CloseReason::ALL {
            assert_eq!(CloseReason::parse(reason.as_str()), Some(reason));
        }
        assert_eq!(CloseReason::parse("profit_taken"), None);
    }

    #[test]
    fn test_close_reason_separates_signal_exits_from_mechanical_ones() {
        assert!(CloseReason::Convergence.is_signal());
        assert!(CloseReason::StopLoss.is_signal());
        assert!(!CloseReason::EndOfDay.is_signal());
        assert!(!CloseReason::PositionMissing.is_signal());
    }

    /// A `NaN` hedge ratio makes every later comparison against the spread answer `false`, so the
    /// pair presents as one that simply never closes rather than as a bad record.
    #[test]
    fn test_entry_rejects_a_non_finite_hedge_ratio() {
        assert!(matches!(
            PairEntry::new(pair(), f64::NAN, 2.5, 0.1, None),
            Err(InvalidEntryError {
                field: "hedge_ratio",
                ..
            })
        ));
    }

    /// The spread is `ln(short) - hedge_ratio * ln(long)`, so entry happens when the short leg is
    /// the expensive one and the score is positive. A negative score means the legs were assigned
    /// the wrong way round, which is a silently profitable-looking way to be exactly backwards.
    #[test]
    fn test_entry_rejects_a_non_positive_z_score() {
        for value in [-2.5, 0.0] {
            assert!(matches!(
                PairEntry::new(pair(), 1.0, value, 0.1, None),
                Err(InvalidEntryError {
                    field: "entry_z_score",
                    ..
                })
            ));
        }
        assert!(PairEntry::new(pair(), 1.0, 2.5, 0.1, None).is_ok());
    }

    /// Zero is rejected as well as negative. A neutral forecast is not a reason to hold a pair, and
    /// `PairCandidate::new` already refuses it — the two validators have to agree or a future call
    /// site could build an entry the screen would never have produced.
    #[test]
    fn test_entry_rejects_a_non_positive_signal_strength() {
        for value in [-0.1, 0.0] {
            assert!(matches!(
                PairEntry::new(pair(), 1.0, 2.5, value, None),
                Err(InvalidEntryError {
                    field: "signal_strength",
                    ..
                })
            ));
        }
        assert!(PairEntry::new(pair(), 1.0, 2.5, 0.01, None).is_ok());
    }

    #[test]
    fn test_entry_exposes_the_legs_from_the_identifier() {
        let entry = PairEntry::new(pair(), 1.0, 2.5, 0.1, None).unwrap();
        assert_eq!(entry.long_ticker().as_str(), "AAPL");
        assert_eq!(entry.short_ticker().as_str(), "MSFT");
    }

    #[test]
    fn test_minutes_held_counts_from_the_open() {
        let opened = DateTime::parse_from_rfc3339("2026-07-30T14:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let pair = OpenPair::new(Uuid::new_v4(), pair(), 1.0, 2.5, opened);
        assert_eq!(
            pair.minutes_held(opened + chrono::Duration::minutes(45)),
            45
        );
    }

    /// Both bounds are inclusive. The opening fill lands exactly at the open and the closing fill
    /// exactly at the close, so a half-open window drops one leg of every pair it is asked about.
    #[test]
    fn test_closed_pair_covers_its_own_legs_within_its_own_window() {
        let opened = DateTime::parse_from_rfc3339("2026-07-30T14:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let closed = opened + chrono::Duration::hours(2);
        let closed_pair = ClosedPair::new(
            Uuid::new_v4(),
            pair(),
            opened,
            closed,
            CloseReason::Convergence,
        );

        let long = Ticker::new("AAPL").unwrap();
        let unrelated = Ticker::new("TSLA").unwrap();

        assert!(closed_pair.covers(&long, opened));
        assert!(closed_pair.covers(&long, closed));
        assert!(!closed_pair.covers(&long, opened - chrono::Duration::seconds(1)));
        assert!(!closed_pair.covers(&long, closed + chrono::Duration::seconds(1)));
        assert!(!closed_pair.covers(&unrelated, closed));
    }
}
