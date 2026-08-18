//! Nightly purge of rows the export has already written to S3.
//!
//! Runs after the export and never on its own schedule, which is the whole safety property.

use sqlx::PgPool;
use tracing::{info, warn};

use crate::common::types::Dataset;

/// Days of history retained in PostgreSQL after export.
///
/// A window rather than a truncation: a few days in PostgreSQL keeps questions about yesterday a
/// query rather than an S3 download, and gives a silently failed export several nights to be
/// noticed. `i32` because it binds directly to `make_interval(days => ...)`, an `int4`.
pub const RETENTION_DAYS: i32 = 7;

/// Retention must leave enough nights that a silently failed export is noticed before the rows it
/// missed become unrecoverable. Enforced at compile time rather than in a test, because the failure
/// mode is someone tuning the constant down, not the code drifting.
const _: () = assert!(RETENTION_DAYS >= 3);

/// Tables the purge owns, in the order it visits them.
///
/// Both are append-only and fully represented in the nightly export. The other datasets are absent
/// because a row can change after the day it was written, and `equity_bars` because TimescaleDB's
/// retention policy owns it — two mechanisms deleting from one table is how a rolling window
/// becomes an empty one.
const PURGED_TABLES: &[(Dataset, &str)] = &[
    (Dataset::Events, "created_at"),
    (Dataset::EquityPredictions, "timestamp"),
];

/// What one purge accomplished.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct PurgeSummary {
    /// `(table, rows_deleted)` for each table purged cleanly.
    pub purged: Vec<(Dataset, u64)>,
    /// `(table, error)` for each table whose delete failed.
    pub failed: Vec<(Dataset, String)>,
}

impl PurgeSummary {
    /// Rows deleted across every table that purged cleanly.
    ///
    /// Failed tables contribute nothing, so this is what was actually removed rather than what was
    /// attempted.
    pub fn total_rows(&self) -> u64 {
        self.purged.iter().map(|(_, rows)| rows).sum()
    }

    /// Whether every table purged without error.
    pub fn is_clean(&self) -> bool {
        self.failed.is_empty()
    }
}

/// Deletes rows older than [`RETENTION_DAYS`] from the tables the purge owns.
///
/// Never returns `Err`: one table failing should not prevent the others from being purged, and the
/// caller reports the failures in the completion event.
pub async fn purge_exported_tables(pool: &PgPool) -> PurgeSummary {
    let mut summary = PurgeSummary::default();

    for (dataset, timestamp_column) in PURGED_TABLES {
        let table = dataset.as_str();
        // The table and column names come from the constant above, never from input, so formatting
        // them into the statement is safe. The cutoff is still bound as a parameter, as an integer
        // through `make_interval` rather than as a string concatenated into an interval literal --
        // that keeps the type checking in the database instead of in a string cast.
        let statement = format!(
            "DELETE FROM {table} WHERE {timestamp_column} < now() - make_interval(days => $1)"
        );
        match sqlx::query(&statement)
            .bind(RETENTION_DAYS)
            .execute(pool)
            .await
        {
            Ok(result) => {
                let rows = result.rows_affected();
                info!(table, rows, "Purged exported rows");
                summary.purged.push((*dataset, rows));
            }
            Err(error) => {
                warn!(table, error = %error, "Failed to purge table, continuing with the rest");
                summary.failed.push((*dataset, error.to_string()));
            }
        }
    }

    summary
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `equity_bars` must never appear here: TimescaleDB's retention policy already deletes from
    /// it, and a second mechanism on a different window turns a rolling 90 days into whichever
    /// window is shorter. [`Dataset`] has no variant for it, so this now holds by construction —
    /// the assertion is that no variant *names* it, which is what a future variant would break.
    #[test]
    fn test_purge_does_not_own_bars() {
        let tables: Vec<&str> = PURGED_TABLES
            .iter()
            .map(|(dataset, _)| dataset.as_str())
            .collect();
        assert!(!tables.contains(&"equity_bars"));
    }

    /// Tables that hold state a later session can change must not be purged on a rolling window —
    /// a pair opened eight days ago and still open would vanish while the position was live.
    #[test]
    fn test_purge_does_not_own_mutable_state() {
        let tables: Vec<&str> = PURGED_TABLES
            .iter()
            .map(|(dataset, _)| dataset.as_str())
            .collect();
        for protected in ["equity_pairs", "account_snapshots", "account_activities"] {
            assert!(
                !tables.contains(&protected),
                "{protected} must not be purged on a rolling window"
            );
        }
    }

    #[test]
    fn test_purge_owns_the_append_only_tables() {
        let tables: Vec<&str> = PURGED_TABLES
            .iter()
            .map(|(dataset, _)| dataset.as_str())
            .collect();
        assert_eq!(tables, vec!["events", "equity_predictions"]);
    }

    #[test]
    fn test_summary_totals_only_successful_tables() {
        let summary = PurgeSummary {
            purged: vec![(Dataset::Events, 100), (Dataset::EquityPredictions, 50)],
            failed: vec![(Dataset::EquityPairs, "boom".into())],
        };
        assert_eq!(summary.total_rows(), 150);
        assert!(!summary.is_clean());
        assert!(PurgeSummary::default().is_clean());
    }
}
