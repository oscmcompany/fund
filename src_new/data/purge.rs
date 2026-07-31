//! Nightly purge of exported rows.
//!
//! Runs inside the export handler, after S3 has the data, and only over tables whose history lives
//! in the export rather than in the database. That ordering is the entire safety property: a purge
//! that ran first, or ran on its own schedule, could delete rows that never reached S3.
//!
//! Retention is a window rather than a truncation. Keeping a few days in PostgreSQL costs almost
//! nothing and means a question about yesterday — why did that pair close, what did the model say —
//! is a query rather than an S3 download. It also means an export that silently failed has several
//! nights to be noticed before the rows are gone.
//!
//! `equity_bars` is deliberately absent: TimescaleDB's retention policy owns it, and two mechanisms
//! deleting from one table is how a rolling window becomes an empty one.

use sqlx::PgPool;
use tracing::{info, warn};

/// Days of history retained in PostgreSQL after export.
pub const RETENTION_DAYS: i64 = 7;

/// Retention must leave enough nights that a silently failed export is noticed before the rows it
/// missed become unrecoverable. Enforced at compile time rather than in a test, because the failure
/// mode is someone tuning the constant down, not the code drifting.
const _: () = assert!(RETENTION_DAYS >= 3);

/// Tables the purge owns, in the order it visits them.
///
/// Both are append-only and fully represented in the nightly export. `equity_pairs`,
/// `account_snapshots`, and `account_activities` are not here: they are small, they are the
/// dashboard's history, and a row can change after the day it was written.
const PURGED_TABLES: &[(&str, &str)] = &[
    ("events", "created_at"),
    ("equity_predictions", "timestamp"),
];

/// What one purge accomplished.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct PurgeSummary {
    /// `(table, rows_deleted)` for each table purged cleanly.
    pub purged: Vec<(String, u64)>,
    /// `(table, error)` for each table whose delete failed.
    pub failed: Vec<(String, String)>,
}

impl PurgeSummary {
    pub fn total_rows(&self) -> u64 {
        self.purged.iter().map(|(_, rows)| rows).sum()
    }

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

    for (table, timestamp_column) in PURGED_TABLES {
        // The table and column names come from the constant above, never from input, so formatting
        // them into the statement is safe. The cutoff is still bound as a parameter.
        let statement = format!(
            "DELETE FROM {table} WHERE {timestamp_column} < now() - ($1 || ' days')::interval"
        );
        match sqlx::query(&statement)
            .bind(RETENTION_DAYS.to_string())
            .execute(pool)
            .await
        {
            Ok(result) => {
                let rows = result.rows_affected();
                info!(table, rows, "Purged exported rows");
                summary.purged.push(((*table).to_string(), rows));
            }
            Err(error) => {
                warn!(table, error = %error, "Failed to purge table, continuing with the rest");
                summary
                    .failed
                    .push(((*table).to_string(), error.to_string()));
            }
        }
    }

    summary
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `equity_bars` must never appear here. TimescaleDB's retention policy already deletes from
    /// it, and a second mechanism on a different window turns a rolling 90 days into whichever
    /// window is shorter.
    #[test]
    fn test_purge_does_not_own_bars() {
        let tables: Vec<&str> = PURGED_TABLES.iter().map(|(table, _)| *table).collect();
        assert!(!tables.contains(&"equity_bars"));
    }

    /// Tables that hold state a later session can change must not be purged on a rolling window —
    /// a pair opened eight days ago and still open would vanish while the position was live.
    #[test]
    fn test_purge_does_not_own_mutable_state() {
        let tables: Vec<&str> = PURGED_TABLES.iter().map(|(table, _)| *table).collect();
        for protected in ["equity_pairs", "account_snapshots", "account_activities"] {
            assert!(
                !tables.contains(&protected),
                "{protected} must not be purged on a rolling window"
            );
        }
    }

    #[test]
    fn test_purge_owns_the_append_only_tables() {
        let tables: Vec<&str> = PURGED_TABLES.iter().map(|(table, _)| *table).collect();
        assert_eq!(tables, vec!["events", "equity_predictions"]);
    }

    #[test]
    fn test_summary_totals_only_successful_tables() {
        let summary = PurgeSummary {
            purged: vec![("events".into(), 100), ("equity_predictions".into(), 50)],
            failed: vec![("other".into(), "boom".into())],
        };
        assert_eq!(summary.total_rows(), 150);
        assert!(!summary.is_clean());
        assert!(PurgeSummary::default().is_clean());
    }
}
