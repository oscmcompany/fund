//! Market data and storage, across two S3 prefixes with one owner each.
//!
//! [`archive`] owns `data/`; [`export`] owns `exports/`. Neither writes the other's prefix.

pub mod adjust;
pub mod archive;
pub mod bars;
pub mod boundaries;
pub mod cache;
pub mod calendar;
pub mod conditions;
pub mod conditions_table;
pub mod details;
pub mod export;
pub mod purge;
pub mod quotes;
pub mod splits;
pub mod trades;
pub mod truncate;
pub mod universe;

use polars::prelude::*;

/// Keeps each row's earliest `first_seen` when a fetched table is merged with the stored one.
///
/// The column is provenance: whether a corporate action was known to us when a given bar partition
/// was written, which is the question an adjustment disagreement turns into. Taking the minimum is
/// idempotent, commutative, and associative, so re-running a refresh can never advance the date.
pub(crate) fn keep_earliest_first_seen(fetched: LazyFrame, existing: LazyFrame) -> LazyFrame {
    let previously_seen = existing
        .select([col("id"), col("first_seen").alias("previous_first_seen")])
        .group_by([col("id")])
        .agg([col("previous_first_seen").min()]);

    fetched
        .join(
            previously_seen,
            [col("id")],
            [col("id")],
            JoinArgs::new(JoinType::Left),
        )
        // Defaulted before it is compared, because a row absent from the stored table joins to null
        // and a null comparison would propagate rather than choose a branch.
        .with_column(
            coalesce(&[col("previous_first_seen"), col("first_seen")]).alias("previous_first_seen"),
        )
        .with_column(
            when(col("previous_first_seen").lt(col("first_seen")))
                .then(col("previous_first_seen"))
                .otherwise(col("first_seen"))
                .alias("first_seen"),
        )
}
