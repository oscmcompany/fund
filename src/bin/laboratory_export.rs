//! Ships the researcher's journal and logs to the records bucket.
//!
//! The export half of `tools/run-researcher`, which runs it as its last leg.

use chrono::Utc;
use tracing::{error, info};

use fund::common::aws::Producer;
use fund::common::log::{init_tracing, log_directory};
use fund::common::types::SessionDate;
use fund::data::export::export_logs;
use fund::laboratory::export::export_journals;
use fund::laboratory::journal::Journal;

const LOG_FILE: &str = "laboratory-export.log";

#[tokio::main]
async fn main() {
    fund::common::crypto::install_default_crypto_provider();
    let tracing_guard = init_tracing(LOG_FILE, Some("info"), "laboratory-export");

    let code = match export(SessionDate::at(Utc::now())).await {
        Ok(()) => 0,
        Err(message) => {
            error!(%message, "Export failed");
            eprintln!("{message}");
            1
        }
    };

    // `std::process::exit` runs no destructors, so the appender's guard would never drop and its
    // buffered lines would be lost — exactly when the failure log matters.
    drop(tracing_guard);
    std::process::exit(code);
}

/// Both exports, then one refusal naming everything that did not ship.
///
/// Attempted in full before either failure is raised: the logs are most worth having on the day the
/// journal could not be written, and returning early would drop them.
async fn export(today: SessionDate) -> Result<(), String> {
    let bucket = std::env::var("AWS_S3_RECORDS_BUCKET_NAME")
        .map_err(|_| "AWS_S3_RECORDS_BUCKET_NAME must be set".to_string())?;
    let s3_client = fund::common::aws::s3_client().await;

    let journal = Journal::from_env().map_err(|error| format!("no journal to export: {error}"))?;
    let journals = export_journals(&journal, &s3_client, &bucket, today).await;
    info!(
        objects = journals.written.len(),
        failed = journals.failed.len(),
        deleted = journals.files_deleted,
        unparsable = journals.unparsable_lines,
        "Laboratory journal exported"
    );

    let logs = export_logs(
        &log_directory(),
        &s3_client,
        &bucket,
        today,
        Producer::Researcher,
    )
    .await;
    info!(
        files = logs.exported.len(),
        lines = logs.total_lines(),
        failed = logs.failed.len(),
        unparsable = logs.unparsable_lines,
        "Researcher logs exported"
    );

    let refusals = unshipped(&journals, &logs);
    if refusals.is_empty() {
        return Ok(());
    }
    Err(format!(
        "{} of this box's records did not ship: {}",
        refusals.len(),
        refusals.join("; ")
    ))
}

/// Everything that stayed on the box, from both halves.
///
/// A directory that could not be listed counts: finding no logs and failing to look are the same
/// output otherwise, and only one of them is a clean run.
fn unshipped(
    journals: &fund::laboratory::export::ExportSummary,
    logs: &fund::data::export::LogExportSummary,
) -> Vec<String> {
    let mut refusals: Vec<String> = journals.failed.clone();
    refusals.extend(
        logs.failed
            .iter()
            .map(|(date, service, error)| format!("{date} {service}: {error}")),
    );
    if let Some(error) = &logs.directory_error {
        refusals.push(format!("log directory: {error}"));
    }
    refusals
}

#[cfg(test)]
mod tests {
    use super::*;
    use fund::data::export::LogExportSummary;
    use fund::laboratory::export::ExportSummary;

    /// A listing that failed reads as an empty directory, so without this a box whose logs were
    /// unreadable would exit zero having shipped nothing.
    #[test]
    fn test_a_log_directory_that_could_not_be_listed_is_a_refusal() {
        let logs = LogExportSummary {
            directory_error: Some("permission denied".to_string()),
            ..LogExportSummary::default()
        };

        let refusals = unshipped(&ExportSummary::default(), &logs);

        assert_eq!(refusals, vec!["log directory: permission denied"]);
    }

    #[test]
    fn test_a_clean_run_refuses_nothing() {
        let refusals = unshipped(&ExportSummary::default(), &LogExportSummary::default());

        assert!(refusals.is_empty(), "{refusals:?}");
    }
}
