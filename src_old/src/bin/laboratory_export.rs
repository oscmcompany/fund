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
    export_to(&fund::common::aws::s3_client().await, &bucket, today).await
}

/// As [`export`], against a given client, which is what makes the contract above testable.
async fn export_to(
    s3_client: &aws_sdk_s3::Client,
    bucket: &str,
    today: SessionDate,
) -> Result<(), String> {
    // The journal half runs to a summary rather than to `?`, because the paragraph above is a
    // contract and an early return here would break it in the one case it was written for.
    let mut journals = None;
    let mut refusals = Vec::new();
    match Journal::from_env() {
        Ok(journal) => {
            let summary = export_journals(&journal, s3_client, bucket, today).await;
            info!(
                objects = summary.written.len(),
                failed = summary.failed.len(),
                deleted = summary.files_deleted,
                unparsable = summary.unparsable_lines,
                "Laboratory journal exported"
            );
            journals = Some(summary);
        }
        // A refusal rather than a warning: `Journal::new` creates the directory, so the only way
        // this fails is that the box cannot write where its records go.
        Err(error) => refusals.push(format!("no journal to export: {error}")),
    }

    let logs = export_logs(
        &log_directory(),
        s3_client,
        bucket,
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

    refusals.extend(unshipped(journals.as_ref(), &logs));
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
/// Two things that read as clean runs and are not: a directory that could not be listed, because
/// finding no logs and failing to look are otherwise the same output, and a line the Parquet does
/// not hold, because its file is kept rather than deleted and those records never shipped.
fn unshipped(
    journals: Option<&fund::laboratory::export::ExportSummary>,
    logs: &fund::data::export::LogExportSummary,
) -> Vec<String> {
    let mut refusals: Vec<String> = Vec::new();
    if let Some(journals) = journals {
        refusals.extend(journals.failed.iter().cloned());
        if journals.unparsable_lines > 0 {
            refusals.push(format!(
                "{} journal line(s) the Parquet does not hold",
                journals.unparsable_lines
            ));
        }
    }
    refusals.extend(
        logs.failed
            .iter()
            .map(|(date, service, error)| format!("{date} {service}: {error}")),
    );
    if let Some(error) = &logs.directory_error {
        refusals.push(format!("log directory: {error}"));
    }
    if logs.unparsable_lines > 0 {
        refusals.push(format!(
            "{} log line(s) the Parquet does not hold",
            logs.unparsable_lines
        ));
    }
    refusals
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_smithy_http_client::test_util::infallible_client_fn;
    use aws_smithy_types::body::SdkBody;
    use fund::data::export::LogExportSummary;
    use fund::laboratory::export::ExportSummary;
    use percent_encoding::percent_decode_str;

    /// An S3 client that answers from memory and keeps the keys it was handed.
    fn capturing_s3_client(
        captured: std::sync::Arc<std::sync::Mutex<Vec<String>>>,
    ) -> aws_sdk_s3::Client {
        let http_client = infallible_client_fn(move |request| {
            let key = percent_decode_str(request.uri().path())
                .decode_utf8_lossy()
                .trim_start_matches('/')
                .trim_start_matches("test-bucket/")
                .to_string();
            captured
                .lock()
                .expect("the recorder must not be poisoned")
                .push(key);
            http::Response::builder()
                .status(200)
                .body(SdkBody::empty())
                .expect("a canned response must build")
        });
        aws_sdk_s3::Client::from_conf(
            aws_sdk_s3::Config::builder()
                .behavior_version(aws_sdk_s3::config::BehaviorVersion::latest())
                .region(aws_sdk_s3::config::Region::new("us-east-1"))
                .credentials_provider(aws_sdk_s3::config::Credentials::new(
                    "test-key",
                    "test-secret",
                    None,
                    None,
                    "test",
                ))
                .http_client(http_client)
                .build(),
        )
    }

    /// The contract on `export_to`, which its own `?` used to break: the logs are most worth having
    /// on the day the journal could not be opened, so a journal that will not open must not stop
    /// them shipping.
    #[tokio::test]
    #[serial_test::serial]
    async fn test_a_journal_that_cannot_open_still_ships_the_logs() {
        let directory =
            std::env::temp_dir().join(format!("fund-laboratory-export-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&directory);
        std::fs::create_dir_all(&directory).expect("the directory must be creatable");
        // A file where the journal wants a directory, so `create_dir_all` fails for a reason no
        // permission juggling is needed to produce.
        let blocker = directory.join("blocker");
        std::fs::write(&blocker, "").expect("the file must be writable");
        std::fs::write(
            directory.join("2026-08-18.researcher.log"),
            "{\"timestamp\":\"2026-08-18T20:00:00Z\",\"level\":\"INFO\",\"target\":\"t\",\"fields\":{\"message\":\"m\"}}\n",
        )
        .expect("the log must be writable");

        let captured = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let outcome = {
            // SAFETY: Protected by #[serial_test::serial] — no concurrent env access.
            unsafe {
                std::env::set_var("FUND_JOURNAL_DIRECTORY", blocker.join("journal"));
                std::env::set_var("FUND_LOG_DIRECTORY", &directory);
            }
            let outcome = export_to(
                &capturing_s3_client(captured.clone()),
                "test-bucket",
                SessionDate::from_date(
                    chrono::NaiveDate::from_ymd_opt(2026, 8, 18).expect("a real date"),
                ),
            )
            .await;
            // SAFETY: Protected by #[serial_test::serial] — no concurrent env access.
            unsafe {
                std::env::remove_var("FUND_JOURNAL_DIRECTORY");
                std::env::remove_var("FUND_LOG_DIRECTORY");
            }
            outcome
        };

        let error = outcome.expect_err("an unopenable journal is a refusal");
        assert!(error.contains("no journal to export"), "{error}");
        let keys = captured.lock().expect("the recorder must not be poisoned");
        assert_eq!(
            *keys,
            vec![
                "exports/logs/producer=researcher/service=researcher/year=2026/month=08/day=18/data.parquet"
            ],
            "the log shipped even though the journal could not be opened"
        );
        drop(keys);
        let _ = std::fs::remove_dir_all(&directory);
    }

    /// A listing that failed reads as an empty directory, so without this a box whose logs were
    /// unreadable would exit zero having shipped nothing.
    #[test]
    fn test_a_log_directory_that_could_not_be_listed_is_a_refusal() {
        let logs = LogExportSummary {
            directory_error: Some("permission denied".to_string()),
            ..LogExportSummary::default()
        };

        let refusals = unshipped(Some(&ExportSummary::default()), &logs);

        assert_eq!(refusals, vec!["log directory: permission denied"]);
    }

    /// An unparsable line keeps its file from being deleted, so those records did not ship. Without
    /// this the run exits zero and `run-researcher` reports "Records exported" over an incomplete
    /// one.
    #[test]
    fn test_a_line_the_parquet_does_not_hold_is_a_refusal() {
        let journals = ExportSummary {
            unparsable_lines: 2,
            ..ExportSummary::default()
        };
        let logs = LogExportSummary {
            unparsable_lines: 3,
            ..LogExportSummary::default()
        };

        let refusals = unshipped(Some(&journals), &logs);

        assert_eq!(
            refusals,
            vec![
                "2 journal line(s) the Parquet does not hold",
                "3 log line(s) the Parquet does not hold"
            ]
        );
    }

    #[test]
    fn test_a_clean_run_refuses_nothing() {
        let refusals = unshipped(
            Some(&ExportSummary::default()),
            &LogExportSummary::default(),
        );

        assert!(refusals.is_empty(), "{refusals:?}");
    }
}
