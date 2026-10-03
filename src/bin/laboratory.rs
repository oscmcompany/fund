//! Reads the experiment catalogue: every `experiment_ran` the researcher journals hold, one line each, oldest first.
//! Exits 0 when every journal read whole, 1 when any bucket, object or line did not, and 2 when the query was refused.

use std::collections::BTreeSet;
use std::process::ExitCode;

use chrono::{DateTime, NaiveDate, Utc};
use clap::{Parser, Subcommand};

use fund::archive::{Archive, journal};
use fund::common::heal::Leg;
use fund::common::journal::ReadLine;
use fund::common::laboratory::catalogue::{Query, line};
use fund::common::storage::{Host, Key};
use fund::common::time::SessionDate;

const REFUSED_TO_START: u8 = 2;

#[derive(Parser)]
#[command(about = "Read what the laboratory has journaled")]
struct Arguments {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Lists every journaled experiment the filters select.
    Experiments {
        /// Selects labels containing this text, ignoring case.
        #[arg(long)]
        label: Option<String>,
        /// Selects experiments that read this leg, such as `massive_daily_bars`.
        #[arg(long)]
        leg: Option<Leg>,
        /// The first session to read, as YYYY-MM-DD in Eastern terms.
        #[arg(long, value_parser = parse_session)]
        since: Option<SessionDate>,
        /// The last session to read, as YYYY-MM-DD in Eastern terms.
        #[arg(long, value_parser = parse_session)]
        until: Option<SessionDate>,
        /// A records bucket to read, repeatable; defaults to this profile's `AWS_S3_RECORDS_BUCKET_NAME`.
        #[arg(long = "records-bucket")]
        records_buckets: Vec<String>,
    },
}

fn parse_session(raw: &str) -> Result<SessionDate, String> {
    NaiveDate::parse_from_str(raw, "%Y-%m-%d")
        .map(SessionDate::from_date)
        .map_err(|error| format!("{raw} is not a YYYY-MM-DD date: {error}"))
}

#[tokio::main]
async fn main() -> ExitCode {
    match Arguments::parse().command {
        Command::Experiments {
            label,
            leg,
            since,
            until,
            records_buckets,
        } => {
            let query = match Query::new(label.as_deref(), leg, since, until) {
                Ok(query) => query,
                Err(refusal) => {
                    eprintln!("The query was refused: {refusal}");
                    return ExitCode::from(REFUSED_TO_START);
                }
            };
            experiments(&query, records_buckets).await
        }
    }
}

async fn experiments(query: &Query, records_buckets: Vec<String>) -> ExitCode {
    let configuration = aws_config::load_from_env().await;
    let buckets = if records_buckets.is_empty() {
        match Archive::records(&configuration) {
            Ok(records) => vec![records],
            Err(refusal) => {
                eprintln!("No records bucket: {refusal}");
                return ExitCode::from(REFUSED_TO_START);
            }
        }
    } else {
        // A bucket named twice is read once, so no experiment prints twice.
        records_buckets
            .into_iter()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .map(|name| Archive::records_in(&configuration, name))
            .collect()
    };
    let series = Key::Journal {
        host: Host::Researcher,
        session: SessionDate::at(Utc::now()),
    }
    .series();
    let mut lines: Vec<(DateTime<Utc>, String)> = Vec::new();
    let (mut objects, mut unreadable, mut failures) = (0_usize, 0_usize, 0_usize);
    for records in &buckets {
        let paths = match records.list(&series).await {
            Ok(paths) => paths,
            Err(error) => {
                eprintln!("{} did not list: {error}", records.bucket_name());
                failures += 1;
                continue;
            }
        };
        for path in paths {
            let key = match Key::parse(&path) {
                Ok(key) => key,
                Err(_) => {
                    eprintln!(
                        "{} holds {path}, which is not a journal",
                        records.bucket_name()
                    );
                    failures += 1;
                    continue;
                }
            };
            if !query.selects_session(key.session()) {
                continue;
            }
            let held = match records.get(&key).await {
                Ok(Some(body)) => {
                    journal::decode(&key, body).map_err(|refusal| format!("{refusal:?}"))
                }
                Ok(None) => Err("it was listed but is gone".to_string()),
                Err(error) => Err(error.to_string()),
            };
            match held {
                Ok(held) => {
                    objects += 1;
                    for read in held {
                        match read {
                            ReadLine::Read(record) => {
                                if let Some(experiment) = query.selects(&record) {
                                    lines.push((record.timestamp(), line(&record, experiment)));
                                }
                            }
                            ReadLine::Unreadable { line, cause, .. } => {
                                eprintln!("{} line {line} is unreadable: {cause:?}", key.path());
                                unreadable += 1;
                            }
                        }
                    }
                }
                Err(reason) => {
                    eprintln!("{} did not read: {reason}", key.path());
                    failures += 1;
                }
            }
        }
    }
    lines.sort();
    for (_, text) in &lines {
        println!("{text}");
    }
    eprintln!(
        "{} experiments from {objects} journals in {} buckets; {unreadable} unreadable lines, {failures} failures",
        lines.len(),
        buckets.len(),
    );
    // An unreadable line may be an experiment this build cannot read, so the catalogue is not known to be whole.
    match failures + unreadable {
        0 => ExitCode::SUCCESS,
        _ => ExitCode::FAILURE,
    }
}
