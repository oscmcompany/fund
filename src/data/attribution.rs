//! Recovers which route built each archived object, from pass logs and from declared configuration.
//!
//! Logs are one source among two, not a special case; they are simply the only record of the quote
//! archive's two routes.

use std::collections::BTreeMap;
use std::path::Path;

use chrono::NaiveDate;
use serde::Deserialize;

use crate::common::provenance::{AlpacaPlan, MassivePlan, MassiveTransport, Provenance};
use crate::common::types::BarInterval;

/// What one log line attests to: a dataset, optionally one cadence of it, and a session.
///
/// The cadence is `None` where a pass writes every cadence from a single fold — quotes and trades
/// both do — and `Some` where it does not. **One-minute bars are the case that forces this**: they
/// come from a flat file while the five-minute and daily partitions for the same session come from
/// Massive's REST route, so a key without a cadence would attribute all three to the flat file.
pub type Attribution = BTreeMap<(String, Option<BarInterval>, NaiveDate), Vec<Provenance>>;

#[derive(Debug, thiserror::Error)]
pub enum AttributionError {
    #[error("could not read {path}: {source}")]
    Read {
        path: String,
        source: std::io::Error,
    },
    #[error("could not parse {path}: {message}")]
    Parse { path: String, message: String },
}

/// One log line, of the two shapes that name a route.
#[derive(Deserialize)]
struct Line {
    fields: Fields,
}

#[derive(Deserialize)]
struct Fields {
    message: String,
    /// Present on `Folding a session's quoted book`, which names its route directly.
    #[serde(default)]
    session: Option<String>,
    #[serde(default)]
    source: Option<String>,
    /// Present on `Folded a flat file of ...`, whose vendor key names dataset and date together.
    #[serde(default)]
    key: Option<String>,
}

/// Reads every `*.log` under `directory`, recursively, and returns what each session was built by.
///
/// Unparseable lines are skipped rather than refused: a log is an artifact of a run that already
/// happened, and one malformed line must not cost the other ten thousand.
pub fn routes_from_logs(directory: &Path) -> Result<Attribution, AttributionError> {
    let mut found: Attribution = BTreeMap::new();
    for path in log_files(directory)? {
        let text = std::fs::read_to_string(&path).map_err(|source| AttributionError::Read {
            path: path.display().to_string(),
            source,
        })?;
        for line in text.lines() {
            let Ok(parsed) = serde_json::from_str::<Line>(line) else {
                continue;
            };
            if let Some((dataset, interval, date, route)) = attribute(&parsed.fields) {
                let routes = found.entry((dataset, interval, date)).or_default();
                if !routes.contains(&route) {
                    routes.push(route);
                }
            }
        }
    }
    Ok(found)
}

/// What one log line says, where it says anything.
fn attribute(fields: &Fields) -> Option<(String, Option<BarInterval>, NaiveDate, Provenance)> {
    match fields.message.as_str() {
        // The quote pass names its route in the line itself, which is the only reason the two
        // sources are separable at all.
        "Folding a session's quoted book" => {
            let date = fields.session.as_deref()?.parse().ok()?;
            let route = match fields.source.as_deref()? {
                "per-name" => Provenance::alpaca(AlpacaPlan::AlgoTraderPlus),
                "whole-session" => {
                    Provenance::massive(MassivePlan::StocksAdvanced, MassiveTransport::FlatFile)
                }
                _ => return None,
            };
            // Every cadence, because one fold writes all of them.
            Some(("equity_quotes".to_string(), None, date, route))
        }
        // A flat-file fold names the vendor object, which carries both dataset and date.
        "Folded a flat file of trades" | "Folded a flat file of bars" => {
            let (dataset, interval, date) = vendor_key_parts(fields.key.as_deref()?)?;
            Some((
                dataset,
                interval,
                date,
                Provenance::massive(MassivePlan::StocksAdvanced, MassiveTransport::FlatFile),
            ))
        }
        _ => None,
    }
}

/// Splits `us_stocks_sip/trades_v1/2023/06/2023-06-14.csv.gz` into our dataset name and its date.
fn vendor_key_parts(key: &str) -> Option<(String, Option<BarInterval>, NaiveDate)> {
    let mut segments = key.split('/');
    segments.next()?;
    // `minute_aggs_v1` attests to the one-minute cadence and to nothing else; the other two folds
    // write every cadence they have from the same read.
    let (dataset, interval) = match segments.next()? {
        "trades_v1" => ("equity_trades", None),
        "minute_aggs_v1" => ("equity_bars", Some(BarInterval::OneMinute)),
        "quotes_v1" => ("equity_quotes", None),
        _ => return None,
    };
    let file = key.rsplit('/').next()?;
    let date = file.split('.').next()?.parse().ok()?;
    Some((dataset.to_string(), interval, date))
}

/// A route declared for a whole prefix rather than observed on a session.
///
/// The other half of an attribution: a dataset whose route is uniform by construction is described
/// rather than discovered, which logs cannot do for a pass that never logged.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct Declaration {
    pub dataset: String,
    /// One cadence, or every cadence of the dataset when absent.
    #[serde(default)]
    pub interval: Option<BarInterval>,
    #[serde(flatten)]
    pub provenance: Provenance,
}

#[derive(Deserialize)]
struct DeclarationFile {
    declarations: Vec<Declaration>,
}

/// Reads declared routes from a JSON file.
///
/// Refused rather than skipped on a parse failure, unlike a log line: a configuration that does not
/// parse is a mistake being made now, where a malformed log line is a run that already happened.
pub fn routes_from_configuration(path: &Path) -> Result<Vec<Declaration>, AttributionError> {
    let text = std::fs::read_to_string(path).map_err(|source| AttributionError::Read {
        path: path.display().to_string(),
        source,
    })?;
    let parsed: DeclarationFile =
        serde_json::from_str(&text).map_err(|source| AttributionError::Parse {
            path: path.display().to_string(),
            message: source.to_string(),
        })?;
    Ok(parsed.declarations)
}

/// Every `*.log` beneath `directory`, including subdirectories.
fn log_files(directory: &Path) -> Result<Vec<std::path::PathBuf>, AttributionError> {
    let mut found = Vec::new();
    let entries = std::fs::read_dir(directory).map_err(|source| AttributionError::Read {
        path: directory.display().to_string(),
        source,
    })?;
    for entry in entries {
        // Propagated rather than skipped: a directory that cannot be walked yields a partial
        // attribution, and a partial attribution silently under-stamps the archive.
        let entry = entry.map_err(|source| AttributionError::Read {
            path: directory.display().to_string(),
            source,
        })?;
        let path = entry.path();
        if path.is_dir() {
            found.extend(log_files(&path)?);
        } else if path.extension().is_some_and(|extension| extension == "log") {
            found.push(path);
        }
    }
    found.sort();
    Ok(found)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write(directory: &Path, name: &str, lines: &[&str]) {
        std::fs::write(directory.join(name), lines.join("\n")).expect("the fixture writes");
    }

    fn session(year: i32, month: u32, day: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(year, month, day).expect("a real date")
    }

    #[test]
    fn a_quote_session_touched_twice_records_both_routes_in_order() {
        let directory = tempfile::tempdir().expect("a temporary directory");
        write(
            directory.path(),
            "a.log",
            &[
                r#"{"fields":{"message":"Folding a session's quoted book","session":"2025-11-14","source":"whole-session"}}"#,
                r#"{"fields":{"message":"Folding a session's quoted book","session":"2025-11-14","source":"per-name"}}"#,
                // A re-run of the same route must not grow the set.
                r#"{"fields":{"message":"Folding a session's quoted book","session":"2025-11-14","source":"per-name"}}"#,
            ],
        );

        let found = routes_from_logs(directory.path()).expect("the logs parse");
        assert_eq!(
            found[&("equity_quotes".to_string(), None, session(2025, 11, 14))],
            vec![
                Provenance::massive(MassivePlan::StocksAdvanced, MassiveTransport::FlatFile),
                Provenance::alpaca(AlpacaPlan::AlgoTraderPlus),
            ]
        );
    }

    #[test]
    fn a_flat_file_fold_is_attributed_from_its_vendor_key() {
        let directory = tempfile::tempdir().expect("a temporary directory");
        write(
            directory.path(),
            "a.log",
            &[
                r#"{"fields":{"message":"Folded a flat file of trades","key":"us_stocks_sip/trades_v1/2023/06/2023-06-14.csv.gz"}}"#,
                r#"{"fields":{"message":"Folded a flat file of bars","key":"us_stocks_sip/minute_aggs_v1/2023/12/2023-12-20.csv.gz"}}"#,
            ],
        );

        let found = routes_from_logs(directory.path()).expect("the logs parse");
        let flat_file =
            Provenance::massive(MassivePlan::StocksAdvanced, MassiveTransport::FlatFile);
        assert_eq!(
            found[&("equity_trades".to_string(), None, session(2023, 6, 14))],
            vec![flat_file]
        );
        assert_eq!(
            found[&(
                "equity_bars".to_string(),
                Some(BarInterval::OneMinute),
                session(2023, 12, 20)
            )],
            vec![flat_file]
        );
        // The five-minute and daily partitions for that session came from Massive's REST route, so
        // the flat-file line must not speak for them.
        assert!(!found.contains_key(&("equity_bars".to_string(), None, session(2023, 12, 20))));
    }

    #[test]
    fn a_malformed_line_costs_only_itself() {
        let directory = tempfile::tempdir().expect("a temporary directory");
        write(
            directory.path(),
            "a.log",
            &[
                "not json at all",
                r#"{"fields":{"message":"Something else entirely"}}"#,
                r#"{"fields":{"message":"Folding a session's quoted book","session":"nonsense","source":"per-name"}}"#,
                r#"{"fields":{"message":"Folding a session's quoted book","session":"2026-08-25","source":"per-name"}}"#,
            ],
        );

        let found = routes_from_logs(directory.path()).expect("the logs parse");
        assert_eq!(found.len(), 1, "only the one well-formed line counts");
        assert!(found.contains_key(&("equity_quotes".to_string(), None, session(2026, 8, 25))));
    }

    #[test]
    fn a_declaration_names_a_route_for_a_whole_prefix() {
        let directory = tempfile::tempdir().expect("a temporary directory");
        let path = directory.path().join("declared.json");
        std::fs::write(
            &path,
            r#"{"declarations":[
                {"dataset":"equity_bars","interval":"five_minute",
                 "provider":"massive","subscription":"stocks_starter","transport":"rest"},
                {"dataset":"raw_equity_trades",
                 "provider":"massive","subscription":"stocks_advanced","transport":"flat_file"}
            ]}"#,
        )
        .expect("the fixture writes");

        let declared = routes_from_configuration(&path).expect("the configuration parses");
        assert_eq!(declared.len(), 2);
        assert_eq!(declared[0].dataset, "equity_bars");
        assert_eq!(declared[0].interval, Some(BarInterval::FiveMinute));
        assert_eq!(
            declared[0].provenance,
            Provenance::massive(MassivePlan::StocksStarter, MassiveTransport::Rest)
        );
        // A raw dataset has no cadence, and an absent field must read as "every one".
        assert_eq!(declared[1].interval, None);
    }

    /// A configuration that does not parse is a mistake being made now, unlike a malformed log line.
    #[test]
    fn a_broken_configuration_is_refused_rather_than_skipped() {
        let directory = tempfile::tempdir().expect("a temporary directory");
        let path = directory.path().join("declared.json");
        std::fs::write(&path, r#"{"declarations":[{"dataset":"equity_bars"}]}"#)
            .expect("the fixture writes");
        assert!(routes_from_configuration(&path).is_err());
    }

    /// The file shipped in the repository must be the shape the reader expects.
    #[test]
    fn the_checked_in_declaration_parses() {
        let declared = routes_from_configuration(Path::new("data/archive_provenance.json"))
            .expect("the shipped configuration parses");
        assert!(
            declared
                .iter()
                .any(|entry| entry.dataset == "raw_equity_trades"),
            "the raw tee must be declared; nothing else can attribute it"
        );
        assert!(
            declared
                .iter()
                .any(|entry| entry.interval == Some(BarInterval::OneDay)),
            "daily bars have no log to attribute them"
        );
    }

    #[test]
    fn logs_are_found_in_subdirectories_too() {
        let directory = tempfile::tempdir().expect("a temporary directory");
        let nested = directory.path().join("superseded");
        std::fs::create_dir(&nested).expect("the subdirectory is created");
        write(
            &nested,
            "old.log",
            &[
                r#"{"fields":{"message":"Folded a flat file of trades","key":"us_stocks_sip/trades_v1/2021/08/2021-08-26.csv.gz"}}"#,
            ],
        );

        let found = routes_from_logs(directory.path()).expect("the logs parse");
        assert!(found.contains_key(&("equity_trades".to_string(), None, session(2021, 8, 26))));
    }
}
