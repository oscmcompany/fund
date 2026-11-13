//! Prices, then pulls, one schema of Databento rows for a list of symbols into the profile bucket's `scratch/`:
//! `databento_scratch <price|pull> <schema> <first> <last> <ceiling-dollars> <symbols-file> <dataset>...`, where the
//! symbols file holds one ticker a line and `price` sends no request for rows.

use std::path::PathBuf;
use std::process::ExitCode;

use chrono::Utc;
use tracing_subscriber::EnvFilter;

use fund::common::market::{Dollars, Symbol, SymbolRefusal};
use fund::common::monoid::Tally;
use fund::common::time::{SessionDate, SessionRange};
use fund::ingest::alpaca::Alpaca;
use fund::ingest::databento::{Budget, Databento, Dataset, Priced, PullRefusal, Pulled, Schema};
use fund::journal::built_commit;
use fund::records::RefusedToStart;
use fund::storage::Bucket;
use fund::storage::scratch::{ScratchKey, land, landed};

const USAGE: &str = "databento_scratch <price|pull> <schema> <first> <last> <ceiling-dollars> <symbols-file> <dataset>...";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Command {
    Price,
    Pull,
}

#[derive(Debug, PartialEq)]
struct Arguments {
    command: Command,
    schema: Schema,
    range: SessionRange,
    ceiling: Dollars,
    symbols_path: PathBuf,
    datasets: Vec<Dataset>,
}

/// What became of one symbol's session on one dataset.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, strum::Display)]
#[strum(serialize_all = "snake_case")]
enum Outcome {
    Landed,
    AlreadyLanded,
    Unresolved,
    Empty,
    Failed,
}

fn parse_arguments(arguments: &[String]) -> Result<Arguments, String> {
    let [
        command,
        schema,
        first,
        last,
        ceiling,
        symbols_path,
        datasets @ ..,
    ] = arguments
    else {
        return Err("too few arguments".to_string());
    };
    let command = match command.as_str() {
        "price" => Command::Price,
        "pull" => Command::Pull,
        other => return Err(format!("`{other}` is neither price nor pull")),
    };
    let session = |raw: &str| {
        raw.parse()
            .map(SessionDate::from_date)
            .map_err(|error| format!("`{raw}` is not a date: {error}"))
    };
    if datasets.is_empty() {
        return Err("no dataset named".to_string());
    }
    Ok(Arguments {
        command,
        schema: schema
            .parse()
            .map_err(|_| format!("`{schema}` is not a schema"))?,
        range: SessionRange::new(session(first)?, session(last)?)
            .map_err(|refusal| refusal.to_string())?,
        ceiling: ceiling.parse().map_err(|refusal| format!("{refusal}"))?,
        symbols_path: PathBuf::from(symbols_path),
        datasets: datasets
            .iter()
            .map(|raw| Dataset::new(raw).map_err(|refusal| refusal.to_string()))
            .collect::<Result<_, _>>()?,
    })
}

/// One ticker a line; blank lines are skipped.
fn parse_symbols(text: &str) -> Result<Vec<Symbol>, SymbolRefusal> {
    text.lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(Symbol::new)
        .collect()
}

#[tokio::main]
async fn main() -> ExitCode {
    // An operator's command that ships nothing, so it logs to standard output alone.
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();
    let raw: Vec<String> = std::env::args().skip(1).collect();
    let arguments = match parse_arguments(&raw) {
        Ok(arguments) => arguments,
        Err(reason) => {
            tracing::error!(reason, usage = USAGE, "Usage refused");
            return RefusedToStart.into();
        }
    };
    let symbols = match std::fs::read_to_string(&arguments.symbols_path)
        .map_err(|error| error.to_string())
        .and_then(|text| parse_symbols(&text).map_err(|refusal| refusal.to_string()))
    {
        Ok(symbols) if !symbols.is_empty() => symbols,
        Ok(_) => {
            tracing::error!("Symbols file names no symbol");
            return RefusedToStart.into();
        }
        Err(reason) => {
            tracing::error!(reason, "Symbols not read");
            return RefusedToStart.into();
        }
    };
    let http_client = reqwest::Client::new();
    let clients = Databento::from_environment(http_client.clone())
        .and_then(|databento| Ok((databento, Alpaca::from_environment(http_client)?)));
    let (databento, alpaca) = match clients {
        Ok(clients) => clients,
        Err(refusal) => {
            tracing::error!(%refusal, "Client configuration refused");
            return RefusedToStart.into();
        }
    };
    let sessions = match alpaca.calendar(arguments.range).await {
        Ok(calendar) => calendar.trading_days_in_range(arguments.range),
        Err(error) => {
            tracing::error!(%error, "Calendar not read");
            return ExitCode::FAILURE;
        }
    };
    let whole_range = (
        arguments.range.first().bounds().0,
        arguments.range.last().bounds().1,
    );
    let mut available_sessions = Vec::new();
    let mut total = 0;
    for dataset in &arguments.datasets {
        let read = databento
            .available_sessions(dataset, &sessions, Utc::now())
            .await;
        let priced = databento
            .cost(dataset, arguments.schema, &symbols, whole_range)
            .await;
        let ((available, unavailable), priced) = match (read, priced) {
            (Ok(read), Ok(priced)) => (read, priced),
            (Err(error), _) | (_, Err(error)) => {
                tracing::error!(dataset = dataset.as_str(), %error, "Dataset not priced");
                return ExitCode::FAILURE;
            }
        };
        tracing::info!(
            dataset = dataset.as_str(),
            available = available.len(),
            ?unavailable,
            ?priced,
            "Dataset priced"
        );
        match priced {
            Priced::Cost(cost) => total += cost.millionths(),
            Priced::Unresolved => {}
        }
        available_sessions.extend(available);
    }
    let total = Dollars::from_millionths(total);
    tracing::info!(total = %total, ceiling = %arguments.ceiling, symbols = symbols.len(), "Pull priced");
    if total > arguments.ceiling {
        tracing::error!(total = %total, ceiling = %arguments.ceiling, "Pull priced above the ceiling");
        return ExitCode::FAILURE;
    }
    match arguments.command {
        Command::Price => return ExitCode::SUCCESS,
        Command::Pull => {}
    }
    let profile = match Bucket::profile(&aws_config::load_from_env().await) {
        Ok(profile) => profile,
        Err(refusal) => {
            tracing::error!(%refusal, "Profile bucket configuration refused");
            return RefusedToStart.into();
        }
    };
    let mut budget = Budget::new(arguments.ceiling);
    let mut outcomes = Tally::default();
    'pull: for available in &available_sessions {
        for symbol in &symbols {
            let key = ScratchKey::new(available, arguments.schema, symbol.clone());
            let outcome = match landed(&profile, &key).await {
                Ok(true) => Outcome::AlreadyLanded,
                Ok(false) => match databento
                    .rows(available, arguments.schema, symbol, &mut budget)
                    .await
                {
                    Ok(Pulled::Rows { gzipped, cost }) => {
                        match land(&profile, &key, gzipped, cost, Utc::now(), built_commit()).await
                        {
                            Ok(_) => Outcome::Landed,
                            Err(refusal) => {
                                tracing::warn!(?key, %refusal, "Rows not landed");
                                Outcome::Failed
                            }
                        }
                    }
                    Ok(Pulled::Unresolved) => Outcome::Unresolved,
                    Ok(Pulled::Empty) => Outcome::Empty,
                    Err(refusal @ PullRefusal::OverCeiling { .. }) => {
                        tracing::error!(?key, %refusal, "Pull stopped at the ceiling");
                        outcomes.add(Outcome::Failed);
                        break 'pull;
                    }
                    Err(PullRefusal::Fetch(error)) => {
                        tracing::warn!(?key, %error, "Rows not fetched");
                        Outcome::Failed
                    }
                },
                Err(error) => {
                    tracing::warn!(?key, %error, "Landed rows not checked");
                    Outcome::Failed
                }
            };
            outcomes.add(outcome);
        }
        tracing::info!(
            dataset = available.dataset().as_str(),
            session = %available.session(),
            outcomes_so_far = %outcomes,
            remaining = %budget.remaining(),
            "Session pulled"
        );
    }
    tracing::info!(outcomes = %outcomes, remaining = %budget.remaining(), "Pull finished");
    match outcomes.counts().contains_key(&Outcome::Failed) {
        false => ExitCode::SUCCESS,
        true => ExitCode::FAILURE,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn strings(raw: &[&str]) -> Vec<String> {
        raw.iter().map(|text| text.to_string()).collect()
    }

    #[test]
    fn test_arguments_read_into_a_priced_pull() {
        let read = parse_arguments(&strings(&[
            "pull",
            "tbbo",
            "2025-11-13",
            "2026-11-12",
            "250.5",
            "names.txt",
            "XNAS.ITCH",
            "ARCX.PILLAR",
        ]))
        .unwrap();
        let date = |raw: &str| SessionDate::from_date(raw.parse().unwrap());
        assert_eq!(
            read,
            Arguments {
                command: Command::Pull,
                schema: Schema::TradeWithBook,
                range: SessionRange::new(date("2025-11-13"), date("2026-11-12")).unwrap(),
                ceiling: Dollars::from_millionths(250_500_000),
                symbols_path: PathBuf::from("names.txt"),
                datasets: vec![
                    Dataset::new("XNAS.ITCH").unwrap(),
                    Dataset::new("ARCX.PILLAR").unwrap()
                ],
            }
        );
    }

    #[test]
    fn test_arguments_refuse_each_malformed_part() {
        let valid = [
            "price",
            "mbp-10",
            "2026-10-13",
            "2026-11-12",
            "10",
            "names.txt",
            "XNAS.ITCH",
        ];
        assert!(parse_arguments(&strings(&valid)).is_ok());
        assert!(parse_arguments(&strings(&valid[..6])).is_err());
        for (index, bad) in [
            (0, "fetch"),
            (1, "mbp-5"),
            (2, "2026-13-01"),
            (3, "2026-10-12"),
            (4, "-1"),
            (6, "xnas.itch"),
        ] {
            let mut arguments = valid;
            arguments[index] = bad;
            assert!(
                parse_arguments(&strings(&arguments)).is_err(),
                "{bad} was read"
            );
        }
    }

    #[test]
    fn test_a_symbols_file_reads_one_ticker_a_line() {
        let read = parse_symbols("AAPL\n\n  BRK.B \nSPY\n").unwrap();
        let names: Vec<&str> = read.iter().map(Symbol::as_str).collect();
        assert_eq!(names, ["AAPL", "BRK.B", "SPY"]);
        assert!(parse_symbols("AAPL\naapl\n").is_err());
    }
}
