//! Opens, closes, lists and shows accessions in the one Register every profile shares. A person runs this; no host
//! may write the Register.

use std::process::ExitCode;

use chrono::Utc;
use clap::{Parser, Subcommand};

use fund::archive::Archive;
use fund::common::journal::Commit;
use fund::common::market::Dollars;
use fund::common::register::{
    Accession, AccessionNumber, Bid, Closing, Family, Horizon, Interval, Measured, Opening, Sample,
    Status, StudyCost, Universe, Verdict,
};
use fund::common::time::SessionDate;
use fund::register::Register;

#[derive(Parser)]
#[command(about = "The accession Register")]
struct Arguments {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Opens an accession before its study runs.
    Open {
        #[arg(long)]
        family: Family,
        /// `name@version`.
        #[arg(long)]
        universe: Universe,
        /// `<count> sessions`, `<count> <interval> bars`, `<HH:MM>-<HH:MM>` Eastern, or `per <unit>`.
        #[arg(long)]
        horizon: Horizon,
        #[arg(long, allow_hyphen_values = true)]
        hypothesis: String,
        /// `<estimate> [<low>, <high>] <coverage>% <units>`.
        #[arg(long, allow_hyphen_values = true)]
        bid: Interval,
        #[arg(long)]
        supersedes: Option<AccessionNumber>,
        /// Required to supersede an accepted accession.
        #[arg(long, allow_hyphen_values = true)]
        substrate_change: Option<String>,
    },
    /// Closes an open accession with its verdict.
    Close {
        number: AccessionNumber,
        #[arg(long)]
        verdict: Verdict,
        /// The finding in a sentence.
        #[arg(long, allow_hyphen_values = true)]
        statistic: String,
        /// The number the verdict rests on, in the bid's units, or `not-measured`.
        #[arg(long, value_parser = measured, allow_hyphen_values = true)]
        measured: Measured,
        /// The effective sample, `<count> <unit>`, as in `1253 sessions` or `4210 orders`.
        #[arg(long)]
        sample: Sample,
        #[arg(long = "commit")]
        commits: Vec<Commit>,
        #[arg(long, allow_hyphen_values = true)]
        notes: Option<String>,
        #[arg(long)]
        wall_clock_seconds: Option<u64>,
        #[arg(long)]
        bytes_read: Option<u64>,
        #[arg(long)]
        dollars: Option<Dollars>,
    },
    /// One line per accession, or every accession as JSON for a program to read.
    List {
        #[arg(long)]
        json: bool,
    },
    /// One accession as stored.
    Show { number: AccessionNumber },
}

fn measured(raw: &str) -> Result<Measured, String> {
    match raw {
        "not-measured" => Ok(Measured::NotMeasured),
        number => number
            .parse()
            .ok()
            .filter(|value: &f64| value.is_finite())
            .map(Measured::Value)
            .ok_or_else(|| format!("`{raw}` is neither a finite number nor `not-measured`")),
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    let arguments = Arguments::parse();
    let configuration = aws_config::load_from_env().await;
    let register = match Archive::register(&configuration) {
        Ok(archive) => Register::new(archive),
        Err(refusal) => {
            eprintln!("{refusal}");
            return ExitCode::from(2);
        }
    };
    match run(&register, arguments.command).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{error}");
            ExitCode::FAILURE
        }
    }
}

async fn run(register: &Register, command: Command) -> Result<(), Box<dyn std::error::Error>> {
    let today = SessionDate::at(Utc::now());
    match command {
        Command::Open {
            family,
            universe,
            horizon,
            hypothesis,
            bid,
            supersedes,
            substrate_change,
        } => {
            let opening = Opening::new(
                family,
                universe,
                horizon,
                hypothesis,
                Bid::Interval(bid),
                today,
                supersedes,
                substrate_change,
            )
            .map_err(|refusal| refusal.to_string())?;
            let accession = register
                .open(opening)
                .await
                .map_err(|error| error.to_string())?;
            println!("Opened accession {}", accession.number());
        }
        Command::Close {
            number,
            verdict,
            statistic,
            measured,
            sample,
            commits,
            notes,
            wall_clock_seconds,
            bytes_read,
            dollars,
        } => {
            let cost = StudyCost {
                wall_clock_seconds,
                bytes_read,
                dollars,
            };
            let closing = Closing::new(
                verdict, statistic, measured, sample, commits, today, notes, cost,
            )
            .map_err(|refusal| refusal.to_string())?;
            register
                .close(number, closing)
                .await
                .map_err(|error| error.to_string())?;
            println!("Closed accession {number} as {verdict}");
        }
        Command::List { json } => {
            let accessions = register.all().await.map_err(|error| error.to_string())?;
            match json {
                true => println!("{}", serde_json::to_string(&accessions)?),
                false => {
                    for accession in &accessions {
                        println!("{}", line(accession));
                    }
                }
            }
        }
        Command::Show { number } => {
            let accession = register
                .read(number)
                .await
                .map_err(|error| error.to_string())?;
            println!("{}", serde_json::to_string_pretty(&accession)?);
        }
    }
    Ok(())
}

fn line(accession: &Accession) -> String {
    let status = match accession.status() {
        Status::Open => "open".to_string(),
        Status::Closed(closing) => closing.verdict().to_string(),
    };
    let opening = accession.opening();
    format!(
        "{}  {}  {:<18}  {:<24}  {}",
        accession.number(),
        opening.opened(),
        status,
        opening.family().to_string(),
        opening.hypothesis()
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(arguments: &[&str]) -> Result<Command, clap::Error> {
        Arguments::try_parse_from([&["register"], arguments].concat()).map(|parsed| parsed.command)
    }

    #[test]
    fn test_open_reads_every_typed_field() {
        let command = parse(&[
            "open",
            "--family",
            "overnight",
            "--universe",
            "liquid-common@2",
            "--horizon",
            "1 sessions",
            "--hypothesis",
            "close-to-open returns persist",
            "--bid",
            "4 [0, 9] 80% net-bp",
            "--supersedes",
            "5",
        ])
        .unwrap();
        match command {
            Command::Open {
                family,
                universe,
                bid,
                supersedes,
                ..
            } => {
                assert_eq!(family, Family::Overnight);
                assert_eq!(universe, "liquid-common@2".parse().unwrap());
                assert_eq!((bid.estimate(), bid.coverage_percent()), (4.0, 80));
                assert_eq!(supersedes, AccessionNumber::new(5));
            }
            Command::Close { .. } | Command::List { .. } | Command::Show { .. } => {
                panic!("parsed as another command")
            }
        }
    }

    #[test]
    fn test_a_malformed_field_is_refused_before_anything_is_written() {
        let refused = parse(&[
            "open",
            "--family",
            "overnight",
            "--universe",
            "liquid-common",
            "--horizon",
            "1 sessions",
            "--hypothesis",
            "h",
            "--bid",
            "4 [0, 9] 80% net-bp",
        ]);
        assert!(refused.is_err());
        assert!(
            parse(&[
                "close",
                "0",
                "--verdict",
                "refute",
                "--statistic",
                "s",
                "--measured",
                "1",
                "--sample",
                "1 sessions"
            ])
            .is_err()
        );
        assert!(
            parse(&[
                "close",
                "7",
                "--verdict",
                "maybe",
                "--statistic",
                "s",
                "--measured",
                "1",
                "--sample",
                "1 sessions"
            ])
            .is_err()
        );
    }

    #[test]
    fn test_a_leading_hyphen_is_a_value_not_a_flag() {
        let command = parse(&[
            "close",
            "7",
            "--verdict",
            "refute",
            "--statistic",
            "-5.7bp net",
            "--measured",
            "-5.7",
            "--sample",
            "1253 sessions",
        ])
        .unwrap();
        match command {
            Command::Close {
                statistic,
                measured,
                ..
            } => assert_eq!(
                (statistic.as_str(), measured),
                ("-5.7bp net", Measured::Value(-5.7))
            ),
            Command::Open { .. } | Command::List { .. } | Command::Show { .. } => {
                panic!("parsed as another command")
            }
        }
    }

    #[test]
    fn test_a_measurement_is_a_finite_number_or_named_as_absent() {
        assert_eq!(measured("-5.7"), Ok(Measured::Value(-5.7)));
        assert_eq!(measured("not-measured"), Ok(Measured::NotMeasured));
        for refused in ["NaN", "inf", "", "five"] {
            assert!(measured(refused).is_err(), "{refused}");
        }
    }
}
