//! Opens, closes and lists accessions in the Register, which lives in this profile's records bucket.
//!
//! A person opens and closes; nothing here runs on a schedule, and nothing opens an accession unasked.

use chrono::Utc;
use clap::{Args, Parser, Subcommand, ValueEnum};

use fund::common::types::SessionDate;
use fund::laboratory::register::{
    self, Accession, AccessionNumber, Bid, Closing, Opening, Status, StudyCost, Verdict,
};

#[derive(Debug, Parser)]
#[command(
    name = "register",
    about = "The record of every test against the substrate"
)]
struct Arguments {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Open an accession: the hypothesis, family, universe, horizon and bid, before the study runs.
    Open(OpenArguments),
    /// Close an open accession with its verdict.
    Close(CloseArguments),
    /// Print every accession, one line each.
    List,
    /// Print one accession in full.
    Show { number: u32 },
    /// Copy every accession into `register/` for reading. The bucket stays the record.
    Pull,
}

#[derive(Debug, Args)]
struct OpenArguments {
    #[arg(long)]
    family: String,
    /// Named and versioned, never a bare threshold.
    #[arg(long)]
    universe: String,
    #[arg(long)]
    horizon: String,
    #[arg(long)]
    hypothesis: String,
    /// The expected effect and interval in the verdict's units, e.g. "+4bp net, 80% [0, +9]".
    #[arg(long, required_unless_present = "unrecorded_bid")]
    bid: Option<String>,
    /// Only for the seed: tests run before bids existed, which are never reconstructed.
    #[arg(long, conflicts_with = "bid")]
    unrecorded_bid: bool,
    /// The closed accession this one re-measures.
    #[arg(long)]
    supersedes: Option<u32>,
    /// What changed underneath, which a successor to an accepted accession must say.
    #[arg(long)]
    substrate_change: Option<String>,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum VerdictArgument {
    Accept,
    Refute,
    Inconclusive,
}

#[derive(Debug, Args)]
struct CloseArguments {
    number: u32,
    #[arg(long, value_enum)]
    verdict: VerdictArgument,
    #[arg(long)]
    statistic: String,
    #[arg(long)]
    sessions: usize,
    #[arg(long = "commit")]
    commits: Vec<String>,
    /// For an inconclusive verdict, the one change its successor makes.
    #[arg(long)]
    notes: Option<String>,
    #[arg(long)]
    wall_clock_seconds: Option<u64>,
    #[arg(long)]
    bytes_read: Option<u64>,
    #[arg(long)]
    dollars: Option<f64>,
}

#[tokio::main]
async fn main() {
    fund::common::crypto::install_default_crypto_provider();
    let arguments = Arguments::parse();
    let code = match run(arguments.command).await {
        Ok(()) => 0,
        Err(error) => {
            eprintln!("register: {error}");
            1
        }
    };
    std::process::exit(code);
}

fn number(value: u32) -> Result<AccessionNumber, Box<dyn std::error::Error>> {
    AccessionNumber::new(value).ok_or_else(|| "accession numbers start at 1".into())
}

async fn run(command: Command) -> Result<(), Box<dyn std::error::Error>> {
    let bucket = std::env::var("AWS_S3_RECORDS_BUCKET_NAME")
        .map_err(|_| "AWS_S3_RECORDS_BUCKET_NAME must be set")?;
    let s3_client = fund::common::aws::s3_client().await;
    let today = SessionDate::at(Utc::now());

    match command {
        Command::Open(open) => {
            let supersedes = open.supersedes.map(number).transpose()?;
            let opened = register::open(
                &s3_client,
                &bucket,
                Opening {
                    family: open.family,
                    universe: open.universe,
                    horizon: open.horizon,
                    hypothesis: open.hypothesis,
                    bid: match open.bid {
                        Some(bid) => Bid::Recorded(bid),
                        None => Bid::Unrecorded,
                    },
                    opened: today,
                    supersedes,
                    substrate_change: open.substrate_change,
                },
            )
            .await?;
            // The predecessor is pointed at its successor after the successor exists, so a failure
            // here leaves an open accession naming what it supersedes rather than a dangling link.
            if let Some(predecessor) = supersedes {
                let (previous, etag) = register::read(&s3_client, &bucket, predecessor).await?;
                let pointed = previous.superseded_by(&opened)?;
                register::write(&s3_client, &bucket, &pointed, Some(&etag)).await?;
            }
            println!("opened {}", opened.number);
        }
        Command::Close(close) => {
            let (accession, etag) =
                register::read(&s3_client, &bucket, number(close.number)?).await?;
            let closed = accession.close(Closing {
                verdict: match close.verdict {
                    VerdictArgument::Accept => Verdict::Accept,
                    VerdictArgument::Refute => Verdict::Refute,
                    VerdictArgument::Inconclusive => Verdict::Inconclusive,
                },
                statistic: close.statistic,
                sessions: close.sessions,
                commits: close.commits,
                closed: today,
                notes: close.notes,
                cost: StudyCost {
                    wall_clock_seconds: close.wall_clock_seconds,
                    bytes_read: close.bytes_read,
                    dollars: close.dollars,
                },
            })?;
            register::write(&s3_client, &bucket, &closed, Some(&etag)).await?;
            println!("closed {}", closed.number);
        }
        Command::List => {
            for number in register::numbers(&s3_client, &bucket).await? {
                let (accession, _) = register::read(&s3_client, &bucket, number).await?;
                println!("{}", line(&accession));
            }
        }
        Command::Show { number: value } => {
            let (accession, _) = register::read(&s3_client, &bucket, number(value)?).await?;
            println!("{}", serde_json::to_string_pretty(&accession)?);
        }
        Command::Pull => {
            let directory = std::path::Path::new("register");
            std::fs::create_dir_all(directory)?;
            let numbers = register::numbers(&s3_client, &bucket).await?;
            for number in &numbers {
                let (accession, _) = register::read(&s3_client, &bucket, *number).await?;
                std::fs::write(
                    directory.join(format!("{number}.json")),
                    serde_json::to_vec_pretty(&accession)?,
                )?;
            }
            println!("pulled {} accessions into register/", numbers.len());
        }
    }
    Ok(())
}

/// One accession on one line: number, state, family, and the hypothesis.
fn line(accession: &Accession) -> String {
    let state = match &accession.status {
        Status::Open => "open".to_string(),
        Status::Closed(closing) => format!("{:?}", closing.verdict).to_lowercase(),
    };
    let superseded = accession
        .superseded_by
        .map(|successor| format!(" -> {successor}"))
        .unwrap_or_default();
    format!(
        "{}  {:<12} {:<20} {}{superseded}",
        accession.number, state, accession.opening.family, accession.opening.hypothesis
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use fund::laboratory::register::RegisterError;

    #[test]
    fn test_a_register_line_names_the_state_and_the_successor() {
        let opening = Opening {
            family: "overnight".to_string(),
            universe: "liquidity-floor-2026".to_string(),
            horizon: "1 session".to_string(),
            hypothesis: "overnight returns survive costs".to_string(),
            bid: Bid::Unrecorded,
            opened: SessionDate::from_date(chrono::NaiveDate::from_ymd_opt(2026, 8, 21).unwrap()),
            supersedes: None,
            substrate_change: None,
        };
        let mut accession = Accession::open(AccessionNumber::new(5).unwrap(), opening);
        assert_eq!(
            line(&accession),
            "0005  open         overnight            overnight returns survive costs"
        );
        accession.superseded_by = AccessionNumber::new(21);
        assert!(line(&accession).ends_with("-> 0021"));
    }

    #[test]
    fn test_an_opening_needs_exactly_one_of_a_bid_or_its_absence() {
        let base = [
            "register",
            "open",
            "--family",
            "f",
            "--universe",
            "u",
            "--horizon",
            "h",
            "--hypothesis",
            "x",
        ];
        assert!(Arguments::try_parse_from(base).is_err());
        assert!(Arguments::try_parse_from(base.iter().chain(&["--bid", "+4bp"])).is_ok());
        assert!(Arguments::try_parse_from(base.iter().chain(&["--unrecorded-bid"])).is_ok());
        assert!(Arguments::try_parse_from(base.iter().chain(&[
            "--bid",
            "+4bp",
            "--unrecorded-bid"
        ]))
        .is_err());
    }

    #[test]
    fn test_a_missing_accession_is_named() {
        let error = RegisterError::Missing(AccessionNumber::new(9).unwrap());
        assert_eq!(error.to_string(), "accession 0009 does not exist");
    }
}
