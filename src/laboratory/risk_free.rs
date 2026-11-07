//! Loads the daily risk-free rate a study's Sharpe takes off, keeping the response it read among the run's notes so
//! the rate can be read again exactly as it was.

use std::collections::BTreeMap;
use std::io;
use std::path::Path;

use chrono::NaiveDate;

use crate::common::laboratory::risk_free::{AnnualYield, daily_rates};
use crate::common::laboratory::series::{Series, SeriesRefusal};
use crate::common::time::SessionDate;
use crate::ingest::Fetched;
use crate::ingest::massive::Massive;
use crate::ingest::retry::FetchError;
use crate::laboratory::Study;
use crate::laboratory::source::digest_of;

/// The prefix of the note the response is kept under, ahead of its digest, so a second load in one run keeps both.
const NOTE_PREFIX: &str = "risk_free_rate_";

/// How far before the first session the yields are asked from, so it has a published yield before it.
const LEAD_DAYS: i64 = 14;

/// The daily rates and the settings an experiment journals beside them.
#[derive(Debug, Clone, PartialEq)]
pub struct RiskFree {
    rates: Series,
    settings: Vec<(String, String)>,
}

impl RiskFree {
    pub fn rates(&self) -> &Series {
        &self.rates
    }

    /// The source and the digest of the response, which also names its note, for an experiment's parameters.
    pub fn settings(&self) -> &[(String, String)] {
        &self.settings
    }
}

#[derive(Debug, thiserror::Error)]
pub enum RiskFreeError {
    #[error("asked for the rate of no sessions")]
    NoSessions,
    #[error("{0}")]
    Fetch(FetchError),
    #[error("keeping the response among the notes failed: {0}")]
    Note(io::Error),
    #[error("{0}")]
    Series(SeriesRefusal),
}

/// The 3-month Treasury rate for each of `sessions`, from Massive, its response kept among `study`'s notes.
pub async fn risk_free(
    study: &Study,
    massive: &Massive,
    sessions: &[SessionDate],
) -> Result<RiskFree, RiskFreeError> {
    let first = sessions.iter().min().ok_or(RiskFreeError::NoSessions)?;
    let since = first.plus_calendar_days(-LEAD_DAYS).date();
    kept(
        &study.notes(),
        massive.treasury_yields(since).await,
        sessions,
    )
}

/// Keeps every body `fetched` received in `notes` under its digest, even when reading it failed, then reads each
/// session's rate; the digest returned names exactly the bytes kept.
fn kept(
    notes: &Path,
    fetched: Fetched<BTreeMap<NaiveDate, AnnualYield>>,
    sessions: &[SessionDate],
) -> Result<RiskFree, RiskFreeError> {
    let (yields, bodies) = fetched.into_parts();
    let body = bodies.concat();
    let digest = digest_of(&body);
    if !body.is_empty() {
        std::fs::create_dir_all(notes).map_err(RiskFreeError::Note)?;
        let note = notes.join(format!("{NOTE_PREFIX}{}.json", digest.as_str()));
        std::fs::write(note, &body).map_err(RiskFreeError::Note)?;
    }
    let yields = yields.map_err(RiskFreeError::Fetch)?;
    let rates = daily_rates(&yields, sessions.iter().copied()).map_err(RiskFreeError::Series)?;
    Ok(RiskFree {
        rates,
        settings: vec![
            (
                "risk_free.source".to_string(),
                "massive_treasury_yield_3_month".to_string(),
            ),
            ("risk_free.digest".to_string(), digest.as_str().to_string()),
        ],
    })
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use super::*;

    fn session(day: u32) -> SessionDate {
        SessionDate::from_date(NaiveDate::from_ymd_opt(2026, 11, day).unwrap())
    }

    fn temporary_directory() -> std::path::PathBuf {
        std::env::temp_dir().join(format!("fund-risk-free-{}", Uuid::new_v4()))
    }

    fn yields() -> BTreeMap<NaiveDate, AnnualYield> {
        BTreeMap::from([(session(5).date(), AnnualYield::new(5.04).unwrap())])
    }

    fn kept_notes(notes: &Path) -> Vec<(String, Vec<u8>)> {
        let mut kept: Vec<(String, Vec<u8>)> = std::fs::read_dir(notes)
            .unwrap()
            .map(|entry| {
                let path = entry.unwrap().path();
                let name = path.file_name().unwrap().to_string_lossy().to_string();
                (name, std::fs::read(path).unwrap())
            })
            .collect();
        kept.sort();
        kept
    }

    #[test]
    fn test_the_rates_come_back_with_the_digest_of_the_bytes_kept() {
        let directory = temporary_directory();
        let body = b"{\"results\":[]}".to_vec();
        let read = kept(
            &directory,
            Fetched::new(Ok(yields()), vec![body.clone()]),
            &[session(6)],
        )
        .unwrap();
        let rate = read.rates().readings()[&session(6)].unwrap();
        assert!((rate - 0.0002).abs() < 1e-15);
        let digest = digest_of(&body).as_str().to_string();
        assert_eq!(
            read.settings(),
            [
                (
                    "risk_free.source".to_string(),
                    "massive_treasury_yield_3_month".to_string()
                ),
                ("risk_free.digest".to_string(), digest.clone()),
            ]
        );
        assert_eq!(
            kept_notes(&directory),
            [(format!("risk_free_rate_{digest}.json"), body)]
        );
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn test_a_response_that_does_not_read_is_kept_before_the_refusal() {
        let directory = temporary_directory();
        let body = b"not the yields".to_vec();
        let refused = Fetched::new(
            Err(FetchError::Malformed {
                reason: "unreadable".to_string(),
            }),
            vec![body.clone()],
        );
        assert!(matches!(
            kept(&directory, refused, &[session(6)]),
            Err(RiskFreeError::Fetch(_))
        ));
        assert_eq!(
            kept_notes(&directory),
            [(
                format!("risk_free_rate_{}.json", digest_of(&body).as_str()),
                body
            )]
        );
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn test_two_reads_in_one_run_keep_both_responses() {
        let directory = temporary_directory();
        let (first, second) = (b"first".to_vec(), b"second".to_vec());
        let one = kept(
            &directory,
            Fetched::new(Ok(yields()), vec![first.clone()]),
            &[session(6)],
        )
        .unwrap();
        let two = kept(
            &directory,
            Fetched::new(Ok(yields()), vec![second.clone()]),
            &[session(6)],
        )
        .unwrap();
        assert_ne!(one.settings(), two.settings());
        let mut expected = vec![
            (
                format!("risk_free_rate_{}.json", digest_of(&first).as_str()),
                first,
            ),
            (
                format!("risk_free_rate_{}.json", digest_of(&second).as_str()),
                second,
            ),
        ];
        expected.sort();
        assert_eq!(kept_notes(&directory), expected);
        std::fs::remove_dir_all(directory).unwrap();
    }
}
