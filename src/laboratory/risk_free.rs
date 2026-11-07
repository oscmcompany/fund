//! Loads the daily risk-free rate a study's Sharpe takes off, keeping the response it read among the run's notes so
//! the rate can be read again exactly as it was.

use std::io;

use crate::common::laboratory::risk_free::daily_rates;
use crate::common::laboratory::series::{Series, SeriesRefusal};
use crate::common::time::SessionDate;
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

    /// The source, the instant it was read and the digest of the response, which also names its note, for an
    /// experiment's parameters.
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
    let (yields, bodies) = massive.treasury_yields(since).await.into_parts();
    let body = bodies.concat();
    let digest = digest_of(&body);
    if !body.is_empty() {
        let notes = study.notes();
        std::fs::create_dir_all(&notes).map_err(RiskFreeError::Note)?;
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
