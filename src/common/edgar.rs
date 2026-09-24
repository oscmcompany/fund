//! SEC EDGAR, read for one thing: the industry code a filer registered under.
//!
//! Keyed by CIK rather than ticker, because a ticker can pass to a different company and a CIK cannot.

use serde::Deserialize;

use crate::common::types::{Cik, SicCode};

/// Where EDGAR serves one filer's submissions record.
const SUBMISSIONS_BASE_URL: &str = "https://data.sec.gov/submissions";

/// Requests a second, kept under the SEC's published ceiling of ten.
///
/// The ceiling is enforced by blocking the caller's address for ten minutes, not by slowing it, so
/// the margin is deliberate: a burst that crosses it costs far more than the time it saved.
pub const REQUESTS_PER_SECOND: u32 = 8;

#[derive(Debug, thiserror::Error)]
pub enum EdgarError {
    /// The SEC refuses requests whose User-Agent carries no contact, so there is no anonymous
    /// fallback to try.
    #[error("SEC_EDGAR_CONTACT_EMAIL must be set; EDGAR refuses requests without a contact")]
    MissingContact,
    #[error("EDGAR request failed: {0}")]
    Request(#[from] reqwest::Error),
    #[error("EDGAR answered {status} for CIK {cik}")]
    Status { cik: String, status: u16 },
}

/// What EDGAR registered one filer under.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Registration {
    pub sic_code: SicCode,
    pub sic_description: Option<String>,
}

#[derive(Deserialize)]
struct Submissions {
    sic: Option<String>,
    #[serde(rename = "sicDescription")]
    sic_description: Option<String>,
}

#[derive(Clone)]
pub struct EdgarClient {
    http_client: reqwest::Client,
}

impl EdgarClient {
    /// Builds a client whose User-Agent names the contact the SEC requires.
    ///
    /// The contact is a secret in `secretspec.toml` rather than a literal, so each profile names its
    /// own and none is committed.
    pub fn from_env() -> Result<Self, EdgarError> {
        let contact = std::env::var("SEC_EDGAR_CONTACT_EMAIL")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .ok_or(EdgarError::MissingContact)?;
        let http_client = reqwest::Client::builder()
            .user_agent(format!("oscmcompany-fund {contact}"))
            .connect_timeout(std::time::Duration::from_secs(10))
            .timeout(std::time::Duration::from_secs(30))
            .build()?;
        Ok(Self { http_client })
    }

    /// The code `cik` is registered under, or `None` when EDGAR has no filer or no code for it.
    ///
    /// Both absences are answers: a filer with no SIC code on record is common among funds and
    /// shells, and a `404` is a CIK EDGAR does not know.
    pub async fn registration(&self, cik: &Cik) -> Result<Option<Registration>, EdgarError> {
        let response = self.http_client.get(submissions_url(cik)).send().await?;
        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if !response.status().is_success() {
            return Err(EdgarError::Status {
                cik: cik.as_str().to_string(),
                status: response.status().as_u16(),
            });
        }
        Ok(registration_of(response.json::<Submissions>().await?))
    }
}

fn submissions_url(cik: &Cik) -> String {
    format!("{SUBMISSIONS_BASE_URL}/CIK{}.json", cik.as_str())
}

/// A code EDGAR sends in a shape `SicCode` will not admit is no code, for the reason the Massive
/// route gives: the filer is still usable without an industry.
fn registration_of(submissions: Submissions) -> Option<Registration> {
    let sic_code = submissions.sic.as_deref().and_then(SicCode::new)?;
    Some(Registration {
        sic_code,
        sic_description: submissions
            .sic_description
            .filter(|description| !description.is_empty()),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The key is zero-padded in the path, which is the only spelling EDGAR serves.
    #[test]
    fn test_the_url_pads_the_cik_to_ten_digits() {
        assert_eq!(
            submissions_url(&Cik::new("901832").unwrap()),
            "https://data.sec.gov/submissions/CIK0000901832.json"
        );
    }

    /// Shaped on the live record for CIK 0000901832 read 2026-09-24, trimmed to the fields read.
    #[test]
    fn test_a_registration_reads_the_code_and_an_empty_one_is_absent() {
        let astrazeneca: Submissions = serde_json::from_str(
            r#"{"cik":"901832","name":"ASTRAZENECA PLC","sic":"2834","sicDescription":"Pharmaceutical Preparations"}"#,
        )
        .unwrap();
        let shell: Submissions =
            serde_json::from_str(r#"{"cik":"1","sic":"","sicDescription":""}"#).unwrap();

        assert_eq!(
            registration_of(astrazeneca),
            Some(Registration {
                sic_code: SicCode::new("2834").unwrap(),
                sic_description: Some("Pharmaceutical Preparations".to_string()),
            })
        );
        assert_eq!(registration_of(shell), None);
    }
}
