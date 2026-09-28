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

/// Attempts per lookup. A run asks about a thousand filers and one dropped connection would
/// otherwise refuse the whole table, which is what the first run against the live archive did.
const ATTEMPTS: u32 = 3;

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

/// Cheap to clone, and every clone shares one pace: the SEC's limit is on the address, so concurrent
/// lookups and their retries all draw from the same eight requests a second.
#[derive(Clone)]
pub struct EdgarClient {
    http_client: reqwest::Client,
    base_url: String,
    pace: std::sync::Arc<tokio::sync::Mutex<tokio::time::Interval>>,
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
        Ok(Self::with(http_client, SUBMISSIONS_BASE_URL))
    }

    fn with(http_client: reqwest::Client, base_url: &str) -> Self {
        let mut pace = tokio::time::interval(std::time::Duration::from_millis(
            1000 / u64::from(REQUESTS_PER_SECOND),
        ));
        // Delay rather than burst after a stall: catching up on missed ticks is exactly the burst
        // the SEC answers by blocking the address.
        pace.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        Self {
            http_client,
            base_url: base_url.to_string(),
            pace: std::sync::Arc::new(tokio::sync::Mutex::new(pace)),
        }
    }

    #[cfg(test)]
    fn for_tests(base_url: &str) -> Self {
        Self::with(reqwest::Client::new(), base_url)
    }

    /// The code `cik` is registered under, or `None` when EDGAR has no filer or no code for it.
    ///
    /// Both absences are answers: a filer with no SIC code on record is common among funds and
    /// shells, and a `404` is a CIK EDGAR does not know.
    ///
    /// A dropped connection or a server error is retried; a `403` or `429` is not, because both are
    /// the SEC saying to stop and a retry would extend the block.
    pub async fn registration(&self, cik: &Cik) -> Result<Option<Registration>, EdgarError> {
        let mut attempt = 1;
        loop {
            self.pace.lock().await.tick().await;
            let outcome = self.attempt(cik).await;
            let retryable = match &outcome {
                Err(EdgarError::Request(_)) => true,
                Err(EdgarError::Status { status, .. }) => *status >= 500,
                Err(EdgarError::MissingContact) | Ok(_) => false,
            };
            if !retryable || attempt == ATTEMPTS {
                return outcome;
            }
            tokio::time::sleep(std::time::Duration::from_secs(u64::from(attempt) * 2)).await;
            attempt += 1;
        }
    }

    async fn attempt(&self, cik: &Cik) -> Result<Option<Registration>, EdgarError> {
        let response = self
            .http_client
            .get(submissions_url(&self.base_url, cik))
            .send()
            .await?;
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

fn submissions_url(base_url: &str, cik: &Cik) -> String {
    format!("{base_url}/CIK{}.json", cik.as_str())
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
            submissions_url(SUBMISSIONS_BASE_URL, &Cik::new("901832").unwrap()),
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

    /// A server error is transient and a dropped connection costs a whole table, so both retry.
    #[tokio::test]
    async fn test_a_server_error_is_retried_and_the_answer_kept() {
        let mut server = mockito::Server::new_async().await;
        let failing = server
            .mock("GET", "/CIK0000901832.json")
            .with_status(503)
            .expect(1)
            .create_async()
            .await;
        let answering = server
            .mock("GET", "/CIK0000901832.json")
            .with_status(200)
            .with_body(r#"{"sic":"2834","sicDescription":"Pharmaceutical Preparations"}"#)
            .expect(1)
            .create_async()
            .await;

        let registration = EdgarClient::for_tests(&server.url())
            .registration(&Cik::new("901832").unwrap())
            .await
            .expect("the retry must succeed");

        failing.assert_async().await;
        answering.assert_async().await;
        assert_eq!(
            registration.map(|found| found.sic_code.as_str().to_string()),
            Some("2834".to_string())
        );
    }

    /// A 403 is the SEC blocking the address. Retrying would extend the block, so it is asked once.
    #[tokio::test]
    async fn test_a_refusal_is_not_retried() {
        let mut server = mockito::Server::new_async().await;
        let refused = server
            .mock("GET", "/CIK0000901832.json")
            .with_status(403)
            .expect(1)
            .create_async()
            .await;

        let outcome = EdgarClient::for_tests(&server.url())
            .registration(&Cik::new("901832").unwrap())
            .await;

        refused.assert_async().await;
        assert!(
            matches!(outcome, Err(EdgarError::Status { status: 403, .. })),
            "{outcome:?}"
        );
    }
}
