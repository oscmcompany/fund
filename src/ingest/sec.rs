//! SEC EDGAR: a filer's annual reports and the industry code each report's header assigned it, asked one request at
//! a time under the SEC's fair-access rate.

use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::Deserialize;

use super::retry::{send, with_retries};
use super::{FetchError, Fetched, VariableRefusal, variable};
use crate::common::market::industry::AccessionNumber;
use crate::common::market::security_details::{CentralIndexKey, IndustryCode};

/// The wait before every request, keeping a sequential client at eight a second under the SEC's ten, since a burst over
/// it blocks the address for ten minutes rather than slowing it.
const PACE: Duration = Duration::from_millis(125);

/// The annual reports whose headers are read: domestic, foreign private and Canadian issuers', amendments left out.
const ANNUAL_FORMS: [&str; 3] = ["10-K", "20-F", "40-F"];

pub struct Sec {
    http_client: reqwest::Client,
    user_agent: String,
}

/// One annual report a filer's submissions list.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AnnualReport {
    accession: AccessionNumber,
    accepted_at: DateTime<Utc>,
}

impl AnnualReport {
    pub fn new(accession: AccessionNumber, accepted_at: DateTime<Utc>) -> Self {
        Self {
            accession,
            accepted_at,
        }
    }

    pub fn accession(&self) -> &AccessionNumber {
        &self.accession
    }

    pub fn accepted_at(&self) -> DateTime<Utc> {
        self.accepted_at
    }
}

#[derive(Deserialize)]
struct Submissions {
    cik: String,
    filings: Filings,
}

#[derive(Deserialize)]
struct Filings {
    recent: Recent,
}

/// The submissions' recent filings, one array per field, read by index.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Recent {
    accession_number: Vec<String>,
    acceptance_date_time: Vec<String>,
    form: Vec<String>,
}

impl Sec {
    /// Reads `SEC_EDGAR_CONTACT_EMAIL`, which the SEC requires in every request's user agent.
    pub fn from_environment(http_client: reqwest::Client) -> Result<Self, VariableRefusal> {
        Ok(Self {
            http_client,
            user_agent: format!("oscmcompany {}", variable("SEC_EDGAR_CONTACT_EMAIL")?),
        })
    }

    async fn get(&self, url: &str) -> Result<Vec<u8>, FetchError> {
        with_retries(|| async {
            tokio::time::sleep(PACE).await;
            send(
                self.http_client
                    .get(url)
                    .header(reqwest::header::USER_AGENT, &self.user_agent),
            )
            .await
        })
        .await
    }

    /// The filer's annual reports accepted at or after `since`, from the submissions' recent filings.
    pub async fn annual_reports(
        &self,
        filer: CentralIndexKey,
        since: DateTime<Utc>,
    ) -> Fetched<Vec<AnnualReport>> {
        let url = format!(
            "https://data.sec.gov/submissions/CIK{:010}.json",
            filer.value()
        );
        match self.get(&url).await {
            Ok(body) => Fetched::new(parse_annual_reports(&body, filer, since), vec![body]),
            Err(error) => Fetched::new(Err(error), Vec::new()),
        }
    }

    /// The industry code `report`'s header assigned `filer`, or `None` where it assigned none; the header is kept as
    /// a JSON line beside its accession number, since it spans lines, and one that is not UTF-8 is refused unkept.
    pub async fn assigned_code(
        &self,
        filer: CentralIndexKey,
        report: &AnnualReport,
    ) -> Fetched<Option<IndustryCode>> {
        let accession = report.accession.as_str();
        let url = format!(
            "https://www.sec.gov/Archives/edgar/data/{}/{}/{accession}.hdr.sgml",
            filer.value(),
            accession.replace('-', ""),
        );
        match self.get(&url).await.and_then(|body| {
            String::from_utf8(body).map_err(|error| malformed(format!("{accession}: {error}")))
        }) {
            Ok(header) => {
                let line = serde_json::json!({ "accession_number": accession, "header": header });
                Fetched::new(
                    parse_assigned_code(&header, filer),
                    vec![line.to_string().into_bytes()],
                )
            }
            Err(error) => Fetched::new(Err(error), Vec::new()),
        }
    }
}

fn malformed(reason: impl std::fmt::Display) -> FetchError {
    FetchError::Malformed {
        reason: reason.to_string(),
    }
}

/// The annual reports a submissions body lists for `filer` accepted at or after `since`.
fn parse_annual_reports(
    body: &[u8],
    filer: CentralIndexKey,
    since: DateTime<Utc>,
) -> Result<Vec<AnnualReport>, FetchError> {
    let submissions: Submissions = serde_json::from_slice(body).map_err(malformed)?;
    if submissions.cik.parse::<u64>().ok() != Some(filer.value()) {
        return Err(malformed(format!(
            "submissions for `{}` answered {}",
            submissions.cik,
            filer.value()
        )));
    }
    let recent = submissions.filings.recent;
    let rows = recent.form.len();
    if recent.accession_number.len() != rows || recent.acceptance_date_time.len() != rows {
        return Err(malformed("the recent filings' columns differ in length"));
    }
    let mut reports = Vec::new();
    for ((form, accession), accepted_at) in recent
        .form
        .iter()
        .zip(&recent.accession_number)
        .zip(&recent.acceptance_date_time)
    {
        if !ANNUAL_FORMS.contains(&form.as_str()) {
            continue;
        }
        let accepted_at = accepted_at
            .parse::<DateTime<Utc>>()
            .map_err(|error| malformed(format!("`{accepted_at}`: {error}")))?;
        if accepted_at >= since {
            reports.push(AnnualReport {
                accession: AccessionNumber::new(accession).map_err(malformed)?,
                accepted_at,
            });
        }
    }
    Ok(reports)
}

/// The code a header's company data for `filer` assigns, `None` where it assigns none or `0000`, which the SEC writes
/// for a filer it has not classified.
fn parse_assigned_code(
    header: &str,
    filer: CentralIndexKey,
) -> Result<Option<IndustryCode>, FetchError> {
    let mut ours = false;
    for line in header.lines() {
        match line.split_once('>') {
            Some(("<CIK", value)) => ours = value.trim().parse::<u64>().ok() == Some(filer.value()),
            Some(("<ASSIGNED-SIC", value)) if ours => {
                let code = IndustryCode::new(value.trim()).map_err(malformed)?;
                return Ok((code.code() != 0).then_some(code));
            }
            Some(("</COMPANY-DATA", _)) => ours = false,
            Some(_) | None => {}
        }
    }
    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn filer() -> CentralIndexKey {
        CentralIndexKey::new(4_321).unwrap()
    }

    fn since() -> DateTime<Utc> {
        "2022-01-01T00:00:00Z".parse().unwrap()
    }

    const SUBMISSIONS: &str = r#"{"cik":"0000004321","name":"EXAMPLE CO","filings":{"recent":{
        "accessionNumber":["0000004321-25-000003","0000004321-25-000002","0000004321-24-000001","0000004321-21-000009","0000004321-24-000004"],
        "acceptanceDateTime":["2025-03-01T21:00:00.000Z","2025-02-18T21:06:30.000Z","2024-02-20T21:32:15.000Z","2021-02-16T11:08:39.000Z","2024-03-01T10:00:00.000Z"],
        "form":["6-K","20-F","40-F","20-F","20-F/A"]},"files":[]}}"#;

    #[test]
    fn test_annual_reports_since_the_cutoff_are_read_and_others_left() {
        let reports = parse_annual_reports(SUBMISSIONS.as_bytes(), filer(), since()).unwrap();
        let read: Vec<(&str, String)> = reports
            .iter()
            .map(|report| {
                (
                    report.accession().as_str(),
                    report.accepted_at().to_rfc3339(),
                )
            })
            .collect();
        assert_eq!(
            read,
            [
                (
                    "0000004321-25-000002",
                    "2025-02-18T21:06:30+00:00".to_string()
                ),
                (
                    "0000004321-24-000001",
                    "2024-02-20T21:32:15+00:00".to_string()
                ),
            ]
        );
    }

    #[test]
    fn test_submissions_for_another_filer_or_ragged_columns_are_refused() {
        let other = CentralIndexKey::new(1_234).unwrap();
        assert!(matches!(
            parse_annual_reports(SUBMISSIONS.as_bytes(), other, since()),
            Err(FetchError::Malformed { .. })
        ));
        let ragged = SUBMISSIONS.replace(r#""form":["6-K","#, r#""form":["#);
        assert!(matches!(
            parse_annual_reports(ragged.as_bytes(), filer(), since()),
            Err(FetchError::Malformed { .. })
        ));
    }

    fn header(companies: &[(&str, &str)]) -> String {
        let mut text =
            "<SEC-HEADER>0000004321-25-000002.hdr.sgml : 20250218\n<TYPE>20-F\n".to_string();
        for (cik, code) in companies {
            text.push_str(&format!(
                "<FILER>\n<COMPANY-DATA>\n<CONFORMED-NAME>EXAMPLE CO\n<CIK>{cik}\n<ASSIGNED-SIC>{code}\n</COMPANY-DATA>\n</FILER>\n"
            ));
        }
        text
    }

    #[test]
    fn test_the_header_code_is_the_filers_own() {
        let code = |text: &str| {
            parse_assigned_code(text, filer()).map(|code| code.map(IndustryCode::code))
        };
        assert_eq!(code(&header(&[("0000004321", "2834")])), Ok(Some(2834)));
        assert_eq!(
            code(&header(&[("0000009999", "6029"), ("0000004321", "4832")])),
            Ok(Some(4832))
        );
        assert_eq!(code(&header(&[("0000009999", "6029")])), Ok(None));
        assert_eq!(code(&header(&[("0000004321", "0000")])), Ok(None));
        assert!(matches!(
            code(&header(&[("0000004321", "28")])),
            Err(FetchError::Malformed { .. })
        ));
    }

    /// Reads one foreign private issuer's annual reports and the code each header assigned it.
    #[tokio::test]
    #[ignore = "fetches from EDGAR and needs SEC_EDGAR_CONTACT_EMAIL"]
    async fn live_a_foreign_issuer_reads_its_filed_code() {
        let sec = Sec::from_environment(reqwest::Client::new()).unwrap();
        let filer = CentralIndexKey::new(901_832).unwrap();
        let (reports, bodies) = sec.annual_reports(filer, since()).await.into_parts();
        let reports = reports.unwrap();
        assert_eq!(bodies.len(), 1);
        assert!(reports.len() >= 4, "{reports:?}");
        let (code, bodies) = sec.assigned_code(filer, &reports[0]).await.into_parts();
        assert_eq!(code.unwrap().map(IndustryCode::code), Some(2834));
        assert!(!bodies[0].contains(&b'\n'));
    }
}
