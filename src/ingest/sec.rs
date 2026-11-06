//! SEC EDGAR: a filer's annual reports and the industry code each report's header assigned it, asked one request at
//! a time under the SEC's fair-access rate.

use std::time::Duration;

use chrono::{DateTime, NaiveDate, Utc};
use serde::Deserialize;

use super::retry::{send, with_retries};
use super::{FetchError, Fetched, VariableRefusal, variable};
use crate::common::market::industry::AccessionNumber;
use crate::common::market::security_details::{CentralIndexKey, IndustryCode};
use crate::common::time::SessionDate;

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
    recent: Columns,
    /// Older filings EDGAR has moved out of `recent`, one file per entry.
    #[serde(default)]
    files: Vec<HistoryFile>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct HistoryFile {
    name: String,
    filing_to: NaiveDate,
}

/// Filings one array per field, read by index: the submissions' recent filings, or a whole history file.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Columns {
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

    /// The filer's annual reports accepted at or after `since`, from the submissions' recent filings and every history
    /// file whose filings reach `since`.
    pub async fn annual_reports(
        &self,
        filer: CentralIndexKey,
        since: DateTime<Utc>,
    ) -> Fetched<Vec<AnnualReport>> {
        let url = format!(
            "https://data.sec.gov/submissions/CIK{:010}.json",
            filer.value()
        );
        let body = match self.get(&url).await {
            Ok(body) => body,
            Err(error) => return Fetched::new(Err(error), Vec::new()),
        };
        let (mut reports, history) = match parse_annual_reports(&body, filer, since) {
            Ok(read) => read,
            Err(error) => return Fetched::new(Err(error), vec![body]),
        };
        let mut bodies = vec![body];
        for name in history {
            let read = match self
                .get(&format!("https://data.sec.gov/submissions/{name}"))
                .await
            {
                // A missing history file is not a filer the SEC does not know, which is what a 404 here reads as.
                Err(FetchError::Refused { status: 404, .. }) => {
                    Err(malformed(format!("history file `{name}` is missing")))
                }
                fetched => fetched,
            }
            .and_then(|body| {
                let read = parse_history(&body, since);
                bodies.push(body);
                read
            });
            match read {
                Ok(older) => reports.extend(older),
                Err(error) => return Fetched::new(Err(error), bodies),
            }
        }
        Fetched::new(Ok(reports), bodies)
    }

    /// The industry code `report`'s header assigned `filer`, or `None` where it assigned none or `0000`; the header is
    /// kept as a JSON line beside its accession number, since it spans lines, and one that is not UTF-8 is refused
    /// unkept, as is one that does not name `filer`.
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

/// The annual reports a submissions body lists for `filer` accepted at or after `since`, beside the history files that
/// reach `since`; a history file named outside the filer's own is refused.
fn parse_annual_reports(
    body: &[u8],
    filer: CentralIndexKey,
    since: DateTime<Utc>,
) -> Result<(Vec<AnnualReport>, Vec<String>), FetchError> {
    let submissions: Submissions = serde_json::from_slice(body).map_err(malformed)?;
    if submissions.cik.parse::<u64>().ok() != Some(filer.value()) {
        return Err(malformed(format!(
            "submissions for `{}` answered {}",
            submissions.cik,
            filer.value()
        )));
    }
    let prefix = format!("CIK{:010}-submissions-", filer.value());
    let mut history = Vec::new();
    for file in submissions.filings.files {
        let own = file
            .name
            .strip_prefix(&prefix)
            .and_then(|rest| rest.strip_suffix(".json"))
            .is_some_and(|number| {
                !number.is_empty() && number.bytes().all(|byte| byte.is_ascii_digit())
            });
        match (
            own,
            SessionDate::from_date(file.filing_to) >= SessionDate::at(since),
        ) {
            (false, _) => return Err(malformed(format!("history file `{}`", file.name))),
            (true, true) => history.push(file.name),
            (true, false) => {}
        }
    }
    Ok((reports_in(submissions.filings.recent, since)?, history))
}

/// The annual reports a history file lists accepted at or after `since`.
fn parse_history(body: &[u8], since: DateTime<Utc>) -> Result<Vec<AnnualReport>, FetchError> {
    reports_in(serde_json::from_slice(body).map_err(malformed)?, since)
}

/// The annual reports among `columns` accepted at or after `since`.
fn reports_in(columns: Columns, since: DateTime<Utc>) -> Result<Vec<AnnualReport>, FetchError> {
    let rows = columns.form.len();
    if columns.accession_number.len() != rows || columns.acceptance_date_time.len() != rows {
        return Err(malformed("the filings' columns differ in length"));
    }
    let mut reports = Vec::new();
    for ((form, accession), accepted_at) in columns
        .form
        .iter()
        .zip(&columns.accession_number)
        .zip(&columns.acceptance_date_time)
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
    let mut named = false;
    for line in header.lines() {
        match line.split_once('>') {
            Some(("<CIK", value)) => {
                ours = value.trim().parse::<u64>().ok() == Some(filer.value());
                named |= ours;
            }
            Some(("<ASSIGNED-SIC", value)) if ours => {
                let code = IndustryCode::new(value.trim()).map_err(malformed)?;
                return Ok((code.code() != 0).then_some(code));
            }
            Some(("</COMPANY-DATA", _)) => ours = false,
            Some(_) | None => {}
        }
    }
    match named {
        true => Ok(None),
        false => Err(malformed(format!(
            "the header names no filer {}",
            filer.value()
        ))),
    }
}

#[cfg(test)]
mod tests {
    use chrono::Datelike;

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
        let (reports, history) =
            parse_annual_reports(SUBMISSIONS.as_bytes(), filer(), since()).unwrap();
        assert_eq!(history, Vec::<String>::new());
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

    #[test]
    fn test_history_files_reaching_the_cutoff_are_named_and_read() {
        let listed = SUBMISSIONS.replace(
            r#""files":[]"#,
            r#""files":[{"name":"CIK0000004321-submissions-002.json","filingCount":900,"filingFrom":"2020-06-01","filingTo":"2022-03-31"},{"name":"CIK0000004321-submissions-001.json","filingCount":1000,"filingFrom":"2001-01-02","filingTo":"2020-05-29"}]"#,
        );
        let (_, history) = parse_annual_reports(listed.as_bytes(), filer(), since()).unwrap();
        assert_eq!(history, ["CIK0000004321-submissions-002.json"]);
        let foreign = SUBMISSIONS.replace(
            r#""files":[]"#,
            r#""files":[{"name":"../CIK0000004321-submissions-002.json","filingCount":1,"filingFrom":"2022-01-01","filingTo":"2022-03-31"}]"#,
        );
        assert!(matches!(
            parse_annual_reports(foreign.as_bytes(), filer(), since()),
            Err(FetchError::Malformed { .. })
        ));
        let older = r#"{"accessionNumber":["0000004321-22-000007","0000004321-21-000008"],
            "acceptanceDateTime":["2022-02-15T21:00:00.000Z","2021-02-15T21:00:00.000Z"],
            "form":["40-F","40-F"]}"#;
        let reports = parse_history(older.as_bytes(), since()).unwrap();
        let read: Vec<&str> = reports
            .iter()
            .map(|report| report.accession().as_str())
            .collect();
        assert_eq!(read, ["0000004321-22-000007"]);
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
        assert!(matches!(
            code(&header(&[("0000009999", "6029")])),
            Err(FetchError::Malformed { .. })
        ));
        assert_eq!(code(&header(&[("0000004321", "0000")])), Ok(None));
        let unassigned = header(&[("0000004321", "2834")]).replace("<ASSIGNED-SIC>2834\n", "");
        assert_eq!(code(&unassigned), Ok(None));
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

    /// Reads a frequent filer whose annual reports since 2020 sit in its history files as well as its recent list.
    #[tokio::test]
    #[ignore = "fetches from EDGAR and needs SEC_EDGAR_CONTACT_EMAIL"]
    async fn live_a_frequent_filer_reads_its_history_files() {
        let sec = Sec::from_environment(reqwest::Client::new()).unwrap();
        let filer = CentralIndexKey::new(1_000_275).unwrap();
        let since = "2020-08-01T00:00:00Z".parse().unwrap();
        let (reports, bodies) = sec.annual_reports(filer, since).await.into_parts();
        let reports = reports.unwrap();
        let years: std::collections::BTreeSet<i32> = reports
            .iter()
            .map(|report| report.accepted_at().year())
            .collect();
        println!(
            "{} reports over {} bodies: {years:?}",
            reports.len(),
            bodies.len()
        );
        assert!(bodies.len() > 1);
        assert!(years.is_superset(&(2021..=2025).collect()), "{years:?}");
    }
}
