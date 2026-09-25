//! The Register: every test against the substrate, opened before it runs and closed with a verdict.
//!
//! Private state, so it lives in the records bucket under `exports/register/`; any local copy is scratch.

use aws_sdk_s3::operation::get_object::GetObjectError;
use aws_sdk_s3::Client as S3Client;
use serde::{Deserialize, Serialize};

use crate::common::types::SessionDate;
use crate::data::archive::{put_object_with_precondition, Precondition, WriteOutcome};

/// The prefix every accession is stored under, one object per number.
pub const REGISTER_PREFIX: &str = "exports/register";

/// An accession's number: assigned once, in order, and never reused.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct AccessionNumber(u32);

impl AccessionNumber {
    pub const FIRST: Self = Self(1);

    pub fn new(number: u32) -> Option<Self> {
        (number > 0).then_some(Self(number))
    }

    /// The number after this one, or `None` once `u32` is spent.
    pub fn next(self) -> Option<Self> {
        self.0.checked_add(1).map(Self)
    }

    /// The object this accession is stored at.
    pub fn key(self) -> String {
        format!("{REGISTER_PREFIX}/{self}.json")
    }

    /// Reads a number back from its key, or `None` for anything else under the prefix.
    pub fn from_key(key: &str) -> Option<Self> {
        let number = key
            .strip_prefix(REGISTER_PREFIX)?
            .strip_prefix('/')?
            .strip_suffix(".json")?;
        number
            .parse()
            .ok()
            .and_then(Self::new)
            .filter(|parsed| parsed.key() == key)
    }
}

impl std::fmt::Display for AccessionNumber {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{:04}", self.0)
    }
}

/// What a closed accession found.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Verdict {
    /// Cleared the named baseline, the split, cost and the family haircut. Frozen.
    Accept,
    /// This hypothesis, universe and horizon, and nothing wider.
    Refute,
    /// Unmeasurable, underpowered, or the control fired too. Still counts in the family.
    Inconclusive,
}

/// The expected effect and interval committed before the study runs, in the verdict's units.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Bid {
    Recorded(String),
    /// Opened before bids existed. Counted as unscored, never reconstructed.
    Unrecorded,
}

/// How many sessions the verdict was measured over.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Sessions {
    Counted(usize),
    /// A seed result whose count was never written down. Never reconstructed.
    Unrecorded,
}

/// Everything committed when an accession opens, before the study binary runs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Opening {
    /// The multiple-testing bucket the haircut is taken over.
    pub family: String,
    /// Named and versioned, never a bare threshold.
    pub universe: String,
    pub horizon: String,
    pub hypothesis: String,
    pub bid: Bid,
    pub opened: SessionDate,
    pub supersedes: Option<AccessionNumber>,
    /// Why a successor to an accepted accession is a new test rather than a re-measure.
    pub substrate_change: Option<String>,
}

/// What a test cost, so the Register counts a two-week fold differently from a minute's check.
#[derive(Debug, Clone, Copy, PartialEq, Default, Serialize, Deserialize)]
pub struct StudyCost {
    pub wall_clock_seconds: Option<u64>,
    pub bytes_read: Option<u64>,
    /// Where the pass touched a metered source.
    pub dollars: Option<f64>,
}

/// Everything recorded when an accession closes.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Closing {
    pub verdict: Verdict,
    /// The number the verdict rests on, in the bid's units.
    pub statistic: String,
    pub sessions: Sessions,
    pub commits: Vec<String>,
    pub closed: SessionDate,
    /// For an inconclusive verdict, the one change its successor makes.
    pub notes: Option<String>,
    pub cost: StudyCost,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Status {
    Open,
    Closed(Closing),
}

/// One test against the substrate.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Accession {
    pub number: AccessionNumber,
    pub opening: Opening,
    pub status: Status,
}

/// Why a change to the Register was refused.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum RegisterRefusal {
    #[error("accession {0} is already closed")]
    AlreadyClosed(AccessionNumber),
    #[error("accession {0} is open, so it has no verdict to supersede yet")]
    StillOpen(AccessionNumber),
    #[error(
        "accession {0} was accepted and is frozen; a successor must name the substrate change"
    )]
    AcceptedIsFrozen(AccessionNumber),
    #[error("accession {0} is already superseded by {1}")]
    AlreadySuperseded(AccessionNumber, AccessionNumber),
    #[error("an inconclusive verdict must name the one change its successor makes")]
    InconclusiveWithoutNotes,
}

/// Text that says something: `None` for an absent or blank value, so neither can stand in for one.
fn stated(text: &Option<String>) -> Option<&str> {
    text.as_deref()
        .map(str::trim)
        .filter(|text| !text.is_empty())
}

impl Accession {
    pub fn open(number: AccessionNumber, opening: Opening) -> Self {
        Self {
            number,
            opening,
            status: Status::Open,
        }
    }

    pub fn is_open(&self) -> bool {
        matches!(self.status, Status::Open)
    }

    /// Records the verdict. An accession closes once; a second reading is a new accession.
    pub fn close(mut self, closing: Closing) -> Result<Self, RegisterRefusal> {
        match (&self.status, closing.verdict, stated(&closing.notes)) {
            (Status::Closed(_), _, _) => Err(RegisterRefusal::AlreadyClosed(self.number)),
            (Status::Open, Verdict::Inconclusive, None) => {
                Err(RegisterRefusal::InconclusiveWithoutNotes)
            }
            (Status::Open, _, _) => {
                self.status = Status::Closed(closing);
                Ok(self)
            }
        }
    }

    /// Refuses `opening` as a successor unless this accession has a verdict and no successor yet.
    ///
    /// An accepted accession is frozen: its successor must name what changed underneath it, because
    /// a new window over the same substrate is a new free choice.
    pub fn admit_successor(
        &self,
        existing_successor: Option<AccessionNumber>,
        opening: &Opening,
    ) -> Result<(), RegisterRefusal> {
        if let Some(existing) = existing_successor {
            return Err(RegisterRefusal::AlreadySuperseded(self.number, existing));
        }
        match &self.status {
            Status::Open => Err(RegisterRefusal::StillOpen(self.number)),
            Status::Closed(closing) => match (closing.verdict, stated(&opening.substrate_change)) {
                (Verdict::Accept, None) => Err(RegisterRefusal::AcceptedIsFrozen(self.number)),
                (Verdict::Accept, Some(_)) | (Verdict::Refute | Verdict::Inconclusive, _) => Ok(()),
            },
        }
    }
}

/// The accession that names `predecessor` in its `supersedes`, derived rather than stored so the
/// two directions of the link cannot disagree.
pub fn successor_of(
    accessions: &[Accession],
    predecessor: AccessionNumber,
) -> Option<AccessionNumber> {
    accessions
        .iter()
        .find(|accession| accession.opening.supersedes == Some(predecessor))
        .map(|accession| accession.number)
}

/// Errors reading or writing the Register.
#[derive(Debug, thiserror::Error)]
pub enum RegisterError {
    #[error("failed to reach s3://{bucket}/{key}: {message}")]
    Storage {
        bucket: String,
        key: String,
        message: String,
    },
    #[error("accession at {key} does not parse: {source}")]
    Parse {
        key: String,
        source: serde_json::Error,
    },
    #[error("accession {0} does not exist")]
    Missing(AccessionNumber),
    #[error("accession {0} changed since it was read; read it again")]
    Contended(AccessionNumber),
    #[error("{0}")]
    Refused(#[from] RegisterRefusal),
    #[error("every accession number is taken")]
    Exhausted,
}

/// Every accession number stored, in order.
pub async fn numbers(
    s3_client: &S3Client,
    bucket: &str,
) -> Result<Vec<AccessionNumber>, RegisterError> {
    let mut numbers = Vec::new();
    let mut pages = s3_client
        .list_objects_v2()
        .bucket(bucket)
        .prefix(format!("{REGISTER_PREFIX}/"))
        .into_paginator()
        .send();
    while let Some(page) = pages.next().await {
        let page = page.map_err(|error| RegisterError::Storage {
            bucket: bucket.to_string(),
            key: REGISTER_PREFIX.to_string(),
            message: error.to_string(),
        })?;
        numbers.extend(
            page.contents()
                .iter()
                .filter_map(|object| object.key().and_then(AccessionNumber::from_key)),
        );
    }
    numbers.sort();
    Ok(numbers)
}

/// One accession and the ETag it carried, which a later write must still find.
pub async fn read(
    s3_client: &S3Client,
    bucket: &str,
    number: AccessionNumber,
) -> Result<(Accession, String), RegisterError> {
    let key = number.key();
    let response = match s3_client.get_object().bucket(bucket).key(&key).send().await {
        Ok(response) => response,
        Err(error) => {
            return match error.into_service_error() {
                GetObjectError::NoSuchKey(_) => Err(RegisterError::Missing(number)),
                other => Err(RegisterError::Storage {
                    bucket: bucket.to_string(),
                    key,
                    message: other.to_string(),
                }),
            }
        }
    };
    let etag = response.e_tag().unwrap_or_default().to_string();
    let bytes = response
        .body
        .collect()
        .await
        .map_err(|error| RegisterError::Storage {
            bucket: bucket.to_string(),
            key: key.clone(),
            message: error.to_string(),
        })?
        .into_bytes();
    let accession =
        serde_json::from_slice(&bytes).map_err(|source| RegisterError::Parse { key, source })?;
    Ok((accession, etag))
}

/// Writes an accession, creating it only if absent or replacing only the version that was read.
pub async fn write(
    s3_client: &S3Client,
    bucket: &str,
    accession: &Accession,
    read_etag: Option<&str>,
) -> Result<(), RegisterError> {
    let body = serde_json::to_vec_pretty(accession).map_err(|source| RegisterError::Parse {
        key: accession.number.key(),
        source,
    })?;
    let precondition = match read_etag {
        Some(etag) => Precondition::Match(etag.to_string()),
        None => Precondition::Absent,
    };
    match put_object_with_precondition(
        s3_client,
        bucket,
        &accession.number.key(),
        body,
        "application/json",
        &precondition,
    )
    .await
    {
        WriteOutcome::Written => Ok(()),
        WriteOutcome::Contended => Err(RegisterError::Contended(accession.number)),
        WriteOutcome::Failed(message) => Err(RegisterError::Storage {
            bucket: bucket.to_string(),
            key: accession.number.key(),
            message,
        }),
    }
}

/// Every accession stored, in order.
pub async fn read_all(s3_client: &S3Client, bucket: &str) -> Result<Vec<Accession>, RegisterError> {
    let mut accessions = Vec::new();
    for number in numbers(s3_client, bucket).await? {
        accessions.push(read(s3_client, bucket, number).await?.0);
    }
    Ok(accessions)
}

/// Opens an accession under the next unused number, after its predecessor, if any, admits it.
///
/// Two successors opened at once can both pass the admission; the derived link then shows both
/// rather than leaving either dangling.
pub async fn open(
    s3_client: &S3Client,
    bucket: &str,
    opening: Opening,
) -> Result<Accession, RegisterError> {
    if let Some(predecessor) = opening.supersedes {
        let accessions = read_all(s3_client, bucket).await?;
        let stored = accessions
            .iter()
            .find(|accession| accession.number == predecessor)
            .ok_or(RegisterError::Missing(predecessor))?;
        stored.admit_successor(successor_of(&accessions, predecessor), &opening)?;
    }
    create(s3_client, bucket, |number| {
        Ok(Accession::open(number, opening.clone()))
    })
    .await
}

/// Records a test run before the Register existed, opened and closed in one write.
///
/// Its bid is unrecorded by construction, so this path cannot stand in for a pre-registered open.
pub async fn seed(
    s3_client: &S3Client,
    bucket: &str,
    opening: Opening,
    closing: Closing,
) -> Result<Accession, RegisterError> {
    let opening = Opening {
        bid: Bid::Unrecorded,
        supersedes: None,
        substrate_change: None,
        ..opening
    };
    create(s3_client, bucket, |number| {
        Ok(Accession::open(number, opening.clone()).close(closing.clone())?)
    })
    .await
}

/// Writes the accession `build` makes under the next unused number.
///
/// The number is taken from what the bucket holds, never a local count, and the create is
/// conditional: a second writer taking the same number makes this one retry with the next.
async fn create(
    s3_client: &S3Client,
    bucket: &str,
    build: impl Fn(AccessionNumber) -> Result<Accession, RegisterError>,
) -> Result<Accession, RegisterError> {
    let mut number = match numbers(s3_client, bucket).await?.last() {
        Some(last) => last.next().ok_or(RegisterError::Exhausted)?,
        None => AccessionNumber::FIRST,
    };
    for _ in 0..8 {
        let accession = build(number)?;
        match write(s3_client, bucket, &accession, None).await {
            Ok(()) => return Ok(accession),
            Err(RegisterError::Contended(_)) => {
                number = number.next().ok_or(RegisterError::Exhausted)?
            }
            Err(other) => return Err(other),
        }
    }
    Err(RegisterError::Contended(number))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn session() -> SessionDate {
        SessionDate::from_date(chrono::NaiveDate::from_ymd_opt(2026, 9, 25).unwrap())
    }

    fn opening() -> Opening {
        Opening {
            family: "residual-panel".to_string(),
            universe: "liquidity-floor-2026 ($10, $50M, 30 sessions)".to_string(),
            horizon: "1 session".to_string(),
            hypothesis: "the sector factor explains residual co-movement".to_string(),
            bid: Bid::Recorded("+4pp over the permuted null, 80% [0, +9]".to_string()),
            opened: session(),
            supersedes: None,
            substrate_change: None,
        }
    }

    fn closing(verdict: Verdict) -> Closing {
        Closing {
            verdict,
            statistic: "+5.2pp".to_string(),
            sessions: Sessions::Counted(250),
            commits: vec!["abc1234".to_string()],
            closed: session(),
            notes: None,
            cost: StudyCost::default(),
        }
    }

    fn number(value: u32) -> AccessionNumber {
        AccessionNumber::new(value).unwrap()
    }

    #[test]
    fn test_a_number_is_four_digits_and_reads_back_only_from_its_own_key() {
        assert_eq!(number(7).key(), "exports/register/0007.json");
        assert_eq!(
            AccessionNumber::from_key("exports/register/0007.json"),
            Some(number(7))
        );
        assert_eq!(AccessionNumber::from_key("exports/register/7.json"), None);
        assert_eq!(
            AccessionNumber::from_key("exports/register/0000.json"),
            None
        );
        assert_eq!(
            AccessionNumber::from_key("exports/register/0007.toml"),
            None
        );
        assert_eq!(AccessionNumber::new(0), None);
    }

    #[test]
    fn test_an_accession_closes_once() {
        let closed = Accession::open(number(1), opening())
            .close(closing(Verdict::Refute))
            .expect("an open accession closes");
        assert!(!closed.is_open());
        assert_eq!(
            closed.close(closing(Verdict::Accept)),
            Err(RegisterRefusal::AlreadyClosed(number(1)))
        );
    }

    #[test]
    fn test_an_inconclusive_verdict_must_name_its_successors_change() {
        let open = Accession::open(number(1), opening());
        assert_eq!(
            open.clone().close(closing(Verdict::Inconclusive)),
            Err(RegisterRefusal::InconclusiveWithoutNotes)
        );
        let mut blank = closing(Verdict::Inconclusive);
        blank.notes = Some(" ".to_string());
        assert_eq!(
            open.clone().close(blank),
            Err(RegisterRefusal::InconclusiveWithoutNotes)
        );
        let mut with_notes = closing(Verdict::Inconclusive);
        with_notes.notes = Some("widen to two years".to_string());
        assert!(open.close(with_notes).is_ok());
    }

    #[test]
    fn test_an_accepted_accession_is_superseded_only_over_a_stated_substrate_change() {
        let accepted = Accession::open(number(1), opening())
            .close(closing(Verdict::Accept))
            .unwrap();
        assert_eq!(
            accepted.admit_successor(None, &opening()),
            Err(RegisterRefusal::AcceptedIsFrozen(number(1)))
        );
        let mut blank = opening();
        blank.substrate_change = Some("  ".to_string());
        assert_eq!(
            accepted.admit_successor(None, &blank),
            Err(RegisterRefusal::AcceptedIsFrozen(number(1)))
        );
        let mut changed = opening();
        changed.substrate_change = Some("point-in-time universe (#1144)".to_string());
        assert_eq!(accepted.admit_successor(None, &changed), Ok(()));
        assert_eq!(
            accepted.admit_successor(Some(number(3)), &changed),
            Err(RegisterRefusal::AlreadySuperseded(number(1), number(3)))
        );
    }

    #[test]
    fn test_a_refuted_accession_admits_a_successor_without_a_substrate_change() {
        let refuted = Accession::open(number(1), opening())
            .close(closing(Verdict::Refute))
            .unwrap();
        assert_eq!(refuted.admit_successor(None, &opening()), Ok(()));
    }

    #[test]
    fn test_an_open_accession_has_nothing_to_supersede() {
        let open = Accession::open(number(1), opening());
        assert_eq!(
            open.admit_successor(None, &opening()),
            Err(RegisterRefusal::StillOpen(number(1)))
        );
    }

    #[test]
    fn test_the_successor_link_is_derived_from_the_successor() {
        let first = Accession::open(number(1), opening());
        let mut pointing = opening();
        pointing.supersedes = Some(number(1));
        let second = Accession::open(number(2), pointing);
        let accessions = [first, second];
        assert_eq!(successor_of(&accessions, number(1)), Some(number(2)));
        assert_eq!(successor_of(&accessions, number(2)), None);
    }

    #[test]
    fn test_the_last_number_has_no_next() {
        assert_eq!(number(1).next(), Some(number(2)));
        assert_eq!(number(u32::MAX).next(), None);
    }

    #[test]
    fn test_an_accession_round_trips_through_its_stored_form() {
        let accession = Accession::open(number(12), opening())
            .close(closing(Verdict::Refute))
            .unwrap();
        let stored = serde_json::to_string(&accession).unwrap();
        assert_eq!(
            serde_json::from_str::<Accession>(&stored).unwrap(),
            accession
        );
        assert!(stored.contains(r#""number":12"#), "{stored}");
    }

    /// The number comes from what the bucket holds, and a writer that loses the race for it retries
    /// with the next rather than overwriting the accession that won.
    #[tokio::test]
    async fn test_open_takes_the_next_number_and_retries_past_a_lost_race() {
        use aws_smithy_http_client::test_util::infallible_client_fn;
        use aws_smithy_types::body::SdkBody;
        use std::sync::{Arc, Mutex};

        let puts: Arc<Mutex<Vec<(String, Option<String>)>>> = Arc::default();
        let seen = Arc::clone(&puts);
        let http_client = infallible_client_fn(move |request| {
            let path = request.uri().path().to_string();
            let status = |code: u16, body: &str| {
                http::Response::builder()
                    .status(code)
                    .body(SdkBody::from(body.to_string()))
                    .unwrap()
            };
            match *request.method() {
                http::Method::GET => status(
                    200,
                    "<ListBucketResult><Contents><Key>exports/register/0001.json</Key></Contents>\
                     <Contents><Key>exports/register/0002.json</Key></Contents>\
                     <IsTruncated>false</IsTruncated></ListBucketResult>",
                ),
                http::Method::PUT => {
                    let if_none_match = request
                        .headers()
                        .get("if-none-match")
                        .and_then(|value| value.to_str().ok())
                        .map(str::to_string);
                    let mut puts = seen.lock().unwrap();
                    puts.push((path.clone(), if_none_match));
                    // Another writer took 0003 between the listing and this create.
                    if path.ends_with("0003.json") {
                        status(412, "")
                    } else {
                        status(200, "")
                    }
                }
                _ => status(500, ""),
            }
        });
        let client = S3Client::from_conf(
            aws_sdk_s3::Config::builder()
                .behavior_version(aws_sdk_s3::config::BehaviorVersion::latest())
                .region(aws_sdk_s3::config::Region::new("us-east-1"))
                .credentials_provider(aws_sdk_s3::config::Credentials::new(
                    "test-key",
                    "test-secret",
                    None,
                    None,
                    "test",
                ))
                .http_client(http_client)
                .build(),
        );

        let opened = open(&client, "records", opening())
            .await
            .expect("the next free number opens");

        assert_eq!(opened.number, number(4));
        let puts = puts.lock().unwrap().clone();
        assert_eq!(
            puts,
            vec![
                (
                    "/exports/register/0003.json".to_string(),
                    Some("*".to_string())
                ),
                (
                    "/exports/register/0004.json".to_string(),
                    Some("*".to_string())
                ),
            ]
        );
    }
}
