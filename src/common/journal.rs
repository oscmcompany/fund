//! The journal's records: observations only, one record per state change, each line naming the schema version that
//! wrote it so a reader never infers it.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::common::time::SessionDate;

/// Stamped on every record this build writes; it only goes up, and a reader maps old versions forward.
pub const SCHEMA_VERSION: u64 = 1;

/// One process's run, within which `sequence` orders records.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct RunId(Uuid);

impl RunId {
    /// The writer draws the id, since randomness is an effect.
    pub fn new(id: Uuid) -> Self {
        Self(id)
    }
}

impl std::fmt::Display for RunId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}", self.0)
    }
}

/// A 40-character git sha, suffixed `-dirty` when the tree that built it differed from it.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct Commit(String);

/// Why a commit was refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommitRefusal {
    Malformed { raw: String },
}

impl std::fmt::Display for CommitRefusal {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Malformed { raw } => write!(formatter, "`{raw}` is not a 40-character git sha"),
        }
    }
}

impl Commit {
    pub fn new(raw: &str) -> Result<Self, CommitRefusal> {
        let sha = raw.strip_suffix("-dirty").unwrap_or(raw);
        let hexadecimal = |byte: u8| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte);
        if sha.len() == 40 && sha.bytes().all(hexadecimal) {
            Ok(Self(raw.to_string()))
        } else {
            Err(CommitRefusal::Malformed {
                raw: raw.to_string(),
            })
        }
    }

    pub fn is_dirty(&self) -> bool {
        self.0.ends_with("-dirty")
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for Commit {
    type Error = CommitRefusal;

    fn try_from(raw: String) -> Result<Self, Self::Error> {
        Self::new(&raw)
    }
}

impl From<Commit> for String {
    fn from(commit: Commit) -> Self {
        commit.0
    }
}

/// One line of the journal: an observation and the envelope that orders and attributes it.
///
/// Nothing derivable is stored: the session comes from `timestamp` and the host from the `producer=` partition.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Record {
    schema_version: u64,
    run_id: RunId,
    /// Counts from 1 within a run, so a gap is a lost record.
    sequence: u64,
    timestamp: DateTime<Utc>,
    /// Absent when the build could not ask git.
    commit: Option<Commit>,
    #[serde(flatten)]
    observation: Observation,
}

impl Record {
    pub fn new(
        run_id: RunId,
        sequence: u64,
        timestamp: DateTime<Utc>,
        commit: Option<Commit>,
        observation: Observation,
    ) -> Self {
        Self {
            schema_version: SCHEMA_VERSION,
            run_id,
            sequence,
            timestamp,
            commit,
            observation,
        }
    }

    pub fn run_id(&self) -> RunId {
        self.run_id
    }

    pub fn sequence(&self) -> u64 {
        self.sequence
    }

    pub fn timestamp(&self) -> DateTime<Utc> {
        self.timestamp
    }

    pub fn commit(&self) -> Option<&Commit> {
        self.commit.as_ref()
    }

    pub fn observation(&self) -> &Observation {
        &self.observation
    }

    /// The session the record happened in, which files it.
    pub fn session(&self) -> SessionDate {
        SessionDate::at(self.timestamp)
    }

    /// The record as one JSON line, without the newline.
    pub fn encode(&self) -> String {
        serde_json::to_string(self).expect("a record has only string keys, so it serializes")
    }
}

/// What happened, named `<subject>_<past participle>`. Each variant is one whole state change, so a crash can
/// lose a record but never leave half of one.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, strum::IntoStaticStr)]
#[serde(tag = "event_type", content = "payload", rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum Observation {
    ConfigurationResolved(ConfigurationResolved),
}

impl Observation {
    pub fn event_type(&self) -> &'static str {
        self.into()
    }
}

/// Every parameter a binary resolved at startup and where each value came from, written once per run so replay
/// reads the run's own values rather than today's.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConfigurationResolved {
    parameters: BTreeMap<String, ResolvedParameter>,
}

impl ConfigurationResolved {
    pub fn new(parameters: BTreeMap<String, ResolvedParameter>) -> Self {
        Self { parameters }
    }

    pub fn parameters(&self) -> &BTreeMap<String, ResolvedParameter> {
        &self.parameters
    }
}

/// A parameter's value as text, and whether someone set it or nobody did.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolvedParameter {
    value: String,
    source: ParameterSource,
}

impl ResolvedParameter {
    pub fn new(value: String, source: ParameterSource) -> Self {
        Self { value, source }
    }

    pub fn value(&self) -> &str {
        &self.value
    }

    pub fn source(&self) -> ParameterSource {
        self.source
    }
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    strum::Display,
    strum::EnumString,
    strum::IntoStaticStr,
    strum::EnumIter,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum ParameterSource {
    Environment,
    Default,
}

/// One journal line read back; a line this build cannot type is kept with its cause, never dropped, so a reader
/// never reports a shorter run than the one that happened.
#[derive(Debug, Clone, PartialEq)]
pub enum ReadLine {
    Read(Box<Record>),
    Unreadable { line: usize, cause: UnreadableCause },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UnreadableCause {
    NotJson {
        reason: String,
    },
    NoVersion,
    /// Written under a schema this build does not read.
    OtherVersion {
        schema_version: u64,
    },
    Malformed {
        event_type: Option<String>,
        reason: String,
    },
}

/// Reads every non-blank line of a journal file in the order it was written, checking each line's version first.
pub fn read(contents: &str) -> Vec<ReadLine> {
    contents
        .lines()
        .enumerate()
        .filter(|(_, line)| !line.trim().is_empty())
        .map(|(index, line)| match read_line(line) {
            Ok(record) => ReadLine::Read(Box::new(record)),
            Err(cause) => ReadLine::Unreadable {
                line: index + 1,
                cause,
            },
        })
        .collect()
}

fn read_line(line: &str) -> Result<Record, UnreadableCause> {
    let value: serde_json::Value =
        serde_json::from_str(line).map_err(|error| UnreadableCause::NotJson {
            reason: error.to_string(),
        })?;
    let schema_version = value
        .get("schema_version")
        .and_then(serde_json::Value::as_u64)
        .ok_or(UnreadableCause::NoVersion)?;
    if schema_version != SCHEMA_VERSION {
        return Err(UnreadableCause::OtherVersion { schema_version });
    }
    let event_type = value
        .get("event_type")
        .and_then(serde_json::Value::as_str)
        .map(str::to_string);
    serde_json::from_value(value).map_err(|error| UnreadableCause::Malformed {
        event_type,
        reason: error.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;
    use strum::IntoEnumIterator;

    use super::*;

    const SHA: &str = "0123456789abcdef0123456789abcdef01234567";

    fn record(commit: Option<Commit>) -> Record {
        let parameters = BTreeMap::from([
            (
                "liquidity_floor".to_string(),
                ResolvedParameter::new("2.0".to_string(), ParameterSource::Default),
            ),
            (
                "lookback_sessions".to_string(),
                ResolvedParameter::new("20".to_string(), ParameterSource::Environment),
            ),
        ]);
        Record::new(
            RunId::new(Uuid::from_u128(1)),
            1,
            "2026-07-31T14:30:00Z".parse().unwrap(),
            commit,
            Observation::ConfigurationResolved(ConfigurationResolved::new(parameters)),
        )
    }

    #[test]
    fn test_a_record_encodes_to_its_wire_format() {
        assert_eq!(
            record(Some(Commit::new(SHA).unwrap())).encode(),
            concat!(
                r#"{"schema_version":1,"run_id":"00000000-0000-0000-0000-000000000001","sequence":1,"#,
                r#""timestamp":"2026-07-31T14:30:00Z","commit":"0123456789abcdef0123456789abcdef01234567","#,
                r#""event_type":"configuration_resolved","payload":{"parameters":{"#,
                r#""liquidity_floor":{"value":"2.0","source":"default"},"#,
                r#""lookback_sessions":{"value":"20","source":"environment"}}}}"#,
            )
        );
    }

    #[test]
    fn test_the_event_type_agrees_with_the_serialized_tag() {
        let record = record(None);
        let value: serde_json::Value = serde_json::from_str(&record.encode()).unwrap();
        assert_eq!(value["event_type"], "configuration_resolved");
        assert_eq!(record.observation().event_type(), "configuration_resolved");
    }

    #[test]
    fn test_parameter_source_names_agree_between_strum_and_serde() {
        let names: Vec<&str> = ParameterSource::iter().map(Into::into).collect();
        assert_eq!(names, ["environment", "default"]);
        for source in ParameterSource::iter() {
            assert_eq!(
                serde_json::to_string(&source).unwrap(),
                format!("\"{source}\"")
            );
            assert_eq!(source.to_string().parse(), Ok(source));
        }
    }

    #[test]
    fn test_a_commit_is_a_sha_or_a_dirty_sha() {
        assert!(!Commit::new(SHA).unwrap().is_dirty());
        assert!(Commit::new(&format!("{SHA}-dirty")).unwrap().is_dirty());
        for raw in [
            "",
            "unknown",
            &SHA[..39],
            &SHA.to_uppercase(),
            &format!("{SHA}-clean"),
        ] {
            assert_eq!(
                Commit::new(raw),
                Err(CommitRefusal::Malformed {
                    raw: raw.to_string()
                }),
                "{raw}"
            );
        }
    }

    #[test]
    fn test_a_record_is_filed_under_its_eastern_session() {
        let late = Record::new(
            RunId::new(Uuid::from_u128(1)),
            2,
            // 23:00 Eastern on July 31.
            "2026-08-01T03:00:00Z".parse().unwrap(),
            None,
            record(None).observation().clone(),
        );
        assert_eq!(late.session().to_string(), "2026-07-31");
    }

    #[test]
    fn test_every_line_reads_back_or_says_why_not() {
        let good = record(None).encode();
        let malformed = good.replace("configuration_resolved", "configuration_guessed");
        let contents = [
            good.as_str(),
            "",
            "not json",
            r#"{"sequence":1}"#,
            r#"{"schema_version":2,"event_type":"configuration_resolved"}"#,
            malformed.as_str(),
        ]
        .join("\n");
        let lines = read(&contents);
        assert_eq!(lines.len(), 5);
        assert_eq!(lines[0], ReadLine::Read(Box::new(record(None))));
        let causes: Vec<(usize, UnreadableCause)> = lines[1..]
            .iter()
            .map(|line| match line {
                ReadLine::Unreadable { line, cause } => (*line, cause.clone()),
                ReadLine::Read(record) => panic!("read {record:?}"),
            })
            .map(|(line, cause)| match cause {
                UnreadableCause::NotJson { .. } => (
                    line,
                    UnreadableCause::NotJson {
                        reason: String::new(),
                    },
                ),
                UnreadableCause::Malformed { event_type, .. } => (
                    line,
                    UnreadableCause::Malformed {
                        event_type,
                        reason: String::new(),
                    },
                ),
                cause => (line, cause),
            })
            .collect();
        assert_eq!(
            causes,
            [
                (
                    3,
                    UnreadableCause::NotJson {
                        reason: String::new()
                    }
                ),
                (4, UnreadableCause::NoVersion),
                (5, UnreadableCause::OtherVersion { schema_version: 2 }),
                (
                    6,
                    UnreadableCause::Malformed {
                        event_type: Some("configuration_guessed".to_string()),
                        reason: String::new()
                    }
                ),
            ]
        );
    }

    #[test]
    fn test_a_malformed_commit_is_refused_on_read() {
        let contents = record(None)
            .encode()
            .replace(r#""commit":null"#, r#""commit":"unknown""#);
        assert!(matches!(
            read(&contents).as_slice(),
            [ReadLine::Unreadable {
                line: 1,
                cause: UnreadableCause::Malformed { .. }
            }]
        ));
    }

    fn any_record() -> impl Strategy<Value = Record> {
        (
            any::<u128>(),
            1_u64..u64::MAX,
            0_i64..4_102_444_800,
            0_u32..1_000_000_000,
            prop::option::of(("[0-9a-f]{40}", any::<bool>())),
            prop::collection::btree_map(
                "[a-z_]{1,20}",
                (
                    ".{0,20}",
                    prop::sample::select(vec![
                        ParameterSource::Environment,
                        ParameterSource::Default,
                    ]),
                ),
                0..8,
            ),
        )
            .prop_map(
                |(run, sequence, seconds, nanoseconds, commit, parameters)| {
                    let commit = commit.map(|(sha, dirty)| {
                        Commit::new(&if dirty { format!("{sha}-dirty") } else { sha }).unwrap()
                    });
                    let parameters = parameters
                        .into_iter()
                        .map(|(name, (value, source))| {
                            (name, ResolvedParameter::new(value, source))
                        })
                        .collect();
                    Record::new(
                        RunId::new(Uuid::from_u128(run)),
                        sequence,
                        DateTime::from_timestamp(seconds, nanoseconds).unwrap(),
                        commit,
                        Observation::ConfigurationResolved(ConfigurationResolved::new(parameters)),
                    )
                },
            )
    }

    proptest! {
        #[test]
        fn property_a_record_reads_back_as_itself(record in any_record()) {
            prop_assert_eq!(read(&record.encode()), vec![ReadLine::Read(Box::new(record))]);
        }
    }
}
