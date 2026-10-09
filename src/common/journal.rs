//! The journal's records: observations only, one record per state change, each line naming the schema version that
//! wrote it so a reader never infers it.

use std::collections::BTreeMap;
use std::num::NonZeroU64;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::common::book::{Book, Cash, Position};
use crate::common::guard::{OrderGuarded, TradabilityRead, TradabilityUnread};
use crate::common::heal::{Leg, PartitionFailureKind, SessionOutcome, Unrecognized, Window};
use crate::common::laboratory::experiment::{DatasetRead, ExperimentRan};
use crate::common::market::Symbol;
use crate::common::market::quote_bars::QuoteFoldCounts;
use crate::common::market::refusal::RowRefusalKind;
use crate::common::market::trade_bars::{BarBuilt, TradeFoldCounts};
use crate::common::monoid::Tally;
use crate::common::order::{OrderClosed, OrderRefused, OrderSubmitted, OrderUnresolved};
use crate::common::parameter::Parameter;
use crate::common::playbook::PlaybookRead;
use crate::common::reconcile::BookReconciled;
use crate::common::risk::TargetDecided;
use crate::common::standing::{FeedChanged, SessionClosed, SessionHalted};
use crate::common::storage::Key;
use crate::common::time::SessionDate;

/// Stamped on every record this build writes; a line under another version reads back unreadable.
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

/// A 40-character git sha and whether the tree that built it differed from it, written with a `-dirty` suffix if so.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct Commit {
    sha: String,
    tree: Tree,
}

/// Whether the working tree that built a commit matched it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Tree {
    Clean,
    Dirty,
}

/// Why a commit was refused.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CommitRefusal {
    #[error("`{raw}` is not a 40-character git sha")]
    Malformed { raw: String },
}

impl Commit {
    pub fn new(raw: &str) -> Result<Self, CommitRefusal> {
        let (sha, tree) = match raw.strip_suffix(DIRTY_SUFFIX) {
            Some(sha) => (sha, Tree::Dirty),
            None => (raw, Tree::Clean),
        };
        let hexadecimal = |byte: u8| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte);
        if sha.len() == 40 && sha.bytes().all(hexadecimal) {
            Ok(Self {
                sha: sha.to_string(),
                tree,
            })
        } else {
            Err(CommitRefusal::Malformed {
                raw: raw.to_string(),
            })
        }
    }

    pub fn is_dirty(&self) -> bool {
        match self.tree {
            Tree::Clean => false,
            Tree::Dirty => true,
        }
    }
}

const DIRTY_SUFFIX: &str = "-dirty";

impl std::fmt::Display for Commit {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let suffix = match self.tree {
            Tree::Clean => "",
            Tree::Dirty => DIRTY_SUFFIX,
        };
        write!(formatter, "{}{suffix}", self.sha)
    }
}

impl std::str::FromStr for Commit {
    type Err = CommitRefusal;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        Self::new(raw)
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
        commit.to_string()
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
    sequence: NonZeroU64,
    timestamp: DateTime<Utc>,
    /// Absent when the build could not ask git.
    commit: Option<Commit>,
    #[serde(flatten)]
    observation: Observation,
}

impl Record {
    pub fn new(
        run_id: RunId,
        sequence: NonZeroU64,
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

    pub fn schema_version(&self) -> u64 {
        self.schema_version
    }

    pub fn run_id(&self) -> RunId {
        self.run_id
    }

    pub fn sequence(&self) -> u64 {
        self.sequence.get()
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
    PartitionWritten(PartitionWritten),
    PartitionFailed(PartitionFailed),
    ConditionsWritten(ConditionsWritten),
    ObjectWritten(ObjectWritten),
    ObjectDeleted(ObjectDeleted),
    HealFinished(HealFinished),
    DatasetRead(Box<DatasetRead>),
    ExperimentRan(Box<ExperimentRan>),
    OrderSubmitted(OrderSubmitted),
    OrderClosed(OrderClosed),
    OrderRefused(OrderRefused),
    OrderUnresolved(OrderUnresolved),
    OrderGuarded(OrderGuarded),
    TradabilityRead(TradabilityRead),
    TradabilityUnread(TradabilityUnread),
    BookReconciled(BookReconciled),
    TargetDecided(TargetDecided),
    SessionOpened(SessionOpened),
    BarBuilt(BarBuilt),
    PlaybookRead(PlaybookRead),
    FeedChanged(FeedChanged),
    SessionHalted(SessionHalted),
    SessionClosed(SessionClosed),
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
    parameters: BTreeMap<Parameter, ResolvedParameter>,
}

impl ConfigurationResolved {
    pub fn new(parameters: BTreeMap<Parameter, ResolvedParameter>) -> Self {
        Self { parameters }
    }

    pub fn parameters(&self) -> &BTreeMap<Parameter, ResolvedParameter> {
        &self.parameters
    }
}

/// The book a trading session started from, as the broker reported it, and its worth at the previous session's closes,
/// from which the session's loss is measured.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionOpened {
    session: SessionDate,
    cash: Cash,
    positions: BTreeMap<Symbol, Position>,
    opening: Cash,
}

impl SessionOpened {
    pub fn new(session: SessionDate, book: &Book, opening: Cash) -> Self {
        Self {
            session,
            cash: book.cash(),
            positions: book.positions().clone(),
            opening,
        }
    }
}

/// One leg's session written to the archive and read back as `rows` rows, with every symbol and row that was refused.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionWritten {
    leg: Leg,
    session: SessionDate,
    rows: u64,
    refused: Tally<RowRefusalKind>,
    unanswered: BTreeMap<Symbol, Unanswered>,
    /// What a tick leg's fold did with each row it was offered; empty for a leg that folds nothing.
    folded: BTreeMap<FoldCount, u64>,
}

impl PartitionWritten {
    pub fn new(
        leg: Leg,
        session: SessionDate,
        rows: u64,
        refused: Tally<RowRefusalKind>,
        unanswered: BTreeMap<Symbol, Unanswered>,
        folded: BTreeMap<FoldCount, u64>,
    ) -> Self {
        Self {
            leg,
            session,
            rows,
            refused,
            unanswered,
            folded,
        }
    }

    pub fn leg(&self) -> Leg {
        self.leg
    }

    pub fn session(&self) -> SessionDate {
        self.session
    }

    pub fn rows(&self) -> u64 {
        self.rows
    }

    pub fn refused(&self) -> &Tally<RowRefusalKind> {
        &self.refused
    }

    pub fn unanswered(&self) -> &BTreeMap<Symbol, Unanswered> {
        &self.unanswered
    }

    pub fn folded(&self) -> &BTreeMap<FoldCount, u64> {
        &self.folded
    }
}

/// One count a tick leg's fold keeps, named as the fold's counts name it.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
    strum::Display,
    strum::EnumString,
    strum::IntoStaticStr,
    strum::EnumIter,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum FoldCount {
    Accepted,
    OutOfOrder,
    OneSided,
    Folded,
    OtherSession,
    Withdrawn,
    VolumeIneligible,
    UnsizedPrints,
    Unresolved,
    Late,
}

/// Every count of a quote fold, with the one-sided quotes left out before it.
pub fn quotes_folded(counts: QuoteFoldCounts, one_sided: u64) -> BTreeMap<FoldCount, u64> {
    BTreeMap::from([
        (FoldCount::Accepted, counts.accepted()),
        (FoldCount::OutOfOrder, counts.out_of_order()),
        (FoldCount::OneSided, one_sided),
    ])
}

/// Every count of a trade fold; the unresolved prints are counted whole, their causes left to the log.
pub fn trades_folded(counts: &TradeFoldCounts) -> BTreeMap<FoldCount, u64> {
    BTreeMap::from([
        (FoldCount::Folded, counts.folded()),
        (FoldCount::OtherSession, counts.other_session()),
        (FoldCount::Withdrawn, counts.withdrawn()),
        (FoldCount::VolumeIneligible, counts.volume_ineligible()),
        (FoldCount::UnsizedPrints, counts.unsized_prints()),
        (FoldCount::Unresolved, counts.unresolved().total()),
        (FoldCount::Late, counts.late()),
    ])
}

/// One owed partition of a leg left unwritten, recorded when it failed rather than when the run finished.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionFailed {
    leg: Leg,
    session: SessionDate,
    failure: PartitionFailureKind,
    cause: String,
}

impl PartitionFailed {
    pub fn new(
        leg: Leg,
        session: SessionDate,
        failure: PartitionFailureKind,
        cause: String,
    ) -> Self {
        Self {
            leg,
            session,
            failure,
            cause,
        }
    }
}

/// A conditions snapshot fetched and written because none in the archive read, which the trade leg folds under.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConditionsWritten {
    as_of: SessionDate,
    conditions: u64,
}

impl ConditionsWritten {
    pub fn new(as_of: SessionDate, conditions: u64) -> Self {
        Self { as_of, conditions }
    }
}

/// One archive object an operator command wrote, and its size.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectWritten {
    key: Key,
    bytes: u64,
}

impl ObjectWritten {
    pub fn new(key: Key, bytes: u64) -> Self {
        Self { key, bytes }
    }
}

/// One archive object an operator command deleted.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectDeleted {
    key: Key,
}

impl ObjectDeleted {
    pub fn new(key: Key) -> Self {
        Self { key }
    }
}

/// Why a symbol asked for returned no rows.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Unanswered {
    /// Left out of the answer without a word, as Alpaca does with a name it does not know.
    Missing,
    /// Named invalid by the vendor and dropped from the request.
    Invalid,
}

/// A run's heal: the window it covered, how each owed session of each leg ended, and every path under a leg's
/// series that was not one of its keys. A session of the window with no outcome was already held.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HealFinished {
    window: Window,
    outcomes: BTreeMap<Leg, BTreeMap<SessionDate, SessionOutcome>>,
    unrecognized: BTreeMap<Leg, BTreeMap<String, Unrecognized>>,
}

impl HealFinished {
    pub fn new(
        window: Window,
        outcomes: BTreeMap<Leg, BTreeMap<SessionDate, SessionOutcome>>,
        unrecognized: BTreeMap<Leg, BTreeMap<String, Unrecognized>>,
    ) -> Self {
        Self {
            window,
            outcomes,
            unrecognized,
        }
    }

    pub fn window(&self) -> &Window {
        &self.window
    }

    pub fn outcomes(&self) -> &BTreeMap<Leg, BTreeMap<SessionDate, SessionOutcome>> {
        &self.outcomes
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
    /// `text` is the line as written, kept so an unsupported record can be preserved or migrated.
    Unreadable {
        line: usize,
        text: String,
        cause: UnreadableCause,
    },
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

/// Reads every line of a journal file in the order it was written, checking each line's version first. The writer
/// never emits a blank line, so one reads back as unreadable rather than being skipped.
pub fn read(contents: &str) -> Vec<ReadLine> {
    contents
        .lines()
        .enumerate()
        .map(|(index, text)| read_one(index + 1, text))
        .collect()
}

/// Line `line` of a journal file, as `read` would give it.
pub fn read_one(line: usize, text: &str) -> ReadLine {
    match read_line(text) {
        Ok(record) => ReadLine::Read(Box::new(record)),
        Err(cause) => ReadLine::Unreadable {
            line,
            text: text.to_string(),
            cause,
        },
    }
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

/// `held` with every line of `adding` it lacks appended, so two writers shipping one session's object both keep
/// their records. A record is the same record by run and sequence; an unreadable line by its text.
pub fn merge(held: Vec<ReadLine>, adding: Vec<ReadLine>) -> Vec<ReadLine> {
    let identity = |line: &ReadLine| match line {
        ReadLine::Read(record) => (Some((record.run_id(), record.sequence())), None),
        ReadLine::Unreadable { text, .. } => (None, Some(text.clone())),
    };
    let mut seen: std::collections::BTreeSet<_> = held.iter().map(identity).collect();
    let mut merged = held;
    merged.extend(
        adding
            .into_iter()
            .filter(|line| seen.insert(identity(line))),
    );
    merged
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;
    use strum::IntoEnumIterator;

    use super::*;
    use crate::common::market::aggregate::TradeTotals;
    use crate::common::market::record::BarInterval;
    use crate::common::market::trade_bars::{HighLow, OpenClose, TradeBar, TradeSums};
    use crate::common::market::{DollarVolume, Price, Shares, StampedPrice, TradeCount};
    use crate::common::monoid::concatenate;

    const SHA: &str = "0123456789abcdef0123456789abcdef01234567";

    fn record(commit: Option<Commit>) -> Record {
        let parameters = BTreeMap::from([
            (
                Parameter::BudgetMinutes,
                ResolvedParameter::new("240".to_string(), ParameterSource::Default),
            ),
            (
                Parameter::LookbackSessions,
                ResolvedParameter::new("20".to_string(), ParameterSource::Environment),
            ),
        ]);
        Record::new(
            RunId::new(Uuid::from_u128(1)),
            NonZeroU64::MIN,
            "2026-07-31T14:30:00Z".parse().unwrap(),
            commit,
            Observation::ConfigurationResolved(ConfigurationResolved::new(parameters)),
        )
    }

    /// A line for run `run` at `sequence`, or an unreadable line holding `text` when `run` is zero.
    fn line(run: u128, sequence: u64) -> ReadLine {
        match run {
            0 => read_one(1, &format!("torn {sequence}")),
            _ => ReadLine::Read(Box::new(Record::new(
                RunId::new(Uuid::from_u128(run)),
                NonZeroU64::new(sequence).unwrap(),
                "2026-07-31T14:30:00Z".parse().unwrap(),
                None,
                Observation::ConfigurationResolved(ConfigurationResolved::new(BTreeMap::new())),
            ))),
        }
    }

    #[test]
    fn test_a_merge_keeps_both_writers_records_once() {
        let held = vec![line(1, 1), line(1, 2), line(0, 7)];
        let adding = vec![line(2, 1), line(1, 2), line(0, 7), line(0, 8)];
        assert_eq!(
            merge(held, adding),
            [line(1, 1), line(1, 2), line(0, 7), line(2, 1), line(0, 8)]
        );
    }

    #[test]
    fn test_a_record_encodes_to_its_wire_format() {
        assert_eq!(
            record(Some(Commit::new(SHA).unwrap())).encode(),
            concat!(
                r#"{"schema_version":1,"run_id":"00000000-0000-0000-0000-000000000001","sequence":1,"#,
                r#""timestamp":"2026-07-31T14:30:00Z","commit":"0123456789abcdef0123456789abcdef01234567","#,
                r#""event_type":"configuration_resolved","payload":{"parameters":{"#,
                r#""lookback_sessions":{"value":"20","source":"environment"},"#,
                r#""budget_minutes":{"value":"240","source":"default"}}}}"#,
            )
        );
    }

    #[test]
    fn test_the_heal_records_encode_to_their_wire_format() {
        let session = SessionDate::from_date(chrono::NaiveDate::from_ymd_opt(2026, 9, 29).unwrap());
        let envelope = |observation| {
            Record::new(
                RunId::new(Uuid::from_u128(1)),
                NonZeroU64::MIN,
                "2026-09-30T07:00:00Z".parse().unwrap(),
                None,
                observation,
            )
            .encode()
        };
        let written = PartitionWritten::new(
            Leg::AlpacaMinuteBars,
            session,
            1_872_987,
            concatenate(
                [
                    RowRefusalKind::Duplicate,
                    RowRefusalKind::DollarVolume,
                    RowRefusalKind::Duplicate,
                ]
                .map(Tally::of),
            ),
            BTreeMap::from([
                (Symbol::new("ABC").unwrap(), Unanswered::Missing),
                (Symbol::new("BC.PRC").unwrap(), Unanswered::Invalid),
            ]),
            quotes_folded(QuoteFoldCounts::default(), 3),
        );
        let failed = PartitionFailed::new(
            Leg::AlpacaTrades,
            session,
            PartitionFailureKind::NoRows,
            "the vendor answered with no rows".to_string(),
        );
        let conditions = ConditionsWritten::new(session, 70);
        let raw_trades = Key::RawTrades {
            provider: crate::common::storage::Provider::Massive,
            session,
        };
        let object_written = ObjectWritten::new(raw_trades.clone(), 4_096);
        let object_deleted = ObjectDeleted::new(raw_trades);
        let finished = HealFinished::new(
            Window::new(vec![session]).unwrap(),
            BTreeMap::from([(
                Leg::MassiveDailyBars,
                BTreeMap::from([
                    (
                        session,
                        SessionOutcome::Failed {
                            failure: PartitionFailureKind::Fetch,
                            cause: "refused with 403".to_string(),
                        },
                    ),
                    (session.plus_calendar_days(-1), SessionOutcome::Unreached),
                ]),
            )]),
            BTreeMap::from([(
                Leg::AlpacaQuotes,
                BTreeMap::from([("data/stray.parquet".to_string(), Unrecognized::NotAKey)]),
            )]),
        );
        let prefix = concat!(
            r#"{"schema_version":1,"run_id":"00000000-0000-0000-0000-000000000001","sequence":1,"#,
            r#""timestamp":"2026-09-30T07:00:00Z","commit":null,"#,
        );
        assert_eq!(
            envelope(Observation::PartitionWritten(written)),
            format!(
                "{prefix}{}",
                concat!(
                    r#""event_type":"partition_written","payload":{"leg":"alpaca_minute_bars","#,
                    r#""session":"2026-09-29","rows":1872987,"#,
                    r#""refused":{"dollar_volume":1,"duplicate":2},"#,
                    r#""unanswered":{"ABC":"missing","BC.PRC":"invalid"},"#,
                    r#""folded":{"accepted":0,"out_of_order":0,"one_sided":3}}}"#,
                )
            )
        );
        assert_eq!(
            envelope(Observation::PartitionFailed(failed)),
            format!(
                "{prefix}{}",
                concat!(
                    r#""event_type":"partition_failed","payload":{"leg":"alpaca_trades","#,
                    r#""session":"2026-09-29","failure":"no_rows","#,
                    r#""cause":"the vendor answered with no rows"}}"#,
                )
            )
        );
        assert_eq!(
            envelope(Observation::ConditionsWritten(conditions)),
            format!(
                "{prefix}{}",
                concat!(
                    r#""event_type":"conditions_written","#,
                    r#""payload":{"as_of":"2026-09-29","conditions":70}}"#,
                )
            )
        );
        let raw_path =
            "data/equity/stage=raw/trades/provider=massive/year=2026/month=09/day=29/data.csv.gz";
        assert_eq!(
            envelope(Observation::ObjectWritten(object_written)),
            format!(
                r#"{prefix}"event_type":"object_written","payload":{{"key":"{raw_path}","bytes":4096}}}}"#
            )
        );
        assert_eq!(
            envelope(Observation::ObjectDeleted(object_deleted)),
            format!(r#"{prefix}"event_type":"object_deleted","payload":{{"key":"{raw_path}"}}}}"#)
        );
        assert_eq!(
            envelope(Observation::HealFinished(finished)),
            format!(
                "{prefix}{}",
                concat!(
                    r#""event_type":"heal_finished","payload":{"window":["2026-09-29"],"#,
                    r#""outcomes":{"massive_daily_bars":{"2026-09-28":{"outcome":"unreached"},"#,
                    r#""2026-09-29":{"outcome":"failed","failure":"fetch","#,
                    r#""cause":"refused with 403"}}},"#,
                    r#""unrecognized":{"alpaca_quotes":{"data/stray.parquet":"not_a_key"}}}}"#,
                )
            )
        );
    }

    #[test]
    fn test_serde_and_strum_agree_on_every_fold_count() {
        let names: Vec<String> = FoldCount::iter()
            .map(|count| {
                let json = serde_json::to_string(&count).unwrap();
                assert_eq!(json, format!("\"{count}\""));
                assert_eq!(count.to_string().parse::<FoldCount>(), Ok(count));
                assert_eq!(serde_json::from_str::<FoldCount>(&json).unwrap(), count);
                json
            })
            .collect();
        assert_eq!(names.len(), 10);
        assert_eq!(
            names[..3],
            [r#""accepted""#, r#""out_of_order""#, r#""one_sided""#]
        );
    }

    /// Each count reaches the journal under its own name: one print unresolved, two withdrawn and three of another
    /// session, so a swapped field changes the map.
    #[test]
    fn test_a_trade_fold_journals_every_count_under_its_name() {
        use crate::common::market::record::Trade;
        use crate::common::market::trade_bars::{
            ConditionCode, Correction, Print, TradeConditions, TradeFold,
        };
        let session = SessionDate::from_date(chrono::NaiveDate::from_ymd_opt(2026, 10, 2).unwrap());
        let print = |at: &str| {
            Print::Trade(
                Trade::new(
                    Symbol::new("AAPL").unwrap(),
                    at.parse().unwrap(),
                    Price::from_dollars(100.0).unwrap(),
                    Shares::whole(10).unwrap(),
                )
                .unwrap(),
            )
        };
        let mut fold = TradeFold::new(session, TradeConditions::new(BTreeMap::new()));
        fold.push(
            &print("2026-10-02T13:30:00Z"),
            &[ConditionCode::new(99)],
            Correction::Stands,
        );
        for _ in 0..2 {
            fold.push(&print("2026-10-02T13:31:00Z"), &[], Correction::Withdrawn);
        }
        for _ in 0..3 {
            fold.push(&print("2026-10-03T13:30:00Z"), &[], Correction::Stands);
        }
        let (_, counts) = fold.finish();
        let folded: Vec<(&'static str, u64)> = trades_folded(&counts)
            .into_iter()
            .map(|(count, value)| (count.into(), value))
            .collect();
        assert_eq!(
            folded,
            [
                ("folded", 1),
                ("other_session", 3),
                ("withdrawn", 2),
                ("volume_ineligible", 0),
                ("unsized_prints", 0),
                ("unresolved", 1),
                ("late", 0),
            ]
        );
    }

    /// The order records as the trader writes them, pinned so a rename shows up as a changed wire format.
    #[test]
    fn test_the_order_records_encode_to_their_wire_format() {
        use crate::common::book::{Book, Cash, Position, ValuationRefusal};
        use crate::common::guard::{Tradability, TradabilityRead, TradabilityUnread, guard};
        use crate::common::market::{Price, Shares};
        use crate::common::order::{
            BrokerFailure, ClientOrderId, OrderClosed, OrderEnding, OrderExecution, OrderRefused,
            OrderReport, OrderRequest, OrderState, OrderStatus, OrderUnresolved, UnresolvedCause,
        };
        use crate::common::reconcile::{reconcile, rounding_allowance};
        use crate::common::risk::{Limits, TargetDecided, risk};
        use crate::common::strategy::{Target, orders};
        use crate::common::time::calendar::SessionPhase;

        let id = ClientOrderId::new(RunId::new(Uuid::from_u128(2)), 7);
        let target = Target::new(BTreeMap::from([(
            Symbol::new("SPY").unwrap(),
            Shares::whole(5).unwrap(),
        )]));
        let order = orders(&Book::default(), &target).remove(0);
        let closed = OrderState::submitted()
            .observe(
                &order,
                OrderReport::new(
                    OrderStatus::Closed(OrderEnding::Canceled),
                    OrderExecution::new(
                        Shares::whole(3).unwrap(),
                        Price::from_ticks(12_500_000).unwrap(),
                    ),
                    "2026-10-06T14:00:00Z".parse().unwrap(),
                ),
            )
            .unwrap();
        let fraction = Target::new(BTreeMap::from([(
            Symbol::new("VWDRY").unwrap(),
            Shares::from_units(1_500_000),
        )]));
        let whole_only =
            BTreeMap::from([(Symbol::new("VWDRY").unwrap(), Tradability::WholeSharesOnly)]);
        let guarded =
            guard(orders(&Book::default(), &fraction), &whole_only, |_| None).held()[0].clone();
        let sliver = Target::new(BTreeMap::from([(
            Symbol::new("DIA").unwrap(),
            Shares::from_units(19),
        )]));
        let fractionable =
            BTreeMap::from([(Symbol::new("DIA").unwrap(), Tradability::Fractionable)]);
        let rolling = crate::common::playbook::Playbook::parse(
            "roll_off_minutes = 10\n\n[[entries]]\nfrom = \"09:30\"\nuntil = \"12:00\"\nstrategy = { kind = \"flat\" }\nnote = \"n\"\n\n[[entries]]\nfrom = \"12:00\"\nuntil = \"16:00\"\nstrategy = { kind = \"flat\" }\nnote = \"n\"\n",
        )
        .unwrap()
        .play(&std::collections::BTreeSet::new())
        .stretch_at(&crate::common::market::state::MarketState::of(
            crate::common::market::state::MarketEvent::Clock(
                "2026-10-07T16:05:00Z".parse().unwrap(),
            ),
        ));
        let below = guard(orders(&Book::default(), &sliver), &fractionable, |_| {
            Price::from_ticks(470_000_000).ok()
        })
        .held()[0]
            .clone();
        let observations = [
            Observation::OrderSubmitted(OrderSubmitted::of(&OrderRequest::new(order, id))),
            Observation::OrderClosed(OrderClosed::of(id, closed).unwrap()),
            Observation::OrderRefused(OrderRefused::new(
                id,
                403,
                "insufficient buying power".to_string(),
            )),
            Observation::OrderUnresolved(OrderUnresolved::new(
                id,
                UnresolvedCause::OpenPastCancel {
                    reads: 20,
                    last: None,
                },
                OrderExecution::new(
                    Shares::from_units(500_000),
                    Price::from_ticks(12_400_000).unwrap(),
                ),
            )),
            Observation::OrderGuarded(guarded),
            Observation::OrderGuarded(below),
            Observation::TradabilityRead(TradabilityRead::new(whole_only.clone())),
            Observation::TradabilityUnread(TradabilityUnread::new(BrokerFailure::Exhausted {
                attempts: 3,
                last: "timed out".to_string(),
            })),
            Observation::BookReconciled(reconcile(
                &Book::reported(Cash::from_units(1_000), []),
                &Book::reported(
                    Cash::from_units(-5),
                    [(
                        Symbol::new("SPY").unwrap(),
                        Position::from_units(-2_000_000),
                    )],
                ),
                rounding_allowance(&[]),
            )),
            Observation::TargetDecided(TargetDecided::new(
                "2026-10-07T14:05:00Z".parse().unwrap(),
                rolling,
                target.clone(),
                risk(
                    &Limits::new(
                        Cash::from_units(1),
                        Cash::from_units(1),
                        Cash::from_units(1),
                        chrono::TimeDelta::zero(),
                    )
                    .unwrap(),
                    SessionPhase::BeforeOpen {
                        until_open: chrono::TimeDelta::minutes(5),
                    },
                    Cash::from_units(0),
                    &Book::default(),
                    |_| None,
                    target.clone(),
                ),
            )),
            Observation::TargetDecided(TargetDecided::new(
                "2026-10-07T14:05:00Z".parse().unwrap(),
                None,
                target.clone(),
                Err(ValuationRefusal::Unpriced {
                    symbol: Symbol::new("SPY").unwrap(),
                }),
            )),
            Observation::SessionOpened(SessionOpened::new(
                SessionDate::from_date(chrono::NaiveDate::from_ymd_opt(2026, 10, 7).unwrap()),
                &Book::reported(
                    Cash::from_units(-7),
                    [(Symbol::new("SPY").unwrap(), Position::from_units(2_000_000))],
                ),
                Cash::from_units(9),
            )),
            Observation::PlaybookRead(crate::common::playbook::PlaybookRead::new(
                "roll_off_minutes = 5\n".to_string(),
            )),
        ];
        let payloads: Vec<String> = observations
            .iter()
            .map(|observation| serde_json::to_string(observation).unwrap())
            .collect();
        let id = r#""client_order_id":"fund:00000000-0000-0000-0000-000000000002:7""#;
        assert_eq!(
            payloads,
            [
                format!(
                    r#"{{"event_type":"order_submitted","payload":{{{id},"symbol":"SPY","side":"buy","shares":5000000}}}}"#
                ),
                format!(
                    r#"{{"event_type":"order_closed","payload":{{{id},"ending":"canceled","executed":{{"shares":3000000,"average_price":12500000}},"closed_at":"2026-10-06T14:00:00Z"}}}}"#
                ),
                format!(
                    r#"{{"event_type":"order_refused","payload":{{{id},"status":403,"body":"insufficient buying power"}}}}"#
                ),
                format!(
                    r#"{{"event_type":"order_unresolved","payload":{{{id},"cause":{{"open_past_cancel":{{"reads":20,"last":null}}}},"executed":{{"shares":500000,"average_price":12400000}}}}}}"#
                ),
                r#"{"event_type":"order_guarded","payload":{"symbol":"VWDRY","side":"buy","shares":1500000,"cause":"fractional"}}"#.to_string(),
                r#"{"event_type":"order_guarded","payload":{"symbol":"DIA","side":"buy","shares":19,"cause":{"below_minimum":{"price":470000000}}}}"#.to_string(),
                r#"{"event_type":"tradability_read","payload":{"readings":{"VWDRY":"whole_shares_only"}}}"#.to_string(),
                r#"{"event_type":"tradability_unread","payload":{"cause":{"exhausted":{"attempts":3,"last":"timed out"}}}}"#.to_string(),
                r#"{"event_type":"book_reconciled","payload":{"expected_cash":"1000","reported_cash":"-5","allowance":"0","gaps":[{"symbol":"SPY","expected":"0","reported":"-2000000"}]}}"#.to_string(),
                r#"{"event_type":"target_decided","payload":{"bar":"2026-10-07T14:05:00Z","stretch":{"from":"12:00:00","progress":500000},"wanted":{"SPY":5000000},"restrained":{"target":{},"cuts":[{"outside_trading_window":{"phase":{"before_open":{"until_open":300000000000}}}}]}}}"#.to_string(),
                r#"{"event_type":"target_decided","payload":{"bar":"2026-10-07T14:05:00Z","stretch":null,"wanted":{"SPY":5000000},"refused":{"unpriced":{"symbol":"SPY"}}}}"#.to_string(),
                r#"{"event_type":"session_opened","payload":{"session":"2026-10-07","cash":"-7","positions":{"SPY":"2000000"},"opening":"9"}}"#.to_string(),
                r#"{"event_type":"playbook_read","payload":{"contents":"roll_off_minutes = 5\n"}}"#.to_string(),
            ]
        );
        for (observation, payload) in observations.iter().zip(&payloads) {
            assert_eq!(
                &serde_json::from_str::<Observation>(payload).unwrap(),
                observation
            );
        }
        let before_stretches = r#"{"event_type":"target_decided","payload":{"bar":"2026-10-07T14:05:00Z","wanted":{"SPY":5000000},"refused":{"unpriced":{"symbol":"SPY"}}}}"#;
        assert_eq!(
            &serde_json::from_str::<Observation>(before_stretches).unwrap(),
            &observations[10]
        );
    }

    /// Every cause an unresolved order or an unread tradability journals, pinned so a rename shows up as a changed
    /// wire format.
    #[test]
    fn test_the_order_causes_encode_to_their_wire_format() {
        use crate::common::guard::TradabilityUnread;
        use crate::common::market::{Price, Shares, SymbolRefusal};
        use crate::common::order::{
            BrokerFailure, ClientOrderId, OrderEnding, OrderExecution, OrderRefusal, OrderReport,
            OrderStatus, OrderTrouble, OrderUnresolved, UnresolvedCause,
        };

        let id = ClientOrderId::new(RunId::new(Uuid::from_u128(2)), 7);
        let shares = |count| Shares::whole(count).unwrap();
        let failures = [
            BrokerFailure::NotPaper,
            BrokerFailure::Unanswered {
                cause: "status 503".to_string(),
            },
            BrokerFailure::Refused {
                status: 403,
                body: "forbidden".to_string(),
            },
            BrokerFailure::Exhausted {
                attempts: 3,
                last: "timed out".to_string(),
            },
            BrokerFailure::Unparsed {
                reason: "EOF".to_string(),
            },
            BrokerFailure::Malformed {
                field: "qty".to_string(),
                raw: "1e3".to_string(),
            },
            BrokerFailure::Symbol(SymbolRefusal::Malformed {
                raw: "spy".to_string(),
            }),
            BrokerFailure::UnmappedStatus {
                status: "replaced".to_string(),
            },
        ];
        let refusals = [
            OrderRefusal::ExecutionShrank {
                held: shares(2),
                reported: shares(1),
            },
            OrderRefusal::Overfilled {
                ordered: shares(1),
                reported: shares(2),
            },
            OrderRefusal::FilledShort {
                ordered: shares(2),
                reported: shares(1),
            },
            OrderRefusal::ChangedAfterClosing {
                ending: OrderEnding::Canceled,
                executed: None,
                report: OrderReport::new(
                    OrderStatus::Closed(OrderEnding::Filled),
                    OrderExecution::new(shares(1), Price::from_ticks(1_000_000).unwrap()),
                    "2026-10-06T14:00:00Z".parse().unwrap(),
                ),
            },
        ];
        let causes =
            [
                UnresolvedCause::SubmittedThenUnreadable {
                    failure: BrokerFailure::NotPaper,
                },
                UnresolvedCause::OpenPastCancel {
                    reads: 20,
                    last: Some(OrderTrouble::CancelFailed(BrokerFailure::NotPaper)),
                },
                UnresolvedCause::OpenPastCancel {
                    reads: 20,
                    last: Some(OrderTrouble::Unreadable(BrokerFailure::NotPaper)),
                },
            ]
            .into_iter()
            .chain(refusals.into_iter().map(|refusal| {
                UnresolvedCause::OpenPastCancel {
                    reads: 20,
                    last: Some(OrderTrouble::ReportRefused(refusal)),
                }
            }));
        let observations: Vec<Observation> =
            failures
                .into_iter()
                .map(|failure| Observation::TradabilityUnread(TradabilityUnread::new(failure)))
                .chain(causes.map(|cause| {
                    Observation::OrderUnresolved(OrderUnresolved::new(id, cause, None))
                }))
                .collect();
        let payloads: Vec<String> = observations
            .iter()
            .map(|observation| serde_json::to_string(observation).unwrap())
            .collect();
        let unread = |cause: &str| {
            format!(r#"{{"event_type":"tradability_unread","payload":{{"cause":{cause}}}}}"#)
        };
        let unresolved = |cause: &str| {
            format!(
                r#"{{"event_type":"order_unresolved","payload":{{"client_order_id":"fund:00000000-0000-0000-0000-000000000002:7","cause":{cause},"executed":null}}}}"#
            )
        };
        let past_cancel = |last: &str| {
            unresolved(&format!(
                r#"{{"open_past_cancel":{{"reads":20,"last":{last}}}}}"#
            ))
        };
        assert_eq!(
            payloads,
            [
                unread(r#""not_paper""#),
                unread(r#"{"unanswered":{"cause":"status 503"}}"#),
                unread(r#"{"refused":{"status":403,"body":"forbidden"}}"#),
                unread(r#"{"exhausted":{"attempts":3,"last":"timed out"}}"#),
                unread(r#"{"unparsed":{"reason":"EOF"}}"#),
                unread(r#"{"malformed":{"field":"qty","raw":"1e3"}}"#),
                unread(r#"{"symbol":{"malformed":{"raw":"spy"}}}"#),
                unread(r#"{"unmapped_status":{"status":"replaced"}}"#),
                unresolved(r#"{"submitted_then_unreadable":{"failure":"not_paper"}}"#),
                past_cancel(r#"{"cancel_failed":"not_paper"}"#),
                past_cancel(r#"{"unreadable":"not_paper"}"#),
                past_cancel(
                    r#"{"report_refused":{"execution_shrank":{"held":2000000,"reported":1000000}}}"#
                ),
                past_cancel(
                    r#"{"report_refused":{"overfilled":{"ordered":1000000,"reported":2000000}}}"#
                ),
                past_cancel(
                    r#"{"report_refused":{"filled_short":{"ordered":2000000,"reported":1000000}}}"#
                ),
                past_cancel(concat!(
                    r#"{"report_refused":{"changed_after_closing":{"ending":"canceled","executed":null,"#,
                    r#""report":{"status":{"closed":"filled"},"executed":{"shares":1000000,"average_price":1000000},"#,
                    r#""at":"2026-10-06T14:00:00Z"}}}}"#
                )),
            ]
        );
        for (observation, payload) in observations.iter().zip(&payloads) {
            assert_eq!(
                &serde_json::from_str::<Observation>(payload).unwrap(),
                observation
            );
        }
    }

    /// Every record of a session's standing, each cause and ending among them, pinned so a rename shows up as a changed
    /// wire format.
    #[test]
    fn test_the_standing_records_encode_to_their_wire_format() {
        use crate::common::order::{BrokerFailure, ClientOrderId};
        use crate::common::standing::{
            FeedChanged, FeedContinuity, FeedTransition, HaltCause, SessionClosed, SessionEnding,
            SessionHalted,
        };

        let changes = [
            (FeedContinuity::Whole, FeedTransition::Lost),
            (
                FeedContinuity::Interrupted,
                FeedTransition::Reopened { attempts: 2 },
            ),
            (
                FeedContinuity::Reopened,
                FeedTransition::Backfilled {
                    since: "2026-10-07T14:04:00Z".parse().unwrap(),
                    fresh: 3,
                },
            ),
        ]
        .map(|(from, transition)| FeedChanged::of(from, transition).unwrap());
        let causes = [
            HaltCause::JournalRefused {
                error: "disk full".to_string(),
            },
            HaltCause::ReconcileUnread(BrokerFailure::NotPaper),
            HaltCause::Diverged,
            HaltCause::Unresolved {
                client_order_id: ClientOrderId::new(RunId::new(Uuid::from_u128(2)), 7),
            },
        ];
        let endings = [
            SessionEnding::NoSession,
            SessionEnding::ToTheClose,
            SessionEnding::HeldAtTheClose { positions: 2 },
            SessionEnding::Halted,
            SessionEnding::StoppedBeforeTheOpen,
            SessionEnding::StoppedTrading,
        ];
        let session = SessionDate::from_date(chrono::NaiveDate::from_ymd_opt(2026, 10, 7).unwrap());
        let observations: Vec<Observation> = changes
            .into_iter()
            .map(Observation::FeedChanged)
            .chain(
                causes
                    .into_iter()
                    .map(|cause| Observation::SessionHalted(SessionHalted::new(cause))),
            )
            .chain(
                endings
                    .into_iter()
                    .map(|ending| Observation::SessionClosed(SessionClosed::new(session, ending))),
            )
            .collect();
        let payloads: Vec<String> = observations
            .iter()
            .map(|observation| serde_json::to_string(observation).unwrap())
            .collect();
        let changed =
            |payload: &str| format!(r#"{{"event_type":"feed_changed","payload":{payload}}}"#);
        let halted = |cause: &str| {
            format!(r#"{{"event_type":"session_halted","payload":{{"cause":{cause}}}}}"#)
        };
        let closed = |ending: &str| {
            format!(
                r#"{{"event_type":"session_closed","payload":{{"session":"2026-10-07","ending":{ending}}}}}"#
            )
        };
        assert_eq!(
            payloads,
            [
                changed(r#"{"from":"whole","transition":"lost"}"#),
                changed(r#"{"from":"interrupted","transition":{"reopened":{"attempts":2}}}"#),
                changed(
                    r#"{"from":"reopened","transition":{"backfilled":{"since":"2026-10-07T14:04:00Z","fresh":3}}}"#
                ),
                halted(r#"{"journal_refused":{"error":"disk full"}}"#),
                halted(r#"{"reconcile_unread":"not_paper"}"#),
                halted(r#""diverged""#),
                halted(
                    r#"{"unresolved":{"client_order_id":"fund:00000000-0000-0000-0000-000000000002:7"}}"#
                ),
                closed(r#""no_session""#),
                closed(r#""to_the_close""#),
                closed(r#"{"held_at_the_close":{"positions":2}}"#),
                closed(r#""halted""#),
                closed(r#""stopped_before_the_open""#),
                closed(r#""stopped_trading""#),
            ]
        );
        for (observation, payload) in observations.iter().zip(&payloads) {
            assert_eq!(
                &serde_json::from_str::<Observation>(payload).unwrap(),
                observation
            );
        }
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
    fn test_a_commit_writes_back_the_text_it_was_read_from() {
        let clean = Commit::new("0123456789abcdef0123456789abcdef01234567").unwrap();
        let dirty = Commit::new("0123456789abcdef0123456789abcdef01234567-dirty").unwrap();
        assert_eq!(
            serde_json::to_string(&clean).unwrap(),
            r#""0123456789abcdef0123456789abcdef01234567""#
        );
        assert_eq!(
            serde_json::to_string(&dirty).unwrap(),
            r#""0123456789abcdef0123456789abcdef01234567-dirty""#
        );
        assert_eq!(
            String::from(dirty),
            "0123456789abcdef0123456789abcdef01234567-dirty"
        );
    }

    proptest! {
        #[test]
        fn test_a_commit_round_trips_through_its_text(
            sha in "[0-9a-f]{40}",
            dirty in any::<bool>(),
        ) {
            let raw = if dirty { format!("{sha}-dirty") } else { sha };
            let commit = Commit::new(&raw).unwrap();
            prop_assert_eq!(commit.is_dirty(), dirty);
            prop_assert_eq!(commit.to_string(), raw.clone());
            let serialized = serde_json::to_string(&commit).unwrap();
            prop_assert_eq!(&serialized, &format!("\"{raw}\""));
            prop_assert_eq!(serde_json::from_str::<Commit>(&serialized).unwrap(), commit);
        }
    }

    #[test]
    fn test_a_record_is_filed_under_its_eastern_session() {
        let late = Record::new(
            RunId::new(Uuid::from_u128(1)),
            NonZeroU64::new(2).unwrap(),
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
        let unreadable = [
            "",
            "not json",
            r#"{"sequence":1}"#,
            r#"{"schema_version":2,"event_type":"configuration_resolved"}"#,
            malformed.as_str(),
        ];
        let contents = [&[good.as_str()][..], &unreadable].concat().join("\n");
        let lines = read(&contents);
        assert_eq!(lines.len(), 6);
        assert_eq!(lines[0], ReadLine::Read(Box::new(record(None))));
        // Parser messages are serde's, so the reasons are blanked before comparing.
        let found: Vec<(usize, String, UnreadableCause)> = lines[1..]
            .iter()
            .map(|line| match line {
                ReadLine::Unreadable { line, text, cause } => {
                    let cause = match cause.clone() {
                        UnreadableCause::NotJson { .. } => UnreadableCause::NotJson {
                            reason: String::new(),
                        },
                        UnreadableCause::Malformed { event_type, .. } => {
                            UnreadableCause::Malformed {
                                event_type,
                                reason: String::new(),
                            }
                        }
                        cause @ (UnreadableCause::NoVersion
                        | UnreadableCause::OtherVersion { .. }) => cause,
                    };
                    (*line, text.clone(), cause)
                }
                ReadLine::Read(record) => panic!("read {record:?}"),
            })
            .collect();
        let not_json = UnreadableCause::NotJson {
            reason: String::new(),
        };
        let causes = [
            not_json.clone(),
            not_json,
            UnreadableCause::NoVersion,
            UnreadableCause::OtherVersion { schema_version: 2 },
            UnreadableCause::Malformed {
                event_type: Some("configuration_guessed".to_string()),
                reason: String::new(),
            },
        ];
        let expected: Vec<(usize, String, UnreadableCause)> = unreadable
            .iter()
            .zip(causes)
            .enumerate()
            .map(|(index, (text, cause))| (index + 2, text.to_string(), cause))
            .collect();
        assert_eq!(found, expected);
    }

    #[test]
    fn test_a_zero_sequence_is_refused_on_read() {
        let contents = record(None)
            .encode()
            .replace(r#""sequence":1"#, r#""sequence":0"#);
        assert!(matches!(
            read(&contents).as_slice(),
            [ReadLine::Unreadable {
                line: 1,
                cause: UnreadableCause::Malformed { .. },
                ..
            }]
        ));
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
                cause: UnreadableCause::Malformed { .. },
                ..
            }]
        ));
    }

    fn any_session() -> impl Strategy<Value = SessionDate> {
        (0_i64..40_000).prop_map(|days| {
            SessionDate::from_date(chrono::NaiveDate::from_ymd_opt(1990, 1, 1).unwrap())
                .plus_calendar_days(days)
        })
    }

    /// A one-minute bar with each price pair present or absent, the open at or before the close.
    fn any_bar_built() -> impl Strategy<Value = BarBuilt> {
        (
            0_i64..1_440,
            (any::<u64>(), any::<u64>(), any::<u128>()),
            prop::option::of((
                0_i64..60_000_000_000,
                0_i64..60_000_000_000,
                1_i64..2_000_000,
                1_i64..2_000_000,
            )),
            prop::option::of((1_i64..2_000_000, 1_i64..2_000_000)),
        )
            .prop_map(|(minute, (count, volume, dollars), open_close, high_low)| {
                let start = DateTime::from_timestamp(1_791_000_000 + minute * 60, 0).unwrap();
                let price = |ticks| Price::from_ticks(ticks).unwrap();
                let open_close = open_close.map(|(first, second, open, close)| {
                    let at = |nanoseconds| start + chrono::TimeDelta::nanoseconds(nanoseconds);
                    let (open, close) = (
                        StampedPrice::new(at(first), price(open)),
                        StampedPrice::new(at(second), price(close)),
                    );
                    OpenClose::new(open.min(close), open.max(close)).unwrap()
                });
                let high_low = high_low.map(|(first, second)| {
                    HighLow::new(price(first.max(second)), price(first.min(second))).unwrap()
                });
                let totals = TradeTotals::new(
                    TradeCount::new(count),
                    Shares::from_units(volume),
                    DollarVolume::from_units(dollars),
                );
                BarBuilt::of(
                    &TradeBar::new(
                        Symbol::new("AAPL").unwrap(),
                        BarInterval::OneMinute,
                        start,
                        TradeSums::new(totals, open_close, high_low),
                    )
                    .unwrap(),
                )
            })
    }

    fn any_observation() -> impl Strategy<Value = Observation> {
        let parameter = prop::sample::select(Parameter::iter().collect::<Vec<_>>());
        let source = prop::sample::select(ParameterSource::iter().collect::<Vec<_>>());
        let leg = prop::sample::select(Leg::iter().collect::<Vec<_>>());
        let symbol = "[A-Z]{1,5}(\\.[A-Z]{1,3})?".prop_map(|raw| Symbol::new(&raw).unwrap());
        let unanswered = prop::sample::select(vec![Unanswered::Missing, Unanswered::Invalid]);
        let refusal = prop::sample::select(RowRefusalKind::iter().collect::<Vec<_>>());
        let fold_count = prop::sample::select(FoldCount::iter().collect::<Vec<_>>());
        let stray = prop::sample::select(Unrecognized::iter().collect::<Vec<_>>());
        let outcome = prop_oneof![
            Just(SessionOutcome::Written),
            Just(SessionOutcome::Unreached),
            (
                prop::sample::select(PartitionFailureKind::iter().collect::<Vec<_>>()),
                ".{0,20}"
            )
                .prop_map(|(failure, cause)| SessionOutcome::Failed { failure, cause }),
        ];
        prop_oneof![
            prop::collection::btree_map(parameter, (".{0,20}", source), 0..6).prop_map(
                |parameters| {
                    Observation::ConfigurationResolved(ConfigurationResolved::new(
                        parameters
                            .into_iter()
                            .map(|(name, (value, source))| {
                                (name, ResolvedParameter::new(value, source))
                            })
                            .collect(),
                    ))
                }
            ),
            (
                leg.clone(),
                any_session(),
                any::<u64>(),
                prop::collection::vec(refusal, 0..6),
                prop::collection::btree_map(symbol, unanswered, 0..6),
                prop::collection::btree_map(fold_count, any::<u64>(), 0..4),
            )
                .prop_map(|(leg, session, rows, refused, unanswered, folded)| {
                    Observation::PartitionWritten(PartitionWritten::new(
                        leg,
                        session,
                        rows,
                        concatenate(refused.into_iter().map(Tally::of)),
                        unanswered,
                        folded,
                    ))
                }),
            (
                leg.clone(),
                any_session(),
                prop::sample::select(PartitionFailureKind::iter().collect::<Vec<_>>()),
                ".{0,20}",
            )
                .prop_map(|(leg, session, failure, cause)| {
                    Observation::PartitionFailed(PartitionFailed::new(leg, session, failure, cause))
                }),
            (any_session(), any::<u64>()).prop_map(|(as_of, conditions)| {
                Observation::ConditionsWritten(ConditionsWritten::new(as_of, conditions))
            }),
            (crate::common::storage::tests::any_key(), any::<u64>()).prop_map(|(key, bytes)| {
                Observation::ObjectWritten(ObjectWritten::new(key, bytes))
            }),
            crate::common::storage::tests::any_key()
                .prop_map(|key| Observation::ObjectDeleted(ObjectDeleted::new(key))),
            (
                prop::collection::btree_set(any_session(), 1..6),
                prop::collection::btree_map(
                    leg.clone(),
                    prop::collection::btree_map(any_session(), outcome, 0..4),
                    0..3
                ),
                prop::collection::btree_map(
                    leg,
                    prop::collection::btree_map(".{0,20}", stray, 0..3),
                    0..3
                ),
            )
                .prop_map(|(window, outcomes, unrecognized)| {
                    let window = Window::new(window.into_iter().collect::<Vec<_>>()).unwrap();
                    Observation::HealFinished(HealFinished::new(window, outcomes, unrecognized))
                }),
            any_bar_built().prop_map(Observation::BarBuilt),
        ]
    }

    fn any_record() -> impl Strategy<Value = Record> {
        (
            any::<u128>(),
            1_u64..u64::MAX,
            0_i64..4_102_444_800,
            0_u32..1_000_000_000,
            prop::option::of(("[0-9a-f]{40}", any::<bool>())),
            any_observation(),
        )
            .prop_map(
                |(run, sequence, seconds, nanoseconds, commit, observation)| {
                    let commit = commit.map(|(sha, dirty)| {
                        Commit::new(&if dirty { format!("{sha}-dirty") } else { sha }).unwrap()
                    });
                    Record::new(
                        RunId::new(Uuid::from_u128(run)),
                        NonZeroU64::new(sequence).unwrap(),
                        DateTime::from_timestamp(seconds, nanoseconds).unwrap(),
                        commit,
                        observation,
                    )
                },
            )
    }

    fn lines() -> impl Strategy<Value = Vec<ReadLine>> {
        prop::collection::btree_set((0..4u128, 1..6u64), 0..12).prop_map(|keys| {
            keys.into_iter()
                .map(|(run, sequence)| line(run, sequence))
                .collect()
        })
    }

    proptest! {
        /// Shipping a file twice changes nothing, and a merge loses no line either writer held.
        #[test]
        fn test_merge_is_idempotent_and_loses_nothing(held in lines(), adding in lines()) {
            let merged = merge(held.clone(), adding.clone());
            prop_assert_eq!(merge(merged.clone(), adding.clone()), merged.clone());
            prop_assert_eq!(merge(held.clone(), held.clone()), held.clone());
            prop_assert!(held.iter().chain(&adding).all(|line| merged.contains(line)));
            prop_assert_eq!(&merged[..held.len()], &held[..]);
        }

        /// On files of distinct lines, an empty file changes nothing on either side, and three writers' files merge
        /// to the same object whichever pair ships first.
        #[test]
        fn property_merge_has_an_identity_and_is_associative(
            first in lines(),
            second in lines(),
            third in lines(),
        ) {
            prop_assert_eq!(merge(Vec::new(), first.clone()), first.clone());
            prop_assert_eq!(merge(first.clone(), Vec::new()), first.clone());
            prop_assert_eq!(
                merge(merge(first.clone(), second.clone()), third.clone()),
                merge(first, merge(second, third))
            );
        }

        #[test]
        fn property_a_record_reads_back_as_itself(record in any_record()) {
            prop_assert_eq!(read(&record.encode()), vec![ReadLine::Read(Box::new(record))]);
        }
    }
}
