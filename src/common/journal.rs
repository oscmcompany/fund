//! The journal's records: observations only, one record per state change, each line naming the schema version that
//! wrote it so a reader never infers it.

use std::collections::BTreeMap;
use std::num::NonZeroU64;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::common::book::{Book, Cash, Position};
use crate::common::guard::{OrderGuarded, TradabilityUnread};
use crate::common::heal::{Leg, SessionOutcome};
use crate::common::laboratory::experiment::{DatasetRead, ExperimentRan};
use crate::common::market::Symbol;
use crate::common::market::trade_bars::BarBuilt;
use crate::common::order::{OrderClosed, OrderRefused, OrderSubmitted, OrderUnresolved};
use crate::common::parameter::Parameter;
use crate::common::reconcile::BookReconciled;
use crate::common::risk::TargetDecided;
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

impl std::error::Error for CommitRefusal {}

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
    HealFinished(HealFinished),
    DatasetRead(Box<DatasetRead>),
    ExperimentRan(Box<ExperimentRan>),
    OrderSubmitted(OrderSubmitted),
    OrderClosed(OrderClosed),
    OrderRefused(OrderRefused),
    OrderUnresolved(OrderUnresolved),
    OrderGuarded(OrderGuarded),
    TradabilityUnread(TradabilityUnread),
    BookReconciled(BookReconciled),
    TargetDecided(TargetDecided),
    SessionOpened(SessionOpened),
    BarBuilt(BarBuilt),
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

/// One leg's session written to the archive and read back, with every symbol and row that did not become a bar.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionWritten {
    leg: Leg,
    session: SessionDate,
    bars: u64,
    /// Refused rows counted by cause, the `RowRefusal` variant in snake case.
    refused: BTreeMap<String, u64>,
    unanswered: BTreeMap<Symbol, Unanswered>,
}

impl PartitionWritten {
    pub fn new(
        leg: Leg,
        session: SessionDate,
        bars: u64,
        refused: BTreeMap<String, u64>,
        unanswered: BTreeMap<Symbol, Unanswered>,
    ) -> Self {
        Self {
            leg,
            session,
            bars,
            refused,
            unanswered,
        }
    }

    pub fn leg(&self) -> Leg {
        self.leg
    }

    pub fn session(&self) -> SessionDate {
        self.session
    }

    pub fn bars(&self) -> u64 {
        self.bars
    }

    pub fn refused(&self) -> &BTreeMap<String, u64> {
        &self.refused
    }

    pub fn unanswered(&self) -> &BTreeMap<Symbol, Unanswered> {
        &self.unanswered
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

/// A run's heal: the window it covered and how each owed session of each leg ended. A session of the window with no
/// outcome was already held.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HealFinished {
    window: Vec<SessionDate>,
    outcomes: BTreeMap<Leg, BTreeMap<SessionDate, SessionOutcome>>,
}

impl HealFinished {
    pub fn new(
        window: Vec<SessionDate>,
        outcomes: BTreeMap<Leg, BTreeMap<SessionDate, SessionOutcome>>,
    ) -> Self {
        Self { window, outcomes }
    }

    pub fn window(&self) -> &[SessionDate] {
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
            BTreeMap::from([("duplicate".to_string(), 2)]),
            BTreeMap::from([
                (Symbol::new("ABC").unwrap(), Unanswered::Missing),
                (Symbol::new("BC.PRC").unwrap(), Unanswered::Invalid),
            ]),
        );
        let finished = HealFinished::new(
            vec![session],
            BTreeMap::from([(
                Leg::MassiveDailyBars,
                BTreeMap::from([
                    (
                        session,
                        SessionOutcome::Failed {
                            cause: "refused with 403".to_string(),
                        },
                    ),
                    (session.plus_calendar_days(-1), SessionOutcome::Unreached),
                ]),
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
                    r#""session":"2026-09-29","bars":1872987,"refused":{"duplicate":2},"#,
                    r#""unanswered":{"ABC":"missing","BC.PRC":"invalid"}}}"#,
                )
            )
        );
        assert_eq!(
            envelope(Observation::HealFinished(finished)),
            format!(
                "{prefix}{}",
                concat!(
                    r#""event_type":"heal_finished","payload":{"window":["2026-09-29"],"#,
                    r#""outcomes":{"massive_daily_bars":{"2026-09-28":{"outcome":"unreached"},"#,
                    r#""2026-09-29":{"outcome":"failed","cause":"refused with 403"}}}}}"#,
                )
            )
        );
    }

    /// The order records as the trader writes them, pinned so a rename shows up as a changed wire format.
    #[test]
    fn test_the_order_records_encode_to_their_wire_format() {
        use crate::common::book::{Book, Cash, Position, ValuationRefusal};
        use crate::common::guard::{Tradability, TradabilityUnread, guard};
        use crate::common::market::{Price, Shares};
        use crate::common::order::{
            ClientOrderId, OrderClosed, OrderEnding, OrderExecution, OrderRefused, OrderReport,
            OrderRequest, OrderState, OrderStatus, OrderUnresolved,
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
                "cancel failed".to_string(),
                OrderExecution::new(
                    Shares::from_units(500_000),
                    Price::from_ticks(12_400_000).unwrap(),
                ),
            )),
            Observation::OrderGuarded(guarded),
            Observation::OrderGuarded(below),
            Observation::TradabilityUnread(TradabilityUnread::new("timed out".to_string())),
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
                    r#"{{"event_type":"order_unresolved","payload":{{{id},"cause":"cancel failed","executed":{{"shares":500000,"average_price":12400000}}}}}}"#
                ),
                r#"{"event_type":"order_guarded","payload":{"symbol":"VWDRY","side":"buy","shares":1500000,"cause":"fractional"}}"#.to_string(),
                r#"{"event_type":"order_guarded","payload":{"symbol":"DIA","side":"buy","shares":19,"cause":{"below_minimum":{"price":470000000}}}}"#.to_string(),
                r#"{"event_type":"tradability_unread","payload":{"cause":"timed out"}}"#.to_string(),
                r#"{"event_type":"book_reconciled","payload":{"expected_cash":"1000","reported_cash":"-5","allowance":"0","gaps":[{"symbol":"SPY","expected":"0","reported":"-2000000"}]}}"#.to_string(),
                r#"{"event_type":"target_decided","payload":{"bar":"2026-10-07T14:05:00Z","wanted":{"SPY":5000000},"restrained":{"target":{},"cuts":[{"outside_trading_window":{"phase":{"before_open":{"until_open":300000000000}}}}]}}}"#.to_string(),
                r#"{"event_type":"target_decided","payload":{"bar":"2026-10-07T14:05:00Z","wanted":{"SPY":5000000},"refused":{"unpriced":{"symbol":"SPY"}}}}"#.to_string(),
                r#"{"event_type":"session_opened","payload":{"session":"2026-10-07","cash":"-7","positions":{"SPY":"2000000"},"opening":"9"}}"#.to_string(),
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

    fn any_observation() -> impl Strategy<Value = Observation> {
        let parameter = prop::sample::select(Parameter::iter().collect::<Vec<_>>());
        let source = prop::sample::select(ParameterSource::iter().collect::<Vec<_>>());
        let leg = prop::sample::select(Leg::iter().collect::<Vec<_>>());
        let symbol = "[A-Z]{1,5}(\\.[A-Z]{1,3})?".prop_map(|raw| Symbol::new(&raw).unwrap());
        let unanswered = prop::sample::select(vec![Unanswered::Missing, Unanswered::Invalid]);
        let outcome = prop_oneof![
            Just(SessionOutcome::Written),
            Just(SessionOutcome::Unreached),
            ".{0,20}".prop_map(|cause| SessionOutcome::Failed { cause }),
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
                prop::collection::btree_map("[a-z_]{1,12}", any::<u64>(), 0..4),
                prop::collection::btree_map(symbol, unanswered, 0..6),
            )
                .prop_map(|(leg, session, bars, refused, unanswered)| {
                    Observation::PartitionWritten(PartitionWritten::new(
                        leg, session, bars, refused, unanswered,
                    ))
                }),
            (
                prop::collection::vec(any_session(), 0..6),
                prop::collection::btree_map(
                    leg,
                    prop::collection::btree_map(any_session(), outcome, 0..4),
                    0..3
                ),
            )
                .prop_map(|(window, outcomes)| {
                    Observation::HealFinished(HealFinished::new(window, outcomes))
                }),
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

        #[test]
        fn property_a_record_reads_back_as_itself(record in any_record()) {
            prop_assert_eq!(read(&record.encode()), vec![ReadLine::Read(Box::new(record))]);
        }
    }
}
