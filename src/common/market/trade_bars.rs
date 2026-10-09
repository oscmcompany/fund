//! Trade bars: what a session's prints, extended hours and closing cross included, add up to under the consolidated
//! tape's condition rules, as exact totals and the prices only eligible prints may set, rolling up from one minute to five and the day.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};

use super::aggregate::{BarKey, RollsUp, Rollup, TradeTotals};
use super::record::{BarInterval, Trade};
use super::{DollarVolume, Price, Shares, StampedPrice, Symbol, TradeCount};
use crate::common::monoid::{Monoid, Semigroup, Tally};
use crate::common::time::SessionDate;

/// What a print carrying one sale condition may update on the consolidated tape; every combination is valid.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UpdateRules {
    pub volume: bool,
    pub high_low: bool,
    pub open_close: bool,
}

impl UpdateRules {
    /// A regular sale's rules, which a print's conditions can only narrow.
    pub const EVERYTHING: Self = Self {
        volume: true,
        high_low: true,
        open_close: true,
    };

    /// Shares count and no price is set, as for a print whose condition is not placed.
    pub const VOLUME_ONLY: Self = Self {
        volume: true,
        high_low: false,
        open_close: false,
    };
}

/// Rules combine to the updates both allow, so a print's conditions fold to what all of them allow.
impl Monoid for UpdateRules {
    fn empty() -> Self {
        Self::EVERYTHING
    }

    fn combine(self, other: Self) -> Self {
        Self {
            volume: self.volume && other.volume,
            high_low: self.high_low && other.high_low,
            open_close: self.open_close && other.open_close,
        }
    }
}

/// The consolidated tape a print was reported on, which decides how its condition letters read.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Tape {
    /// Tapes A and B, NYSE and other listings, under the Consolidated Tape Association's letters.
    ConsolidatedTape,
    /// Tape C, Nasdaq listings, under the Unlisted Trading Privileges plan's letters.
    UnlistedTrading,
}

impl Tape {
    /// The letter that marks a regular sale, which carries no condition of its own.
    fn regular_sale(self) -> ConditionLetter {
        match self {
            Self::ConsolidatedTape => ConditionLetter(' '),
            Self::UnlistedTrading => ConditionLetter('@'),
        }
    }
}

/// A vendor's numeric code for a sale condition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ConditionCode(u16);

impl ConditionCode {
    pub const fn new(code: u16) -> Self {
        Self(code)
    }

    pub fn get(self) -> u16 {
        self.0
    }
}

/// The one character a tape spells a sale condition with.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ConditionLetter(char);

/// A condition spelled with other than exactly one character, with the spelling.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConditionLetterRefusal {
    pub raw: String,
}

impl std::fmt::Display for ConditionLetterRefusal {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "`{}` is not one condition letter", self.raw)
    }
}

impl std::error::Error for ConditionLetterRefusal {}

impl ConditionLetter {
    pub const fn of(letter: char) -> Self {
        Self(letter)
    }

    pub fn new(spelled: &str) -> Result<Self, ConditionLetterRefusal> {
        let mut characters = spelled.chars();
        match (characters.next(), characters.next()) {
            (Some(letter), None) => Ok(Self(letter)),
            (None, _) | (Some(_), Some(_)) => Err(ConditionLetterRefusal {
                raw: spelled.to_string(),
            }),
        }
    }

    pub fn get(self) -> char {
        self.0
    }
}

/// Whether the vendor still prints a condition or keeps its code only for history.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConditionStatus {
    Current,
    /// A current print spelled with a retired condition's letter means the current condition.
    Retired,
}

/// One sale condition: the rules it imposes and the letter each tape spells it with.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Condition {
    rules: UpdateRules,
    consolidated_tape: Option<ConditionLetter>,
    unlisted_trading: Option<ConditionLetter>,
    status: ConditionStatus,
}

impl Condition {
    pub fn new(
        rules: UpdateRules,
        consolidated_tape: Option<ConditionLetter>,
        unlisted_trading: Option<ConditionLetter>,
        status: ConditionStatus,
    ) -> Self {
        Self {
            rules,
            consolidated_tape,
            unlisted_trading,
            status,
        }
    }

    pub fn rules(&self) -> UpdateRules {
        self.rules
    }

    pub fn consolidated_tape(&self) -> Option<ConditionLetter> {
        self.consolidated_tape
    }

    pub fn unlisted_trading(&self) -> Option<ConditionLetter> {
        self.unlisted_trading
    }

    pub fn status(&self) -> ConditionStatus {
        self.status
    }

    fn letter(&self, tape: Tape) -> Option<ConditionLetter> {
        match tape {
            Tape::ConsolidatedTape => self.consolidated_tape,
            Tape::UnlistedTrading => self.unlisted_trading,
        }
    }
}

/// The vendor's sale conditions by code, with each tape letter's rules indexed from them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TradeConditions {
    conditions: BTreeMap<ConditionCode, Condition>,
    /// Derived from `conditions` at construction: the rules a current condition's letter imposes on each tape, or
    /// `None` where two current conditions share the letter and disagree.
    letters: BTreeMap<(Tape, ConditionLetter), Option<UpdateRules>>,
}

/// What a print's conditions let it update; a condition the table cannot place leaves the print unresolved.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Eligibility {
    /// Every condition is known; a print updates what all of them allow.
    Resolved(UpdateRules),
    /// A condition the table does not place: the print still counts toward volume, as the legacy fold counted it,
    /// but sets no price, since nothing says it may.
    Unresolved(Unplaced),
}

impl Eligibility {
    /// What every placed rule allows together, or the first condition that could not be placed.
    fn of(mut placed: impl Iterator<Item = Result<UpdateRules, Unplaced>>) -> Self {
        match placed.try_fold(UpdateRules::empty(), |allowed, rules| {
            rules.map(|rules| allowed.combine(rules))
        }) {
            Ok(allowed) => Self::Resolved(allowed),
            Err(unplaced) => Self::Unresolved(unplaced),
        }
    }
}

/// Why a print's condition could not be placed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Unplaced {
    UnknownCode {
        code: ConditionCode,
    },
    UnknownLetter {
        tape: Tape,
        letter: ConditionLetter,
    },
    /// Two current conditions share the letter with different rules.
    AmbiguousLetter {
        tape: Tape,
        letter: ConditionLetter,
    },
}

impl std::fmt::Display for Unplaced {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnknownCode { code } => write!(formatter, "unknown code {}", code.get()),
            Self::UnknownLetter { tape, letter } => {
                write!(formatter, "unknown letter '{}' on {tape:?}", letter.get())
            }
            Self::AmbiguousLetter { tape, letter } => {
                write!(formatter, "ambiguous letter '{}' on {tape:?}", letter.get())
            }
        }
    }
}

impl TradeConditions {
    pub fn new(conditions: BTreeMap<ConditionCode, Condition>) -> Self {
        let mut letters: BTreeMap<(Tape, ConditionLetter), Option<UpdateRules>> = BTreeMap::new();
        let current = conditions
            .values()
            .filter(|condition| match condition.status {
                ConditionStatus::Current => true,
                ConditionStatus::Retired => false,
            });
        for condition in current {
            for tape in [Tape::ConsolidatedTape, Tape::UnlistedTrading] {
                if let Some(letter) = condition.letter(tape) {
                    let entry = letters
                        .entry((tape, letter))
                        .or_insert(Some(condition.rules));
                    if *entry != Some(condition.rules) {
                        *entry = None;
                    }
                }
            }
        }
        Self {
            conditions,
            letters,
        }
    }

    pub fn conditions(&self) -> &BTreeMap<ConditionCode, Condition> {
        &self.conditions
    }

    /// The rules a print with numeric `codes` falls under: each update allowed only if every code allows it.
    pub fn eligibility(&self, codes: &[ConditionCode]) -> Eligibility {
        Eligibility::of(codes.iter().map(|code| match self.conditions.get(code) {
            Some(condition) => Ok(condition.rules),
            None => Err(Unplaced::UnknownCode { code: *code }),
        }))
    }

    /// The rules a print reported on `tape` with condition `letters` falls under; the regular-sale letter adds none.
    pub fn eligibility_of_letters(&self, tape: Tape, letters: &[ConditionLetter]) -> Eligibility {
        let placed = letters
            .iter()
            .filter(|letter| **letter != tape.regular_sale())
            .map(|letter| match self.letters.get(&(tape, *letter)) {
                Some(Some(rules)) => Ok(*rules),
                Some(None) => Err(Unplaced::AmbiguousLetter {
                    tape,
                    letter: *letter,
                }),
                None => Err(Unplaced::UnknownLetter {
                    tape,
                    letter: *letter,
                }),
            });
        Eligibility::of(placed)
    }
}

/// One print as the fold sees it: a trade, or a price published with no shares, as the corrected consolidated close
/// is after the session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Print {
    Trade(Trade),
    Unsized {
        symbol: Symbol,
        timestamp: DateTime<Utc>,
        price: Price,
    },
}

impl Print {
    /// A trade, or an unsized print when `size` is zero.
    pub fn new(symbol: Symbol, timestamp: DateTime<Utc>, price: Price, size: Shares) -> Self {
        match size.is_zero() {
            true => Self::Unsized {
                symbol,
                timestamp,
                price,
            },
            false => Self::Trade(
                Trade::new(symbol, timestamp, price, size).expect("a print with shares is a trade"),
            ),
        }
    }

    pub fn symbol(&self) -> &Symbol {
        match self {
            Self::Trade(trade) => trade.symbol(),
            Self::Unsized { symbol, .. } => symbol,
        }
    }

    pub fn timestamp(&self) -> DateTime<Utc> {
        match self {
            Self::Trade(trade) => trade.timestamp(),
            Self::Unsized { timestamp, .. } => *timestamp,
        }
    }

    pub fn price(&self) -> Price {
        match self {
            Self::Trade(trade) => trade.price(),
            Self::Unsized { price, .. } => *price,
        }
    }
}

/// Whether a print still stands once the tape's corrections and cancels are applied.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Correction {
    /// Never corrected, or the record that replaces a corrected print.
    Stands,
    /// An original later corrected or canceled, or a cancel's own record, which the fold leaves out entirely.
    Withdrawn,
}

/// The earliest and latest prices a bar's eligible prints set; equal instants break on price so the combine is
/// commutative.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OpenClose {
    open: StampedPrice,
    close: StampedPrice,
}

/// Why an open and close were refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpenCloseRefusal {
    /// The open orders after the close, by instant and then price.
    Inverted {
        open: StampedPrice,
        close: StampedPrice,
    },
}

impl std::fmt::Display for OpenCloseRefusal {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Inverted { open, close } => write!(
                formatter,
                "the open {} at {} orders after the close {} at {}",
                open.price(),
                open.at(),
                close.price(),
                close.at()
            ),
        }
    }
}

impl std::error::Error for OpenCloseRefusal {}

impl OpenClose {
    pub fn new(open: StampedPrice, close: StampedPrice) -> Result<Self, OpenCloseRefusal> {
        if open > close {
            return Err(OpenCloseRefusal::Inverted { open, close });
        }
        Ok(Self { open, close })
    }

    fn of(print: &Print) -> Self {
        let point = StampedPrice::new(print.timestamp(), print.price());
        Self {
            open: point,
            close: point,
        }
    }

    fn combine(self, other: Self) -> Self {
        Self {
            open: self.open.min(other.open),
            close: self.close.max(other.close),
        }
    }

    /// The opening print's time and price.
    pub fn open(&self) -> StampedPrice {
        self.open
    }

    /// The closing print's time and price.
    pub fn close(&self) -> StampedPrice {
        self.close
    }
}

/// The highest and lowest prices a bar's eligible prints set.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HighLow {
    high: Price,
    low: Price,
}

/// Why a high and low were refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HighLowRefusal {
    Inverted { high: Price, low: Price },
}

impl std::fmt::Display for HighLowRefusal {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Inverted { high, low } => {
                write!(formatter, "the high {high} is below the low {low}")
            }
        }
    }
}

impl std::error::Error for HighLowRefusal {}

impl HighLow {
    pub fn new(high: Price, low: Price) -> Result<Self, HighLowRefusal> {
        if low > high {
            return Err(HighLowRefusal::Inverted { high, low });
        }
        Ok(Self { high, low })
    }

    fn of(print: &Print) -> Self {
        Self {
            high: print.price(),
            low: print.price(),
        }
    }

    fn combine(self, other: Self) -> Self {
        Self {
            high: self.high.max(other.high),
            low: self.low.min(other.low),
        }
    }

    pub fn high(&self) -> Price {
        self.high
    }

    pub fn low(&self) -> Price {
        self.low
    }
}

/// One bar's sums: totals over prints eligible for volume, and each price pair over the prints eligible to set it,
/// `None` when no print in the bar was.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TradeSums {
    totals: TradeTotals,
    open_close: Option<OpenClose>,
    high_low: Option<HighLow>,
}

impl TradeSums {
    pub fn new(
        totals: TradeTotals,
        open_close: Option<OpenClose>,
        high_low: Option<HighLow>,
    ) -> Self {
        Self {
            totals,
            open_close,
            high_low,
        }
    }

    /// The sums one print contributes under the updates it is allowed; a print with no shares adds no totals.
    fn of(print: &Print, allowed: UpdateRules) -> Self {
        let totals = match print {
            Print::Trade(trade) if allowed.volume => TradeTotals::of(trade),
            Print::Trade(_) | Print::Unsized { .. } => TradeTotals::empty(),
        };
        Self {
            totals,
            open_close: allowed.open_close.then(|| OpenClose::of(print)),
            high_low: allowed.high_low.then(|| HighLow::of(print)),
        }
    }

    pub fn totals(&self) -> TradeTotals {
        self.totals
    }

    pub fn open_close(&self) -> Option<OpenClose> {
        self.open_close
    }

    pub fn high_low(&self) -> Option<HighLow> {
        self.high_low
    }
}

impl Semigroup for TradeSums {
    fn combine(self, other: Self) -> Self {
        Self {
            totals: self.totals.combine(other.totals),
            open_close: either(self.open_close, other.open_close, OpenClose::combine),
            high_low: either(self.high_low, other.high_low, HighLow::combine),
        }
    }
}

/// Two optional fragments combined, `None` being the identity.
fn either<T>(left: Option<T>, right: Option<T>, combine: fn(T, T) -> T) -> Option<T> {
    match (left, right) {
        (Some(left), Some(right)) => Some(combine(left, right)),
        (Some(only), None) | (None, Some(only)) => Some(only),
        (None, None) => None,
    }
}

/// One symbol's trade bar; it exists only for an interval some folded print of the session fell in.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TradeBar {
    symbol: Symbol,
    interval: BarInterval,
    timestamp: DateTime<Utc>,
    sums: TradeSums,
}

/// Why a trade bar was refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TradeBarRefusal {
    Misaligned {
        interval: BarInterval,
        timestamp: DateTime<Utc>,
    },
}

impl std::fmt::Display for TradeBarRefusal {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Misaligned {
                interval,
                timestamp,
            } => write!(formatter, "{timestamp} does not end a {interval} bar"),
        }
    }
}

impl std::error::Error for TradeBarRefusal {}

impl TradeBar {
    pub fn new(
        symbol: Symbol,
        interval: BarInterval,
        timestamp: DateTime<Utc>,
        sums: TradeSums,
    ) -> Result<Self, TradeBarRefusal> {
        if interval.bucket(timestamp) != timestamp {
            return Err(TradeBarRefusal::Misaligned {
                interval,
                timestamp,
            });
        }
        Ok(Self {
            symbol,
            interval,
            timestamp,
            sums,
        })
    }

    pub fn symbol(&self) -> &Symbol {
        &self.symbol
    }

    pub fn interval(&self) -> BarInterval {
        self.interval
    }

    /// The bar's start for an intraday bar, and the 16:00 Eastern close for a daily one, as for `Bar`.
    pub fn timestamp(&self) -> DateTime<Utc> {
        self.timestamp
    }

    pub fn sums(&self) -> &TradeSums {
        &self.sums
    }
}

/// A bar the trader built from the tape, journaled as `bar_built` with the archive's trade bar columns so a session's
/// bars can be diffed against the archive's for the same minutes; a line is read back through `TradeBar::new`.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(try_from = "BarBuiltColumns", into = "BarBuiltColumns")]
pub struct BarBuilt(TradeBar);

/// `BarBuilt` as journaled: the archive's trade bar columns, each price pair whole or absent.
#[derive(serde::Serialize, serde::Deserialize)]
struct BarBuiltColumns {
    symbol: Symbol,
    interval: BarInterval,
    timestamp: DateTime<Utc>,
    trade_count: TradeCount,
    volume: Shares,
    dollar_volume: DollarVolume,
    opened_at: Option<DateTime<Utc>>,
    open: Option<Price>,
    closed_at: Option<DateTime<Utc>>,
    close: Option<Price>,
    high: Option<Price>,
    low: Option<Price>,
}

/// Why a journaled bar was refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BarBuiltRefusal {
    /// Some columns of a price pair are null, named here.
    PartlyNull {
        null: Vec<&'static str>,
    },
    OpenClose(OpenCloseRefusal),
    HighLow(HighLowRefusal),
    TradeBar(TradeBarRefusal),
}

impl std::fmt::Display for BarBuiltRefusal {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::PartlyNull { null } => {
                write!(formatter, "a price pair is null in {}", null.join(", "))
            }
            Self::OpenClose(refusal) => refusal.fmt(formatter),
            Self::HighLow(refusal) => refusal.fmt(formatter),
            Self::TradeBar(refusal) => refusal.fmt(formatter),
        }
    }
}

impl std::error::Error for BarBuiltRefusal {}

/// The names of `columns` that are null, when some but not all are.
fn partly_null(columns: &[(&'static str, bool)]) -> BarBuiltRefusal {
    BarBuiltRefusal::PartlyNull {
        null: columns
            .iter()
            .filter(|(_, present)| !present)
            .map(|(name, _)| *name)
            .collect(),
    }
}

impl TryFrom<BarBuiltColumns> for BarBuilt {
    type Error = BarBuiltRefusal;

    fn try_from(columns: BarBuiltColumns) -> Result<Self, Self::Error> {
        let open_close = match (
            columns.opened_at,
            columns.open,
            columns.closed_at,
            columns.close,
        ) {
            (Some(opened_at), Some(open), Some(closed_at), Some(close)) => Some(
                OpenClose::new(
                    StampedPrice::new(opened_at, open),
                    StampedPrice::new(closed_at, close),
                )
                .map_err(BarBuiltRefusal::OpenClose)?,
            ),
            (None, None, None, None) => None,
            (opened_at, open, closed_at, close) => {
                return Err(partly_null(&[
                    ("opened_at", opened_at.is_some()),
                    ("open", open.is_some()),
                    ("closed_at", closed_at.is_some()),
                    ("close", close.is_some()),
                ]));
            }
        };
        let high_low = match (columns.high, columns.low) {
            (Some(high), Some(low)) => {
                Some(HighLow::new(high, low).map_err(BarBuiltRefusal::HighLow)?)
            }
            (None, None) => None,
            (high, low) => {
                return Err(partly_null(&[
                    ("high", high.is_some()),
                    ("low", low.is_some()),
                ]));
            }
        };
        let totals = TradeTotals::new(columns.trade_count, columns.volume, columns.dollar_volume);
        TradeBar::new(
            columns.symbol,
            columns.interval,
            columns.timestamp,
            TradeSums::new(totals, open_close, high_low),
        )
        .map(Self)
        .map_err(BarBuiltRefusal::TradeBar)
    }
}

impl From<BarBuilt> for BarBuiltColumns {
    fn from(BarBuilt(bar): BarBuilt) -> Self {
        let TradeSums {
            totals,
            open_close,
            high_low,
        } = bar.sums;
        Self {
            symbol: bar.symbol,
            interval: bar.interval,
            timestamp: bar.timestamp,
            trade_count: totals.count(),
            volume: totals.volume(),
            dollar_volume: totals.dollar_volume(),
            opened_at: open_close.map(|prices| prices.open.at()),
            open: open_close.map(|prices| prices.open.price()),
            closed_at: open_close.map(|prices| prices.close.at()),
            close: open_close.map(|prices| prices.close.price()),
            high: high_low.map(|prices| prices.high()),
            low: high_low.map(|prices| prices.low()),
        }
    }
}

impl BarBuilt {
    pub fn of(bar: &TradeBar) -> Self {
        Self(bar.clone())
    }
}

impl RollsUp for TradeBar {
    type Sums = TradeSums;

    fn parts(&self) -> (BarKey, TradeSums) {
        let key = BarKey {
            symbol: self.symbol.clone(),
            interval: self.interval,
            timestamp: self.timestamp,
        };
        (key, self.sums)
    }

    fn from_parts(key: BarKey, sums: TradeSums) -> Self {
        Self::new(key.symbol, key.interval, key.timestamp, sums)
            .expect("a rolled-up bucket sits on its interval's grid")
    }
}

/// What a session's fold did with the prints it was offered.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TradeFoldCounts {
    folded: u64,
    /// Stamped for another session's Eastern date.
    other_session: u64,
    /// Withdrawn by a later correction or cancel.
    withdrawn: u64,
    volume_ineligible: u64,
    /// Published with no shares, which may set prices and never volume.
    unsized_prints: u64,
    /// Counted by the first condition each could not place.
    unresolved: Tally<Unplaced>,
    /// Arrived for a minute ending by the cutoff `drain_through` set, whether or not that minute held a bar, and left out so
    /// a bar handed out never changes.
    late: u64,
}

impl TradeFoldCounts {
    pub fn folded(&self) -> u64 {
        self.folded
    }

    pub fn other_session(&self) -> u64 {
        self.other_session
    }

    pub fn withdrawn(&self) -> u64 {
        self.withdrawn
    }

    pub fn volume_ineligible(&self) -> u64 {
        self.volume_ineligible
    }

    pub fn unsized_prints(&self) -> u64 {
        self.unsized_prints
    }

    pub fn unresolved(&self) -> &Tally<Unplaced> {
        &self.unresolved
    }

    pub fn late(&self) -> u64 {
        self.late
    }
}

/// One session's prints folded into one-minute trade bars over its whole Eastern day; the condition rules, not the
/// hours, decide what a print may set, so an extended-hours print adds volume and the closing cross sets the close.
pub struct TradeFold {
    session: SessionDate,
    conditions: TradeConditions,
    minutes: Rollup<TradeBar>,
    /// The cutoff the latest `drain_through` set: every minute ending by it is closed to further prints.
    drained_through: Option<DateTime<Utc>>,
    counts: TradeFoldCounts,
}

impl TradeFold {
    pub fn new(session: SessionDate, conditions: TradeConditions) -> Self {
        Self {
            session,
            conditions,
            minutes: Rollup::empty(),
            drained_through: None,
            counts: TradeFoldCounts::default(),
        }
    }

    /// Folds one print carrying the vendor's numeric condition `codes`.
    pub fn push(&mut self, print: &Print, codes: &[ConditionCode], correction: Correction) {
        let eligibility = self.conditions.eligibility(codes);
        self.push_eligible(print, eligibility, correction);
    }

    /// Folds one print reported on `tape` with condition `letters`, as Alpaca spells them.
    pub fn push_lettered(
        &mut self,
        print: &Print,
        tape: Tape,
        letters: &[ConditionLetter],
        correction: Correction,
    ) {
        let eligibility = self.conditions.eligibility_of_letters(tape, letters);
        self.push_eligible(print, eligibility, correction);
    }

    /// Folds one print under `eligibility`, unless it is another session's or was withdrawn.
    fn push_eligible(&mut self, print: &Print, eligibility: Eligibility, correction: Correction) {
        if SessionDate::at(print.timestamp()) != self.session {
            self.counts.other_session += 1;
            return;
        }
        match correction {
            Correction::Stands => {}
            Correction::Withdrawn => {
                self.counts.withdrawn += 1;
                return;
            }
        }
        if self.closed(print) {
            self.counts.late += 1;
            return;
        }
        let allowed = match eligibility {
            Eligibility::Resolved(allowed) => allowed,
            Eligibility::Unresolved(unplaced) => {
                self.counts.unresolved.add(unplaced);
                UpdateRules::VOLUME_ONLY
            }
        };
        match print {
            Print::Trade(_) if !allowed.volume => self.counts.volume_ineligible += 1,
            Print::Unsized { .. } => self.counts.unsized_prints += 1,
            Print::Trade(_) => {}
        }
        self.counts.folded += 1;
        let key = BarKey {
            symbol: print.symbol().clone(),
            interval: BarInterval::OneMinute,
            timestamp: BarInterval::OneMinute.bucket(print.timestamp()),
        };
        self.minutes.add(key, TradeSums::of(print, allowed));
    }

    /// Whether `print`'s minute ended by the cutoff the latest `drain_through` set, so the fold counts it late.
    fn closed(&self, print: &Print) -> bool {
        self.drained_through
            .is_some_and(|drained| minute_ended_by(print, drained))
    }

    /// The one-minute bars that have ended by `through`, each handed out once; bars handed out minute by minute and
    /// then by `finish` are the bars folding the same prints whole would give, when no print arrives late.
    pub fn drain_through(&mut self, through: DateTime<Utc>) -> Vec<TradeBar> {
        self.drained_through = Some(
            self.drained_through
                .map_or(through, |drained| drained.max(through)),
        );
        self.minutes.split_through(through).into_bars()
    }

    /// The one-minute bars not yet handed out and what the fold did with every print.
    pub fn finish(self) -> (Vec<TradeBar>, TradeFoldCounts) {
        (self.minutes.into_bars(), self.counts)
    }
}

/// Whether `print`'s minute has ended by `through`, the one test of a minute being closed to further prints.
fn minute_ended_by(print: &Print, through: DateTime<Utc>) -> bool {
    BarInterval::OneMinute.ends(BarInterval::OneMinute.bucket(print.timestamp())) <= through
}

/// A print held whole until its minute is drained, so a correction or cancel can still withdraw it.
#[derive(Debug, Clone)]
struct HeldPrint {
    print: Print,
    tape: Tape,
    letters: Vec<ConditionLetter>,
}

/// What a correction or cancel did to the print it names.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Withdrawal {
    /// The print's minute was still open, so no bar holds it.
    Applied,
    /// No open minute holds the print: its bar was already handed out, or the fold never saw it, so no bar changes.
    NotHeld,
}

/// A `TradeFold` fed from a live tape, where a correction or cancel names an earlier print by `Identity` rather than
/// arriving as a labelled record beside it.
pub struct LiveTradeFold<Identity> {
    fold: TradeFold,
    held: BTreeMap<Identity, HeldPrint>,
}

impl<Identity: Ord> LiveTradeFold<Identity> {
    pub fn new(session: SessionDate, conditions: TradeConditions) -> Self {
        Self {
            fold: TradeFold::new(session, conditions),
            held: BTreeMap::new(),
        }
    }

    /// Holds a standing print until its minute drains; a withdrawn print, or one for a minute already drained, is folded
    /// at once, as `TradeFold` counts it.
    pub fn push(
        &mut self,
        identity: Identity,
        print: Print,
        tape: Tape,
        letters: Vec<ConditionLetter>,
        correction: Correction,
    ) {
        match correction {
            Correction::Stands if self.fold.closed(&print) => {
                self.fold
                    .push_lettered(&print, tape, &letters, Correction::Stands);
            }
            Correction::Stands => {
                self.held.insert(
                    identity,
                    HeldPrint {
                        print,
                        tape,
                        letters,
                    },
                );
            }
            Correction::Withdrawn => {
                self.fold
                    .push_lettered(&print, tape, &letters, Correction::Withdrawn);
            }
        }
    }

    /// Withdraws the print `identity` names, as a correction's original or a cancel's print is withdrawn.
    pub fn withdraw(&mut self, identity: &Identity) -> Withdrawal {
        match self.held.remove(identity) {
            Some(held) => {
                self.fold.push_lettered(
                    &held.print,
                    held.tape,
                    &held.letters,
                    Correction::Withdrawn,
                );
                Withdrawal::Applied
            }
            None => Withdrawal::NotHeld,
        }
    }

    /// Folds every held print whose minute has ended by `through`, then hands out the bars `TradeFold` would.
    pub fn drain_through(&mut self, through: DateTime<Utc>) -> Vec<TradeBar> {
        let (due, open): (BTreeMap<_, _>, BTreeMap<_, _>) = std::mem::take(&mut self.held)
            .into_iter()
            .partition(|(_, held)| minute_ended_by(&held.print, through));
        self.held = open;
        for held in due.values() {
            self.fold
                .push_lettered(&held.print, held.tape, &held.letters, Correction::Stands);
        }
        self.fold.drain_through(through)
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use chrono::TimeDelta;

    use super::*;
    use crate::common::market::Shares;
    use crate::common::market::aggregate::{check_rollups_compose, roll_up};
    use crate::common::monoid::{concatenate, laws};

    fn codes(raw: &[u16]) -> Vec<ConditionCode> {
        raw.iter().copied().map(ConditionCode::new).collect()
    }

    fn letters(raw: &[char]) -> Vec<ConditionLetter> {
        raw.iter().copied().map(ConditionLetter::of).collect()
    }

    fn instant(text: &str) -> DateTime<Utc> {
        text.parse().unwrap()
    }

    fn trade(at: &str, dollars: f64, shares: f64) -> Print {
        Print::Trade(trade_record(at, dollars, shares))
    }

    fn trade_record(at: &str, dollars: f64, shares: f64) -> Trade {
        Trade::new(
            Symbol::new("AAPL").unwrap(),
            instant(at),
            Price::from_dollars(dollars).unwrap(),
            Shares::from_float(shares).unwrap(),
        )
        .unwrap()
    }

    /// Codes 10 (derivatively priced), 37 (odd lot) and 15 (official close) as Massive's table gives them on
    /// 2026-10-05.
    fn conditions() -> TradeConditions {
        TradeConditions::new(BTreeMap::from([
            (
                ConditionCode::new(10),
                Condition::new(
                    UpdateRules {
                        volume: true,
                        high_low: true,
                        open_close: false,
                    },
                    None,
                    None,
                    ConditionStatus::Current,
                ),
            ),
            (
                ConditionCode::new(15),
                Condition::new(
                    UpdateRules {
                        volume: false,
                        high_low: false,
                        open_close: false,
                    },
                    None,
                    None,
                    ConditionStatus::Current,
                ),
            ),
            (
                ConditionCode::new(37),
                Condition::new(
                    UpdateRules::VOLUME_ONLY,
                    None,
                    None,
                    ConditionStatus::Current,
                ),
            ),
        ]))
    }

    fn october_second() -> SessionDate {
        SessionDate::from_date(chrono::NaiveDate::from_ymd_opt(2026, 10, 2).unwrap())
    }

    fn session() -> TradeFold {
        TradeFold::new(october_second(), conditions())
    }

    /// A print for a minute already drained is late at once, so a later withdrawal of it applies to nothing, while one
    /// for an open minute is held and withdrawn.
    #[test]
    fn test_a_print_for_a_drained_minute_is_never_held() {
        let mut fold = LiveTradeFold::new(october_second(), conditions());
        assert!(
            fold.drain_through(instant("2026-10-02T14:02:00Z"))
                .is_empty()
        );
        for (identity, at) in [(1, "2026-10-02T14:01:10Z"), (2, "2026-10-02T14:02:10Z")] {
            fold.push(
                identity,
                trade(at, 100.0, 1.0),
                Tape::ConsolidatedTape,
                letters(&[' ']),
                Correction::Stands,
            );
        }
        assert_eq!(
            (fold.withdraw(&1), fold.withdraw(&2)),
            (Withdrawal::NotHeld, Withdrawal::Applied)
        );
        assert!(
            fold.drain_through(instant("2026-10-02T14:05:00Z"))
                .is_empty()
        );
        let (bars, counts) = fold.fold.finish();
        assert!(bars.is_empty());
        assert_eq!((counts.late(), counts.withdrawn()), (1, 1));
    }

    #[test]
    fn test_each_price_comes_only_from_prints_allowed_to_set_it() {
        let mut fold = session();
        fold.push(
            &trade("2026-10-02T13:30:01Z", 100.00, 50.0),
            &codes(&[37]),
            Correction::Stands,
        );
        fold.push(
            &trade("2026-10-02T13:30:02Z", 101.00, 200.0),
            &[],
            Correction::Stands,
        );
        fold.push(
            &trade("2026-10-02T13:30:03Z", 99.00, 100.0),
            &codes(&[10]),
            Correction::Stands,
        );
        fold.push(
            &trade("2026-10-02T13:30:04Z", 150.00, 900.0),
            &codes(&[15]),
            Correction::Stands,
        );
        fold.push(
            &trade("2026-10-02T13:30:05Z", 100.50, 10.0),
            &[],
            Correction::Withdrawn,
        );
        fold.push(
            &trade("2026-10-02T13:31:00Z", 100.25, 0.5),
            &codes(&[37]),
            Correction::Stands,
        );
        let (bars, counts) = fold.finish();
        assert_eq!(bars.len(), 2);
        let first = bars[0].sums();
        assert_eq!(first.totals().count().count(), 3);
        assert_eq!(first.totals().volume().units(), 350_000_000);
        let open_close = first.open_close().unwrap();
        assert_eq!(open_close.open().price().ticks(), 101_000_000);
        assert_eq!(open_close.close().price().ticks(), 101_000_000);
        let high_low = first.high_low().unwrap();
        assert_eq!(
            (high_low.high().ticks(), high_low.low().ticks()),
            (101_000_000, 99_000_000)
        );
        // An odd-lot-only minute has volume but nothing to set its prices.
        let second = bars[1].sums();
        assert_eq!(second.totals().volume().units(), 500_000);
        assert_eq!((second.open_close(), second.high_low()), (None, None));
        assert_eq!(
            (
                counts.folded(),
                counts.withdrawn(),
                counts.volume_ineligible()
            ),
            (5, 1, 1)
        );
    }

    #[test]
    fn test_the_closing_cross_after_four_sets_the_close_and_after_hours_adds_volume_only() {
        // The closing print's code sets everything and lands after 16:00 Eastern; Form T only adds volume.
        let conditions = TradeConditions::new(BTreeMap::from([
            (
                ConditionCode::new(8),
                Condition::new(
                    UpdateRules::EVERYTHING,
                    None,
                    None,
                    ConditionStatus::Current,
                ),
            ),
            (
                ConditionCode::new(12),
                Condition::new(
                    UpdateRules::VOLUME_ONLY,
                    None,
                    None,
                    ConditionStatus::Current,
                ),
            ),
        ]));
        let mut fold = TradeFold::new(october_second(), conditions);
        fold.push(
            &trade("2026-10-02T19:59:59Z", 100.00, 100.0),
            &[],
            Correction::Stands,
        );
        fold.push(
            &trade("2026-10-02T20:02:10Z", 100.05, 7_000.0),
            &codes(&[8]),
            Correction::Stands,
        );
        fold.push(
            &trade("2026-10-02T21:30:00Z", 101.00, 50.0),
            &codes(&[12]),
            Correction::Stands,
        );
        let minutes = fold.finish().0;
        let daily = roll_up(&minutes, BarInterval::OneDay).unwrap();
        let sums = daily[0].sums();
        assert_eq!(sums.totals().volume().units(), 7_150_000_000);
        assert_eq!(
            sums.open_close().unwrap().close().price().ticks(),
            100_050_000
        );
        assert_eq!(sums.high_low().unwrap().high().ticks(), 100_050_000);
    }

    #[test]
    fn test_the_unsized_corrected_close_sets_the_close_and_no_volume() {
        // Code 38 as Massive's table gives it: no volume, but the high, low, open and close.
        let conditions = TradeConditions::new(BTreeMap::from([(
            ConditionCode::new(38),
            Condition::new(
                UpdateRules {
                    volume: false,
                    high_low: true,
                    open_close: true,
                },
                None,
                None,
                ConditionStatus::Current,
            ),
        )]));
        let mut fold = TradeFold::new(october_second(), conditions);
        fold.push(
            &trade("2026-10-02T19:59:55Z", 87.67, 100.0),
            &[],
            Correction::Stands,
        );
        let corrected_close = Print::Unsized {
            symbol: Symbol::new("AAPL").unwrap(),
            timestamp: instant("2026-10-02T20:10:00.003861Z"),
            price: Price::from_dollars(87.68).unwrap(),
        };
        fold.push(&corrected_close, &codes(&[38]), Correction::Stands);
        let (minutes, counts) = fold.finish();
        let daily = roll_up(&minutes, BarInterval::OneDay).unwrap();
        let sums = daily[0].sums();
        assert_eq!(
            sums.open_close().unwrap().close().price().ticks(),
            87_680_000
        );
        assert_eq!(sums.totals().count().count(), 1);
        assert_eq!(sums.totals().volume().units(), 100_000_000);
        assert_eq!(counts.unsized_prints(), 1);
    }

    #[test]
    fn test_a_letter_reads_as_the_current_condition_it_spells() {
        // As Massive lists them: CTA "I" is the retired CAP election (6) and the odd lot (37); "K" is rules 155 and 127.
        let everything = UpdateRules::EVERYTHING;
        let conditions = TradeConditions::new(BTreeMap::from([
            (
                ConditionCode::new(6),
                Condition::new(
                    everything,
                    Some(ConditionLetter::of('I')),
                    None,
                    ConditionStatus::Retired,
                ),
            ),
            (
                ConditionCode::new(37),
                Condition::new(
                    UpdateRules::VOLUME_ONLY,
                    Some(ConditionLetter::of('I')),
                    Some(ConditionLetter::of('I')),
                    ConditionStatus::Current,
                ),
            ),
            (
                ConditionCode::new(23),
                Condition::new(
                    everything,
                    Some(ConditionLetter::of('K')),
                    None,
                    ConditionStatus::Current,
                ),
            ),
            (
                ConditionCode::new(24),
                Condition::new(
                    everything,
                    Some(ConditionLetter::of('K')),
                    None,
                    ConditionStatus::Current,
                ),
            ),
            (
                ConditionCode::new(9),
                Condition::new(
                    everything,
                    None,
                    Some(ConditionLetter::of('X')),
                    ConditionStatus::Current,
                ),
            ),
            (
                ConditionCode::new(41),
                Condition::new(
                    UpdateRules {
                        volume: true,
                        high_low: false,
                        open_close: true,
                    },
                    None,
                    Some(ConditionLetter::of('X')),
                    ConditionStatus::Current,
                ),
            ),
        ]));
        assert_eq!(
            conditions.eligibility_of_letters(Tape::ConsolidatedTape, &letters(&[' ', 'I'])),
            Eligibility::Resolved(UpdateRules::VOLUME_ONLY)
        );
        assert_eq!(
            conditions.eligibility_of_letters(Tape::ConsolidatedTape, &letters(&['K'])),
            Eligibility::Resolved(everything)
        );
        assert_eq!(
            conditions.eligibility_of_letters(Tape::UnlistedTrading, &letters(&['@'])),
            Eligibility::Resolved(everything)
        );
        assert_eq!(
            conditions.eligibility_of_letters(Tape::UnlistedTrading, &letters(&['X'])),
            Eligibility::Unresolved(Unplaced::AmbiguousLetter {
                tape: Tape::UnlistedTrading,
                letter: ConditionLetter::of('X')
            })
        );
        assert_eq!(
            conditions.eligibility_of_letters(Tape::ConsolidatedTape, &letters(&['Z'])),
            Eligibility::Unresolved(Unplaced::UnknownLetter {
                tape: Tape::ConsolidatedTape,
                letter: ConditionLetter::of('Z')
            })
        );
        // A narrowing letter before them changes nothing, and the first letter that cannot be placed is the one named.
        assert_eq!(
            conditions.eligibility_of_letters(Tape::UnlistedTrading, &letters(&['I', 'Z', 'X'])),
            Eligibility::Unresolved(Unplaced::UnknownLetter {
                tape: Tape::UnlistedTrading,
                letter: ConditionLetter::of('Z')
            })
        );
        assert_eq!(
            conditions.eligibility_of_letters(Tape::UnlistedTrading, &letters(&['X', 'Z'])),
            Eligibility::Unresolved(Unplaced::AmbiguousLetter {
                tape: Tape::UnlistedTrading,
                letter: ConditionLetter::of('X')
            })
        );
    }

    #[test]
    fn test_an_unknown_condition_counts_volume_and_sets_no_price() {
        let mut fold = session();
        fold.push(
            &trade("2026-10-02T13:30:01Z", 100.00, 50.0),
            &codes(&[99]),
            Correction::Stands,
        );
        // The first code the table cannot place is the one counted.
        fold.push(
            &trade("2026-10-02T13:30:02Z", 100.00, 50.0),
            &codes(&[98, 99]),
            Correction::Stands,
        );
        // 00:30 Eastern on the next day is another session's.
        fold.push(
            &trade("2026-10-03T04:30:00Z", 100.00, 50.0),
            &[],
            Correction::Stands,
        );
        let (bars, counts) = fold.finish();
        assert_eq!(bars.len(), 1);
        assert_eq!(bars[0].sums().totals().volume().units(), 100_000_000);
        assert_eq!(bars[0].sums().open_close(), None);
        assert_eq!(
            counts.unresolved().to_string(),
            "unknown code 98=1, unknown code 99=1"
        );
        assert_eq!(
            (counts.unresolved().total(), counts.other_session()),
            (2, 1)
        );
    }

    #[test]
    fn test_an_open_after_its_close_is_refused() {
        let price = Price::from_dollars(100.00).unwrap();
        let (early, late) = (
            instant("2026-10-02T13:30:00Z"),
            instant("2026-10-02T13:30:01Z"),
        );
        let (early, late) = (
            StampedPrice::new(early, price),
            StampedPrice::new(late, price),
        );
        assert!(OpenClose::new(early, late).is_ok());
        assert_eq!(
            OpenClose::new(late, early),
            Err(OpenCloseRefusal::Inverted {
                open: late,
                close: early
            })
        );
    }

    const BUILT: &str = r#"{"symbol":"AAPL","interval":"one_minute","timestamp":"2026-10-08T14:00:00Z","trade_count":2,"volume":2000000,"dollar_volume":"201000000000000","opened_at":"2026-10-08T14:00:10Z","open":100000000,"closed_at":"2026-10-08T14:00:50Z","close":101000000,"high":101000000,"low":100000000}"#;

    /// `BUILT` with `replace` swapped for `with`, read back as a journaled bar.
    fn read_built(replace: &str, with: &str) -> Result<BarBuilt, String> {
        serde_json::from_str::<BarBuilt>(&BUILT.replace(replace, with))
            .map_err(|error| error.to_string())
    }

    /// The journaled columns are the archive's, and a line reads back to the bar that wrote it.
    #[test]
    fn test_a_built_bar_writes_the_archive_columns_and_reads_back() {
        let price = |ticks| Price::from_ticks(ticks).unwrap();
        let bar = TradeBar::new(
            Symbol::new("AAPL").unwrap(),
            BarInterval::OneMinute,
            instant("2026-10-08T14:00:00Z"),
            TradeSums::new(
                TradeTotals::new(
                    TradeCount::new(2),
                    Shares::whole(2).unwrap(),
                    DollarVolume::from_units(201_000_000_000_000),
                ),
                Some(
                    OpenClose::new(
                        StampedPrice::new(instant("2026-10-08T14:00:10Z"), price(100_000_000)),
                        StampedPrice::new(instant("2026-10-08T14:00:50Z"), price(101_000_000)),
                    )
                    .unwrap(),
                ),
                Some(HighLow::new(price(101_000_000), price(100_000_000)).unwrap()),
            ),
        )
        .unwrap();
        let built = BarBuilt::of(&bar);
        assert_eq!(serde_json::to_string(&built).unwrap(), BUILT);
        assert_eq!(read_built("", ""), Ok(built));
    }

    /// A line `OpenClose`, `HighLow` or `TradeBar` would refuse is refused on read, as is a pair half null.
    #[test]
    fn test_a_built_bar_that_could_not_have_been_built_is_refused() {
        let refusals = [
            read_built(
                r#""opened_at":"2026-10-08T14:00:10Z""#,
                r#""opened_at":null"#,
            ),
            read_built(r#""low":100000000"#, r#""low":null"#),
            read_built(
                r#""opened_at":"2026-10-08T14:00:10Z""#,
                r#""opened_at":"2026-10-08T14:00:55Z""#,
            ),
            read_built(r#""high":101000000"#, r#""high":99000000"#),
            read_built(
                r#""timestamp":"2026-10-08T14:00:00Z""#,
                r#""timestamp":"2026-10-08T14:00:30Z""#,
            ),
        ]
        .map(|read| read.unwrap_err());
        let expected = [
            "a price pair is null in opened_at",
            "a price pair is null in low",
            "the open 100.00 at 2026-10-08 14:00:55 UTC orders after the close 101.00 at 2026-10-08 14:00:50 UTC",
            "the high 99.00 is below the low 100.00",
            "2026-10-08 14:00:30 UTC does not end a one_minute bar",
        ];
        for (refusal, expected) in refusals.iter().zip(expected) {
            assert!(refusal.starts_with(expected), "{refusal}");
        }
    }

    /// One print's fragment, stamped within `seconds` of the open.
    fn any_rollup(seconds: std::ops::Range<i64>) -> impl Strategy<Value = Rollup<TradeBar>> {
        (
            seconds,
            1_i64..2_000_000,
            1_u64..1_000_000,
            any::<[bool; 3]>(),
        )
            .prop_map(|(second, ticks, units, [volume, high_low, open_close])| {
                let trade = Trade::new(
                    Symbol::new("AAPL").unwrap(),
                    instant("2026-10-02T13:30:00Z") + TimeDelta::seconds(second),
                    Price::from_ticks(ticks).unwrap(),
                    Shares::from_units(units),
                )
                .unwrap();
                let key = BarKey {
                    symbol: trade.symbol().clone(),
                    interval: BarInterval::OneMinute,
                    timestamp: BarInterval::OneMinute.bucket(trade.timestamp()),
                };
                let allowed = UpdateRules {
                    volume,
                    high_low,
                    open_close,
                };
                let mut rollup = Rollup::empty();
                rollup.add(key, TradeSums::of(&Print::Trade(trade), allowed));
                rollup
            })
    }

    /// A print with no shares is unsized and any other is a trade.
    #[test]
    fn test_a_print_is_unsized_only_with_no_shares() {
        let at = instant("2026-10-02T20:10:00Z");
        let print = |units| {
            Print::new(
                Symbol::new("AAPL").unwrap(),
                at,
                Price::from_dollars(87.68).unwrap(),
                Shares::from_units(units),
            )
        };
        assert_eq!(
            print(0),
            Print::Unsized {
                symbol: Symbol::new("AAPL").unwrap(),
                timestamp: at,
                price: Price::from_dollars(87.68).unwrap(),
            }
        );
        assert!(matches!(print(1), Print::Trade(trade) if trade.size().units() == 1));
    }

    #[test]
    fn test_a_condition_letter_is_exactly_one_character() {
        let read: Vec<Result<char, ConditionLetterRefusal>> = ["I", " ", "@", "", "XY"]
            .into_iter()
            .map(|spelled| ConditionLetter::new(spelled).map(ConditionLetter::get))
            .collect();
        let refused = |raw: &str| {
            Err(ConditionLetterRefusal {
                raw: raw.to_string(),
            })
        };
        assert_eq!(
            read,
            [Ok('I'), Ok(' '), Ok('@'), refused(""), refused("XY")]
        );
    }

    /// A five-minute bar from 13:30 ends at 13:35: split off only once that instant is reached.
    #[test]
    fn test_a_five_minute_bar_ends_five_minutes_after_it_starts() {
        let mut fold = session();
        fold.push(
            &trade("2026-10-02T13:31:10Z", 100.00, 100.0),
            &[],
            Correction::Stands,
        );
        let minute = fold
            .drain_through(instant("2026-10-02T13:32:00Z"))
            .remove(0);
        let mut five = Rollup::of(&minute, BarInterval::FiveMinute).unwrap();
        assert_eq!(
            five.split_through(instant("2026-10-02T13:34:59Z")),
            Rollup::empty()
        );
        let ended = five
            .split_through(instant("2026-10-02T13:35:00Z"))
            .into_bars();
        assert_eq!(ended.len(), 1);
        assert_eq!(ended[0].timestamp(), instant("2026-10-02T13:30:00Z"));
    }

    /// Draining hands out the closed minute once; a print for it arriving afterwards is counted late and left out,
    /// one stamped exactly at its end belongs to the next minute, and the open minute stays for `finish`.
    #[test]
    fn test_a_print_for_a_minute_already_handed_out_is_late() {
        let mut fold = session();
        fold.push(
            &trade("2026-10-02T13:30:01Z", 100.00, 100.0),
            &[],
            Correction::Stands,
        );
        fold.push(
            &trade("2026-10-02T13:31:05Z", 101.00, 100.0),
            &[],
            Correction::Stands,
        );
        assert_eq!(fold.drain_through(instant("2026-10-02T13:30:59Z")), []);
        let drained = fold.drain_through(instant("2026-10-02T13:31:00Z"));
        assert_eq!(drained.len(), 1);
        assert_eq!(drained[0].timestamp(), instant("2026-10-02T13:30:00Z"));
        fold.push(
            &trade("2026-10-02T13:30:30Z", 99.00, 100.0),
            &[],
            Correction::Stands,
        );
        fold.push(
            &trade("2026-10-02T13:31:00Z", 102.00, 100.0),
            &[],
            Correction::Stands,
        );
        assert_eq!(fold.drain_through(instant("2026-10-02T13:31:00Z")), []);
        let (rest, counts) = fold.finish();
        assert_eq!(rest.len(), 1);
        assert_eq!(rest[0].timestamp(), instant("2026-10-02T13:31:00Z"));
        assert_eq!((counts.folded(), counts.late()), (3, 1));
    }

    fn any_rules() -> impl Strategy<Value = UpdateRules> {
        any::<(bool, bool, bool)>().prop_map(|(volume, high_low, open_close)| UpdateRules {
            volume,
            high_low,
            open_close,
        })
    }

    proptest! {
        /// Rules narrow in any order and any grouping, and a condition repeated narrows nothing further.
        #[test]
        fn property_update_rules_are_an_idempotent_commutative_monoid(
            first in any_rules(),
            second in any_rules(),
            third in any_rules(),
        ) {
            laws::check(first, second, third)?;
            prop_assert_eq!(first.combine(first), first);
        }

        /// Bars handed out minute by minute as the clock passes them, then by `finish`, are the bars folding the same
        /// prints whole gives, whatever the drain points, when prints arrive in time order.
        #[test]
        fn property_draining_minute_by_minute_equals_folding_whole(
            offsets in prop::collection::vec(0..600i64, 0..40),
            drains in prop::collection::btree_set(0..40usize, 0..10),
            codes in prop::collection::vec(prop::sample::select(vec![0u16, 10, 15, 37]), 40),
        ) {
            let mut offsets = offsets;
            offsets.sort_unstable();
            let open = instant("2026-10-02T13:30:00Z");
            let prints: Vec<(Print, Vec<ConditionCode>)> = offsets
                .iter()
                .zip(&codes)
                .map(|(offset, code)| {
                    let at = open + TimeDelta::seconds(*offset);
                    let codes = match code { 0 => vec![], code => vec![ConditionCode::new(*code)] };
                    (Print::Trade(Trade::new(
                        Symbol::new("AAPL").unwrap(),
                        at,
                        Price::from_ticks(100_000_000 + offset).unwrap(),
                        Shares::whole(100).unwrap(),
                    ).unwrap()), codes)
                })
                .collect();
            let mut whole = session();
            let mut live = session();
            let mut handed_out = Vec::new();
            for (index, (print, codes)) in prints.iter().enumerate() {
                if drains.contains(&index) {
                    handed_out.extend(live.drain_through(BarInterval::OneMinute.bucket(print.timestamp())));
                }
                whole.push(print, codes, Correction::Stands);
                live.push(print, codes, Correction::Stands);
            }
            let (rest, counts) = live.finish();
            handed_out.extend(rest);
            handed_out.sort_by_key(TradeBar::timestamp);
            prop_assert_eq!(counts.late(), 0);
            prop_assert_eq!(handed_out, whole.finish().0);
        }

        /// With nothing withdrawn, the live fold hands out at every drain exactly the bars the plain fold does, late
        /// prints included, and holds nothing once the clock has passed every print's minute.
        #[test]
        fn property_the_live_fold_hands_out_the_plain_folds_bars(
            offsets in prop::collection::vec(0..600i64, 0..40),
            drains in prop::collection::btree_map(0..40usize, 0..600i64, 0..10),
        ) {
            let open = instant("2026-10-02T13:30:00Z");
            let mut plain = session();
            let mut live = LiveTradeFold::new(october_second(), conditions());
            for (index, offset) in offsets.iter().enumerate() {
                if let Some(drain) = drains.get(&index) {
                    let through = open + TimeDelta::seconds(*drain);
                    prop_assert_eq!(live.drain_through(through), plain.drain_through(through));
                }
                let print = Print::Trade(Trade::new(
                    Symbol::new("AAPL").unwrap(),
                    open + TimeDelta::seconds(*offset),
                    Price::from_ticks(100_000_000 + offset).unwrap(),
                    Shares::whole(100).unwrap(),
                ).unwrap());
                plain.push_lettered(&print, Tape::UnlistedTrading, &[], Correction::Stands);
                live.push(index, print, Tape::UnlistedTrading, vec![], Correction::Stands);
            }
            let close = open + TimeDelta::minutes(11);
            prop_assert_eq!(live.drain_through(close), plain.drain_through(close));
            prop_assert_eq!(live.held.len(), 0);
        }

        #[test]
        fn property_trade_rollups_are_a_commutative_monoid(
            first in any_rollup(0..120),
            second in any_rollup(0..120),
            third in any_rollup(0..120),
        ) {
            laws::check(first, second, third)?;
        }

        /// The daily bar's totals are its minutes' totals, whatever the grouping, and in stages or at once.
        #[test]
        fn property_a_daily_bar_totals_its_minutes(minutes in prop::collection::vec(any_rollup(0..1_200), 1..30)) {
            let minute_bars = concatenate(minutes).into_bars();
            check_rollups_compose(&minute_bars)?;
            let daily = roll_up(&minute_bars, BarInterval::OneDay).unwrap();
            prop_assert_eq!(daily.len(), 1);
            let volume: u64 = minute_bars.iter().map(|bar| bar.sums().totals().volume().units()).sum();
            prop_assert_eq!(daily[0].sums().totals().volume().units(), volume);
            let high = minute_bars.iter().filter_map(|bar| bar.sums().high_low()).map(|pair| pair.high()).max();
            prop_assert_eq!(daily[0].sums().high_low().map(|pair| pair.high()), high);
        }
    }
}
