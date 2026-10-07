//! Trade bars: what a session's prints, extended hours and closing cross included, add up to under the consolidated
//! tape's condition rules, as exact totals and the prices only eligible prints may set, rolling up from one minute to five and the day.

use std::collections::BTreeMap;

use chrono::{DateTime, TimeDelta, Timelike, Utc};

use super::aggregate::TradeTotals;
use super::record::{BarInterval, Trade};
use super::{DollarVolume, Price, Shares, Symbol, TradeCount};
use crate::common::monoid::Monoid;
use crate::common::time::SessionDate;

/// What a print carrying one sale condition may update on the consolidated tape.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UpdateRules {
    volume: bool,
    high_low: bool,
    open_close: bool,
}

impl UpdateRules {
    pub fn new(volume: bool, high_low: bool, open_close: bool) -> Self {
        Self {
            volume,
            high_low,
            open_close,
        }
    }

    pub fn volume(&self) -> bool {
        self.volume
    }

    pub fn high_low(&self) -> bool {
        self.high_low
    }

    pub fn open_close(&self) -> bool {
        self.open_close
    }

    /// The updates both rules allow.
    fn and(self, other: Self) -> Self {
        Self::new(
            self.volume && other.volume,
            self.high_low && other.high_low,
            self.open_close && other.open_close,
        )
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
    fn regular_sale(self) -> char {
        match self {
            Self::ConsolidatedTape => ' ',
            Self::UnlistedTrading => '@',
        }
    }
}

/// The condition letter `spelled` holds, or `None` unless it is exactly one character.
pub fn condition_letter(spelled: &str) -> Option<char> {
    let mut characters = spelled.chars();
    match (characters.next(), characters.next()) {
        (Some(letter), None) => Some(letter),
        (None, _) | (Some(_), Some(_)) => None,
    }
}

/// One sale condition: the rules it imposes and the letter each tape spells it with.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Condition {
    rules: UpdateRules,
    consolidated_tape: Option<char>,
    unlisted_trading: Option<char>,
    /// The vendor keeps the code for history; a current print spelled with its letter means the current condition.
    retired: bool,
}

impl Condition {
    pub fn new(
        rules: UpdateRules,
        consolidated_tape: Option<char>,
        unlisted_trading: Option<char>,
        retired: bool,
    ) -> Self {
        Self {
            rules,
            consolidated_tape,
            unlisted_trading,
            retired,
        }
    }

    pub fn rules(&self) -> UpdateRules {
        self.rules
    }

    pub fn consolidated_tape(&self) -> Option<char> {
        self.consolidated_tape
    }

    pub fn unlisted_trading(&self) -> Option<char> {
        self.unlisted_trading
    }

    pub fn retired(&self) -> bool {
        self.retired
    }

    fn letter(&self, tape: Tape) -> Option<char> {
        match tape {
            Tape::ConsolidatedTape => self.consolidated_tape,
            Tape::UnlistedTrading => self.unlisted_trading,
        }
    }
}

/// The vendor's sale conditions by code, with each tape letter's rules indexed from them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TradeConditions {
    conditions: BTreeMap<u16, Condition>,
    /// Derived from `conditions` at construction: the rules a current condition's letter imposes on each tape, or
    /// `None` where two current conditions share the letter and disagree.
    letters: BTreeMap<(Tape, char), Option<UpdateRules>>,
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

/// Why a print's condition could not be placed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Unplaced {
    UnknownCode {
        code: u16,
    },
    UnknownLetter {
        tape: Tape,
        letter: char,
    },
    /// Two current conditions share the letter with different rules.
    AmbiguousLetter {
        tape: Tape,
        letter: char,
    },
}

impl TradeConditions {
    pub fn new(conditions: BTreeMap<u16, Condition>) -> Self {
        let mut letters: BTreeMap<(Tape, char), Option<UpdateRules>> = BTreeMap::new();
        for condition in conditions.values().filter(|condition| !condition.retired) {
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

    pub fn conditions(&self) -> &BTreeMap<u16, Condition> {
        &self.conditions
    }

    /// The rules a print with numeric `codes` falls under: each update allowed only if every code allows it.
    pub fn eligibility(&self, codes: &[u16]) -> Eligibility {
        let mut allowed = UpdateRules::new(true, true, true);
        for code in codes {
            match self.conditions.get(code) {
                Some(condition) => allowed = allowed.and(condition.rules),
                None => return Eligibility::Unresolved(Unplaced::UnknownCode { code: *code }),
            }
        }
        Eligibility::Resolved(allowed)
    }

    /// The rules a print reported on `tape` with condition `letters` falls under; the regular-sale letter adds none.
    pub fn eligibility_of_letters(&self, tape: Tape, letters: &[char]) -> Eligibility {
        let mut allowed = UpdateRules::new(true, true, true);
        for letter in letters
            .iter()
            .filter(|letter| **letter != tape.regular_sale())
        {
            match self.letters.get(&(tape, *letter)) {
                Some(Some(rules)) => allowed = allowed.and(*rules),
                Some(None) => {
                    return Eligibility::Unresolved(Unplaced::AmbiguousLetter {
                        tape,
                        letter: *letter,
                    });
                }
                None => {
                    return Eligibility::Unresolved(Unplaced::UnknownLetter {
                        tape,
                        letter: *letter,
                    });
                }
            }
        }
        Eligibility::Resolved(allowed)
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
    open: (DateTime<Utc>, Price),
    close: (DateTime<Utc>, Price),
}

/// Why an open and close were refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpenCloseRefusal {
    /// The open orders after the close, by instant and then price.
    Inverted {
        open: (DateTime<Utc>, Price),
        close: (DateTime<Utc>, Price),
    },
}

impl OpenClose {
    pub fn new(
        open: (DateTime<Utc>, Price),
        close: (DateTime<Utc>, Price),
    ) -> Result<Self, OpenCloseRefusal> {
        if open > close {
            return Err(OpenCloseRefusal::Inverted { open, close });
        }
        Ok(Self { open, close })
    }

    fn of(print: &Print) -> Self {
        let point = (print.timestamp(), print.price());
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
    pub fn open(&self) -> (DateTime<Utc>, Price) {
        self.open
    }

    /// The closing print's time and price.
    pub fn close(&self) -> (DateTime<Utc>, Price) {
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

    fn combine(self, other: Self) -> Self {
        Self {
            totals: self.totals.combine(other.totals),
            open_close: either(self.open_close, other.open_close, OpenClose::combine),
            high_low: either(self.high_low, other.high_low, HighLow::combine),
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

impl TradeBar {
    pub fn new(
        symbol: Symbol,
        interval: BarInterval,
        timestamp: DateTime<Utc>,
        sums: TradeSums,
    ) -> Result<Self, TradeBarRefusal> {
        if bucket(timestamp, interval) != timestamp {
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
/// bars can be diffed against the archive's for the same minutes.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct BarBuilt {
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

impl BarBuilt {
    pub fn of(bar: &TradeBar) -> Self {
        let totals = bar.sums.totals;
        let open_close = bar.sums.open_close;
        let high_low = bar.sums.high_low;
        Self {
            symbol: bar.symbol.clone(),
            interval: bar.interval,
            timestamp: bar.timestamp,
            trade_count: totals.count(),
            volume: totals.volume(),
            dollar_volume: totals.dollar_volume(),
            opened_at: open_close.map(|prices| prices.open.0),
            open: open_close.map(|prices| prices.open.1),
            closed_at: open_close.map(|prices| prices.close.0),
            close: open_close.map(|prices| prices.close.1),
            high: high_low.map(|prices| prices.high()),
            low: high_low.map(|prices| prices.low()),
        }
    }
}

/// The bucket an instant falls in at `interval`; a daily bucket is its session's close.
fn bucket(instant: DateTime<Utc>, interval: BarInterval) -> DateTime<Utc> {
    let minute = instant
        .with_second(0)
        .and_then(|instant| instant.with_nanosecond(0))
        .expect("zero seconds and nanoseconds exist in every minute");
    match interval {
        BarInterval::OneMinute => minute,
        BarInterval::FiveMinute => minute - TimeDelta::minutes(i64::from(minute.minute() % 5)),
        BarInterval::OneDay => SessionDate::at(instant).regular_close(),
    }
}

/// Trade bars built so far, one per symbol, interval and bucket.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TradeRollup(BTreeMap<(Symbol, BarInterval, DateTime<Utc>), TradeSums>);

/// Why a trade bar could not be rolled up.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TradeRollupRefusal {
    Finer { from: BarInterval, to: BarInterval },
}

impl TradeRollup {
    /// The fragment `bar` contributes to the `interval` bar containing it.
    pub fn of(bar: &TradeBar, interval: BarInterval) -> Result<Self, TradeRollupRefusal> {
        if interval < bar.interval {
            return Err(TradeRollupRefusal::Finer {
                from: bar.interval,
                to: interval,
            });
        }
        let key = (
            bar.symbol.clone(),
            interval,
            bucket(bar.timestamp, interval),
        );
        Ok(Self(BTreeMap::from([(key, bar.sums)])))
    }

    fn print(print: &Print, allowed: UpdateRules) -> Self {
        let key = (
            print.symbol().clone(),
            BarInterval::OneMinute,
            bucket(print.timestamp(), BarInterval::OneMinute),
        );
        Self(BTreeMap::from([(key, TradeSums::of(print, allowed))]))
    }

    /// Takes out every bar that has ended by `through`, leaving the rest.
    fn split_through(&mut self, through: DateTime<Utc>) -> Self {
        let (ended, open) = std::mem::take(&mut self.0)
            .into_iter()
            .partition(|((_, interval, timestamp), _)| interval.ends(*timestamp) <= through);
        self.0 = open;
        Self(ended)
    }

    /// Every bar built, ordered by symbol, interval and timestamp.
    pub fn into_bars(self) -> Vec<TradeBar> {
        self.0
            .into_iter()
            .map(|((symbol, interval, timestamp), sums)| TradeBar {
                symbol,
                interval,
                timestamp,
                sums,
            })
            .collect()
    }
}

impl Monoid for TradeRollup {
    fn empty() -> Self {
        Self::default()
    }

    fn combine(mut self, other: Self) -> Self {
        for (key, sums) in other.0 {
            let merged = match self.0.remove(&key) {
                Some(existing) => existing.combine(sums),
                None => sums,
            };
            self.0.insert(key, merged);
        }
        self
    }
}

/// What a session's fold did with the prints it was offered.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TradeFoldCounts {
    folded: u64,
    /// Stamped for another session's Eastern date.
    other_session: u64,
    /// Withdrawn by a later correction or cancel.
    withdrawn: u64,
    volume_ineligible: u64,
    /// Published with no shares, which may set prices and never volume.
    unsized_prints: u64,
    unresolved: u64,
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

    pub fn unresolved(&self) -> u64 {
        self.unresolved
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
    minutes: TradeRollup,
    /// The cutoff the latest `drain_through` set: every minute ending by it is closed to further prints.
    drained_through: Option<DateTime<Utc>>,
    counts: TradeFoldCounts,
}

impl TradeFold {
    pub fn new(session: SessionDate, conditions: TradeConditions) -> Self {
        Self {
            session,
            conditions,
            minutes: TradeRollup::empty(),
            drained_through: None,
            counts: TradeFoldCounts::default(),
        }
    }

    /// Folds one print carrying the vendor's numeric condition `codes`.
    pub fn push(&mut self, print: &Print, codes: &[u16], correction: Correction) {
        let eligibility = self.conditions.eligibility(codes);
        self.push_eligible(print, eligibility, correction);
    }

    /// Folds one print reported on `tape` with condition `letters`, as Alpaca spells them.
    pub fn push_lettered(
        &mut self,
        print: &Print,
        tape: Tape,
        letters: &[char],
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
        let minute_ends =
            BarInterval::OneMinute.ends(bucket(print.timestamp(), BarInterval::OneMinute));
        if self
            .drained_through
            .is_some_and(|drained| minute_ends <= drained)
        {
            self.counts.late += 1;
            return;
        }
        let allowed = match eligibility {
            Eligibility::Resolved(allowed) => allowed,
            Eligibility::Unresolved(_) => {
                self.counts.unresolved += 1;
                UpdateRules::new(true, false, false)
            }
        };
        match print {
            Print::Trade(_) if !allowed.volume => self.counts.volume_ineligible += 1,
            Print::Unsized { .. } => self.counts.unsized_prints += 1,
            Print::Trade(_) => {}
        }
        self.counts.folded += 1;
        let minutes = std::mem::take(&mut self.minutes);
        self.minutes = minutes.combine(TradeRollup::print(print, allowed));
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

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;
    use crate::common::market::Shares;
    use crate::common::monoid::{concatenate, laws};

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
                10,
                Condition::new(UpdateRules::new(true, true, false), None, None, false),
            ),
            (
                15,
                Condition::new(UpdateRules::new(false, false, false), None, None, false),
            ),
            (
                37,
                Condition::new(UpdateRules::new(true, false, false), None, None, false),
            ),
        ]))
    }

    fn october_second() -> SessionDate {
        SessionDate::from_date(chrono::NaiveDate::from_ymd_opt(2026, 10, 2).unwrap())
    }

    fn session() -> TradeFold {
        TradeFold::new(october_second(), conditions())
    }

    #[test]
    fn test_each_price_comes_only_from_prints_allowed_to_set_it() {
        let mut fold = session();
        fold.push(
            &trade("2026-10-02T13:30:01Z", 100.00, 50.0),
            &[37],
            Correction::Stands,
        );
        fold.push(
            &trade("2026-10-02T13:30:02Z", 101.00, 200.0),
            &[],
            Correction::Stands,
        );
        fold.push(
            &trade("2026-10-02T13:30:03Z", 99.00, 100.0),
            &[10],
            Correction::Stands,
        );
        fold.push(
            &trade("2026-10-02T13:30:04Z", 150.00, 900.0),
            &[15],
            Correction::Stands,
        );
        fold.push(
            &trade("2026-10-02T13:30:05Z", 100.50, 10.0),
            &[],
            Correction::Withdrawn,
        );
        fold.push(
            &trade("2026-10-02T13:31:00Z", 100.25, 0.5),
            &[37],
            Correction::Stands,
        );
        let (bars, counts) = fold.finish();
        assert_eq!(bars.len(), 2);
        let first = bars[0].sums();
        assert_eq!(first.totals().count().count(), 3);
        assert_eq!(first.totals().volume().units(), 350_000_000);
        let open_close = first.open_close().unwrap();
        assert_eq!(open_close.open().1.ticks(), 101_000_000);
        assert_eq!(open_close.close().1.ticks(), 101_000_000);
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
                8,
                Condition::new(UpdateRules::new(true, true, true), None, None, false),
            ),
            (
                12,
                Condition::new(UpdateRules::new(true, false, false), None, None, false),
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
            &[8],
            Correction::Stands,
        );
        fold.push(
            &trade("2026-10-02T21:30:00Z", 101.00, 50.0),
            &[12],
            Correction::Stands,
        );
        let minutes = fold.finish().0;
        let daily = concatenate(
            minutes
                .iter()
                .map(|bar| TradeRollup::of(bar, BarInterval::OneDay).unwrap()),
        )
        .into_bars();
        let sums = daily[0].sums();
        assert_eq!(sums.totals().volume().units(), 7_150_000_000);
        assert_eq!(sums.open_close().unwrap().close().1.ticks(), 100_050_000);
        assert_eq!(sums.high_low().unwrap().high().ticks(), 100_050_000);
    }

    #[test]
    fn test_the_unsized_corrected_close_sets_the_close_and_no_volume() {
        // Code 38 as Massive's table gives it: no volume, but the high, low, open and close.
        let conditions = TradeConditions::new(BTreeMap::from([(
            38,
            Condition::new(UpdateRules::new(false, true, true), None, None, false),
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
        fold.push(&corrected_close, &[38], Correction::Stands);
        let (minutes, counts) = fold.finish();
        let daily = concatenate(
            minutes
                .iter()
                .map(|bar| TradeRollup::of(bar, BarInterval::OneDay).unwrap()),
        )
        .into_bars();
        let sums = daily[0].sums();
        assert_eq!(sums.open_close().unwrap().close().1.ticks(), 87_680_000);
        assert_eq!(sums.totals().count().count(), 1);
        assert_eq!(sums.totals().volume().units(), 100_000_000);
        assert_eq!(counts.unsized_prints(), 1);
    }

    #[test]
    fn test_a_letter_reads_as_the_current_condition_it_spells() {
        // As Massive lists them: CTA "I" is the retired CAP election (6) and the odd lot (37); "K" is rules 155 and 127.
        let everything = UpdateRules::new(true, true, true);
        let conditions = TradeConditions::new(BTreeMap::from([
            (6, Condition::new(everything, Some('I'), None, true)),
            (
                37,
                Condition::new(
                    UpdateRules::new(true, false, false),
                    Some('I'),
                    Some('I'),
                    false,
                ),
            ),
            (23, Condition::new(everything, Some('K'), None, false)),
            (24, Condition::new(everything, Some('K'), None, false)),
            (9, Condition::new(everything, None, Some('X'), false)),
            (
                41,
                Condition::new(UpdateRules::new(true, false, true), None, Some('X'), false),
            ),
        ]));
        assert_eq!(
            conditions.eligibility_of_letters(Tape::ConsolidatedTape, &[' ', 'I']),
            Eligibility::Resolved(UpdateRules::new(true, false, false))
        );
        assert_eq!(
            conditions.eligibility_of_letters(Tape::ConsolidatedTape, &['K']),
            Eligibility::Resolved(everything)
        );
        assert_eq!(
            conditions.eligibility_of_letters(Tape::UnlistedTrading, &['@']),
            Eligibility::Resolved(everything)
        );
        assert_eq!(
            conditions.eligibility_of_letters(Tape::UnlistedTrading, &['X']),
            Eligibility::Unresolved(Unplaced::AmbiguousLetter {
                tape: Tape::UnlistedTrading,
                letter: 'X'
            })
        );
        assert_eq!(
            conditions.eligibility_of_letters(Tape::ConsolidatedTape, &['Z']),
            Eligibility::Unresolved(Unplaced::UnknownLetter {
                tape: Tape::ConsolidatedTape,
                letter: 'Z'
            })
        );
    }

    #[test]
    fn test_an_unknown_condition_counts_volume_and_sets_no_price() {
        let mut fold = session();
        fold.push(
            &trade("2026-10-02T13:30:01Z", 100.00, 50.0),
            &[99],
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
        assert_eq!(bars[0].sums().totals().volume().units(), 50_000_000);
        assert_eq!(bars[0].sums().open_close(), None);
        assert_eq!((counts.unresolved(), counts.other_session()), (1, 1));
    }

    #[test]
    fn test_an_open_after_its_close_is_refused() {
        let price = Price::from_dollars(100.00).unwrap();
        let (early, late) = (
            instant("2026-10-02T13:30:00Z"),
            instant("2026-10-02T13:30:01Z"),
        );
        assert!(OpenClose::new((early, price), (late, price)).is_ok());
        assert_eq!(
            OpenClose::new((late, price), (early, price)),
            Err(OpenCloseRefusal::Inverted {
                open: (late, price),
                close: (early, price)
            })
        );
    }

    fn any_rollup() -> impl Strategy<Value = TradeRollup> {
        (
            0_i64..120,
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
                TradeRollup::print(
                    &Print::Trade(trade),
                    UpdateRules::new(volume, high_low, open_close),
                )
            })
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
        let mut five = TradeRollup::of(&minute, BarInterval::FiveMinute).unwrap();
        assert_eq!(
            five.split_through(instant("2026-10-02T13:34:59Z")),
            TradeRollup::empty()
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

    proptest! {
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
            let prints: Vec<(Print, Vec<u16>)> = offsets
                .iter()
                .zip(&codes)
                .map(|(offset, code)| {
                    let at = open + TimeDelta::seconds(*offset);
                    let codes = match code { 0 => vec![], code => vec![*code] };
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
                    handed_out.extend(live.drain_through(bucket(print.timestamp(), BarInterval::OneMinute)));
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

        #[test]
        fn property_trade_rollups_are_a_commutative_monoid(
            first in any_rollup(),
            second in any_rollup(),
            third in any_rollup(),
        ) {
            laws::check(first, second, third)?;
        }

        /// The daily bar's totals are its minutes' totals, whatever the grouping.
        #[test]
        fn property_a_daily_bar_totals_its_minutes(minutes in prop::collection::vec(any_rollup(), 1..30)) {
            let minute_bars = concatenate(minutes).into_bars();
            let daily = concatenate(minute_bars.iter().map(|bar| TradeRollup::of(bar, BarInterval::OneDay).unwrap())).into_bars();
            prop_assert_eq!(daily.len(), 1);
            let volume: u64 = minute_bars.iter().map(|bar| bar.sums().totals().volume().units()).sum();
            prop_assert_eq!(daily[0].sums().totals().volume().units(), volume);
            let high = minute_bars.iter().filter_map(|bar| bar.sums().high_low()).map(|pair| pair.high()).max();
            prop_assert_eq!(daily[0].sums().high_low().map(|pair| pair.high()), high);
        }
    }
}
