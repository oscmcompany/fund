//! The risk a target is held to: flat outside the session's trading window and past the daily loss limit, otherwise
//! each name and the whole book capped, less the names barred by their own loss; and the record of a decision under it.

use std::collections::BTreeMap;

use chrono::{DateTime, TimeDelta, Utc};
use serde::{Deserialize, Serialize};

use crate::common::book::{Book, Cash, Fill, ValuationRefusal};
use crate::common::constraint::{Constraint, Constraints};
use crate::common::market::{Price, Symbol};
use crate::common::monoid::{Monoid, concatenate};
use crate::common::playbook::Stretch;
use crate::common::solver::{Solution, Solver};
use crate::common::strategy::{Holding, Target};
use crate::common::time::calendar::SessionPhase;

/// The grid a gross cut's scale is taken on; flooring onto it keeps a cut target inside the limit.
pub(crate) const SCALE_GRID: u128 = 100_000_000;

/// The largest dollar limit, in cash units, whose gross scale `limit * SCALE_GRID` still fits u128.
const MAXIMUM_LIMIT: u128 = u128::MAX / SCALE_GRID;

/// The fund's limits on what a target may hold and when, each required and none defaulted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    gross: DollarLimit,
    per_name: DollarLimit,
    daily_loss: DollarLimit,
    per_name_loss: DollarLimit,
    flat_before_close: TimeDelta,
}

/// A dollar limit's cash units, positive and at most `MAXIMUM_LIMIT`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "Cash", into = "Cash")]
pub struct DollarLimit(u128);

/// Why an amount cannot be a dollar limit, naming the amount.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum DollarLimitRefusal {
    #[error("{:.2} dollars is not positive", .value.dollars())]
    NotPositive { value: Cash },
    #[error("{:.2} dollars is past the most a limit may be", .value.dollars())]
    BeyondRange { value: Cash },
}

impl TryFrom<Cash> for DollarLimit {
    type Error = DollarLimitRefusal;

    fn try_from(value: Cash) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<DollarLimit> for Cash {
    fn from(limit: DollarLimit) -> Self {
        limit.cash()
    }
}

impl DollarLimit {
    pub fn new(value: Cash) -> Result<Self, DollarLimitRefusal> {
        match u128::try_from(value.units()) {
            Ok(0) | Err(_) => Err(DollarLimitRefusal::NotPositive { value }),
            Ok(units) if units > MAXIMUM_LIMIT => Err(DollarLimitRefusal::BeyondRange { value }),
            Ok(units) => Ok(Self(units)),
        }
    }

    pub fn units(self) -> u128 {
        self.0
    }

    /// Exact, as `MAXIMUM_LIMIT` is under `i128::MAX`.
    fn cash(self) -> Cash {
        Cash::from_units(self.0.cast_signed())
    }
}

/// Which of the fund's dollar limits a refusal names.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    strum::Display,
    strum::EnumString,
    strum::IntoStaticStr,
    strum::EnumIter,
)]
#[strum(serialize_all = "snake_case")]
pub enum Limit {
    Gross,
    PerName,
    DailyLoss,
    PerNameLoss,
}

/// Why limits were refused, naming the value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum LimitsRefusal {
    #[error("the {limit} limit is {:.2} dollars, which is not positive", .value.dollars())]
    NotPositive { limit: Limit, value: Cash },
    /// Past `MAXIMUM_LIMIT`, the most a gross limit can be and still scale a target; held to every dollar limit.
    #[error("the {limit} limit is {:.2} dollars, past the most a limit may be", .value.dollars())]
    BeyondRange { limit: Limit, value: Cash },
    #[error("the flat window of {flat_before_close} before the close is negative")]
    NegativeWindow { flat_before_close: TimeDelta },
}

impl LimitsRefusal {
    fn of(limit: Limit, refusal: DollarLimitRefusal) -> Self {
        match refusal {
            DollarLimitRefusal::NotPositive { value } => Self::NotPositive { limit, value },
            DollarLimitRefusal::BeyondRange { value } => Self::BeyondRange { limit, value },
        }
    }
}

impl Limits {
    /// `gross` caps the target's worth, `per_name` each holding's, `daily_loss` the loss from the session's opening
    /// equity before going flat, `per_name_loss` one name's loss since the open before it is barred for the session,
    /// and `flat_before_close` how long before the close the target is emptied.
    pub fn new(
        gross: Cash,
        per_name: Cash,
        daily_loss: Cash,
        per_name_loss: Cash,
        flat_before_close: TimeDelta,
    ) -> Result<Self, LimitsRefusal> {
        let named = |limit, value| {
            DollarLimit::new(value).map_err(|refusal| LimitsRefusal::of(limit, refusal))
        };
        let gross = named(Limit::Gross, gross)?;
        let per_name = named(Limit::PerName, per_name)?;
        let daily_loss = named(Limit::DailyLoss, daily_loss)?;
        let per_name_loss = named(Limit::PerNameLoss, per_name_loss)?;
        if flat_before_close < TimeDelta::zero() {
            return Err(LimitsRefusal::NegativeWindow { flat_before_close });
        }
        Ok(Self {
            gross,
            per_name,
            daily_loss,
            per_name_loss,
            flat_before_close,
        })
    }

    pub fn per_name(&self) -> DollarLimit {
        self.per_name
    }

    pub fn dollars(&self, limit: Limit) -> Cash {
        match limit {
            Limit::Gross => self.gross.cash(),
            Limit::PerName => self.per_name.cash(),
            Limit::DailyLoss => self.daily_loss.cash(),
            Limit::PerNameLoss => self.per_name_loss.cash(),
        }
    }

    pub fn flat_before_close(&self) -> TimeDelta {
        self.flat_before_close
    }

    /// The risk a target is held to, given where the session stands, the equity it opened with, and the book now;
    /// refused when a holding has no price, since the loss cannot be read.
    pub fn risk(
        &self,
        phase: SessionPhase,
        opening: Cash,
        book: &Book,
        price: impl Fn(&Symbol) -> Option<Price>,
    ) -> Result<Risk, ValuationRefusal> {
        let trading = match phase {
            SessionPhase::Open { until_close } => until_close > self.flat_before_close,
            SessionPhase::NoPublishedSession
            | SessionPhase::BeforeOpen { .. }
            | SessionPhase::AfterClose => false,
        };
        if !trading {
            return Ok(Risk::Flat(Flat::OutsideTradingWindow { phase }));
        }
        let loss = Cash::from_units(
            opening
                .units()
                .checked_sub(book.value(&price)?.units())
                .expect("cash fits i128"),
        );
        if loss >= self.daily_loss.cash() {
            return Ok(Risk::Flat(Flat::LossLimit { loss }));
        }
        Ok(Risk::Capped {
            per_name: self.per_name,
            gross: self.gross,
        })
    }
}

/// What risk allows at one decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Risk {
    Flat(Flat),
    Capped {
        per_name: DollarLimit,
        gross: DollarLimit,
    },
}

/// Why risk allows nothing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Flat {
    /// The session is not open with more than the flat window left before its close.
    OutsideTradingWindow { phase: SessionPhase },
    /// The book has lost `loss` since the session opened, at least the daily limit.
    LossLimit { loss: Cash },
}

impl Risk {
    /// The targets this risk admits.
    pub fn constraints(&self) -> Constraints {
        match *self {
            Self::Flat(_) => Constraints::from(Constraint::Flat),
            Self::Capped { per_name, gross } => concatenate([
                Constraints::from(Constraint::PerName(per_name)),
                Constraints::from(Constraint::Gross(gross)),
            ]),
        }
    }
}

/// Each name's session so far, as its opening position bought at the previous closes combined with every fill in it
/// since, so its value at a price is its gain since the open, realized and unrealized; and each name barred for its loss.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct NameLosses {
    names: BTreeMap<Symbol, Book>,
    /// Each barred name with the loss that barred it; a name once barred stays barred.
    barred: BTreeMap<Symbol, Cash>,
}

/// A name barred for the rest of the session, as its loss since the open reached the per-name loss limit.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NameBarred {
    symbol: Symbol,
    loss: Cash,
    limit: DollarLimit,
}

impl NameLosses {
    /// The session opening with `book`, each position marked at `close`; refused for a position with no close.
    pub fn open(
        book: &Book,
        close: impl Fn(&Symbol) -> Option<Price>,
    ) -> Result<Self, ValuationRefusal> {
        let names = book
            .positions()
            .iter()
            .map(|(symbol, position)| {
                let held = Book::reported(Cash::empty(), [(symbol.clone(), *position)]);
                let paid = negated(held.value(&close)?);
                Ok((symbol.clone(), Book::funded(paid).combine(held)))
            })
            .collect::<Result<_, ValuationRefusal>>()?;
        Ok(Self {
            names,
            barred: BTreeMap::new(),
        })
    }

    /// Folds `fill` into its name, opening one with nothing held for a name the session opened without.
    pub fn take(&mut self, fill: &Fill) {
        let name = self.names.remove(fill.symbol()).unwrap_or_default();
        self.names
            .insert(fill.symbol().clone(), name.combine(Book::of(fill)));
    }

    /// Bars each name whose loss at `price` is at least the per-name loss limit, returning the names barred now; a
    /// name holding a position `price` cannot answer is unmeasured and left as it stands.
    pub fn bar(
        &mut self,
        limits: &Limits,
        price: impl Fn(&Symbol) -> Option<Price>,
    ) -> Vec<NameBarred> {
        let limit = limits.per_name_loss;
        let mut barred = Vec::new();
        for (symbol, name) in &self.names {
            if self.barred.contains_key(symbol) {
                continue;
            }
            let Ok(gain) = name.value(&price) else {
                continue;
            };
            let loss = negated(gain);
            if loss >= limit.cash() {
                barred.push(NameBarred {
                    symbol: symbol.clone(),
                    loss,
                    limit,
                });
            }
        }
        for name in &barred {
            self.barred.insert(name.symbol.clone(), name.loss);
        }
        barred
    }

    /// Each barred name with the loss that barred it.
    pub fn barred(&self) -> &BTreeMap<Symbol, Cash> {
        &self.barred
    }
}

fn negated(cash: Cash) -> Cash {
    Cash::from_units(cash.units().checked_neg().expect("cash fits i128"))
}

/// A decision as journaled: the playbook stretch that decided, the strategy's target, the names it barred, the solver
/// asked, and its solution, or why risk could not price the book.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TargetDecided {
    /// The end of the decision bar decided at; a bar missed by a late call has no record of its own.
    bar: DateTime<Utc>,
    /// `None` when the state had no clock, so no stretch decided.
    stretch: Option<Stretch>,
    wanted: Target,
    /// The names this decision barred for their loss, each journaled once, at the decision that barred it.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    barred: Vec<NameBarred>,
    solver: Solver,
    #[serde(flatten)]
    decision: Decision,
}

/// The decision's outcome as journaled, keyed `solution` or `refused` beside the wanted target.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Decision {
    Solution(Solution),
    Refused(ValuationRefusal),
}

impl TargetDecided {
    pub fn new(
        bar: DateTime<Utc>,
        stretch: Option<Stretch>,
        wanted: Target,
        barred: Vec<NameBarred>,
        solver: Solver,
        solution: Result<Solution, ValuationRefusal>,
    ) -> Self {
        let decision = match solution {
            Ok(solution) => Decision::Solution(solution),
            Err(refusal) => Decision::Refused(refusal),
        };
        Self {
            bar,
            stretch,
            wanted,
            barred,
            solver,
            decision,
        }
    }
}

/// One way a solver lowered a target, with the reading that forced it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Cut {
    /// Flat, as the session is not open with more than the window left before its close.
    OutsideTradingWindow { phase: SessionPhase },
    /// Flat, as the book has lost `loss` since the session opened, at least the daily limit.
    LossLimit { loss: Cash },
    /// `symbol` held at zero, as it has no price and the book does not hold it.
    Unpriced { symbol: Symbol, wanted: Holding },
    /// `symbol` held at zero, as its loss since the open reached `loss`, at least the per-name loss limit, and barred it.
    PerNameLoss {
        symbol: Symbol,
        wanted: Holding,
        loss: Cash,
    },
    /// `symbol` held to the most the per-name limit buys at its price, a short to whole shares.
    PerName {
        symbol: Symbol,
        wanted: Holding,
        kept: Holding,
    },
    /// Every holding scaled down together, as the target's worth `wanted` exceeded the gross limit.
    Gross { wanted: Cash },
}

impl From<Flat> for Cut {
    fn from(flat: Flat) -> Self {
        match flat {
            Flat::OutsideTradingWindow { phase } => Self::OutsideTradingWindow { phase },
            Flat::LossLimit { loss } => Self::LossLimit { loss },
        }
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use std::collections::BTreeMap;

    use super::*;
    use crate::common::book::{Position, Side};
    use crate::common::market::{DollarVolume, Shares};
    use crate::common::problem::formulate;

    const DOLLAR: i128 = 1_000_000_000_000;

    fn symbol(raw: &str) -> Symbol {
        Symbol::new(raw).unwrap()
    }

    fn dollars(count: i128) -> Cash {
        Cash::from_units(count * DOLLAR)
    }

    fn limits(gross: i128, per_name: i128) -> Limits {
        Limits::new(
            dollars(gross),
            dollars(per_name),
            dollars(1_000),
            dollars(1_000),
            TimeDelta::minutes(15),
        )
        .unwrap()
    }

    fn open() -> SessionPhase {
        SessionPhase::Open {
            until_close: TimeDelta::hours(2),
        }
    }

    fn prices(listed: &[(&str, i64)]) -> impl Fn(&Symbol) -> Option<Price> {
        let listed: BTreeMap<Symbol, Price> = listed
            .iter()
            .map(|(raw, ticks)| (symbol(raw), Price::from_ticks(*ticks).unwrap()))
            .collect();
        move |symbol| listed.get(symbol).copied()
    }

    fn target(wanted: &[(&str, u64)]) -> Target {
        Target::new(
            wanted
                .iter()
                .map(|(raw, units)| (symbol(raw), Shares::from_units(*units)))
                .collect(),
        )
    }

    fn funded(count: i128) -> Book {
        Book::funded(dollars(count))
    }

    /// Risk as the trader applies it: formulated, then solved by the clip arm, read as its target and cuts.
    fn risk(
        limits: &Limits,
        phase: SessionPhase,
        opening: Cash,
        book: &Book,
        price: impl Fn(&Symbol) -> Option<Price>,
        target: Target,
    ) -> Result<(Target, Vec<Cut>), ValuationRefusal> {
        formulate(
            limits,
            phase,
            opening,
            book,
            &NameLosses::default(),
            price,
            target,
        )
        .map(|problem| {
            let solution = Solver::Clip.solve(&problem);
            (solution.target().clone(), solution.cuts().to_vec())
        })
    }

    /// `risk` as it stood before formulate and solve split it, frozen as the oracle the split must reproduce.
    fn risk_before_the_split(
        limits: &Limits,
        phase: SessionPhase,
        opening: Cash,
        book: &Book,
        price: impl Fn(&Symbol) -> Option<Price>,
        target: Target,
    ) -> Result<(Target, Vec<Cut>), ValuationRefusal> {
        let trading = match phase {
            SessionPhase::Open { until_close } => until_close > limits.flat_before_close(),
            SessionPhase::NoPublishedSession
            | SessionPhase::BeforeOpen { .. }
            | SessionPhase::AfterClose => false,
        };
        if !trading {
            return Ok(flat(Cut::OutsideTradingWindow { phase }));
        }
        let loss = Cash::from_units(
            opening
                .units()
                .checked_sub(book.value(&price)?.units())
                .expect("cash fits i128"),
        );
        if loss >= limits.dollars(Limit::DailyLoss) {
            return Ok(flat(Cut::LossLimit { loss }));
        }
        let mut cuts = Vec::new();
        let mut holdings = BTreeMap::new();
        let mut worth = 0_u128;
        for (symbol, wanted) in target.holdings() {
            let ticks = ticks(&price, symbol)?;
            let affordable = limits.dollars(Limit::PerName).units().cast_unsigned() / ticks;
            let wanted = long(*wanted);
            let kept = u128::from(wanted.units()).min(affordable);
            let kept = Shares::from_units(
                u64::try_from(kept).expect("a kept holding is at most the wanted one"),
            );
            if kept != wanted {
                cuts.push(Cut::PerName {
                    symbol: symbol.clone(),
                    wanted: wanted.into(),
                    kept: kept.into(),
                });
            }
            worth = worth
                .checked_add(ticks * u128::from(kept.units()))
                .expect("a target's worth fits u128");
            holdings.insert(symbol.clone(), (kept, ticks));
        }
        let gross = limits.dollars(Limit::Gross).units().cast_unsigned();
        if worth <= gross {
            let target = Target::new(
                holdings
                    .into_iter()
                    .map(|(symbol, (kept, _))| (symbol, kept))
                    .collect(),
            );
            return Ok((target, cuts));
        }
        // Flooring the scale and each holding keeps the cut target's worth at or under the limit.
        let scale = gross
            .checked_mul(SCALE_GRID)
            .expect("a gross limit within MAXIMUM_LIMIT fits u128 on the grid")
            / worth;
        cuts.push(Cut::Gross {
            wanted: Cash::from_units(i128::try_from(worth).expect("a target's worth fits i128")),
        });
        let target = Target::new(
            holdings
                .into_iter()
                .map(|(symbol, (kept, _))| {
                    let scaled = u128::from(kept.units()) * scale / SCALE_GRID;
                    (
                        symbol,
                        Shares::from_units(u64::try_from(scaled).expect("a cut holding fits u64")),
                    )
                })
                .collect(),
        );
        Ok((target, cuts))
    }

    /// The oracle's holding, which predates shorts.
    fn long(holding: Holding) -> Shares {
        match holding {
            Holding::Long(shares) => shares,
            Holding::Short(short) => panic!("the frozen oracle holds no short, given {short:?}"),
        }
    }

    fn flat(cut: Cut) -> (Target, Vec<Cut>) {
        (Target::default(), vec![cut])
    }

    fn ticks(
        price: impl Fn(&Symbol) -> Option<Price>,
        symbol: &Symbol,
    ) -> Result<u128, ValuationRefusal> {
        let price = price(symbol).ok_or_else(|| ValuationRefusal::Unpriced {
            symbol: symbol.clone(),
        })?;
        Ok(u128::from(price.ticks_unsigned()))
    }

    /// A capped risk journals its limits as cash and reads back, and a journaled limit that is not one is refused.
    #[test]
    fn test_capped_risk_round_trips_and_refuses_an_invalid_limit() {
        let capped = Risk::Capped {
            per_name: DollarLimit::new(Cash::from_units(5)).unwrap(),
            gross: DollarLimit::new(Cash::from_units(7)).unwrap(),
        };
        let json = r#"{"capped":{"per_name":"5","gross":"7"}}"#;
        assert_eq!(serde_json::to_string(&capped).unwrap(), json);
        assert_eq!(serde_json::from_str::<Risk>(json).unwrap(), capped);
        assert!(
            serde_json::from_str::<Risk>(r#"{"capped":{"per_name":"0","gross":"7"}}"#).is_err()
        );
    }

    #[test]
    fn test_limits_refuse_a_value_that_is_not_positive() {
        let window = TimeDelta::minutes(15);
        assert_eq!(
            Limits::new(dollars(0), dollars(1), dollars(1), dollars(1), window),
            Err(LimitsRefusal::NotPositive {
                limit: Limit::Gross,
                value: dollars(0)
            })
        );
        assert_eq!(
            Limits::new(dollars(1), dollars(-1), dollars(1), dollars(1), window),
            Err(LimitsRefusal::NotPositive {
                limit: Limit::PerName,
                value: dollars(-1)
            })
        );
        assert_eq!(
            Limits::new(dollars(1), dollars(1), dollars(0), dollars(1), window),
            Err(LimitsRefusal::NotPositive {
                limit: Limit::DailyLoss,
                value: dollars(0)
            })
        );
        assert_eq!(
            Limits::new(dollars(1), dollars(1), dollars(1), dollars(0), window),
            Err(LimitsRefusal::NotPositive {
                limit: Limit::PerNameLoss,
                value: dollars(0)
            })
        );
        assert_eq!(
            Limits::new(
                dollars(1),
                dollars(1),
                dollars(1),
                dollars(1),
                TimeDelta::minutes(-1)
            ),
            Err(LimitsRefusal::NegativeWindow {
                flat_before_close: TimeDelta::minutes(-1)
            })
        );
        assert!(
            Limits::new(
                dollars(1),
                dollars(1),
                dollars(1),
                dollars(1),
                TimeDelta::zero()
            )
            .is_ok()
        );
    }

    #[test]
    fn test_each_limit_is_named_and_a_refusal_reads_with_its_value() {
        use strum::IntoEnumIterator;
        let names: Vec<&'static str> = Limit::iter().map(Into::into).collect();
        for limit in Limit::iter() {
            assert_eq!(limit.to_string().parse(), Ok(limit));
        }
        assert_eq!(names, ["gross", "per_name", "daily_loss", "per_name_loss"]);
        let refused = Limits::new(
            dollars(1),
            dollars(-2),
            dollars(1),
            dollars(1),
            TimeDelta::zero(),
        );
        assert_eq!(
            refused.unwrap_err().to_string(),
            "the per_name limit is -2.00 dollars, which is not positive"
        );
    }

    /// A limit past `u128::MAX / SCALE_GRID` units is refused, and the largest accepted scales the largest target at
    /// the highest price without overflowing.
    #[test]
    fn test_limits_past_what_gross_scaling_can_hold_are_refused() {
        let window = TimeDelta::minutes(15);
        let beyond = Cash::from_units(10_i128.pow(31));
        assert_eq!(
            Limits::new(beyond, beyond, beyond, beyond, window),
            Err(LimitsRefusal::BeyondRange {
                limit: Limit::Gross,
                value: beyond
            })
        );
        let largest = Cash::from_units(3_402_823_669_209_384_634_633_746_074_317);
        assert!(matches!(
            Limits::new(
                largest,
                Cash::from_units(largest.units() + 1),
                largest,
                largest,
                window
            ),
            Err(LimitsRefusal::BeyondRange {
                limit: Limit::PerName,
                ..
            })
        ));
        let limits = Limits::new(largest, largest, largest, largest, window).unwrap();
        let price = prices(&[("SPY", 10_000_000_000_000), ("AAPL", 10_000_000_000_000)]);
        let wanted = target(&[("SPY", u64::MAX), ("AAPL", u64::MAX)]);
        let solved = risk(
            &limits,
            open(),
            largest,
            &Book::funded(largest),
            &price,
            wanted,
        )
        .unwrap();
        assert!(matches!(solved.1.last(), Some(Cut::Gross { .. })));
    }

    /// Flat unless the session is open with more than fifteen minutes left; exactly fifteen is already flat.
    #[test]
    fn test_the_target_is_flat_outside_the_trading_window() {
        let wanted = target(&[("SPY", 1_000_000)]);
        let price = prices(&[("SPY", 500_000_000)]);
        for phase in [
            SessionPhase::NoPublishedSession,
            SessionPhase::BeforeOpen {
                until_open: TimeDelta::minutes(5),
            },
            SessionPhase::AfterClose,
            SessionPhase::Open {
                until_close: TimeDelta::minutes(15),
            },
        ] {
            assert_eq!(
                risk(
                    &limits(10_000, 10_000),
                    phase,
                    dollars(10_000),
                    &funded(10_000),
                    &price,
                    wanted.clone()
                ),
                Ok((Target::default(), vec![Cut::OutsideTradingWindow { phase }])),
                "{phase:?}"
            );
        }
        let later = SessionPhase::Open {
            until_close: TimeDelta::minutes(16),
        };
        assert_eq!(
            risk(
                &limits(10_000, 10_000),
                later,
                dollars(10_000),
                &funded(10_000),
                &price,
                wanted.clone()
            )
            .unwrap()
            .0,
            wanted
        );
    }

    /// The book marked at its prices: a loss of exactly the limit is flat, a dollar less is not.
    #[test]
    fn test_the_target_is_flat_past_the_daily_loss_limit() {
        let wanted = target(&[("SPY", 1_000_000)]);
        let price = prices(&[("SPY", 500_000_000), ("AAPL", 100_000_000)]);
        let holding = |cash: i128| {
            Book::reported(
                dollars(cash),
                [(symbol("AAPL"), Position::from_units(10_000_000))],
            )
        };
        assert_eq!(
            risk(
                &limits(10_000, 10_000),
                open(),
                dollars(10_000),
                &holding(8_000),
                &price,
                wanted.clone()
            ),
            Ok((
                Target::default(),
                vec![Cut::LossLimit {
                    loss: dollars(1_000)
                }]
            ))
        );
        assert_eq!(
            risk(
                &limits(10_000, 10_000),
                open(),
                dollars(10_000),
                &holding(8_001),
                &price,
                wanted.clone()
            )
            .unwrap(),
            (wanted, vec![])
        );
    }

    /// A short counts against the book's worth: 10 AAPL short at $100 owes $1,000, so $10,000 cash is $9,000 of
    /// equity and $1,000 lost from the $10,000 opening, where $10,001 cash loses less.
    #[test]
    fn test_a_short_position_is_valued_as_owed() {
        let wanted = target(&[("SPY", 1_000_000)]);
        let price = prices(&[("SPY", 500_000_000), ("AAPL", 100_000_000)]);
        let holding = |cash: i128| {
            Book::reported(
                dollars(cash),
                [(symbol("AAPL"), Position::from_units(-10_000_000))],
            )
        };
        let flat = risk(
            &limits(10_000, 10_000),
            open(),
            dollars(10_000),
            &holding(10_000),
            &price,
            wanted.clone(),
        );
        assert_eq!(
            flat,
            Ok((
                Target::default(),
                vec![Cut::LossLimit {
                    loss: dollars(1_000)
                }]
            ))
        );
        let kept = risk(
            &limits(10_000, 10_000),
            open(),
            dollars(10_000),
            &holding(10_001),
            &price,
            wanted.clone(),
        );
        assert_eq!(kept.unwrap(), (wanted, vec![]));
    }

    /// $1,000 a name at $300 buys 3.333333 shares, floored onto the share grid.
    #[test]
    fn test_a_name_is_held_to_what_its_limit_buys() {
        let price = prices(&[("SPY", 300_000_000), ("AAPL", 100_000_000)]);
        let wanted = target(&[("SPY", 10_000_000), ("AAPL", 5_000_000)]);
        assert_eq!(
            risk(
                &limits(100_000, 1_000),
                open(),
                dollars(10_000),
                &funded(10_000),
                &price,
                wanted
            ),
            Ok((
                target(&[("SPY", 3_333_333), ("AAPL", 5_000_000)]),
                vec![Cut::PerName {
                    symbol: symbol("SPY"),
                    wanted: Shares::from_units(10_000_000).into(),
                    kept: Shares::from_units(3_333_333).into(),
                }]
            ))
        );
    }

    /// $1,600 wanted against a $1,000 gross limit scales every holding by 0.625.
    #[test]
    fn test_the_whole_target_is_scaled_to_the_gross_limit() {
        let price = prices(&[("SPY", 400_000_000), ("AAPL", 200_000_000)]);
        let wanted = target(&[("SPY", 2_000_000), ("AAPL", 4_000_000)]);
        assert_eq!(
            risk(
                &limits(1_000, 1_000),
                open(),
                dollars(10_000),
                &funded(10_000),
                &price,
                wanted
            ),
            Ok((
                target(&[("SPY", 1_250_000), ("AAPL", 2_500_000)]),
                vec![Cut::Gross {
                    wanted: dollars(1_600)
                }]
            ))
        );
    }

    /// $1,600 wanted against a $1,600 gross limit is already inside it, so nothing is cut.
    #[test]
    fn test_a_target_worth_exactly_the_gross_limit_is_not_cut() {
        let price = prices(&[("SPY", 400_000_000), ("AAPL", 200_000_000)]);
        let wanted = target(&[("SPY", 2_000_000), ("AAPL", 4_000_000)]);
        assert_eq!(
            risk(
                &limits(1_600, 1_000),
                open(),
                dollars(10_000),
                &funded(10_000),
                &price,
                wanted.clone()
            ),
            Ok((wanted, vec![]))
        );
    }

    #[test]
    fn test_an_unpriced_holding_is_refused_and_an_unpriced_want_held_at_zero() {
        let price = prices(&[("SPY", 400_000_000)]);
        let unpriced = |raw: &str| {
            Err(ValuationRefusal::Unpriced {
                symbol: symbol(raw),
            })
        };
        assert_eq!(
            risk(
                &limits(1_000, 1_000),
                open(),
                dollars(10_000),
                &funded(10_000),
                &price,
                target(&[("AAPL", 2), ("SPY", 1)])
            ),
            Ok((
                target(&[("SPY", 1)]),
                vec![Cut::Unpriced {
                    symbol: symbol("AAPL"),
                    wanted: Shares::from_units(2).into(),
                }]
            ))
        );
        let held = Book::reported(dollars(10_000), [(symbol("MSFT"), Position::from_units(1))]);
        assert_eq!(
            risk(
                &limits(1_000, 1_000),
                open(),
                dollars(10_000),
                &held,
                &price,
                target(&[("SPY", 1)])
            ),
            unpriced("MSFT")
        );
    }

    /// A problem under capped risk excludes its unpriced want, so no solver may hold it, and admits the priced one.
    #[test]
    fn test_a_problem_excludes_its_unpriced_want() {
        let price = prices(&[("SPY", 400_000_000)]);
        let problem = formulate(
            &limits(1_000, 1_000),
            open(),
            dollars(10_000),
            &funded(10_000),
            &NameLosses::default(),
            &price,
            target(&[("AAPL", 2), ("SPY", 1)]),
        )
        .unwrap();
        assert_eq!(
            problem
                .constraints()
                .admits(&target(&[("AAPL", 1)]), |_| Price::from_ticks(1).ok()),
            Ok(false)
        );
        assert_eq!(
            problem.constraints().admits(&target(&[("SPY", 1)]), &price),
            Ok(true)
        );
    }

    /// Limits whose only binding one is the loss in any one name.
    fn losing(per_name_loss: i128) -> Limits {
        Limits::new(
            dollars(1_000_000),
            dollars(1_000_000),
            dollars(1_000_000),
            dollars(per_name_loss),
            TimeDelta::minutes(15),
        )
        .unwrap()
    }

    /// `units` millionths of AAPL, short when negative, bought at the previous close of $100.
    fn opened(units: i128) -> NameLosses {
        let book = Book::reported(dollars(0), [(symbol("AAPL"), Position::from_units(units))]);
        NameLosses::open(&book, prices(&[("AAPL", 100_000_000)])).unwrap()
    }

    /// The names barred under a $50 limit when AAPL is marked at `ticks`.
    fn barred_at(mut losses: NameLosses, ticks: i64) -> Vec<NameBarred> {
        losses.bar(&losing(50), prices(&[("AAPL", ticks)]))
    }

    fn barred(loss: Cash) -> Vec<NameBarred> {
        vec![NameBarred {
            symbol: symbol("AAPL"),
            loss,
            limit: DollarLimit::new(dollars(50)).unwrap(),
        }]
    }

    /// Ten long from $100 lose exactly $50 at $95 and are barred; a millionth of a dollar higher loses less.
    #[test]
    fn test_a_long_is_barred_at_exactly_its_loss_limit() {
        assert_eq!(
            barred_at(opened(10_000_000), 95_000_000),
            barred(dollars(50))
        );
        assert_eq!(barred_at(opened(10_000_000), 95_000_001), []);
    }

    /// Ten short from $100 lose exactly $50 at $105 and are barred; a millionth of a dollar lower loses less.
    #[test]
    fn test_a_short_is_barred_at_exactly_its_loss_limit() {
        assert_eq!(
            barred_at(opened(-10_000_000), 105_000_000),
            barred(dollars(50))
        );
        assert_eq!(barred_at(opened(-10_000_000), 104_999_999), []);
    }

    /// Ten long from $100 sold at $95 is a realized $50 loss, read with no price since nothing is held.
    #[test]
    fn test_a_realized_loss_bars_a_name_no_longer_held() {
        let mut losses = opened(10_000_000);
        let sold = Fill::new(
            DateTime::UNIX_EPOCH,
            symbol("AAPL"),
            Side::Sell,
            Shares::whole(10).unwrap(),
            Price::from_ticks(95_000_000).unwrap(),
            DollarVolume::default(),
        )
        .unwrap();
        losses.take(&sold);
        assert_eq!(losses.bar(&losing(50), |_| None), barred(dollars(50)));
    }

    /// A barred name is journaled once, stays barred however its price recovers, and is cut to zero with its loss.
    #[test]
    fn test_a_barred_name_stays_barred_and_is_cut_to_zero() {
        let mut losses = opened(10_000_000);
        assert_eq!(
            losses.bar(&losing(50), prices(&[("AAPL", 90_000_000)])),
            barred(dollars(100))
        );
        let recovered = prices(&[("AAPL", 120_000_000)]);
        assert_eq!(losses.bar(&losing(50), &recovered), []);
        let book = Book::reported(
            dollars(0),
            [(symbol("AAPL"), Position::from_units(10_000_000))],
        );
        let problem = formulate(
            &losing(50),
            open(),
            dollars(0),
            &book,
            &losses,
            &recovered,
            target(&[("AAPL", 10_000_000)]),
        )
        .unwrap();
        let solution = Solver::Clip.solve(&problem);
        assert_eq!(solution.target(), &Target::default());
        assert_eq!(
            solution.cuts(),
            [Cut::PerNameLoss {
                symbol: symbol("AAPL"),
                wanted: Shares::whole(10).unwrap().into(),
                loss: dollars(100),
            }]
        );
    }

    /// A barred name journals its symbol, loss and limit as text and reads back, and a decision barring nothing journals
    /// no `barred` key.
    #[test]
    fn test_a_barred_name_round_trips_beside_its_decision() {
        let decided = |barred| {
            TargetDecided::new(
                DateTime::UNIX_EPOCH,
                None,
                Target::default(),
                barred,
                Solver::Clip,
                Err(ValuationRefusal::Unpriced {
                    symbol: symbol("SPY"),
                }),
            )
        };
        let json = serde_json::to_value(decided(barred(dollars(50)))).unwrap();
        assert_eq!(
            json["barred"],
            serde_json::json!([{"symbol": "AAPL", "loss": "50000000000000", "limit": "50000000000000"}])
        );
        assert_eq!(
            serde_json::from_value::<TargetDecided>(json).unwrap(),
            decided(barred(dollars(50)))
        );
        let none = serde_json::to_value(decided(Vec::new())).unwrap();
        assert!(none.get("barred").is_none());
        assert_eq!(
            serde_json::from_value::<TargetDecided>(none).unwrap(),
            decided(Vec::new())
        );
    }

    const SYMBOLS: [&str; 4] = ["AAPL", "MSFT", "SPY", "QQQ"];

    fn arbitrary_long_target() -> impl Strategy<Value = Target> {
        prop::collection::btree_map(
            prop::sample::select(SYMBOLS.to_vec()).prop_map(symbol),
            (0..2_000_000_000u64).prop_map(Shares::from_units),
            0..4,
        )
        .prop_map(Target::new)
    }

    /// Longs of any millionths beside shorts of whole shares.
    fn arbitrary_target() -> impl Strategy<Value = Target> {
        prop::collection::btree_map(
            prop::sample::select(SYMBOLS.to_vec()).prop_map(symbol),
            prop_oneof![
                (0..2_000_000_000u64).prop_map(|units| Holding::from(Shares::from_units(units))),
                (0..2_000u64)
                    .prop_map(|count| Holding::short(Shares::whole(count).unwrap()).unwrap()),
            ],
            0..4,
        )
        .prop_map(Target::new)
    }

    fn arbitrary_prices() -> impl Strategy<Value = BTreeMap<Symbol, Price>> {
        prop::collection::vec(1..2_000_000_000i64, 4).prop_map(|ticks| {
            SYMBOLS
                .iter()
                .zip(ticks)
                .map(|(raw, ticks)| (symbol(raw), Price::from_ticks(ticks).unwrap()))
                .collect()
        })
    }

    /// One to ten million dollars, spread across scales so per-name and gross each bind alone and together.
    fn arbitrary_dollar_scale() -> impl Strategy<Value = i128> {
        (0..7u32, 1..10i128).prop_map(|(exponent, digit)| digit * 10_i128.pow(exponent))
    }

    fn worth(target: &Target, prices: &BTreeMap<Symbol, Price>) -> Vec<u128> {
        target
            .holdings()
            .iter()
            .map(|(symbol, holding)| {
                u128::from(prices[symbol].ticks_unsigned()) * u128::from(holding.shares().units())
            })
            .collect()
    }

    proptest! {
        /// Risk only moves holdings toward zero on their own side, leaves each name and the whole target within limits
        /// long or short, keeps every short whole, and risk applied to its own output changes nothing.
        #[test]
        fn property_risk_lowers_into_the_limits_and_is_idempotent(
            wanted in arbitrary_target(),
            prices in arbitrary_prices(),
            gross in 1..1_000_000i128,
            per_name in 1..1_000_000i128,
        ) {
            let limits = limits(gross, per_name);
            let price = |symbol: &Symbol| prices.get(symbol).copied();
            let once = risk(&limits, open(), dollars(10_000), &funded(10_000), price, wanted.clone()).unwrap();
            for (symbol, kept) in once.0.holdings() {
                let (kept, wanted) = (kept.position().units(), wanted.holdings()[symbol].position().units());
                prop_assert!(kept.signum() == wanted.signum() && kept.abs() <= wanted.abs());
            }
            let worths = worth(&once.0, &prices);
            prop_assert!(worths.iter().all(|value| *value <= dollars(per_name).units().cast_unsigned()));
            prop_assert!(worths.iter().sum::<u128>() <= dollars(gross).units().cast_unsigned());
            let twice = risk(&limits, open(), dollars(10_000), &funded(10_000), price, once.0.clone()).unwrap();
            prop_assert_eq!(twice, (once.0, vec![]));
        }

        /// Once a name's loss reaches the limit it is barred, and every later decision in the session leaves it flat
        /// however its price moves, while a name never barred is never cut for its loss.
        #[test]
        fn property_a_barred_name_is_flat_for_every_later_decision(
            held in prop::collection::vec(-20i128..=20, 2),
            path in prop::collection::vec(prop::collection::vec(50_000_000..150_000_000i64, 2), 1..12),
            wanted in prop::collection::vec(1..=20u64, 2),
            per_name_loss in 1..200i128,
        ) {
            let names = [symbol("AAPL"), symbol("SPY")];
            let book = Book::reported(
                dollars(0),
                names.iter().cloned().zip(held.iter().map(|count| Position::from_units(count * 1_000_000))),
            );
            let close = |_: &Symbol| Price::from_ticks(100_000_000).ok();
            let opening = book.value(close).unwrap();
            let mut losses = NameLosses::open(&book, close).unwrap();
            let limits = losing(per_name_loss);
            let wanted = Target::new(
                names
                    .iter()
                    .cloned()
                    .zip(wanted.iter().map(|count| Holding::from(Shares::whole(*count).unwrap())))
                    .collect(),
            );
            let mut ever_barred = BTreeMap::new();
            for ticks in path {
                let marks: BTreeMap<Symbol, Price> = names
                    .iter()
                    .cloned()
                    .zip(ticks.iter().map(|tick| Price::from_ticks(*tick).unwrap()))
                    .collect();
                let price = |symbol: &Symbol| marks.get(symbol).copied();
                for name in losses.bar(&limits, price) {
                    prop_assert!(name.loss >= limits.dollars(Limit::PerNameLoss));
                    prop_assert!(ever_barred.insert(name.symbol, name.loss).is_none());
                }
                prop_assert_eq!(losses.barred(), &ever_barred);
                let problem = formulate(&limits, open(), opening, &book, &losses, price, wanted.clone())
                    .unwrap();
                let solution = Solver::Clip.solve(&problem);
                for symbol in &names {
                    let flat = !solution.target().holdings().contains_key(symbol);
                    let cut = solution
                        .cuts()
                        .iter()
                        .any(|cut| matches!(cut, Cut::PerNameLoss { symbol: barred, .. } if barred == symbol));
                    match ever_barred.contains_key(symbol) {
                        true => prop_assert!(flat && cut),
                        false => prop_assert!(!cut),
                    }
                }
            }
        }

        /// A target already within every limit passes untouched.
        #[test]
        fn property_a_target_within_its_limits_passes_unchanged(
            wanted in arbitrary_target(),
            prices in arbitrary_prices(),
        ) {
            let worths = worth(&wanted, &prices);
            let per_name = i128::try_from(worths.iter().copied().max().unwrap_or(0)).unwrap() / DOLLAR + 1;
            let gross = i128::try_from(worths.iter().sum::<u128>()).unwrap() / DOLLAR + 1;
            let price = |symbol: &Symbol| prices.get(symbol).copied();
            prop_assert_eq!(
                risk(&limits(gross, per_name), open(), dollars(10_000), &funded(10_000), price, wanted.clone()),
                Ok((wanted, vec![]))
            );
        }

        /// Formulating then solving with the clip arm reproduces the frozen `risk` over the priced wants, refusals
        /// included, and cuts each unpriced want to zero unless risk is flat.
        #[test]
        fn property_the_clip_arm_reproduces_risk_before_the_split(
            wanted in arbitrary_long_target(),
            listed in prop::collection::vec(prop::option::weighted(0.75, 1..2_000_000_000i64), 4),
            held in prop::collection::vec(prop::option::weighted(0.3, -5_000_000i128..5_000_000), 4),
            gross in arbitrary_dollar_scale(),
            per_name in arbitrary_dollar_scale(),
            daily_loss in 1..2_000i128,
            opening in 9_000..11_000i128,
            phase in prop_oneof![
                1 => Just(SessionPhase::NoPublishedSession),
                1 => Just(SessionPhase::AfterClose),
                1 => (0..60i64).prop_map(|minutes| SessionPhase::BeforeOpen { until_open: TimeDelta::minutes(minutes) }),
                12 => (0..400i64).prop_map(|minutes| SessionPhase::Open { until_close: TimeDelta::minutes(minutes) }),
            ],
            window in 0..30i64,
        ) {
            let prices: BTreeMap<Symbol, Price> = SYMBOLS
                .iter()
                .zip(listed)
                .filter_map(|(raw, ticks)| Some((symbol(raw), Price::from_ticks(ticks?).unwrap())))
                .collect();
            let price = |symbol: &Symbol| prices.get(symbol).copied();
            let book = Book::reported(
                dollars(10_000),
                SYMBOLS
                    .iter()
                    .zip(held)
                    .filter_map(|(raw, units)| Some((symbol(raw), Position::from_units(units?)))),
            );
            let limits = Limits::new(
                dollars(gross),
                dollars(per_name),
                dollars(daily_loss),
                dollars(daily_loss),
                TimeDelta::minutes(window),
            )
            .unwrap();
            let priced = Target::new(
                wanted
                    .holdings()
                    .iter()
                    .filter(|(symbol, _)| price(symbol).is_some())
                    .map(|(symbol, shares)| (symbol.clone(), *shares))
                    .collect(),
            );
            let solved = risk(&limits, phase, dollars(opening), &book, price, wanted.clone());
            let frozen = risk_before_the_split(&limits, phase, dollars(opening), &book, price, priced);
            match (solved, frozen) {
                (Ok((target, cuts)), Ok((frozen_target, frozen_cuts))) => {
                    let (unpriced, rest): (Vec<Cut>, Vec<Cut>) =
                        cuts.into_iter().partition(|cut| matches!(cut, Cut::Unpriced { .. }));
                    prop_assert_eq!(target, frozen_target);
                    prop_assert_eq!(rest, frozen_cuts.clone());
                    let flat = matches!(
                        frozen_cuts.first(),
                        Some(Cut::OutsideTradingWindow { .. } | Cut::LossLimit { .. })
                    );
                    let expected: Vec<Cut> = wanted
                        .holdings()
                        .iter()
                        .filter(|(symbol, _)| !flat && price(symbol).is_none())
                        .map(|(symbol, shares)| Cut::Unpriced { symbol: symbol.clone(), wanted: *shares })
                        .collect();
                    prop_assert_eq!(unpriced, expected);
                }
                (solved, frozen) => prop_assert_eq!(solved, frozen),
            }
        }

        /// The clip arm's target is one the problem admits, unpriced names excluded.
        #[test]
        fn property_the_clip_arm_lands_inside_its_problem(
            wanted in arbitrary_target(),
            prices in arbitrary_prices(),
            unlisted in prop::collection::btree_set(prop::sample::select(SYMBOLS.to_vec()).prop_map(symbol), 0..=2),
            gross in 1..1_000_000i128,
            per_name in 1..1_000_000i128,
            open in prop::bool::ANY,
        ) {
            let phase = match open {
                true => super::tests::open(),
                false => SessionPhase::AfterClose,
            };
            let price = |symbol: &Symbol| prices.get(symbol).filter(|_| !unlisted.contains(symbol)).copied();
            let problem = formulate(
                &limits(gross, per_name),
                phase,
                dollars(10_000),
                &funded(10_000),
                &NameLosses::default(),
                price,
                wanted,
            )
                .unwrap();
            let solved = Solver::Clip.solve(&problem);
            prop_assert_eq!(problem.constraints().admits(solved.target(), price), Ok(true));
        }
    }
}
