//! The risk a target is held to: flat outside the session's trading window and past the daily loss limit, otherwise
//! each name and the whole book capped; and the record of a decision made under it.

use chrono::{DateTime, TimeDelta, Utc};
use serde::{Deserialize, Serialize};

use crate::common::book::{Book, Cash, ValuationRefusal};
use crate::common::constraint::{Constraint, Constraints};
use crate::common::market::{Price, Shares, Symbol};
use crate::common::monoid::concatenate;
use crate::common::playbook::Stretch;
use crate::common::solver::Solver;
use crate::common::strategy::Target;
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
    /// equity before going flat, and `flat_before_close` how long before the close the target is emptied.
    pub fn new(
        gross: Cash,
        per_name: Cash,
        daily_loss: Cash,
        flat_before_close: TimeDelta,
    ) -> Result<Self, LimitsRefusal> {
        let named = |limit, value| {
            DollarLimit::new(value).map_err(|refusal| LimitsRefusal::of(limit, refusal))
        };
        let gross = named(Limit::Gross, gross)?;
        let per_name = named(Limit::PerName, per_name)?;
        let daily_loss = named(Limit::DailyLoss, daily_loss)?;
        if flat_before_close < TimeDelta::zero() {
            return Err(LimitsRefusal::NegativeWindow { flat_before_close });
        }
        Ok(Self {
            gross,
            per_name,
            daily_loss,
            flat_before_close,
        })
    }

    pub fn dollars(&self, limit: Limit) -> Cash {
        match limit {
            Limit::Gross => self.gross.cash(),
            Limit::PerName => self.per_name.cash(),
            Limit::DailyLoss => self.daily_loss.cash(),
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

/// A target after risk, with every cut that changed it, in the order they applied.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Restrained {
    target: Target,
    cuts: Vec<Cut>,
    /// `None` on a decision journaled before risk was.
    risk: Option<Risk>,
}

impl Restrained {
    pub(crate) fn new(target: Target, cuts: Vec<Cut>, risk: Risk) -> Self {
        Self {
            target,
            cuts,
            risk: Some(risk),
        }
    }

    pub fn target(&self) -> &Target {
        &self.target
    }

    pub fn cuts(&self) -> &[Cut] {
        &self.cuts
    }
}

/// A decision as journaled: the playbook stretch that decided, the strategy's target, the solver asked, and what
/// risk made of the target, or why risk could not price it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TargetDecided {
    /// The end of the decision bar decided at; a bar missed by a late call has no record of its own.
    bar: DateTime<Utc>,
    /// `None` when the state had no clock, so no stretch decided.
    stretch: Option<Stretch>,
    wanted: Target,
    /// `None` on a decision journaled before solvers were.
    solver: Option<Solver>,
    #[serde(flatten)]
    decision: Decision,
}

/// Risk's answer as journaled, keyed `restrained` or `refused` beside the wanted target.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Decision {
    Restrained(Restrained),
    Refused(ValuationRefusal),
}

impl TargetDecided {
    pub fn new(
        bar: DateTime<Utc>,
        stretch: Option<Stretch>,
        wanted: Target,
        solver: Solver,
        restrained: Result<Restrained, ValuationRefusal>,
    ) -> Self {
        let decision = match restrained {
            Ok(restrained) => Decision::Restrained(restrained),
            Err(refusal) => Decision::Refused(refusal),
        };
        Self {
            bar,
            stretch,
            wanted,
            solver: Some(solver),
            decision,
        }
    }
}

/// One way risk lowered a target, with the reading that forced it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Cut {
    /// Flat, as the session is not open with more than the window left before its close.
    OutsideTradingWindow { phase: SessionPhase },
    /// Flat, as the book has lost `loss` since the session opened, at least the daily limit.
    LossLimit { loss: Cash },
    /// `symbol` held to the most the per-name limit buys at its price.
    PerName {
        symbol: Symbol,
        wanted: Shares,
        kept: Shares,
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
    use crate::common::book::Position;
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
        formulate(limits, phase, opening, book, price, target).map(|problem| {
            let restrained = Solver::Clip.solve(&problem);
            (restrained.target().clone(), restrained.cuts().to_vec())
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
            let kept = u128::from(wanted.units()).min(affordable);
            let kept = Shares::from_units(
                u64::try_from(kept).expect("a kept holding is at most the wanted one"),
            );
            if kept != *wanted {
                cuts.push(Cut::PerName {
                    symbol: symbol.clone(),
                    wanted: *wanted,
                    kept,
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
            Limits::new(dollars(0), dollars(1), dollars(1), window),
            Err(LimitsRefusal::NotPositive {
                limit: Limit::Gross,
                value: dollars(0)
            })
        );
        assert_eq!(
            Limits::new(dollars(1), dollars(-1), dollars(1), window),
            Err(LimitsRefusal::NotPositive {
                limit: Limit::PerName,
                value: dollars(-1)
            })
        );
        assert_eq!(
            Limits::new(dollars(1), dollars(1), dollars(0), window),
            Err(LimitsRefusal::NotPositive {
                limit: Limit::DailyLoss,
                value: dollars(0)
            })
        );
        assert_eq!(
            Limits::new(dollars(1), dollars(1), dollars(1), TimeDelta::minutes(-1)),
            Err(LimitsRefusal::NegativeWindow {
                flat_before_close: TimeDelta::minutes(-1)
            })
        );
        assert!(Limits::new(dollars(1), dollars(1), dollars(1), TimeDelta::zero()).is_ok());
    }

    #[test]
    fn test_each_limit_is_named_and_a_refusal_reads_with_its_value() {
        use strum::IntoEnumIterator;
        let names: Vec<&'static str> = Limit::iter().map(Into::into).collect();
        for limit in Limit::iter() {
            assert_eq!(limit.to_string().parse(), Ok(limit));
        }
        assert_eq!(names, ["gross", "per_name", "daily_loss"]);
        let refused = Limits::new(dollars(1), dollars(-2), dollars(1), TimeDelta::zero());
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
            Limits::new(beyond, beyond, beyond, window),
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
                window
            ),
            Err(LimitsRefusal::BeyondRange {
                limit: Limit::PerName,
                ..
            })
        ));
        let limits = Limits::new(largest, largest, largest, window).unwrap();
        let price = prices(&[("SPY", 10_000_000_000_000), ("AAPL", 10_000_000_000_000)]);
        let wanted = target(&[("SPY", u64::MAX), ("AAPL", u64::MAX)]);
        let restrained = risk(
            &limits,
            open(),
            largest,
            &Book::funded(largest),
            &price,
            wanted,
        )
        .unwrap();
        assert!(matches!(restrained.1.last(), Some(Cut::Gross { .. })));
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
                    wanted: Shares::from_units(10_000_000),
                    kept: Shares::from_units(3_333_333),
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
    fn test_an_unpriced_holding_or_want_is_refused() {
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
                target(&[("AAPL", 1)])
            ),
            unpriced("AAPL")
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

    const SYMBOLS: [&str; 4] = ["AAPL", "MSFT", "SPY", "QQQ"];

    fn arbitrary_target() -> impl Strategy<Value = Target> {
        prop::collection::btree_map(
            prop::sample::select(SYMBOLS.to_vec()).prop_map(symbol),
            (0..2_000_000_000u64).prop_map(Shares::from_units),
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
            .map(|(symbol, shares)| {
                u128::from(prices[symbol].ticks_unsigned()) * u128::from(shares.units())
            })
            .collect()
    }

    proptest! {
        /// Risk only lowers holdings, leaves each name and the whole target within limits, and risk applied to its own
        /// output changes nothing.
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
                prop_assert!(*kept <= wanted.holdings()[symbol]);
            }
            let worths = worth(&once.0, &prices);
            prop_assert!(worths.iter().all(|value| *value <= dollars(per_name).units().cast_unsigned()));
            prop_assert!(worths.iter().sum::<u128>() <= dollars(gross).units().cast_unsigned());
            let twice = risk(&limits, open(), dollars(10_000), &funded(10_000), price, once.0.clone()).unwrap();
            prop_assert_eq!(twice, (once.0, vec![]));
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

        /// Formulating then solving with the clip arm reproduces the frozen `risk` on every input, refusals included.
        #[test]
        fn property_the_clip_arm_reproduces_risk_before_the_split(
            wanted in arbitrary_target(),
            listed in prop::collection::vec(prop::option::weighted(0.9, 1..2_000_000_000i64), 4),
            held in prop::collection::vec(-5_000_000i128..5_000_000, 4),
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
                SYMBOLS.iter().zip(held).map(|(raw, units)| (symbol(raw), Position::from_units(units))),
            );
            let limits = Limits::new(
                dollars(gross),
                dollars(per_name),
                dollars(daily_loss),
                TimeDelta::minutes(window),
            )
            .unwrap();
            prop_assert_eq!(
                risk(&limits, phase, dollars(opening), &book, price, wanted.clone()),
                risk_before_the_split(&limits, phase, dollars(opening), &book, price, wanted)
            );
        }

        /// The clip arm's target is one the problem's risk admits.
        #[test]
        fn property_the_clip_arm_lands_inside_its_risk(
            wanted in arbitrary_target(),
            prices in arbitrary_prices(),
            gross in 1..1_000_000i128,
            per_name in 1..1_000_000i128,
            open in prop::bool::ANY,
        ) {
            let phase = match open {
                true => super::tests::open(),
                false => SessionPhase::AfterClose,
            };
            let price = |symbol: &Symbol| prices.get(symbol).copied();
            let problem = formulate(&limits(gross, per_name), phase, dollars(10_000), &funded(10_000), price, wanted)
                .unwrap();
            let solved = Solver::Clip.solve(&problem);
            prop_assert_eq!(problem.risk().constraints().admits(solved.target(), price), Ok(true));
        }
    }
}
