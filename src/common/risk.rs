//! The risk layer a strategy's target passes through: flat outside the session's trading window and past the
//! daily loss limit, then each name and the whole book capped. It only ever lowers a holding, and is idempotent.

use std::collections::BTreeMap;

use chrono::TimeDelta;
use serde::{Deserialize, Serialize};

use crate::common::book::{Book, Cash, ValuationRefusal};
use crate::common::market::{Price, Shares, Symbol};
use crate::common::strategy::Target;
use crate::common::time::calendar::SessionPhase;

/// The grid a gross cut's scale is taken on; flooring onto it keeps a cut target inside the limit.
const SCALE_GRID: u128 = 100_000_000;

/// The largest dollar limit, in cash units, whose gross scale `limit * SCALE_GRID` still fits u128.
const MAXIMUM_LIMIT: u128 = u128::MAX / SCALE_GRID;

/// The fund's limits on what a target may hold and when, each required and none defaulted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    gross: Cash,
    per_name: Cash,
    daily_loss: Cash,
    flat_before_close: TimeDelta,
}

/// Which of the fund's dollar limits a refusal names.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Limit {
    Gross,
    PerName,
    DailyLoss,
}

/// Why limits were refused, naming the value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LimitsRefusal {
    NotPositive {
        limit: Limit,
        value: Cash,
    },
    /// Past `MAXIMUM_LIMIT`, the most a gross limit can be and still scale a target; held to every dollar limit.
    BeyondRange {
        limit: Limit,
        value: Cash,
    },
    NegativeWindow {
        flat_before_close: TimeDelta,
    },
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
        for (limit, value) in [
            (Limit::Gross, gross),
            (Limit::PerName, per_name),
            (Limit::DailyLoss, daily_loss),
        ] {
            if value.units() <= 0 {
                return Err(LimitsRefusal::NotPositive { limit, value });
            }
            if value.units().cast_unsigned() > MAXIMUM_LIMIT {
                return Err(LimitsRefusal::BeyondRange { limit, value });
            }
        }
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
}

/// A target after risk, with every cut that changed it, in the order they applied.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Restrained {
    target: Target,
    cuts: Vec<Cut>,
}

impl Restrained {
    pub fn target(&self) -> &Target {
        &self.target
    }

    pub fn cuts(&self) -> &[Cut] {
        &self.cuts
    }
}

/// A decision as journaled: the strategy's target and what risk made of it, or why risk could not price it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TargetDecided {
    wanted: Target,
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
    pub fn new(wanted: Target, restrained: Result<Restrained, ValuationRefusal>) -> Self {
        let decision = match restrained {
            Ok(restrained) => Decision::Restrained(restrained),
            Err(refusal) => Decision::Refused(refusal),
        };
        Self { wanted, decision }
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

/// `target` within `limits`, given where the session stands, the equity it opened with, the book now and a price for
/// every symbol held or wanted; refused when one of them has no price, since an unpriced exposure cannot be capped.
pub fn risk(
    limits: &Limits,
    phase: SessionPhase,
    opening: Cash,
    book: &Book,
    price: impl Fn(&Symbol) -> Option<Price>,
    target: Target,
) -> Result<Restrained, ValuationRefusal> {
    let trading = match phase {
        SessionPhase::Open { until_close } => until_close > limits.flat_before_close,
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
    if loss >= limits.daily_loss {
        return Ok(flat(Cut::LossLimit { loss }));
    }
    let mut cuts = Vec::new();
    let mut holdings = BTreeMap::new();
    let mut worth = 0_u128;
    for (symbol, wanted) in target.holdings() {
        let ticks = ticks(&price, symbol)?;
        let affordable = units(limits.per_name) / ticks;
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
    let gross = units(limits.gross);
    if worth <= gross {
        let target = Target::new(
            holdings
                .into_iter()
                .map(|(symbol, (kept, _))| (symbol, kept))
                .collect(),
        );
        return Ok(Restrained { target, cuts });
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
    Ok(Restrained { target, cuts })
}

fn flat(cut: Cut) -> Restrained {
    Restrained {
        target: Target::default(),
        cuts: vec![cut],
    }
}

fn ticks(
    price: impl Fn(&Symbol) -> Option<Price>,
    symbol: &Symbol,
) -> Result<u128, ValuationRefusal> {
    let price = price(symbol).ok_or_else(|| ValuationRefusal::Unpriced {
        symbol: symbol.clone(),
    })?;
    Ok(u128::from(price.ticks().unsigned_abs()))
}

/// A limit's units, positive by `Limits::new`.
fn units(limit: Cash) -> u128 {
    u128::try_from(limit.units()).expect("a limit is positive")
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;
    use crate::common::book::Position;

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
        assert!(matches!(restrained.cuts().last(), Some(Cut::Gross { .. })));
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
                Ok(Restrained {
                    target: Target::default(),
                    cuts: vec![Cut::OutsideTradingWindow { phase }]
                }),
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
            .target,
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
            Ok(Restrained {
                target: Target::default(),
                cuts: vec![Cut::LossLimit {
                    loss: dollars(1_000)
                }]
            })
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
            Restrained {
                target: wanted,
                cuts: vec![]
            }
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
            Ok(Restrained {
                target: target(&[("SPY", 3_333_333), ("AAPL", 5_000_000)]),
                cuts: vec![Cut::PerName {
                    symbol: symbol("SPY"),
                    wanted: Shares::from_units(10_000_000),
                    kept: Shares::from_units(3_333_333),
                }],
            })
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
            Ok(Restrained {
                target: target(&[("SPY", 1_250_000), ("AAPL", 2_500_000)]),
                cuts: vec![Cut::Gross {
                    wanted: dollars(1_600)
                }],
            })
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

    fn worth(target: &Target, prices: &BTreeMap<Symbol, Price>) -> Vec<u128> {
        target
            .holdings()
            .iter()
            .map(|(symbol, shares)| {
                u128::from(prices[symbol].ticks().unsigned_abs()) * u128::from(shares.units())
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
            for (symbol, kept) in once.target.holdings() {
                prop_assert!(*kept <= wanted.holdings()[symbol]);
            }
            let worths = worth(&once.target, &prices);
            prop_assert!(worths.iter().all(|value| *value <= dollars(per_name).units().cast_unsigned()));
            prop_assert!(worths.iter().sum::<u128>() <= dollars(gross).units().cast_unsigned());
            let twice = risk(&limits, open(), dollars(10_000), &funded(10_000), price, once.target.clone()).unwrap();
            prop_assert_eq!(twice, Restrained { target: once.target, cuts: vec![] });
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
                Ok(Restrained { target: wanted, cuts: vec![] })
            );
        }
    }
}
