//! A strategy that knows nothing: each decision holds each symbol of its universe on a seeded coin flip. Over flat
//! prices it loses exactly its costs; over moving prices it earns its exposure, so compare it with holding its expected
//! position.

use std::collections::{BTreeMap, BTreeSet};

use crate::common::book::Book;
use crate::common::laboratory::permutation::Generator;
use crate::common::market::state::MarketState;
use crate::common::market::{Shares, Symbol};
use crate::common::strategy::{Strategy, Target};

/// Holds `shares` of each of `universe` on a coin drawn from the seed and the decision's clock, so a replay is
/// reproducible from its seed and each decision draws afresh; decisions within one second draw alike.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Noise {
    universe: BTreeSet<Symbol>,
    shares: Shares,
    seed: u64,
}

impl Noise {
    pub fn new(universe: BTreeSet<Symbol>, shares: Shares, seed: u64) -> Self {
        Self {
            universe,
            shares,
            seed,
        }
    }
}

impl Strategy for Noise {
    /// Holds nothing before any clock, which a replay always reports before deciding.
    fn decide(&self, state: &MarketState, _: &Book) -> Target {
        let Some(clock) = state.clock() else {
            return Target::default();
        };
        let mut coins = Generator::new(
            Generator::new(self.seed).next_u64() ^ clock.timestamp().cast_unsigned(),
        );
        Target::new(
            self.universe
                .iter()
                .filter(|_| coins.coin())
                .map(|symbol| (symbol.clone(), self.shares))
                .collect::<BTreeMap<_, _>>(),
        )
    }
}

#[cfg(test)]
mod tests {
    use chrono::{DateTime, NaiveDate, Utc};
    use proptest::prelude::{any, prop, prop_assert_eq, proptest};
    use proptest::strategy::Strategy as _;

    use super::*;
    use crate::common::book::Cash;
    use crate::common::laboratory::cost::{BasisPoints, FillStyle};
    use crate::common::market::Price;
    use crate::common::market::record::{Bar, BarInterval, BarPrices};
    use crate::common::market::state::MarketEvent;
    use crate::common::monoid::{Monoid, concatenate};
    use crate::common::replay::{FillModel, Replay, Replayer};
    use crate::common::time::SessionDate;

    const PINNED: [&[&str]; 6] = [
        &["AAPL"],
        &["AAPL"],
        &["AAPL"],
        &[],
        &["AAPL"],
        &["AAPL", "SPY"],
    ];

    fn symbol(raw: &str) -> Symbol {
        Symbol::new(raw).unwrap()
    }

    fn noise(seed: u64) -> Noise {
        Noise::new(
            ["AAPL", "MSFT", "SPY"].into_iter().map(symbol).collect(),
            Shares::whole(1).unwrap(),
            seed,
        )
    }

    fn day(index: i64) -> DateTime<Utc> {
        SessionDate::from_date(NaiveDate::from_ymd_opt(2026, 9, 21).unwrap())
            .plus_calendar_days(index)
            .regular_close()
    }

    fn at(index: i64) -> MarketState {
        MarketState::of(MarketEvent::Clock(day(index)))
    }

    fn daily(raw: &str, index: i64, ticks: i64) -> Bar {
        let price = Price::from_ticks(ticks).unwrap();
        Bar::new(
            symbol(raw),
            BarInterval::OneDay,
            day(index),
            BarPrices::new(price, price, price, price).unwrap(),
            Shares::whole(100).unwrap(),
            None,
            None,
        )
        .unwrap()
    }

    /// Daily bars at arbitrary prices over a stream of any length, with sessions skipped and any of the three symbols
    /// missing from any session.
    fn arbitrary_stream() -> impl prop::strategy::Strategy<Value = Vec<Bar>> {
        prop::collection::vec(
            (
                1..4i64,
                prop::collection::vec(prop::option::of(1..500_000_000i64), 3),
            ),
            0..25,
        )
        .prop_map(|sessions| {
            let mut index = 0;
            let mut bars = Vec::new();
            for (gap, prices) in sessions {
                index += gap;
                for (raw, ticks) in ["AAPL", "MSFT", "SPY"].into_iter().zip(prices) {
                    bars.extend(ticks.map(|ticks| daily(raw, index, ticks)));
                }
            }
            bars
        })
    }

    fn costs(replay: &Replay) -> Cash {
        concatenate(
            replay
                .fills()
                .iter()
                .map(|fill| Cash::from_units(-i128::try_from(fill.cost().units()).unwrap())),
        )
    }

    /// The draws for a seed are part of a replay's record, so they are pinned and may never change.
    #[test]
    fn test_the_draws_for_a_seed_never_change() {
        let held: Vec<Vec<String>> = (0..6)
            .map(|index| {
                noise(7)
                    .decide(&at(index), &Book::empty())
                    .holdings()
                    .keys()
                    .map(Symbol::to_string)
                    .collect()
            })
            .collect();
        assert_eq!(held, PINNED);
    }

    #[test]
    fn test_noise_draws_again_from_its_seed_and_clock_and_needs_a_clock() {
        assert_eq!(
            noise(7).decide(&at(0), &Book::empty()),
            noise(7).decide(&at(0), &Book::empty())
        );
        assert_eq!(
            noise(7).decide(&MarketState::empty(), &Book::empty()),
            Target::default()
        );
        let held: Vec<usize> = (0..64)
            .map(|index| noise(7).decide(&at(index), &Book::empty()).holdings().len())
            .collect();
        assert!(held.contains(&0) && held.contains(&3), "{held:?}");
        let seeds: BTreeSet<_> = (0..16)
            .map(|seed| format!("{:?}", noise(seed).decide(&at(0), &Book::empty())))
            .collect();
        assert!(seeds.len() > 1);
    }

    /// The flat-price law is not vacuous: a seed over a whole stream trades and pays, and still ends exactly.
    #[test]
    fn test_noise_over_flat_prices_trades_and_pays() {
        let flat = Price::from_ticks(1_000_000).unwrap();
        let bars = (0..20)
            .flat_map(|index| ["AAPL", "MSFT", "SPY"].map(|raw| daily(raw, index, flat.ticks())));
        let opening = Cash::from_units(1_000_000 * 1_000_000_000_000);
        let replay = Replayer::new(
            noise(3),
            FillModel::new(FillStyle::Aggressive, BasisPoints::new(10.0).unwrap()).unwrap(),
            BarInterval::OneDay,
        )
        .act(Replay::open(Book::funded(opening)), bars)
        .unwrap();
        assert!(replay.fills().len() > 5, "{}", replay.fills().len());
        assert!(costs(&replay) < Cash::empty());
        assert_eq!(
            replay.book().value(|_| Some(flat)),
            Ok(opening.combine(costs(&replay)))
        );
    }

    /// The control can fail: over a stream where every price doubles, noise's gross return is not zero.
    #[test]
    fn test_noise_over_moving_prices_earns_more_than_its_costs() {
        let bars = (0..20).flat_map(|index| {
            ["AAPL", "MSFT", "SPY"].map(|raw| daily(raw, index, 1_000_000 << (index / 5)))
        });
        let opening = Cash::from_units(1_000_000 * 1_000_000_000_000);
        let replay = Replayer::new(
            noise(3),
            FillModel::new(FillStyle::Aggressive, BasisPoints::new(0.0).unwrap()).unwrap(),
            BarInterval::OneDay,
        )
        .act(Replay::open(Book::funded(opening)), bars)
        .unwrap();
        assert!(!replay.fills().is_empty());
        let last = *replay.marks().last_key_value().unwrap().1.as_ref().unwrap();
        assert!(last > opening, "{last:?}");
    }

    proptest! {
        /// Where no price moves, noise ends at its opening less exactly its costs, whatever the seed and spread.
        #[test]
        fn property_noise_over_flat_prices_loses_exactly_its_costs(
            bars in arbitrary_stream(),
            seed in any::<u64>(),
            spread in 0.0..100.0f64,
            ticks in 1..500_000_000i64,
        ) {
            let flat = Price::from_ticks(ticks).unwrap();
            let opening = Cash::from_units(1_000_000_000 * 1_000_000_000_000);
            let replay = Replayer::new(
                noise(seed),
                FillModel::new(FillStyle::Aggressive, BasisPoints::new(spread).unwrap()).unwrap(),
                BarInterval::OneDay,
            )
            .act(Replay::open(Book::funded(opening)), bars.iter().map(|bar| bar.at_price(flat)))
            .unwrap()
            .finish();
            prop_assert_eq!(replay.book().value(|_| Some(flat)), Ok(opening.combine(costs(&replay))));
        }
    }
}
