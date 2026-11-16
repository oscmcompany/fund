//! A model as a strategy: a component that reads a decision's market state and book and writes a target, so a fitted
//! model replays and compares like any other strategy.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::fmt::Display;

use crate::common::book::Book;
use crate::common::learning::component::{Component, Diagnostics};
use crate::common::market::state::MarketState;
use crate::common::strategy::{Strategy, Target};

/// What a strategy reads at one decision.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Decision<'a> {
    pub state: &'a MarketState,
    pub book: &'a Book,
}

/// Decisions read and, by the refusal's message, how many the component refused.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Decisions {
    read: u64,
    refused: BTreeMap<String, u64>,
}

impl Decisions {
    /// Every decision read, refused or not.
    pub fn read(&self) -> u64 {
        self.read
    }

    pub fn refused(&self) -> &BTreeMap<String, u64> {
        &self.refused
    }
}

/// A component as a strategy: a refusal holds nothing and is counted by its cause, so a replay over a model that
/// refused reports how often beside its returns; the component's per-decision diagnostics are not kept.
#[derive(Debug, Clone)]
pub struct Modelled<C> {
    component: C,
    decisions: RefCell<Decisions>,
}

impl<C> Modelled<C> {
    pub fn new(component: C) -> Self {
        Self {
            component,
            decisions: RefCell::new(Decisions::default()),
        }
    }

    pub fn decisions(&self) -> Decisions {
        self.decisions.borrow().clone()
    }
}

impl<C, Refusal> Strategy for Modelled<C>
where
    C: for<'a> Component<Decision<'a>, Output = Target, Refusal = Refusal>,
    Refusal: Display,
{
    fn decide(&self, state: &MarketState, book: &Book) -> Target {
        let decided = self
            .component
            .apply(Decision { state, book }, &mut Diagnostics::default());
        let mut decisions = self.decisions.borrow_mut();
        decisions.read += 1;
        match decided {
            Ok(target) => target,
            Err(refusal) => {
                *decisions.refused.entry(refusal.to_string()).or_insert(0) += 1;
                Target::default()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use chrono::DateTime;

    use super::*;
    use crate::common::book::Cash;
    use crate::common::laboratory::experiment::Name;
    use crate::common::learning::component::{Named, Then};
    use crate::common::market::state::MarketEvent;
    use crate::common::market::{Shares, Symbol};

    /// Reads the clock's seconds, refusing a decision before any clock.
    struct Seconds;

    #[derive(Debug, Clone, Copy, PartialEq, thiserror::Error)]
    #[error("no clock was reported")]
    struct NoClock;

    impl<'a> Component<Decision<'a>> for Seconds {
        type Output = i64;
        type Refusal = NoClock;

        fn apply(
            &self,
            input: Decision<'a>,
            diagnostics: &mut Diagnostics,
        ) -> Result<i64, NoClock> {
            let seconds = input.state.clock().ok_or(NoClock)?.timestamp();
            diagnostics.record("seconds", seconds as f64);
            Ok(seconds)
        }
    }

    /// Holds one share of SPY on even seconds and refuses odd ones.
    struct EvenHolds;

    #[derive(Debug, Clone, Copy, PartialEq, thiserror::Error)]
    #[error("an odd second")]
    struct Odd;

    impl Component<i64> for EvenHolds {
        type Output = Target;
        type Refusal = Odd;

        fn apply(&self, seconds: i64, _: &mut Diagnostics) -> Result<Target, Odd> {
            match seconds % 2 {
                0 => Ok(Target::new(BTreeMap::from([(
                    Symbol::new("SPY").unwrap(),
                    Shares::whole(1).unwrap(),
                )]))),
                _ => Err(Odd),
            }
        }
    }

    fn at(seconds: i64) -> MarketState {
        MarketState::of(MarketEvent::Clock(
            DateTime::from_timestamp(seconds, 0).unwrap(),
        ))
    }

    /// A wired model decides through the strategy, holding flat on each refusal and counting it by its cause.
    #[test]
    fn test_a_modelled_strategy_decides_through_its_component_and_counts_refusals() {
        let strategy = Modelled::new(Then {
            first: Named {
                name: Name::new("clock").unwrap(),
                component: Seconds,
            },
            second: EvenHolds,
        });
        let book = Book::funded(Cash::from_units(0));
        let held: Vec<usize> = [at(2), at(3), MarketState::default(), at(4), at(5)]
            .iter()
            .map(|state| strategy.decide(state, &book).holdings().len())
            .collect();
        assert_eq!(held, [1, 0, 0, 1, 0]);
        let decisions = strategy.decisions();
        assert_eq!(decisions.read(), 5);
        assert_eq!(
            decisions.refused(),
            &BTreeMap::from([
                (
                    "the first component refused: no clock was reported".to_string(),
                    1
                ),
                ("the second component refused: an odd second".to_string(), 2),
            ])
        );
    }
}
