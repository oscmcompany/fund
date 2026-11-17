//! A model as a strategy: a component that reads a decision's market state and book and writes a target, so a fitted
//! model replays and compares like any other strategy.

use std::cell::RefCell;
use std::collections::BTreeMap;

use crate::common::book::Book;
use crate::common::learning::component::{Cause, Component, Diagnostics};
use crate::common::market::state::MarketState;
use crate::common::strategy::{Holding, HoldingRefusal, Strategy, Target};

/// What a strategy reads at one decision.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Decision<'a> {
    pub state: &'a MarketState,
    pub book: &'a Book,
}

/// How many refusals of one kind a component made, and the first of them with the numbers that produced it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Refused<R> {
    count: u64,
    first: R,
}

impl<R> Refused<R> {
    pub fn count(&self) -> u64 {
        self.count
    }

    pub fn first(&self) -> &R {
        &self.first
    }
}

/// Decisions read and, by the refusal's kind, how many the component refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Decisions<R: Cause> {
    read: u64,
    refused: BTreeMap<R::Kind, Refused<R>>,
}

impl<R: Cause> Default for Decisions<R> {
    fn default() -> Self {
        Self {
            read: 0,
            refused: BTreeMap::new(),
        }
    }
}

impl<R: Cause> Decisions<R> {
    /// Every decision read, refused or not.
    pub fn read(&self) -> u64 {
        self.read
    }

    pub fn refused(&self) -> &BTreeMap<R::Kind, Refused<R>> {
        &self.refused
    }
}

/// A component as a strategy: a refusal keeps the book as it stands, since going flat would trade on a decision the
/// model refused, and is counted by its kind, so a replay over a model that refused reports how often beside its
/// returns; the component's per-decision diagnostics are not kept.
#[derive(Debug, Clone)]
pub struct Modelled<C, R: Cause> {
    component: C,
    decisions: RefCell<Decisions<R>>,
}

impl<C, R: Cause> Modelled<C, R>
where
    C: for<'a> Component<Decision<'a>, Output = Target, Refusal = R>,
{
    pub fn new(component: C) -> Self {
        Self {
            component,
            decisions: RefCell::new(Decisions::default()),
        }
    }
}

impl<C, R: Cause + Clone> Modelled<C, R> {
    pub fn decisions(&self) -> Decisions<R> {
        self.decisions.borrow().clone()
    }
}

impl<C, R: Cause> Strategy for Modelled<C, R>
where
    C: for<'a> Component<Decision<'a>, Output = Target, Refusal = R>,
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
                decisions
                    .refused
                    .entry(refusal.kind())
                    .and_modify(|refused| refused.count += 1)
                    .or_insert(Refused {
                        count: 1,
                        first: refusal,
                    });
                held(book)
            }
        }
    }
}

/// The target that holds `book` as it stands, a fractional short truncated toward zero to whole shares as `roll` does,
/// and a count past what a holding holds wanted flat.
fn held(book: &Book) -> Target {
    Target::new(
        book.positions()
            .iter()
            .filter_map(|(symbol, position)| {
                let holding = match Holding::try_from(*position) {
                    Ok(holding) => holding,
                    Err(HoldingRefusal::FractionalShort { position }) => {
                        Holding::truncated(position)
                    }
                    Err(HoldingRefusal::BeyondRange { .. }) => return None,
                };
                Some((symbol.clone(), holding))
            })
            .collect(),
    )
}

#[cfg(test)]
mod tests {
    use chrono::DateTime;

    use super::*;
    use crate::common::book::{Cash, Position};
    use crate::common::laboratory::experiment::Name;
    use crate::common::learning::component::{Named, Then, ThenRefusal};
    use crate::common::market::state::MarketEvent;
    use crate::common::market::{Shares, Symbol};

    /// Reads the clock's seconds, refusing a decision before any clock.
    struct Seconds;

    #[derive(Debug, Clone, Copy, PartialEq, thiserror::Error)]
    #[error("no clock was reported")]
    struct NoClock;

    impl Cause for NoClock {
        type Kind = ();

        fn kind(&self) {}
    }

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

    /// Holds one share of SPY on even seconds and refuses odd ones, naming the second.
    struct EvenHolds;

    #[derive(Debug, Clone, Copy, PartialEq, thiserror::Error)]
    #[error("the odd second {seconds}")]
    struct Odd {
        seconds: i64,
    }

    impl Cause for Odd {
        type Kind = ();

        fn kind(&self) {}
    }

    impl Component<i64> for EvenHolds {
        type Output = Target;
        type Refusal = Odd;

        fn apply(&self, seconds: i64, _: &mut Diagnostics) -> Result<Target, Odd> {
            match seconds % 2 {
                0 => Ok(Target::new(BTreeMap::from([(
                    Symbol::new("SPY").unwrap(),
                    Shares::whole(1).unwrap(),
                )]))),
                _ => Err(Odd { seconds }),
            }
        }
    }

    fn at(seconds: i64) -> MarketState {
        MarketState::of(MarketEvent::Clock(
            DateTime::from_timestamp(seconds, 0).unwrap(),
        ))
    }

    /// A wired model decides through the strategy, keeping the book on each refusal and counting it by its kind, the
    /// first of each kind kept with its number.
    #[test]
    fn test_a_modelled_strategy_decides_through_its_component_and_counts_refusals() {
        let strategy = Modelled::new(Then {
            first: Named {
                name: Name::new("clock").unwrap(),
                component: Seconds,
            },
            second: EvenHolds,
        });
        let spy = Symbol::new("SPY").unwrap();
        let qqq = Symbol::new("QQQ").unwrap();
        let book = Book::reported(
            Cash::from_units(0),
            [
                (qqq.clone(), Position::from_units(-3_000_000)),
                (spy.clone(), Position::from_units(2_500_000)),
            ],
        );
        // Handed over by reference as a replay takes it, so the counts stay readable after the strategy is consumed.
        fn decide_all(
            strategy: impl Strategy,
            states: &[MarketState],
            book: &Book,
        ) -> Vec<Vec<(Symbol, i128)>> {
            states
                .iter()
                .map(|state| {
                    strategy
                        .decide(state, book)
                        .holdings()
                        .iter()
                        .map(|(symbol, holding)| (symbol.clone(), holding.position().units()))
                        .collect()
                })
                .collect()
        }
        let decided = decide_all(
            &strategy,
            &[at(2), at(3), MarketState::default(), at(4), at(5)],
            &book,
        );
        let wanted = vec![(spy.clone(), 1_000_000)];
        let kept = vec![(qqq, -3_000_000), (spy, 2_500_000)];
        assert_eq!(
            decided,
            [wanted.clone(), kept.clone(), kept.clone(), wanted, kept]
        );
        let decisions = strategy.decisions();
        assert_eq!(decisions.read(), 5);
        let counted: Vec<_> = decisions
            .refused()
            .iter()
            .map(|(kind, refused)| (kind.clone(), refused.count(), refused.first().clone()))
            .collect();
        assert_eq!(
            counted,
            [
                (ThenRefusal::First(()), 1, ThenRefusal::First(NoClock)),
                (
                    ThenRefusal::Second(()),
                    2,
                    ThenRefusal::Second(Odd { seconds: 3 })
                ),
            ]
        );
    }

    /// A short no broker lends, a fraction of a share, is the one position a refusal does not keep whole: it is
    /// truncated to whole shares rather than flattened.
    #[test]
    fn test_a_refusal_truncates_a_fractional_short_to_whole_shares() {
        let strategy = Modelled::new(Then {
            first: Seconds,
            second: EvenHolds,
        });
        let spy = Symbol::new("SPY").unwrap();
        let qqq = Symbol::new("QQQ").unwrap();
        let book = Book::reported(
            Cash::from_units(0),
            [
                (qqq.clone(), Position::from_units(-2_500_000)),
                (spy.clone(), Position::from_units(4_000_000)),
            ],
        );
        let decided = strategy.decide(&MarketState::default(), &book);
        assert_eq!(
            decided,
            Target::new(BTreeMap::from([
                (qqq, Holding::short(Shares::whole(2).unwrap()).unwrap()),
                (spy, Holding::from(Shares::whole(4).unwrap())),
            ]))
        );
    }
}
