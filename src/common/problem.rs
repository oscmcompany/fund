//! What a decision asks of a solver: the strategy's objective, the risk it is held to, the names barred for their loss,
//! the wanted names it cannot price, and the prices risk reads.

use std::collections::{BTreeMap, BTreeSet};

use crate::common::book::{Book, Cash, ValuationRefusal};
use crate::common::constraint::{Constraint, Constraints};
use crate::common::market::{Price, Symbol};
use crate::common::monoid::concatenate;
use crate::common::risk::{Limits, NameLosses, Risk};
use crate::common::strategy::Target;
use crate::common::time::calendar::SessionPhase;

/// What the strategy wants a solver to reach.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Objective {
    /// The allowed target nearest this one.
    Nearest(Target),
}

/// One decision's objective and risk; risk is required, so no strategy can formulate without it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Problem {
    objective: Objective,
    risk: Risk,
    /// The wanted symbols that have a price, read only when risk is capped.
    prices: BTreeMap<Symbol, Price>,
    /// Each name barred for its loss with the loss that barred it, read only when risk is capped.
    barred: BTreeMap<Symbol, Cash>,
}

impl Problem {
    pub fn objective(&self) -> &Objective {
        &self.objective
    }

    pub fn risk(&self) -> Risk {
        self.risk
    }

    /// The wanted symbols capped risk could not price, which are held at zero; the book holds none of them, or risk
    /// would have refused to value it.
    fn unpriced(&self) -> BTreeSet<&Symbol> {
        match (self.risk, &self.objective) {
            (Risk::Flat(_), _) => BTreeSet::new(),
            (Risk::Capped { .. }, Objective::Nearest(wanted)) => wanted
                .holdings()
                .keys()
                .filter(|symbol| !self.prices.contains_key(*symbol))
                .collect(),
        }
    }

    /// The targets the problem admits: its risk, with every unpriced and every barred symbol excluded.
    pub fn constraints(&self) -> Constraints {
        concatenate(
            self.unpriced()
                .into_iter()
                .chain(self.barred.keys())
                .map(|symbol| Constraints::from(Constraint::Excluded(symbol.clone())))
                .chain([self.risk.constraints()]),
        )
    }

    /// The loss that barred `symbol` for the session, absent when it is not barred or risk is flat.
    pub fn barred(&self, symbol: &Symbol) -> Option<Cash> {
        self.barred.get(symbol).copied()
    }

    /// The price of a wanted symbol, absent when risk is flat or the symbol is unpriced.
    pub fn price(&self, symbol: &Symbol) -> Option<Price> {
        self.prices.get(symbol).copied()
    }
}

/// The problem of reaching `wanted` within `limits`, given where the session stands, the equity it opened with, the
/// book now, the names barred for their loss and a price for every symbol held or wanted; refused when risk must value
/// a holding with no price, while a wanted symbol with no price is held at zero.
pub fn formulate(
    limits: &Limits,
    phase: SessionPhase,
    opening: Cash,
    book: &Book,
    losses: &NameLosses,
    price: impl Fn(&Symbol) -> Option<Price>,
    wanted: Target,
) -> Result<Problem, ValuationRefusal> {
    let risk = limits.risk(phase, opening, book, &price)?;
    let (prices, barred) = match risk {
        Risk::Flat(_) => (BTreeMap::new(), BTreeMap::new()),
        Risk::Capped { .. } => (
            wanted
                .holdings()
                .keys()
                .filter_map(|symbol| Some((symbol.clone(), price(symbol)?)))
                .collect(),
            losses.barred().clone(),
        ),
    };
    Ok(Problem {
        objective: Objective::Nearest(wanted),
        risk,
        prices,
        barred,
    })
}
