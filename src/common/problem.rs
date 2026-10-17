//! What a decision asks of a solver: the strategy's objective, the risk it is held to, and the prices risk reads.

use std::collections::BTreeMap;

use crate::common::book::{Book, Cash, ValuationRefusal};
use crate::common::market::{Price, Symbol};
use crate::common::risk::{Limits, Risk};
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
    /// Every symbol the objective holds, unless risk is flat and reads none.
    prices: BTreeMap<Symbol, Price>,
}

impl Problem {
    pub fn objective(&self) -> &Objective {
        &self.objective
    }

    pub fn risk(&self) -> Risk {
        self.risk
    }

    /// The price of a symbol the objective holds, present whenever risk is capped.
    pub fn price(&self, symbol: &Symbol) -> Option<Price> {
        self.prices.get(symbol).copied()
    }
}

/// The problem of reaching `wanted` within `limits`, given where the session stands, the equity it opened with, the
/// book now and a price for every symbol held or wanted; refused when capped risk meets a symbol with no price.
pub fn formulate(
    limits: &Limits,
    phase: SessionPhase,
    opening: Cash,
    book: &Book,
    price: impl Fn(&Symbol) -> Option<Price>,
    wanted: Target,
) -> Result<Problem, ValuationRefusal> {
    let risk = limits.risk(phase, opening, book, &price)?;
    let prices = match risk {
        Risk::Flat(_) => BTreeMap::new(),
        Risk::Capped { .. } => wanted
            .holdings()
            .keys()
            .map(|symbol| {
                let price = price(symbol).ok_or_else(|| ValuationRefusal::Unpriced {
                    symbol: symbol.clone(),
                })?;
                Ok((symbol.clone(), price))
            })
            .collect::<Result<_, _>>()?,
    };
    Ok(Problem {
        objective: Objective::Nearest(wanted),
        risk,
        prices,
    })
}
