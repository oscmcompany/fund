//! The ways a problem becomes a target: a closed set, each journaled by name beside the decision it made.

use crate::common::book::Cash;
use crate::common::problem::{Objective, Problem};
use crate::common::risk::{Cut, Risk, SCALE_GRID};
use crate::common::strategy::Target;

/// One way to solve a problem.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    strum::Display,
    strum::EnumString,
    strum::IntoStaticStr,
    strum::EnumIter,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum Solver {
    /// Each barred name held at zero, each holding cut to what its per-name limit buys, then the whole target scaled
    /// down to the gross limit.
    Clip,
}

/// A solver's target, with every cut that lowered it from the objective in the order they applied, and the risk it was
/// held to.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Solution {
    target: Target,
    cuts: Vec<Cut>,
    risk: Risk,
}

impl Solution {
    fn new(target: Target, cuts: Vec<Cut>, risk: Risk) -> Self {
        Self { target, cuts, risk }
    }

    pub fn target(&self) -> &Target {
        &self.target
    }

    pub fn cuts(&self) -> &[Cut] {
        &self.cuts
    }
}

impl Solver {
    pub fn solve(self, problem: &Problem) -> Solution {
        match self {
            Self::Clip => clip(problem),
        }
    }
}

fn clip(problem: &Problem) -> Solution {
    let risk = problem.risk();
    let (per_name, gross) = match risk {
        Risk::Flat(flat) => return Solution::new(Target::default(), vec![Cut::from(flat)], risk),
        Risk::Capped { per_name, gross } => (per_name.units(), gross.units()),
    };
    let Objective::Nearest(wanted) = problem.objective();
    let mut cuts = Vec::new();
    let mut holdings = Vec::new();
    let mut worth = 0_u128;
    for (symbol, wanted) in wanted.holdings() {
        if let Some(loss) = problem.barred(symbol) {
            cuts.push(Cut::PerNameLoss {
                symbol: symbol.clone(),
                wanted: *wanted,
                loss,
            });
            continue;
        }
        let Some(price) = problem.price(symbol) else {
            cuts.push(Cut::Unpriced {
                symbol: symbol.clone(),
                wanted: *wanted,
            });
            continue;
        };
        let ticks = u128::from(price.ticks_unsigned());
        let kept = wanted.capped(per_name / ticks);
        if kept != *wanted {
            cuts.push(Cut::PerName {
                symbol: symbol.clone(),
                wanted: *wanted,
                kept,
            });
        }
        worth = worth
            .checked_add(ticks * u128::from(kept.shares().units()))
            .expect("a target's worth fits u128");
        holdings.push((symbol.clone(), kept));
    }
    if worth <= gross {
        return Solution::new(Target::new(holdings.into_iter().collect()), cuts, risk);
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
            .map(|(symbol, kept)| {
                let scaled = u128::from(kept.shares().units()) * scale / SCALE_GRID;
                (symbol, kept.capped(scaled))
            })
            .collect(),
    );
    Solution::new(target, cuts, risk)
}

#[cfg(test)]
mod tests {
    use strum::IntoEnumIterator;

    use super::*;

    /// Every solver's journaled name, pinned so a rename shows up as a changed literal, and serde agreeing with strum.
    #[test]
    fn test_solver_names_round_trip() {
        assert_eq!(
            Solver::iter().map(<&str>::from).collect::<Vec<_>>(),
            ["clip"]
        );
        for solver in Solver::iter() {
            assert_eq!(solver.to_string().parse(), Ok(solver));
            assert_eq!(
                serde_json::to_string(&solver).unwrap(),
                format!("\"{solver}\"")
            );
            assert_eq!(
                serde_json::from_str::<Solver>(&format!("\"{solver}\"")).unwrap(),
                solver
            );
        }
    }
}
