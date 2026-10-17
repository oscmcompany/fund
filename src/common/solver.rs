//! The ways a problem becomes a target: a closed set, each journaled by name beside the decision it made.

use crate::common::book::Cash;
use crate::common::market::Shares;
use crate::common::problem::{Objective, Problem};
use crate::common::risk::{Cut, Restrained, Risk, SCALE_GRID};
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
    /// Each holding cut to what its per-name limit buys, then the whole target scaled down to the gross limit.
    Clip,
}

impl Solver {
    pub fn solve(self, problem: &Problem) -> Restrained {
        match self {
            Self::Clip => clip(problem),
        }
    }
}

fn clip(problem: &Problem) -> Restrained {
    let risk = problem.risk();
    let (per_name, gross) = match risk {
        Risk::Flat(flat) => return Restrained::new(Target::default(), vec![Cut::from(flat)], risk),
        Risk::Capped { per_name, gross } => (per_name.units(), gross.units()),
    };
    let Objective::Nearest(wanted) = problem.objective();
    let mut cuts = Vec::new();
    let mut holdings = Vec::new();
    let mut worth = 0_u128;
    for (symbol, wanted) in wanted.holdings() {
        let ticks = u128::from(
            problem
                .price(symbol)
                .expect("capped risk prices every wanted symbol")
                .ticks_unsigned(),
        );
        let kept = u128::from(wanted.units()).min(per_name / ticks);
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
        holdings.push((symbol.clone(), kept));
    }
    if worth <= gross {
        return Restrained::new(Target::new(holdings.into_iter().collect()), cuts, risk);
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
                let scaled = u128::from(kept.units()) * scale / SCALE_GRID;
                (
                    symbol,
                    Shares::from_units(u64::try_from(scaled).expect("a cut holding fits u64")),
                )
            })
            .collect(),
    );
    Restrained::new(target, cuts, risk)
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
        }
    }
}
