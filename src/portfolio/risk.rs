//! The risk gate: four conditions, all of which only ever stop an *entry*.
//!
//! Nothing here can block an exit. The book is flat overnight without exception, so a gate that
//! could refuse to close a position would be a way to carry one. Exits run first and
//! unconditionally in [`crate::portfolio::evaluate`]; this is consulted only afterwards.
//!
//! Deliberately small: everything modelling overnight exposure went with the requirement to hold
//! overnight, and the beta-neutral optimizer went with the machinery that estimated the betas.

use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use tracing::{info, warn};

use crate::common::alpaca::AccountSnapshot;
use crate::portfolio::size::{SizedPair, SizingParameters};

/// Fraction of the previous session's equity the account may lose before entries stop.
pub const DRAWDOWN_THRESHOLD: f64 = 0.10;

/// Minutes a pair must be able to hold before the close for it to be worth opening.
///
/// Thirty, against a 15:45 liquidation, so nothing new opens after 15:15. A pair opened later than
/// that pays two spreads to hold a position for minutes, which is a cost with no thesis attached.
pub const MINIMUM_HOLD_MINUTES: i64 = 30;

/// Why the gate refused.
///
/// Each variant carries the numbers that produced it, so the `portfolio_evaluation_completed`
/// payload can say what stopped the pass rather than that something did. A session with no entries
/// and no explanation is indistinguishable from a session with no candidates.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum RiskBlock {
    /// Account equity has fallen too far below the previous session's close.
    Drawdown { observed: f64, threshold: f64 },
    /// The book is already at its pair limit.
    PairCapacity { open: usize, maximum: usize },
    /// Too little of the session remains to hold a new pair.
    TooCloseToClose {
        minutes_remaining: i64,
        minimum: i64,
    },
    /// The session's close time is unknown, so the hold window cannot be checked.
    SessionEndUnknown,
    /// Opening this pair would push gross exposure past the cap.
    GrossExposure { projected: Decimal, cap: Decimal },
}

impl RiskBlock {
    /// A stable short name for the event payload and the logs.
    pub fn as_str(&self) -> &'static str {
        match self {
            RiskBlock::Drawdown { .. } => "drawdown",
            RiskBlock::PairCapacity { .. } => "pair_capacity",
            RiskBlock::TooCloseToClose { .. } => "too_close_to_close",
            RiskBlock::SessionEndUnknown => "session_end_unknown",
            RiskBlock::GrossExposure { .. } => "gross_exposure",
        }
    }
}

impl std::fmt::Display for RiskBlock {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RiskBlock::Drawdown {
                observed,
                threshold,
            } => write!(
                formatter,
                "drawdown of {observed:.4} exceeds the threshold of {threshold:.4}"
            ),
            RiskBlock::PairCapacity { open, maximum } => {
                write!(formatter, "the book holds {open} of {maximum} pairs")
            }
            RiskBlock::TooCloseToClose {
                minutes_remaining,
                minimum,
            } => write!(
                formatter,
                "{minutes_remaining} minutes remain, below the {minimum} minute hold"
            ),
            RiskBlock::SessionEndUnknown => {
                write!(formatter, "the session close time is unknown")
            }
            RiskBlock::GrossExposure { projected, cap } => write!(
                formatter,
                "projected gross exposure {projected} exceeds the cap of {cap}"
            ),
        }
    }
}

/// The entry gate for one evaluation pass.
///
/// Constructed once per pass from the account as Alpaca reports it, then consulted twice: once for
/// the session-wide conditions, and once per pair as exposure accumulates.
#[derive(Debug, Clone)]
pub struct RiskGate {
    equity: Decimal,
    previous_equity: Option<Decimal>,
    gross_exposure_used: Decimal,
    open_pair_count: usize,
    minutes_until_close: Option<i64>,
    parameters: SizingParameters,
}

impl RiskGate {
    /// Builds the gate from the account and the state of the book.
    ///
    /// `previous_equity` is the previous session's `account_snapshots.equity`. `None` means no
    /// prior session was recorded — a first run, or a post-close sync that did not complete — and
    /// the drawdown check is skipped rather than assumed breached. There is no reference to measure
    /// against, and refusing to trade because yesterday was never written down would make a missed
    /// post-close job silently halt the strategy.
    pub fn new(
        account: &AccountSnapshot,
        previous_equity: Option<Decimal>,
        open_pair_count: usize,
        minutes_until_close: Option<i64>,
        parameters: SizingParameters,
    ) -> Self {
        Self {
            equity: account.equity(),
            previous_equity,
            gross_exposure_used: account.gross_exposure(),
            open_pair_count,
            minutes_until_close,
            parameters,
        }
    }

    /// Slots the book has room for, before any other condition is considered.
    pub fn vacant_slots(&self) -> usize {
        self.parameters
            .maximum_concurrent_pairs()
            .saturating_sub(self.open_pair_count)
    }

    /// The gross exposure ceiling: equity times the configured multiple.
    pub fn gross_exposure_cap(&self) -> Decimal {
        Decimal::from_f64_retain(self.parameters.gross_exposure_multiple())
            .map(|multiple| self.equity * multiple)
            .unwrap_or(Decimal::ZERO)
    }

    /// Realized drawdown against the previous session, as a fraction in `[0, 1]`.
    ///
    /// `None` when there is no reference, or when the reference is non-positive and a ratio against
    /// it would be meaningless rather than merely large.
    pub fn drawdown(&self) -> Option<f64> {
        let previous = self.previous_equity?;
        if previous <= Decimal::ZERO {
            return None;
        }
        let fall = (previous - self.equity) / previous;
        fall.to_f64().map(|value| value.max(0.0))
    }

    /// Conditions that stop the pass opening anything at all.
    ///
    /// Checked once, before any screening, because each of them makes the whole entry half of the
    /// pass pointless — screening seven thousand symbols to then refuse every result is the
    /// expensive way to do nothing.
    pub fn session_block(&self) -> Option<RiskBlock> {
        if let Some(drawdown) = self.drawdown() {
            if drawdown >= DRAWDOWN_THRESHOLD {
                return Some(RiskBlock::Drawdown {
                    observed: drawdown,
                    threshold: DRAWDOWN_THRESHOLD,
                });
            }
        }

        if self.vacant_slots() == 0 {
            return Some(RiskBlock::PairCapacity {
                open: self.open_pair_count,
                maximum: self.parameters.maximum_concurrent_pairs(),
            });
        }

        // An unknown close time is a block, not a pass. The calendar answers `None` for a date
        // outside its published horizon, and opening a position when the system cannot say when the
        // market shuts is how a pair gets held overnight.
        let Some(minutes_remaining) = self.minutes_until_close else {
            return Some(RiskBlock::SessionEndUnknown);
        };
        if minutes_remaining < MINIMUM_HOLD_MINUTES {
            return Some(RiskBlock::TooCloseToClose {
                minutes_remaining,
                minimum: MINIMUM_HOLD_MINUTES,
            });
        }

        None
    }

    /// Admits one sized pair, charging its exposure against the cap.
    ///
    /// Returns the block on refusal and leaves the accumulated exposure untouched, so a pair too
    /// large to fit does not consume headroom a smaller one behind it could have used.
    pub fn admit(&mut self, pair: &SizedPair) -> Option<RiskBlock> {
        let projected = self.gross_exposure_used + pair.gross_exposure();
        let cap = self.gross_exposure_cap();
        if projected > cap {
            return Some(RiskBlock::GrossExposure { projected, cap });
        }
        self.gross_exposure_used = projected;
        self.open_pair_count += 1;
        None
    }

    /// Runs `admit` across a selection, returning what passed and what each refusal was.
    pub fn admit_all(&mut self, sized: &[SizedPair]) -> (Vec<SizedPair>, Vec<RiskBlock>) {
        let mut approved = Vec::with_capacity(sized.len());
        let mut blocks = Vec::new();

        for pair in sized {
            if self.vacant_slots() == 0 {
                blocks.push(RiskBlock::PairCapacity {
                    open: self.open_pair_count,
                    maximum: self.parameters.maximum_concurrent_pairs(),
                });
                break;
            }
            match self.admit(pair) {
                Some(block) => {
                    warn!(
                        pair_id = %pair.candidate().pair_id(),
                        block = block.as_str(),
                        reason = %block,
                        "Pair refused by the risk gate"
                    );
                    blocks.push(block);
                }
                None => approved.push(pair.clone()),
            }
        }

        info!(
            approved = approved.len(),
            refused = blocks.len(),
            "Risk gate applied"
        );
        (approved, blocks)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::types::{Dollars, PairID, Ticker};
    use crate::portfolio::screen::PairCandidate;
    use crate::portfolio::size::size_pair;

    fn account(equity: i64, long: i64, short: i64) -> AccountSnapshot {
        AccountSnapshot::new(
            Decimal::from(equity),
            Decimal::from(equity),
            Decimal::from(equity * 2),
            Decimal::from(long),
            Decimal::from(short),
        )
    }

    fn sized(long: &str, short: &str, short_price: f64, budget: i64) -> SizedPair {
        let candidate = PairCandidate::new(
            PairID::new(Ticker::new(long).unwrap(), Ticker::new(short).unwrap()),
            1.0,
            2.5,
            0.02,
            100.0,
            short_price,
        )
        .unwrap();
        size_pair(&candidate, Dollars::new(Decimal::from(budget)).unwrap())
            .expect("the test pair must size")
    }

    fn gate(
        account: &AccountSnapshot,
        previous_equity: Option<i64>,
        open_pair_count: usize,
        minutes_until_close: Option<i64>,
    ) -> RiskGate {
        RiskGate::new(
            account,
            previous_equity.map(Decimal::from),
            open_pair_count,
            minutes_until_close,
            SizingParameters::default(),
        )
    }

    #[test]
    fn test_drawdown_measures_the_fall_from_the_previous_session() {
        let gate = gate(&account(90_000, 0, 0), Some(100_000), 0, Some(120));
        let drawdown = gate.drawdown().expect("a reference exists");
        assert!((drawdown - 0.10).abs() < 1e-9, "got {drawdown}");
    }

    /// A gain is not a negative drawdown. Clamping at zero keeps the comparison against the
    /// threshold reading as "how far down", which is the only direction the gate cares about.
    #[test]
    fn test_a_gain_is_not_a_drawdown() {
        let gate = gate(&account(110_000, 0, 0), Some(100_000), 0, Some(120));
        assert_eq!(gate.drawdown(), Some(0.0));
        assert_eq!(gate.session_block(), None);
    }

    #[test]
    fn test_a_breached_drawdown_stops_the_entry_half() {
        let gate = gate(&account(85_000, 0, 0), Some(100_000), 0, Some(120));
        assert!(matches!(
            gate.session_block(),
            Some(RiskBlock::Drawdown { .. })
        ));
    }

    /// A missed post-close sync leaves no reference. Treating that as a breach would let one failed
    /// overnight job halt the strategy indefinitely, and nothing would say why.
    #[test]
    fn test_a_missing_previous_session_skips_the_drawdown_check() {
        let gate = gate(&account(50_000, 0, 0), None, 0, Some(120));
        assert_eq!(gate.drawdown(), None);
        assert_eq!(gate.session_block(), None);
    }

    #[test]
    fn test_a_full_book_has_no_vacant_slots() {
        let gate = gate(&account(100_000, 0, 0), Some(100_000), 10, Some(120));
        assert_eq!(gate.vacant_slots(), 0);
        assert!(matches!(
            gate.session_block(),
            Some(RiskBlock::PairCapacity {
                open: 10,
                maximum: 10
            })
        ));
    }

    #[test]
    fn test_entries_stop_inside_the_minimum_hold_window() {
        let inside_window = gate(&account(100_000, 0, 0), Some(100_000), 0, Some(20));
        assert!(matches!(
            inside_window.session_block(),
            Some(RiskBlock::TooCloseToClose {
                minutes_remaining: 20,
                minimum: MINIMUM_HOLD_MINUTES,
            })
        ));

        // Exactly at the minimum is enough; the check is "below", not "at or below".
        let ample = gate(
            &account(100_000, 0, 0),
            Some(100_000),
            0,
            Some(MINIMUM_HOLD_MINUTES),
        );
        assert_eq!(ample.session_block(), None);
    }

    /// The calendar answers `None` outside its published horizon. Opening a position when the
    /// system cannot say when the market shuts is how a pair ends up held overnight, which is the
    /// one outcome the whole design forbids.
    #[test]
    fn test_an_unknown_session_end_blocks_entries() {
        let gate = gate(&account(100_000, 0, 0), Some(100_000), 0, None);
        assert_eq!(gate.session_block(), Some(RiskBlock::SessionEndUnknown));
    }

    #[test]
    fn test_admit_charges_exposure_and_refuses_past_the_cap() {
        // Equity 20,000 at a 1.0 multiple is a 20,000 cap; the account already holds 12,000 gross.
        let mut gate = gate(&account(20_000, 8_000, -4_000), Some(20_000), 1, Some(120));
        assert_eq!(gate.gross_exposure_cap(), Decimal::from(20_000));

        // 5,000 a leg with a 100-dollar short is 5,000 + 5,000 = 10,000 gross, over the headroom.
        let large = sized("AAAA", "BBBB", 100.0, 5_000);
        assert!(matches!(
            gate.admit(&large),
            Some(RiskBlock::GrossExposure { .. })
        ));

        // A refusal must not consume headroom a smaller pair behind it could have used.
        let small = sized("CCCC", "DDDD", 100.0, 4_000);
        assert_eq!(gate.admit(&small), None);
    }

    #[test]
    fn test_admit_all_stops_once_the_book_is_full() {
        let mut gate = gate(&account(1_000_000, 0, 0), Some(1_000_000), 9, Some(120));
        let pairs = vec![
            sized("AAAA", "BBBB", 100.0, 1_000),
            sized("CCCC", "DDDD", 100.0, 1_000),
            sized("EEEE", "FFFF", 100.0, 1_000),
        ];
        let (approved, blocks) = gate.admit_all(&pairs);

        assert_eq!(approved.len(), 1);
        assert!(matches!(
            blocks.as_slice(),
            [RiskBlock::PairCapacity { .. }]
        ));
    }

    #[test]
    fn test_every_block_has_a_stable_name() {
        let blocks = [
            RiskBlock::Drawdown {
                observed: 0.2,
                threshold: 0.1,
            },
            RiskBlock::PairCapacity {
                open: 10,
                maximum: 10,
            },
            RiskBlock::TooCloseToClose {
                minutes_remaining: 5,
                minimum: 30,
            },
            RiskBlock::SessionEndUnknown,
            RiskBlock::GrossExposure {
                projected: Decimal::ONE,
                cap: Decimal::ZERO,
            },
        ];
        let names: Vec<&str> = blocks.iter().map(RiskBlock::as_str).collect();
        assert_eq!(
            names,
            vec![
                "drawdown",
                "pair_capacity",
                "too_close_to_close",
                "session_end_unknown",
                "gross_exposure"
            ]
        );
        for block in blocks {
            assert!(!block.to_string().is_empty());
        }
    }
}
