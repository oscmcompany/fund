//! Position sizing: fixed-fraction, equal-weight, both legs the same dollar amount.
//!
//! Each pair is allocated the same slice of equity whether the book holds one pair or ten, so a
//! pair's size does not change when an unrelated pair closes. The consequence, and it is
//! deliberate: a book holding three of ten slots runs at roughly three tenths of its exposure
//! target rather than concentrating the full target into the pairs that happen to be open.
//!
//! **Both legs get equal notional, not hedge-ratio-weighted notional.** The hedge ratio decides
//! where the spread's mean is, not how much to buy. Sizing the short leg at `hedge_ratio x` the
//! long would make the book hedge-ratio-neutral instead of dollar-neutral, which is a different
//! and larger claim about what the two legs have in common — and the machinery that justified it
//! is what `risk_management_reintroduction.md` records as removed. Dollar neutrality is the
//! assumption this version can actually support.

use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use std::num::NonZeroU32;
use tracing::{debug, warn};

use crate::common::types::Dollars;
use crate::portfolio::screen::PairCandidate;

/// Pairs the book will hold at once when full.
pub const MAXIMUM_CONCURRENT_PAIRS: usize = 10;

/// Gross exposure the book targets, as a multiple of account equity.
///
/// One, so a full book is roughly fully invested with no deliberate leverage. Reg T allows more;
/// the strategy does not ask for it.
pub const GROSS_EXPOSURE_MULTIPLE: f64 = 1.0;

/// Legs per pair. Named because it is what turns a per-pair budget into a per-leg one, and a stray
/// factor of two in a sizing calculation is not visible in the result.
const LEGS_PER_PAIR: u32 = 2;

/// Sizing configuration.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SizingParameters {
    maximum_concurrent_pairs: usize,
    gross_exposure_multiple: f64,
}

impl SizingParameters {
    /// Constructs parameters, rejecting values that cannot describe a book.
    pub fn new(maximum_concurrent_pairs: usize, gross_exposure_multiple: f64) -> Option<Self> {
        if maximum_concurrent_pairs == 0 {
            return None;
        }
        if !gross_exposure_multiple.is_finite() || gross_exposure_multiple <= 0.0 {
            return None;
        }
        Some(Self {
            maximum_concurrent_pairs,
            gross_exposure_multiple,
        })
    }

    /// Reads `MAXIMUM_CONCURRENT_PAIRS` and `GROSS_EXPOSURE_MULTIPLE`, falling back to the defaults.
    ///
    /// An unparsable or nonsensical value falls back with a warning rather than failing startup.
    /// The defaults are a working configuration; refusing to start over a malformed override trades
    /// a mis-sized book for no book at all.
    pub fn from_env() -> Self {
        let maximum_concurrent_pairs =
            read_env("MAXIMUM_CONCURRENT_PAIRS").unwrap_or(MAXIMUM_CONCURRENT_PAIRS);
        let gross_exposure_multiple =
            read_env("GROSS_EXPOSURE_MULTIPLE").unwrap_or(GROSS_EXPOSURE_MULTIPLE);

        Self::new(maximum_concurrent_pairs, gross_exposure_multiple).unwrap_or_else(|| {
            warn!(
                maximum_concurrent_pairs,
                gross_exposure_multiple, "Sizing overrides are unusable; falling back to defaults"
            );
            Self::default()
        })
    }

    pub fn maximum_concurrent_pairs(&self) -> usize {
        self.maximum_concurrent_pairs
    }

    pub fn gross_exposure_multiple(&self) -> f64 {
        self.gross_exposure_multiple
    }

    /// The dollar notional allocated to one leg of one pair.
    ///
    /// `equity x multiple / (pairs x 2)`. Returns `None` for non-positive equity, which is an
    /// account that cannot open anything.
    pub fn notional_per_leg(&self, equity: Decimal) -> Option<Dollars> {
        if equity <= Decimal::ZERO {
            return None;
        }
        let multiple = Decimal::from_f64_retain(self.gross_exposure_multiple)?;
        let slots = Decimal::from(self.maximum_concurrent_pairs as u64 * LEGS_PER_PAIR as u64);
        Dollars::new((equity * multiple / slots).round_dp(2)).ok()
    }
}

impl Default for SizingParameters {
    /// The defaults, through the validated constructor rather than a struct literal.
    ///
    /// `notional_per_leg` divides by `maximum_concurrent_pairs * LEGS_PER_PAIR`, and `Decimal`
    /// division by zero panics. The constants are sound today; routing through `new` means a future
    /// edit that makes one of them zero fails here, at construction, rather than inside sizing.
    fn default() -> Self {
        Self::new(MAXIMUM_CONCURRENT_PAIRS, GROSS_EXPOSURE_MULTIPLE)
            .expect("the sizing defaults must satisfy their own validation")
    }
}

fn read_env<T: std::str::FromStr>(variable: &str) -> Option<T> {
    std::env::var(variable).ok()?.trim().parse().ok()
}

/// A candidate with both legs sized.
///
/// The two legs are sized in different units because Alpaca accepts different units: the long is a
/// dollar notional filled fractionally, the short a whole share count. `short_notional` is
/// therefore the *realized* short exposure — shares times price — and is smaller than
/// `long_notional` by up to one share's worth of rounding.
#[derive(Debug, Clone, PartialEq)]
pub struct SizedPair {
    candidate: PairCandidate,
    long_notional: Dollars,
    short_shares: NonZeroU32,
    short_notional: Dollars,
}

impl SizedPair {
    pub fn candidate(&self) -> &PairCandidate {
        &self.candidate
    }

    pub fn long_notional(&self) -> Dollars {
        self.long_notional
    }

    pub fn short_shares(&self) -> NonZeroU32 {
        self.short_shares
    }

    /// The short leg's realized notional: whole shares at the current price.
    pub fn short_notional(&self) -> Dollars {
        self.short_notional
    }

    /// Gross exposure this pair adds: both legs, both magnitudes.
    pub fn gross_exposure(&self) -> Decimal {
        self.long_notional.value() + self.short_notional.value()
    }
}

/// Sizes one candidate against a per-leg budget.
///
/// Returns `None` when the short leg would round to zero shares — a symbol priced above the per-leg
/// budget. That pair cannot be opened dollar-neutral at this account size, and opening the long leg
/// alone would be a naked directional position rather than a spread.
pub fn size_pair(candidate: &PairCandidate, notional_per_leg: Dollars) -> Option<SizedPair> {
    let budget = notional_per_leg.value().to_f64()?;
    let short_price = candidate.short_price();
    if !short_price.is_finite() || short_price <= 0.0 {
        return None;
    }

    let whole_shares = (budget / short_price).floor();
    if !whole_shares.is_finite() || whole_shares < 1.0 || whole_shares > u32::MAX as f64 {
        debug!(
            pair_id = %candidate.pair_id(),
            short_price,
            budget,
            "Short leg does not round to a usable whole-share quantity"
        );
        return None;
    }
    let short_shares = NonZeroU32::new(whole_shares as u32)?;

    let short_notional = Dollars::new(
        (Decimal::from(short_shares.get()) * Decimal::from_f64_retain(short_price)?).round_dp(2),
    )
    .ok()?;

    Some(SizedPair {
        candidate: candidate.clone(),
        long_notional: notional_per_leg,
        short_shares,
        short_notional,
    })
}

/// Sizes a selection of candidates, dropping those that cannot be sized.
pub fn size_pairs(
    candidates: &[PairCandidate],
    equity: Decimal,
    parameters: &SizingParameters,
) -> Vec<SizedPair> {
    let Some(notional_per_leg) = parameters.notional_per_leg(equity) else {
        warn!(%equity, "Account equity does not support a position; nothing sized");
        return Vec::new();
    };

    let sized: Vec<SizedPair> = candidates
        .iter()
        .filter_map(|candidate| size_pair(candidate, notional_per_leg))
        .collect();

    debug!(
        supplied = candidates.len(),
        sized = sized.len(),
        notional_per_leg = %notional_per_leg.value(),
        "Candidates sized"
    );
    sized
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::types::{PairID, Ticker};

    fn candidate(long_price: f64, short_price: f64) -> PairCandidate {
        PairCandidate::new(
            PairID::new(Ticker::new("AAAA").unwrap(), Ticker::new("BBBB").unwrap()),
            1.0,
            2.5,
            0.02,
            long_price,
            short_price,
        )
        .expect("the test candidate must be constructible")
    }

    #[test]
    fn test_notional_per_leg_divides_equity_across_every_slot() {
        let parameters = SizingParameters::new(10, 1.0).unwrap();
        // 100,000 across ten pairs of two legs is 5,000 a leg.
        assert_eq!(
            parameters
                .notional_per_leg(Decimal::new(100_000, 0))
                .unwrap()
                .value(),
            Decimal::new(5_000, 0)
        );
    }

    /// A pair's size must not depend on how many other pairs are open, or every close would resize
    /// the rest of the book and the exposure target would be chased rather than held.
    #[test]
    fn test_notional_per_leg_scales_with_the_exposure_multiple_only() {
        let single = SizingParameters::new(10, 1.0).unwrap();
        let doubled = SizingParameters::new(10, 2.0).unwrap();
        let equity = Decimal::new(100_000, 0);
        assert_eq!(
            doubled.notional_per_leg(equity).unwrap().value(),
            single.notional_per_leg(equity).unwrap().value() * Decimal::TWO
        );
    }

    #[test]
    fn test_notional_per_leg_refuses_a_non_positive_account() {
        let parameters = SizingParameters::default();
        assert_eq!(parameters.notional_per_leg(Decimal::ZERO), None);
        assert_eq!(parameters.notional_per_leg(Decimal::new(-1, 0)), None);
    }

    #[test]
    fn test_parameters_reject_a_book_with_no_slots_or_no_exposure() {
        assert_eq!(SizingParameters::new(0, 1.0), None);
        assert_eq!(SizingParameters::new(10, 0.0), None);
        assert_eq!(SizingParameters::new(10, f64::NAN), None);
    }

    #[test]
    fn test_short_leg_rounds_down_to_whole_shares() {
        let sized = size_pair(
            &candidate(50.0, 300.0),
            Dollars::new(Decimal::new(5_000, 0)).unwrap(),
        )
        .expect("the pair must size");

        // 5,000 / 300 is 16.67, so sixteen shares at 300 is 4,800 of realized short exposure.
        assert_eq!(sized.short_shares().get(), 16);
        assert_eq!(sized.short_notional().value(), Decimal::new(4_800, 0));
        assert_eq!(sized.long_notional().value(), Decimal::new(5_000, 0));
    }

    /// Alpaca will not take a fractional short, so a symbol priced above the per-leg budget cannot
    /// be one. Opening the long leg alone would leave a naked directional position wearing the name
    /// of a market-neutral pair.
    #[test]
    fn test_a_short_leg_priced_above_the_budget_is_not_sized() {
        assert_eq!(
            size_pair(
                &candidate(50.0, 6_000.0),
                Dollars::new(Decimal::new(5_000, 0)).unwrap()
            ),
            None
        );
    }

    #[test]
    fn test_gross_exposure_sums_both_legs() {
        let sized = size_pair(
            &candidate(50.0, 250.0),
            Dollars::new(Decimal::new(5_000, 0)).unwrap(),
        )
        .unwrap();
        assert_eq!(sized.gross_exposure(), Decimal::new(10_000, 0));
    }

    #[test]
    fn test_size_pairs_drops_only_what_cannot_be_sized() {
        let candidates = vec![candidate(50.0, 250.0), candidate(50.0, 99_999.0)];
        let sized = size_pairs(
            &candidates,
            Decimal::new(100_000, 0),
            &SizingParameters::default(),
        );
        assert_eq!(sized.len(), 1);
    }

    #[test]
    fn test_size_pairs_returns_nothing_for_an_empty_account() {
        assert!(size_pairs(
            &[candidate(50.0, 250.0)],
            Decimal::ZERO,
            &SizingParameters::default()
        )
        .is_empty());
    }
}
