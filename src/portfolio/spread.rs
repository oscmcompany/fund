//! Pair spread construction shared by the live trigger and the authoritative pass.
//!
//! Two questions decide whether a pair can be priced right now, and they are not
//! the same question:
//!
//! - **Absolute age** — is this price still true? Enforced upstream, by the one
//!   [`StalenessWindow`](crate::domain::freshness::StalenessWindow) applied to
//!   streamed and snapshot quotes alike, so a leg that is too old never reaches
//!   here.
//! - **Leg skew** — is this *spread* real? Enforced here.
//!
//! Filtering each leg independently on age does not answer the second. Two legs
//! that both pass a five-minute window can still be four minutes apart from each
//! other, and the difference of two prices observed four minutes apart is not a
//! spread — it is a spread plus four minutes of drift in whichever leg moved.
//! That number is what a 4.0 z-score stop-loss fires on.
//!
//! The interaction between the two bounds is the point:
//!
//! - Two legs equally four minutes old give a *coherent* spread, measured four
//!   minutes ago. For a mean-reverting spread that is usable information.
//! - Two legs sixty seconds apart give an *incoherent* spread however fresh
//!   either one is. That is not usable at any age.
//!
//! Both paths call [`current_spread`] rather than each computing
//! `long - hedge_ratio * short` themselves, so the guard cannot be applied to one
//! and not the other. That failure has already happened once between these two
//! call sites — see the module comment on
//! [`live_evaluator`](crate::portfolio::live_evaluator) for the livelock it
//! produced — and it is the reason this lives in one place.

use chrono::{DateTime, Duration, Utc};
use tracing::debug;

use crate::domain::market::{Ticker, UsableQuote};

/// Maximum gap allowed between the observation times of a pair's two legs.
///
/// Thirty seconds is short relative to the five-minute absolute window, which is
/// deliberate: the absolute window decides whether a price is still true, and
/// this decides whether two prices describe the same moment. A pair whose legs
/// quote steadily is well inside it; one whose legs quote minutes apart is
/// exactly the case where the difference of the two carries more drift than
/// signal.
///
/// Set from reasoning rather than from this system's own data — there is no
/// production sample of realised leg skew yet. [`current_spread`] logs every
/// pair's skew at `debug!` so this becomes a percentile once production runs.
pub const MAXIMUM_LEG_SKEW_SECONDS: i64 = 30;

/// Where a leg's price came from.
///
/// Recorded so the skew distribution can be read per source once production
/// runs: streamed and snapshot legs have different arrival characteristics, and
/// a mixed pair is the case most likely to skew.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QuoteSource {
    /// Delivered over the quote-stream WebSocket and held in the live cache.
    Streamed,
    /// Pulled from the REST snapshot endpoint for a leg the stream did not cover.
    Snapshot,
}

impl QuoteSource {
    /// Returns the source name for structured logging.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Streamed => "streamed",
            Self::Snapshot => "snapshot",
        }
    }
}

/// One leg of a pair, priced and stamped with when the quote was observed.
///
/// Carrying `observed_at` alongside the mid price is what makes the skew check
/// possible at all. The previous representation was a bare `f64`, which is why
/// no skew check existed: by the time a spread was computed, the information
/// needed to reject it had already been discarded.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PricedLeg {
    mid_price: f64,
    observed_at: DateTime<Utc>,
    source: QuoteSource,
}

impl PricedLeg {
    /// Builds a priced leg from a validated quote.
    pub fn from_quote(quote: &UsableQuote, source: QuoteSource) -> Self {
        Self {
            mid_price: quote.mid_price(),
            observed_at: quote.observed_at(),
            source,
        }
    }

    /// Returns the mid price.
    pub fn mid_price(&self) -> f64 {
        self.mid_price
    }

    /// Returns when the underlying quote was observed.
    pub fn observed_at(&self) -> DateTime<Utc> {
        self.observed_at
    }

    /// Returns where the price came from.
    pub fn source(&self) -> QuoteSource {
        self.source
    }

    /// Builds a priced leg directly, for tests.
    ///
    /// Production code constructs legs from a validated quote, which is what
    /// makes a `PricedLeg` proof of a usable book. Spread and z-score fixtures
    /// need mid prices that no real book would carry — zero, negative, an
    /// arbitrary series value — so they bypass that rather than contorting a
    /// quote to produce them.
    #[cfg(test)]
    pub fn for_tests(mid_price: f64, observed_at: DateTime<Utc>, source: QuoteSource) -> Self {
        Self {
            mid_price,
            observed_at,
            source,
        }
    }
}

/// Why a pair could not be priced this pass.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpreadRejection {
    /// At least one leg has no usable current price.
    ///
    /// Both legs are required or neither is used: pricing one leg current
    /// against the other's prior close moves the spread by a day of drift in
    /// that leg alone and reads as a signal.
    LegUnpriced {
        long_priced: bool,
        short_priced: bool,
    },
    /// Both legs are priced, but too far apart in time to describe one spread.
    LegSkew { skew_seconds: i64 },
}

/// Returns the current spread for a pair, or why it cannot be measured.
///
/// The spread is `long - hedge_ratio * short`, computed only when both legs are
/// present and observed within [`MAXIMUM_LEG_SKEW_SECONDS`] of each other.
///
/// Logs every decision at `debug!` — each leg's age, the skew, each source, and
/// the verdict. That is the measurement the skew bound was set without; once
/// production runs it turns both this bound and the absolute staleness window
/// into percentiles rather than reasoned guesses.
pub fn current_spread(
    pair_id: &str,
    now: DateTime<Utc>,
    long_ticker: &Ticker,
    short_ticker: &Ticker,
    long_leg: Option<&PricedLeg>,
    short_leg: Option<&PricedLeg>,
    hedge_ratio: f64,
) -> Result<f64, SpreadRejection> {
    let (Some(long_leg), Some(short_leg)) = (long_leg, short_leg) else {
        let rejection = SpreadRejection::LegUnpriced {
            long_priced: long_leg.is_some(),
            short_priced: short_leg.is_some(),
        };
        debug!(
            pair_id,
            long_ticker = long_ticker.as_str(),
            short_ticker = short_ticker.as_str(),
            long_priced = long_leg.is_some(),
            short_priced = short_leg.is_some(),
            verdict = "rejected",
            cause = "leg_unpriced",
            "Pair spread not measurable"
        );
        return Err(rejection);
    };

    let skew = (long_leg.observed_at() - short_leg.observed_at()).abs();
    let long_age_seconds = (now - long_leg.observed_at()).num_seconds();
    let short_age_seconds = (now - short_leg.observed_at()).num_seconds();
    let skew_seconds = skew.num_seconds();

    if skew > Duration::seconds(MAXIMUM_LEG_SKEW_SECONDS) {
        debug!(
            pair_id,
            long_ticker = long_ticker.as_str(),
            short_ticker = short_ticker.as_str(),
            long_age_seconds,
            short_age_seconds,
            skew_seconds,
            long_source = long_leg.source().as_str(),
            short_source = short_leg.source().as_str(),
            verdict = "rejected",
            cause = "leg_skew",
            "Pair spread not measurable"
        );
        return Err(SpreadRejection::LegSkew { skew_seconds });
    }

    let spread = long_leg.mid_price() - hedge_ratio * short_leg.mid_price();

    debug!(
        pair_id,
        long_ticker = long_ticker.as_str(),
        short_ticker = short_ticker.as_str(),
        long_age_seconds,
        short_age_seconds,
        skew_seconds,
        long_source = long_leg.source().as_str(),
        short_source = short_leg.source().as_str(),
        verdict = "accepted",
        "Pair spread measured"
    );

    Ok(spread)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::market::{BookQualityLimits, EquityQuote};

    fn ticker(symbol: &str) -> Ticker {
        Ticker::new(symbol).unwrap()
    }

    /// Builds a priced leg directly, bypassing quote construction.
    fn leg(mid_price: f64, observed_at: DateTime<Utc>, source: QuoteSource) -> PricedLeg {
        PricedLeg {
            mid_price,
            observed_at,
            source,
        }
    }

    fn measure(
        long_leg: Option<&PricedLeg>,
        short_leg: Option<&PricedLeg>,
        hedge_ratio: f64,
    ) -> Result<f64, SpreadRejection> {
        current_spread(
            "pair-1",
            Utc::now(),
            &ticker("AAA"),
            &ticker("BBB"),
            long_leg,
            short_leg,
            hedge_ratio,
        )
    }

    #[test]
    fn test_simultaneous_legs_produce_the_expected_spread() {
        let now = Utc::now();
        let long_leg = leg(100.0, now, QuoteSource::Streamed);
        let short_leg = leg(40.0, now, QuoteSource::Streamed);

        let spread = measure(Some(&long_leg), Some(&short_leg), 2.0).unwrap();
        assert!((spread - 20.0).abs() < f64::EPSILON);
    }

    /// The behaviour this module exists for: two fresh legs, far apart in time.
    #[test]
    fn test_skewed_legs_are_rejected_however_fresh() {
        let now = Utc::now();
        let long_leg = leg(100.0, now, QuoteSource::Streamed);
        let short_leg = leg(40.0, now - Duration::seconds(31), QuoteSource::Streamed);

        assert_eq!(
            measure(Some(&long_leg), Some(&short_leg), 2.0),
            Err(SpreadRejection::LegSkew { skew_seconds: 31 })
        );
    }

    /// Skew is symmetric: which leg is older must not change the verdict.
    #[test]
    fn test_skew_is_symmetric() {
        let now = Utc::now();
        let older_long = leg(100.0, now - Duration::seconds(45), QuoteSource::Streamed);
        let fresh_short = leg(40.0, now, QuoteSource::Streamed);

        assert_eq!(
            measure(Some(&older_long), Some(&fresh_short), 2.0),
            Err(SpreadRejection::LegSkew { skew_seconds: 45 })
        );
        assert_eq!(
            measure(Some(&fresh_short), Some(&older_long), 2.0),
            Err(SpreadRejection::LegSkew { skew_seconds: 45 })
        );
    }

    #[test]
    fn test_skew_exactly_at_the_bound_is_accepted() {
        let now = Utc::now();
        let long_leg = leg(100.0, now, QuoteSource::Streamed);
        let short_leg = leg(
            40.0,
            now - Duration::seconds(MAXIMUM_LEG_SKEW_SECONDS),
            QuoteSource::Streamed,
        );

        assert!(measure(Some(&long_leg), Some(&short_leg), 2.0).is_ok());
    }

    /// Equally-aged legs are coherent at any age inside the absolute window.
    /// This is the case the old sixty-second streamed window rejected.
    #[test]
    fn test_equally_aged_legs_are_accepted_at_several_ages() {
        let now = Utc::now();
        for age_seconds in [0, 60, 120, 299] {
            let observed_at = now - Duration::seconds(age_seconds);
            let long_leg = leg(100.0, observed_at, QuoteSource::Streamed);
            let short_leg = leg(40.0, observed_at, QuoteSource::Snapshot);
            assert!(
                measure(Some(&long_leg), Some(&short_leg), 2.0).is_ok(),
                "legs aged {age_seconds}s together must be measurable"
            );
        }
    }

    /// Mixed sourcing is not itself a rejection cause — skew is.
    #[test]
    fn test_mixed_sources_within_the_bound_are_accepted() {
        let now = Utc::now();
        let streamed = leg(100.0, now - Duration::seconds(5), QuoteSource::Streamed);
        let snapshot = leg(40.0, now - Duration::seconds(20), QuoteSource::Snapshot);

        assert!(measure(Some(&streamed), Some(&snapshot), 2.0).is_ok());
    }

    #[test]
    fn test_an_unpriced_leg_is_rejected_before_skew_is_considered() {
        let now = Utc::now();
        let long_leg = leg(100.0, now, QuoteSource::Streamed);

        assert_eq!(
            measure(Some(&long_leg), None, 2.0),
            Err(SpreadRejection::LegUnpriced {
                long_priced: true,
                short_priced: false,
            })
        );
        assert_eq!(
            measure(None, Some(&long_leg), 2.0),
            Err(SpreadRejection::LegUnpriced {
                long_priced: false,
                short_priced: true,
            })
        );
        assert_eq!(
            measure(None, None, 2.0),
            Err(SpreadRejection::LegUnpriced {
                long_priced: false,
                short_priced: false,
            })
        );
    }

    #[test]
    fn test_from_quote_carries_the_observation_time() {
        let observed_at = Utc::now() - Duration::seconds(12);
        let quote = EquityQuote::new(
            observed_at,
            ticker("AAA"),
            99.99,
            100.01,
            crate::domain::market::MINIMUM_QUOTE_SIZE,
            crate::domain::market::MINIMUM_QUOTE_SIZE,
        );
        let usable =
            UsableQuote::new(&quote, BookQualityLimits::default()).expect("book is usable");

        let priced = PricedLeg::from_quote(&usable, QuoteSource::Snapshot);
        assert_eq!(priced.observed_at(), observed_at);
        assert_eq!(priced.source(), QuoteSource::Snapshot);
        assert!((priced.mid_price() - 100.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_quote_source_names() {
        assert_eq!(QuoteSource::Streamed.as_str(), "streamed");
        assert_eq!(QuoteSource::Snapshot.as_str(), "snapshot");
    }
}
