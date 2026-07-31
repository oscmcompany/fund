//! Live-quote exit trigger.
//!
//! Watches streamed quotes for open pairs and emits
//! `portfolio_evaluation_requested` when a spread crosses a close threshold, so
//! an exit is acted on within seconds rather than waiting for the five-minute
//! heartbeat.
//!
//! The hot path is pure arithmetic against cached baselines: no Alpaca calls, no
//! database reads, no writes. Only a crossing pays for a durable event — raw
//! quotes stay ephemeral and only a decision crosses into PostgreSQL.
//!
//! This is a trigger, not a decision. It answers "something may have crossed, go
//! look"; the authoritative close is made by `evaluate_open_pairs` inside the
//! rebalance pass.
//!
//! The two must agree on both the rule and the measurement. They share
//! [`close_reason_for`] for the rule and [`z_score_against`] for the
//! measurement, each standardizing the live spread against the historical daily
//! distribution. An earlier version let them diverge — the pass appended the
//! live point to the series it was scored against while the trigger did not —
//! which made the trigger's magnitude systematically larger. That livelocks:
//! the trigger fires, the pass computes a smaller magnitude and keeps the pair
//! open, the debounce expires, and it fires again, forever. The only intended
//! difference is *when* each runs, never what either computes.

use std::collections::HashMap;
use std::time::Duration;

use chrono::{DateTime, Utc};
use sqlx::PgPool;
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::common::events::{emit_event, EventType};
use crate::common::market_hours::{
    duration_until_quote_stream_window, is_within_quote_stream_window,
};
use crate::domain::market::{PairID, Ticker};
use crate::portfolio::database::{fetch_historical_equity_prices_for, fetch_open_pairs, OpenPair};
use crate::portfolio::live_prices::LivePriceCache;
use crate::portfolio::math::{standard_deviation, z_score_against};
use crate::portfolio::rebalance::close_reason_for;
use crate::portfolio::spread::{current_spread, PricedLeg};

/// How often baselines are rebuilt from the database.
///
/// Baselines change only when the open-pair set changes or a new daily bar
/// lands, so a minute is frequent enough while keeping the query off the hot
/// path entirely.
const BASELINE_REFRESH_INTERVAL: Duration = Duration::from_secs(60);

/// How often the cached quotes are checked for a crossing.
///
/// Evaluating per received quote would recheck the same pair thousands of times
/// a second for no additional information, since the check reads the cache
/// rather than the individual quote.
const CROSSING_CHECK_INTERVAL: Duration = Duration::from_secs(1);

/// Minimum gap between emitted evaluation requests.
///
/// A crossing persists until the rebalance pass closes the position, so without
/// this the trigger would emit continuously for the whole duration of the pass
/// it already requested.
const EMISSION_DEBOUNCE: Duration = Duration::from_secs(30);

/// Sample size below which a spread series cannot support a z-score.
const MINIMUM_SPREAD_SAMPLES: usize = 2;

/// Precomputed spread statistics for one open pair.
///
/// Holds everything the hot path needs to turn two mid-prices into a z-score,
/// so a crossing check touches no I/O.
#[derive(Debug, Clone)]
struct PairBaseline {
    pair_id: PairID,
    long_ticker: Ticker,
    short_ticker: Ticker,
    hedge_ratio: f64,
    entry_z_score: f64,
    /// Historical daily spread, retained so the trigger standardizes through the
    /// same [`z_score_against`] the authoritative pass uses. Storing a
    /// precomputed mean and deviation instead would leave two places to keep the
    /// estimator in sync, which is how they diverged before.
    spread_history: Vec<f64>,
}

impl PairBaseline {
    /// Returns the z-score of the live spread against the daily baseline.
    ///
    /// `None` when the pair cannot be priced — either leg missing, or the two
    /// legs observed too far apart to describe one spread. Both conditions are
    /// decided by [`current_spread`], which the authoritative pass calls too, so
    /// the trigger cannot come to fire on a spread the pass would refuse to
    /// measure.
    fn live_z_score(
        &self,
        live_legs: &HashMap<Ticker, PricedLeg>,
        now: DateTime<Utc>,
    ) -> Option<f64> {
        let live_spread = current_spread(
            self.pair_id.as_str(),
            now,
            &self.long_ticker,
            &self.short_ticker,
            live_legs.get(&self.long_ticker),
            live_legs.get(&self.short_ticker),
            self.hedge_ratio,
        )
        .ok()?;
        Some(z_score_against(&self.spread_history, live_spread))
    }
}

/// Rebuilds baselines for every open pair from daily closes.
async fn load_baselines(pool: &PgPool) -> Result<Vec<PairBaseline>, sqlx::Error> {
    let open_pairs = fetch_open_pairs(pool).await?;
    if open_pairs.is_empty() {
        return Ok(Vec::new());
    }

    // Scoped to the open legs. This runs once a minute through the session, and
    // the unfiltered query materializes the full 90-day price table for every
    // ticker in the universe to use a few dozen of them.
    let legs: Vec<Ticker> = open_pairs
        .iter()
        .flat_map(|pair| [pair.long_ticker().clone(), pair.short_ticker().clone()])
        .collect();
    let historical_prices = fetch_historical_equity_prices_for(pool, &legs).await?;

    let baselines = open_pairs
        .iter()
        .filter_map(|pair| {
            baseline_for(
                pair,
                historical_prices.get(pair.long_ticker())?,
                historical_prices.get(pair.short_ticker())?,
            )
        })
        .collect();

    Ok(baselines)
}

/// Builds one pair's baseline, or `None` when its history cannot support a z-score.
///
/// Separated from the query so the discard rules are reachable without a pool.
fn baseline_for(
    pair: &OpenPair,
    long_prices: &[f64],
    short_prices: &[f64],
) -> Option<PairBaseline> {
    let common_length = long_prices.len().min(short_prices.len());
    if common_length < MINIMUM_SPREAD_SAMPLES {
        return None;
    }

    let spread_history: Vec<f64> = long_prices[long_prices.len() - common_length..]
        .iter()
        .zip(short_prices[short_prices.len() - common_length..].iter())
        .map(|(long, short)| long - pair.hedge_ratio() * short)
        .collect();

    // A flat spread cannot produce a meaningful z-score. z_score_against returns
    // zero for it, which close_reason_for reads as no signal, but discarding it
    // here keeps the trigger from re-deriving that on every one-second tick.
    let deviation = standard_deviation(&spread_history, 0);
    if deviation <= 0.0 || !deviation.is_finite() {
        return None;
    }

    Some(PairBaseline {
        pair_id: pair.pair_id().clone(),
        long_ticker: pair.long_ticker().clone(),
        short_ticker: pair.short_ticker().clone(),
        hedge_ratio: pair.hedge_ratio(),
        entry_z_score: pair.entry_z_score(),
        spread_history,
    })
}

/// Returns the pairs whose live spread has crossed a close threshold.
fn crossed_pairs(
    baselines: &[PairBaseline],
    live_legs: &HashMap<Ticker, PricedLeg>,
    now: DateTime<Utc>,
) -> Vec<(PairID, f64)> {
    baselines
        .iter()
        .filter_map(|baseline| {
            let z_score = baseline.live_z_score(live_legs, now)?;
            close_reason_for(baseline.entry_z_score, z_score)?;
            Some((baseline.pair_id.clone(), z_score))
        })
        .collect()
}

/// Spawns the live exit trigger.
pub fn spawn_live_evaluator(
    pool: PgPool,
    cache: LivePriceCache,
    shutdown_token: CancellationToken,
) -> JoinHandle<()> {
    tokio::spawn(run_live_evaluator(pool, cache, shutdown_token))
}

async fn run_live_evaluator(
    pool: PgPool,
    cache: LivePriceCache,
    shutdown_token: CancellationToken,
) {
    info!("Live exit trigger started");

    let mut baselines: Vec<PairBaseline> = Vec::new();
    let mut since_refresh = Duration::ZERO;
    let mut since_emission = EMISSION_DEBOUNCE;
    let mut refresh_due = true;

    loop {
        // Outside the quote-stream window the cache is guaranteed empty, because
        // the producer sleeps through it too. Refreshing baselines here would
        // issue queries a minute, all night and all weekend, for a check that
        // cannot fire.
        //
        // The fixed window is used rather than Alpaca's real session, unlike the
        // producer, which derives its own. That is safe because the fixed window
        // is a strict superset: every real session falls inside 09:25–16:05 on a
        // weekday, so this never sleeps through live quotes. It only costs
        // unnecessary polling on a holiday or after an early close, where the
        // producer has already stopped and the cache stays empty. Deriving the
        // session here would need the Alpaca client threaded into this task for
        // a saving that is entirely in idle queries.
        if !is_within_quote_stream_window() {
            let wait = duration_until_quote_stream_window(Utc::now());
            info!(
                wait_seconds = wait.as_secs(),
                "Outside quote stream window; live exit trigger sleeping"
            );
            tokio::select! {
                _ = sleep(wait) => {}
                _ = shutdown_token.cancelled() => break,
            }
            // The open-pair set will have moved on across a session boundary.
            refresh_due = true;
            continue;
        }

        if refresh_due {
            match load_baselines(&pool).await {
                Ok(loaded) => {
                    info!(pairs = loaded.len(), "Live exit baselines refreshed");
                    baselines = loaded;
                }
                Err(error) => {
                    // Keep the existing baselines: they describe positions that
                    // are still open, and discarding them would blind the
                    // trigger over a transient query failure.
                    warn!(error = %error, "Could not refresh live exit baselines");
                }
            }
            since_refresh = Duration::ZERO;
            refresh_due = false;
        }

        tokio::select! {
            _ = sleep(CROSSING_CHECK_INTERVAL) => {}
            _ = shutdown_token.cancelled() => break,
        }
        since_refresh += CROSSING_CHECK_INTERVAL;
        since_emission += CROSSING_CHECK_INTERVAL;
        if since_refresh >= BASELINE_REFRESH_INTERVAL {
            refresh_due = true;
        }

        if baselines.is_empty() || since_emission < EMISSION_DEBOUNCE {
            continue;
        }

        let now = Utc::now();
        let live_legs = cache.fresh_legs(now).await;
        let crossed = crossed_pairs(&baselines, &live_legs, now);
        if crossed.is_empty() {
            continue;
        }

        info!(
            pairs = crossed.len(),
            first_pair = crossed[0].0.as_str(),
            first_z_score = crossed[0].1,
            "Live spread crossed a close threshold; requesting evaluation"
        );

        // The debounce is reset on both outcomes. A crossing persists until a
        // pass closes the position, so leaving it unreset on failure would retry
        // every second — and with the reload below, that is two queries plus a
        // failing insert per second, precisely when the database is least able
        // to absorb them.
        since_emission = Duration::ZERO;
        match emit_event(
            &pool,
            EventType::PortfolioEvaluationRequested,
            // The flagged pair travels with the request so the authoritative
            // pass can say whether it agreed. Observability only: by the time
            // the pass reads this z-score, reconciliation, a positions fetch,
            // and a historical price load have all happened, so it reprices
            // rather than trusting the number.
            &serde_json::json!({
                "reason": "live_threshold_crossing",
                "pair_id": crossed[0].0.as_str(),
                "z_score": crossed[0].1,
            }),
        )
        .await
        {
            // Baselines are stale the moment a pass may have closed something.
            Ok(_) => refresh_due = true,
            Err(error) => {
                // No reload on failure: nothing acted on the trigger, so the
                // baselines still describe the open positions accurately.
                warn!(error = %error, "Failed to emit portfolio_evaluation_requested");
            }
        }
    }

    info!("Live exit trigger stopped");
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ticker(symbol: &str) -> Ticker {
        Ticker::new(symbol).expect("valid ticker")
    }

    /// Builds a baseline over `history`, which becomes the distribution live
    /// spreads are standardized against.
    fn baseline_with(entry_z_score: f64, history: Vec<f64>) -> PairBaseline {
        PairBaseline {
            pair_id: PairID::new(ticker("AAPL"), ticker("MSFT")),
            long_ticker: ticker("AAPL"),
            short_ticker: ticker("MSFT"),
            hedge_ratio: 1.0,
            entry_z_score,
            spread_history: history,
        }
    }

    /// History with mean 0 and population standard deviation 1, so a live spread
    /// value reads directly as its own z-score.
    fn unit_baseline(entry_z_score: f64) -> PairBaseline {
        baseline_with(entry_z_score, vec![-1.0, 1.0])
    }

    /// Both legs observed at the same instant, so skew is zero and these
    /// fixtures exercise the z-score rule rather than the skew guard.
    fn live(long: f64, short: f64) -> HashMap<Ticker, PricedLeg> {
        legs_observed_at(long, Utc::now(), short, Utc::now())
    }

    /// Legs with independent observation times, for the skew cases.
    fn legs_observed_at(
        long: f64,
        long_observed_at: DateTime<Utc>,
        short: f64,
        short_observed_at: DateTime<Utc>,
    ) -> HashMap<Ticker, PricedLeg> {
        let mut legs = HashMap::new();
        legs.insert(ticker("AAPL"), priced_leg(long, long_observed_at));
        legs.insert(ticker("MSFT"), priced_leg(short, short_observed_at));
        legs
    }

    /// Calls `crossed_pairs` at the current instant.
    fn crossed_pairs_now(
        baselines: &[PairBaseline],
        live_legs: &HashMap<Ticker, PricedLeg>,
    ) -> Vec<(PairID, f64)> {
        crossed_pairs(baselines, live_legs, Utc::now())
    }

    /// Builds a priced leg at `mid_price`, observed at `observed_at`.
    ///
    /// Fixtures here use spread values a real book would never quote — zero and
    /// negative mids among them — so they construct the leg directly rather than
    /// through a quote.
    fn priced_leg(mid_price: f64, observed_at: DateTime<Utc>) -> PricedLeg {
        use crate::portfolio::spread::QuoteSource;

        PricedLeg::for_tests(mid_price, observed_at, QuoteSource::Streamed)
    }

    #[test]
    fn test_live_z_score_uses_history_distribution() {
        // History mean 10, population standard deviation 2.
        let baseline = baseline_with(2.5, vec![8.0, 12.0]);

        // Spread is 20 - 4 = 16; (16 - 10) / 2 = 3.
        let z_score = baseline.live_z_score(&live(20.0, 4.0), Utc::now()).unwrap();
        assert!((z_score - 3.0).abs() < 1e-9);
    }

    #[test]
    fn test_live_z_score_applies_hedge_ratio() {
        let mut baseline = unit_baseline(2.5);
        baseline.hedge_ratio = 2.0;

        // Spread is 20 - 2 * 5 = 10.
        assert!((baseline.live_z_score(&live(20.0, 5.0), Utc::now()).unwrap() - 10.0).abs() < 1e-9);
    }

    #[test]
    fn test_live_z_score_requires_both_legs() {
        let baseline = unit_baseline(2.5);
        let mut only_long = HashMap::new();
        only_long.insert(ticker("AAPL"), priced_leg(20.0, Utc::now()));

        assert!(baseline.live_z_score(&only_long, Utc::now()).is_none());
        assert!(baseline.live_z_score(&HashMap::new(), Utc::now()).is_none());
    }

    #[test]
    fn test_crossing_detected_on_convergence() {
        // Entered long-spread at z = 2.5; the live spread has crossed back
        // through zero.
        let baselines = vec![unit_baseline(2.5)];
        let crossed = crossed_pairs_now(&baselines, &live(1.0, 2.0));

        assert_eq!(crossed.len(), 1);
        assert!(crossed[0].1 < 0.0);
    }

    #[test]
    fn test_crossing_detected_on_stop_loss() {
        // Entered at z = 2.5 and the spread widened past the stop-loss level.
        let baselines = vec![unit_baseline(2.5)];
        let crossed = crossed_pairs_now(&baselines, &live(5.0, 0.0));

        assert_eq!(crossed.len(), 1);
        assert!(crossed[0].1 >= 4.0);
    }

    #[test]
    fn test_no_crossing_while_within_range() {
        // Still on the entry side of zero and short of the stop-loss level.
        let baselines = vec![unit_baseline(2.5)];
        assert!(crossed_pairs_now(&baselines, &live(2.0, 0.0)).is_empty());
    }

    #[test]
    fn test_no_crossing_without_quotes() {
        let baselines = vec![unit_baseline(2.5)];
        assert!(crossed_pairs_now(&baselines, &HashMap::new()).is_empty());
    }

    #[test]
    fn test_no_crossing_without_baselines() {
        assert!(crossed_pairs_now(&[], &live(1.0, 2.0)).is_empty());
    }

    #[test]
    fn test_trigger_z_score_matches_authoritative_pass() {
        // The livelock regression. The trigger standardizes the live spread
        // against history; so does evaluate_open_pairs. If the two used
        // different distributions the trigger would fire on a larger magnitude
        // than the pass computes, the pass would keep the pair open, the
        // debounce would expire, and the trigger would fire again forever.
        let history = vec![10.0, 12.0, 11.0, 9.0, 13.0, 8.0];
        let baseline = baseline_with(2.5, history.clone());

        let long_mid = 30.0;
        let short_mid = 4.0;
        let trigger_z = baseline
            .live_z_score(&live(long_mid, short_mid), Utc::now())
            .unwrap();

        // What evaluate_open_pairs computes for the same inputs.
        let live_spread = long_mid - baseline.hedge_ratio * short_mid;
        let authoritative_z = z_score_against(&history, live_spread);

        assert!(
            (trigger_z - authoritative_z).abs() < 1e-12,
            "trigger and authoritative z-scores must be identical: {trigger_z} vs {authoritative_z}"
        );
        // And a crossing seen by the trigger is therefore actionable by the pass.
        assert_eq!(
            close_reason_for(baseline.entry_z_score, trigger_z),
            close_reason_for(baseline.entry_z_score, authoritative_z)
        );
    }

    #[test]
    fn test_flat_history_yields_no_signal() {
        // z_score_against returns zero for a zero-deviation series, which
        // close_reason_for reads as no signal rather than convergence.
        let baseline = baseline_with(2.5, vec![5.0, 5.0, 5.0]);
        assert_eq!(
            baseline.live_z_score(&live(99.0, 0.0), Utc::now()),
            Some(0.0)
        );
        assert!(crossed_pairs_now(&[baseline], &live(99.0, 0.0)).is_empty());
    }

    // --- baseline_for discard rules ---

    fn open_pair(hedge_ratio: f64) -> OpenPair {
        OpenPair::new_for_test(
            uuid::Uuid::new_v4(),
            PairID::new(ticker("AAPL"), ticker("MSFT")),
            ticker("AAPL"),
            ticker("MSFT"),
            2.5,
            hedge_ratio,
        )
    }

    #[test]
    fn test_baseline_built_from_aligned_history() {
        let baseline = baseline_for(&open_pair(1.0), &[10.0, 12.0, 14.0], &[1.0, 2.0, 3.0])
            .expect("varying spread should produce a baseline");
        assert_eq!(baseline.spread_history.len(), 3);
    }

    #[test]
    fn test_baseline_truncates_to_common_length() {
        // Legs with unequal history align on the shorter one.
        let baseline = baseline_for(&open_pair(1.0), &[1.0, 10.0, 12.0, 14.0], &[1.0, 2.0])
            .expect("baseline should build from the common tail");
        assert_eq!(baseline.spread_history.len(), 2);
    }

    #[test]
    fn test_baseline_rejects_too_few_samples() {
        assert!(baseline_for(&open_pair(1.0), &[10.0], &[1.0]).is_none());
        assert!(baseline_for(&open_pair(1.0), &[], &[]).is_none());
    }

    #[test]
    fn test_baseline_rejects_flat_spread() {
        // A constant spread has zero deviation; dividing by it would yield
        // infinity and fire the trigger permanently.
        assert!(baseline_for(&open_pair(1.0), &[10.0, 11.0, 12.0], &[1.0, 2.0, 3.0]).is_none());
    }

    #[test]
    fn test_trigger_agrees_with_authoritative_close_rule() {
        // The trigger must not invent its own notion of a close. Both sides call
        // close_reason_for, so a z-score the trigger fires on is one the
        // rebalance pass would also act on given the same value.
        for (entry_z, current_z) in [(2.5, -0.5), (-2.5, 0.5), (2.5, 4.5), (-2.5, -4.5)] {
            assert!(close_reason_for(entry_z, current_z).is_some());
            let baselines = vec![unit_baseline(entry_z)];
            assert_eq!(
                crossed_pairs_now(&baselines, &live(current_z, 0.0)).len(),
                1
            );
        }
    }

    #[test]
    fn test_trigger_does_not_fire_on_a_skewed_spread() {
        // A crossing z-score is not enough: the two legs must also describe one
        // moment. The authoritative pass rejects a skewed pair through the same
        // `current_spread`, so a trigger that fired here would request a pass
        // that declines to act — fire, decline, debounce, fire again.
        let now = Utc::now();
        for (entry_z, current_z) in [(2.5, -0.5), (-2.5, -4.5)] {
            let baselines = vec![unit_baseline(entry_z)];

            let simultaneous = legs_observed_at(current_z, now, 0.0, now);
            assert_eq!(crossed_pairs(&baselines, &simultaneous, now).len(), 1);

            let skewed = legs_observed_at(
                current_z,
                now,
                0.0,
                now - chrono::Duration::seconds(
                    crate::portfolio::spread::MAXIMUM_LEG_SKEW_SECONDS + 1,
                ),
            );
            assert!(crossed_pairs(&baselines, &skewed, now).is_empty());
        }
    }

    #[test]
    fn test_trigger_still_fires_on_equally_aged_legs() {
        // The absolute staleness window is five minutes for streamed and
        // snapshot quotes alike. Two legs equally old inside it give a coherent
        // spread measured a few minutes ago, which is usable for a
        // mean-reverting series — and is what the retired sixty-second streamed
        // window rejected.
        let now = Utc::now();
        let observed_at = now - chrono::Duration::seconds(240);
        let baselines = vec![unit_baseline(2.5)];
        let legs = legs_observed_at(-0.5, observed_at, 0.0, observed_at);

        assert_eq!(crossed_pairs(&baselines, &legs, now).len(), 1);
    }

    #[test]
    fn test_debounce_exceeds_check_interval() {
        // A crossing persists until the rebalance pass closes the position, so
        // the debounce must outlast several checks or the trigger would emit on
        // every tick of the pass it already requested.
        assert!(EMISSION_DEBOUNCE > CROSSING_CHECK_INTERVAL);
        assert!(BASELINE_REFRESH_INTERVAL > CROSSING_CHECK_INTERVAL);
    }
}
