//! Live-quote exit trigger.
//!
//! Watches streamed quotes for open pairs and emits
//! `portfolio_evaluation_requested` when a spread crosses a close threshold, so
//! an exit is acted on within seconds rather than waiting for the five-minute
//! heartbeat.
//!
//! The hot path is pure arithmetic against cached baselines: no Alpaca calls, no
//! database reads, no writes. Only a crossing pays for a durable event, which is
//! the `DataBoundary` contract — raw quotes stay ephemeral and only a decision
//! crosses into PostgreSQL.
//!
//! This is a trigger, not a decision. It answers "something may have crossed,
//! go look" and the authoritative close is made by `evaluate_open_pairs` inside
//! the rebalance pass, which recomputes the z-score over the full daily series.
//! Both sides share [`close_reason_for`] so they can never disagree about what a
//! given z-score means, but the trigger's z-score is deliberately the cheaper
//! approximation: it measures the live spread against baseline statistics rather
//! than recomputing them. Being slightly eager is the correct bias for a trigger
//! whose only cost is one extra evaluation pass.

use std::collections::HashMap;
use std::time::Duration;

use chrono::Utc;
use sqlx::PgPool;
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::common::events::{emit_event, EventType};
use crate::domain::market::{PairID, Ticker};
use crate::portfolio::database::{fetch_historical_equity_prices, fetch_open_pairs};
use crate::portfolio::live_prices::LivePriceCache;
use crate::portfolio::math::{mean, standard_deviation};
use crate::portfolio::rebalance::close_reason_for;

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
    spread_mean: f64,
    spread_standard_deviation: f64,
}

impl PairBaseline {
    /// Returns the z-score of the live spread against the daily baseline.
    ///
    /// `None` when either leg lacks a fresh quote. Both legs are required for
    /// the same reason the authoritative evaluation requires them: a live long
    /// against a stale short measures a day of drift in one leg, not a spread.
    fn live_z_score(&self, live_mid_prices: &HashMap<Ticker, f64>) -> Option<f64> {
        let long_mid = live_mid_prices.get(&self.long_ticker)?;
        let short_mid = live_mid_prices.get(&self.short_ticker)?;
        let spread = long_mid - self.hedge_ratio * short_mid;
        Some((spread - self.spread_mean) / self.spread_standard_deviation)
    }
}

/// Rebuilds baselines for every open pair from daily closes.
async fn load_baselines(pool: &PgPool) -> Result<Vec<PairBaseline>, sqlx::Error> {
    let open_pairs = fetch_open_pairs(pool).await?;
    if open_pairs.is_empty() {
        return Ok(Vec::new());
    }
    let historical_prices = fetch_historical_equity_prices(pool).await?;

    let mut baselines = Vec::new();
    for pair in &open_pairs {
        let (Some(long_prices), Some(short_prices)) = (
            historical_prices.get(pair.long_ticker()),
            historical_prices.get(pair.short_ticker()),
        ) else {
            continue;
        };

        let common_length = long_prices.len().min(short_prices.len());
        if common_length < MINIMUM_SPREAD_SAMPLES {
            continue;
        }

        let spread: Vec<f64> = long_prices[long_prices.len() - common_length..]
            .iter()
            .zip(short_prices[short_prices.len() - common_length..].iter())
            .map(|(long, short)| long - pair.hedge_ratio() * short)
            .collect();

        let spread_standard_deviation = standard_deviation(&spread, 1);
        // A flat spread cannot produce a meaningful z-score, and dividing by it
        // would yield infinity and fire the trigger permanently.
        if spread_standard_deviation <= 0.0 || !spread_standard_deviation.is_finite() {
            continue;
        }

        baselines.push(PairBaseline {
            pair_id: pair.pair_id().clone(),
            long_ticker: pair.long_ticker().clone(),
            short_ticker: pair.short_ticker().clone(),
            hedge_ratio: pair.hedge_ratio(),
            entry_z_score: pair.entry_z_score(),
            spread_mean: mean(&spread),
            spread_standard_deviation,
        });
    }

    Ok(baselines)
}

/// Returns the pairs whose live spread has crossed a close threshold.
fn crossed_pairs(
    baselines: &[PairBaseline],
    live_mid_prices: &HashMap<Ticker, f64>,
) -> Vec<(PairID, f64)> {
    baselines
        .iter()
        .filter_map(|baseline| {
            let z_score = baseline.live_z_score(live_mid_prices)?;
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

        let live_mid_prices = cache.fresh_mid_prices(Utc::now()).await;
        let crossed = crossed_pairs(&baselines, &live_mid_prices);
        if crossed.is_empty() {
            continue;
        }

        info!(
            pairs = crossed.len(),
            first_pair = crossed[0].0.as_str(),
            first_z_score = crossed[0].1,
            "Live spread crossed a close threshold; requesting evaluation"
        );

        match emit_event(
            &pool,
            EventType::PortfolioEvaluationRequested,
            &serde_json::json!({"reason": "live_threshold_crossing"}),
        )
        .await
        {
            Ok(_) => since_emission = Duration::ZERO,
            Err(error) => {
                warn!(error = %error, "Failed to emit portfolio_evaluation_requested");
            }
        }

        // Baselines are stale the moment a pass may have closed something.
        refresh_due = true;
    }

    info!("Live exit trigger stopped");
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ticker(symbol: &str) -> Ticker {
        Ticker::new(symbol).expect("valid ticker")
    }

    /// Baseline with mean 0 and standard deviation 1, so the live spread value
    /// is itself the z-score and the tests read directly.
    fn unit_baseline(entry_z_score: f64) -> PairBaseline {
        PairBaseline {
            pair_id: PairID::new(ticker("AAPL"), ticker("MSFT")),
            long_ticker: ticker("AAPL"),
            short_ticker: ticker("MSFT"),
            hedge_ratio: 1.0,
            entry_z_score,
            spread_mean: 0.0,
            spread_standard_deviation: 1.0,
        }
    }

    fn live(long: f64, short: f64) -> HashMap<Ticker, f64> {
        let mut prices = HashMap::new();
        prices.insert(ticker("AAPL"), long);
        prices.insert(ticker("MSFT"), short);
        prices
    }

    #[test]
    fn test_live_z_score_uses_baseline_statistics() {
        let mut baseline = unit_baseline(2.5);
        baseline.spread_mean = 10.0;
        baseline.spread_standard_deviation = 2.0;

        // Spread is 20 - 4 = 16; (16 - 10) / 2 = 3.
        let z_score = baseline.live_z_score(&live(20.0, 4.0)).unwrap();
        assert!((z_score - 3.0).abs() < 1e-9);
    }

    #[test]
    fn test_live_z_score_applies_hedge_ratio() {
        let mut baseline = unit_baseline(2.5);
        baseline.hedge_ratio = 2.0;

        // Spread is 20 - 2 * 5 = 10.
        assert!((baseline.live_z_score(&live(20.0, 5.0)).unwrap() - 10.0).abs() < 1e-9);
    }

    #[test]
    fn test_live_z_score_requires_both_legs() {
        let baseline = unit_baseline(2.5);
        let mut only_long = HashMap::new();
        only_long.insert(ticker("AAPL"), 20.0);

        assert!(baseline.live_z_score(&only_long).is_none());
        assert!(baseline.live_z_score(&HashMap::new()).is_none());
    }

    #[test]
    fn test_crossing_detected_on_convergence() {
        // Entered long-spread at z = 2.5; the live spread has crossed back
        // through zero.
        let baselines = vec![unit_baseline(2.5)];
        let crossed = crossed_pairs(&baselines, &live(1.0, 2.0));

        assert_eq!(crossed.len(), 1);
        assert!(crossed[0].1 < 0.0);
    }

    #[test]
    fn test_crossing_detected_on_stop_loss() {
        // Entered at z = 2.5 and the spread widened past the stop-loss level.
        let baselines = vec![unit_baseline(2.5)];
        let crossed = crossed_pairs(&baselines, &live(5.0, 0.0));

        assert_eq!(crossed.len(), 1);
        assert!(crossed[0].1 >= 4.0);
    }

    #[test]
    fn test_no_crossing_while_within_range() {
        // Still on the entry side of zero and short of the stop-loss level.
        let baselines = vec![unit_baseline(2.5)];
        assert!(crossed_pairs(&baselines, &live(2.0, 0.0)).is_empty());
    }

    #[test]
    fn test_no_crossing_without_quotes() {
        let baselines = vec![unit_baseline(2.5)];
        assert!(crossed_pairs(&baselines, &HashMap::new()).is_empty());
    }

    #[test]
    fn test_no_crossing_without_baselines() {
        assert!(crossed_pairs(&[], &live(1.0, 2.0)).is_empty());
    }

    #[test]
    fn test_trigger_agrees_with_authoritative_close_rule() {
        // The trigger must not invent its own notion of a close. Both sides call
        // close_reason_for, so a z-score the trigger fires on is one the
        // rebalance pass would also act on given the same value.
        for (entry_z, current_z) in [(2.5, -0.5), (-2.5, 0.5), (2.5, 4.5), (-2.5, -4.5)] {
            assert!(close_reason_for(entry_z, current_z).is_some());
            let baselines = vec![unit_baseline(entry_z)];
            assert_eq!(crossed_pairs(&baselines, &live(current_z, 0.0)).len(), 1);
        }
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
