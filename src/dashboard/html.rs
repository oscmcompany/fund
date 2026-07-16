//! Server-rendered HTML page for the dashboard.
//!
//! Renders all five data sections (positions, performance, trades, predictions,
//! events) as a single vertically-stacked page with an amber CRT terminal
//! aesthetic inspired by `oscm-terminal.html`. The page auto-refreshes every
//! 30 seconds via `<meta http-equiv="refresh">`.

use chrono::{DateTime, Utc};
use num_traits::ToPrimitive;

use crate::dashboard::cache::DashboardState;
use crate::dashboard::events::event_type_css_class;
use crate::dashboard::events::truncate_payload;
use crate::dashboard::performance::format_return;
use crate::dashboard::performance::return_css_class;
use crate::dashboard::positions::format_dollars;
use crate::dashboard::positions::rebalance_age_css_class;
use crate::dashboard::predictions::format_age;
use crate::dashboard::predictions::model_run_age_css_class;
use crate::dashboard::trades::format_holding_duration;

/// Maximum number of events to display on the page.
const EVENTS_DISPLAY_LIMIT: usize = 10;

/// Renders the complete HTML page from the current dashboard state.
pub fn render_html(state: &DashboardState) -> String {
    let now = Utc::now();
    let date_string = now.format("%d-%b-%Y").to_string().to_uppercase();
    let time_string = now.format("%H:%M:%S UTC").to_string();
    let updated = format_last_updated(state);
    let positions = render_positions_section(state);
    let performance = render_performance_section(state);
    let trades = render_trades_section(state);
    let predictions = render_predictions_section(state);
    let events = render_events_section(state);

    format!(
        r#"<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta http-equiv="refresh" content="30">
<title>OSCM</title>
<style>{CSS}</style>
</head>
<body>
<header class="sys-header">
<span>{date_string}</span>
<span>OSCM // FUND MONITOR</span>
<span>{time_string}</span>
</header>
<div class="updated">{updated}</div>
{positions}
{performance}
{predictions}
{trades}
{events}
<footer class="sys-footer">
<span>AUTO-REFRESH: 30S</span>
<span>STATUS: NOMINAL</span>
</footer>
</body>
</html>"#,
    )
}

fn format_last_updated(state: &DashboardState) -> String {
    match (&state.last_updated, &state.database_error) {
        (Some(time), Some(_)) => format!(
            "Database error (last ok: {})",
            time.format("%Y-%m-%d %H:%M:%S UTC")
        ),
        (None, Some(_)) => "Database error".to_string(),
        (Some(time), None) => {
            format!("Last updated: {}", time.format("%Y-%m-%d %H:%M:%S UTC"))
        }
        (None, None) => "Loading...".to_string(),
    }
}

fn render_positions_section(state: &DashboardState) -> String {
    let rebalance_indicator = match state.last_rebalance_completed_at {
        Some(completed_at) => {
            let css_class = rebalance_age_css_class(completed_at);
            format!(
                r#" <span class="{css_class}">Rebalance: {}</span>"#,
                format_age(completed_at),
            )
        }
        None => String::new(),
    };

    let table_body = if state.open_positions.is_empty() {
        "<tr><td colspan=\"8\" class=\"dim\">No open positions</td></tr>".to_string()
    } else {
        state
            .open_positions
            .iter()
            .map(|position| {
                format!(
                    "<tr><td>{}</td><td>{}</td><td>{}</td><td>{:.2}</td><td>{:.2}</td>\
                     <td>{}</td><td>{}</td><td>{}</td></tr>",
                    html_escape(position.pair_id.as_str()),
                    html_escape(position.long_ticker.as_str()),
                    html_escape(position.short_ticker.as_str()),
                    position.z_score.to_f64().unwrap_or(0.0),
                    position.signal_strength.to_f64().unwrap_or(0.0),
                    format_dollars(position.long_dollar_amount),
                    format_dollars(position.short_dollar_amount),
                    position.opened_at.format("%Y-%m-%d %H:%M"),
                )
            })
            .collect::<Vec<_>>()
            .join("\n")
    };

    let pair_count = state.open_positions.len();
    let pair_word = if pair_count == 1 {
        "pair open"
    } else {
        "pairs open"
    };

    format!(
        r#"<section>
<div class="panel-header">Open Positions{rebalance_indicator}</div>
<table>
<thead><tr><th>PAIR</th><th>LONG</th><th>SHORT</th><th>Z-SCORE</th><th>SIGNAL</th><th>LONG $</th><th>SHORT $</th><th>OPENED</th></tr></thead>
<tbody>{table_body}</tbody>
</table>
<p class="summary">{pair_count} {pair_word} | Gross: {} | Net: {}</p>
</section>"#,
        format_dollars(state.gross_exposure),
        format_dollars(state.net_exposure),
    )
}

fn render_performance_section(state: &DashboardState) -> String {
    let returns = &state.period_returns;

    let rows = [
        ("1D", returns.fund_one_day, returns.spy_one_day),
        ("1W", returns.fund_one_week, returns.spy_one_week),
        ("1M", returns.fund_one_month, returns.spy_one_month),
        ("YTD", returns.fund_year_to_date, returns.spy_year_to_date),
        (
            "Inception",
            returns.fund_since_inception,
            returns.spy_since_inception,
        ),
    ]
    .iter()
    .map(|(label, fund, spy)| {
        let fund_class = return_css_class(*fund);
        let spy_class = return_css_class(*spy);
        format!(
            "<tr><td>{label}</td><td class=\"{fund_class}\">{}</td><td class=\"{spy_class}\">{}</td></tr>",
            format_return(*fund),
            format_return(*spy),
        )
    })
    .collect::<Vec<_>>()
    .join("\n");

    format!(
        r#"<section>
<div class="panel-header">Performance</div>
<table>
<thead><tr><th>PERIOD</th><th>FUND</th><th>SPY</th></tr></thead>
<tbody>{rows}</tbody>
</table>
</section>"#,
    )
}

fn render_trades_section(state: &DashboardState) -> String {
    let summary = &state.closed_trades_summary;

    if summary.total_closed == 0 {
        return r#"<section>
<div class="panel-header">Closed Trades</div>
<p class="dim">No closed trades</p>
</section>"#
            .to_string();
    }

    let win_rate = summary
        .win_rate
        .map(|rate| format!("{:.1}%", rate * 100.0))
        .unwrap_or_else(|| "\u{2014}".to_string());
    let profit_factor = summary
        .profit_factor
        .map(|factor| format!("{:.2}", factor))
        .unwrap_or_else(|| "\u{2014}".to_string());
    let average_return = summary
        .average_return_percent
        .map(|return_percent| format!("{:+.2}%", return_percent))
        .unwrap_or_else(|| "\u{2014}".to_string());
    let average_holding = summary
        .average_holding_seconds
        .map(|seconds| format_holding_duration(seconds as i64))
        .unwrap_or_else(|| "\u{2014}".to_string());
    let total_profit_and_loss = summary
        .total_realized_profit_and_loss
        .map(|value| format!("${:.2}", value))
        .unwrap_or_else(|| "\u{2014}".to_string());
    let total_profit_and_loss_class = match summary.total_realized_profit_and_loss {
        Some(value) if value.is_sign_positive() && !value.is_zero() => "positive",
        Some(value) if value.is_sign_negative() => "negative",
        _ => "",
    };

    format!(
        r#"<section>
<div class="panel-header">Closed Trades</div>
<table>
<thead><tr><th>METRIC</th><th>VALUE</th></tr></thead>
<tbody>
<tr><td>Total closed</td><td>{}</td></tr>
<tr><td>Win rate</td><td>{win_rate}</td></tr>
<tr><td>Profit factor</td><td>{profit_factor}</td></tr>
<tr><td>Avg return</td><td>{average_return}</td></tr>
<tr><td>Avg holding</td><td>{average_holding}</td></tr>
<tr><td>Total P&amp;L</td><td class="{total_profit_and_loss_class}">{total_profit_and_loss}</td></tr>
</tbody>
</table>
</section>"#,
        summary.total_closed,
    )
}

fn render_predictions_section(state: &DashboardState) -> String {
    let freshness = render_freshness_line(state);

    if state.predictions.is_empty() {
        return format!(
            r#"<section>
<div class="panel-header">Model Predictions</div>
<p class="freshness">{freshness}</p>
<p class="dim">No predictions available</p>
</section>"#,
        );
    }

    let count = state.predictions.len();
    let bullish = state
        .predictions
        .iter()
        .filter(|prediction| prediction.quantile_50 > 0.0)
        .count();
    let bearish = state
        .predictions
        .iter()
        .filter(|prediction| prediction.quantile_50 < 0.0)
        .count();
    let neutral = count - bullish - bearish;

    let q50_values: Vec<f64> = {
        let mut values: Vec<f64> = state
            .predictions
            .iter()
            .map(|prediction| prediction.quantile_50)
            .collect();
        values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        values
    };
    let median_q50 = if q50_values.len() % 2 == 0 {
        let mid = q50_values.len() / 2;
        (q50_values[mid - 1] + q50_values[mid]) / 2.0
    } else {
        q50_values[q50_values.len() / 2]
    };
    let median_q50_class = if median_q50 > 0.0 {
        "positive"
    } else if median_q50 < 0.0 {
        "negative"
    } else {
        ""
    };

    let q10_minimum = state
        .predictions
        .iter()
        .map(|prediction| prediction.quantile_10)
        .fold(f64::INFINITY, f64::min);
    let q10_maximum = state
        .predictions
        .iter()
        .map(|prediction| prediction.quantile_10)
        .fold(f64::NEG_INFINITY, f64::max);
    let q90_minimum = state
        .predictions
        .iter()
        .map(|prediction| prediction.quantile_90)
        .fold(f64::INFINITY, f64::min);
    let q90_maximum = state
        .predictions
        .iter()
        .map(|prediction| prediction.quantile_90)
        .fold(f64::NEG_INFINITY, f64::max);

    format!(
        r#"<section>
<div class="panel-header">Model Predictions</div>
<p class="freshness">{freshness}</p>
<table>
<thead><tr><th>METRIC</th><th>VALUE</th></tr></thead>
<tbody>
<tr><td>Tickers</td><td>{count}</td></tr>
<tr><td>Bullish / Bearish / Neutral</td><td>{bullish} / {bearish} / {neutral}</td></tr>
<tr><td>Median Q50</td><td class="{median_q50_class}">{median_q50:+.4}</td></tr>
<tr><td>Q10 range</td><td>{q10_minimum:+.4} to {q10_maximum:+.4}</td></tr>
<tr><td>Q90 range</td><td>{q90_minimum:+.4} to {q90_maximum:+.4}</td></tr>
</tbody>
</table>
</section>"#,
    )
}

fn render_freshness_line(state: &DashboardState) -> String {
    let mut parts: Vec<String> = Vec::new();

    match &state.model_run_information {
        None => {
            parts.push(r#"<span class="dim">Model run: no completed runs</span>"#.to_string());
        }
        Some(info) => {
            let age_class = model_run_age_css_class(info);
            parts.push(format!(
                r#"Run: <span class="{age_class}">{}</span>"#,
                format_age(info.completed_at()),
            ));

            if let Some(crps) = info.continuous_ranked_probability_score() {
                parts.push(format!(r#"<span class="dim">CRPS:</span> {crps:.3}"#));
            }
            if let Some(directional_accuracy) = info.directional_accuracy() {
                parts.push(format!(
                    r#"<span class="dim">DA:</span> {:.1}%"#,
                    directional_accuracy * 100.0,
                ));
            }
        }
    }

    if let Some(inserted_at) = state.latest_bars_inserted_at {
        let style = bars_age_css_class(inserted_at);
        parts.push(format!(
            r#"<span class="dim">Bars:</span> <span class="{style}">{}</span>"#,
            format_age(inserted_at),
        ));
    }

    parts.join(" &nbsp; ")
}

fn render_events_section(state: &DashboardState) -> String {
    let rows: Vec<String> = state
        .events
        .iter()
        .take(EVENTS_DISPLAY_LIMIT)
        .map(|entry| {
            let time = entry.received_at.format("%H:%M:%S");
            let event_class = event_type_css_class(entry.event_type.as_str());
            let payload_summary = html_escape(&truncate_payload(&entry.payload));
            format!(
                r#"<tr><td class="dim">{time}</td><td class="{event_class}">{}</td><td class="dim">{payload_summary}</td></tr>"#,
                html_escape(entry.event_type.as_str()),
            )
        })
        .collect();

    let table_body = if rows.is_empty() {
        "<tr><td colspan=\"3\" class=\"dim\">No events received yet</td></tr>".to_string()
    } else {
        rows.join("\n")
    };

    format!(
        r#"<section>
<div class="panel-header">Recent Events</div>
<table>
<thead><tr><th>TIME</th><th>EVENT</th><th>PAYLOAD</th></tr></thead>
<tbody>{table_body}</tbody>
</table>
</section>"#,
    )
}

/// Returns the CSS class for bar insertion freshness.
///
/// Delegates to [`compute_bars_age_css_class`] with the current time.
fn bars_age_css_class(inserted_at: DateTime<Utc>) -> &'static str {
    compute_bars_age_css_class(inserted_at, Utc::now())
}

/// Computes the bar staleness CSS class given an explicit `now` for testability.
///
/// Red on weekdays when `inserted_at` predates the most recent expected
/// ingest date. Green on weekends and when the bar is current.
fn compute_bars_age_css_class(inserted_at: DateTime<Utc>, now: DateTime<Utc>) -> &'static str {
    use chrono::{Datelike, Duration, Weekday};
    if inserted_at >= now {
        return "fresh";
    }
    // Most recent expected trading day: step back from yesterday until we hit a weekday.
    let mut last_expected_ingest_date = now.date_naive() - Duration::days(1);
    while matches!(
        last_expected_ingest_date.weekday(),
        Weekday::Sat | Weekday::Sun
    ) {
        last_expected_ingest_date -= Duration::days(1);
    }
    if inserted_at.date_naive() < last_expected_ingest_date {
        "stale"
    } else {
        "fresh"
    }
}

/// Escapes `&`, `<`, `>`, and `"` for safe HTML embedding.
fn html_escape(input: &str) -> String {
    input
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

/// Inline CSS for the dashboard page. Amber CRT terminal aesthetic with
/// scanline overlay, matching the `oscm-terminal.html` reference design.
const CSS: &str = r#"
@import url('https://fonts.googleapis.com/css2?family=VT323&display=swap');
:root {
    --bg: #0a0700;
    --amber: #ffb400;
    --amber-dim: #a67500;
    --amber-dark: #4a3400;
    --green: #4ec9b0;
    --red: #f44747;
}
* { margin: 0; padding: 0; box-sizing: border-box; }
body {
    font-family: 'VT323', 'Courier New', Courier, monospace;
    font-size: 18px;
    line-height: 1.2;
    background-color: var(--bg);
    color: var(--amber);
    padding: 12px 16px;
    max-width: 1200px;
    margin: 0 auto;
    background-image: linear-gradient(
        to bottom,
        rgba(255, 180, 0, 0),
        rgba(255, 180, 0, 0) 50%,
        rgba(0, 0, 0, 0.2) 50%,
        rgba(0, 0, 0, 0.2)
    );
    background-size: 100% 4px;
    text-shadow: 0 0 2px rgba(255, 180, 0, 0.4);
}
.sys-header {
    display: flex;
    justify-content: space-between;
    padding-bottom: 4px;
    border-bottom: 1px solid var(--amber);
    margin-bottom: 4px;
    text-transform: uppercase;
}
.updated {
    color: var(--amber-dim);
    font-size: 16px;
    margin-bottom: 8px;
}
section { margin-top: 16px; }
.panel-header {
    text-transform: uppercase;
    border-bottom: 1px solid var(--amber-dim);
    padding-bottom: 4px;
    margin-bottom: 8px;
    color: var(--amber-dim);
}
table { border-collapse: collapse; width: 100%; }
th, td { text-align: left; padding: 2px 12px 2px 0; white-space: nowrap; }
th {
    color: var(--amber-dim);
    font-weight: normal;
    font-size: 16px;
    text-transform: uppercase;
    border-bottom: 1px dashed var(--amber-dark);
    padding-bottom: 4px;
}
tr:hover { background-color: rgba(255, 180, 0, 0.1); }
.positive { color: var(--green); }
.negative { color: var(--red); }
.stale { color: var(--red); }
.warning { color: var(--amber); }
.fresh { color: var(--green); }
.dim { color: var(--amber-dim); }
.summary { color: var(--amber-dim); margin-top: 6px; font-size: 16px; }
.freshness { color: var(--amber); margin-bottom: 8px; font-size: 16px; }
.event-errored { color: var(--red); }
.event-completed { color: var(--green); }
.event-started { color: var(--amber); }
.sys-footer {
    margin-top: 20px;
    background-color: var(--amber);
    color: var(--bg);
    padding: 4px 8px;
    display: flex;
    justify-content: space-between;
    text-shadow: none;
    text-transform: uppercase;
}
"#;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::events::EventType;
    use crate::dashboard::cache::{
        ClosedTrade, ClosedTradesSummary, DashboardState, EventEntry, ModelRunInformation,
        OpenPosition, PerformanceSnapshot, PeriodReturns, PredictionRow,
    };
    use crate::domain::market::{PairID, Ticker};
    use crate::domain::trading::CloseReason;
    use chrono::{Duration, NaiveDate};
    use rust_decimal::Decimal;

    #[test]
    fn test_render_html_default_state_contains_loading() {
        let state = DashboardState::default();
        let html = render_html(&state);
        assert!(html.contains("Loading..."));
        assert!(html.contains("OSCM"));
        assert!(html.contains("<meta http-equiv=\"refresh\" content=\"30\">"));
    }

    #[test]
    fn test_render_html_with_updated_timestamp() {
        let mut state = DashboardState::default();
        state.last_updated = Some(Utc::now());
        let html = render_html(&state);
        assert!(html.contains("Last updated:"));
    }

    #[test]
    fn test_render_html_with_database_error() {
        let mut state = DashboardState::default();
        state.database_error = Some("connection refused".to_string());
        let html = render_html(&state);
        assert!(html.contains("Database error"));
        assert!(!html.contains("connection refused"));
    }

    #[test]
    fn test_render_html_database_error_does_not_render_raw_details() {
        let mut state = DashboardState::default();
        state.last_updated = Some(Utc::now());
        state.database_error = Some("<script>alert(1)</script>".to_string());
        let html = render_html(&state);
        assert!(html.contains("Database error (last ok:"));
        assert!(!html.contains("<script>"));
        assert!(!html.contains("alert(1)"));
    }

    #[test]
    fn test_render_html_empty_positions_shows_placeholder() {
        let state = DashboardState::default();
        let html = render_html(&state);
        assert!(html.contains("No open positions"));
    }

    #[test]
    fn test_render_html_with_positions() {
        let mut state = DashboardState::default();
        state.open_positions = vec![OpenPosition {
            pair_id: PairID::parse("AAPL-MSFT").unwrap(),
            long_ticker: Ticker::new("AAPL").unwrap(),
            short_ticker: Ticker::new("MSFT").unwrap(),
            z_score: Decimal::new(15, 1),
            hedge_ratio: Decimal::ONE,
            signal_strength: Decimal::new(8, 1),
            long_dollar_amount: Decimal::new(10000, 0),
            short_dollar_amount: Decimal::new(9500, 0),
            opened_at: Utc::now(),
        }];
        state.gross_exposure = Decimal::new(19500, 0);
        state.net_exposure = Decimal::new(500, 0);
        let html = render_html(&state);
        assert!(html.contains("AAPL"));
        assert!(html.contains("MSFT"));
        assert!(html.contains("1 pair open"));
        assert!(html.contains("$19500.00"));
    }

    #[test]
    fn test_render_html_plural_pairs() {
        let mut state = DashboardState::default();
        state.open_positions = vec![
            OpenPosition {
                pair_id: PairID::parse("AAPL-MSFT").unwrap(),
                long_ticker: Ticker::new("AAPL").unwrap(),
                short_ticker: Ticker::new("MSFT").unwrap(),
                z_score: Decimal::ONE,
                hedge_ratio: Decimal::ONE,
                signal_strength: Decimal::ONE,
                long_dollar_amount: Decimal::new(10000, 0),
                short_dollar_amount: Decimal::new(9500, 0),
                opened_at: Utc::now(),
            },
            OpenPosition {
                pair_id: PairID::parse("TSLA-NVDA").unwrap(),
                long_ticker: Ticker::new("TSLA").unwrap(),
                short_ticker: Ticker::new("NVDA").unwrap(),
                z_score: Decimal::ONE,
                hedge_ratio: Decimal::ONE,
                signal_strength: Decimal::ONE,
                long_dollar_amount: Decimal::new(8000, 0),
                short_dollar_amount: Decimal::new(7500, 0),
                opened_at: Utc::now(),
            },
        ];
        let html = render_html(&state);
        assert!(html.contains("2 pairs open"));
    }

    #[test]
    fn test_render_html_with_rebalance_age() {
        let mut state = DashboardState::default();
        state.last_rebalance_completed_at = Some(Utc::now() - Duration::minutes(5));
        let html = render_html(&state);
        assert!(html.contains("Rebalance:"));
        assert!(html.contains("fresh"));
    }

    #[test]
    fn test_render_html_performance_section_labels() {
        let state = DashboardState::default();
        let html = render_html(&state);
        assert!(html.contains("Performance"));
        assert!(html.contains("1D"));
        assert!(html.contains("YTD"));
        assert!(html.contains("Inception"));
        assert!(html.contains("FUND"));
        assert!(html.contains("SPY"));
    }

    #[test]
    fn test_render_html_performance_with_values() {
        let mut state = DashboardState::default();
        state.period_returns = PeriodReturns {
            fund_one_day: Some(10.0),
            spy_one_day: Some(1.1),
            ..Default::default()
        };
        let html = render_html(&state);
        assert!(html.contains("+10.00%"));
        assert!(html.contains("+1.10%"));
    }

    #[test]
    fn test_render_html_empty_trades_shows_placeholder() {
        let state = DashboardState::default();
        let html = render_html(&state);
        assert!(html.contains("No closed trades"));
    }

    #[test]
    fn test_render_html_with_closed_trade_summary() {
        let mut state = DashboardState::default();
        state.closed_trades = vec![ClosedTrade {
            pair_id: PairID::parse("AAPL-MSFT").unwrap(),
            long_ticker: Ticker::new("AAPL").unwrap(),
            short_ticker: Ticker::new("MSFT").unwrap(),
            realized_profit_and_loss: Some(Decimal::new(500, 0)),
            return_percent: Some(Decimal::new(2, 0)),
            holding_seconds: Some(3600),
            close_reason: Some(CloseReason::ProfitTaken),
            closed_at: Some(Utc::now()),
        }];
        state.closed_trades_summary = ClosedTradesSummary {
            total_closed: 1,
            win_rate: Some(1.0),
            profit_factor: None,
            average_return_percent: Some(2.0),
            average_holding_seconds: Some(3600.0),
            total_realized_profit_and_loss: Some(Decimal::new(500, 0)),
        };
        let html = render_html(&state);
        assert!(html.contains("$500.00"));
        assert!(html.contains("positive"));
        assert!(html.contains("1h 0m"));
        assert!(html.contains("Total closed"));
        assert!(html.contains("Win rate"));
        assert!(html.contains("100.0%"));
    }

    #[test]
    fn test_render_html_empty_predictions_shows_placeholder() {
        let state = DashboardState::default();
        let html = render_html(&state);
        assert!(html.contains("No predictions available"));
        assert!(html.contains("no completed runs"));
    }

    #[test]
    fn test_render_html_with_predictions_summary() {
        let mut state = DashboardState::default();
        state.predictions = vec![
            PredictionRow {
                ticker: Ticker::new("AAPL").unwrap(),
                quantile_10: -0.001,
                quantile_50: 0.002,
                quantile_90: 0.005,
                model_run_id: "run-abc123".to_string(),
                timestamp: Utc::now() - Duration::hours(2),
            },
            PredictionRow {
                ticker: Ticker::new("MSFT").unwrap(),
                quantile_10: -0.003,
                quantile_50: -0.001,
                quantile_90: 0.002,
                model_run_id: "run-abc123".to_string(),
                timestamp: Utc::now() - Duration::hours(2),
            },
        ];
        state.model_run_information = Some(
            ModelRunInformation::new(
                Utc::now() - Duration::hours(2),
                Some(NaiveDate::from_ymd_opt(2025, 1, 1).unwrap()),
                Some(NaiveDate::from_ymd_opt(2025, 12, 31).unwrap()),
                Some(0.123),
                Some(0.725),
            )
            .unwrap(),
        );
        let html = render_html(&state);
        assert!(html.contains("Tickers"));
        assert!(html.contains("2"));
        assert!(html.contains("Bullish / Bearish / Neutral"));
        assert!(html.contains("1 / 1 / 0"));
        assert!(html.contains("Median Q50"));
        assert!(html.contains("CRPS:"));
        assert!(html.contains("DA:"));
    }

    #[test]
    fn test_render_html_with_bars_freshness() {
        let mut state = DashboardState::default();
        state.latest_bars_inserted_at = Some(Utc::now() - Duration::hours(3));
        let html = render_html(&state);
        assert!(html.contains("Bars:"));
    }

    #[test]
    fn test_render_html_empty_events_shows_placeholder() {
        let state = DashboardState::default();
        let html = render_html(&state);
        assert!(html.contains("No events received yet"));
    }

    #[test]
    fn test_render_html_with_events() {
        let mut state = DashboardState::default();
        state.events.push_back(EventEntry {
            event_id: 1,
            event_type: EventType::PortfolioRebalanceCompleted,
            payload: serde_json::json!({"session_id": "abc"}),
            received_at: Utc::now(),
        });
        let html = render_html(&state);
        assert!(html.contains("portfolio_rebalance_completed"));
        assert!(html.contains("event-completed"));
    }

    #[test]
    fn test_render_html_events_limited_to_10() {
        let mut state = DashboardState::default();
        for index in 0..20 {
            state.events.push_back(EventEntry {
                event_id: index,
                event_type: EventType::PortfolioRebalanceCompleted,
                payload: serde_json::json!({"index": index}),
                received_at: Utc::now(),
            });
        }
        let html = render_html(&state);
        let event_row_count = html.matches("portfolio_rebalance_completed").count();
        assert_eq!(event_row_count, 10);
    }

    #[test]
    fn test_html_escape_special_characters() {
        assert_eq!(html_escape("<script>"), "&lt;script&gt;");
        assert_eq!(html_escape("a&b"), "a&amp;b");
        assert_eq!(html_escape("\"hi\""), "&quot;hi&quot;");
    }

    #[test]
    fn test_compute_bars_age_css_class_fresh_on_saturday_with_friday_ingest() {
        // Saturday Jan 4, inserted Friday Jan 3 -> fresh
        let now = NaiveDate::from_ymd_opt(2025, 1, 4)
            .unwrap()
            .and_hms_opt(9, 0, 0)
            .unwrap()
            .and_utc();
        let inserted_at = NaiveDate::from_ymd_opt(2025, 1, 3)
            .unwrap()
            .and_hms_opt(22, 0, 0)
            .unwrap()
            .and_utc();
        assert_eq!(compute_bars_age_css_class(inserted_at, now), "fresh");
    }

    #[test]
    fn test_compute_bars_age_css_class_stale_on_saturday_with_old_ingest() {
        // Saturday Jan 4, inserted Wednesday Jan 1 -> stale (missed Thu+Fri)
        let now = NaiveDate::from_ymd_opt(2025, 1, 4)
            .unwrap()
            .and_hms_opt(9, 0, 0)
            .unwrap()
            .and_utc();
        let inserted_at = NaiveDate::from_ymd_opt(2025, 1, 1)
            .unwrap()
            .and_hms_opt(22, 0, 0)
            .unwrap()
            .and_utc();
        assert_eq!(compute_bars_age_css_class(inserted_at, now), "stale");
    }

    #[test]
    fn test_compute_bars_age_css_class_fresh_on_sunday_with_friday_ingest() {
        // Sunday Jan 5, inserted Friday Jan 3 -> fresh
        let now = NaiveDate::from_ymd_opt(2025, 1, 5)
            .unwrap()
            .and_hms_opt(9, 0, 0)
            .unwrap()
            .and_utc();
        let inserted_at = NaiveDate::from_ymd_opt(2025, 1, 3)
            .unwrap()
            .and_hms_opt(22, 0, 0)
            .unwrap()
            .and_utc();
        assert_eq!(compute_bars_age_css_class(inserted_at, now), "fresh");
    }

    #[test]
    fn test_compute_bars_age_css_class_stale_on_sunday_with_old_ingest() {
        // Sunday Jan 5, inserted Thursday Jan 2 -> stale (missed Friday)
        let now = NaiveDate::from_ymd_opt(2025, 1, 5)
            .unwrap()
            .and_hms_opt(9, 0, 0)
            .unwrap()
            .and_utc();
        let inserted_at = NaiveDate::from_ymd_opt(2025, 1, 2)
            .unwrap()
            .and_hms_opt(22, 0, 0)
            .unwrap()
            .and_utc();
        assert_eq!(compute_bars_age_css_class(inserted_at, now), "stale");
    }

    #[test]
    fn test_compute_bars_age_css_class_red_when_weekday_ingest_missed() {
        let now = NaiveDate::from_ymd_opt(2025, 1, 7)
            .unwrap()
            .and_hms_opt(9, 0, 0)
            .unwrap()
            .and_utc();
        let inserted_at = NaiveDate::from_ymd_opt(2025, 1, 3)
            .unwrap()
            .and_hms_opt(22, 0, 0)
            .unwrap()
            .and_utc();
        assert_eq!(compute_bars_age_css_class(inserted_at, now), "stale");
    }

    #[test]
    fn test_compute_bars_age_css_class_green_on_monday_after_weekend() {
        let now = NaiveDate::from_ymd_opt(2025, 1, 6)
            .unwrap()
            .and_hms_opt(9, 0, 0)
            .unwrap()
            .and_utc();
        let inserted_at = NaiveDate::from_ymd_opt(2025, 1, 3)
            .unwrap()
            .and_hms_opt(22, 0, 0)
            .unwrap()
            .and_utc();
        assert_eq!(compute_bars_age_css_class(inserted_at, now), "fresh");
    }

    #[test]
    fn test_render_html_is_valid_structure() {
        let state = DashboardState::default();
        let html = render_html(&state);
        assert!(html.starts_with("<!DOCTYPE html>"));
        assert!(html.contains("</html>"));
        assert!(html.contains("<style>"));
        assert!(html.contains("</body>"));
    }

    #[test]
    fn test_render_html_has_amber_terminal_styling() {
        let state = DashboardState::default();
        let html = render_html(&state);
        assert!(html.contains("VT323"));
        assert!(html.contains("#0a0700"));
        assert!(html.contains("#ffb400"));
        assert!(html.contains("sys-header"));
        assert!(html.contains("sys-footer"));
        assert!(html.contains("FUND MONITOR"));
    }

    #[test]
    fn test_render_html_escapes_xss_in_prediction_summary() {
        let mut state = DashboardState::default();
        state.predictions = vec![PredictionRow {
            ticker: Ticker::new("AAPL").unwrap(),
            quantile_10: 0.0,
            quantile_50: 0.0,
            quantile_90: 0.0,
            model_run_id: "<script>alert(1)</script>".to_string(),
            timestamp: Utc::now(),
        }];
        let html = render_html(&state);
        // Predictions are now a summary table, so model_run_id is not rendered.
        // XSS vector no longer applies, but verify no script tags sneak through.
        assert!(!html.contains("<script>"));
    }

    #[test]
    fn test_performance_snapshot_not_needed_for_html() {
        // Verify performance section renders even with empty history
        let mut state = DashboardState::default();
        state.performance_history = vec![PerformanceSnapshot {
            snapshot_timestamp: Utc::now(),
            net_asset_value: Decimal::new(100000, 0),
            gross_return: None,
            net_return: None,
            total_slippage_cost: Decimal::ZERO,
            spy_close: None,
        }];
        let html = render_html(&state);
        assert!(html.contains("Performance"));
    }
}
