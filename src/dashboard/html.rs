//! Server-rendered HTML page for the dashboard.
//!
//! Renders all five data sections (positions, performance, trades, predictions,
//! events) as a single vertically-stacked page with a dark terminal aesthetic.
//! The page auto-refreshes every 30 seconds via `<meta http-equiv="refresh">`.

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
const EVENTS_DISPLAY_LIMIT: usize = 50;

/// Renders the complete HTML page from the current dashboard state.
pub fn render_html(state: &DashboardState) -> String {
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
<header>
<h1>OSCM</h1>
<span class="updated">{updated}</span>
</header>
{positions}
{performance}
{trades}
{predictions}
{events}
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
        "<tr><td colspan=\"8\" class=\"muted\">No open positions</td></tr>".to_string()
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
<h2>Open Positions{rebalance_indicator}</h2>
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
<h2>Performance</h2>
<table>
<thead><tr><th>PERIOD</th><th>FUND</th><th>SPY</th></tr></thead>
<tbody>{rows}</tbody>
</table>
</section>"#,
    )
}

fn render_trades_section(state: &DashboardState) -> String {
    let table_body = if state.closed_trades.is_empty() {
        "<tr><td colspan=\"8\" class=\"muted\">No closed trades</td></tr>".to_string()
    } else {
        state
            .closed_trades
            .iter()
            .map(|trade| {
                let profit_and_loss_str = trade
                    .realized_profit_and_loss
                    .map(|value| format!("${:.2}", value))
                    .unwrap_or_else(|| "\u{2014}".to_string());
                let profit_and_loss_class = match trade.realized_profit_and_loss {
                    Some(value) if value.is_sign_positive() && !value.is_zero() => "positive",
                    Some(value) if value.is_sign_negative() => "negative",
                    _ => "",
                };

                let return_str = trade
                    .return_percent
                    .map(|value| format!("{:+.2}%", value))
                    .unwrap_or_else(|| "\u{2014}".to_string());
                let return_class = match trade.return_percent {
                    Some(value) if value.is_sign_positive() && !value.is_zero() => "positive",
                    Some(value) if value.is_sign_negative() => "negative",
                    _ => "",
                };

                let holding = trade
                    .holding_seconds
                    .map(format_holding_duration)
                    .unwrap_or_else(|| "\u{2014}".to_string());
                let reason = trade
                    .close_reason
                    .as_ref()
                    .map(|reason| reason.as_str())
                    .unwrap_or("\u{2014}");
                let closed = trade
                    .closed_at
                    .map(|timestamp| timestamp.format("%Y-%m-%d").to_string())
                    .unwrap_or_else(|| "\u{2014}".to_string());

                format!(
                    "<tr><td>{}</td><td>{}</td><td>{}</td>\
                     <td class=\"{profit_and_loss_class}\">{profit_and_loss_str}</td>\
                     <td class=\"{return_class}\">{return_str}</td>\
                     <td>{holding}</td><td>{}</td><td>{closed}</td></tr>",
                    html_escape(trade.pair_id.as_str()),
                    html_escape(trade.long_ticker.as_str()),
                    html_escape(trade.short_ticker.as_str()),
                    html_escape(reason),
                )
            })
            .collect::<Vec<_>>()
            .join("\n")
    };

    let summary = &state.closed_trades_summary;
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

    format!(
        r#"<section>
<h2>Closed Trades</h2>
<table>
<thead><tr><th>PAIR</th><th>LONG</th><th>SHORT</th><th>P&amp;L</th><th>RETURN</th><th>HOLDING</th><th>REASON</th><th>CLOSED</th></tr></thead>
<tbody>{table_body}</tbody>
</table>
<p class="summary">{} closed | Win: {win_rate} | PF: {profit_factor} | Avg return: {average_return} | Avg hold: {average_holding} | Total P&amp;L: {total_profit_and_loss}</p>
</section>"#,
        summary.total_closed,
    )
}

fn render_predictions_section(state: &DashboardState) -> String {
    let freshness = render_freshness_line(state);

    let table_body = if state.predictions.is_empty() {
        "<tr><td colspan=\"6\" class=\"muted\">No predictions available</td></tr>".to_string()
    } else {
        state
            .predictions
            .iter()
            .map(|prediction| {
                let q50_class = if prediction.quantile_50 > 0.0 {
                    "positive"
                } else if prediction.quantile_50 < 0.0 {
                    "negative"
                } else {
                    ""
                };

                format!(
                    "<tr><td>{}</td><td>{:+.4}</td><td class=\"{q50_class}\">{:+.4}</td>\
                     <td>{:+.4}</td><td>{}</td><td>{}</td></tr>",
                    html_escape(prediction.ticker.as_str()),
                    prediction.quantile_10,
                    prediction.quantile_50,
                    prediction.quantile_90,
                    html_escape(&prediction.model_run_id),
                    format_age(prediction.timestamp),
                )
            })
            .collect::<Vec<_>>()
            .join("\n")
    };

    format!(
        r#"<section>
<h2>Model Predictions</h2>
<p class="freshness">{freshness}</p>
<table>
<thead><tr><th>TICKER</th><th>Q10</th><th>Q50</th><th>Q90</th><th>MODEL RUN</th><th>AGE</th></tr></thead>
<tbody>{table_body}</tbody>
</table>
</section>"#,
    )
}

fn render_freshness_line(state: &DashboardState) -> String {
    let mut parts: Vec<String> = Vec::new();

    match &state.model_run_information {
        None => {
            parts.push(r#"<span class="muted">Model run: no completed runs</span>"#.to_string());
        }
        Some(info) => {
            let age_class = model_run_age_css_class(info);
            parts.push(format!(
                r#"Run: <span class="{age_class}">{}</span>"#,
                format_age(info.completed_at()),
            ));

            if let Some(crps) = info.continuous_ranked_probability_score() {
                parts.push(format!(r#"<span class="muted">CRPS:</span> {crps:.3}"#));
            }
            if let Some(directional_accuracy) = info.directional_accuracy() {
                parts.push(format!(
                    r#"<span class="muted">DA:</span> {:.1}%"#,
                    directional_accuracy * 100.0,
                ));
            }
        }
    }

    if let Some(inserted_at) = state.latest_bars_inserted_at {
        let style = bars_age_css_class(inserted_at);
        parts.push(format!(
            r#"<span class="muted">Bars:</span> <span class="{style}">{}</span>"#,
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
                r#"<tr><td class="muted">{time}</td><td class="{event_class}">{}</td><td class="muted">{payload_summary}</td></tr>"#,
                html_escape(entry.event_type.as_str()),
            )
        })
        .collect();

    let table_body = if rows.is_empty() {
        "<tr><td colspan=\"3\" class=\"muted\">No events received yet</td></tr>".to_string()
    } else {
        rows.join("\n")
    };

    format!(
        r#"<section>
<h2>Recent Events</h2>
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

/// Inline CSS for the dashboard page. Dark terminal aesthetic with monospace font.
const CSS: &str = r#"
:root {
    --bg: #1a1a2e;
    --fg: #e0e0e0;
    --muted: #666;
    --green: #4ec9b0;
    --red: #f44747;
    --yellow: #dcdcaa;
    --border: #333;
}
* { margin: 0; padding: 0; box-sizing: border-box; }
body {
    font-family: 'SF Mono', 'Fira Code', 'Cascadia Code', 'JetBrains Mono', monospace;
    font-size: 13px;
    background: var(--bg);
    color: var(--fg);
    padding: 16px;
    max-width: 1200px;
    margin: 0 auto;
}
header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding-bottom: 12px;
    border-bottom: 1px solid var(--border);
}
h1 { font-size: 16px; font-weight: 700; }
.updated { color: var(--muted); font-size: 12px; }
section { margin-top: 20px; }
h2 {
    font-size: 13px;
    font-weight: 600;
    margin-bottom: 8px;
    border-bottom: 1px solid var(--border);
    padding-bottom: 4px;
    text-transform: uppercase;
    letter-spacing: 0.5px;
    color: var(--fg);
}
table { border-collapse: collapse; width: 100%; }
th, td { text-align: left; padding: 3px 12px 3px 0; white-space: nowrap; }
th { color: var(--muted); font-weight: 600; font-size: 11px; text-transform: uppercase; }
.positive { color: var(--green); }
.negative { color: var(--red); }
.stale { color: var(--red); }
.warning { color: var(--yellow); }
.fresh { color: var(--green); }
.muted { color: var(--muted); }
.summary { color: var(--muted); margin-top: 6px; font-size: 12px; }
.freshness { color: var(--fg); margin-bottom: 8px; font-size: 12px; }
.event-errored { color: var(--red); }
.event-completed { color: var(--green); }
.event-started { color: var(--yellow); }
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
    fn test_render_html_with_closed_trade() {
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
        assert!(html.contains("1 closed"));
    }

    #[test]
    fn test_render_html_empty_predictions_shows_placeholder() {
        let state = DashboardState::default();
        let html = render_html(&state);
        assert!(html.contains("No predictions available"));
        assert!(html.contains("no completed runs"));
    }

    #[test]
    fn test_render_html_with_predictions() {
        let mut state = DashboardState::default();
        state.predictions = vec![PredictionRow {
            ticker: Ticker::new("AAPL").unwrap(),
            quantile_10: -0.001,
            quantile_50: 0.002,
            quantile_90: 0.005,
            model_run_id: "run-abc123".to_string(),
            timestamp: Utc::now() - Duration::hours(2),
        }];
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
        assert!(html.contains("AAPL"));
        assert!(html.contains("run-abc123"));
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
    fn test_html_escape_special_characters() {
        assert_eq!(html_escape("<script>"), "&lt;script&gt;");
        assert_eq!(html_escape("a&b"), "a&amp;b");
        assert_eq!(html_escape("\"hi\""), "&quot;hi&quot;");
    }

    #[test]
    fn test_compute_bars_age_css_class_fresh_on_saturday_with_friday_ingest() {
        // Saturday Jan 4, inserted Friday Jan 3 → fresh
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
        // Saturday Jan 4, inserted Wednesday Jan 1 → stale (missed Thu+Fri)
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
        // Sunday Jan 5, inserted Friday Jan 3 → fresh
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
        // Sunday Jan 5, inserted Thursday Jan 2 → stale (missed Friday)
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
    fn test_render_html_escapes_xss_in_model_run_id() {
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
        assert!(!html.contains("<script>"));
        assert!(html.contains("&lt;script&gt;"));
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
