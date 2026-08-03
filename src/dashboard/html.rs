//! The server-rendered page.
//!
//! Five sections stacked vertically — account, open pairs, predictions, closed pairs, events — in
//! an amber CRT aesthetic. The page refreshes every thirty seconds with a `<meta>` tag, matching
//! the poll interval; no client-side JavaScript and nothing loaded from a CDN.
//!
//! It renders only what the database can answer. There is no benchmark curve (the universe holds no
//! index ETF) and no training metrics (the trainer publishes those to S3, not to a table).

use chrono::{DateTime, Duration, Utc};
use rust_decimal::Decimal;

use crate::dashboard::cache::DashboardState;

/// Events shown on the page, out of the several hundred the buffer holds.
const EVENTS_DISPLAY_LIMIT: usize = 12;

/// Closed pairs shown on the page. The summary underneath covers all of them.
const CLOSED_PAIRS_DISPLAY_LIMIT: usize = 20;

/// Predictions shown on the page.
const PREDICTIONS_DISPLAY_LIMIT: usize = 15;

/// How old daily bars may be before the freshness line turns amber, then red.
const BARS_WARNING_AGE: Duration = Duration::hours(24);
const BARS_STALE_AGE: Duration = Duration::hours(48);

/// Renders the whole page from the current state.
pub fn render_html(state: &DashboardState) -> String {
    render_html_at(state, Utc::now())
}

/// Renders the page as of `now`.
///
/// The clock is a parameter so the age calculations can be tested at a fixed instant rather than
/// whenever the suite happens to run.
fn render_html_at(state: &DashboardState, now: DateTime<Utc>) -> String {
    let date_string = now.format("%d-%b-%Y").to_string().to_uppercase();
    let time_string = now.format("%H:%M:%S UTC").to_string();
    let updated = render_updated_line(state, now);
    let freshness = render_freshness_line(state, now);
    let account = render_account_section(state);
    let open_pairs = render_open_pairs_section(state, now);
    let predictions = render_predictions_section(state);
    let closed_pairs = render_closed_pairs_section(state);
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
<div class="freshness">{freshness}</div>
{account}
{open_pairs}
{predictions}
{closed_pairs}
{events}
<footer class="sys-footer">
<span>AUTO-REFRESH: 30S</span>
<span>STATUS: NOMINAL</span>
</footer>
</body>
</html>"#,
    )
}

/// States when the page last refreshed, and says so loudly when the last poll failed.
fn render_updated_line(state: &DashboardState, now: DateTime<Utc>) -> String {
    match (&state.last_updated, &state.last_error) {
        (Some(updated), Some(error)) => format!(
            r#"<span class="stale">DATA STALE — last good refresh {} ago: {}</span>"#,
            format_age(now - *updated),
            html_escape(error)
        ),
        (Some(updated), None) => format!("Updated {} ago", format_age(now - *updated)),
        (None, Some(error)) => format!(
            r#"<span class="stale">No data yet: {}</span>"#,
            html_escape(error)
        ),
        (None, None) => "Awaiting first refresh".to_string(),
    }
}

/// Reports how old the most recent daily bar is.
///
/// The one number that says whether anything upstream is still running: predictions, the screen,
/// and every exit decision read from `equity_bars`, so a stale table silently freezes the strategy
/// rather than stopping it.
fn render_freshness_line(state: &DashboardState, now: DateTime<Utc>) -> String {
    match state.latest_bars_inserted_at {
        Some(inserted_at) => {
            let age = now - inserted_at;
            let class = if age >= BARS_STALE_AGE {
                "stale"
            } else if age >= BARS_WARNING_AGE {
                "warning"
            } else {
                "fresh"
            };
            format!(
                r#"BARS: <span class="{class}">{} old</span>"#,
                format_age(age)
            )
        }
        None => r#"BARS: <span class="stale">none</span>"#.to_string(),
    }
}

fn render_account_section(state: &DashboardState) -> String {
    let Some(account) = &state.account else {
        return section(
            "Account",
            r#"<p class="dim">No account snapshot yet.</p>"#.to_string(),
        );
    };

    let returns = &state.period_returns;
    let body = format!(
        r#"<table>
<tr><th>Equity</th><th>Cash</th><th>Buying power</th><th>Gross exposure</th><th>Net exposure</th><th>As of</th></tr>
<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>
</table>
<table>
<tr><th>1D</th><th>1W</th><th>1M</th><th>YTD</th><th>Inception</th></tr>
<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>
</table>
<p class="summary">{} sessions of history</p>"#,
        format_dollars(account.equity),
        format_dollars(account.cash),
        format_dollars(account.buying_power),
        format_dollars(account.gross_exposure()),
        format_dollars(account.net_exposure()),
        account.session_date,
        format_return(returns.one_day),
        format_return(returns.one_week),
        format_return(returns.one_month),
        format_return(returns.year_to_date),
        format_return(returns.since_inception),
        state.equity_history.len(),
    );
    section("Account", body)
}

fn render_open_pairs_section(state: &DashboardState, now: DateTime<Utc>) -> String {
    if state.open_pairs.is_empty() {
        return section(
            "Open pairs",
            r#"<p class="dim">No open pairs.</p>"#.to_string(),
        );
    }

    let rows: String = state
        .open_pairs
        .iter()
        .map(|pair| {
            format!(
                "<tr><td>{}</td><td>{}</td><td>{}</td><td>{:.2}</td><td>{:.3}</td><td>{:.4}</td><td>{}</td></tr>",
                html_escape(pair.pair_id.as_str()),
                html_escape(pair.long_ticker.as_str()),
                html_escape(pair.short_ticker.as_str()),
                pair.entry_z_score,
                pair.hedge_ratio,
                pair.signal_strength,
                format_age(now - pair.opened_at),
            )
        })
        .collect();

    let body = format!(
        r#"<table>
<tr><th>Pair</th><th>Long</th><th>Short</th><th>Entry Z</th><th>Hedge</th><th>Signal</th><th>Age</th></tr>
{rows}
</table>
<p class="summary">{} open</p>"#,
        state.open_pairs.len()
    );
    section("Open pairs", body)
}

fn render_predictions_section(state: &DashboardState) -> String {
    if state.predictions.is_empty() {
        return section(
            "Predictions",
            r#"<p class="dim">No predictions yet.</p>"#.to_string(),
        );
    }

    let rows: String = state
        .predictions
        .iter()
        .take(PREDICTIONS_DISPLAY_LIMIT)
        .map(|prediction| {
            format!(
                "<tr><td>{}</td><td>{:.4}</td><td>{:.4}</td><td>{:.4}</td></tr>",
                html_escape(prediction.ticker.as_str()),
                prediction.quantile_10,
                prediction.quantile_50,
                prediction.quantile_90,
            )
        })
        .collect();

    let run = match (&state.prediction_model_run_id, state.prediction_timestamp) {
        (Some(run_id), Some(timestamp)) => format!(
            "Run {} at {}",
            html_escape(run_id),
            timestamp.format("%Y-%m-%d %H:%M UTC")
        ),
        (Some(run_id), None) => format!("Run {}", html_escape(run_id)),
        _ => "Run unknown".to_string(),
    };

    let body = format!(
        r#"<table>
<tr><th>Ticker</th><th>Q10</th><th>Q50</th><th>Q90</th></tr>
{rows}
</table>
<p class="summary">{run} — {} tickers, ranked by median forecast</p>"#,
        state.predictions.len()
    );
    section("Predictions", body)
}

fn render_closed_pairs_section(state: &DashboardState) -> String {
    if state.closed_pairs.is_empty() {
        return section(
            "Closed pairs",
            r#"<p class="dim">No closed pairs.</p>"#.to_string(),
        );
    }

    let rows: String = state
        .closed_pairs
        .iter()
        .take(CLOSED_PAIRS_DISPLAY_LIMIT)
        .map(|pair| {
            let (realized, class) = match pair.realized_profit_and_loss {
                Some(value) if value > Decimal::ZERO => (format_dollars(value), "positive"),
                Some(value) if value < Decimal::ZERO => (format_dollars(value), "negative"),
                Some(value) => (format_dollars(value), "dim"),
                None => ("—".to_string(), "dim"),
            };
            format!(
                r#"<tr><td>{}</td><td>{:.2}</td><td class="{class}">{realized}</td><td>{}</td><td>{}</td><td>{}</td></tr>"#,
                html_escape(pair.pair_id.as_str()),
                pair.entry_z_score,
                html_escape(pair.close_reason.as_str()),
                format_holding(pair.holding_hours()),
                pair.closed_at.format("%Y-%m-%d %H:%M"),
            )
        })
        .collect();

    let summary = &state.closed_summary;
    let reasons: String = summary
        .by_close_reason
        .iter()
        .map(|(reason, count)| format!("{}={count}", reason.as_str()))
        .collect::<Vec<_>>()
        .join(" ");

    let body = format!(
        r#"<table>
<tr><th>Pair</th><th>Entry Z</th><th>Realized</th><th>Reason</th><th>Held</th><th>Closed</th></tr>
{rows}
</table>
<p class="summary">{} closed — win rate {} — profit factor {} — total {} — average hold {}</p>
<p class="summary">Exits: {reasons} — signal share {}</p>"#,
        summary.total_closed,
        format_percent(summary.win_rate),
        format_ratio(summary.profit_factor),
        format_dollars(summary.total_realized),
        summary
            .average_holding_hours
            .map(format_holding)
            .unwrap_or_else(|| "—".to_string()),
        format_percent(summary.signal_exit_share),
    );
    section("Closed pairs", body)
}

fn render_events_section(state: &DashboardState) -> String {
    if state.events.is_empty() {
        return section("Events", r#"<p class="dim">No events yet.</p>"#.to_string());
    }

    let rows: String = state
        .events
        .iter()
        .take(EVENTS_DISPLAY_LIMIT)
        .map(|event| {
            format!(
                r#"<tr><td>{}</td><td class="{}">{}</td><td>{}</td></tr>"#,
                event.created_at.format("%Y-%m-%d %H:%M:%S"),
                event_css_class(&event.event_type),
                html_escape(&event.event_type),
                html_escape(&truncate_payload(&event.payload)),
            )
        })
        .collect();

    let body = format!(
        r#"<table>
<tr><th>Time</th><th>Event</th><th>Payload</th></tr>
{rows}
</table>"#
    );
    section("Events", body)
}

fn section(title: &str, body: String) -> String {
    format!(
        r#"<section>
<div class="panel-header">{title}</div>
{body}
</section>"#
    )
}

/// Colours an event row by its outcome suffix, so an error is visible without reading the name.
fn event_css_class(event_type: &str) -> &'static str {
    if event_type.ends_with("_errored") {
        "event-errored"
    } else if event_type.ends_with("_completed") {
        "event-completed"
    } else {
        "event-requested"
    }
}

/// Renders a payload as a single line, bounded so one large summary cannot push the table wide.
fn truncate_payload(payload: &serde_json::Value) -> String {
    const LIMIT: usize = 120;
    let rendered = payload.to_string();
    if rendered.chars().count() <= LIMIT {
        return rendered;
    }
    // Truncated by characters rather than bytes: slicing a multi-byte character in half panics.
    let mut truncated: String = rendered.chars().take(LIMIT).collect();
    truncated.push('…');
    truncated
}

/// Formats a signed dollar amount to two places with thousands separators.
fn format_dollars(value: Decimal) -> String {
    let rounded = value.round_dp(2);
    let negative = rounded < Decimal::ZERO;
    let text = rounded.abs().to_string();
    let (whole, fraction) = match text.split_once('.') {
        Some((whole, fraction)) => (whole.to_string(), format!("{fraction:0<2}")),
        None => (text, "00".to_string()),
    };

    let mut grouped = String::new();
    for (index, character) in whole.chars().enumerate() {
        if index > 0 && (whole.len() - index) % 3 == 0 {
            grouped.push(',');
        }
        grouped.push(character);
    }

    let sign = if negative { "-" } else { "" };
    format!("{sign}${grouped}.{}", &fraction[..2])
}

/// Formats a percentage return with an explicit sign and a colour class.
fn format_return(value: Option<f64>) -> String {
    match value {
        Some(percent) if percent > 0.0 => {
            format!(r#"<span class="positive">+{percent:.2}%</span>"#)
        }
        Some(percent) if percent < 0.0 => format!(r#"<span class="negative">{percent:.2}%</span>"#),
        Some(percent) => format!("{percent:.2}%"),
        None => r#"<span class="dim">—</span>"#.to_string(),
    }
}

fn format_percent(value: Option<f64>) -> String {
    value
        .map(|percent| format!("{percent:.1}%"))
        .unwrap_or_else(|| "—".to_string())
}

fn format_ratio(value: Option<f64>) -> String {
    value
        .map(|ratio| format!("{ratio:.2}"))
        .unwrap_or_else(|| "—".to_string())
}

/// Formats a holding period in the largest unit that keeps it readable.
fn format_holding(hours: f64) -> String {
    if hours < 1.0 {
        format!("{:.0}m", hours * 60.0)
    } else if hours < 48.0 {
        format!("{hours:.1}h")
    } else {
        format!("{:.1}d", hours / 24.0)
    }
}

/// Formats an elapsed duration, tolerating a negative one.
///
/// A negative age is reachable without any clock skew: `last_updated` is stamped by the poller and
/// the header clock is read later in the same request, but a snapshot's `session_date` and a
/// pair's `opened_at` come from elsewhere. Rendering "-3s" is noise; rendering it as zero is not a
/// lie anyone can act on.
fn format_age(elapsed: Duration) -> String {
    let seconds = elapsed.num_seconds().max(0);
    if seconds < 60 {
        format!("{seconds}s")
    } else if seconds < 3_600 {
        format!("{}m", seconds / 60)
    } else if seconds < 86_400 {
        format!("{}h", seconds / 3_600)
    } else {
        format!("{}d", seconds / 86_400)
    }
}

/// Escapes `&`, `<`, `>`, and `"` for safe embedding.
///
/// Everything that reaches the page from the database goes through this. Tickers and close reasons
/// are validated on the way in and cannot carry markup, but the event payload is arbitrary JSON
/// written by handlers, and an error string in it can hold anything the broker returned.
fn html_escape(input: &str) -> String {
    input
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

/// Inline CSS. Amber CRT terminal aesthetic, carried forward unchanged.
const CSS: &str = r#"
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
    font-family: 'Courier New', Courier, monospace;
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
.updated { color: var(--amber-dim); font-size: 16px; margin-bottom: 4px; }
.freshness { color: var(--amber); margin-bottom: 8px; font-size: 16px; }
section { margin-top: 16px; }
.panel-header {
    text-transform: uppercase;
    border-bottom: 1px solid var(--amber-dim);
    padding-bottom: 4px;
    margin-bottom: 8px;
    color: var(--amber-dim);
}
table { border-collapse: collapse; width: 100%; margin-bottom: 6px; }
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
.event-errored { color: var(--red); }
.event-completed { color: var(--green); }
.event-requested { color: var(--amber); }
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
    use crate::common::types::{PairID, Ticker};
    use crate::dashboard::cache::{
        AccountSnapshot, ClosedPair, EventEntry, OpenPair, PeriodReturns, Prediction,
    };
    use crate::dashboard::database::compute_closed_summary;
    use crate::portfolio::pairs::CloseReason;
    use chrono::{NaiveDate, TimeZone};
    use serde_json::json;
    use std::str::FromStr;

    fn decimal(value: &str) -> Decimal {
        Decimal::from_str(value).expect("a valid test decimal")
    }

    fn now() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 7, 31, 20, 0, 0).unwrap()
    }

    fn populated_state() -> DashboardState {
        let closed_pairs = vec![ClosedPair {
            pair_id: PairID::parse("AAA-BBB").expect("a valid pair"),
            long_ticker: Ticker::new("AAA").expect("a valid ticker"),
            short_ticker: Ticker::new("BBB").expect("a valid ticker"),
            entry_z_score: 2.4,
            realized_profit_and_loss: Some(decimal("250.5")),
            close_reason: CloseReason::Convergence,
            opened_at: now() - Duration::hours(6),
            closed_at: now() - Duration::hours(1),
        }];

        DashboardState {
            account: Some(AccountSnapshot {
                session_date: NaiveDate::from_ymd_opt(2026, 7, 31).expect("a valid date"),
                equity: decimal("1234567.89"),
                cash: decimal("500000"),
                buying_power: decimal("2000000"),
                long_market_value: decimal("400000"),
                short_market_value: decimal("-400000"),
            }),
            equity_history: Vec::new(),
            period_returns: PeriodReturns {
                one_day: Some(1.25),
                one_week: Some(-0.5),
                one_month: None,
                year_to_date: Some(12.0),
                since_inception: Some(23.4),
            },
            open_pairs: vec![OpenPair {
                pair_id: PairID::parse("CCC-DDD").expect("a valid pair"),
                long_ticker: Ticker::new("CCC").expect("a valid ticker"),
                short_ticker: Ticker::new("DDD").expect("a valid ticker"),
                hedge_ratio: 0.873,
                entry_z_score: 2.15,
                signal_strength: 0.0042,
                opened_at: now() - Duration::hours(3),
            }],
            closed_summary: compute_closed_summary(&closed_pairs),
            closed_pairs,
            predictions: vec![Prediction {
                ticker: Ticker::new("EEE").expect("a valid ticker"),
                quantile_10: -0.01,
                quantile_50: 0.005,
                quantile_90: 0.02,
            }],
            prediction_model_run_id: Some("run-2026-07-31".to_string()),
            prediction_timestamp: Some(now() - Duration::hours(12)),
            events: vec![EventEntry {
                event_type: "portfolio_evaluation_completed".to_string(),
                created_at: now() - Duration::minutes(5),
                payload: json!({"pairs_opened": 2}),
            }]
            .into(),
            latest_bars_inserted_at: Some(now() - Duration::hours(2)),
            last_updated: Some(now() - Duration::seconds(10)),
            last_error: None,
        }
    }

    #[test]
    fn test_an_empty_state_renders_a_whole_page() {
        let html = render_html_at(&DashboardState::default(), now());
        assert!(html.starts_with("<!DOCTYPE html>"));
        assert!(html.contains("OSCM"));
        assert!(html.ends_with("</html>"));
        for title in [
            "Account",
            "Open pairs",
            "Predictions",
            "Closed pairs",
            "Events",
        ] {
            assert!(html.contains(title), "missing the {title} section");
        }
        assert!(html.contains("Awaiting first refresh"));
    }

    #[test]
    fn test_a_populated_state_renders_every_section() {
        let html = render_html_at(&populated_state(), now());
        assert!(html.contains("$1,234,567.89"));
        assert!(html.contains("CCC-DDD"));
        assert!(html.contains("AAA-BBB"));
        assert!(html.contains("run-2026-07-31"));
        assert!(html.contains("portfolio_evaluation_completed"));
        assert!(html.contains("convergence=1"));
    }

    /// A failed poll must say so. Serving the previous numbers under a header that reads "Updated
    /// 10s ago" is worse than serving nothing, because it looks current.
    #[test]
    fn test_a_failed_poll_marks_the_page_stale() {
        let mut state = populated_state();
        state.last_error = Some("connection refused".to_string());
        let html = render_html_at(&state, now());
        assert!(html.contains("DATA STALE"));
        assert!(html.contains("connection refused"));
    }

    #[test]
    fn test_bar_freshness_escalates_with_age() {
        let mut state = populated_state();

        state.latest_bars_inserted_at = Some(now() - Duration::hours(2));
        assert!(render_freshness_line(&state, now()).contains("fresh"));

        state.latest_bars_inserted_at = Some(now() - Duration::hours(30));
        assert!(render_freshness_line(&state, now()).contains("warning"));

        state.latest_bars_inserted_at = Some(now() - Duration::hours(72));
        assert!(render_freshness_line(&state, now()).contains("stale"));

        state.latest_bars_inserted_at = None;
        assert!(render_freshness_line(&state, now()).contains("none"));
    }

    /// The event payload is the one field on the page that carries arbitrary text — a broker error
    /// string reaches it verbatim — so this is where an injected tag would land.
    #[test]
    fn test_an_event_payload_containing_markup_is_escaped() {
        let mut state = populated_state();
        state.events = vec![EventEntry {
            event_type: "portfolio_evaluation_errored".to_string(),
            created_at: now(),
            payload: json!({"error": "<script>alert(1)</script>"}),
        }]
        .into();

        let html = render_html_at(&state, now());
        assert!(!html.contains("<script>alert(1)</script>"));
        assert!(html.contains("&lt;script&gt;"));
    }

    #[test]
    fn test_event_rows_are_coloured_by_outcome() {
        assert_eq!(event_css_class("predictions_errored"), "event-errored");
        assert_eq!(event_css_class("predictions_completed"), "event-completed");
        assert_eq!(event_css_class("predictions_requested"), "event-requested");
    }

    /// Truncation counts characters, not bytes. A byte slice through a multi-byte character panics,
    /// and a payload holding a ticker name with an accent is enough to reach it.
    #[test]
    fn test_a_long_multibyte_payload_truncates_without_panicking() {
        let payload = json!({ "note": "é".repeat(400) });
        let truncated = truncate_payload(&payload);
        assert!(truncated.ends_with('…'));
        assert_eq!(truncated.chars().count(), 121);
    }

    #[test]
    fn test_a_short_payload_is_left_alone() {
        let payload = json!({"pairs_opened": 2});
        assert_eq!(truncate_payload(&payload), r#"{"pairs_opened":2}"#);
    }

    #[test]
    fn test_dollars_group_thousands_and_carry_a_sign() {
        assert_eq!(format_dollars(decimal("0")), "$0.00");
        assert_eq!(format_dollars(decimal("999")), "$999.00");
        assert_eq!(format_dollars(decimal("1000")), "$1,000.00");
        assert_eq!(format_dollars(decimal("1234567.891")), "$1,234,567.89");
        assert_eq!(format_dollars(decimal("-4321.5")), "-$4,321.50");
    }

    #[test]
    fn test_returns_carry_a_sign_and_a_colour() {
        assert!(format_return(Some(1.5)).contains(r#"class="positive">+1.50%"#));
        assert!(format_return(Some(-1.5)).contains(r#"class="negative">-1.50%"#));
        assert!(format_return(None).contains("—"));
    }

    #[test]
    fn test_holding_periods_pick_a_readable_unit() {
        assert_eq!(format_holding(0.5), "30m");
        assert_eq!(format_holding(6.25), "6.2h");
        assert_eq!(format_holding(72.0), "3.0d");
    }

    #[test]
    fn test_a_negative_age_renders_as_zero_rather_than_a_negative_duration() {
        assert_eq!(format_age(Duration::seconds(-30)), "0s");
        assert_eq!(format_age(Duration::seconds(45)), "45s");
        assert_eq!(format_age(Duration::minutes(5)), "5m");
        assert_eq!(format_age(Duration::hours(5)), "5h");
        assert_eq!(format_age(Duration::days(3)), "3d");
    }
}
