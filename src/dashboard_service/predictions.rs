//! Tab 4: latest model quantile forecasts per ticker.

use chrono::Utc;
use ratatui::layout::{Constraint, Direction, Layout};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Cell, Paragraph, Row, Table};

use crate::dashboard_service::cache::{DashboardState, ModelRunInformation};

/// Age threshold in hours above which model run data is considered stale.
const MODEL_RUN_STALE_HOURS: i64 = 36;

/// Age threshold in hours above which model run data is approaching stale (warning).
const MODEL_RUN_WARNING_HOURS: i64 = 25;

/// Age threshold in hours above which equity bar data is considered stale on a weekday.
const BARS_STALE_WEEKDAY_HOURS: i64 = 25;

/// Renders Tab 4: a table of the latest quantile predictions with model run age.
///
/// When model run metadata is available, renders a one-line freshness summary
/// (run age, CRPS, directional accuracy, and bar insertion age) above the
/// prediction table inside the outer block.
pub fn render_predictions(
    frame: &mut ratatui::Frame,
    area: ratatui::layout::Rect,
    state: &DashboardState,
) {
    let outer_block = Block::default()
        .title("Model Predictions")
        .borders(Borders::ALL);

    if state.predictions.is_empty() {
        let placeholder = Paragraph::new("No predictions available")
            .block(outer_block)
            .style(Style::default().fg(Color::DarkGray));
        frame.render_widget(placeholder, area);
        return;
    }

    let inner_area = outer_block.inner(area);
    frame.render_widget(outer_block, area);

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(1), Constraint::Min(0)])
        .split(inner_area);

    render_freshness_line(frame, chunks[0], state);
    render_predictions_table(frame, chunks[1], state);
}

/// Renders the one-line freshness summary: model run age, CRPS, DA, and bar age.
fn render_freshness_line(
    frame: &mut ratatui::Frame,
    area: ratatui::layout::Rect,
    state: &DashboardState,
) {
    let mut spans: Vec<Span> = Vec::new();

    match &state.model_run_information {
        None => {
            spans.push(Span::styled(
                "Model run: no completed runs",
                Style::default().fg(Color::DarkGray),
            ));
        }
        Some(info) => {
            let age_style = model_run_age_style(info);
            spans.push(Span::raw("Run: "));
            spans.push(Span::styled(format_age(info.completed_at), age_style));

            if let Some(crps) = info.continuous_ranked_probability_score {
                spans.push(Span::styled(
                    "  CRPS: ",
                    Style::default().fg(Color::DarkGray),
                ));
                spans.push(Span::raw(format!("{crps:.3}")));
            }
            if let Some(directional_accuracy) = info.directional_accuracy {
                spans.push(Span::styled("  DA: ", Style::default().fg(Color::DarkGray)));
                spans.push(Span::raw(format!("{:.1}%", directional_accuracy * 100.0)));
            }
        }
    }

    if let Some(inserted_at) = state.latest_bars_inserted_at {
        let style = bars_age_style(inserted_at);
        spans.push(Span::styled(
            "  Bars: ",
            Style::default().fg(Color::DarkGray),
        ));
        spans.push(Span::styled(format_age(inserted_at), style));
    }

    frame.render_widget(Paragraph::new(Line::from(spans)), area);
}

/// Renders the predictions table without an outer block (already rendered by caller).
fn render_predictions_table(
    frame: &mut ratatui::Frame,
    area: ratatui::layout::Rect,
    state: &DashboardState,
) {
    let header = Row::new([
        Cell::from("TICKER"),
        Cell::from("Q10"),
        Cell::from("Q50"),
        Cell::from("Q90"),
        Cell::from("MODEL RUN"),
        Cell::from("AGE"),
    ])
    .style(Style::default().add_modifier(Modifier::BOLD));

    let rows: Vec<Row> = state
        .predictions
        .iter()
        .map(|prediction| {
            let q50_style = if prediction.quantile_50 > 0.0 {
                Style::default().fg(Color::Green)
            } else if prediction.quantile_50 < 0.0 {
                Style::default().fg(Color::Red)
            } else {
                Style::default()
            };

            Row::new([
                Cell::from(prediction.ticker.as_str().to_string()),
                Cell::from(format!("{:+.4}", prediction.quantile_10)),
                Cell::from(format!("{:+.4}", prediction.quantile_50)).style(q50_style),
                Cell::from(format!("{:+.4}", prediction.quantile_90)),
                Cell::from(prediction.model_run_id.clone()),
                Cell::from(format_age(prediction.timestamp)),
            ])
        })
        .collect();

    let widths = [
        Constraint::Length(8),
        Constraint::Length(10),
        Constraint::Length(10),
        Constraint::Length(10),
        Constraint::Length(20),
        Constraint::Min(0),
    ];

    let table = Table::new(rows, widths).header(header);
    frame.render_widget(table, area);
}

/// Returns the style for a model run age based on staleness thresholds.
///
/// Red when older than [`MODEL_RUN_STALE_HOURS`], yellow when older than
/// [`MODEL_RUN_WARNING_HOURS`], green otherwise.
fn model_run_age_style(info: &ModelRunInformation) -> Style {
    let hours = (Utc::now() - info.completed_at).num_hours();
    if hours > MODEL_RUN_STALE_HOURS {
        Style::default().fg(Color::Red)
    } else if hours > MODEL_RUN_WARNING_HOURS {
        Style::default().fg(Color::Yellow)
    } else {
        Style::default().fg(Color::Green)
    }
}

/// Returns the style for equity bar insertion age.
///
/// Red when the most recent bar is older than [`BARS_STALE_WEEKDAY_HOURS`]
/// on a weekday (nightly ingest is expected). On weekends staleness is normal
/// and the indicator stays green.
fn bars_age_style(inserted_at: chrono::DateTime<Utc>) -> Style {
    use chrono::Datelike;
    use chrono::Weekday;
    let now = Utc::now();
    let hours = (now - inserted_at).num_hours();
    let is_weekday = matches!(
        now.weekday(),
        Weekday::Mon | Weekday::Tue | Weekday::Wed | Weekday::Thu | Weekday::Fri
    );
    if is_weekday && hours > BARS_STALE_WEEKDAY_HOURS {
        Style::default().fg(Color::Red)
    } else {
        Style::default().fg(Color::Green)
    }
}

/// Formats the age of a prediction timestamp as a human-readable string.
///
/// Returns `"0m"` when the timestamp is in the future (e.g. due to clock skew
/// between the database host and the TUI host) to avoid rendering negative ages.
pub fn format_age(timestamp: chrono::DateTime<Utc>) -> String {
    let age = Utc::now() - timestamp;
    if age < chrono::Duration::zero() {
        return "0m".to_string();
    }
    if age.num_days() > 0 {
        format!("{}d {}h", age.num_days(), age.num_hours() % 24)
    } else if age.num_hours() > 0 {
        format!("{}h {}m", age.num_hours(), age.num_minutes() % 60)
    } else {
        format!("{}m", age.num_minutes())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Duration, NaiveDate, Utc};
    use ratatui::backend::TestBackend;
    use ratatui::Terminal;

    use crate::dashboard_service::cache::{DashboardState, ModelRunInformation, PredictionRow};
    use crate::domain::market::Ticker;

    fn render_to_string(width: u16, height: u16, state: &DashboardState) -> String {
        let backend = TestBackend::new(width, height);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal
            .draw(|frame| render_predictions(frame, frame.area(), state))
            .unwrap();
        terminal
            .backend()
            .buffer()
            .clone()
            .content()
            .iter()
            .map(|cell| cell.symbol().to_string())
            .collect()
    }

    fn make_prediction(ticker: &str, q10: f64, q50: f64, q90: f64) -> PredictionRow {
        PredictionRow {
            ticker: Ticker::new(ticker).unwrap(),
            quantile_10: q10,
            quantile_50: q50,
            quantile_90: q90,
            model_run_id: "run-abc123".to_string(),
            timestamp: Utc::now() - Duration::hours(2),
        }
    }

    #[test]
    fn test_render_predictions_empty_shows_placeholder() {
        let state = DashboardState::default();
        let output = render_to_string(120, 40, &state);
        assert!(output.contains("No predictions available"));
    }

    #[test]
    fn test_render_predictions_shows_block_title() {
        let state = DashboardState::default();
        let output = render_to_string(120, 40, &state);
        assert!(output.contains("Model Predictions"));
    }

    #[test]
    fn test_render_predictions_shows_ticker() {
        let mut state = DashboardState::default();
        state.predictions = vec![make_prediction("AAPL", -0.001, 0.002, 0.005)];
        let output = render_to_string(120, 40, &state);
        assert!(output.contains("AAPL"));
    }

    #[test]
    fn test_render_predictions_shows_model_run_id() {
        let mut state = DashboardState::default();
        state.predictions = vec![make_prediction("AAPL", -0.001, 0.002, 0.005)];
        let output = render_to_string(120, 40, &state);
        assert!(output.contains("run-abc123"));
    }

    #[test]
    fn test_render_predictions_shows_column_headers() {
        let mut state = DashboardState::default();
        state.predictions = vec![make_prediction("AAPL", -0.001, 0.002, 0.005)];
        let output = render_to_string(120, 40, &state);
        assert!(output.contains("Q10"));
        assert!(output.contains("Q50"));
        assert!(output.contains("Q90"));
    }

    #[test]
    fn test_render_predictions_shows_age() {
        let mut state = DashboardState::default();
        state.predictions = vec![make_prediction("AAPL", -0.001, 0.002, 0.005)];
        let output = render_to_string(120, 40, &state);
        // Prediction was made 2 hours ago
        assert!(output.contains("2h"));
    }

    #[test]
    fn test_format_age_minutes() {
        let timestamp = Utc::now() - Duration::minutes(45);
        assert_eq!(format_age(timestamp), "45m");
    }

    #[test]
    fn test_format_age_hours_and_minutes() {
        let timestamp = Utc::now() - Duration::hours(3) - Duration::minutes(20);
        let result = format_age(timestamp);
        assert!(
            result.starts_with("3h"),
            "expected '3h ...', got '{result}'"
        );
    }

    #[test]
    fn test_format_age_days_and_hours() {
        let timestamp = Utc::now() - Duration::days(2) - Duration::hours(5);
        let result = format_age(timestamp);
        assert!(
            result.starts_with("2d"),
            "expected '2d ...', got '{result}'"
        );
    }

    #[test]
    fn test_format_age_future_timestamp_returns_zero() {
        // Timestamp in the future (e.g. DB clock ahead of TUI host).
        let timestamp = Utc::now() + Duration::minutes(5);
        assert_eq!(format_age(timestamp), "0m");
    }

    fn make_model_run_information(hours_ago: i64) -> ModelRunInformation {
        ModelRunInformation {
            completed_at: Utc::now() - Duration::hours(hours_ago),
            start_date: NaiveDate::from_ymd_opt(2025, 1, 1).unwrap(),
            end_date: NaiveDate::from_ymd_opt(2025, 12, 31).unwrap(),
            continuous_ranked_probability_score: Some(0.123),
            directional_accuracy: Some(0.725),
        }
    }

    #[test]
    fn test_render_predictions_shows_freshness_line_with_model_info() {
        let mut state = DashboardState::default();
        state.predictions = vec![make_prediction("AAPL", -0.001, 0.002, 0.005)];
        state.model_run_information = Some(make_model_run_information(2));
        let output = render_to_string(120, 40, &state);
        assert!(output.contains("Run:"));
        assert!(output.contains("CRPS:"));
        assert!(output.contains("DA:"));
    }

    #[test]
    fn test_render_predictions_shows_no_completed_runs_when_none() {
        let mut state = DashboardState::default();
        state.predictions = vec![make_prediction("AAPL", -0.001, 0.002, 0.005)];
        // model_run_information is None by default
        let output = render_to_string(120, 40, &state);
        assert!(output.contains("no completed runs"));
    }

    #[test]
    fn test_render_predictions_shows_bars_freshness_when_present() {
        let mut state = DashboardState::default();
        state.predictions = vec![make_prediction("AAPL", -0.001, 0.002, 0.005)];
        state.latest_bars_inserted_at = Some(Utc::now() - Duration::hours(3));
        let output = render_to_string(120, 40, &state);
        assert!(output.contains("Bars:"));
    }

    #[test]
    fn test_model_run_age_style_green_when_fresh() {
        let info = make_model_run_information(1);
        let style = model_run_age_style(&info);
        assert_eq!(style.fg, Some(Color::Green));
    }

    #[test]
    fn test_model_run_age_style_yellow_when_warning() {
        let info = make_model_run_information(30);
        let style = model_run_age_style(&info);
        assert_eq!(style.fg, Some(Color::Yellow));
    }

    #[test]
    fn test_model_run_age_style_red_when_stale() {
        let info = make_model_run_information(40);
        let style = model_run_age_style(&info);
        assert_eq!(style.fg, Some(Color::Red));
    }
}
