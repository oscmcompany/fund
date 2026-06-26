//! Tab 4: latest model quantile forecasts per ticker.

use chrono::Utc;
use ratatui::layout::Constraint;
use ratatui::style::{Color, Modifier, Style};
use ratatui::widgets::{Block, Borders, Cell, Paragraph, Row, Table};

use crate::dashboard_service::cache::DashboardState;

/// Renders Tab 4: a table of the latest quantile predictions with model run age.
pub fn render_predictions(
    frame: &mut ratatui::Frame,
    area: ratatui::layout::Rect,
    state: &DashboardState,
) {
    let block = Block::default()
        .title("Model Predictions")
        .borders(Borders::ALL);

    if state.predictions.is_empty() {
        let placeholder = Paragraph::new("No predictions available")
            .block(block)
            .style(Style::default().fg(Color::DarkGray));
        frame.render_widget(placeholder, area);
        return;
    }

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

    let table = Table::new(rows, widths).header(header).block(block);
    frame.render_widget(table, area);
}

/// Formats the age of a prediction timestamp as a human-readable string.
pub fn format_age(timestamp: chrono::DateTime<Utc>) -> String {
    let age = Utc::now() - timestamp;
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
    use chrono::{Duration, Utc};
    use ratatui::backend::TestBackend;
    use ratatui::Terminal;

    use crate::dashboard_service::cache::{DashboardState, PredictionRow};
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
}
