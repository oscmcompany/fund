//! Tab 2: portfolio NAV sparkline and period-return comparison vs SPY.

use num_traits::ToPrimitive;
use ratatui::layout::{Constraint, Direction, Layout};
use ratatui::style::{Color, Modifier, Style};
use ratatui::widgets::{Block, Borders, Cell, Paragraph, Row, Sparkline, Table};

use crate::dashboard_service::cache::DashboardState;

/// Renders Tab 2: a NAV sparkline above a period-return comparison table.
pub fn render_performance(
    frame: &mut ratatui::Frame,
    area: ratatui::layout::Rect,
    state: &DashboardState,
) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(5), Constraint::Min(0)])
        .split(area);

    render_sparkline(frame, chunks[0], state);
    render_period_returns_table(frame, chunks[1], state);
}

fn render_sparkline(
    frame: &mut ratatui::Frame,
    area: ratatui::layout::Rect,
    state: &DashboardState,
) {
    let block = Block::default().title("NAV History").borders(Borders::ALL);

    if state.performance_history.is_empty() {
        let placeholder = Paragraph::new("No performance history")
            .block(block)
            .style(Style::default().fg(Color::DarkGray));
        frame.render_widget(placeholder, area);
        return;
    }

    let sparkline_data = nav_sparkline_data(&state.performance_history);
    let sparkline = Sparkline::default()
        .block(block)
        .data(&sparkline_data)
        .style(Style::default().fg(Color::Cyan));
    frame.render_widget(sparkline, area);
}

fn render_period_returns_table(
    frame: &mut ratatui::Frame,
    area: ratatui::layout::Rect,
    state: &DashboardState,
) {
    let returns = &state.period_returns;

    let header = Row::new([Cell::from("PERIOD"), Cell::from("FUND"), Cell::from("SPY")])
        .style(Style::default().add_modifier(Modifier::BOLD));

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
    .map(|(label, fund, spy)| {
        let fund_cell = Cell::from(format_return(fund)).style(return_style(fund));
        let spy_cell = Cell::from(format_return(spy)).style(return_style(spy));
        Row::new([Cell::from(label), fund_cell, spy_cell])
    });

    let widths = [
        Constraint::Length(10),
        Constraint::Length(12),
        Constraint::Length(12),
    ];

    let table = Table::new(rows, widths).header(header).block(
        Block::default()
            .title("Period Returns")
            .borders(Borders::ALL),
    );
    frame.render_widget(table, area);
}

/// Normalises NAV history (newest-first) to a `u64` range for the sparkline.
///
/// The sparkline renders oldest-to-newest (left-to-right), so the history
/// slice is reversed before normalisation. Returns an empty vec when history
/// is empty; returns uniform 50s when all NAV values are identical.
pub fn nav_sparkline_data(
    history: &[crate::dashboard_service::cache::PerformanceSnapshot],
) -> Vec<u64> {
    let nav_values: Vec<f64> = history
        .iter()
        .rev()
        .filter_map(|snapshot| snapshot.net_asset_value.to_f64())
        .collect();

    if nav_values.is_empty() {
        return vec![];
    }

    let min_nav = nav_values.iter().cloned().fold(f64::INFINITY, f64::min);
    let max_nav = nav_values.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
    let range = max_nav - min_nav;

    if range == 0.0 {
        return vec![50u64; nav_values.len()];
    }

    nav_values
        .iter()
        .map(|&nav| ((nav - min_nav) / range * 100.0).clamp(0.0, 100.0) as u64)
        .collect()
}

/// Formats an optional return percentage with sign and two decimal places.
fn format_return(value: Option<f64>) -> String {
    match value {
        Some(return_value) => format!("{:+.2}%", return_value),
        None => "—".to_string(),
    }
}

/// Returns a coloured style for a return value: green positive, red negative, gray absent.
fn return_style(value: Option<f64>) -> Style {
    match value {
        Some(return_value) if return_value > 0.0 => Style::default().fg(Color::Green),
        Some(return_value) if return_value < 0.0 => Style::default().fg(Color::Red),
        Some(_) => Style::default(),
        None => Style::default().fg(Color::DarkGray),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use ratatui::backend::TestBackend;
    use ratatui::Terminal;
    use rust_decimal::Decimal;

    use crate::dashboard_service::cache::{DashboardState, PerformanceSnapshot, PeriodReturns};

    fn render_to_string(width: u16, height: u16, state: &DashboardState) -> String {
        let backend = TestBackend::new(width, height);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal
            .draw(|frame| render_performance(frame, frame.area(), state))
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

    fn make_snapshot(nav: i64, spy: Option<f64>) -> PerformanceSnapshot {
        PerformanceSnapshot {
            snapshot_timestamp: Utc::now(),
            net_asset_value: Decimal::new(nav, 0),
            gross_return: None,
            net_return: None,
            total_slippage_cost: Decimal::ZERO,
            spy_close: spy,
        }
    }

    #[test]
    fn test_render_performance_empty_shows_placeholder() {
        let state = DashboardState::default();
        let output = render_to_string(120, 40, &state);
        assert!(output.contains("No performance history"));
    }

    #[test]
    fn test_render_performance_shows_period_labels() {
        let state = DashboardState::default();
        let output = render_to_string(120, 40, &state);
        assert!(output.contains("1D"));
        assert!(output.contains("1W"));
        assert!(output.contains("1M"));
        assert!(output.contains("YTD"));
        assert!(output.contains("Inception"));
    }

    #[test]
    fn test_render_performance_shows_fund_and_spy_columns() {
        let state = DashboardState::default();
        let output = render_to_string(120, 40, &state);
        assert!(output.contains("FUND"));
        assert!(output.contains("SPY"));
    }

    #[test]
    fn test_render_performance_shows_return_values() {
        let mut state = DashboardState::default();
        state.performance_history = vec![make_snapshot(110_000, Some(455.0))];
        state.period_returns = PeriodReturns {
            fund_one_day: Some(10.0),
            spy_one_day: Some(1.1),
            ..Default::default()
        };
        let output = render_to_string(120, 40, &state);
        assert!(output.contains("+10.00%"));
        assert!(output.contains("+1.10%"));
    }

    #[test]
    fn test_render_performance_shows_dash_for_none_returns() {
        let mut state = DashboardState::default();
        state.performance_history = vec![make_snapshot(100_000, None)];
        let output = render_to_string(120, 40, &state);
        assert!(output.contains("—"));
    }

    #[test]
    fn test_nav_sparkline_data_empty() {
        let data = nav_sparkline_data(&[]);
        assert!(data.is_empty());
    }

    #[test]
    fn test_nav_sparkline_data_uniform_nav() {
        let history = vec![
            make_snapshot(100_000, None),
            make_snapshot(100_000, None),
            make_snapshot(100_000, None),
        ];
        let data = nav_sparkline_data(&history);
        assert_eq!(data, vec![50, 50, 50]);
    }

    #[test]
    fn test_nav_sparkline_data_range() {
        // Three snapshots newest-first; reversed for sparkline: 80k, 100k, 120k.
        // Min=80k, max=120k, range=40k.
        // 80k → 0, 100k → 50, 120k → 100.
        let history = vec![
            make_snapshot(120_000, None),
            make_snapshot(100_000, None),
            make_snapshot(80_000, None),
        ];
        let data = nav_sparkline_data(&history);
        assert_eq!(data, vec![0, 50, 100]);
    }

    #[test]
    fn test_format_return_positive() {
        assert_eq!(format_return(Some(5.25)), "+5.25%");
    }

    #[test]
    fn test_format_return_negative() {
        assert_eq!(format_return(Some(-3.10)), "-3.10%");
    }

    #[test]
    fn test_format_return_none() {
        assert_eq!(format_return(None), "—");
    }

    #[test]
    fn test_return_style_positive_is_green() {
        let style = return_style(Some(1.0));
        assert_eq!(style.fg, Some(Color::Green));
    }

    #[test]
    fn test_return_style_negative_is_red() {
        let style = return_style(Some(-1.0));
        assert_eq!(style.fg, Some(Color::Red));
    }

    #[test]
    fn test_return_style_none_is_dark_gray() {
        let style = return_style(None);
        assert_eq!(style.fg, Some(Color::DarkGray));
    }
}
