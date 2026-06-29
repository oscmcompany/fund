//! Tab 1: open long/short pair positions and exposure summary.

use num_traits::ToPrimitive;
use ratatui::layout::{Constraint, Direction, Layout};
use ratatui::style::{Color, Modifier, Style};
use ratatui::widgets::{Block, Borders, Cell, Paragraph, Row, Table};

use crate::dashboard_service::cache::DashboardState;

/// Renders Tab 1: an open-pairs table above an exposure summary footer row.
pub fn render_positions(
    frame: &mut ratatui::Frame,
    area: ratatui::layout::Rect,
    state: &DashboardState,
) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(0), Constraint::Length(1)])
        .split(area);

    render_positions_table(frame, chunks[0], state);
    render_positions_footer(frame, chunks[1], state);
}

/// Renders the open-pairs table showing tickers, z-score, signal, and dollar amounts.
fn render_positions_table(
    frame: &mut ratatui::Frame,
    area: ratatui::layout::Rect,
    state: &DashboardState,
) {
    let block = Block::default()
        .title("Open Positions")
        .borders(Borders::ALL);

    if state.open_positions.is_empty() {
        let placeholder = Paragraph::new("No open positions")
            .block(block)
            .style(Style::default().fg(Color::DarkGray));
        frame.render_widget(placeholder, area);
        return;
    }

    let header = Row::new([
        Cell::from("PAIR"),
        Cell::from("LONG"),
        Cell::from("SHORT"),
        Cell::from("Z-SCORE"),
        Cell::from("SIGNAL"),
        Cell::from("LONG $"),
        Cell::from("SHORT $"),
        Cell::from("OPENED"),
    ])
    .style(Style::default().add_modifier(Modifier::BOLD));

    let rows: Vec<Row> = state
        .open_positions
        .iter()
        .map(|position| {
            Row::new([
                Cell::from(position.pair_id.as_str().to_string()),
                Cell::from(position.long_ticker.as_str().to_string()),
                Cell::from(position.short_ticker.as_str().to_string()),
                Cell::from(format!("{:.2}", position.z_score.to_f64().unwrap_or(0.0))),
                Cell::from(format!(
                    "{:.2}",
                    position.signal_strength.to_f64().unwrap_or(0.0)
                )),
                Cell::from(format_dollars(position.long_dollar_amount)),
                Cell::from(format_dollars(position.short_dollar_amount)),
                Cell::from(position.opened_at.format("%Y-%m-%d %H:%M").to_string()),
            ])
        })
        .collect();

    let widths = [
        Constraint::Length(12),
        Constraint::Length(6),
        Constraint::Length(6),
        Constraint::Length(9),
        Constraint::Length(8),
        Constraint::Length(12),
        Constraint::Length(12),
        Constraint::Min(0),
    ];

    let table = Table::new(rows, widths).header(header).block(block);
    frame.render_widget(table, area);
}

/// Renders the one-line exposure summary footer below the positions table.
fn render_positions_footer(
    frame: &mut ratatui::Frame,
    area: ratatui::layout::Rect,
    state: &DashboardState,
) {
    let pair_count = state.open_positions.len();
    let text = format!(
        "  {} {}  |  Gross: {}  |  Net: {}",
        pair_count,
        if pair_count == 1 {
            "pair open"
        } else {
            "pairs open"
        },
        format_dollars(state.gross_exposure),
        format_dollars(state.net_exposure),
    );
    let footer = Paragraph::new(text).style(Style::default().fg(Color::DarkGray));
    frame.render_widget(footer, area);
}

/// Formats a `Decimal` value as a dollar string with two decimal places (e.g. `"$1000.50"`).
///
/// Formats directly in decimal space to preserve cent-level precision without
/// converting through floating point.
fn format_dollars(decimal: rust_decimal::Decimal) -> String {
    format!("${:.2}", decimal)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use ratatui::backend::TestBackend;
    use ratatui::Terminal;
    use rust_decimal::Decimal;

    use crate::dashboard_service::cache::{DashboardState, OpenPosition};
    use crate::domain::market::{PairID, Ticker};

    fn render_to_string(width: u16, height: u16, state: &DashboardState) -> String {
        let backend = TestBackend::new(width, height);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal
            .draw(|frame| render_positions(frame, frame.area(), state))
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

    fn make_position(long: &str, short: &str, long_amount: i64, short_amount: i64) -> OpenPosition {
        OpenPosition {
            pair_id: PairID::parse(&format!("{long}-{short}")).unwrap(),
            long_ticker: Ticker::new(long).unwrap(),
            short_ticker: Ticker::new(short).unwrap(),
            z_score: Decimal::new(15, 1),
            hedge_ratio: Decimal::ONE,
            signal_strength: Decimal::new(8, 1),
            long_dollar_amount: Decimal::new(long_amount, 0),
            short_dollar_amount: Decimal::new(short_amount, 0),
            opened_at: Utc::now(),
        }
    }

    #[test]
    fn test_render_positions_empty_shows_placeholder() {
        let state = DashboardState::default();
        let output = render_to_string(120, 40, &state);
        assert!(output.contains("No open positions"));
    }

    #[test]
    fn test_render_positions_shows_table_header() {
        let state = DashboardState::default();
        let output = render_to_string(120, 40, &state);
        // Block title always shows
        assert!(output.contains("Open Positions"));
    }

    #[test]
    fn test_render_positions_shows_tickers() {
        let mut state = DashboardState::default();
        state.open_positions = vec![make_position("AAPL", "MSFT", 10000, 9500)];
        state.gross_exposure = Decimal::new(19500, 0);
        state.net_exposure = Decimal::new(500, 0);
        let output = render_to_string(120, 40, &state);
        assert!(output.contains("AAPL"));
        assert!(output.contains("MSFT"));
    }

    #[test]
    fn test_render_positions_footer_shows_exposure_labels() {
        let mut state = DashboardState::default();
        state.open_positions = vec![make_position("AAPL", "MSFT", 10000, 9500)];
        state.gross_exposure = Decimal::new(19500, 0);
        state.net_exposure = Decimal::new(500, 0);
        let output = render_to_string(120, 40, &state);
        assert!(output.contains("Gross"));
        assert!(output.contains("Net"));
    }

    #[test]
    fn test_render_positions_footer_singular_pair() {
        let mut state = DashboardState::default();
        state.open_positions = vec![make_position("AAPL", "MSFT", 10000, 9500)];
        let output = render_to_string(120, 40, &state);
        assert!(output.contains("1 pair open"));
    }

    #[test]
    fn test_render_positions_footer_plural_pairs() {
        let mut state = DashboardState::default();
        state.open_positions = vec![
            make_position("AAPL", "MSFT", 10000, 9500),
            make_position("TSLA", "NVDA", 8000, 7500),
        ];
        state.gross_exposure = Decimal::new(35000, 0);
        state.net_exposure = Decimal::new(1000, 0);
        let output = render_to_string(120, 40, &state);
        assert!(output.contains("2 pairs open"));
    }

    #[test]
    fn test_format_dollars_positive() {
        assert_eq!(format_dollars(Decimal::new(1000, 0)), "$1000.00");
    }

    #[test]
    fn test_format_dollars_zero() {
        assert_eq!(format_dollars(Decimal::ZERO), "$0.00");
    }

    #[test]
    fn test_format_dollars_fractional() {
        assert_eq!(format_dollars(Decimal::new(10050, 2)), "$100.50");
    }
}
