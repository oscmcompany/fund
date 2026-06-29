//! Tab 3: closed pair trades table with aggregate statistics footer.

use ratatui::layout::{Constraint, Direction, Layout};
use ratatui::style::{Color, Modifier, Style};
use ratatui::widgets::{Block, Borders, Cell, Paragraph, Row, Table};

use crate::dashboard_service::cache::{ClosedTradesSummary, DashboardState};

/// Renders Tab 3: a closed-trades table above an aggregate statistics footer.
pub fn render_trades(
    frame: &mut ratatui::Frame,
    area: ratatui::layout::Rect,
    state: &DashboardState,
) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(0), Constraint::Length(2)])
        .split(area);

    render_trades_table(frame, chunks[0], state);
    render_trades_footer(frame, chunks[1], &state.closed_trades_summary);
}

/// Renders the closed-trades table showing pair P&L, return, holding time, and close reason.
fn render_trades_table(
    frame: &mut ratatui::Frame,
    area: ratatui::layout::Rect,
    state: &DashboardState,
) {
    let block = Block::default()
        .title("Closed Trades")
        .borders(Borders::ALL);

    if state.closed_trades.is_empty() {
        let placeholder = Paragraph::new("No closed trades")
            .block(block)
            .style(Style::default().fg(Color::DarkGray));
        frame.render_widget(placeholder, area);
        return;
    }

    let header = Row::new([
        Cell::from("PAIR"),
        Cell::from("LONG"),
        Cell::from("SHORT"),
        Cell::from("P&L"),
        Cell::from("RETURN"),
        Cell::from("HOLDING"),
        Cell::from("REASON"),
        Cell::from("CLOSED"),
    ])
    .style(Style::default().add_modifier(Modifier::BOLD));

    let rows: Vec<Row> = state
        .closed_trades
        .iter()
        .map(|trade| {
            let profit_and_loss_str = trade
                .realized_profit_and_loss
                .map(|value| format!("${:.2}", value))
                .unwrap_or_else(|| "—".to_string());
            let profit_and_loss_style = match trade.realized_profit_and_loss {
                Some(value) if value.is_sign_positive() && !value.is_zero() => {
                    Style::default().fg(Color::Green)
                }
                Some(value) if value.is_sign_negative() => Style::default().fg(Color::Red),
                _ => Style::default(),
            };

            let return_str = trade
                .return_percent
                .map(|value| format!("{:+.2}%", value))
                .unwrap_or_else(|| "—".to_string());
            let return_style = match trade.return_percent {
                Some(value) if value.is_sign_positive() && !value.is_zero() => {
                    Style::default().fg(Color::Green)
                }
                Some(value) if value.is_sign_negative() => Style::default().fg(Color::Red),
                _ => Style::default(),
            };

            Row::new([
                Cell::from(trade.pair_id.as_str().to_string()),
                Cell::from(trade.long_ticker.as_str().to_string()),
                Cell::from(trade.short_ticker.as_str().to_string()),
                Cell::from(profit_and_loss_str).style(profit_and_loss_style),
                Cell::from(return_str).style(return_style),
                Cell::from(
                    trade
                        .holding_seconds
                        .map(format_holding_duration)
                        .unwrap_or_else(|| "—".to_string()),
                ),
                Cell::from(
                    trade
                        .close_reason
                        .as_ref()
                        .map(|reason| reason.as_str())
                        .unwrap_or("—")
                        .to_string(),
                ),
                Cell::from(
                    trade
                        .closed_at
                        .map(|timestamp| timestamp.format("%Y-%m-%d").to_string())
                        .unwrap_or_else(|| "—".to_string()),
                ),
            ])
        })
        .collect();

    let widths = [
        Constraint::Length(12),
        Constraint::Length(6),
        Constraint::Length(6),
        Constraint::Length(11),
        Constraint::Length(9),
        Constraint::Length(9),
        Constraint::Length(14),
        Constraint::Min(0),
    ];

    let table = Table::new(rows, widths).header(header).block(block);
    frame.render_widget(table, area);
}

/// Renders the one-line aggregate statistics footer below the trades table.
fn render_trades_footer(
    frame: &mut ratatui::Frame,
    area: ratatui::layout::Rect,
    summary: &ClosedTradesSummary,
) {
    let win_rate = summary
        .win_rate
        .map(|rate| format!("{:.1}%", rate * 100.0))
        .unwrap_or_else(|| "—".to_string());
    let profit_factor = summary
        .profit_factor
        .map(|factor| format!("{:.2}", factor))
        .unwrap_or_else(|| "—".to_string());
    let average_return = summary
        .average_return_percent
        .map(|return_percent| format!("{:+.2}%", return_percent))
        .unwrap_or_else(|| "—".to_string());
    let average_holding = summary
        .average_holding_seconds
        .map(|seconds| format_holding_duration(seconds as i64))
        .unwrap_or_else(|| "—".to_string());
    let total_profit_and_loss = summary
        .total_realized_profit_and_loss
        .map(|value| format!("${:.2}", value))
        .unwrap_or_else(|| "—".to_string());

    let text = format!(
        "  {} closed  |  Win: {}  |  PF: {}  |  Avg return: {}  |  Avg hold: {}  |  Total P&L: {}",
        summary.total_closed,
        win_rate,
        profit_factor,
        average_return,
        average_holding,
        total_profit_and_loss,
    );
    let footer = Paragraph::new(text).style(Style::default().fg(Color::DarkGray));
    frame.render_widget(footer, area);
}

/// Formats a holding duration in seconds to a human-readable string.
pub fn format_holding_duration(seconds: i64) -> String {
    if seconds < 3600 {
        format!("{}m", seconds / 60)
    } else if seconds < 86400 {
        format!("{}h {}m", seconds / 3600, (seconds % 3600) / 60)
    } else {
        format!("{}d {}h", seconds / 86400, (seconds % 86400) / 3600)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use ratatui::backend::TestBackend;
    use ratatui::Terminal;
    use rust_decimal::Decimal;

    use crate::dashboard_service::cache::{ClosedTrade, DashboardState};
    use crate::domain::market::{PairID, Ticker};
    use crate::domain::trading::CloseReason;

    fn render_to_string(width: u16, height: u16, state: &DashboardState) -> String {
        let backend = TestBackend::new(width, height);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal
            .draw(|frame| render_trades(frame, frame.area(), state))
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

    fn make_trade(
        long: &str,
        short: &str,
        profit_and_loss: Option<i64>,
        return_percent: Option<&str>,
        holding_seconds: Option<i64>,
    ) -> ClosedTrade {
        ClosedTrade {
            pair_id: PairID::parse(&format!("{long}-{short}")).unwrap(),
            long_ticker: Ticker::new(long).unwrap(),
            short_ticker: Ticker::new(short).unwrap(),
            realized_profit_and_loss: profit_and_loss.map(|v| Decimal::new(v, 0)),
            return_percent: return_percent.map(|s| s.parse::<Decimal>().expect("valid decimal")),
            holding_seconds,
            close_reason: Some(CloseReason::ProfitTaken),
            closed_at: Some(Utc::now()),
        }
    }

    #[test]
    fn test_render_trades_empty_shows_placeholder() {
        let state = DashboardState::default();
        let output = render_to_string(120, 40, &state);
        assert!(output.contains("No closed trades"));
    }

    #[test]
    fn test_render_trades_shows_table_header() {
        let state = DashboardState::default();
        let output = render_to_string(120, 40, &state);
        assert!(output.contains("Closed Trades"));
    }

    #[test]
    fn test_render_trades_shows_pair_tickers() {
        let mut state = DashboardState::default();
        state.closed_trades = vec![make_trade("AAPL", "MSFT", Some(500), Some("2"), Some(3600))];
        let output = render_to_string(120, 40, &state);
        assert!(output.contains("AAPL"));
        assert!(output.contains("MSFT"));
    }

    #[test]
    fn test_render_trades_footer_shows_summary_labels() {
        let state = DashboardState::default();
        let output = render_to_string(120, 40, &state);
        assert!(output.contains("closed"));
        assert!(output.contains("Win"));
    }

    #[test]
    fn test_render_trades_shows_dash_for_none_fields() {
        let mut state = DashboardState::default();
        state.closed_trades = vec![make_trade("AAPL", "MSFT", None, None, None)];
        let output = render_to_string(120, 40, &state);
        assert!(output.contains("—"));
    }

    #[test]
    fn test_format_holding_duration_minutes() {
        assert_eq!(format_holding_duration(300), "5m");
    }

    #[test]
    fn test_format_holding_duration_hours_and_minutes() {
        assert_eq!(format_holding_duration(3900), "1h 5m");
    }

    #[test]
    fn test_format_holding_duration_days_and_hours() {
        assert_eq!(format_holding_duration(90000), "1d 1h");
    }

    #[test]
    fn test_format_holding_duration_zero() {
        assert_eq!(format_holding_duration(0), "0m");
    }

    #[test]
    fn test_format_holding_duration_exactly_one_hour() {
        assert_eq!(format_holding_duration(3600), "1h 0m");
    }

    #[test]
    fn test_format_holding_duration_exactly_one_day() {
        assert_eq!(format_holding_duration(86400), "1d 0h");
    }
}
