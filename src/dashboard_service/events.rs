//! Tab 5: real-time event feed with category filter.
//!
//! [`EventsViewState`] holds the active filter and is owned by [`super::application::Application`].
//! [`render_events`] is the top-level render entry point called from the application event loop.

use ratatui::layout::{Constraint, Direction, Layout};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, List, ListItem, Paragraph};

use crate::dashboard_service::cache::DashboardState;

/// Category filter for the event feed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventsFilter {
    All,
    Trading,
    Data,
    Predictions,
}

impl EventsFilter {
    /// Returns all filter variants in display order.
    pub fn all() -> &'static [EventsFilter] {
        &[
            EventsFilter::All,
            EventsFilter::Trading,
            EventsFilter::Data,
            EventsFilter::Predictions,
        ]
    }

    /// Returns the display label for the filter.
    pub fn label(self) -> &'static str {
        match self {
            EventsFilter::All => "All",
            EventsFilter::Trading => "Trading",
            EventsFilter::Data => "Data",
            EventsFilter::Predictions => "Predictions",
        }
    }

    /// Returns `true` if `event_type` matches this filter category.
    pub fn matches(self, event_type: &str) -> bool {
        match self {
            EventsFilter::All => true,
            EventsFilter::Trading => {
                event_type.contains("portfolio")
                    || event_type.contains("rebalance")
                    || event_type.contains("liquidation")
                    || event_type.contains("market_session")
            }
            EventsFilter::Data => {
                event_type.contains("equity_bars")
                    || event_type.contains("export")
                    || event_type.contains("backup")
                    || event_type.contains("database")
            }
            EventsFilter::Predictions => event_type.contains("prediction"),
        }
    }

    /// Advances to the next filter in cycle order.
    pub fn next(self) -> EventsFilter {
        match self {
            EventsFilter::All => EventsFilter::Trading,
            EventsFilter::Trading => EventsFilter::Data,
            EventsFilter::Data => EventsFilter::Predictions,
            EventsFilter::Predictions => EventsFilter::All,
        }
    }
}

/// Per-tab state for the Events view: the active category filter.
pub struct EventsViewState {
    pub filter: EventsFilter,
}

impl Default for EventsViewState {
    fn default() -> Self {
        Self::new()
    }
}

impl EventsViewState {
    /// Creates a new state with the `All` filter selected.
    pub fn new() -> Self {
        Self {
            filter: EventsFilter::All,
        }
    }

    /// Advances the filter to the next category.
    pub fn cycle_filter(&mut self) {
        self.filter = self.filter.next();
    }
}

/// Renders Tab 5: a filter bar above the live event list.
pub fn render_events(
    frame: &mut ratatui::Frame,
    area: ratatui::layout::Rect,
    state: &DashboardState,
    events_state: &EventsViewState,
) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(2), Constraint::Min(0)])
        .split(area);

    render_filter_bar(frame, chunks[0], events_state);
    render_event_list(frame, chunks[1], state, events_state);
}

/// Renders the horizontal filter bar showing each category option.
fn render_filter_bar(
    frame: &mut ratatui::Frame,
    area: ratatui::layout::Rect,
    events_state: &EventsViewState,
) {
    let spans: Vec<Span> = EventsFilter::all()
        .iter()
        .flat_map(|&filter| {
            let style = if filter == events_state.filter {
                Style::default()
                    .add_modifier(Modifier::BOLD)
                    .fg(Color::White)
            } else {
                Style::default().fg(Color::DarkGray)
            };
            [
                Span::styled(format!("[{}]", filter.label()), style),
                Span::raw("  "),
            ]
        })
        .collect();

    let bar = Paragraph::new(Line::from(spans)).style(Style::default().fg(Color::DarkGray));
    frame.render_widget(bar, area);
}

/// Renders the filtered event list from the ring buffer.
fn render_event_list(
    frame: &mut ratatui::Frame,
    area: ratatui::layout::Rect,
    state: &DashboardState,
    events_state: &EventsViewState,
) {
    let block = Block::default().title("Events").borders(Borders::ALL);

    let items: Vec<ListItem> = state
        .events
        .iter()
        .filter(|entry| events_state.filter.matches(&entry.event_type))
        .map(|entry| {
            let time = entry.received_at.format("%H:%M:%S").to_string();
            let payload_summary = truncate_payload(&entry.payload);
            ListItem::new(Line::from(vec![
                Span::styled(format!("{time}  "), Style::default().fg(Color::DarkGray)),
                Span::styled(
                    format!("{:<42}", entry.event_type),
                    event_type_style(&entry.event_type),
                ),
                Span::styled(payload_summary, Style::default().fg(Color::DarkGray)),
            ]))
        })
        .collect();

    if items.is_empty() {
        let message = match events_state.filter {
            EventsFilter::All => "No events received yet",
            _ => "No events match this filter",
        };
        let placeholder = Paragraph::new(message)
            .block(block)
            .style(Style::default().fg(Color::DarkGray));
        frame.render_widget(placeholder, area);
        return;
    }

    let list = List::new(items).block(block);
    frame.render_widget(list, area);
}

/// Returns a style based on the event outcome encoded in `event_type`.
fn event_type_style(event_type: &str) -> Style {
    if event_type.ends_with("errored") {
        Style::default().fg(Color::Red)
    } else if event_type.ends_with("completed") {
        Style::default().fg(Color::Green)
    } else if event_type.ends_with("started") || event_type.ends_with("requested") {
        Style::default().fg(Color::Yellow)
    } else {
        Style::default()
    }
}

/// Truncates a JSON payload to a short summary string.
///
/// Truncation is performed on Unicode codepoint boundaries (not byte boundaries)
/// to avoid panicking on multibyte characters such as CJK, emoji, or accented text.
fn truncate_payload(payload: &serde_json::Value) -> String {
    let serialized = payload.to_string();
    if serialized.chars().count() > 58 {
        let truncated: String = serialized.chars().take(57).collect();
        format!("{truncated}…")
    } else {
        serialized
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use ratatui::backend::TestBackend;
    use ratatui::Terminal;

    use crate::dashboard_service::cache::{DashboardState, EventEntry};

    fn render_to_string(
        width: u16,
        height: u16,
        state: &DashboardState,
        events_state: &EventsViewState,
    ) -> String {
        let backend = TestBackend::new(width, height);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal
            .draw(|frame| render_events(frame, frame.area(), state, events_state))
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

    fn make_event(event_type: &str) -> EventEntry {
        EventEntry {
            event_id: 1,
            event_type: event_type.to_string(),
            payload: serde_json::json!({"session_id": "abc"}),
            received_at: Utc::now(),
        }
    }

    #[test]
    fn test_events_filter_all_variants_covered() {
        assert_eq!(EventsFilter::all().len(), 4);
    }

    #[test]
    fn test_events_filter_cycle_wraps() {
        let mut state = EventsViewState::new();
        assert_eq!(state.filter, EventsFilter::All);
        state.cycle_filter();
        assert_eq!(state.filter, EventsFilter::Trading);
        state.cycle_filter();
        assert_eq!(state.filter, EventsFilter::Data);
        state.cycle_filter();
        assert_eq!(state.filter, EventsFilter::Predictions);
        state.cycle_filter();
        assert_eq!(state.filter, EventsFilter::All);
    }

    #[test]
    fn test_events_filter_all_matches_everything() {
        assert!(EventsFilter::All.matches("portfolio_rebalance_completed"));
        assert!(EventsFilter::All.matches("equity_bars_sync_completed"));
        assert!(EventsFilter::All.matches("equity_predictions_completed"));
    }

    #[test]
    fn test_events_filter_trading_matches_portfolio_events() {
        assert!(EventsFilter::Trading.matches("portfolio_rebalance_completed"));
        assert!(EventsFilter::Trading.matches("portfolio_liquidation_errored"));
        assert!(EventsFilter::Trading.matches("market_session_check"));
        assert!(!EventsFilter::Trading.matches("equity_bars_sync_completed"));
        assert!(!EventsFilter::Trading.matches("equity_predictions_completed"));
    }

    #[test]
    fn test_events_filter_data_matches_data_events() {
        assert!(EventsFilter::Data.matches("equity_bars_sync_completed"));
        assert!(EventsFilter::Data.matches("trading_history_export_completed"));
        assert!(EventsFilter::Data.matches("database_backup_completed"));
        assert!(!EventsFilter::Data.matches("portfolio_rebalance_completed"));
        assert!(!EventsFilter::Data.matches("equity_predictions_completed"));
    }

    #[test]
    fn test_events_filter_predictions_matches_prediction_events() {
        assert!(EventsFilter::Predictions.matches("equity_predictions_completed"));
        assert!(EventsFilter::Predictions.matches("equity_predictions_errored"));
        assert!(!EventsFilter::Predictions.matches("portfolio_rebalance_completed"));
        assert!(!EventsFilter::Predictions.matches("equity_bars_sync_completed"));
    }

    #[test]
    fn test_render_events_empty_state_shows_placeholder() {
        let state = DashboardState::default();
        let events_state = EventsViewState::new();
        let output = render_to_string(120, 40, &state, &events_state);
        assert!(output.contains("No events"));
    }

    #[test]
    fn test_render_events_shows_filter_labels() {
        let state = DashboardState::default();
        let events_state = EventsViewState::new();
        let output = render_to_string(120, 40, &state, &events_state);
        assert!(output.contains("All"));
        assert!(output.contains("Trading"));
        assert!(output.contains("Data"));
        assert!(output.contains("Predictions"));
    }

    #[test]
    fn test_render_events_shows_event_type() {
        let mut state = DashboardState::default();
        state
            .events
            .push_back(make_event("portfolio_rebalance_completed"));
        let events_state = EventsViewState::new();
        let output = render_to_string(120, 40, &state, &events_state);
        assert!(output.contains("portfolio_rebalance_completed"));
    }

    #[test]
    fn test_render_events_filter_hides_non_matching() {
        let mut state = DashboardState::default();
        state
            .events
            .push_back(make_event("portfolio_rebalance_completed"));
        let mut events_state = EventsViewState::new();
        events_state.filter = EventsFilter::Data;
        let output = render_to_string(120, 40, &state, &events_state);
        // Data filter active — trading event should not render in the list.
        assert!(output.contains("No events match this filter"));
    }

    #[test]
    fn test_truncate_payload_short_value() {
        let payload = serde_json::json!({"id": 1});
        let result = truncate_payload(&payload);
        assert!(!result.contains('…'));
    }

    #[test]
    fn test_truncate_payload_long_value() {
        let payload = serde_json::json!({"key": "a very long value that exceeds the display limit for dashboard rendering"});
        let result = truncate_payload(&payload);
        assert!(result.contains('…'));
        // Displayed portion is 57 visible chars + ellipsis.
        assert!(result.len() <= 61); // 57 chars + "…" (multi-byte)
    }

    #[test]
    fn test_truncate_payload_multibyte_does_not_panic() {
        // JSON value with CJK characters (3 bytes each in UTF-8).
        // 60 repetitions produces a serialized string well over 58 chars,
        // so truncation is triggered. A byte-index slice would panic here
        // if the cut landed mid-codepoint.
        let long_cjk = "日".repeat(60);
        let payload = serde_json::json!({"key": long_cjk});
        let result = truncate_payload(&payload);
        assert!(result.contains('…'));
        assert!(result.is_char_boundary(result.len()));
    }

    #[test]
    fn test_event_type_style_errored_is_red() {
        let style = event_type_style("equity_bars_sync_errored");
        assert_eq!(style.fg, Some(Color::Red));
    }

    #[test]
    fn test_event_type_style_completed_is_green() {
        let style = event_type_style("portfolio_rebalance_completed");
        assert_eq!(style.fg, Some(Color::Green));
    }

    #[test]
    fn test_event_type_style_started_is_yellow() {
        let style = event_type_style("equity_bars_sync_started");
        assert_eq!(style.fg, Some(Color::Yellow));
    }
}
