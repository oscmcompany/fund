//! TUI application: tab state, key handling, and the main ratatui render loop.
//!
//! [`run_event_loop`] is the top-level entry point. It sets up the alternate
//! screen, drives the render-poll-input cycle, and restores the terminal on
//! exit. Actual view rendering for each tab is added in PR 2; this module
//! provides the tab bar, header, footer, and layout scaffolding.

use std::time::Duration;

use crossterm::event::{self, Event, KeyCode, KeyEvent, KeyModifiers};
use crossterm::terminal::{self, EnterAlternateScreen, LeaveAlternateScreen};
use crossterm::ExecutableCommand;
use ratatui::backend::CrosstermBackend;
use ratatui::layout::{Constraint, Direction, Layout};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Paragraph, Tabs};
use ratatui::Terminal;
use tracing::info;

use crate::dashboard_service::cache::{DashboardState, SharedState};

/// Minimum terminal width required to render the dashboard layout.
const MINIMUM_TERMINAL_WIDTH: u16 = 80;

/// Minimum terminal height required to render the dashboard layout.
const MINIMUM_TERMINAL_HEIGHT: u16 = 24;

/// How often the event loop polls for crossterm input events.
const INPUT_POLL_INTERVAL: Duration = Duration::from_millis(100);

/// The five dashboard tabs in display order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tab {
    Positions,
    Performance,
    Trades,
    Predictions,
    Events,
}

impl Tab {
    /// Returns all tabs in display order.
    fn all() -> &'static [Tab] {
        &[
            Tab::Positions,
            Tab::Performance,
            Tab::Trades,
            Tab::Predictions,
            Tab::Events,
        ]
    }

    /// Returns the tab bar label, including the digit keybind prefix.
    fn label(self) -> &'static str {
        match self {
            Tab::Positions => "1 Positions",
            Tab::Performance => "2 Performance",
            Tab::Trades => "3 Trades",
            Tab::Predictions => "4 Predictions",
            Tab::Events => "5 Events",
        }
    }

    /// Returns the zero-based index of this tab within [`Tab::all`].
    fn index(self) -> usize {
        Tab::all().iter().position(|&tab| tab == self).unwrap_or(0)
    }

    /// Maps a digit character to the corresponding tab, or `None` if out of range.
    pub fn from_digit(digit: char) -> Option<Tab> {
        match digit {
            '1' => Some(Tab::Positions),
            '2' => Some(Tab::Performance),
            '3' => Some(Tab::Trades),
            '4' => Some(Tab::Predictions),
            '5' => Some(Tab::Events),
            _ => None,
        }
    }
}

/// Top-level application state: the currently selected tab.
pub struct Application {
    pub current_tab: Tab,
}

impl Application {
    /// Creates a new application with Tab 1 (Positions) selected.
    pub fn new() -> Self {
        Self {
            current_tab: Tab::Positions,
        }
    }
}

/// Runs the ratatui terminal event loop until the user quits.
///
/// Sets up alternate screen and raw mode, loops over render-poll-input cycles,
/// then restores the terminal. Reads dashboard data under a shared read lock
/// on each frame without blocking the background polling task.
pub async fn run_event_loop(state: SharedState) {
    let mut terminal = setup_terminal().expect("Failed to set up terminal");
    let mut app = Application::new();

    info!("Dashboard event loop started");

    loop {
        let dashboard = state.read().await;
        terminal
            .draw(|frame| render(frame, &app, &*dashboard))
            .expect("Failed to draw frame");
        drop(dashboard);

        if event::poll(INPUT_POLL_INTERVAL).expect("Failed to poll for events") {
            match event::read().expect("Failed to read event") {
                Event::Key(key) => {
                    if should_quit(key) {
                        break;
                    }
                    if let Some(tab) = key_to_tab(key) {
                        app.current_tab = tab;
                    }
                }
                _ => {}
            }
        }
    }

    teardown_terminal(&mut terminal).expect("Failed to restore terminal");
    info!("Dashboard event loop exited");
}

/// Returns `true` if the key event should exit the application.
fn should_quit(key: KeyEvent) -> bool {
    match key.code {
        KeyCode::Char('q') | KeyCode::Char('Q') => true,
        KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => true,
        _ => false,
    }
}

/// Maps a digit key event to the corresponding tab, or `None`.
fn key_to_tab(key: KeyEvent) -> Option<Tab> {
    match key.code {
        KeyCode::Char(digit) => Tab::from_digit(digit),
        _ => None,
    }
}

/// Renders a single frame: size guard, header, tab bar, content area, footer.
pub fn render(frame: &mut ratatui::Frame, app: &Application, state: &DashboardState) {
    let area = frame.area();

    if area.width < MINIMUM_TERMINAL_WIDTH || area.height < MINIMUM_TERMINAL_HEIGHT {
        let message = Paragraph::new("Terminal too small — resize to at least 80×24")
            .style(Style::default().fg(Color::Yellow));
        frame.render_widget(message, area);
        return;
    }

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1), // header
            Constraint::Length(3), // tab bar
            Constraint::Min(0),    // content area
            Constraint::Length(1), // footer
        ])
        .split(area);

    render_header(frame, chunks[0], state);
    render_tab_bar(frame, chunks[1], app);
    render_content(frame, chunks[2], app, state);
    render_footer(frame, chunks[3]);
}

/// Renders the single-line header: fund name and last-updated timestamp.
fn render_header(frame: &mut ratatui::Frame, area: ratatui::layout::Rect, state: &DashboardState) {
    let timestamp = state
        .last_updated
        .map(|time| format!("Last updated: {}", time.format("%Y-%m-%d %H:%M:%S UTC")))
        .unwrap_or_else(|| "Loading...".to_string());

    let header = Paragraph::new(Line::from(vec![
        Span::styled("OSCM Fund  ", Style::default().add_modifier(Modifier::BOLD)),
        Span::styled(timestamp, Style::default().fg(Color::DarkGray)),
    ]));
    frame.render_widget(header, area);
}

/// Renders the tab bar with the currently selected tab highlighted.
fn render_tab_bar(frame: &mut ratatui::Frame, area: ratatui::layout::Rect, app: &Application) {
    let titles: Vec<Line> = Tab::all()
        .iter()
        .map(|tab| Line::from(tab.label()))
        .collect();
    let tab_bar = Tabs::new(titles)
        .block(Block::default().borders(Borders::BOTTOM))
        .select(app.current_tab.index())
        .highlight_style(
            Style::default()
                .add_modifier(Modifier::BOLD)
                .fg(Color::White),
        );
    frame.render_widget(tab_bar, area);
}

/// Renders a placeholder content area for the currently selected tab.
///
/// Full per-tab rendering is implemented in PR 2. Until then, the content
/// area shows either a database error or a "loading" / placeholder message.
fn render_content(
    frame: &mut ratatui::Frame,
    area: ratatui::layout::Rect,
    app: &Application,
    state: &DashboardState,
) {
    let text = match &state.database_error {
        Some(error) => format!(
            "Data unavailable — database error: {}\n\nLast successful update: {}",
            error,
            state
                .last_updated
                .map(|time| time.format("%Y-%m-%d %H:%M:%S UTC").to_string())
                .unwrap_or_else(|| "never".to_string()),
        ),
        None => {
            if state.last_updated.is_none() {
                "Loading data...".to_string()
            } else {
                let tab_name = match app.current_tab {
                    Tab::Positions => "Positions",
                    Tab::Performance => "Performance",
                    Tab::Trades => "Trades",
                    Tab::Predictions => "Predictions",
                    Tab::Events => "Events",
                };
                format!("{tab_name} — rendering coming in PR 2")
            }
        }
    };

    let content = Paragraph::new(text)
        .block(Block::default().borders(Borders::ALL))
        .style(Style::default().fg(Color::Gray));
    frame.render_widget(content, area);
}

/// Renders the single-line footer with keybind hints.
fn render_footer(frame: &mut ratatui::Frame, area: ratatui::layout::Rect) {
    let footer = Paragraph::new(Line::from(vec![
        Span::raw("1-5 switch tabs  "),
        Span::raw("q quit  "),
        Span::styled(
            "ssh dashboard.oscm.company",
            Style::default().fg(Color::DarkGray),
        ),
    ]))
    .style(Style::default().fg(Color::DarkGray));
    frame.render_widget(footer, area);
}

fn setup_terminal() -> std::io::Result<Terminal<CrosstermBackend<std::io::Stdout>>> {
    terminal::enable_raw_mode()?;
    std::io::stdout().execute(EnterAlternateScreen)?;
    Terminal::new(CrosstermBackend::new(std::io::stdout()))
}

fn teardown_terminal(
    terminal: &mut Terminal<CrosstermBackend<std::io::Stdout>>,
) -> std::io::Result<()> {
    terminal::disable_raw_mode()?;
    terminal.backend_mut().execute(LeaveAlternateScreen)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use ratatui::backend::TestBackend;

    fn render_to_string(
        width: u16,
        height: u16,
        app: &Application,
        state: &DashboardState,
    ) -> String {
        let backend = TestBackend::new(width, height);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal.draw(|frame| render(frame, app, state)).unwrap();
        terminal
            .backend()
            .buffer()
            .clone()
            .content()
            .iter()
            .map(|cell| cell.symbol().to_string())
            .collect()
    }

    #[test]
    fn test_tab_from_digit_all_valid() {
        assert_eq!(Tab::from_digit('1'), Some(Tab::Positions));
        assert_eq!(Tab::from_digit('2'), Some(Tab::Performance));
        assert_eq!(Tab::from_digit('3'), Some(Tab::Trades));
        assert_eq!(Tab::from_digit('4'), Some(Tab::Predictions));
        assert_eq!(Tab::from_digit('5'), Some(Tab::Events));
    }

    #[test]
    fn test_tab_from_digit_out_of_range() {
        assert_eq!(Tab::from_digit('0'), None);
        assert_eq!(Tab::from_digit('6'), None);
        assert_eq!(Tab::from_digit('a'), None);
    }

    #[test]
    fn test_tab_index_is_ordered() {
        for (expected_index, tab) in Tab::all().iter().enumerate() {
            assert_eq!(tab.index(), expected_index);
        }
    }

    #[test]
    fn test_tab_labels_are_unique() {
        let labels: std::collections::HashSet<&str> =
            Tab::all().iter().map(|tab| tab.label()).collect();
        assert_eq!(labels.len(), Tab::all().len());
    }

    #[test]
    fn test_should_quit_on_lowercase_q() {
        let key = KeyEvent::new(KeyCode::Char('q'), KeyModifiers::NONE);
        assert!(should_quit(key));
    }

    #[test]
    fn test_should_quit_on_uppercase_q() {
        let key = KeyEvent::new(KeyCode::Char('Q'), KeyModifiers::NONE);
        assert!(should_quit(key));
    }

    #[test]
    fn test_should_quit_on_ctrl_c() {
        let key = KeyEvent::new(KeyCode::Char('c'), KeyModifiers::CONTROL);
        assert!(should_quit(key));
    }

    #[test]
    fn test_should_not_quit_on_digit() {
        for digit in '1'..='5' {
            let key = KeyEvent::new(KeyCode::Char(digit), KeyModifiers::NONE);
            assert!(!should_quit(key), "digit '{digit}' should not quit");
        }
    }

    #[test]
    fn test_key_to_tab_maps_digits() {
        let key = KeyEvent::new(KeyCode::Char('3'), KeyModifiers::NONE);
        assert_eq!(key_to_tab(key), Some(Tab::Trades));
    }

    #[test]
    fn test_key_to_tab_ignores_non_digit() {
        let key = KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE);
        assert_eq!(key_to_tab(key), None);
    }

    #[test]
    fn test_render_shows_terminal_too_small_message() {
        let app = Application::new();
        let state = DashboardState::default();
        let output = render_to_string(40, 10, &app, &state);
        assert!(output.contains("Terminal too small"));
    }

    #[test]
    fn test_render_full_size_shows_all_tab_labels() {
        let app = Application::new();
        let state = DashboardState::default();
        let output = render_to_string(120, 40, &app, &state);
        assert!(output.contains("Positions"));
        assert!(output.contains("Performance"));
        assert!(output.contains("Trades"));
        assert!(output.contains("Predictions"));
        assert!(output.contains("Events"));
    }

    #[test]
    fn test_render_shows_loading_before_first_poll() {
        let app = Application::new();
        let state = DashboardState::default(); // last_updated is None
        let output = render_to_string(120, 40, &app, &state);
        assert!(output.contains("Loading"));
    }

    #[test]
    fn test_render_shows_database_error() {
        let app = Application::new();
        let mut state = DashboardState::default();
        state.database_error = Some("connection refused".to_string());
        let output = render_to_string(120, 40, &app, &state);
        assert!(output.contains("database error"));
        assert!(output.contains("connection refused"));
    }

    #[test]
    fn test_render_shows_fund_name_in_header() {
        let app = Application::new();
        let state = DashboardState::default();
        let output = render_to_string(120, 40, &app, &state);
        assert!(output.contains("OSCM Fund"));
    }

    #[test]
    fn test_render_shows_footer_keybinds() {
        let app = Application::new();
        let state = DashboardState::default();
        let output = render_to_string(120, 40, &app, &state);
        assert!(output.contains("quit"));
    }
}
