//! Generates the market a specification describes, one session at a time, as the quotes and prints the archive holds.

use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, NaiveTime, TimeDelta, Utc};
use rand::rngs::StdRng;

use super::draw::{exponential, standard_normal, uniform};
use super::{Mechanism, MechanismKind, Specification, Window};
use crate::common::book::Side;
use crate::common::laboratory::cost::BasisPoints;
use crate::common::market::record::Quote;
use crate::common::market::trade_bars::{Correction, Print, TradeBar, TradeConditions, TradeFold};
use crate::common::market::{PRICE_SCALE, Price, Shares, Symbol};
use crate::common::time::SessionDate;
use crate::common::time::calendar::TradingSession;

/// A full session's seconds and minutes, over which a daily volatility accrues.
const FULL_SESSION_SECONDS: f64 = 23_400.0;
const FULL_SESSION_MINUTES: f64 = 390.0;

/// Price ticks to the cent for a quote whose ask reaches a dollar, and to the hundredth of a cent otherwise, as Reg NMS
/// sets the quoting grid.
const CENT: i64 = PRICE_SCALE / 100;
const SUBPENNY: i64 = PRICE_SCALE / 10_000;

/// The highest midpoint quoted, far inside what a `Price` holds.
const HIGHEST_MIDPOINT_DOLLARS: f64 = 1_000_000.0;

/// A faded impact smaller than this is dropped at a session's open.
const NEGLIGIBLE_IMPACT: f64 = 1e-9;

/// The stream the market's own path draws from, apart from every name's.
const MARKET_STREAM: u64 = u64::MAX;

/// One mechanism planted on one name: its side, its first and last pieces, and the strength it moved the price by.
#[derive(Debug, Clone, PartialEq)]
pub struct Planted {
    pub symbol: Symbol,
    pub kind: MechanismKind,
    pub side: Side,
    pub first: DateTime<Utc>,
    pub last: DateTime<Utc>,
    pub strength: BasisPoints,
}

/// One generated session: its quotes and prints in time order, and what was planted in it.
#[derive(Debug, Clone, PartialEq)]
pub struct Session {
    date: SessionDate,
    quotes: Vec<Quote>,
    prints: Vec<Print>,
    planted: Vec<Planted>,
}

impl Session {
    pub fn date(&self) -> SessionDate {
        self.date
    }

    pub fn quotes(&self) -> &[Quote] {
        &self.quotes
    }

    pub fn prints(&self) -> &[Print] {
        &self.prints
    }

    pub fn planted(&self) -> &[Planted] {
        &self.planted
    }

    /// The session's one-minute trade bars, folded as the archive folds a regular-sale tape.
    pub fn trade_bars(&self) -> Vec<TradeBar> {
        let mut fold = TradeFold::new(self.date, TradeConditions::new(BTreeMap::new()));
        for print in &self.prints {
            fold.push(print, &[], Correction::Stands);
        }
        fold.finish().0
    }
}

/// Why a market could not be generated.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum GenerationRefusal {
    #[error("a market needs at least one name")]
    NoNames,
    #[error("{next} does not follow {previous}")]
    Unordered {
        previous: SessionDate,
        next: SessionDate,
    },
    /// A flow's pieces need at least the window's length, so no start fits inside it.
    #[error("a {kind} flow lasting {duration} does not fit in its window")]
    FlowOutlastsWindow {
        kind: MechanismKind,
        duration: TimeDelta,
    },
    #[error("a {kind} window from {from} to {to} is not inside {session}'s hours")]
    WindowOutsideSession {
        kind: MechanismKind,
        session: SessionDate,
        from: NaiveTime,
        to: NaiveTime,
    },
    #[error("a {kind} flow lasting {duration} starts before {session} opens")]
    FlowOutlastsSession {
        kind: MechanismKind,
        session: SessionDate,
        duration: TimeDelta,
    },
}

/// The market's log level at each minute of one session, from the open.
struct MarketPath {
    open: DateTime<Utc>,
    levels: Vec<f64>,
}

impl MarketPath {
    /// The level at `at`, interpolated within its minute so the path has no steps.
    fn level(&self, at: DateTime<Utc>) -> f64 {
        let minutes = (at - self.open).as_seconds_f64().max(0.0) / 60.0;
        let last = self.levels.len() - 2;
        let index = (minutes.floor() as usize).min(last);
        let fraction = (minutes - index as f64).min(1.0);
        self.levels[index] + fraction * (self.levels[index + 1] - self.levels[index])
    }
}

/// A transient impact: `amplitude` in log price at `at`, halving every `half_life_seconds`.
struct Impulse {
    at: DateTime<Utc>,
    amplitude: f64,
    half_life_seconds: f64,
}

impl Impulse {
    fn at(&self, instant: DateTime<Utc>) -> f64 {
        let elapsed = (instant - self.at).as_seconds_f64();
        self.amplitude * 0.5_f64.powf(elapsed / self.half_life_seconds)
    }
}

/// One name's drawn traits and its price path so far.
struct Path {
    symbol: Symbol,
    opening_price: f64,
    volatility_per_root_second: f64,
    beta: f64,
    trades_per_second: f64,
    quotes_per_second: f64,
    spread_ticks: f64,
    trade_size: f64,
    quote_size: f64,
    /// The name's own log return to date, permanent impact included.
    level: f64,
    impulses: Vec<Impulse>,
    standing: Option<Quote>,
}

/// One name: its path, and the streams it draws from; the background tape and each mechanism draw from their own,
/// so the null arm's tape is the treatment's with the planted flow taken out.
struct Name {
    path: Path,
    background: StdRng,
    planting: Vec<StdRng>,
}

/// What happens to a name at one instant, in the order ties resolve.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Event {
    Quote,
    Trade,
    Piece { mechanism: usize, side: Side },
}

impl Path {
    fn midpoint(&self, at: DateTime<Utc>, market: &MarketPath) -> f64 {
        let transient: f64 = self.impulses.iter().map(|impulse| impulse.at(at)).sum();
        let logarithm = self.level + self.beta * market.level(at) + transient;
        (self.opening_price * logarithm.exp()).min(HIGHEST_MIDPOINT_DOLLARS)
    }

    /// Quotes around the midpoint at `at`, the spread and sizes drawn from `generator`, and stands the quote.
    fn quote(&mut self, at: DateTime<Utc>, market: &MarketPath, generator: &mut StdRng) -> Quote {
        let midpoint = self.midpoint(at, market);
        let ticks = 1 + exponential(generator, self.spread_ticks - 1.0).round() as i64;
        let subpenny_ask = midpoint * PRICE_SCALE as f64 + (ticks * SUBPENNY) as f64 / 2.0;
        let tick = match subpenny_ask >= PRICE_SCALE as f64 {
            true => CENT,
            false => SUBPENNY,
        };
        let below = midpoint * PRICE_SCALE as f64 - (ticks * tick) as f64 / 2.0;
        let bid = ((below / tick as f64).floor() as i64 * tick).max(tick);
        let mut lots = || {
            let lots = (exponential(generator, self.quote_size) / 100.0)
                .round()
                .max(1.0);
            Shares::whole(lots as u64 * 100).expect("a drawn quote size fits")
        };
        let (bid_size, ask_size) = (lots(), lots());
        let quote = Quote::new(
            self.symbol.clone(),
            at,
            Price::from_ticks(bid)
                .expect("the bid is a tick above zero and below the highest midpoint"),
            Price::from_ticks(bid + ticks * tick).expect("the ask is inside a price's range"),
            bid_size,
            ask_size,
        )
        .expect("the ask is above the bid");
        self.standing = Some(quote.clone());
        quote
    }

    /// A print of a drawn size against the standing quote's `side`.
    fn print(&self, at: DateTime<Utc>, side: Side, generator: &mut StdRng) -> Print {
        let standing = self
            .standing
            .as_ref()
            .expect("a session opens with a quote");
        let price = match side {
            Side::Buy => standing.ask(),
            Side::Sell => standing.bid(),
        };
        let shares = exponential(generator, self.trade_size).round().max(1.0) as u64;
        Print::new(
            self.symbol.clone(),
            at,
            price,
            Shares::whole(shares).expect("a drawn trade size fits"),
        )
    }
}

impl Name {
    fn new(specification: &Specification, symbol: Symbol) -> Self {
        let name_seed = specification.seed.child(fnv(symbol.as_str()));
        let mut background = name_seed.child(0).generator();
        let shapes = specification.shapes;
        let opening_price = shapes.price.draw(&mut background);
        let volatility = shapes.volatility.draw(&mut background);
        let beta = shapes.beta.draw(&mut background);
        let trades_per_minute = shapes.trades_per_minute.draw(&mut background);
        let quotes_per_minute = shapes.quotes_per_minute.draw(&mut background);
        let spread_ticks = shapes.spread_ticks.draw(&mut background).max(1.0);
        let trade_size = shapes.trade_size.draw(&mut background);
        let quote_size = shapes.quote_size.draw(&mut background);
        Self {
            path: Path {
                symbol,
                opening_price,
                volatility_per_root_second: volatility / FULL_SESSION_SECONDS.sqrt(),
                beta,
                trades_per_second: trades_per_minute / 60.0,
                quotes_per_second: quotes_per_minute / 60.0,
                spread_ticks,
                trade_size,
                quote_size,
                level: 0.0,
                impulses: Vec::new(),
                standing: None,
            },
            background,
            planting: (0..specification.mechanisms.len() as u64)
                .map(|index| name_seed.child(1 + index).generator())
                .collect(),
        }
    }

    /// Arrival instants of a Poisson process at `rate` per second over `[open, close)`.
    fn arrivals(
        &mut self,
        rate: f64,
        open: DateTime<Utc>,
        close: DateTime<Utc>,
    ) -> Vec<DateTime<Utc>> {
        let mut instants = Vec::new();
        let mut at = open;
        loop {
            at += nanoseconds(exponential(&mut self.background, 1.0 / rate));
            match at < close {
                true => instants.push(at),
                false => return instants,
            }
        }
    }

    /// Plants each mechanism the name's session carries, recording it and returning its pieces.
    fn plant(
        &mut self,
        session: &TradingSession,
        mechanisms: &[Mechanism],
        planted: &mut Vec<Planted>,
    ) -> Vec<(DateTime<Utc>, Event)> {
        let (open, close) = session.hours();
        let mut pieces = Vec::new();
        for (index, mechanism) in mechanisms.iter().enumerate() {
            let generator = &mut self.planting[index];
            let flow = mechanism.flow();
            if uniform(generator) >= flow.prevalence.value() {
                continue;
            }
            let side = side(generator);
            let first = match mechanism {
                Mechanism::ImpatientFlow { window, .. }
                | Mechanism::WorkedParent { window, .. } => {
                    let earliest = open + (window.from() - session.open());
                    let room = (window.to() - window.from()) - flow.duration();
                    earliest + nanoseconds(uniform(generator) * room.as_seconds_f64())
                }
                Mechanism::ClosingFlow { .. } => close - flow.duration() - flow.spacing.get(),
            };
            for piece in 0..flow.piece_count() {
                let at = first + flow.spacing.get() * piece;
                pieces.push((
                    at,
                    Event::Piece {
                        mechanism: index,
                        side,
                    },
                ));
            }
            planted.push(Planted {
                symbol: self.path.symbol.clone(),
                kind: mechanism.kind(),
                side,
                first,
                last: first + flow.duration(),
                strength: flow.strength,
            });
        }
        pieces
    }

    fn session(
        &mut self,
        session: &TradingSession,
        mechanisms: &[Mechanism],
        market: &MarketPath,
        generated: &mut Session,
    ) {
        let (open, close) = session.hours();
        self.path
            .impulses
            .retain(|impulse| impulse.at(open).abs() >= NEGLIGIBLE_IMPACT);
        let mut events = self.plant(session, mechanisms, &mut generated.planted);
        let trades = self.arrivals(self.path.trades_per_second, open, close);
        let quotes = self.arrivals(self.path.quotes_per_second, open, close);
        events.extend(trades.into_iter().map(|at| (at, Event::Trade)));
        events.extend(quotes.into_iter().map(|at| (at, Event::Quote)));
        events.sort();

        generated
            .quotes
            .push(self.path.quote(open, market, &mut self.background));
        let mut last_background = open;
        for (at, event) in events {
            match event {
                Event::Quote | Event::Trade => {
                    let elapsed = (at - last_background).as_seconds_f64();
                    self.path.level += self.path.volatility_per_root_second
                        * elapsed.sqrt()
                        * standard_normal(&mut self.background);
                    last_background = at;
                }
                Event::Piece { .. } => {}
            }
            match event {
                Event::Quote => {
                    generated
                        .quotes
                        .push(self.path.quote(at, market, &mut self.background))
                }
                Event::Trade => {
                    let side = side(&mut self.background);
                    generated
                        .prints
                        .push(self.path.print(at, side, &mut self.background));
                }
                Event::Piece { mechanism, side } => {
                    let generator = &mut self.planting[mechanism];
                    generated.prints.push(self.path.print(at, side, generator));
                    let planted = &mechanisms[mechanism];
                    let flow = planted.flow();
                    let sign = match side {
                        Side::Buy => 1.0,
                        Side::Sell => -1.0,
                    };
                    let amplitude =
                        sign * flow.strength.value() / 10_000.0 / flow.pieces.get() as f64;
                    match planted.half_life() {
                        Some(half_life) => self.path.impulses.push(Impulse {
                            at,
                            amplitude,
                            half_life_seconds: half_life.seconds(),
                        }),
                        None => self.path.level += amplitude,
                    }
                    generated
                        .quotes
                        .push(self.path.quote(at, market, generator));
                }
            }
        }
    }
}

/// A fair draw of a side.
fn side(generator: &mut StdRng) -> Side {
    match uniform(generator) < 0.5 {
        true => Side::Buy,
        false => Side::Sell,
    }
}

/// The market a specification describes over its sessions, yielded one session at a time in date order.
pub struct Market {
    sessions: std::vec::IntoIter<TradingSession>,
    mechanisms: Vec<Mechanism>,
    path: StdRng,
    market_volatility_per_minute: f64,
    market_level: f64,
    names: Vec<Name>,
}

impl Market {
    pub fn new(
        specification: &Specification,
        universe: &BTreeSet<Symbol>,
        sessions: &[TradingSession],
    ) -> Result<Self, GenerationRefusal> {
        if universe.is_empty() {
            return Err(GenerationRefusal::NoNames);
        }
        if let Some(pair) = sessions
            .windows(2)
            .find(|pair| pair[0].date() >= pair[1].date())
        {
            return Err(GenerationRefusal::Unordered {
                previous: pair[0].date(),
                next: pair[1].date(),
            });
        }
        for mechanism in &specification.mechanisms {
            check(mechanism, sessions)?;
        }
        Ok(Self {
            sessions: Vec::from(sessions).into_iter(),
            mechanisms: specification.mechanisms.clone(),
            path: specification.seed.child(MARKET_STREAM).generator(),
            market_volatility_per_minute: specification.shapes.market_volatility.value()
                / FULL_SESSION_MINUTES.sqrt(),
            market_level: 0.0,
            names: universe
                .iter()
                .map(|symbol| Name::new(specification, symbol.clone()))
                .collect(),
        })
    }
}

/// Refuses a mechanism whose pieces cannot all land inside every session.
fn check(mechanism: &Mechanism, sessions: &[TradingSession]) -> Result<(), GenerationRefusal> {
    let kind = mechanism.kind();
    let duration = mechanism.flow().duration();
    let window = |window: &Window| match duration < window.to() - window.from() {
        true => Ok(()),
        false => Err(GenerationRefusal::FlowOutlastsWindow { kind, duration }),
    };
    match mechanism {
        Mechanism::ImpatientFlow { window: hours, .. }
        | Mechanism::WorkedParent { window: hours, .. } => {
            window(hours)?;
            match sessions
                .iter()
                .find(|session| hours.from() < session.open() || hours.to() > session.close())
            {
                Some(session) => Err(GenerationRefusal::WindowOutsideSession {
                    kind,
                    session: session.date(),
                    from: hours.from(),
                    to: hours.to(),
                }),
                None => Ok(()),
            }
        }
        Mechanism::ClosingFlow { flow, .. } => {
            let needs = duration + flow.spacing.get();
            match sessions
                .iter()
                .find(|session| session.close() - session.open() < needs)
            {
                Some(session) => Err(GenerationRefusal::FlowOutlastsSession {
                    kind,
                    session: session.date(),
                    duration: needs,
                }),
                None => Ok(()),
            }
        }
    }
}

impl Iterator for Market {
    type Item = Session;

    fn next(&mut self) -> Option<Session> {
        let session = self.sessions.next()?;
        let (open, close) = session.hours();
        let minutes = ((close - open).as_seconds_f64() / 60.0).ceil() as usize;
        let mut levels = Vec::with_capacity(minutes + 1);
        levels.push(self.market_level);
        for _ in 0..minutes {
            self.market_level +=
                self.market_volatility_per_minute * standard_normal(&mut self.path);
            levels.push(self.market_level);
        }
        let market = MarketPath { open, levels };
        let mut generated = Session {
            date: session.date(),
            quotes: Vec::new(),
            prints: Vec::new(),
            planted: Vec::new(),
        };
        for name in &mut self.names {
            name.session(&session, &self.mechanisms, &market, &mut generated);
        }
        generated.quotes.sort_by(|left, right| {
            (left.timestamp(), left.symbol()).cmp(&(right.timestamp(), right.symbol()))
        });
        generated.prints.sort_by(|left, right| {
            (left.timestamp(), left.symbol()).cmp(&(right.timestamp(), right.symbol()))
        });
        Some(generated)
    }
}

fn nanoseconds(seconds: f64) -> TimeDelta {
    TimeDelta::nanoseconds((seconds * 1e9).round() as i64)
}

/// FNV-1a of a symbol, so a name's stream follows its symbol rather than its place in the universe.
fn fnv(text: &str) -> u64 {
    text.bytes().fold(0xcbf2_9ce4_8422_2325, |hash, byte| {
        (hash ^ u64::from(byte)).wrapping_mul(0x0100_0000_01b3)
    })
}

#[cfg(test)]
mod tests {
    use chrono::NaiveDate;

    use super::super::draw::{LogNormal, Seed};
    use super::super::tests::{flow, shapes, window};
    use super::super::{Shapes, Span, Volatility};
    use super::*;

    fn trading_session(date: &str, close: &str) -> TradingSession {
        TradingSession::new(
            SessionDate::from_date(date.parse::<NaiveDate>().unwrap()),
            "09:30:00".parse().unwrap(),
            close.parse().unwrap(),
        )
        .unwrap()
    }

    fn sessions() -> Vec<TradingSession> {
        vec![
            trading_session("2026-03-02", "16:00:00"),
            trading_session("2026-03-03", "16:00:00"),
        ]
    }

    fn universe(symbols: &[&str]) -> BTreeSet<Symbol> {
        symbols
            .iter()
            .map(|symbol| Symbol::new(symbol).unwrap())
            .collect()
    }

    /// Shapes with no diffusion, so a midpoint moves only by what was planted.
    fn still() -> Shapes {
        Shapes {
            volatility: LogNormal::new(1e-12, 0.0).unwrap(),
            market_volatility: Volatility::new(0.0).unwrap(),
            ..shapes()
        }
    }

    fn specification(shapes: Shapes, mechanisms: Vec<Mechanism>) -> Specification {
        Specification {
            seed: Seed::new(42),
            shapes,
            mechanisms,
        }
    }

    fn generate(specification: &Specification, symbols: &[&str]) -> Vec<Session> {
        Market::new(specification, &universe(symbols), &sessions())
            .unwrap()
            .collect()
    }

    fn midpoint(quote: &Quote) -> f64 {
        (quote.bid().dollars() + quote.ask().dollars()) / 2.0
    }

    /// The midpoint of the last quote before `at`, and of the first at or after it.
    fn around(session: &Session, at: DateTime<Utc>) -> (f64, f64) {
        let before = session
            .quotes()
            .iter()
            .rfind(|quote| quote.timestamp() < at)
            .unwrap();
        let after = session
            .quotes()
            .iter()
            .find(|quote| quote.timestamp() >= at)
            .unwrap();
        (midpoint(before), midpoint(after))
    }

    fn basis_points(from: f64, to: f64) -> f64 {
        (to / from - 1.0) * 10_000.0
    }

    fn signed(side: Side) -> f64 {
        match side {
            Side::Buy => 1.0,
            Side::Sell => -1.0,
        }
    }

    fn impatient(strength: f64, half_life_seconds: i64) -> Mechanism {
        Mechanism::ImpatientFlow {
            flow: flow(strength, 1.0, 1, 1),
            half_life: Span::new(TimeDelta::seconds(half_life_seconds)).unwrap(),
            window: window("10:00:00", "14:00:00"),
        }
    }

    #[test]
    fn test_a_seed_rebuilds_the_same_market() {
        let specification = specification(shapes(), vec![impatient(30.0, 600)]);
        let first = generate(&specification, &["AAA", "BBB"]);
        assert_eq!(first.len(), 2);
        assert!(!first[0].prints().is_empty());
        assert_eq!(first, generate(&specification, &["AAA", "BBB"]));
        let other = Specification {
            seed: Seed::new(43),
            ..specification
        };
        assert_ne!(first, generate(&other, &["AAA", "BBB"]));
    }

    #[test]
    fn test_a_names_tape_follows_its_symbol_rather_than_the_universe() {
        let specification = specification(shapes(), vec![impatient(30.0, 600)]);
        let only = |sessions: Vec<Session>| -> Vec<Vec<Print>> {
            sessions
                .iter()
                .map(|session| {
                    session
                        .prints()
                        .iter()
                        .filter(|print| print.symbol().as_str() == "BBB")
                        .cloned()
                        .collect()
                })
                .collect()
        };
        let beside_a = only(generate(&specification, &["AAA", "BBB"]));
        assert!(!beside_a[0].is_empty());
        assert_eq!(beside_a, only(generate(&specification, &["BBB", "CCC"])));
    }

    #[test]
    fn test_every_print_lifts_the_standing_ask_or_hits_the_standing_bid() {
        let specification = specification(shapes(), vec![impatient(30.0, 600)]);
        let sessions = generate(&specification, &["AAA", "BBB"]);
        let mut checked = 0;
        for session in &sessions {
            for print in session.prints() {
                let standing = session
                    .quotes()
                    .iter()
                    .rfind(|quote| {
                        quote.symbol() == print.symbol() && quote.timestamp() < print.timestamp()
                    })
                    .unwrap();
                assert!(
                    print.price() == standing.bid() || print.price() == standing.ask(),
                    "{print:?} against {standing:?}"
                );
                checked += 1;
            }
        }
        assert!(checked > 1_000, "{checked}");
    }

    #[test]
    fn test_background_prints_buy_and_sell_in_equal_measure() {
        let sessions = generate(&specification(shapes(), Vec::new()), &["AAA", "BBB"]);
        let (mut lifted, mut total) = (0, 0);
        for session in &sessions {
            for print in session.prints() {
                let standing = session
                    .quotes()
                    .iter()
                    .rfind(|quote| {
                        quote.symbol() == print.symbol() && quote.timestamp() < print.timestamp()
                    })
                    .unwrap();
                lifted += usize::from(print.price() == standing.ask());
                total += 1;
            }
        }
        let share = lifted as f64 / total as f64;
        assert!((0.45..0.55).contains(&share), "{lifted} of {total}");
    }

    #[test]
    fn test_the_null_tape_is_the_treatment_tape_without_the_planted_pieces() {
        let treatment = specification(shapes(), vec![impatient(30.0, 600)]);
        let planted = generate(&treatment, &["AAA", "BBB"]);
        let null = generate(&treatment.null(), &["AAA", "BBB"]);
        for (planted, null) in planted.iter().zip(&null) {
            assert!(null.planted().is_empty());
            assert_eq!(planted.planted().len(), 2);
            let pieces: Vec<(DateTime<Utc>, &Symbol)> = planted
                .planted()
                .iter()
                .map(|planted| (planted.first, &planted.symbol))
                .collect();
            let background = |at: DateTime<Utc>, symbol: &Symbol| !pieces.contains(&(at, symbol));
            let prints = |prints: &[Print]| -> Vec<(DateTime<Utc>, Symbol, u64)> {
                prints
                    .iter()
                    .filter(|print| background(print.timestamp(), print.symbol()))
                    .map(|print| {
                        let size = match print {
                            Print::Trade(trade) => trade.size().units(),
                            Print::Unsized { .. } => 0,
                        };
                        (print.timestamp(), print.symbol().clone(), size)
                    })
                    .collect()
            };
            let quotes = |quotes: &[Quote]| -> Vec<(DateTime<Utc>, Symbol, u64, u64)> {
                quotes
                    .iter()
                    .filter(|quote| background(quote.timestamp(), quote.symbol()))
                    .map(|quote| {
                        (
                            quote.timestamp(),
                            quote.symbol().clone(),
                            quote.bid_size().units(),
                            quote.ask_size().units(),
                        )
                    })
                    .collect()
            };
            assert_eq!(planted.prints().len(), null.prints().len() + 2);
            assert_eq!(planted.quotes().len(), null.quotes().len() + 2);
            assert_eq!(prints(planted.prints()), prints(null.prints()));
            assert_eq!(quotes(planted.quotes()), quotes(null.quotes()));
        }
    }

    #[test]
    fn test_an_impatient_flow_moves_the_price_by_its_strength_and_fades_by_its_half_life() {
        let sessions = generate(
            &specification(still(), vec![impatient(100.0, 600)]),
            &["AAA"],
        );
        let session = &sessions[0];
        let planted = &session.planted()[0];
        let (before, after) = around(session, planted.first);
        let moved = signed(planted.side) * basis_points(before, after);
        assert!((moved - 100.0).abs() < 2.0, "{moved}");
        let (_, faded) = around(session, planted.first + TimeDelta::seconds(600));
        let left = signed(planted.side) * basis_points(before, faded);
        assert!((left - 50.0).abs() < 3.0, "{left}");
    }

    #[test]
    fn test_a_worked_parent_moves_the_price_for_good() {
        let worked = Mechanism::WorkedParent {
            flow: flow(100.0, 1.0, 10, 60),
            window: window("10:00:00", "14:00:00"),
        };
        let sessions = generate(&specification(still(), vec![worked]), &["AAA"]);
        let session = &sessions[0];
        let planted = &session.planted()[0];
        assert_eq!(planted.last - planted.first, TimeDelta::seconds(540));
        let (before, _) = around(session, planted.first);
        let closing = midpoint(session.quotes().last().unwrap());
        let moved = signed(planted.side) * basis_points(before, closing);
        assert!((moved - 100.0).abs() < 2.0, "{moved}");
    }

    #[test]
    fn test_closing_flow_lands_before_the_close_and_is_gone_by_the_next_open() {
        let closing = Mechanism::ClosingFlow {
            flow: flow(80.0, 1.0, 5, 60),
            half_life: Span::new(TimeDelta::seconds(1800)).unwrap(),
        };
        let sessions = generate(&specification(still(), vec![closing]), &["AAA"]);
        let planted = &sessions[0].planted()[0];
        let (_, close) = sessions[0].date().bounds();
        assert_eq!(
            planted.last,
            sessions[0].date().regular_close() - TimeDelta::seconds(60)
        );
        let (before, _) = around(&sessions[0], planted.first);
        let (_, into_close) = around(&sessions[0], planted.last);
        let moved = signed(planted.side) * basis_points(before, into_close);
        // Five 16bp pieces a minute apart, each fading over a half-hour: 16 × Σ 2^(-k/30) for k in 0..5 ≈ 76.4bp.
        assert!((moved - 76.4).abs() < 2.0, "{moved}");
        let next_open = midpoint(&sessions[1].quotes()[0]);
        let left = signed(planted.side) * basis_points(before, next_open);
        assert!(left.abs() < 2.0, "{left} at {close}");
    }

    #[test]
    fn test_a_quote_reaching_a_dollar_is_on_the_cent_grid_and_one_below_on_the_subpenny_grid() {
        let near_a_dollar = Shapes {
            price: LogNormal::new(1.0, 0.0).unwrap(),
            volatility: LogNormal::new(0.05, 0.0).unwrap(),
            ..shapes()
        };
        let sessions = generate(&specification(near_a_dollar, Vec::new()), &["AAA", "BBB"]);
        let (mut cents, mut subpennies) = (0, 0);
        for quote in sessions.iter().flat_map(Session::quotes) {
            let (bid, ask) = (quote.bid().ticks(), quote.ask().ticks());
            match ask >= 1_000_000 {
                true => {
                    assert!(bid % 10_000 == 0 && ask % 10_000 == 0, "{quote:?}");
                    cents += 1;
                }
                false => {
                    assert!(bid % 100 == 0 && ask % 100 == 0, "{quote:?}");
                    subpennies += usize::from(bid % 10_000 != 0 || ask % 10_000 != 0);
                }
            }
        }
        assert!(cents > 0 && subpennies > 0, "{cents} / {subpennies}");
    }

    #[test]
    fn test_names_share_the_market_move() {
        let market = Shapes {
            market_volatility: Volatility::new(0.03).unwrap(),
            ..still()
        };
        let sessions = generate(&specification(market, Vec::new()), &["AAA", "BBB"]);
        let day = |symbol: &str| {
            let quotes: Vec<&Quote> = sessions[0]
                .quotes()
                .iter()
                .filter(|quote| quote.symbol().as_str() == symbol)
                .collect();
            basis_points(midpoint(quotes[0]), midpoint(quotes[quotes.len() - 1]))
        };
        assert!(day("AAA").abs() > 5.0, "{}", day("AAA"));
        assert!((day("AAA") - day("BBB")).abs() < 2.0);
    }

    #[test]
    fn test_the_bars_hold_every_printed_share() {
        let sessions = generate(&specification(shapes(), Vec::new()), &["AAA", "BBB"]);
        let printed: u64 = sessions[0]
            .prints()
            .iter()
            .map(|print| match print {
                Print::Trade(trade) => trade.size().units(),
                Print::Unsized { .. } => 0,
            })
            .sum();
        let folded: u64 = sessions[0]
            .trade_bars()
            .iter()
            .map(|bar| bar.sums().totals().volume().units())
            .sum();
        assert!(printed > 0);
        assert_eq!(folded, printed);
    }

    #[test]
    fn test_a_market_that_cannot_hold_its_mechanisms_is_refused() {
        let half_day = vec![trading_session("2026-11-27", "13:00:00")];
        let refuse = |mechanisms: Vec<Mechanism>, symbols: &[&str], sessions: &[TradingSession]| {
            Market::new(
                &specification(shapes(), mechanisms),
                &universe(symbols),
                sessions,
            )
            .err()
        };
        assert_eq!(
            refuse(Vec::new(), &[], &sessions()),
            Some(GenerationRefusal::NoNames)
        );
        let backwards: Vec<TradingSession> = sessions().into_iter().rev().collect();
        assert_eq!(
            refuse(Vec::new(), &["AAA"], &backwards),
            Some(GenerationRefusal::Unordered {
                previous: backwards[0].date(),
                next: backwards[1].date(),
            })
        );
        let long = Mechanism::WorkedParent {
            flow: flow(10.0, 1.0, 61, 60),
            window: window("10:00:00", "11:00:00"),
        };
        assert_eq!(
            refuse(vec![long], &["AAA"], &sessions()),
            Some(GenerationRefusal::FlowOutlastsWindow {
                kind: MechanismKind::WorkedParent,
                duration: TimeDelta::seconds(3600),
            })
        );
        assert_eq!(
            refuse(vec![impatient(10.0, 60)], &["AAA"], &half_day),
            Some(GenerationRefusal::WindowOutsideSession {
                kind: MechanismKind::ImpatientFlow,
                session: half_day[0].date(),
                from: "10:00:00".parse().unwrap(),
                to: "14:00:00".parse().unwrap(),
            })
        );
        let closing = Mechanism::ClosingFlow {
            flow: flow(10.0, 1.0, 300, 60),
            half_life: Span::new(TimeDelta::seconds(60)).unwrap(),
        };
        assert_eq!(
            refuse(vec![closing], &["AAA"], &half_day),
            Some(GenerationRefusal::FlowOutlastsSession {
                kind: MechanismKind::ClosingFlow,
                session: half_day[0].date(),
                duration: TimeDelta::seconds(18_000),
            })
        );
    }
}
