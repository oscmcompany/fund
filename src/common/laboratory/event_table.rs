//! The event-clock feature table: per name and session, a draw of the name's quote updates or prints, each read a set
//! delay after it, with every feature and target a DuckDB statement computes from the cleaned ticks in scratch.

use std::collections::BTreeSet;
use std::fmt::Write;
use std::num::{NonZeroU16, NonZeroU32};
use std::ops::RangeInclusive;

use chrono::{DateTime, TimeDelta, Utc};
use strum::IntoEnumIterator;

use crate::common::book::Side;
use crate::common::cost::FillBound;
use crate::common::laboratory::universe::Universe;
use crate::common::market::Symbol;
use crate::common::market::industry::Industry;
use crate::common::market::record::BarInterval;
use crate::common::storage::{
    Key, Provider, ReferenceKey, ReferenceTable, TickProvider, TradesKey,
};
use crate::common::time::SessionDate;
use crate::common::time::calendar::TradingCalendar;

/// The trading days of daily closes the table reads before a session, giving sixty daily returns.
const DAILY_LOOKBACK: usize = 61;

/// Binds the scratch ticks and the archive the table reads to the buckets named by `AWS_S3_PROFILE_BUCKET_NAME` and
/// `AWS_S3_ARCHIVE_BUCKET_NAME`.
pub const BUCKET_PREAMBLE: &str = "
SET TimeZone='UTC';
INSTALL httpfs; LOAD httpfs; INSTALL aws; LOAD aws;
CREATE SECRET (TYPE s3, PROVIDER credential_chain, REGION 'us-east-1');
SET VARIABLE scratch = 's3://' || getenv('AWS_S3_PROFILE_BUCKET_NAME') || '/scratch/ticks';
SET VARIABLE archive = 's3://' || getenv('AWS_S3_ARCHIVE_BUCKET_NAME');
";

/// A daily return this large or larger in either direction is left out of the daily features.
const DAILY_RETURN_LIMIT: f64 = 0.4;

/// A print of at least this many shares, or of at least `BLOCK_DOLLARS`, is a block.
const BLOCK_SHARES: u32 = 10_000;

const BLOCK_DOLLARS: u32 = 200_000;

/// A print of fewer shares is an odd lot.
const ROUND_LOT_SHARES: u32 = 100;

/// The hundredths of a cent an off-exchange print's price ends in that mark it a retail buy, and a retail sell.
const RETAIL_BUY_HUNDREDTHS: RangeInclusive<u8> = 61..=99;

const RETAIL_SELL_HUNDREDTHS: RangeInclusive<u8> = 1..=39;

/// Massive's correction codes, and Alpaca's update labels, of a print withdrawn by a later correction or cancel.
const MASSIVE_WITHDRAWN_CORRECTIONS: [&str; 3] = ["1", "8", "10"];

const ALPACA_WITHDRAWN_UPDATES: [&str; 2] = ["corrected", "canceled"];

/// `values` as a SQL list of string literals.
fn quoted(values: &[&str]) -> String {
    values
        .iter()
        .map(|value| format!("'{value}'"))
        .collect::<Vec<_>>()
        .join(", ")
}

/// The macros saying whether a print may set the session's high and low, by Massive's codes and by Alpaca's letters,
/// from the archive's conditions snapshot of `as_of`; a withdrawn print never reaches them.
fn eligibility_macros(as_of: SessionDate) -> String {
    let conditions = Key::from(ReferenceKey::new(
        Provider::Massive,
        ReferenceTable::Conditions,
        as_of,
    ))
    .path();
    format!(
        r#"CREATE TEMP TABLE condition_rules AS SELECT * FROM read_parquet(getvariable('archive') || '/{conditions}');
SET VARIABLE eligible_codes = (SELECT list(code::VARCHAR) FROM condition_rules WHERE updates_high_low);
CREATE MACRO eligible(conditions) AS
    list_bool_and(list_transform(string_split(coalesce(conditions, ''), ','),
        lambda code: code = '' OR list_contains(getvariable('eligible_codes'), code)));
-- Alpaca's letters, per tape, that set a price; a letter two current conditions share with different rules sets none.
DELETE FROM condition_rules WHERE retired;
SET VARIABLE consolidated_letters = (
    SELECT list(consolidated_tape_letter) FROM (
        SELECT consolidated_tape_letter FROM condition_rules WHERE consolidated_tape_letter IS NOT NULL GROUP BY ALL
        HAVING count(DISTINCT (updates_volume, updates_high_low, updates_open_close)) = 1 AND bool_and(updates_high_low)));
SET VARIABLE unlisted_letters = (
    SELECT list(unlisted_trading_letter) FROM (
        SELECT unlisted_trading_letter FROM condition_rules WHERE unlisted_trading_letter IS NOT NULL GROUP BY ALL
        HAVING count(DISTINCT (updates_volume, updates_high_low, updates_open_close)) = 1 AND bool_and(updates_high_low)));
-- A print with no letters is unrestricted, as in the trade fold; a letter that cannot be placed restricts it.
CREATE MACRO eligible_letters(conditions, tape) AS
    CASE
        WHEN tape IN ('A', 'B') THEN len(list_filter(coalesce(conditions, []),
            lambda letter: NOT coalesce(letter = ' ' OR list_contains(getvariable('consolidated_letters'), letter), false))) = 0
        WHEN tape = 'C' THEN len(list_filter(coalesce(conditions, []),
            lambda letter: NOT coalesce(letter = '@' OR list_contains(getvariable('unlisted_letters'), letter), false))) = 0
        ELSE false END;
"#
    )
}

/// `draw_order` spelled as DuckDB macros over unsigned 64-bit integers, every product kept below 2^64.
pub const DRAW_ORDER_MACROS: &str = "
CREATE OR REPLACE MACRO draw_shift(hash, bits) AS xor(hash, hash >> bits);
CREATE OR REPLACE MACRO draw_times(hash, factor) AS (hash * factor::UBIGINT) % 4294967296::UBIGINT;
CREATE OR REPLACE MACRO draw_mix(hash) AS
    draw_shift(draw_times(draw_shift(draw_times(draw_shift(hash::UBIGINT, 16), 2246822507), 13), 3266489909), 16);
CREATE OR REPLACE MACRO draw_order(seed, event_number) AS
    draw_mix((seed::UBIGINT + draw_mix(event_number::UBIGINT % 4294967296::UBIGINT)) % 4294967296::UBIGINT);
";

/// MurmurHash3's 32-bit finalizer, which `draw_mix` spells in SQL.
fn mix(mut hash: u32) -> u32 {
    hash ^= hash >> 16;
    hash = hash.wrapping_mul(0x85eb_ca6b);
    hash ^= hash >> 13;
    hash = hash.wrapping_mul(0xc2b2_ae35);
    hash ^ (hash >> 16)
}

/// FNV-1a over `symbol/session/kind`, so a name's draws differ across sessions and clocks.
pub fn draw_seed(symbol: &Symbol, session: SessionDate, clock: EventClock) -> u32 {
    let (_, kind) = clock.source();
    format!("{}/{session}/{kind}", symbol.as_str())
        .bytes()
        .fold(0x811c_9dc5, |hash, byte| {
            (hash ^ u32::from(byte)).wrapping_mul(0x0100_0193)
        })
}

/// Where an event falls in its name's draw, lowest first; only the low 32 bits of `event_number` count.
pub fn draw_order(seed: u32, event_number: u64) -> u32 {
    let low = u32::try_from(event_number & u64::from(u32::MAX)).expect("masked to 32 bits");
    mix(seed.wrapping_add(mix(low)))
}

/// How one provider's cleaned ticks spell what the table reads of them.
struct Dialect {
    time: &'static str,
    not_withdrawn: String,
    sweep: &'static str,
    off_exchange: &'static str,
    eligible: &'static str,
}

fn dialect(provider: TickProvider) -> Dialect {
    match provider {
        TickProvider::Massive => Dialect {
            time: "sip_timestamp",
            not_withdrawn: format!(
                "coalesce(correction, '0') NOT IN ({})",
                quoted(&MASSIVE_WITHDRAWN_CORRECTIONS)
            ),
            sweep: "list_contains(string_split(coalesce(conditions, ''), ','), '14')",
            off_exchange: "exchange = '4'",
            eligible: "eligible(conditions)",
        },
        TickProvider::Alpaca => Dialect {
            time: "timestamp",
            not_withdrawn: format!(
                "coalesce(\"update\", '') NOT IN ({})",
                quoted(&ALPACA_WITHDRAWN_UPDATES)
            ),
            sweep: "list_contains(coalesce(conditions, []), 'F')",
            off_exchange: "exchange = 'D'",
            eligible: "eligible_letters(conditions, tape)",
        },
    }
}

/// Which events a row is drawn from.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    strum::Display,
    strum::EnumString,
    strum::IntoStaticStr,
    strum::EnumIter,
)]
#[strum(serialize_all = "snake_case")]
pub enum EventClock {
    QuoteUpdates,
    Prints,
}

impl EventClock {
    /// The table the events are drawn from and the `kind` their rows carry.
    fn source(self) -> (&'static str, &'static str) {
        match self {
            Self::QuoteUpdates => ("quote_events", "quote"),
            Self::Prints => ("trade_events", "print"),
        }
    }
}

/// The time from an event to the table's read of it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReadDelay(u32);

impl ReadDelay {
    pub fn from_milliseconds(milliseconds: u32) -> Self {
        Self(milliseconds)
    }

    pub fn milliseconds(self) -> u32 {
        self.0
    }
}

/// How many events are drawn per name and session, fewer where a name has fewer in the window.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Draws(NonZeroU16);

impl Draws {
    pub fn new(draws: NonZeroU16) -> Self {
        Self(draws)
    }
}

/// The minutes after the open that events are drawn from, `[start, end)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EntryWindow {
    start: u16,
    end: u16,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("the window from minute {start} to minute {end} is empty")]
pub struct EmptyWindow {
    start: u16,
    end: u16,
}

impl EntryWindow {
    pub fn new(start: u16, end: u16) -> Result<Self, EmptyWindow> {
        match start < end {
            true => Ok(Self { start, end }),
            false => Err(EmptyWindow { start, end }),
        }
    }
}

/// The delays after arrival at which the forward targets are read, each named by its whole minutes or else by
/// `seconds_` and its seconds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForwardHorizons(BTreeSet<NonZeroU32>);

impl ForwardHorizons {
    pub fn from_seconds(seconds: BTreeSet<NonZeroU32>) -> Self {
        Self(seconds)
    }

    /// Each horizon's name in the columns it adds beside its seconds, shortest first.
    fn named(&self) -> Vec<(String, u32)> {
        self.0
            .iter()
            .map(|seconds| (Horizon::After(*seconds).suffix(), seconds.get()))
            .collect()
    }
}

/// A horizon the forward columns are read at: a delay after arrival, or the session's last read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Horizon {
    After(NonZeroU32),
    Close,
}

impl Horizon {
    /// The suffix of the horizon's forward columns: its whole minutes, else `seconds_` and its seconds, or `close`.
    pub fn suffix(self) -> String {
        match self {
            Self::After(seconds) => match seconds.get() % 60 {
                0 => (seconds.get() / 60).to_string(),
                _ => format!("seconds_{seconds}"),
            },
            Self::Close => "close".to_string(),
        }
    }
}

/// One, five, fifteen, thirty and sixty minutes.
impl Default for ForwardHorizons {
    fn default() -> Self {
        Self(
            [60, 300, 900, 1800, 3600]
                .into_iter()
                .filter_map(NonZeroU32::new)
                .collect(),
        )
    }
}

/// How long each side's order rests at the touch in force at arrival; a row whose window passes the last read has no
/// resting labels.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RestingWindow(NonZeroU32);

impl RestingWindow {
    pub fn from_seconds(seconds: NonZeroU32) -> Self {
        Self(seconds)
    }

    pub fn seconds(self) -> u32 {
        self.0.get()
    }
}

/// Sixty seconds.
impl Default for RestingWindow {
    fn default() -> Self {
        Self(NonZeroU32::new(60).expect("sixty is not zero"))
    }
}

/// What a table draws and how it reads it, the same for every session it is built over.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventTable {
    provider: TickProvider,
    clock: EventClock,
    delay: ReadDelay,
    draws: Draws,
    window: EntryWindow,
    horizons: ForwardHorizons,
    resting: RestingWindow,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum EventTableRefusal {
    #[error("{session} is not a trading day of the calendar")]
    NotATradingDay { session: SessionDate },
    #[error("{session} trades {session_minutes} minutes, ending before the window's minute {end}")]
    WindowPastClose {
        session: SessionDate,
        end: u16,
        session_minutes: i64,
    },
    #[error("the conditions snapshot of {as_of} was not taken before {session}")]
    ConditionsNotBeforeSession {
        as_of: SessionDate,
        session: SessionDate,
    },
    #[error("the calendar holds no trading day before {session}")]
    NoEarlierTradingDay { session: SessionDate },
    /// The trading day before the session, or one between it and an earlier day that is held, whose bars are not.
    #[error("{provider}'s daily bars of {absent} are absent")]
    DailyBarsAbsent {
        provider: TickProvider,
        absent: SessionDate,
    },
}

impl EventTable {
    pub fn new(
        provider: TickProvider,
        clock: EventClock,
        delay: ReadDelay,
        draws: Draws,
        window: EntryWindow,
    ) -> Self {
        Self {
            provider,
            clock,
            delay,
            draws,
            window,
            horizons: ForwardHorizons::default(),
            resting: RestingWindow::default(),
        }
    }

    pub fn with_horizons(self, horizons: ForwardHorizons) -> Self {
        Self { horizons, ..self }
    }

    pub fn with_resting_window(self, resting: RestingWindow) -> Self {
        Self { resting, ..self }
    }

    /// The statements writing the universe's session's rows to `table/rows_<provider>_<session>.parquet` and its
    /// members to `table/universe_<session>.parquet`, reading the daily bars among `daily_bars` that are the table's
    /// provider's and the conditions snapshot of `conditions_as_of`; run after `BUCKET_PREAMBLE` or another binding of
    /// `scratch` and `archive`.
    pub fn statements(
        &self,
        calendar: &TradingCalendar,
        universe: &Universe,
        daily_bars: &[TradesKey],
        conditions_as_of: SessionDate,
    ) -> Result<String, EventTableRefusal> {
        let session = universe.session();
        let trading = calendar
            .session(session)
            .ok_or(EventTableRefusal::NotATradingDay { session })?;
        if conditions_as_of >= session {
            return Err(EventTableRefusal::ConditionsNotBeforeSession {
                as_of: conditions_as_of,
                session,
            });
        }
        let (opened, closed) = trading.hours();
        let session_minutes = (closed - opened).num_minutes();
        if i64::from(self.window.end) > session_minutes {
            return Err(EventTableRefusal::WindowPastClose {
                session,
                end: self.window.end,
                session_minutes,
            });
        }
        let daily_files = daily_lookback(calendar, session, self.provider, daily_bars)?
            .into_iter()
            .map(|day| {
                let key = TradesKey::new(self.provider.provider(), BarInterval::OneDay, day);
                format!("getvariable('archive') || '/{}'", Key::from(key).path())
            })
            .collect::<Vec<_>>()
            .join(", ");
        let (open, close) = (timestamp(opened), timestamp(closed));
        // The last instant any read, drawn entry or forward probe, may land on.
        let last_read = timestamp(closed - TimeDelta::seconds(1));
        let Dialect {
            time,
            not_withdrawn,
            sweep,
            off_exchange,
            eligible,
        } = dialect(self.provider);
        let provider = self.provider.provider();
        let eligibility = eligibility_macros(conditions_as_of);
        let members = members(universe, self.clock);
        let between =
            |range: RangeInclusive<u8>| format!("BETWEEN {} AND {}", range.start(), range.end());
        let (retail_buy, retail_sell) = (
            between(RETAIL_BUY_HUNDREDTHS),
            between(RETAIL_SELL_HUNDREDTHS),
        );
        let block = format!("size >= {BLOCK_SHARES} OR price * size >= {BLOCK_DOLLARS}");
        let (events, kind) = self.clock.source();
        let (delay, draws) = (self.delay.0, self.draws.0);
        let EntryWindow { start, end } = self.window;
        let entries = format!(
            "CREATE TEMP TABLE entries AS
    SELECT ticker, '{kind}' AS kind, event_number AS key, timestamp + INTERVAL {delay} MILLISECOND AS arrival
    FROM {events} JOIN universe USING (ticker)
    WHERE timestamp >= {open} + INTERVAL {start} MINUTE AND timestamp < {open} + INTERVAL {end} MINUTE
      AND timestamp + INTERVAL {delay} MILLISECOND <= {last_read}
    QUALIFY row_number() OVER (PARTITION BY ticker ORDER BY draw_order(draw_seed, event_number), event_number) <= {draws};
"
        );
        let horizons = self.horizons.named();
        // Each horizon's spelling of `template`, each followed by a comma, so no horizon spells nothing.
        let each = |template: &dyn Fn(&str, u32) -> String| -> String {
            horizons
                .iter()
                .map(|(name, seconds)| format!("{}, ", template(name, *seconds)))
                .collect()
        };
        let forward_offsets = each(&|name, seconds| {
            format!("('own', 'forward_{name}', {seconds}), ('SPY', 'forward_{name}', {seconds})")
        });
        let forward_pivots = each(&|name, _| {
            format!(
                "max(mid) FILTER (WHERE source = 'own' AND probe = 'forward_{name}') AS mid_forward_{name},
        max(spread_basis_points) FILTER (WHERE source = 'own' AND probe = 'forward_{name}') AS spread_forward_{name},
        max(mid) FILTER (WHERE source = 'SPY' AND probe = 'forward_{name}') AS spy_forward_{name}"
            )
        });
        let forward_spreads =
            each(&|name, _| format!("spread_forward_{name} AS forward_spread_{name}"));
        let raw = each(&|name, _| format!("10000 * (mid_forward_{name} / mid - 1) AS raw_{name}"));
        let net = each(&|name, _| {
            format!("raw_{name} - 10000 * (spy_forward_{name} / spy_mid - 1) AS net_{name}")
        });
        let forward_spy = each(&|name, _| {
            format!("10000 * (spy_forward_{name} / spy_mid - 1) AS forward_spy_{name}")
        });
        let resting = self.resting.seconds();
        let (mut fill_times, mut resting_labels, mut resting_columns) =
            (String::new(), String::new(), String::new());
        for (side, bound) in
            Side::iter().flat_map(|side| FillBound::iter().map(move |bound| (side, bound)))
        {
            let (touch, cross, through) = resting_spelling(side, bound);
            let time = format!("{side}_{bound}_time");
            let _ = write!(
                fill_times,
                "min(trade_events.timestamp) FILTER (WHERE trade_events.price {through} spans.{touch}) AS {time}, "
            );
            let _ = write!(
                resting_labels,
                "CASE WHEN {touch} IS NOT NULL THEN {time} IS NOT NULL END AS forward_{side}_filled_{bound},
        epoch_ns({time} - arrival) / 1e9 AS forward_{side}_fill_seconds_{bound},
        CASE WHEN {time} IS NOT NULL THEN {touch} WHEN {touch} IS NOT NULL THEN {cross}_at_window END AS forward_{side}_entry_price_{bound},
        "
            );
            let _ = write!(
                resting_columns,
                "forward_{side}_filled_{bound}, forward_{side}_fill_seconds_{bound}, forward_{side}_entry_price_{bound},
        "
            );
        }
        let fill_times = fill_times.trim_end_matches(", ");
        Ok(format!(
            r#"
{DRAW_ORDER_MACROS}{eligibility}CREATE TEMP TABLE universe AS
    SELECT * FROM (VALUES {members})
        AS members(ticker, dollar_volume_rank, industry_code, central_index_key, industry, draw_seed);
COPY (SELECT '{session}' AS session, ticker, industry_code, central_index_key FROM universe ORDER BY dollar_volume_rank)
    TO 'table/universe_{session}.parquet';
CREATE TEMP TABLE ranks AS SELECT ticker, dollar_volume_rank FROM universe;
CREATE TEMP TABLE bars AS
    SELECT symbol AS ticker, closed_at, close::DOUBLE AS close
    FROM read_parquet([{daily_files}])
    WHERE (symbol IN (SELECT ticker FROM universe) OR symbol = 'SPY') AND closed_at < {open};
CREATE TEMP TABLE previous_close AS SELECT ticker, arg_max(close, closed_at) AS close FROM bars GROUP BY ticker;
CREATE TEMP TABLE daily AS
    WITH returns AS (
        SELECT ticker, (closed_at AT TIME ZONE 'America/New_York')::DATE AS day,
            (lag(closed_at) OVER daily_order AT TIME ZONE 'America/New_York')::DATE AS previous_day,
            close / lag(close) OVER daily_order - 1 AS daily_return,
            row_number() OVER (PARTITION BY ticker ORDER BY closed_at DESC) AS recency
        FROM bars WINDOW daily_order AS (PARTITION BY ticker ORDER BY closed_at)),
    kept AS (SELECT * FROM returns WHERE abs(daily_return) < {DAILY_RETURN_LIMIT}),
    -- Windows over one ordered frame rather than GROUP BY, so floating sums run in the same order on every build.
    own AS (
        SELECT ticker,
            10000 * stddev_samp(daily_return) FILTER (WHERE recency <= 20) OVER whole AS daily_volatility_20,
            10000 * max(daily_return) FILTER (WHERE recency = 1) OVER whole AS previous_return
        FROM kept WHERE ticker <> 'SPY'
        WINDOW whole AS (PARTITION BY ticker ORDER BY day ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
        QUALIFY row_number() OVER whole = 1),
    -- Beta reads only days where the name's and SPY's returns span the same two closes, so a hole in either drops both.
    matched AS (
        SELECT name.ticker, regr_slope(name.daily_return, spy.daily_return) OVER whole AS beta,
            count(*) OVER whole AS beta_days
        FROM kept name JOIN kept spy
          ON spy.ticker = 'SPY' AND spy.day = name.day AND spy.previous_day = name.previous_day
        WHERE name.ticker <> 'SPY'
        WINDOW whole AS (PARTITION BY name.ticker ORDER BY name.day ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
        QUALIFY row_number() OVER whole = 1)
    SELECT own.ticker, matched.beta, own.daily_volatility_20, own.previous_return, coalesce(matched.beta_days, 0) AS beta_days
    FROM own LEFT JOIN matched USING (ticker);
CREATE TEMP TABLE quote_events AS
    WITH quotes AS (
        SELECT ticker, {time} AS timestamp, bid_price::DOUBLE AS bid, ask_price::DOUBLE AS ask,
            bid_size::DOUBLE AS bid_size, ask_size::DOUBLE AS ask_size, (bid_price::DOUBLE + ask_price::DOUBLE) / 2 AS mid
        FROM read_parquet(getvariable('scratch') || '/kind=quotes/provider={provider}/session={session}/data.parquet')
        WHERE (ticker IN (SELECT ticker FROM universe) OR ticker IN ('SPY', 'QQQ'))
          AND bid_price > 0 AND ask_price > bid_price AND bid_size > 0 AND ask_size > 0
          AND {time} >= {open} AND {time} < {close}),
    flowed AS (
        SELECT *, coalesce(
            CASE WHEN bid >= lag(bid) OVER quote_order THEN bid_size ELSE 0 END
            - CASE WHEN bid <= lag(bid) OVER quote_order THEN lag(bid_size) OVER quote_order ELSE 0 END
            - CASE WHEN ask <= lag(ask) OVER quote_order THEN ask_size ELSE 0 END
            + CASE WHEN ask >= lag(ask) OVER quote_order THEN lag(ask_size) OVER quote_order ELSE 0 END, 0) AS flow,
            coalesce(power(ln(mid / lag(mid) OVER quote_order), 2), 0) AS squared_return,
            CASE WHEN mid <> lag(mid) OVER quote_order THEN timestamp END AS mid_changed_at
        FROM quotes WINDOW quote_order AS (PARTITION BY ticker ORDER BY timestamp, bid, ask, bid_size, ask_size))
    SELECT ticker, timestamp, bid, ask, bid_size, ask_size, mid,
        (bid * ask_size + ask * bid_size) / (bid_size + ask_size) AS microprice,
        (bid_size - ask_size) / (bid_size + ask_size) AS quote_imbalance,
        10000 * (ask - bid) / mid AS spread_basis_points,
        row_number() OVER running AS event_number,
        sum(flow) OVER running AS cumulative_flow,
        sum(abs(flow)) OVER running AS cumulative_absolute_flow,
        sum(squared_return) OVER running AS cumulative_squared_return,
        avg(10000 * (ask - bid) / mid) OVER running AS session_mean_spread,
        first_value(mid) OVER running AS first_mid,
        last_value(mid_changed_at IGNORE NULLS) OVER running AS last_mid_change
    FROM flowed
    WINDOW running AS (PARTITION BY ticker ORDER BY timestamp, bid, ask, bid_size, ask_size ROWS UNBOUNDED PRECEDING);
-- The book after the last update of each instant, so a read never depends on which of an instant's ties it lands on.
CREATE TEMP TABLE quote_states AS
    SELECT * FROM quote_events
    QUALIFY row_number() OVER (PARTITION BY ticker, timestamp ORDER BY event_number DESC) = 1;
CREATE TEMP TABLE signed AS
    WITH prints AS (
        SELECT ticker, {time} AS timestamp, price::DOUBLE AS price, size::DOUBLE AS size, {sweep} AS sweep,
            {off_exchange} AS off_exchange, {eligible} AS eligible, (price * 10000)::BIGINT % 100 AS hundredths_of_a_cent
        FROM read_parquet(getvariable('scratch') || '/kind=trades/provider={provider}/session={session}/data.parquet')
        WHERE ticker IN (SELECT ticker FROM universe) AND {not_withdrawn} AND size > 0
          AND {time} >= {open} AND {time} < {close})
    SELECT prints.*, sign(prints.price - quote_states.mid) AS print_sign,
        CASE WHEN off_exchange AND hundredths_of_a_cent {retail_buy} THEN 1
             WHEN off_exchange AND hundredths_of_a_cent {retail_sell} THEN -1 END AS retail_sign
    FROM prints ASOF LEFT JOIN quote_states
      -- Strictly before, unlike every other read: a quote stamped with the print may be the book the print left.
      ON prints.ticker = quote_states.ticker AND prints.timestamp > quote_states.timestamp;
CREATE TEMP TABLE trade_events AS
    SELECT ticker, timestamp, sweep, print_sign, size, price, eligible,
        row_number() OVER print_order AS event_number,
        sum(coalesce(print_sign, 0) * size) OVER print_order AS cumulative_signed,
        sum(size) OVER print_order AS cumulative_volume,
        sum(CASE WHEN sweep THEN coalesce(print_sign, 0) * size ELSE 0 END) OVER print_order AS cumulative_sweep_signed,
        sum(CASE WHEN sweep THEN size ELSE 0 END) OVER print_order AS cumulative_sweep_volume,
        sum(coalesce(retail_sign, 0) * size) OVER print_order AS cumulative_retail_signed,
        sum(CASE WHEN retail_sign IS NOT NULL THEN size ELSE 0 END) OVER print_order AS cumulative_retail_volume,
        sum(CASE WHEN {block} THEN coalesce(print_sign, 0) * size ELSE 0 END) OVER print_order AS cumulative_block_signed,
        sum(CASE WHEN {block} THEN size ELSE 0 END) OVER print_order AS cumulative_block_volume,
        sum(CASE WHEN size < {ROUND_LOT_SHARES} THEN coalesce(print_sign, 0) * size ELSE 0 END) OVER print_order AS cumulative_odd_lot_signed,
        sum(CASE WHEN size < {ROUND_LOT_SHARES} THEN size ELSE 0 END) OVER print_order AS cumulative_odd_lot_volume,
        sum(CASE WHEN off_exchange THEN size ELSE 0 END) OVER print_order AS cumulative_off_exchange_volume,
        sum(CASE WHEN print_sign = 0 THEN 1 ELSE 0 END) OVER print_order AS cumulative_midpoint_count,
        sum(price * size) OVER print_order AS cumulative_dollars,
        max(CASE WHEN eligible THEN price END) OVER print_order AS session_high,
        min(CASE WHEN eligible THEN price END) OVER print_order AS session_low,
        last_value(CASE WHEN sweep AND print_sign <> 0 THEN timestamp END IGNORE NULLS) OVER print_order AS last_sweep_time,
        last_value(CASE WHEN sweep AND print_sign <> 0 THEN print_sign END IGNORE NULLS) OVER print_order AS last_sweep_sign
    FROM signed
    WINDOW print_order AS (
        PARTITION BY ticker ORDER BY timestamp, price, size, sweep, off_exchange, eligible ROWS UNBOUNDED PRECEDING);
CREATE TEMP TABLE trade_states AS
    SELECT * FROM trade_events
    QUALIFY row_number() OVER (PARTITION BY ticker, timestamp ORDER BY event_number DESC) = 1;
-- Universe-wide signed and total dollars at each second's end, cumulative, so any window is a difference of two reads.
CREATE TEMP TABLE market_flow AS
    WITH running AS (
        SELECT time_bucket(INTERVAL 1 SECOND, timestamp) + INTERVAL 1 SECOND AS timestamp,
            row_number() OVER market_order AS market_number,
            sum(coalesce(print_sign, 0) * price * size) OVER market_order AS cumulative_signed_dollars,
            sum(price * size) OVER market_order AS cumulative_dollars
        FROM signed
        WINDOW market_order AS (
            ORDER BY timestamp, ticker, price, size, sweep, off_exchange, eligible ROWS UNBOUNDED PRECEDING))
    SELECT timestamp, arg_max(cumulative_signed_dollars, market_number) AS cumulative_signed_dollars,
        arg_max(cumulative_dollars, market_number) AS cumulative_dollars
    FROM running GROUP BY timestamp;
{entries}-- Every instant a quote is read at, for the name, SPY and QQQ, in one ASOF join.
CREATE TEMP TABLE quote_probes AS
    WITH sources AS (
        SELECT entries.*, sources.source, CASE sources.source WHEN 'own' THEN entries.ticker ELSE sources.source END AS read_ticker
        FROM entries, (VALUES ('own'), ('SPY'), ('QQQ')) AS sources(source)),
    offsets AS (
        SELECT * FROM (VALUES {forward_offsets}
            ('own', 'now', 0), ('own', 'back_5', -5), ('own', 'back_30', -30), ('own', 'back_60', -60), ('own', 'back_120', -120),
            ('own', 'back_300', -300), ('own', 'back_900', -900), ('own', 'back_1800', -1800),
            ('SPY', 'now', 0), ('SPY', 'back_5', -5), ('SPY', 'back_30', -30), ('SPY', 'back_120', -120), ('SPY', 'back_1800', -1800),
            ('QQQ', 'now', 0), ('QQQ', 'back_5', -5), ('QQQ', 'back_30', -30), ('QQQ', 'back_120', -120)
        ) AS offsets(source, probe, seconds))
    SELECT sources.ticker, kind, key, arrival, sources.source, read_ticker, probe, arrival + to_seconds(seconds) AS probe_time
    FROM sources JOIN offsets USING (source)
    WHERE arrival + to_seconds(seconds) <= {last_read}
    UNION ALL
    SELECT ticker, kind, key, arrival, source, read_ticker, 'forward_close', {last_read}
    FROM sources WHERE source IN ('own', 'SPY');
CREATE TEMP TABLE quote_probed AS
    SELECT quote_probes.*, quote_states.* EXCLUDE (ticker)
    FROM quote_probes ASOF LEFT JOIN quote_states
      ON quote_probes.read_ticker = quote_states.ticker AND quote_probes.probe_time >= quote_states.timestamp;
CREATE TEMP TABLE quote_events_back AS
    SELECT now.ticker, now.kind, now.key, windows.window_length,
        quote_events.cumulative_flow, quote_events.cumulative_absolute_flow
    FROM (SELECT * FROM quote_probed WHERE source = 'own' AND probe = 'now') now
    CROSS JOIN (VALUES (10), (50), (250)) AS windows(window_length)
    JOIN quote_events ON quote_events.ticker = now.ticker AND quote_events.event_number = now.event_number - windows.window_length;
CREATE TEMP TABLE trade_probes AS
    SELECT ticker, kind, key, arrival, probe, arrival + to_seconds(seconds) AS probe_time
    FROM entries, (VALUES ('now', 0), ('back_5', -5), ('back_30', -30), ('back_120', -120)) AS offsets(probe, seconds);
CREATE TEMP TABLE trade_probed AS
    SELECT trade_probes.*, trade_states.* EXCLUDE (ticker)
    FROM trade_probes ASOF LEFT JOIN trade_states
      ON trade_probes.ticker = trade_states.ticker AND trade_probes.probe_time >= trade_states.timestamp;
CREATE TEMP TABLE trade_events_back AS
    SELECT now.ticker, now.kind, now.key, windows.window_length, trade_events.* EXCLUDE (ticker, sweep, print_sign, size, price, eligible)
    FROM (SELECT * FROM trade_probed WHERE probe = 'now') now
    CROSS JOIN (VALUES (10), (50), (250)) AS windows(window_length)
    JOIN trade_events ON trade_events.ticker = now.ticker AND trade_events.event_number = now.event_number - windows.window_length;
CREATE TEMP TABLE market_probed AS
    WITH probes AS (
        SELECT ticker, kind, key, probe, arrival + to_seconds(seconds) AS probe_time
        FROM entries, (VALUES ('now', 0), ('back_30', -30)) AS offsets(probe, seconds))
    SELECT probes.*, market_flow.cumulative_signed_dollars, market_flow.cumulative_dollars
    FROM probes ASOF LEFT JOIN market_flow ON probes.probe_time >= market_flow.timestamp;
-- Each side rests at the touch in force at arrival, filled by the first eligible print after it through its limit
-- (conservative) or at it (optimistic); no such print is unfilled, while no touch or a window past the last read is
-- unknown. An unfilled buy's entry is the ask at the window's end, and a sell's the bid.
CREATE TEMP TABLE resting AS
    WITH touched AS (
        SELECT entries.ticker, entries.kind, entries.key, entries.arrival, quote_states.bid, quote_states.ask
        FROM entries ASOF LEFT JOIN quote_states
          ON entries.ticker = quote_states.ticker AND entries.arrival >= quote_states.timestamp
        WHERE entries.arrival + INTERVAL {resting} SECOND <= {last_read}),
    started AS (
        SELECT touched.*, start_print.event_number AS start_event
        FROM touched ASOF LEFT JOIN trade_states start_print
          ON touched.ticker = start_print.ticker AND touched.arrival >= start_print.timestamp),
    ended AS (
        SELECT started.*, end_print.event_number AS end_event
        FROM started ASOF LEFT JOIN trade_states end_print
          ON started.ticker = end_print.ticker AND started.arrival + INTERVAL {resting} SECOND >= end_print.timestamp),
    windowed AS (
        SELECT ended.*, window_quote.bid AS bid_at_window, window_quote.ask AS ask_at_window
        FROM ended ASOF LEFT JOIN quote_states window_quote
          ON ended.ticker = window_quote.ticker AND ended.arrival + INTERVAL {resting} SECOND >= window_quote.timestamp),
    spans AS (
        SELECT ticker, kind, key, bid, ask, unnest(range(coalesce(start_event, 0) + 1, end_event + 1)) AS event_number
        FROM windowed WHERE bid IS NOT NULL AND end_event > coalesce(start_event, 0)),
    fills AS (
        SELECT spans.ticker, spans.kind, spans.key, {fill_times}
        FROM spans JOIN trade_events
          ON trade_events.ticker = spans.ticker AND trade_events.event_number = spans.event_number AND trade_events.eligible
        GROUP BY spans.ticker, spans.kind, spans.key)
    SELECT windowed.ticker, windowed.kind, windowed.key, {resting_labels}
        bid_at_window AS forward_bid_at_window, ask_at_window AS forward_ask_at_window
    FROM windowed LEFT JOIN fills USING (ticker, kind, key);
-- Pivots: one column per (source, probe, field) a feature reads, so the features below are plain arithmetic.
CREATE TEMP TABLE quotes_wide AS
    SELECT ticker, kind, key, any_value(arrival) AS arrival,
        max(mid) FILTER (WHERE source = 'own' AND probe = 'now') AS mid,
        max(bid) FILTER (WHERE source = 'own' AND probe = 'now') AS bid,
        max(ask) FILTER (WHERE source = 'own' AND probe = 'now') AS ask,
        max(bid_size) FILTER (WHERE source = 'own' AND probe = 'now') AS bid_size,
        max(ask_size) FILTER (WHERE source = 'own' AND probe = 'now') AS ask_size,
        max(microprice) FILTER (WHERE source = 'own' AND probe = 'now') AS microprice,
        max(quote_imbalance) FILTER (WHERE source = 'own' AND probe = 'now') AS quote_imbalance,
        max(spread_basis_points) FILTER (WHERE source = 'own' AND probe = 'now') AS spread_basis_points,
        max(session_mean_spread) FILTER (WHERE source = 'own' AND probe = 'now') AS session_mean_spread,
        max(first_mid) FILTER (WHERE source = 'own' AND probe = 'now') AS first_mid,
        max(last_mid_change) FILTER (WHERE source = 'own' AND probe = 'now') AS last_mid_change,
        max(timestamp) FILTER (WHERE source = 'own' AND probe = 'now') AS quote_time,
        max(event_number) FILTER (WHERE source = 'own' AND probe = 'now') AS quote_number,
        max(event_number) FILTER (WHERE source = 'own' AND probe = 'back_30') AS quote_number_30,
        max(cumulative_squared_return) FILTER (WHERE source = 'own' AND probe = 'now') AS squared_now,
        max(cumulative_squared_return) FILTER (WHERE source = 'own' AND probe = 'back_60') AS squared_60,
        max(cumulative_squared_return) FILTER (WHERE source = 'own' AND probe = 'back_300') AS squared_300,
        max(cumulative_squared_return) FILTER (WHERE source = 'own' AND probe = 'back_1800') AS squared_1800,
        max(cumulative_flow) FILTER (WHERE source = 'own' AND probe = 'now') AS flow_now,
        max(cumulative_absolute_flow) FILTER (WHERE source = 'own' AND probe = 'now') AS absolute_flow_now,
        max(cumulative_flow) FILTER (WHERE source = 'own' AND probe = 'back_5') AS flow_5,
        max(cumulative_absolute_flow) FILTER (WHERE source = 'own' AND probe = 'back_5') AS absolute_flow_5,
        max(cumulative_flow) FILTER (WHERE source = 'own' AND probe = 'back_30') AS flow_30,
        max(cumulative_absolute_flow) FILTER (WHERE source = 'own' AND probe = 'back_30') AS absolute_flow_30,
        max(cumulative_flow) FILTER (WHERE source = 'own' AND probe = 'back_120') AS flow_120,
        max(cumulative_absolute_flow) FILTER (WHERE source = 'own' AND probe = 'back_120') AS absolute_flow_120,
        max(mid) FILTER (WHERE source = 'own' AND probe = 'back_60') AS mid_60,
        max(mid) FILTER (WHERE source = 'own' AND probe = 'back_300') AS mid_300,
        max(mid) FILTER (WHERE source = 'own' AND probe = 'back_900') AS mid_900,
        max(mid) FILTER (WHERE source = 'own' AND probe = 'back_1800') AS mid_1800,
        {forward_pivots}max(mid) FILTER (WHERE source = 'own' AND probe = 'forward_close') AS mid_forward_close,
        max(spread_basis_points) FILTER (WHERE source = 'own' AND probe = 'forward_close') AS spread_forward_close,
        max(mid) FILTER (WHERE source = 'SPY' AND probe = 'now') AS spy_mid,
        max(mid) FILTER (WHERE source = 'SPY' AND probe = 'back_1800') AS spy_mid_1800,
        max(mid) FILTER (WHERE source = 'SPY' AND probe = 'forward_close') AS spy_forward_close,
        max(microprice) FILTER (WHERE source = 'SPY' AND probe = 'now') AS spy_microprice,
        max(microprice) FILTER (WHERE source = 'SPY' AND probe = 'back_5') AS spy_microprice_5,
        max(microprice) FILTER (WHERE source = 'SPY' AND probe = 'back_30') AS spy_microprice_30,
        max(microprice) FILTER (WHERE source = 'SPY' AND probe = 'back_120') AS spy_microprice_120,
        max(cumulative_flow) FILTER (WHERE source = 'SPY' AND probe = 'now') AS spy_flow_now,
        max(cumulative_absolute_flow) FILTER (WHERE source = 'SPY' AND probe = 'now') AS spy_absolute_flow_now,
        max(cumulative_flow) FILTER (WHERE source = 'SPY' AND probe = 'back_30') AS spy_flow_30,
        max(cumulative_absolute_flow) FILTER (WHERE source = 'SPY' AND probe = 'back_30') AS spy_absolute_flow_30,
        max(microprice) FILTER (WHERE source = 'QQQ' AND probe = 'now') AS qqq_microprice,
        max(microprice) FILTER (WHERE source = 'QQQ' AND probe = 'back_5') AS qqq_microprice_5,
        max(microprice) FILTER (WHERE source = 'QQQ' AND probe = 'back_30') AS qqq_microprice_30,
        max(microprice) FILTER (WHERE source = 'QQQ' AND probe = 'back_120') AS qqq_microprice_120
    FROM quote_probed GROUP BY ticker, kind, key;
CREATE TEMP TABLE trades_wide AS
    SELECT now.ticker, now.kind, now.key, now.event_number AS print_number, now.cumulative_volume AS volume_now,
        now.cumulative_dollars AS dollars_now, now.session_high, now.session_low, now.last_sweep_time, now.last_sweep_sign,
        now.cumulative_signed AS signed_now, now.cumulative_sweep_signed AS sweep_signed_now,
        now.cumulative_sweep_volume AS sweep_volume_now,
        now.cumulative_retail_signed AS retail_signed_now, now.cumulative_retail_volume AS retail_volume_now,
        now.cumulative_block_signed AS block_signed_now, now.cumulative_block_volume AS block_volume_now,
        now.cumulative_odd_lot_signed AS odd_lot_signed_now, now.cumulative_odd_lot_volume AS odd_lot_volume_now,
        now.cumulative_off_exchange_volume AS off_exchange_now, now.cumulative_midpoint_count AS midpoint_now,
        seconds_5.event_number AS print_number_seconds_5, seconds_5.cumulative_signed AS signed_seconds_5, seconds_5.cumulative_volume AS volume_seconds_5,
        seconds_5.cumulative_sweep_signed AS sweep_signed_seconds_5, seconds_5.cumulative_sweep_volume AS sweep_volume_seconds_5,
        seconds_30.event_number AS print_number_seconds_30, seconds_30.cumulative_signed AS signed_seconds_30, seconds_30.cumulative_volume AS volume_seconds_30,
        seconds_30.cumulative_sweep_signed AS sweep_signed_seconds_30, seconds_30.cumulative_sweep_volume AS sweep_volume_seconds_30,
        seconds_120.event_number AS print_number_seconds_120, seconds_120.cumulative_signed AS signed_seconds_120, seconds_120.cumulative_volume AS volume_seconds_120,
        seconds_120.cumulative_sweep_signed AS sweep_signed_seconds_120, seconds_120.cumulative_sweep_volume AS sweep_volume_seconds_120,
        seconds_120.cumulative_block_signed AS block_signed_seconds_120, seconds_120.cumulative_block_volume AS block_volume_seconds_120,
        events_10.cumulative_signed AS signed_events_10, events_10.cumulative_volume AS volume_events_10,
        events_10.cumulative_sweep_signed AS sweep_signed_events_10, events_10.cumulative_sweep_volume AS sweep_volume_events_10,
        events_50.cumulative_signed AS signed_events_50, events_50.cumulative_volume AS volume_events_50,
        events_50.cumulative_sweep_signed AS sweep_signed_events_50, events_50.cumulative_sweep_volume AS sweep_volume_events_50,
        events_50.cumulative_retail_signed AS retail_signed_events_50, events_50.cumulative_retail_volume AS retail_volume_events_50,
        events_50.cumulative_odd_lot_signed AS odd_lot_signed_events_50, events_50.cumulative_odd_lot_volume AS odd_lot_volume_events_50,
        events_250.cumulative_signed AS signed_events_250, events_250.cumulative_volume AS volume_events_250,
        events_250.cumulative_sweep_signed AS sweep_signed_events_250, events_250.cumulative_sweep_volume AS sweep_volume_events_250,
        events_250.cumulative_retail_volume AS retail_volume_events_250, events_250.cumulative_off_exchange_volume AS off_exchange_events_250,
        events_250.cumulative_midpoint_count AS midpoint_events_250
    FROM (SELECT * FROM trade_probed WHERE probe = 'now') now
    LEFT JOIN (SELECT * FROM trade_probed WHERE probe = 'back_5') seconds_5 USING (ticker, kind, key)
    LEFT JOIN (SELECT * FROM trade_probed WHERE probe = 'back_30') seconds_30 USING (ticker, kind, key)
    LEFT JOIN (SELECT * FROM trade_probed WHERE probe = 'back_120') seconds_120 USING (ticker, kind, key)
    LEFT JOIN (SELECT * FROM trade_events_back WHERE window_length = 10) events_10 USING (ticker, kind, key)
    LEFT JOIN (SELECT * FROM trade_events_back WHERE window_length = 50) events_50 USING (ticker, kind, key)
    LEFT JOIN (SELECT * FROM trade_events_back WHERE window_length = 250) events_250 USING (ticker, kind, key);
COPY (
    WITH joined AS (
        SELECT quotes_wide.*, trades_wide.* EXCLUDE (ticker, kind, key),
            quote_10.cumulative_flow AS flow_events_10, quote_10.cumulative_absolute_flow AS absolute_flow_events_10,
            quote_50.cumulative_flow AS flow_events_50, quote_50.cumulative_absolute_flow AS absolute_flow_events_50,
            quote_250.cumulative_flow AS flow_events_250, quote_250.cumulative_absolute_flow AS absolute_flow_events_250,
            market_now.cumulative_signed_dollars - coalesce(market_30.cumulative_signed_dollars, 0) AS market_signed_30,
            market_now.cumulative_dollars - coalesce(market_30.cumulative_dollars, 0) AS market_dollars_30,
            ranks.dollar_volume_rank, previous_close.close AS previous_close,
            daily.beta, daily.daily_volatility_20, daily.previous_return, daily.beta_days, universe.industry,
            resting.* EXCLUDE (ticker, kind, key),
            epoch_ns(quotes_wide.arrival - {open}) / 6e10 AS minutes_since_open
        FROM quotes_wide JOIN trades_wide USING (ticker, kind, key)
        LEFT JOIN (SELECT * FROM quote_events_back WHERE window_length = 10) quote_10 USING (ticker, kind, key)
        LEFT JOIN (SELECT * FROM quote_events_back WHERE window_length = 50) quote_50 USING (ticker, kind, key)
        LEFT JOIN (SELECT * FROM quote_events_back WHERE window_length = 250) quote_250 USING (ticker, kind, key)
        LEFT JOIN (SELECT * FROM market_probed WHERE probe = 'now') market_now USING (ticker, kind, key)
        LEFT JOIN (SELECT * FROM market_probed WHERE probe = 'back_30') market_30 USING (ticker, kind, key)
        LEFT JOIN ranks USING (ticker) LEFT JOIN previous_close USING (ticker)
        LEFT JOIN daily USING (ticker) LEFT JOIN universe USING (ticker)
        LEFT JOIN resting USING (ticker, kind, key))
    SELECT '{provider}' AS provider, '{session}' AS session, ticker, kind, key, arrival,
        -- book
        quote_imbalance,
        10000 * (microprice - mid) / mid AS microprice_offset,
        spread_basis_points,
        spread_basis_points / nullif(session_mean_spread, 0) AS spread_relative,
        (round(ask - bid, 4) <= 0.01)::INTEGER AS one_tick_spread,
        ((bid_size + ask_size) / 2) / nullif(volume_now / nullif(print_number, 0), 0) AS touch_depth,
        epoch_ns(arrival - last_mid_change) / 1e9 AS seconds_since_mid_change,
        epoch_ns(arrival - quote_time) / 1e9 AS screen_quote_age, session_mean_spread AS screen_trailing_spread,
        bid AS screen_bid, ask AS screen_ask, bid_size AS screen_bid_size, ask_size AS screen_ask_size,
        {forward_spreads}spread_forward_close AS forward_spread_close,
        (quote_number - coalesce(quote_number_30, 0)) / 30.0 AS quote_rate,
        -- flow
        (flow_now - coalesce(flow_5, 0)) / nullif(absolute_flow_now - coalesce(absolute_flow_5, 0), 0) AS order_flow_seconds_5,
        (flow_now - coalesce(flow_30, 0)) / nullif(absolute_flow_now - coalesce(absolute_flow_30, 0), 0) AS order_flow_seconds_30,
        (flow_now - coalesce(flow_120, 0)) / nullif(absolute_flow_now - coalesce(absolute_flow_120, 0), 0) AS order_flow_seconds_120,
        (flow_now - flow_events_10) / nullif(absolute_flow_now - absolute_flow_events_10, 0) AS order_flow_events_10,
        (flow_now - flow_events_50) / nullif(absolute_flow_now - absolute_flow_events_50, 0) AS order_flow_events_50,
        (flow_now - flow_events_250) / nullif(absolute_flow_now - absolute_flow_events_250, 0) AS order_flow_events_250,
        (signed_now - coalesce(signed_seconds_5, 0)) / nullif(volume_now - coalesce(volume_seconds_5, 0), 0) AS trade_sign_seconds_5,
        (signed_now - coalesce(signed_seconds_30, 0)) / nullif(volume_now - coalesce(volume_seconds_30, 0), 0) AS trade_sign_seconds_30,
        (signed_now - coalesce(signed_seconds_120, 0)) / nullif(volume_now - coalesce(volume_seconds_120, 0), 0) AS trade_sign_seconds_120,
        (signed_now - signed_events_10) / nullif(volume_now - volume_events_10, 0) AS trade_sign_events_10,
        (signed_now - signed_events_50) / nullif(volume_now - volume_events_50, 0) AS trade_sign_events_50,
        (signed_now - signed_events_250) / nullif(volume_now - volume_events_250, 0) AS trade_sign_events_250,
        (sweep_signed_now - coalesce(sweep_signed_seconds_5, 0)) / nullif(sweep_volume_now - coalesce(sweep_volume_seconds_5, 0), 0) AS sweep_seconds_5,
        (sweep_signed_now - coalesce(sweep_signed_seconds_30, 0)) / nullif(sweep_volume_now - coalesce(sweep_volume_seconds_30, 0), 0) AS sweep_seconds_30,
        (sweep_signed_now - coalesce(sweep_signed_seconds_120, 0)) / nullif(sweep_volume_now - coalesce(sweep_volume_seconds_120, 0), 0) AS sweep_seconds_120,
        (sweep_signed_now - sweep_signed_events_10) / nullif(sweep_volume_now - sweep_volume_events_10, 0) AS sweep_events_10,
        (sweep_signed_now - sweep_signed_events_50) / nullif(sweep_volume_now - sweep_volume_events_50, 0) AS sweep_events_50,
        (sweep_signed_now - sweep_signed_events_250) / nullif(sweep_volume_now - sweep_volume_events_250, 0) AS sweep_events_250,
        epoch_ns(arrival - last_sweep_time) / 1e9 AS seconds_since_sweep,
        last_sweep_sign AS last_sweep_side,
        (block_signed_now - coalesce(block_signed_seconds_120, 0)) / nullif(block_volume_now - coalesce(block_volume_seconds_120, 0), 0) AS block_seconds_120,
        (odd_lot_signed_now - odd_lot_signed_events_50) / nullif(odd_lot_volume_now - odd_lot_volume_events_50, 0) AS odd_lot_events_50,
        (retail_signed_now - retail_signed_events_50) / nullif(retail_volume_now - retail_volume_events_50, 0) AS retail_events_50,
        (retail_volume_now - retail_volume_events_250) / nullif(volume_now - volume_events_250, 0) AS retail_share_events_250,
        (off_exchange_now - off_exchange_events_250) / nullif(volume_now - volume_events_250, 0) AS off_exchange_share_events_250,
        (midpoint_now - midpoint_events_250) / 250.0 AS midpoint_share_events_250,
        ((volume_now - volume_events_50) / 50.0) / nullif(volume_now / nullif(print_number, 0), 0) AS print_size_relative,
        -- activity
        ((print_number - coalesce(print_number_seconds_30, 0)) / 30.0) / nullif(print_number / nullif(minutes_since_open * 60, 0), 0) AS intensity_seconds_30,
        ((print_number - coalesce(print_number_seconds_120, 0)) / 120.0) / nullif(print_number / nullif(minutes_since_open * 60, 0), 0) AS intensity_seconds_120,
        10000 * sqrt(greatest(squared_now - coalesce(squared_60, 0), 0)) AS volatility_60,
        10000 * sqrt(greatest(squared_now - coalesce(squared_300, 0), 0)) AS volatility_300,
        10000 * sqrt(greatest(squared_now - coalesce(squared_1800, 0), 0)) AS volatility_1800,
        10000 * sqrt(squared_now / nullif(minutes_since_open, 0)) AS volatility_per_minute,
        -- cross-name
        10000 * (spy_microprice / spy_microprice_5 - 1) AS spy_lead_seconds_5,
        10000 * (spy_microprice / spy_microprice_30 - 1) AS spy_lead_seconds_30,
        10000 * (spy_microprice / spy_microprice_120 - 1) AS spy_lead_seconds_120,
        10000 * (qqq_microprice / qqq_microprice_5 - 1) AS qqq_lead_seconds_5,
        10000 * (qqq_microprice / qqq_microprice_30 - 1) AS qqq_lead_seconds_30,
        10000 * (qqq_microprice / qqq_microprice_120 - 1) AS qqq_lead_seconds_120,
        (spy_flow_now - spy_flow_30) / nullif(spy_absolute_flow_now - spy_absolute_flow_30, 0) AS spy_order_flow_seconds_30,
        market_signed_30 / nullif(market_dollars_30, 0) AS market_trade_sign_seconds_30,
        10000 * (mid / mid_1800 - 1) - 10000 * (spy_mid / spy_mid_1800 - 1) AS residual_return_1800,
        -- context
        minutes_since_open,
        {session_minutes} - minutes_since_open AS minutes_to_close,
        dollar_volume_rank,
        ln(mid) AS log_mid,
        10000 * (mid / mid_60 - 1) AS return_60,
        10000 * (mid / mid_300 - 1) AS return_300,
        10000 * (mid / mid_900 - 1) AS return_900,
        10000 * (mid / mid_1800 - 1) AS return_1800,
        10000 * (mid / first_mid - 1) AS return_since_open,
        10000 * (mid / nullif(dollars_now / nullif(volume_now, 0), 0) - 1) AS vwap_distance,
        (mid - session_low) / nullif(session_high - session_low, 0) AS range_position,
        10000 * (first_mid / previous_close - 1) AS overnight_gap,
        -- daily
        beta, daily_volatility_20, previous_return, beta_days, industry,
        -- targets
        {raw}10000 * (mid_forward_close / mid - 1) AS raw_close,
        {net}raw_close - 10000 * (spy_forward_close / spy_mid - 1) AS net_close,
        {forward_spy}10000 * (spy_forward_close / spy_mid - 1) AS forward_spy_close,
        volatility_per_minute AS scale_per_root_minute,
        -- resting labels
        {resting_columns}forward_bid_at_window, forward_ask_at_window
    FROM joined ORDER BY ticker, key
) TO 'table/rows_{provider}_{session}.parquet';
"#
        ))
    }
}

/// The quote a resting `side` order stands at, the one it crosses to unfilled, and the comparison of a print to its
/// limit that fills it under `bound`, spelling `FillBound::fills` in SQL.
fn resting_spelling(side: Side, bound: FillBound) -> (&'static str, &'static str, &'static str) {
    let (touch, cross) = match side {
        Side::Buy => ("bid", "ask"),
        Side::Sell => ("ask", "bid"),
    };
    let through = match (side, bound) {
        (Side::Buy, FillBound::Conservative) => "<",
        (Side::Buy, FillBound::Optimistic) => "<=",
        (Side::Sell, FillBound::Conservative) => ">",
        (Side::Sell, FillBound::Optimistic) => ">=",
    };
    (touch, cross, through)
}

/// `instant` as a DuckDB timestamp literal, read as UTC under the preamble's time zone.
fn timestamp(instant: DateTime<Utc>) -> String {
    format!("TIMESTAMP '{}'", instant.format("%Y-%m-%d %H:%M:%S"))
}

/// Each member as a row of the universe's VALUES: ticker, rank, industry code, central index key, industry number and
/// the seed of its draw under `clock`.
fn members(universe: &Universe, clock: EventClock) -> String {
    let optional = |value: Option<String>, kind: &str| {
        format!("{}::{kind}", value.unwrap_or_else(|| "NULL".into()))
    };
    universe
        .members()
        .iter()
        .fold(String::new(), |mut rows, member| {
            let separator = match rows.is_empty() {
                true => "",
                false => ", ",
            };
            let _ = write!(
                rows,
                "{separator}('{}', {}, {}, {}, {}, {}::UINTEGER)",
                member.symbol().as_str(),
                member.rank(),
                optional(
                    member.industry_code().map(|code| code.code().to_string()),
                    "SMALLINT"
                ),
                optional(
                    member
                        .central_index_key()
                        .map(|key| key.value().to_string()),
                    "BIGINT"
                ),
                industry_number(member.industry()),
                draw_seed(member.symbol(), universe.session(), clock),
            );
            rows
        })
}

/// Ken French's number for `industry`, 1 to 12 in the order he lists them.
fn industry_number(industry: Industry) -> usize {
    Industry::iter()
        .position(|listed| listed == industry)
        .expect("every industry is listed")
        + 1
}

/// The trading days before `session`, newest first, whose `provider` daily bars the table reads: back to the lookback's
/// start or the first day `daily_bars` lacks, provided no lacked day falls between two held ones.
fn daily_lookback(
    calendar: &TradingCalendar,
    session: SessionDate,
    provider: TickProvider,
    daily_bars: &[TradesKey],
) -> Result<Vec<SessionDate>, EventTableRefusal> {
    let held: BTreeSet<SessionDate> = daily_bars
        .iter()
        .filter(|key| {
            key.provider() == provider.provider() && key.interval() == BarInterval::OneDay
        })
        .map(TradesKey::session)
        .collect();
    let days: Vec<SessionDate> =
        std::iter::successors(calendar.previous_trading_day(session), |day| {
            calendar.previous_trading_day(*day)
        })
        .take(DAILY_LOOKBACK)
        .collect();
    let run = days.iter().take_while(|day| held.contains(day)).count();
    let (read, rest) = days.split_at(run);
    match (read.is_empty(), rest.first()) {
        (true, None) => Err(EventTableRefusal::NoEarlierTradingDay { session }),
        (true, Some(&absent)) => Err(EventTableRefusal::DailyBarsAbsent { provider, absent }),
        (false, Some(&absent)) if rest.iter().any(|day| held.contains(day)) => {
            Err(EventTableRefusal::DailyBarsAbsent { provider, absent })
        }
        (false, None | Some(_)) => Ok(read.to_vec()),
    }
}

#[cfg(test)]
mod tests {
    use chrono::{Datelike, Months, NaiveDate, NaiveTime};

    use super::*;
    use crate::common::laboratory::universe::{UniverseSize, universe};
    use crate::common::market::aggregate::TradeTotals;
    use crate::common::market::security_details::{
        CentralIndexKey, DetailsSnapshot, IndustryCode, SecurityDetails, SecurityType,
    };
    use crate::common::market::trade_bars::{TradeBar, TradeSums};
    use crate::common::market::{DollarVolume, Price, Shares, TradeCount};
    use crate::common::time::SessionRange;
    use crate::common::time::calendar::TradingSession;

    fn date(year: i32, month: u32, day: u32) -> SessionDate {
        SessionDate::from_date(NaiveDate::from_ymd_opt(year, month, day).unwrap())
    }

    /// Every weekday of 2025 and 2026 trading 09:30 to 16:00 Eastern, but `half_day`, closing at 13:00.
    fn calendar(half_day: SessionDate) -> TradingCalendar {
        let at = |hour, minute| NaiveTime::from_hms_opt(hour, minute, 0).unwrap();
        let (first, last) = (date(2025, 1, 1), date(2026, 12, 31));
        let sessions = std::iter::successors(Some(first), |day| Some(day.plus_calendar_days(1)))
            .take_while(|day| *day <= last)
            .filter(|day| !day.is_weekend())
            .map(|day| {
                let close = match day == half_day {
                    true => at(13, 0),
                    false => at(16, 0),
                };
                TradingSession::new(day, at(9, 30), close).unwrap()
            })
            .collect();
        TradingCalendar::new(sessions, SessionRange::new(first, last).unwrap()).unwrap()
    }

    /// `provider`'s daily bars of every trading day in `calendar` from `first` to the day before `session`.
    fn held(
        calendar: &TradingCalendar,
        provider: Provider,
        first: SessionDate,
        session: SessionDate,
    ) -> Vec<TradesKey> {
        calendar
            .trading_days_in_range(
                SessionRange::new(first, session.plus_calendar_days(-1)).unwrap(),
            )
            .into_iter()
            .map(|day| TradesKey::new(provider, BarInterval::OneDay, day))
            .collect()
    }

    /// `day`'s universe of AAA, the larger, filed under a central index key, and BBB, without industry or key.
    fn named(day: SessionDate) -> Universe {
        let month = day.date().with_day(1).unwrap() - Months::new(1);
        let bar = |raw: &str, shares: u64| {
            let shares = Shares::whole(shares).unwrap();
            TradeBar::new(
                Symbol::new(raw).unwrap(),
                BarInterval::OneDay,
                SessionDate::from_date(month.with_day(2).unwrap()).regular_close(),
                TradeSums::new(
                    TradeTotals::new(
                        TradeCount::new(1),
                        shares,
                        DollarVolume::of(Price::from_dollars(10.0).unwrap(), shares),
                    ),
                    None,
                    None,
                ),
            )
            .unwrap()
        };
        let details = vec![
            SecurityDetails::new(
                Symbol::new("AAA").unwrap(),
                Some(SecurityType::CommonStock),
                Some(IndustryCode::from_code(3571).unwrap()),
                None,
                None,
                None,
                None,
                Some(CentralIndexKey::new(320_193).unwrap()),
            ),
            SecurityDetails::new(
                Symbol::new("BBB").unwrap(),
                Some(SecurityType::CommonStock),
                None,
                None,
                None,
                None,
                None,
                None,
            ),
        ];
        universe(
            day,
            &[bar("AAA", 200), bar("BBB", 100)],
            &DetailsSnapshot::new(day.plus_calendar_days(-1), details),
            |_, _| true,
            UniverseSize::new(NonZeroU16::new(10).unwrap()),
        )
        .unwrap()
    }

    fn table(provider: TickProvider, end: u16) -> EventTable {
        EventTable::new(
            provider,
            EventClock::QuoteUpdates,
            ReadDelay::from_milliseconds(1000),
            Draws::new(NonZeroU16::new(5).unwrap()),
            EntryWindow::new(15, end).unwrap(),
        )
    }

    /// Each provider's ticks and daily bars are read under its own spelling and path, and nothing is left
    /// unsubstituted.
    #[test]
    fn test_each_provider_reads_its_own_ticks_and_bars_in_its_own_spelling() {
        let day = date(2025, 6, 16);
        let calendar = calendar(date(2025, 11, 28));
        for (provider, path, spelled, foreign) in [
            (
                TickProvider::Massive,
                "/kind=quotes/provider=massive/session=2025-06-16/",
                "sip_timestamp AS timestamp",
                "\"update\"",
            ),
            (
                TickProvider::Alpaca,
                "/kind=trades/provider=alpaca/session=2025-06-16/",
                "eligible_letters(conditions, tape) AS eligible",
                "sip_timestamp",
            ),
        ] {
            let bars = held(&calendar, provider.provider(), date(2025, 1, 1), day);
            let statements = table(provider, 360)
                .statements(&calendar, &named(day), &bars, date(2025, 6, 13))
                .unwrap();
            assert!(statements.contains(path), "{provider}");
            assert!(statements.contains(spelled), "{provider}");
            assert!(statements.contains(&format!(
                "/data/equity/stage=parsed/trades/provider={provider}/interval=one_day/year=2025/month=06/day=13/data.parquet"
            )));
            assert!(!statements.contains(foreign), "{provider}");
            assert!(!statements.contains(['{', '}']), "{provider}");
            assert!(
                statements.contains(&format!("TO 'table/rows_{provider}_2025-06-16.parquet'")),
                "{provider}"
            );
        }
    }

    /// The session's own hours bound the table, so a half-day closing at 13:00 Eastern counts 210 minutes and refuses a
    /// window reaching past them, and a day the calendar does not trade is refused.
    #[test]
    fn test_a_half_day_bounds_the_window_by_its_own_close() {
        let day = date(2025, 11, 28);
        let calendar = calendar(day);
        let bars = held(&calendar, Provider::Massive, date(2025, 1, 1), day);
        let conditions = date(2025, 11, 27);
        assert_eq!(
            table(TickProvider::Massive, 360).statements(&calendar, &named(day), &bars, conditions),
            Err(EventTableRefusal::WindowPastClose {
                session: day,
                end: 360,
                session_minutes: 210,
            })
        );
        let statements = table(TickProvider::Massive, 210)
            .statements(&calendar, &named(day), &bars, conditions)
            .unwrap();
        assert!(statements.contains("TIMESTAMP '2025-11-28 14:30:00' + INTERVAL 210 MINUTE"));
        assert!(statements.contains("< TIMESTAMP '2025-11-28 18:00:00'"));
        assert!(statements.contains("210 - minutes_since_open AS minutes_to_close"));
        assert!(statements.contains("timestamp + INTERVAL 1000 MILLISECOND AS arrival"));
        let saturday = date(2025, 11, 29);
        assert_eq!(
            table(TickProvider::Massive, 210).statements(
                &calendar,
                &named(saturday),
                &bars,
                conditions
            ),
            Err(EventTableRefusal::NotATradingDay { session: saturday })
        );
    }

    /// The statements read the conditions snapshot of the date they are given, and refuse one not taken before the
    /// session, as a snapshot of the session's own date may already know its prints.
    #[test]
    fn test_the_conditions_snapshot_must_precede_the_session() {
        let day = date(2026, 10, 16);
        let calendar = calendar(date(2025, 11, 28));
        let bars = held(&calendar, Provider::Alpaca, date(2026, 9, 14), day);
        let build = |as_of| {
            table(TickProvider::Alpaca, 360).statements(&calendar, &named(day), &bars, as_of)
        };
        assert!(build(date(2026, 10, 15)).unwrap().contains(
            "getvariable('archive') || \
             '/data/equity/stage=parsed/reference/provider=massive/table=conditions/as_of=2026-10-15/data.parquet'"
        ));
        for as_of in [day, date(2026, 10, 17)] {
            assert_eq!(
                build(as_of),
                Err(EventTableRefusal::ConditionsNotBeforeSession {
                    as_of,
                    session: day
                })
            );
        }
    }

    /// The thresholds reach the SQL as the constants spell them.
    #[test]
    fn test_the_thresholds_are_spelled_into_the_statements() {
        let day = date(2025, 6, 16);
        let calendar = calendar(date(2025, 11, 28));
        let bars = held(&calendar, Provider::Massive, date(2025, 1, 1), day);
        let statements = table(TickProvider::Massive, 360)
            .statements(&calendar, &named(day), &bars, date(2025, 6, 13))
            .unwrap();
        for spelled in [
            "abs(daily_return) < 0.4)",
            "WHEN size >= 10000 OR price * size >= 200000 THEN size",
            "WHEN size < 100 THEN size",
            "hundredths_of_a_cent BETWEEN 61 AND 99 THEN 1",
            "hundredths_of_a_cent BETWEEN 1 AND 39 THEN -1",
            "coalesce(correction, '0') NOT IN ('1', '8', '10')",
        ] {
            assert!(statements.contains(spelled), "{spelled}");
        }
        let alpaca = held(&calendar, Provider::Alpaca, date(2025, 1, 1), day);
        assert!(
            table(TickProvider::Alpaca, 360)
                .statements(&calendar, &named(day), &alpaca, date(2025, 6, 13))
                .unwrap()
                .contains("coalesce(\"update\", '') NOT IN ('corrected', 'canceled')")
        );
    }

    /// The default horizons spell today's minute targets, a horizon of whole minutes is named by its minutes and any
    /// other by its seconds, and the resting window reaches every read of the window's end.
    #[test]
    fn test_horizons_and_the_resting_window_are_spelled_into_the_statements() {
        let day = date(2025, 6, 16);
        let calendar = calendar(date(2025, 11, 28));
        let bars = held(&calendar, Provider::Massive, date(2025, 1, 1), day);
        let build = |table: EventTable| {
            table
                .statements(&calendar, &named(day), &bars, date(2025, 6, 13))
                .unwrap()
        };
        let defaults = build(table(TickProvider::Massive, 360));
        for spelled in [
            "('own', 'forward_1', 60), ('SPY', 'forward_1', 60), ('own', 'forward_5', 300)",
            "('SPY', 'forward_60', 3600), \n",
            "10000 * (mid_forward_60 / mid - 1) AS raw_60, 10000 * (mid_forward_close / mid - 1) AS raw_close",
            "raw_15 - 10000 * (spy_forward_15 / spy_mid - 1) AS net_15",
            "spread_forward_30 AS forward_spread_30",
            "entries.arrival + INTERVAL 60 SECOND <= TIMESTAMP '2025-06-16 19:59:59'",
            "forward_sell_entry_price_conservative,\n        forward_bid_at_window",
        ] {
            assert!(defaults.contains(spelled), "{spelled}");
        }
        let horizons = [10, 30, 60, 120, 300]
            .into_iter()
            .filter_map(NonZeroU32::new)
            .collect();
        let study = build(
            table(TickProvider::Massive, 360)
                .with_horizons(ForwardHorizons::from_seconds(horizons))
                .with_resting_window(RestingWindow::from_seconds(NonZeroU32::new(15).unwrap())),
        );
        for spelled in [
            "('own', 'forward_seconds_10', 10)",
            "AS raw_seconds_30",
            "AS net_2",
            "AS forward_spy_5",
            "started.arrival + INTERVAL 15 SECOND >= end_print.timestamp",
        ] {
            assert!(study.contains(spelled), "{spelled}");
        }
        assert!(!study.contains("raw_15"));
    }

    #[test]
    fn test_an_empty_window_is_refused() {
        assert_eq!(
            EntryWindow::new(15, 15),
            Err(EmptyWindow { start: 15, end: 15 })
        );
    }

    /// The lookback reads the provider's own bars of the 61 trading days before the session, or fewer when its history
    /// starts later, and refuses the session before missing, a day missing between held ones, or no day before at all.
    #[test]
    fn test_the_daily_lookback_follows_the_provider_and_refuses_what_is_absent() {
        let day = date(2026, 11, 16);
        let calendar = calendar(date(2025, 11, 28));
        let massive = held(
            &calendar,
            Provider::Massive,
            date(2025, 1, 1),
            date(2026, 10, 10),
        );
        let alpaca = held(&calendar, Provider::Alpaca, date(2026, 9, 14), day);
        let both: Vec<TradesKey> = massive.iter().chain(&alpaca).copied().collect();
        let read = daily_lookback(&calendar, day, TickProvider::Alpaca, &both).unwrap();
        assert_eq!(
            (read.len(), read.first(), read.last()),
            (45, Some(&date(2026, 11, 13)), Some(&date(2026, 9, 14)))
        );
        assert_eq!(
            daily_lookback(&calendar, day, TickProvider::Massive, &both),
            Err(EventTableRefusal::DailyBarsAbsent {
                provider: TickProvider::Massive,
                absent: date(2026, 11, 13),
            })
        );
        let full =
            daily_lookback(&calendar, date(2026, 10, 1), TickProvider::Massive, &both).unwrap();
        assert_eq!(
            (full.len(), full.first(), full.last()),
            (61, Some(&date(2026, 9, 30)), Some(&date(2026, 7, 8)))
        );
        let holed: Vec<TradesKey> = alpaca
            .iter()
            .filter(|key| key.session() != date(2026, 10, 20))
            .copied()
            .collect();
        assert_eq!(
            daily_lookback(&calendar, day, TickProvider::Alpaca, &holed),
            Err(EventTableRefusal::DailyBarsAbsent {
                provider: TickProvider::Alpaca,
                absent: date(2026, 10, 20),
            })
        );
        let minutes: Vec<TradesKey> = alpaca
            .iter()
            .map(|key| key.at_interval(BarInterval::OneMinute))
            .collect();
        assert_eq!(
            daily_lookback(&calendar, day, TickProvider::Alpaca, &minutes),
            Err(EventTableRefusal::DailyBarsAbsent {
                provider: TickProvider::Alpaca,
                absent: date(2026, 11, 13),
            })
        );
        assert_eq!(
            daily_lookback(&calendar, date(2025, 1, 1), TickProvider::Alpaca, &both),
            Err(EventTableRefusal::NoEarlierTradingDay {
                session: date(2025, 1, 1),
            })
        );
    }

    /// Members carry their rank, industry code, key, Ken French's industry number and their draw's seed, a missing
    /// code reading as industry 12.
    #[test]
    fn test_members_render_with_rank_industry_and_seed() {
        let day = date(2025, 6, 16);
        assert_eq!(
            members(&named(day), EventClock::Prints),
            format!(
                "('AAA', 1, 3571::SMALLINT, 320193::BIGINT, 6, {}::UINTEGER), \
                 ('BBB', 2, NULL::SMALLINT, NULL::BIGINT, 12, {}::UINTEGER)",
                fnv("AAA/2025-06-16/print"),
                fnv("BBB/2025-06-16/print"),
            )
        );
        assert_eq!(industry_number(Industry::ConsumerNondurables), 1);
        assert_eq!(industry_number(Industry::Finance), 11);
    }

    /// FNV-1a, written here apart from `draw_seed`.
    fn fnv(text: &str) -> u32 {
        text.bytes().fold(0x811c_9dc5, |hash, byte| {
            (hash ^ u32::from(byte)).wrapping_mul(0x0100_0193)
        })
    }

    /// The draw's hash is pinned to values computed outside the repo, FNV-1a's published vectors and MurmurHash3's
    /// finalizer, so a rebuilt table draws the same rows.
    #[test]
    fn test_the_draw_hash_is_pinned_to_its_published_definitions() {
        assert_eq!(
            [mix(0), mix(1), mix(u32::MAX)],
            [0, 1_364_076_727, 2_180_083_513]
        );
        assert_eq!(draw_order(u32::MAX, 5_000_000_000), 547_992_234);
        assert_eq!(draw_order(1, 7), 2_520_902_017);
        assert_eq!(
            [fnv(""), fnv("a"), fnv("foobar")],
            [0x811c_9dc5, 0xe40c_292c, 0xbf9c_f968]
        );
        let aaa = Symbol::new("AAA").unwrap();
        assert_eq!(
            draw_seed(&aaa, date(2025, 6, 16), EventClock::QuoteUpdates),
            fnv("AAA/2025-06-16/quote")
        );
        assert_eq!(
            draw_seed(&aaa, date(2025, 6, 17), EventClock::Prints),
            fnv("AAA/2025-06-17/print")
        );
    }
}
