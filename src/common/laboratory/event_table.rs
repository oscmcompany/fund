//! The event-clock feature table: per name and session, a draw of the name's quote updates or prints, each read a set
//! delay after it, with every feature and target a DuckDB statement computes from the cleaned ticks in scratch.

use std::fmt::Write;
use std::num::NonZeroU16;

use chrono::{DateTime, Datelike, Months, Utc};
use strum::IntoEnumIterator;

use crate::common::laboratory::universe::Universe;
use crate::common::market::industry::Industry;
use crate::common::storage::Provider;
use crate::common::time::SessionDate;
use crate::common::time::calendar::TradingSession;

/// Binds the scratch ticks, the parsed archive and the eligibility macros the table reads, for a run against the
/// buckets named by `AWS_S3_PROFILE_BUCKET_NAME` and `AWS_S3_ARCHIVE_BUCKET_NAME`.
pub const BUCKET_PREAMBLE: &str = r#"
SET TimeZone='UTC';
INSTALL httpfs; LOAD httpfs; INSTALL aws; LOAD aws;
CREATE SECRET (TYPE s3, PROVIDER credential_chain, REGION 'us-east-1');
SET VARIABLE scratch = 's3://' || getenv('AWS_S3_PROFILE_BUCKET_NAME') || '/scratch/ticks';
SET VARIABLE parsed = 's3://' || getenv('AWS_S3_ARCHIVE_BUCKET_NAME') || '/data/equity/stage=parsed';
SET VARIABLE eligible_codes = (
    SELECT list(code::VARCHAR)
    FROM read_parquet(getvariable('parsed') || '/reference/provider=massive/table=conditions/as_of=2026-10-05/data.parquet')
    WHERE updates_high_low);
CREATE MACRO eligible(conditions, correction) AS
    coalesce(correction, '0') NOT IN ('1', '8', '10')
    AND list_bool_and(list_transform(string_split(coalesce(conditions, ''), ','),
        code -> code = '' OR list_contains(getvariable('eligible_codes'), code)));
-- Alpaca's letters, per tape, that set a price; a letter two current conditions share with different rules sets none.
CREATE TEMP TABLE letters AS
    SELECT * FROM read_parquet(getvariable('parsed') || '/reference/provider=massive/table=conditions/as_of=2026-10-05/data.parquet')
    WHERE NOT retired;
SET VARIABLE consolidated_letters = (
    SELECT list(consolidated_tape_letter) FROM (
        SELECT consolidated_tape_letter FROM letters WHERE consolidated_tape_letter IS NOT NULL GROUP BY ALL
        HAVING count(DISTINCT (updates_volume, updates_high_low, updates_open_close)) = 1 AND bool_and(updates_high_low)));
SET VARIABLE unlisted_letters = (
    SELECT list(unlisted_trading_letter) FROM (
        SELECT unlisted_trading_letter FROM letters WHERE unlisted_trading_letter IS NOT NULL GROUP BY ALL
        HAVING count(DISTINCT (updates_volume, updates_high_low, updates_open_close)) = 1 AND bool_and(updates_high_low)));
CREATE MACRO eligible_letters(conditions, tape, update_label) AS
    coalesce(update_label, '') NOT IN ('corrected', 'canceled')
    AND CASE
        WHEN tape IN ('A', 'B') THEN list_bool_and(list_transform(coalesce(conditions, []),
            letter -> letter = ' ' OR list_contains(getvariable('consolidated_letters'), letter)))
        WHEN tape = 'C' THEN list_bool_and(list_transform(coalesce(conditions, []),
            letter -> letter = '@' OR list_contains(getvariable('unlisted_letters'), letter)))
        ELSE false END;
"#;

/// A provider whose cleaned ticks the table can read.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    strum::Display,
    strum::EnumString,
    strum::IntoStaticStr,
    strum::EnumIter,
)]
#[strum(serialize_all = "snake_case")]
pub enum TickProvider {
    Massive,
    Alpaca,
}

/// How one provider's cleaned ticks spell what the table reads of them.
struct Dialect {
    time: &'static str,
    not_withdrawn: &'static str,
    sweep: &'static str,
    off_exchange: &'static str,
    eligible: &'static str,
}

impl TickProvider {
    pub fn provider(self) -> Provider {
        match self {
            Self::Massive => Provider::Massive,
            Self::Alpaca => Provider::Alpaca,
        }
    }

    fn dialect(self) -> Dialect {
        match self {
            Self::Massive => Dialect {
                time: "sip_timestamp",
                not_withdrawn: "coalesce(correction, '0') NOT IN ('1', '8', '10')",
                sweep: "list_contains(string_split(coalesce(conditions, ''), ','), '14')",
                off_exchange: "exchange = '4'",
                eligible: "eligible(conditions, correction)",
            },
            Self::Alpaca => Dialect {
                time: "timestamp",
                not_withdrawn: "coalesce(\"update\", '') NOT IN ('corrected', 'canceled')",
                sweep: "list_contains(coalesce(conditions, []), 'F')",
                off_exchange: "exchange = 'D'",
                eligible: "eligible_letters(conditions, tape, \"update\")",
            },
        }
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

/// What a table draws and how it reads it, the same for every session it is built over.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EventTable {
    provider: TickProvider,
    clock: EventClock,
    delay: ReadDelay,
    draws: Draws,
    window: EntryWindow,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum EventTableRefusal {
    #[error("the universe is {universe}'s, not {session}'s")]
    UniverseOfAnotherSession {
        universe: SessionDate,
        session: SessionDate,
    },
    #[error("{session} trades {session_minutes} minutes, ending before the window's minute {end}")]
    WindowPastClose {
        session: SessionDate,
        end: u16,
        session_minutes: i64,
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
        }
    }

    /// The statements writing `session`'s rows to `table/rows_<provider>_<session>.parquet` and its universe to
    /// `table/universe_<session>.parquet`, run after `BUCKET_PREAMBLE` or another binding of the names it reads.
    pub fn statements(
        &self,
        session: &TradingSession,
        universe: &Universe,
    ) -> Result<String, EventTableRefusal> {
        let (opened, closed) = session.hours();
        let session_minutes = (closed - opened).num_minutes();
        let date = session.date();
        if universe.session() != date {
            return Err(EventTableRefusal::UniverseOfAnotherSession {
                universe: universe.session(),
                session: date,
            });
        }
        if i64::from(self.window.end) > session_minutes {
            return Err(EventTableRefusal::WindowPastClose {
                session: date,
                end: self.window.end,
                session_minutes,
            });
        }
        let (open, close) = (timestamp(opened), timestamp(closed));
        let Dialect {
            time,
            not_withdrawn,
            sweep,
            off_exchange,
            eligible,
        } = self.provider.dialect();
        let (provider, session) = (self.provider.provider(), date);
        let (members, daily_paths) = (members(universe), daily_paths(date));
        let (events, kind) = self.clock.source();
        let (delay, draws) = (self.delay.0, self.draws.0);
        let EntryWindow { start, end } = self.window;
        let entries = format!(
            "CREATE TEMP TABLE entries AS
    SELECT ticker, '{kind}' AS kind, event_number AS key, timestamp + INTERVAL {delay} MILLISECOND AS arrival
    FROM {events}
    WHERE ticker IN (SELECT ticker FROM universe)
      AND timestamp >= {open} + INTERVAL {start} MINUTE AND timestamp < {open} + INTERVAL {end} MINUTE
    QUALIFY row_number() OVER (PARTITION BY ticker ORDER BY hash(ticker || '/{session}/{kind}/' || event_number)) <= {draws};
"
        );
        Ok(format!(
            r#"
CREATE TEMP TABLE universe AS
    SELECT * FROM (VALUES {members}) AS members(ticker, dollar_volume_rank, industry_code, central_index_key, industry);
COPY (SELECT '{session}' AS session, ticker, industry_code, central_index_key FROM universe) TO 'table/universe_{session}.parquet';
CREATE TEMP TABLE ranks AS SELECT ticker, dollar_volume_rank FROM universe;
CREATE TEMP TABLE bars AS
    SELECT symbol AS ticker, closed_at, close::DOUBLE AS close
    FROM read_parquet([{daily_paths}])
    WHERE (symbol IN (SELECT ticker FROM universe) OR symbol = 'SPY') AND closed_at < {open}
    QUALIFY row_number() OVER (PARTITION BY symbol ORDER BY closed_at DESC) <= 61;
CREATE TEMP TABLE previous_close AS SELECT ticker, arg_max(close, closed_at) AS close FROM bars GROUP BY ticker;
CREATE TEMP TABLE daily AS
    WITH returns AS (
        SELECT ticker, (closed_at AT TIME ZONE 'America/New_York')::DATE AS day,
            close / lag(close) OVER daily_order - 1 AS daily_return,
            row_number() OVER (PARTITION BY ticker ORDER BY closed_at DESC) AS recency
        FROM bars WINDOW daily_order AS (PARTITION BY ticker ORDER BY closed_at)),
    kept AS (SELECT * FROM returns WHERE abs(daily_return) < 0.4)
    SELECT name.ticker, regr_slope(name.daily_return, spy.daily_return) AS beta,
        10000 * stddev_samp(name.daily_return) FILTER (WHERE name.recency <= 20) AS daily_volatility_20,
        10000 * max(name.daily_return) FILTER (WHERE name.recency = 1) AS previous_return,
        count(*) AS beta_days
    FROM kept name JOIN kept spy ON spy.ticker = 'SPY' AND spy.day = name.day
    WHERE name.ticker <> 'SPY'
    GROUP BY name.ticker;
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
        FROM quotes WINDOW quote_order AS (PARTITION BY ticker ORDER BY timestamp))
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
    FROM flowed WINDOW running AS (PARTITION BY ticker ORDER BY timestamp ROWS UNBOUNDED PRECEDING);
CREATE TEMP TABLE signed AS
    WITH prints AS (
        SELECT ticker, {time} AS timestamp, price::DOUBLE AS price, size::DOUBLE AS size, {sweep} AS sweep,
            {off_exchange} AS off_exchange, {eligible} AS eligible, (price * 10000)::BIGINT % 100 AS hundredths_of_a_cent
        FROM read_parquet(getvariable('scratch') || '/kind=trades/provider={provider}/session={session}/data.parquet')
        WHERE ticker IN (SELECT ticker FROM universe) AND {not_withdrawn} AND size > 0
          AND {time} >= {open} AND {time} < {close})
    SELECT prints.*, sign(prints.price - quote_events.mid) AS print_sign,
        CASE WHEN off_exchange AND hundredths_of_a_cent BETWEEN 61 AND 99 THEN 1
             WHEN off_exchange AND hundredths_of_a_cent BETWEEN 1 AND 39 THEN -1 END AS retail_sign
    FROM prints ASOF LEFT JOIN quote_events
      ON prints.ticker = quote_events.ticker AND prints.timestamp > quote_events.timestamp;
CREATE TEMP TABLE trade_events AS
    SELECT ticker, timestamp, sweep, print_sign, size, price, eligible,
        row_number() OVER print_order AS event_number,
        sum(coalesce(print_sign, 0) * size) OVER print_order AS cumulative_signed,
        sum(size) OVER print_order AS cumulative_volume,
        sum(CASE WHEN sweep THEN coalesce(print_sign, 0) * size ELSE 0 END) OVER print_order AS cumulative_sweep_signed,
        sum(CASE WHEN sweep THEN size ELSE 0 END) OVER print_order AS cumulative_sweep_volume,
        sum(coalesce(retail_sign, 0) * size) OVER print_order AS cumulative_retail_signed,
        sum(CASE WHEN retail_sign IS NOT NULL THEN size ELSE 0 END) OVER print_order AS cumulative_retail_volume,
        sum(CASE WHEN size >= 10000 OR price * size >= 200000 THEN coalesce(print_sign, 0) * size ELSE 0 END) OVER print_order AS cumulative_block_signed,
        sum(CASE WHEN size >= 10000 OR price * size >= 200000 THEN size ELSE 0 END) OVER print_order AS cumulative_block_volume,
        sum(CASE WHEN size < 100 THEN coalesce(print_sign, 0) * size ELSE 0 END) OVER print_order AS cumulative_odd_signed,
        sum(CASE WHEN size < 100 THEN size ELSE 0 END) OVER print_order AS cumulative_odd_volume,
        sum(CASE WHEN off_exchange THEN size ELSE 0 END) OVER print_order AS cumulative_off_exchange_volume,
        sum(CASE WHEN print_sign = 0 THEN 1 ELSE 0 END) OVER print_order AS cumulative_midpoint_count,
        sum(price * size) OVER print_order AS cumulative_dollars,
        max(CASE WHEN eligible THEN price END) OVER print_order AS session_high,
        min(CASE WHEN eligible THEN price END) OVER print_order AS session_low,
        last_value(CASE WHEN sweep AND print_sign <> 0 THEN timestamp END IGNORE NULLS) OVER print_order AS last_sweep_time,
        last_value(CASE WHEN sweep AND print_sign <> 0 THEN print_sign END IGNORE NULLS) OVER print_order AS last_sweep_sign,
        sum(coalesce(print_sign, 0)) OVER last_ten AS sign_sum,
        count(*) OVER last_ten AS sign_count,
        sum(coalesce(print_sign, 0) * size) OVER last_ten AS last_ten_signed
    FROM signed
    WINDOW print_order AS (PARTITION BY ticker ORDER BY timestamp ROWS UNBOUNDED PRECEDING),
        last_ten AS (PARTITION BY ticker ORDER BY timestamp ROWS 9 PRECEDING);
-- Universe-wide signed and total dollars per second, cumulative, so any window is a difference of two reads.
CREATE TEMP TABLE market_flow AS
    WITH seconds AS (
        SELECT time_bucket(INTERVAL 1 SECOND, timestamp) + INTERVAL 1 SECOND AS timestamp,
            sum(coalesce(print_sign, 0) * price * size) AS signed_dollars, sum(price * size) AS dollars
        FROM signed GROUP BY ALL)
    SELECT timestamp, sum(signed_dollars) OVER second_order AS cumulative_signed_dollars,
        sum(dollars) OVER second_order AS cumulative_dollars
    FROM seconds WINDOW second_order AS (ORDER BY timestamp ROWS UNBOUNDED PRECEDING);
{entries}-- Every instant a quote is read at, for the name, SPY and QQQ, in one ASOF join.
CREATE TEMP TABLE quote_probes AS
    WITH sources AS (
        SELECT entries.*, sources.source, CASE sources.source WHEN 'own' THEN entries.ticker ELSE sources.source END AS read_ticker
        FROM entries, (VALUES ('own'), ('SPY'), ('QQQ')) AS sources(source)),
    offsets AS (
        SELECT * FROM (VALUES
            ('own', 'now', 0), ('own', 'back_5', -5), ('own', 'back_30', -30), ('own', 'back_60', -60), ('own', 'back_120', -120),
            ('own', 'back_300', -300), ('own', 'back_900', -900), ('own', 'back_1800', -1800),
            ('own', 'forward_1', 60), ('own', 'forward_5', 300), ('own', 'forward_15', 900), ('own', 'forward_30', 1800),
            ('own', 'forward_60', 3600),
            ('SPY', 'now', 0), ('SPY', 'back_5', -5), ('SPY', 'back_30', -30), ('SPY', 'back_120', -120), ('SPY', 'back_1800', -1800),
            ('SPY', 'forward_1', 60), ('SPY', 'forward_5', 300), ('SPY', 'forward_15', 900), ('SPY', 'forward_30', 1800),
            ('SPY', 'forward_60', 3600),
            ('QQQ', 'now', 0), ('QQQ', 'back_5', -5), ('QQQ', 'back_30', -30), ('QQQ', 'back_120', -120)
        ) AS offsets(source, probe, seconds))
    SELECT sources.ticker, kind, key, arrival, sources.source, read_ticker, probe, arrival + to_seconds(seconds) AS probe_time
    FROM sources JOIN offsets USING (source)
    WHERE arrival + to_seconds(seconds) <= {close} - INTERVAL 1 SECOND
    UNION ALL
    SELECT ticker, kind, key, arrival, source, read_ticker, 'forward_close', {close} - INTERVAL 1 SECOND
    FROM sources WHERE source IN ('own', 'SPY');
CREATE TEMP TABLE quote_probed AS
    SELECT quote_probes.*, quote_events.* EXCLUDE (ticker)
    FROM quote_probes ASOF LEFT JOIN quote_events
      ON quote_probes.read_ticker = quote_events.ticker AND quote_probes.probe_time >= quote_events.timestamp;
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
    SELECT trade_probes.*, trade_events.* EXCLUDE (ticker)
    FROM trade_probes ASOF LEFT JOIN trade_events
      ON trade_probes.ticker = trade_events.ticker AND trade_probes.probe_time >= trade_events.timestamp;
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
        max(mid) FILTER (WHERE source = 'own' AND probe = 'forward_1') AS mid_forward_1,
        max(mid) FILTER (WHERE source = 'own' AND probe = 'forward_5') AS mid_forward_5,
        max(mid) FILTER (WHERE source = 'own' AND probe = 'forward_15') AS mid_forward_15,
        max(mid) FILTER (WHERE source = 'own' AND probe = 'forward_30') AS mid_forward_30,
        max(mid) FILTER (WHERE source = 'own' AND probe = 'forward_60') AS mid_forward_60,
        max(mid) FILTER (WHERE source = 'own' AND probe = 'forward_close') AS mid_forward_close,
        max(spread_basis_points) FILTER (WHERE source = 'own' AND probe = 'forward_1') AS spread_forward_1,
        max(spread_basis_points) FILTER (WHERE source = 'own' AND probe = 'forward_5') AS spread_forward_5,
        max(spread_basis_points) FILTER (WHERE source = 'own' AND probe = 'forward_15') AS spread_forward_15,
        max(spread_basis_points) FILTER (WHERE source = 'own' AND probe = 'forward_30') AS spread_forward_30,
        max(spread_basis_points) FILTER (WHERE source = 'own' AND probe = 'forward_60') AS spread_forward_60,
        max(spread_basis_points) FILTER (WHERE source = 'own' AND probe = 'forward_close') AS spread_forward_close,
        max(mid) FILTER (WHERE source = 'SPY' AND probe = 'now') AS spy_mid,
        max(mid) FILTER (WHERE source = 'SPY' AND probe = 'back_1800') AS spy_mid_1800,
        max(mid) FILTER (WHERE source = 'SPY' AND probe = 'forward_1') AS spy_forward_1,
        max(mid) FILTER (WHERE source = 'SPY' AND probe = 'forward_5') AS spy_forward_5,
        max(mid) FILTER (WHERE source = 'SPY' AND probe = 'forward_15') AS spy_forward_15,
        max(mid) FILTER (WHERE source = 'SPY' AND probe = 'forward_30') AS spy_forward_30,
        max(mid) FILTER (WHERE source = 'SPY' AND probe = 'forward_60') AS spy_forward_60,
        max(mid) FILTER (WHERE source = 'SPY' AND probe = 'forward_close') AS spy_forward_close,
        max(microprice) FILTER (WHERE source = 'SPY' AND probe = 'now') AS spy_micro,
        max(microprice) FILTER (WHERE source = 'SPY' AND probe = 'back_5') AS spy_micro_5,
        max(microprice) FILTER (WHERE source = 'SPY' AND probe = 'back_30') AS spy_micro_30,
        max(microprice) FILTER (WHERE source = 'SPY' AND probe = 'back_120') AS spy_micro_120,
        max(cumulative_flow) FILTER (WHERE source = 'SPY' AND probe = 'now') AS spy_flow_now,
        max(cumulative_absolute_flow) FILTER (WHERE source = 'SPY' AND probe = 'now') AS spy_absolute_flow_now,
        max(cumulative_flow) FILTER (WHERE source = 'SPY' AND probe = 'back_30') AS spy_flow_30,
        max(cumulative_absolute_flow) FILTER (WHERE source = 'SPY' AND probe = 'back_30') AS spy_absolute_flow_30,
        max(microprice) FILTER (WHERE source = 'QQQ' AND probe = 'now') AS qqq_micro,
        max(microprice) FILTER (WHERE source = 'QQQ' AND probe = 'back_5') AS qqq_micro_5,
        max(microprice) FILTER (WHERE source = 'QQQ' AND probe = 'back_30') AS qqq_micro_30,
        max(microprice) FILTER (WHERE source = 'QQQ' AND probe = 'back_120') AS qqq_micro_120
    FROM quote_probed GROUP BY ticker, kind, key;
CREATE TEMP TABLE trades_wide AS
    SELECT now.ticker, now.kind, now.key, now.event_number AS print_number, now.cumulative_volume AS volume_now,
        now.cumulative_dollars AS dollars_now, now.session_high, now.session_low, now.last_sweep_time, now.last_sweep_sign,
        now.cumulative_signed AS signed_now, now.cumulative_sweep_signed AS sweep_signed_now,
        now.cumulative_sweep_volume AS sweep_volume_now,
        now.cumulative_retail_signed AS retail_signed_now, now.cumulative_retail_volume AS retail_volume_now,
        now.cumulative_block_signed AS block_signed_now, now.cumulative_block_volume AS block_volume_now,
        now.cumulative_odd_signed AS odd_signed_now, now.cumulative_odd_volume AS odd_volume_now,
        now.cumulative_off_exchange_volume AS off_exchange_now, now.cumulative_midpoint_count AS midpoint_now,
        seconds_5.event_number AS print_number_s5, seconds_5.cumulative_signed AS signed_s5, seconds_5.cumulative_volume AS volume_s5,
        seconds_5.cumulative_sweep_signed AS sweep_signed_s5, seconds_5.cumulative_sweep_volume AS sweep_volume_s5,
        seconds_30.event_number AS print_number_s30, seconds_30.cumulative_signed AS signed_s30, seconds_30.cumulative_volume AS volume_s30,
        seconds_30.cumulative_sweep_signed AS sweep_signed_s30, seconds_30.cumulative_sweep_volume AS sweep_volume_s30,
        seconds_120.event_number AS print_number_s120, seconds_120.cumulative_signed AS signed_s120, seconds_120.cumulative_volume AS volume_s120,
        seconds_120.cumulative_sweep_signed AS sweep_signed_s120, seconds_120.cumulative_sweep_volume AS sweep_volume_s120,
        seconds_120.cumulative_block_signed AS block_signed_s120, seconds_120.cumulative_block_volume AS block_volume_s120,
        events_10.cumulative_signed AS signed_e10, events_10.cumulative_volume AS volume_e10,
        events_10.cumulative_sweep_signed AS sweep_signed_e10, events_10.cumulative_sweep_volume AS sweep_volume_e10,
        events_50.cumulative_signed AS signed_e50, events_50.cumulative_volume AS volume_e50,
        events_50.cumulative_sweep_signed AS sweep_signed_e50, events_50.cumulative_sweep_volume AS sweep_volume_e50,
        events_50.cumulative_retail_signed AS retail_signed_e50, events_50.cumulative_retail_volume AS retail_volume_e50,
        events_50.cumulative_odd_signed AS odd_signed_e50, events_50.cumulative_odd_volume AS odd_volume_e50,
        events_250.cumulative_signed AS signed_e250, events_250.cumulative_volume AS volume_e250,
        events_250.cumulative_sweep_signed AS sweep_signed_e250, events_250.cumulative_sweep_volume AS sweep_volume_e250,
        events_250.cumulative_retail_volume AS retail_volume_e250, events_250.cumulative_off_exchange_volume AS off_exchange_e250,
        events_250.cumulative_midpoint_count AS midpoint_e250
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
            quote_10.cumulative_flow AS flow_e10, quote_10.cumulative_absolute_flow AS absolute_flow_e10,
            quote_50.cumulative_flow AS flow_e50, quote_50.cumulative_absolute_flow AS absolute_flow_e50,
            quote_250.cumulative_flow AS flow_e250, quote_250.cumulative_absolute_flow AS absolute_flow_e250,
            market_now.cumulative_signed_dollars - coalesce(market_30.cumulative_signed_dollars, 0) AS market_signed_30,
            market_now.cumulative_dollars - coalesce(market_30.cumulative_dollars, 0) AS market_dollars_30,
            ranks.dollar_volume_rank, previous_close.close AS previous_close,
            daily.beta, daily.daily_volatility_20, daily.previous_return, daily.beta_days, universe.industry,
            epoch_ns(quotes_wide.arrival - {open}) / 6e10 AS minutes_since_open
        FROM quotes_wide JOIN trades_wide USING (ticker, kind, key)
        LEFT JOIN (SELECT * FROM quote_events_back WHERE window_length = 10) quote_10 USING (ticker, kind, key)
        LEFT JOIN (SELECT * FROM quote_events_back WHERE window_length = 50) quote_50 USING (ticker, kind, key)
        LEFT JOIN (SELECT * FROM quote_events_back WHERE window_length = 250) quote_250 USING (ticker, kind, key)
        LEFT JOIN (SELECT * FROM market_probed WHERE probe = 'now') market_now USING (ticker, kind, key)
        LEFT JOIN (SELECT * FROM market_probed WHERE probe = 'back_30') market_30 USING (ticker, kind, key)
        LEFT JOIN ranks USING (ticker) LEFT JOIN previous_close USING (ticker)
        LEFT JOIN daily USING (ticker) LEFT JOIN universe USING (ticker))
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
        spread_forward_1 AS forward_spread_1, spread_forward_5 AS forward_spread_5, spread_forward_15 AS forward_spread_15,
        spread_forward_30 AS forward_spread_30, spread_forward_60 AS forward_spread_60, spread_forward_close AS forward_spread_close,
        (quote_number - coalesce(quote_number_30, 0)) / 30.0 AS quote_rate,
        -- flow
        (flow_now - coalesce(flow_5, 0)) / nullif(absolute_flow_now - coalesce(absolute_flow_5, 0), 0) AS order_flow_s5,
        (flow_now - coalesce(flow_30, 0)) / nullif(absolute_flow_now - coalesce(absolute_flow_30, 0), 0) AS order_flow_s30,
        (flow_now - coalesce(flow_120, 0)) / nullif(absolute_flow_now - coalesce(absolute_flow_120, 0), 0) AS order_flow_s120,
        (flow_now - flow_e10) / nullif(absolute_flow_now - absolute_flow_e10, 0) AS order_flow_e10,
        (flow_now - flow_e50) / nullif(absolute_flow_now - absolute_flow_e50, 0) AS order_flow_e50,
        (flow_now - flow_e250) / nullif(absolute_flow_now - absolute_flow_e250, 0) AS order_flow_e250,
        (signed_now - coalesce(signed_s5, 0)) / nullif(volume_now - coalesce(volume_s5, 0), 0) AS trade_sign_s5,
        (signed_now - coalesce(signed_s30, 0)) / nullif(volume_now - coalesce(volume_s30, 0), 0) AS trade_sign_s30,
        (signed_now - coalesce(signed_s120, 0)) / nullif(volume_now - coalesce(volume_s120, 0), 0) AS trade_sign_s120,
        (signed_now - signed_e10) / nullif(volume_now - volume_e10, 0) AS trade_sign_e10,
        (signed_now - signed_e50) / nullif(volume_now - volume_e50, 0) AS trade_sign_e50,
        (signed_now - signed_e250) / nullif(volume_now - volume_e250, 0) AS trade_sign_e250,
        (sweep_signed_now - coalesce(sweep_signed_s5, 0)) / nullif(sweep_volume_now - coalesce(sweep_volume_s5, 0), 0) AS sweep_s5,
        (sweep_signed_now - coalesce(sweep_signed_s30, 0)) / nullif(sweep_volume_now - coalesce(sweep_volume_s30, 0), 0) AS sweep_s30,
        (sweep_signed_now - coalesce(sweep_signed_s120, 0)) / nullif(sweep_volume_now - coalesce(sweep_volume_s120, 0), 0) AS sweep_s120,
        (sweep_signed_now - sweep_signed_e10) / nullif(sweep_volume_now - sweep_volume_e10, 0) AS sweep_e10,
        (sweep_signed_now - sweep_signed_e50) / nullif(sweep_volume_now - sweep_volume_e50, 0) AS sweep_e50,
        (sweep_signed_now - sweep_signed_e250) / nullif(sweep_volume_now - sweep_volume_e250, 0) AS sweep_e250,
        epoch_ns(arrival - last_sweep_time) / 1e9 AS seconds_since_sweep,
        last_sweep_sign AS last_sweep_side,
        (block_signed_now - coalesce(block_signed_s120, 0)) / nullif(block_volume_now - coalesce(block_volume_s120, 0), 0) AS block_s120,
        (odd_signed_now - odd_signed_e50) / nullif(odd_volume_now - odd_volume_e50, 0) AS odd_lot_e50,
        (retail_signed_now - retail_signed_e50) / nullif(retail_volume_now - retail_volume_e50, 0) AS retail_e50,
        (retail_volume_now - retail_volume_e250) / nullif(volume_now - volume_e250, 0) AS retail_share_e250,
        (off_exchange_now - off_exchange_e250) / nullif(volume_now - volume_e250, 0) AS off_exchange_share_e250,
        (midpoint_now - midpoint_e250) / 250.0 AS midpoint_share_e250,
        ((volume_now - volume_e50) / 50.0) / nullif(volume_now / nullif(print_number, 0), 0) AS print_size_relative,
        -- activity
        ((print_number - coalesce(print_number_s30, 0)) / 30.0) / nullif(print_number / nullif(minutes_since_open * 60, 0), 0) AS intensity_s30,
        ((print_number - coalesce(print_number_s120, 0)) / 120.0) / nullif(print_number / nullif(minutes_since_open * 60, 0), 0) AS intensity_s120,
        10000 * sqrt(greatest(squared_now - coalesce(squared_60, 0), 0)) AS volatility_60,
        10000 * sqrt(greatest(squared_now - coalesce(squared_300, 0), 0)) AS volatility_300,
        10000 * sqrt(greatest(squared_now - coalesce(squared_1800, 0), 0)) AS volatility_1800,
        10000 * sqrt(squared_now / nullif(minutes_since_open, 0)) AS volatility_per_minute,
        -- cross-name
        10000 * (spy_micro / spy_micro_5 - 1) AS spy_lead_s5,
        10000 * (spy_micro / spy_micro_30 - 1) AS spy_lead_s30,
        10000 * (spy_micro / spy_micro_120 - 1) AS spy_lead_s120,
        10000 * (qqq_micro / qqq_micro_5 - 1) AS qqq_lead_s5,
        10000 * (qqq_micro / qqq_micro_30 - 1) AS qqq_lead_s30,
        10000 * (qqq_micro / qqq_micro_120 - 1) AS qqq_lead_s120,
        (spy_flow_now - spy_flow_30) / nullif(spy_absolute_flow_now - spy_absolute_flow_30, 0) AS spy_order_flow_s30,
        market_signed_30 / nullif(market_dollars_30, 0) AS market_trade_sign_s30,
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
        10000 * (mid_forward_1 / mid - 1) AS raw_1, 10000 * (mid_forward_5 / mid - 1) AS raw_5,
        10000 * (mid_forward_15 / mid - 1) AS raw_15, 10000 * (mid_forward_30 / mid - 1) AS raw_30,
        10000 * (mid_forward_60 / mid - 1) AS raw_60, 10000 * (mid_forward_close / mid - 1) AS raw_close,
        raw_1 - 10000 * (spy_forward_1 / spy_mid - 1) AS net_1, raw_5 - 10000 * (spy_forward_5 / spy_mid - 1) AS net_5,
        raw_15 - 10000 * (spy_forward_15 / spy_mid - 1) AS net_15, raw_30 - 10000 * (spy_forward_30 / spy_mid - 1) AS net_30,
        raw_60 - 10000 * (spy_forward_60 / spy_mid - 1) AS net_60,
        raw_close - 10000 * (spy_forward_close / spy_mid - 1) AS net_close,
        10000 * (spy_forward_1 / spy_mid - 1) AS forward_spy_1, 10000 * (spy_forward_5 / spy_mid - 1) AS forward_spy_5,
        10000 * (spy_forward_15 / spy_mid - 1) AS forward_spy_15, 10000 * (spy_forward_30 / spy_mid - 1) AS forward_spy_30,
        10000 * (spy_forward_60 / spy_mid - 1) AS forward_spy_60,
        10000 * (spy_forward_close / spy_mid - 1) AS forward_spy_close,
        volatility_per_minute AS scale_per_root_minute
    FROM joined
) TO 'table/rows_{provider}_{session}.parquet';
"#
        ))
    }
}

/// `instant` as a DuckDB timestamp literal, read as UTC under the preamble's time zone.
fn timestamp(instant: DateTime<Utc>) -> String {
    format!("TIMESTAMP '{}'", instant.format("%Y-%m-%d %H:%M:%S"))
}

/// Each member as a row of the universe's VALUES: ticker, rank, industry code, central index key, industry number.
fn members(universe: &Universe) -> String {
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
                "{separator}('{}', {}, {}, {}, {})",
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
            );
            rows
        })
}

/// Ken French's number for `industry`, 1 to 12 in the order he lists them.
fn industry_number(industry: Industry) -> usize {
    Industry::iter()
        .position(|listed| listed == industry)
        .map_or(0, |index| index + 1)
}

/// The parsed daily trade bars of `session`'s calendar month and the three before it.
fn daily_paths(session: SessionDate) -> String {
    let month = session.date().with_day(1).unwrap_or(session.date());
    (0..4)
        .map(|back| {
            let first = month - Months::new(back);
            format!(
                "getvariable('parsed') || '/trades/provider=massive/interval=one_day/year={}/month={:02}/*/data.parquet'",
                first.year(),
                first.month()
            )
        })
        .collect::<Vec<_>>()
        .join(", ")
}

#[cfg(test)]
mod tests {
    use chrono::{NaiveDate, NaiveTime};

    use super::*;
    use crate::common::laboratory::universe::{UniverseSize, universe};
    use crate::common::market::aggregate::TradeTotals;
    use crate::common::market::record::BarInterval;
    use crate::common::market::security_details::{
        CentralIndexKey, IndustryCode, SecurityDetails, SecurityType,
    };
    use crate::common::market::trade_bars::{TradeBar, TradeSums};
    use crate::common::market::{DollarVolume, Price, Shares, Symbol, TradeCount};

    fn date(year: i32, month: u32, day: u32) -> SessionDate {
        SessionDate::from_date(NaiveDate::from_ymd_opt(year, month, day).unwrap())
    }

    /// A session opening 09:30 Eastern and closing at `close_hour`.
    fn trading(day: SessionDate, close_hour: u32) -> TradingSession {
        let at = |hour, minute| NaiveTime::from_hms_opt(hour, minute, 0).unwrap();
        TradingSession::new(day, at(9, 30), at(close_hour, 0)).unwrap()
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
        let details = [
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
            day.plus_calendar_days(-1),
            &details,
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

    /// Each provider's ticks are read under its own spelling and path, and nothing is left unsubstituted.
    #[test]
    fn test_each_provider_reads_its_own_ticks_in_its_own_spelling() {
        let day = date(2025, 6, 16);
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
                "eligible_letters(conditions, tape, \"update\")",
                "sip_timestamp",
            ),
        ] {
            let statements = table(provider, 360)
                .statements(&trading(day, 16), &named(day))
                .unwrap();
            assert!(statements.contains(path), "{provider}");
            assert!(statements.contains(spelled), "{provider}");
            assert!(!statements.contains(foreign), "{provider}");
            assert!(!statements.contains(['{', '}']), "{provider}");
            assert!(
                statements.contains(&format!("TO 'table/rows_{provider}_2025-06-16.parquet'")),
                "{provider}"
            );
        }
    }

    /// The session's own hours bound the table, so a half-day closing at 13:00 Eastern counts 210 minutes and refuses a
    /// window reaching past them.
    #[test]
    fn test_a_half_day_bounds_the_window_by_its_own_close() {
        let day = date(2025, 11, 28);
        assert_eq!(
            table(TickProvider::Massive, 360).statements(&trading(day, 13), &named(day)),
            Err(EventTableRefusal::WindowPastClose {
                session: day,
                end: 360,
                session_minutes: 210,
            })
        );
        let statements = table(TickProvider::Massive, 210)
            .statements(&trading(day, 13), &named(day))
            .unwrap();
        assert!(statements.contains("TIMESTAMP '2025-11-28 14:30:00' + INTERVAL 210 MINUTE"));
        assert!(statements.contains("< TIMESTAMP '2025-11-28 18:00:00'"));
        assert!(statements.contains("210 - minutes_since_open AS minutes_to_close"));
        assert!(statements.contains("timestamp + INTERVAL 1000 MILLISECOND AS arrival"));
    }

    #[test]
    fn test_a_universe_builds_only_its_own_session() {
        assert_eq!(
            table(TickProvider::Massive, 360)
                .statements(&trading(date(2025, 6, 17), 16), &named(date(2025, 6, 16))),
            Err(EventTableRefusal::UniverseOfAnotherSession {
                universe: date(2025, 6, 16),
                session: date(2025, 6, 17),
            })
        );
    }

    #[test]
    fn test_an_empty_window_is_refused() {
        assert_eq!(
            EntryWindow::new(15, 15),
            Err(EmptyWindow { start: 15, end: 15 })
        );
    }

    /// Members carry their rank, industry code, key and Ken French's industry number, a missing code reading as
    /// industry 12.
    #[test]
    fn test_members_render_with_rank_and_industry() {
        assert_eq!(
            members(&named(date(2025, 6, 16))),
            "('AAA', 1, 3571::SMALLINT, 320193::BIGINT, 6), ('BBB', 2, NULL::SMALLINT, NULL::BIGINT, 12)"
        );
        assert_eq!(industry_number(Industry::ConsumerNondurables), 1);
        assert_eq!(industry_number(Industry::Finance), 11);
    }

    #[test]
    fn test_daily_paths_reach_back_across_the_year() {
        let paths = daily_paths(date(2026, 2, 10));
        let months: Vec<&str> = paths
            .split(", ")
            .map(|path| &path[path.find("year=").unwrap()..path.find("/*").unwrap()])
            .collect();
        assert_eq!(
            months,
            [
                "year=2026/month=02",
                "year=2026/month=01",
                "year=2025/month=12",
                "year=2025/month=11"
            ]
        );
    }
}
