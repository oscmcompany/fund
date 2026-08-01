-- Fund platform PostgreSQL schema
-- TimescaleDB operational data layer and event coordination.
--
-- Seven tables. Alpaca is the source of truth for fills, balances, buying power, and positions;
-- nothing here duplicates what the broker already knows authoritatively. PostgreSQL holds model
-- output, the long/short pair mapping, market history, and an audit trail.
--
-- Every DDL statement uses an idempotent form so this file can be re-run against a populated
-- database. Note the one thing that re-running cannot do: CREATE TABLE IF NOT EXISTS is a no-op
-- against a table that already exists with a different shape. Databases provisioned before the
-- rebuild must be dropped and recreated rather than upgraded in place, and equity_bars re-seeded
-- from the S3 parquet exports via seed_equity_bars.

CREATE EXTENSION IF NOT EXISTS timescaledb;
CREATE EXTENSION IF NOT EXISTS pg_cron;

-- ---------------------------------------------------------------------------
-- Market data
-- ---------------------------------------------------------------------------

-- equity_bars: rolling buffer of OHLCV bars at a declared interval.
--
-- bar_interval is part of the primary key so a single table carries daily and intraday bars
-- together. Only '1day' is written today -- the intraday migration is deferred, and the column
-- exists so that migration needs no table rewrite.
--
-- These are the canonical stored values, deliberately not Alpaca's timeframe spelling: Alpaca
-- writes '1Day' and '1Hour' where this column holds '1day' and '60min'. The BarInterval enum in
-- common::types owns both forms and the mapping between them, so no call site has to remember
-- which vocabulary it is in.
--
-- One retention policy covers every interval, which is the accepted cost of a single table. At 90
-- days, daily bars across the filtered universe are on the order of 10^5 rows while 1-minute bars
-- would be on the order of 10^7. Revisit the window when intraday writes begin.
--
-- Source: Alpaca REST, fetched by the application post-close and, independently, by the trainer.
CREATE TABLE IF NOT EXISTS equity_bars (
    ticker                        TEXT             NOT NULL,
    bar_interval                  TEXT             NOT NULL
        CHECK (bar_interval IN ('1min', '5min', '15min', '30min', '60min', '1day')),
    timestamp                     TIMESTAMPTZ      NOT NULL,
    open_price                    DOUBLE PRECISION NOT NULL,
    high_price                    DOUBLE PRECISION NOT NULL,
    low_price                     DOUBLE PRECISION NOT NULL,
    close_price                   DOUBLE PRECISION NOT NULL,
    volume                        BIGINT           NOT NULL,
    volume_weighted_average_price DOUBLE PRECISION,
    transactions                  BIGINT,
    inserted_at                   TIMESTAMPTZ      NOT NULL DEFAULT now(),
    PRIMARY KEY (ticker, bar_interval, timestamp)
);

SELECT create_hypertable('equity_bars', by_range('timestamp'), if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_equity_bars_interval_timestamp -- noqa: PG01
    ON equity_bars (bar_interval, timestamp DESC);
SELECT add_retention_policy('equity_bars', INTERVAL '90 days', if_not_exists => TRUE);

-- equity_details: ticker metadata used to constrain pair selection to cross-sector matches.
-- Seeded from data/equity_details.csv via seed_equity_details; refreshed by the post-close
-- market data sync.
CREATE TABLE IF NOT EXISTS equity_details (
    ticker    TEXT NOT NULL PRIMARY KEY,
    sector    TEXT NOT NULL DEFAULT 'NOT AVAILABLE',
    industry  TEXT NOT NULL DEFAULT 'NOT AVAILABLE'
);

-- ---------------------------------------------------------------------------
-- Model output
-- ---------------------------------------------------------------------------

-- equity_predictions: TiDE quantile output, one row per ticker per prediction batch.
-- Written by the pre-open predictions handler and read by the entry screen for the rest of the
-- session. Identity is (ticker, timestamp); a batch is identified by its shared correlation_id.
CREATE TABLE IF NOT EXISTS equity_predictions (
    correlation_id  UUID             NOT NULL,
    model_run_id    TEXT             NOT NULL,
    ticker          TEXT             NOT NULL,
    timestamp       TIMESTAMPTZ      NOT NULL,
    quantile_10     DOUBLE PRECISION NOT NULL,
    quantile_50     DOUBLE PRECISION NOT NULL,
    quantile_90     DOUBLE PRECISION NOT NULL,
    created_at      TIMESTAMPTZ      NOT NULL DEFAULT now(),
    PRIMARY KEY (ticker, timestamp)
);

SELECT create_hypertable('equity_predictions', by_range('timestamp'), if_not_exists => TRUE);
-- Retention is the nightly purge handler's job, not TimescaleDB's: rows must reach S3 via the
-- export before they are dropped, and only the handler knows whether that happened.
SELECT remove_retention_policy('equity_predictions', if_exists => TRUE);

-- ---------------------------------------------------------------------------
-- Positions
-- ---------------------------------------------------------------------------

-- equity_pairs: the long/short leg mapping, plus the signal that justified the entry.
--
-- This is the application's own record and deliberately not a position ledger -- Alpaca holds the
-- authoritative position, quantity, and fill. What Alpaca cannot answer is which long belongs with
-- which short and why they were opened together, so that is exactly what this table stores.
--
-- realized_profit_and_loss is a derived column, written by the post-close account sync from Alpaca
-- activity data rather than computed by the portfolio module. It is materialized so the dashboard
-- does not re-join activities to pairs on ticker and time window on every page load; Alpaca remains
-- the source.
CREATE TABLE IF NOT EXISTS equity_pairs (
    id                        UUID        PRIMARY KEY,
    pair_id                   TEXT        NOT NULL,
    long_ticker               TEXT        NOT NULL,
    short_ticker              TEXT        NOT NULL,
    -- Statistics rather than money, so double precision rather than NUMERIC. These are an OLS
    -- slope and two standardized scores; they carry no exact decimal value worth preserving, and
    -- typing them as NUMERIC would mean converting every one of them through a decimal
    -- representation on the way in and back out for arithmetic the strategy does in f64 anyway.
    hedge_ratio               DOUBLE PRECISION NOT NULL,
    entry_z_score             DOUBLE PRECISION NOT NULL,
    signal_strength           DOUBLE PRECISION NOT NULL,
    model_run_id              TEXT,
    status                    TEXT        NOT NULL CHECK (status IN ('open', 'closed')),
    opened_at                 TIMESTAMPTZ NOT NULL,
    closed_at                 TIMESTAMPTZ,
    close_reason              TEXT
        CHECK (close_reason IN ('convergence', 'stop_loss', 'end_of_day', 'position_missing')),
    realized_profit_and_loss  NUMERIC,
    UNIQUE (pair_id, opened_at),
    -- Closure is three columns that must agree, so the agreement is enforced here rather than in
    -- the handler that writes them. Without this the table accepts a closed pair with a null
    -- closed_at, which idx_equity_pairs_closed_at then sorts first under DESC -- so the least
    -- complete row would present as the most recent close.
    CONSTRAINT equity_pairs_closure_is_consistent CHECK (
        (status = 'open' AND closed_at IS NULL AND close_reason IS NULL)
        OR (status = 'closed' AND closed_at IS NOT NULL AND close_reason IS NOT NULL)
    )
);

-- The evaluation pass asks for open pairs every five minutes and for nothing else on this table
-- during a session, so the partial index is the one that matters.
CREATE INDEX IF NOT EXISTS idx_equity_pairs_open -- noqa: PG01
    ON equity_pairs (opened_at DESC)
    WHERE status = 'open';
CREATE INDEX IF NOT EXISTS idx_equity_pairs_closed_at -- noqa: PG01
    ON equity_pairs (closed_at DESC)
    WHERE status = 'closed';

-- ---------------------------------------------------------------------------
-- Account, as reported by Alpaca
-- ---------------------------------------------------------------------------

-- account_snapshots: one row per trading session, written by the post-close account sync.
--
-- The previous session's equity is the reference the next session's drawdown gate compares live
-- account equity against. That gate is the reason this table exists; everything else it carries is
-- for the dashboard.
CREATE TABLE IF NOT EXISTS account_snapshots (
    session_date        DATE        PRIMARY KEY,
    equity              NUMERIC     NOT NULL,
    last_equity         NUMERIC     NOT NULL,
    cash                NUMERIC     NOT NULL,
    buying_power        NUMERIC     NOT NULL,
    long_market_value   NUMERIC     NOT NULL,
    short_market_value  NUMERIC     NOT NULL,
    fetched_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- account_activities: Alpaca account activities, stored close to as returned.
--
-- The primary key is Alpaca's own activity ID, which makes the post-close sync idempotent by
-- construction: re-running it against an already-synced session conflicts on every row and changes
-- nothing. Columns are nullable where Alpaca omits them for non-trade activity types.
CREATE TABLE IF NOT EXISTS account_activities (
    id                TEXT        PRIMARY KEY,
    activity_type     TEXT        NOT NULL,
    transaction_time  TIMESTAMPTZ NOT NULL,
    ticker            TEXT,
    side              TEXT,
    quantity          NUMERIC,
    price             NUMERIC,
    order_id          TEXT,
    fetched_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_account_activities_transaction_time -- noqa: PG01
    ON account_activities (transaction_time DESC);
-- Attributing realized P&L back to a pair joins activities to legs on ticker within the pair's
-- open window, so ticker leads and time follows.
CREATE INDEX IF NOT EXISTS idx_account_activities_ticker_time -- noqa: PG01
    ON account_activities (ticker, transaction_time DESC);

-- ---------------------------------------------------------------------------
-- Event coordination
-- ---------------------------------------------------------------------------

-- events: append-only record of every command issued and every outcome reached.
--
-- Six commands, three outcomes. pg_cron writes the '<command>_requested' rows; the service writes
-- '<command>_completed' or '<command>_errored' when it finishes handling one. There is no
-- '_started' outcome -- nothing read it and the structured logs already cover it.
--
-- Completed payloads carry the summary of what happened -- pairs opened and closed with their exit
-- reasons, rows synced, the artifact the predictions ran against and its age. That is what makes
-- the nightly parquet export of this table worth reading afterwards.
--
-- This table is also the restart recovery mechanism. On startup the service scans for today's
-- '_requested' rows with no matching '_completed' or '_errored' and re-runs them, which is why
-- there is no separate consumer offset table.
CREATE TABLE IF NOT EXISTS events (
    id          BIGSERIAL   NOT NULL,
    event_type  TEXT        NOT NULL,
    payload     JSONB       NOT NULL DEFAULT '{}',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (id, created_at)
);

SELECT create_hypertable('events', by_range('created_at'), if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_events_type_id ON events (event_type, id); -- noqa: PG01
-- As with predictions: the purge handler drops these once the export has written them to S3.
SELECT remove_retention_policy('events', if_exists => TRUE);

-- notify_event: fires pg_notify on the 'events' channel after each insert.
--
-- The notification carries event_id, event_type, and normally the payload too, so the consumer can
-- dispatch and read structured data without a second round trip.
--
-- "Normally" is load-bearing. pg_notify rejects a payload of 8000 bytes or more, and this trigger
-- is AFTER INSERT FOR EACH ROW, so that rejection propagates and rolls back the emit_event insert
-- that caused it. The event row would be lost entirely -- and worse, the startup recovery scan
-- looks for requests with no terminal outcome, so a completed row lost this way reads as a command
-- that never finished and gets replayed.
--
-- That is reachable rather than theoretical: completed payloads deliberately carry per-run
-- summaries, and those grow with session activity. So the payload is included only when the whole
-- notification fits, and omitted with payload_truncated set otherwise. The consumer reads the row
-- by event_id in that case. The 7900-byte bound leaves room for the enclosing JSON.
CREATE OR REPLACE FUNCTION notify_event() RETURNS trigger AS $$
DECLARE
    full_notification TEXT;
BEGIN
    full_notification := json_build_object(
        'event_id',   NEW.id,
        'event_type', NEW.event_type,
        'payload',    NEW.payload
    )::text;

    IF octet_length(full_notification) < 7900 THEN
        PERFORM pg_notify('events', full_notification);
    ELSE
        PERFORM pg_notify('events',
            json_build_object(
                'event_id',          NEW.id,
                'event_type',        NEW.event_type,
                'payload_truncated', TRUE
            )::text
        );
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DO $do$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgname = 'events_notify'
          AND tgrelid = 'events'::regclass
    ) THEN
        CREATE TRIGGER events_notify
            AFTER INSERT ON events
            FOR EACH ROW EXECUTE FUNCTION notify_event();
    END IF;
END;
$do$;

-- emit_event: inserts an event row; the trigger fires pg_notify automatically.
CREATE OR REPLACE FUNCTION emit_event(event_type TEXT, payload JSONB) RETURNS void AS $$
BEGIN
    INSERT INTO events (event_type, payload) VALUES (event_type, payload);
END;
$$ LANGUAGE plpgsql;

-- ---------------------------------------------------------------------------
-- Schedule
-- ---------------------------------------------------------------------------
--
-- Six jobs. Every one fires across both DST-candidate UTC hours and gates on the actual Eastern
-- time in its WHERE clause, so a DST transition needs no schema re-apply. Every job is named for
-- the event it emits.
--
-- For the once-daily jobs the gate is lateness tolerance, not a window centred on a target: it
-- starts at the scheduled minute and runs long enough that pg_cron delayed by a busy database
-- still lands inside it. The only hard bound is that a gate must not also match the firing an hour
-- later, so anything under 60 minutes is safe.
--
-- Holidays are not gated here. The trading calendar lives in an in-memory cache refreshed from
-- Alpaca rather than in a table, so SQL cannot consult it; the handlers check the calendar and
-- return early. A market holiday therefore costs a day of no-op events, which is cheaper than
-- keeping a calendar table alive solely to answer a WHERE clause.

-- Pre-open predictions: weekdays at 09:00 Eastern, 30 minutes ahead of a regular open so
-- predictions are ready for the first evaluation pass. The handler resolves the newest model
-- artifact, runs inference, writes equity_predictions, and warms the calendar and universe caches.
DO $do$
BEGIN
    IF EXISTS (SELECT 1 FROM cron.job WHERE jobname = 'predictions-requested') THEN
        PERFORM cron.unschedule('predictions-requested');
    END IF;
    PERFORM cron.schedule(
        'predictions-requested',
        '0 13,14 * * 1-5',
        $$SELECT emit_event('predictions_requested', '{"reason": "pre_open"}'::jsonb)
          WHERE (now() AT TIME ZONE 'America/New_York')::time >= TIME '09:00'
            AND (now() AT TIME ZONE 'America/New_York')::time < TIME '09:20'$$
    );
END;
$do$;

-- Portfolio evaluation: every five minutes through the regular session.
--
-- The gate here is a window rather than a lateness tolerance, which is the one place this file
-- departs from the convention above -- a recurring job has no single target minute to be late for.
-- UTC hours 13-20 cover 09:30-15:40 Eastern in both DST regimes.
--
-- Evaluation stops at 15:40 while liquidation fires at 15:45: entries stop before exits do, and the
-- final pass gets five minutes to finish before the fail-safe. The handler additionally refuses to
-- open a pair within 30 minutes of the close, so the last productive entry pass is around 15:10.
DO $do$
BEGIN
    IF EXISTS (SELECT 1 FROM cron.job WHERE jobname = 'portfolio-evaluation-requested') THEN
        PERFORM cron.unschedule('portfolio-evaluation-requested');
    END IF;
    PERFORM cron.schedule(
        'portfolio-evaluation-requested',
        '*/5 13-20 * * 1-5',
        $$SELECT emit_event('portfolio_evaluation_requested', '{"reason": "scheduled"}'::jsonb)
          WHERE (now() AT TIME ZONE 'America/New_York')::time >= TIME '09:30'
            AND (now() AT TIME ZONE 'America/New_York')::time <= TIME '15:40'$$
    );
END;
$do$;

-- End-of-day liquidation: weekdays at 15:45 Eastern, 15 minutes before a regular close.
--
-- The book is flat overnight without exception, and this is the job that guarantees it. The gate
-- stops at 15:59 rather than taking a longer tolerance because liquidation submits market orders:
-- a trigger arriving after 16:00 would attempt to liquidate into a shut market, which is worse than
-- not firing, since the fail-safe would appear to have run.
--
-- On an early-close day this fires hours after the market shut and finds the book already flat,
-- because the evaluation handler reads the real close from the cached calendar and liquidates
-- against that. Liquidation is idempotent, so both paths firing is harmless.
DO $do$
BEGIN
    IF EXISTS (SELECT 1 FROM cron.job WHERE jobname = 'portfolio-liquidation-requested') THEN
        PERFORM cron.unschedule('portfolio-liquidation-requested');
    END IF;
    PERFORM cron.schedule(
        'portfolio-liquidation-requested',
        '45 19,20 * * 1-5',
        $$SELECT emit_event('portfolio_liquidation_requested', '{"reason": "scheduled"}'::jsonb)
          WHERE (now() AT TIME ZONE 'America/New_York')::time >= TIME '15:45'
            AND (now() AT TIME ZONE 'America/New_York')::time < TIME '15:59'$$
    );
END;
$do$;

-- Account sync: weekdays at 16:15 Eastern, 15 minutes after the close.
-- Fetches account balances and activities from Alpaca, writes account_snapshots and
-- account_activities, and attributes realized profit and loss back to the session's closed pairs.
DO $do$
BEGIN
    IF EXISTS (SELECT 1 FROM cron.job WHERE jobname = 'account-sync-requested') THEN
        PERFORM cron.unschedule('account-sync-requested');
    END IF;
    PERFORM cron.schedule(
        'account-sync-requested',
        '15 20,21 * * 1-5',
        $$SELECT emit_event('account_sync_requested', '{"reason": "post_close"}'::jsonb)
          WHERE (now() AT TIME ZONE 'America/New_York')::time >= TIME '16:15'
            AND (now() AT TIME ZONE 'America/New_York')::time < TIME '16:35'$$
    );
END;
$do$;

-- Market data sync: weekdays at 16:30 Eastern.
-- Pulls the session's bars and any equity detail changes from Alpaca into PostgreSQL, then chains
-- the database export on completion. The export is chained rather than scheduled so it cannot run
-- against a half-synced database; the purge runs inside the export handler once S3 has the data.
--
-- This path does not feed the trainer. The trainer fetches its own data from Alpaca on its own VM
-- and shares only the fetch code, so a failure here costs a backup rather than the next day's model.
DO $do$
BEGIN
    IF EXISTS (SELECT 1 FROM cron.job WHERE jobname = 'market-data-sync-requested') THEN
        PERFORM cron.unschedule('market-data-sync-requested');
    END IF;
    PERFORM cron.schedule(
        'market-data-sync-requested',
        '30 20,21 * * 1-5',
        $$SELECT emit_event('market_data_sync_requested', '{"reason": "post_close"}'::jsonb)
          WHERE (now() AT TIME ZONE 'America/New_York')::time >= TIME '16:30'
            AND (now() AT TIME ZONE 'America/New_York')::time < TIME '16:50'$$
    );
END;
$do$;

-- Daily cleanup of pg_cron run history: retain 7 days, which is enough for health monitoring and
-- debugging without letting the table grow unbounded.
DO $do$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM cron.job WHERE jobname = 'cron-run-details-cleanup') THEN
        PERFORM cron.schedule(
            'cron-run-details-cleanup',
            '0 3 * * *',
            $$DELETE FROM cron.job_run_details WHERE end_time < now() - interval '7 days'$$
        );
    END IF;
END;
$do$;
