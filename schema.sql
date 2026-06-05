-- Fund platform PostgreSQL schema
-- TimescaleDB operational data layer, model metadata, and job scheduling

CREATE EXTENSION IF NOT EXISTS timescaledb;
CREATE EXTENSION IF NOT EXISTS pg_cron;

-- pg_parquet is installed on Linux deployments via nix/pg_parquet.nix.
-- Silently skipped on environments where the extension is not available (e.g., macOS dev).
DO $do$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_available_extensions WHERE name = 'pg_parquet') THEN
        CREATE EXTENSION IF NOT EXISTS pg_parquet;
    ELSE
        RAISE WARNING 'pg_parquet is not available; quote archival is disabled';
    END IF;
END;
$do$;

-- equity_bars: Rolling buffer for equity bar data (last 90 days; ensemble needs 70-day lookback)
-- Source: Massive API (historical), Alpaca REST (EOD backfill)
CREATE TABLE IF NOT EXISTS equity_bars (
    ticker                        TEXT             NOT NULL,
    timestamp                     TIMESTAMPTZ      NOT NULL,
    open_price                    DOUBLE PRECISION NOT NULL,
    high_price                    DOUBLE PRECISION NOT NULL,
    low_price                     DOUBLE PRECISION NOT NULL,
    close_price                   DOUBLE PRECISION NOT NULL,
    volume                        BIGINT           NOT NULL,
    volume_weighted_average_price DOUBLE PRECISION,
    transactions                  BIGINT,
    inserted_at                   TIMESTAMPTZ      NOT NULL DEFAULT now(),
    PRIMARY KEY (ticker, timestamp)
);

SELECT create_hypertable('equity_bars', by_range('timestamp'), if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_equity_bars_inserted_at ON equity_bars (inserted_at);
CREATE INDEX IF NOT EXISTS idx_equity_bars_timestamp ON equity_bars (timestamp DESC);
SELECT add_retention_policy('equity_bars', INTERVAL '90 days', if_not_exists => TRUE);

-- Migrate equity_quotes column types from NUMERIC to DOUBLE PRECISION if running against an older schema.
DO $do$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'equity_quotes' AND column_name = 'bid_price' AND data_type = 'numeric'
    ) THEN
        ALTER TABLE equity_quotes
            ALTER COLUMN bid_price TYPE DOUBLE PRECISION,
            ALTER COLUMN ask_price TYPE DOUBLE PRECISION;
    END IF;
END;
$do$;

-- equity_quotes: intraday bid/ask rolling 24-hour buffer
-- Exported to S3 Parquet daily then purged; future use: replay simulation
CREATE TABLE IF NOT EXISTS equity_quotes (
    timestamp   TIMESTAMPTZ NOT NULL,
    ticker      TEXT        NOT NULL,
    bid_price   DOUBLE PRECISION NOT NULL,
    ask_price   DOUBLE PRECISION NOT NULL,
    bid_size    INTEGER     NOT NULL,
    ask_size    INTEGER     NOT NULL
);
SELECT create_hypertable('equity_quotes', by_range('timestamp'), if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_equity_quotes_ticker_timestamp ON equity_quotes (ticker, timestamp DESC);
SELECT add_retention_policy('equity_quotes', INTERVAL '1 day', if_not_exists => TRUE);

-- export_equity_quotes: exports the current trading day's quotes to S3 Parquet then purges them.
-- Reads the S3 bucket name from the app.bucket_name database GUC (set by data-manager on startup).
-- pg_parquet must be installed and S3 credentials must be available to the PostgreSQL process.
-- Returns early with a WARNING if pg_parquet is not installed.
CREATE OR REPLACE FUNCTION export_equity_quotes() RETURNS void AS $$
DECLARE
    export_date DATE := CURRENT_DATE;
    bucket_name  TEXT := current_setting('app.bucket_name', true);
    s3_path      TEXT;
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_parquet') THEN
        RAISE WARNING 'pg_parquet is not installed; skipping equity quotes export';
        RETURN;
    END IF;
    IF bucket_name IS NULL OR bucket_name = '' THEN
        RAISE EXCEPTION 'app.bucket_name GUC is not set; cannot export equity quotes';
    END IF;
    s3_path := format(
        's3://%s/data/equity/quotes/year=%s/month=%s/day=%s/data.parquet',
        bucket_name,
        to_char(export_date, 'YYYY'),
        to_char(export_date, 'MM'),
        to_char(export_date, 'DD')
    );
    EXECUTE format(
        'COPY (SELECT timestamp, ticker, bid_price, ask_price, bid_size, ask_size FROM equity_quotes WHERE timestamp >= %L AND timestamp < %L) TO %L',
        export_date::timestamptz,
        (export_date + 1)::timestamptz,
        s3_path
    );
    DELETE FROM equity_quotes
    WHERE timestamp >= export_date::timestamptz
      AND timestamp < (export_date + 1)::timestamptz;
END;
$$ LANGUAGE plpgsql;

-- equity_rebalance_sessions: groups one full rebalance cycle (allocation to orders)
CREATE TABLE IF NOT EXISTS equity_rebalance_sessions (
    id              UUID        PRIMARY KEY,
    triggered_at    TIMESTAMPTZ NOT NULL,
    trigger_reason  TEXT        NOT NULL,
    model_run_id    TEXT,       -- set by the training pipeline; references model_runs.run_id; nullable when unavailable
    completed_at    TIMESTAMPTZ,
    status          TEXT        NOT NULL
);

-- Drop NOT NULL on model_run_id if running against the old schema that had it NOT NULL.
DO $do$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'equity_rebalance_sessions'
          AND column_name = 'model_run_id'
          AND is_nullable = 'NO'
    ) THEN
        ALTER TABLE equity_rebalance_sessions ALTER COLUMN model_run_id DROP NOT NULL;
    END IF;
END;
$do$;

-- equity_pairs: one row per cointegrated pair per rebalance cycle
-- Entry signals (z_score, hedge_ratio, signal_strength) are recorded at the time of opening.
-- Matches the pairs_schema pandera definition and ClosedPair struct in data_manager/src/data.rs.
CREATE TABLE IF NOT EXISTS equity_pairs (
    id                         UUID        PRIMARY KEY,
    rebalance_id               UUID        NOT NULL REFERENCES equity_rebalance_sessions(id),
    pair_id                    TEXT        NOT NULL,
    long_ticker                TEXT        NOT NULL,
    short_ticker               TEXT        NOT NULL,
    z_score                    NUMERIC     NOT NULL,
    hedge_ratio                NUMERIC     NOT NULL,
    signal_strength            NUMERIC     NOT NULL,
    status                     TEXT        NOT NULL CHECK (status IN ('open', 'closed')),
    opened_at                  TIMESTAMPTZ NOT NULL,
    closed_at                  TIMESTAMPTZ,
    realized_profit_and_loss   NUMERIC,
    return_percent             NUMERIC,
    holding_days               INTEGER,
    close_reason               TEXT        CHECK (close_reason IN ('profit_taken', 'stop_loss', 'rebalance', 'end_of_day')),
    UNIQUE (pair_id, opened_at)
);

-- Add close_reason column if running against an older schema that lacked it.
DO $do$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'equity_pairs' AND column_name = 'close_reason'
    ) THEN
        ALTER TABLE equity_pairs ADD COLUMN close_reason TEXT
            CHECK (close_reason IN ('profit_taken', 'stop_loss', 'rebalance', 'end_of_day'));
    END IF;
END;
$do$;

-- Add return_percent column if running against an older schema that lacked it.
DO $do$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'equity_pairs' AND column_name = 'return_percent'
    ) THEN
        ALTER TABLE equity_pairs ADD COLUMN return_percent NUMERIC;
    END IF;
END;
$do$;

-- Drop equity_allocations (and equity_orders that reference it) if running against the old schema
-- (target_weight/reference_price columns). The new schema is incompatible: it requires FKs to
-- equity_pairs (a new table), so old rows cannot be preserved.
DO $do$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'equity_allocations' AND column_name = 'target_weight'
    ) THEN
        DROP TABLE IF EXISTS equity_orders;
        DROP TABLE equity_allocations;
    END IF;
END;
$do$;

-- equity_allocations: one row per ticker leg per rebalance cycle
-- side and action match PositionSide/PositionAction enums in portfolio_schema.py
-- quantity: whole-share intent for SHORT legs (nullable for LONG legs).
-- notional: dollar amount for LONG legs (nullable for SHORT legs).
-- CHECK ensures at least one of quantity or notional is set per row.
CREATE TABLE IF NOT EXISTS equity_allocations (
    id               UUID        PRIMARY KEY,
    rebalance_id     UUID        NOT NULL REFERENCES equity_rebalance_sessions(id),
    equity_pair_id   UUID        NOT NULL REFERENCES equity_pairs(id),
    generated_at     TIMESTAMPTZ NOT NULL,
    model_run_id     TEXT,       -- set by the training pipeline; references model_runs.run_id; nullable when unavailable
    ticker           TEXT        NOT NULL,
    side             TEXT        NOT NULL CHECK (side IN ('LONG', 'SHORT')),
    action           TEXT        NOT NULL CHECK (action IN ('OPEN_POSITION', 'CLOSE_POSITION', 'UNSPECIFIED')),
    dollar_amount    NUMERIC     NOT NULL,
    entry_price      NUMERIC,
    quantity         NUMERIC,
    notional         NUMERIC,
    CONSTRAINT equity_allocations_quantity_notional_check
        CHECK (quantity IS NOT NULL OR notional IS NOT NULL)
);

CREATE INDEX IF NOT EXISTS idx_equity_allocations_rebalance_id ON equity_allocations (rebalance_id);

-- Add quantity and notional columns with CHECK if running against an older schema.
DO $do$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'equity_allocations' AND column_name = 'quantity'
    ) THEN
        ALTER TABLE equity_allocations ADD COLUMN quantity NUMERIC;
        ALTER TABLE equity_allocations ADD COLUMN notional NUMERIC;
        ALTER TABLE equity_allocations
            ADD CONSTRAINT equity_allocations_quantity_notional_check
            CHECK (quantity IS NOT NULL OR notional IS NOT NULL) NOT VALID;
    END IF;
END;
$do$;

-- equity_orders: orders submitted to Alpaca, linked to allocations
CREATE TABLE IF NOT EXISTS equity_orders (
    id               UUID        PRIMARY KEY,
    allocation_id    UUID        NOT NULL REFERENCES equity_allocations(id),
    submitted_at     TIMESTAMPTZ NOT NULL,
    ticker           TEXT        NOT NULL,
    side             TEXT        NOT NULL,
    quantity         NUMERIC     NOT NULL,
    order_type       TEXT        NOT NULL,
    limit_price      NUMERIC,
    alpaca_order_id  TEXT        NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_equity_orders_allocation_id ON equity_orders (allocation_id);

-- Add FK from equity_orders.allocation_id to equity_allocations if running against a pre-migration schema
-- that had allocation_id as a plain UUID with no constraint.
DO $do$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.table_constraints
        WHERE table_name = 'equity_orders' AND constraint_name = 'equity_orders_allocation_id_fkey'
    ) THEN
        ALTER TABLE equity_orders
            ADD CONSTRAINT equity_orders_allocation_id_fkey
            FOREIGN KEY (allocation_id) REFERENCES equity_allocations(id);
    END IF;
END;
$do$;

-- Migrate equity_portfolio_snapshots from the old snapshot_date-keyed schema to the new
-- per-rebalance schema with snapshot_type.  Old rows cannot be preserved (incompatible PK).
DO $do$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'equity_portfolio_snapshots' AND column_name = 'snapshot_date'
    ) THEN
        DROP TABLE equity_portfolio_snapshots;
    END IF;
END;
$do$;

-- equity_portfolio_snapshots: per-rebalance portfolio state snapshots
-- 'intraday' rows are recorded after each live rebalance; gross_return and net_return are NULL.
-- 'end_of_day' rows are recorded once per trading day at market close; all columns are populated.
CREATE TABLE IF NOT EXISTS equity_portfolio_snapshots (
    id                   BIGSERIAL   NOT NULL PRIMARY KEY,
    snapshot_timestamp   TIMESTAMPTZ NOT NULL,
    snapshot_type        TEXT        NOT NULL CHECK (snapshot_type IN ('intraday', 'end_of_day')),
    net_asset_value      NUMERIC     NOT NULL,
    gross_return         NUMERIC,
    net_return           NUMERIC,
    total_slippage_cost  NUMERIC     NOT NULL,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Reconcile databases created before the eod -> end_of_day rename. No-ops on a fresh
-- database; on an existing one they migrate stored values and swap the CHECK constraint
-- so end_of_day INSERTs are accepted. The old constraint is dropped BEFORE the UPDATE so
-- the new value does not violate the still-active old check.
ALTER TABLE equity_portfolio_snapshots
    DROP CONSTRAINT IF EXISTS equity_portfolio_snapshots_snapshot_type_check;
UPDATE equity_portfolio_snapshots
    SET snapshot_type = 'end_of_day'
    WHERE snapshot_type = 'eod';
ALTER TABLE equity_portfolio_snapshots
    ADD CONSTRAINT equity_portfolio_snapshots_snapshot_type_check
    CHECK (snapshot_type IN ('intraday', 'end_of_day'));
-- Drop the pre-rename partial unique index; the renamed, UTC-anchored index is created below.
DROP INDEX IF EXISTS uq_equity_portfolio_snapshots_eod_date;

CREATE INDEX IF NOT EXISTS idx_equity_portfolio_snapshots_timestamp
    ON equity_portfolio_snapshots (snapshot_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_equity_portfolio_snapshots_type_timestamp
    ON equity_portfolio_snapshots (snapshot_type, snapshot_timestamp DESC);
CREATE UNIQUE INDEX IF NOT EXISTS uq_equity_portfolio_snapshots_end_of_day_date
    ON equity_portfolio_snapshots (((snapshot_timestamp AT TIME ZONE 'UTC')::date))
    WHERE snapshot_type = 'end_of_day';

-- equity_trades: fills from Alpaca websocket (Phase 3 — not yet wired)
CREATE TABLE IF NOT EXISTS equity_trades (
    timestamp               TIMESTAMPTZ NOT NULL,
    ticker                  TEXT        NOT NULL,
    order_id                UUID        NOT NULL,
    quantity                NUMERIC     NOT NULL,
    price                   NUMERIC     NOT NULL,
    side                    TEXT        NOT NULL,
    slippage_basis_points   NUMERIC
);

-- equity_details: Ticker metadata (sector, industry) seeded from S3 on first startup.
-- Ongoing updates are owned by data-manager when equity details are refreshed.
-- Source: data/equity/details/details.csv in the S3 bucket.
CREATE TABLE IF NOT EXISTS equity_details (
    ticker    TEXT NOT NULL PRIMARY KEY,
    sector    TEXT NOT NULL DEFAULT 'NOT AVAILABLE',
    industry  TEXT NOT NULL DEFAULT 'NOT AVAILABLE'
);

-- model_runs: Training metadata for model artifacts and evaluation metrics
CREATE TABLE IF NOT EXISTS model_runs (
    id                                  BIGSERIAL PRIMARY KEY,
    run_id                              TEXT NOT NULL UNIQUE,
    model_name                          TEXT NOT NULL DEFAULT 'tide',
    artifact_key                        TEXT,
    training_data_key                   TEXT,
    start_date                          DATE,
    end_date                            DATE,
    lookback_days                       INTEGER,
    status                              TEXT NOT NULL DEFAULT 'started',
    continuous_ranked_probability_score DOUBLE PRECISION,
    directional_accuracy                DOUBLE PRECISION,
    quantile_coverage                   DOUBLE PRECISION,
    drift_status                        TEXT,
    stage_counts                        JSONB,
    started_at                          TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at                        TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_model_runs_status ON model_runs (status);
CREATE INDEX IF NOT EXISTS idx_model_runs_started_at ON model_runs (started_at DESC);

-- scheduled_jobs: Job queue for pg_cron + LISTEN/NOTIFY
CREATE TABLE IF NOT EXISTS scheduled_jobs (
    id           BIGSERIAL    PRIMARY KEY,
    job_name     TEXT         NOT NULL,
    scheduled_at TIMESTAMPTZ  NOT NULL DEFAULT now(),
    claimed_at   TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    status       TEXT         NOT NULL DEFAULT 'pending'
                              CHECK (status IN ('pending', 'claimed', 'completed', 'failed')),
    result       TEXT
);

CREATE INDEX IF NOT EXISTS idx_scheduled_jobs_pending
    ON scheduled_jobs (job_name, status) WHERE status = 'pending';

-- Notify function: insert row then send NOTIFY on channel "jobs"
CREATE OR REPLACE FUNCTION schedule_job(name TEXT) RETURNS void AS $$
BEGIN
    INSERT INTO scheduled_jobs (job_name) VALUES (name);
    PERFORM pg_notify('jobs', name);
END;
$$ LANGUAGE plpgsql;

-- Nightly equity bar sync: weekdays at 05:00 UTC
DO $do$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM cron.job WHERE jobname = 'equity-bar-sync') THEN
        PERFORM cron.schedule('equity-bar-sync', '0 5 * * 1-5', $$SELECT schedule_job('equity-bar-sync')$$);
    END IF;
END;
$do$;

-- events: append-only outbox for cross-service event coordination
CREATE TABLE IF NOT EXISTS events (
    id          BIGSERIAL   NOT NULL,
    event_type  TEXT        NOT NULL,
    payload     JSONB       NOT NULL DEFAULT '{}',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (id, created_at)
);

SELECT create_hypertable('events', by_range('created_at'), if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_events_type_id ON events (event_type, id);
SELECT add_retention_policy('events', INTERVAL '30 days', if_not_exists => TRUE);

-- notify_event: fires pg_notify on the 'events' channel after each insert.
-- Payload is JSON with event_id, event_type, and the event payload so consumers
-- can update offsets and access structured data without an extra DB round-trip.
CREATE OR REPLACE FUNCTION notify_event() RETURNS trigger AS $$
BEGIN
    PERFORM pg_notify('events',
        json_build_object(
            'event_id',   NEW.id,
            'event_type', NEW.event_type,
            'payload',    NEW.payload
        )::text
    );
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

-- emit_event: inserts an event row; the trigger fires pg_notify automatically
CREATE OR REPLACE FUNCTION emit_event(event_type TEXT, payload JSONB) RETURNS void AS $$
BEGIN
    INSERT INTO events (event_type, payload) VALUES (event_type, payload);
END;
$$ LANGUAGE plpgsql;

-- event_consumer_offsets: tracks per-consumer polling progress for restart recovery
CREATE TABLE IF NOT EXISTS event_consumer_offsets (
    consumer_name  TEXT        PRIMARY KEY,
    last_event_id  BIGINT      NOT NULL DEFAULT 0,
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Rename predictions to equity_predictions on existing deployments.
DO $do$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.tables WHERE table_name = 'predictions'
    ) AND NOT EXISTS (
        SELECT 1 FROM information_schema.tables WHERE table_name = 'equity_predictions'
    ) THEN
        ALTER TABLE predictions RENAME TO equity_predictions;
    END IF;
END;
$do$;

-- equity_predictions: model output quantiles (7-day rolling buffer)
-- Columns match the Prediction struct in data_manager/src/data.rs and
-- the predictions_schema pandera definition in ensemble_manager.
-- timestamp is TIMESTAMPTZ; callers convert from Unix milliseconds at write time.
-- Identity is (ticker, timestamp) — the TimescaleDB primary key; no surrogate id column.
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
SELECT add_retention_policy('equity_predictions', INTERVAL '7 days', if_not_exists => TRUE);

-- Intraday rebalance check: every 5 minutes during market hours (14:00–20:55 UTC, weekdays).
-- 5 minutes is a conservative starting point; tighten to 1 minute if signal latency becomes an issue.
-- IMPORTANT: this interval must be >= FLUSH_INTERVAL_SECS in equity_quotes.rs (currently 5s).
-- A compile-time assertion in that file enforces the invariant — update both together.
-- Consumers (e.g., portfolio-manager) listen on the 'events' channel and query equity_quotes directly.
DO $do$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM cron.job WHERE jobname = 'intraday-check') THEN
        PERFORM cron.schedule(
            'intraday-check',
            '*/5 14-20 * * 1-5',
            $$SELECT emit_event('intraday_check', '{}')$$
        );
    END IF;
END;
$do$;

-- Remove legacy archive-quotes cron job and function on existing deployments.
DO $do$
BEGIN
    IF EXISTS (SELECT 1 FROM cron.job WHERE jobname = 'archive-quotes') THEN
        PERFORM cron.unschedule('archive-quotes');
    END IF;
END;
$do$;
DROP FUNCTION IF EXISTS archive_equity_quotes();

-- Daily equity quotes export: weekdays at 21:05 UTC (after intraday-check window ends at 20:55 UTC
-- and after 4 PM Eastern market close in both EDT and EST)
-- Only scheduled when pg_parquet is available; export_equity_quotes() also guards at runtime.
DO $do$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_parquet')
       AND NOT EXISTS (SELECT 1 FROM cron.job WHERE jobname = 'export-equity-quotes') THEN
        PERFORM cron.schedule(
            'export-equity-quotes',
            '5 21 * * 1-5',
            $$SELECT export_equity_quotes()$$
        );
    END IF;
END;
$do$;

-- liquidate_end_of_day: emits an event for portfolio-manager to close all open positions
-- and mark all open pairs as closed before the market close.
CREATE OR REPLACE FUNCTION liquidate_end_of_day() RETURNS void AS $$
BEGIN
    PERFORM emit_event('end_of_day_liquidation_requested', '{}');
END;
$$ LANGUAGE plpgsql;

-- End-of-day liquidation trigger: weekdays at 19:45 UTC (3:45 PM EDT, 15 minutes before market close).
-- Fires before the intraday-check window ends so the rebalance lockout window in portfolio-manager
-- prevents any new pairs from being opened after this point.
DO $do$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM cron.job WHERE jobname = 'end-of-day-liquidation') THEN
        PERFORM cron.schedule(
            'end-of-day-liquidation',
            '45 19 * * 1-5',
            $$SELECT liquidate_end_of_day()$$
        );
    END IF;
END;
$do$;

-- record_end_of_day_snapshot: emits an event for portfolio-manager to record the day's final NAV and compute returns.
CREATE OR REPLACE FUNCTION record_end_of_day_snapshot() RETURNS void AS $$
BEGIN
    PERFORM emit_event('end_of_day_snapshot_requested', '{}');
END;
$$ LANGUAGE plpgsql;

-- Nightly EOD snapshot trigger: weekdays at 21:15 UTC (after market close, after quote archival).
-- Runs first so the snapshot is persisted before export_trading_history runs at 21:45.
DO $do$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM cron.job WHERE jobname = 'record-end-of-day-snapshot') THEN
        PERFORM cron.schedule(
            'record-end-of-day-snapshot',
            '15 21 * * 1-5',
            $$SELECT record_end_of_day_snapshot()$$
        );
    END IF;
END;
$do$;

-- Reconcile databases created before the eod -> end_of_day rename: remove the old pg_cron
-- job so it does not fire alongside record-end-of-day-snapshot (which would produce
-- duplicate end-of-day snapshots and orphaned eod_snapshot_requested events).
DO $do$
BEGIN
    IF EXISTS (SELECT 1 FROM cron.job WHERE jobname = 'record-eod-snapshot') THEN
        PERFORM cron.unschedule('record-eod-snapshot');
    END IF;
END;
$do$;

-- Drop the pre-rename function and rename any not-yet-consumed events so the new consumer
-- still processes them (harmless on already-consumed rows; consumers track progress by id).
DROP FUNCTION IF EXISTS record_eod_snapshot();
UPDATE events
    SET event_type = 'end_of_day_snapshot_requested'
    WHERE event_type = 'eod_snapshot_requested';

-- export_equity_bars: exports equity_bars for the past 120 days to S3 Parquet for model training.
-- Reads the S3 bucket name from the app.bucket_name GUC (set by data-manager on startup).
-- Returns early with a WARNING if pg_parquet is not installed.
CREATE OR REPLACE FUNCTION export_equity_bars() RETURNS void AS $$
DECLARE
    export_date  DATE := CURRENT_DATE;
    bucket_name  TEXT := current_setting('app.bucket_name', true);
    s3_path      TEXT;
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_parquet') THEN
        RAISE WARNING 'pg_parquet is not installed; skipping equity bars export';
        RETURN;
    END IF;
    IF bucket_name IS NULL OR bucket_name = '' THEN
        RAISE EXCEPTION 'app.bucket_name GUC is not set; cannot export equity bars';
    END IF;
    s3_path := format(
        's3://%s/data/equity/bars/year=%s/month=%s/day=%s/data.parquet',
        bucket_name,
        to_char(export_date, 'YYYY'),
        to_char(export_date, 'MM'),
        to_char(export_date, 'DD')
    );
    EXECUTE format(
        'COPY (SELECT ticker, timestamp, open_price, high_price, low_price, close_price, volume, volume_weighted_average_price, transactions, inserted_at FROM equity_bars WHERE timestamp >= %L AND timestamp < %L) TO %L',
        export_date::timestamptz,
        (export_date + INTERVAL '1 day')::timestamptz,
        s3_path
    );
END;
$$ LANGUAGE plpgsql;

-- Remove legacy export-training-parquet cron job on existing deployments.
DO $do$
BEGIN
    IF EXISTS (SELECT 1 FROM cron.job WHERE jobname = 'export-training-parquet') THEN
        PERFORM cron.unschedule('export-training-parquet');
    END IF;
END;
$do$;

-- Nightly equity bars export: weekdays at 21:30 UTC
-- Only scheduled when pg_parquet is available; export_equity_bars() also guards at runtime.
DO $do$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_parquet')
       AND NOT EXISTS (SELECT 1 FROM cron.job WHERE jobname = 'export-equity-bars') THEN
        PERFORM cron.schedule(
            'export-equity-bars',
            '30 21 * * 1-5',
            $$SELECT export_equity_bars()$$
        );
    END IF;
END;
$do$;

-- export_trading_history: exports irreplaceable trading tables to S3 Parquet cold storage.
-- Reads the S3 bucket name from the app.bucket_name GUC (set by data-manager on startup).
-- Returns early with a WARNING if pg_parquet is not installed.
CREATE OR REPLACE FUNCTION export_trading_history() RETURNS void AS $$
DECLARE
    export_date  DATE := CURRENT_DATE;
    bucket_name  TEXT := current_setting('app.bucket_name', true);
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_parquet') THEN
        RAISE WARNING 'pg_parquet is not installed; skipping trading history export';
        RETURN;
    END IF;
    IF bucket_name IS NULL OR bucket_name = '' THEN
        RAISE EXCEPTION 'app.bucket_name GUC is not set; cannot export trading history';
    END IF;
    EXECUTE format(
        'COPY (SELECT * FROM equity_rebalance_sessions) TO %L',
        format('s3://%s/exports/equity/rebalance-sessions/year=%s/month=%s/day=%s/data.parquet',
               bucket_name, to_char(export_date, 'YYYY'), to_char(export_date, 'MM'), to_char(export_date, 'DD'))
    );
    EXECUTE format(
        'COPY (SELECT * FROM equity_pairs) TO %L',
        format('s3://%s/exports/equity/pairs/year=%s/month=%s/day=%s/data.parquet',
               bucket_name, to_char(export_date, 'YYYY'), to_char(export_date, 'MM'), to_char(export_date, 'DD'))
    );
    EXECUTE format(
        'COPY (SELECT * FROM equity_allocations) TO %L',
        format('s3://%s/exports/equity/allocations/year=%s/month=%s/day=%s/data.parquet',
               bucket_name, to_char(export_date, 'YYYY'), to_char(export_date, 'MM'), to_char(export_date, 'DD'))
    );
    EXECUTE format(
        'COPY (SELECT * FROM equity_orders) TO %L',
        format('s3://%s/exports/equity/orders/year=%s/month=%s/day=%s/data.parquet',
               bucket_name, to_char(export_date, 'YYYY'), to_char(export_date, 'MM'), to_char(export_date, 'DD'))
    );
    EXECUTE format(
        'COPY (SELECT * FROM equity_portfolio_snapshots) TO %L',
        format('s3://%s/exports/equity/portfolio-snapshots/year=%s/month=%s/day=%s/data.parquet',
               bucket_name, to_char(export_date, 'YYYY'), to_char(export_date, 'MM'), to_char(export_date, 'DD'))
    );
END;
$$ LANGUAGE plpgsql;

-- Remove legacy backup-trading-history cron job on existing deployments.
DO $do$
BEGIN
    IF EXISTS (SELECT 1 FROM cron.job WHERE jobname = 'backup-trading-history') THEN
        PERFORM cron.unschedule('backup-trading-history');
    END IF;
END;
$do$;

-- Nightly trading history export: weekdays at 21:45 UTC (after record-end-of-day-snapshot at 21:15 so today's snapshot is included).
-- Only scheduled when pg_parquet is available; export_trading_history() also guards at runtime.
DO $do$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_parquet')
       AND NOT EXISTS (SELECT 1 FROM cron.job WHERE jobname = 'export-trading-history') THEN
        PERFORM cron.schedule(
            'export-trading-history',
            '45 21 * * 1-5',
            $$SELECT export_trading_history()$$
        );
    END IF;
END;
$do$;

