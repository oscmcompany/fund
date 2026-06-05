-- Fund platform PostgreSQL schema
-- TimescaleDB operational data layer, model metadata, and job scheduling

CREATE EXTENSION timescaledb;
CREATE EXTENSION pg_cron;

-- equity_bars: Rolling buffer for equity bar data (last 90 days; ensemble needs 70-day lookback)
-- Source: Massive API (historical), Alpaca REST (EOD backfill)
CREATE TABLE equity_bars (
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

SELECT create_hypertable('equity_bars', by_range('timestamp'));
CREATE INDEX idx_equity_bars_inserted_at ON equity_bars (inserted_at); -- noqa: PG01
CREATE INDEX idx_equity_bars_timestamp ON equity_bars (timestamp DESC); -- noqa: PG01
SELECT add_retention_policy('equity_bars', INTERVAL '90 days');

-- equity_quotes: intraday bid/ask rolling 24-hour buffer
-- Exported to S3 Parquet daily then purged; future use: replay simulation
CREATE TABLE equity_quotes (
    timestamp   TIMESTAMPTZ NOT NULL,
    ticker      TEXT        NOT NULL,
    bid_price   DOUBLE PRECISION NOT NULL,
    ask_price   DOUBLE PRECISION NOT NULL,
    bid_size    INTEGER     NOT NULL,
    ask_size    INTEGER     NOT NULL
);
SELECT create_hypertable('equity_quotes', by_range('timestamp'));
CREATE INDEX idx_equity_quotes_ticker_timestamp ON equity_quotes (ticker, timestamp DESC); -- noqa: PG01
SELECT add_retention_policy('equity_quotes', INTERVAL '1 day');

-- equity_rebalance_sessions: groups one full rebalance cycle (allocation to orders)
CREATE TABLE equity_rebalance_sessions (
    id              UUID        PRIMARY KEY,
    triggered_at    TIMESTAMPTZ NOT NULL,
    trigger_reason  TEXT        NOT NULL,
    model_run_id    TEXT,       -- set by the training pipeline; references model_runs.run_id; nullable when unavailable
    completed_at    TIMESTAMPTZ,
    status          TEXT        NOT NULL CHECK (status IN ('completed', 'failed'))
);

-- equity_pairs: one row per cointegrated pair per rebalance cycle
-- Entry signals (z_score, hedge_ratio, signal_strength) are recorded at the time of opening.
-- Matches the pairs_schema pandera definition and ClosedPair struct in data_manager/src/data.rs.
CREATE TABLE equity_pairs (
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
    UNIQUE (pair_id, opened_at)
);

-- equity_allocations: one row per ticker leg per rebalance cycle
-- side and action match PositionSide/PositionAction enums in portfolio_schema.py
-- quantity: whole-share intent for SHORT legs (nullable for LONG legs).
-- notional: dollar amount for LONG legs (nullable for SHORT legs).
-- CHECK ensures at least one of quantity or notional is set per row.
CREATE TABLE equity_allocations (
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

CREATE INDEX idx_equity_allocations_rebalance_id ON equity_allocations (rebalance_id); -- noqa: PG01

-- equity_orders: orders submitted to Alpaca, linked to allocations
CREATE TABLE equity_orders (
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

CREATE INDEX idx_equity_orders_allocation_id ON equity_orders (allocation_id); -- noqa: PG01

-- equity_portfolio_snapshots: per-rebalance portfolio state snapshots
-- 'intraday' rows are recorded after each live rebalance; gross_return and net_return are NULL.
-- 'end_of_day' rows are recorded once per trading day at market close; all columns are populated.
CREATE TABLE equity_portfolio_snapshots (
    id                   BIGSERIAL   NOT NULL PRIMARY KEY,
    snapshot_timestamp   TIMESTAMPTZ NOT NULL,
    snapshot_type        TEXT        NOT NULL CHECK (snapshot_type IN ('intraday', 'end_of_day')),
    net_asset_value      NUMERIC     NOT NULL,
    gross_return         NUMERIC,
    net_return           NUMERIC,
    total_slippage_cost  NUMERIC     NOT NULL,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_equity_portfolio_snapshots_timestamp -- noqa: PG01
    ON equity_portfolio_snapshots (snapshot_timestamp DESC);
CREATE INDEX idx_equity_portfolio_snapshots_type_timestamp -- noqa: PG01
    ON equity_portfolio_snapshots (snapshot_type, snapshot_timestamp DESC);
CREATE UNIQUE INDEX uq_equity_portfolio_snapshots_end_of_day_date -- noqa: PG01
    ON equity_portfolio_snapshots (((snapshot_timestamp AT TIME ZONE 'UTC')::date))
    WHERE snapshot_type = 'end_of_day';

-- equity_trades: fills from Alpaca websocket (Phase 3 — not yet wired)
CREATE TABLE equity_trades (
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
CREATE TABLE equity_details (
    ticker    TEXT NOT NULL PRIMARY KEY,
    sector    TEXT NOT NULL DEFAULT 'NOT AVAILABLE',
    industry  TEXT NOT NULL DEFAULT 'NOT AVAILABLE'
);

-- model_runs: Training metadata for model artifacts and evaluation metrics
CREATE TABLE model_runs (
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

CREATE INDEX idx_model_runs_status ON model_runs (status); -- noqa: PG01
CREATE INDEX idx_model_runs_started_at ON model_runs (started_at DESC); -- noqa: PG01

-- scheduled_jobs: Job queue for pg_cron + LISTEN/NOTIFY
CREATE TABLE scheduled_jobs (
    id           BIGSERIAL    PRIMARY KEY,
    job_name     TEXT         NOT NULL,
    scheduled_at TIMESTAMPTZ  NOT NULL DEFAULT now(),
    claimed_at   TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    status       TEXT         NOT NULL DEFAULT 'pending'
                              CHECK (status IN ('pending', 'claimed', 'completed', 'failed')),
    result       TEXT
);

CREATE INDEX idx_scheduled_jobs_pending -- noqa: PG01
    ON scheduled_jobs (job_name, status) WHERE status = 'pending';

-- Notify function: insert row then send NOTIFY on channel "jobs"
CREATE OR REPLACE FUNCTION schedule_job(name TEXT) RETURNS void AS $$
BEGIN
    INSERT INTO scheduled_jobs (job_name) VALUES (name);
    PERFORM pg_notify('jobs', name);
END;
$$ LANGUAGE plpgsql;

-- Nightly equity bar sync: weekdays at 05:00 UTC
SELECT cron.schedule('equity-bar-sync', '0 5 * * 1-5', $$SELECT schedule_job('equity-bar-sync')$$);

-- events: append-only outbox for cross-service event coordination
CREATE TABLE events (
    id          BIGSERIAL   NOT NULL,
    event_type  TEXT        NOT NULL,
    payload     JSONB       NOT NULL DEFAULT '{}',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (id, created_at)
);

SELECT create_hypertable('events', by_range('created_at'));
CREATE INDEX idx_events_type_id ON events (event_type, id); -- noqa: PG01
SELECT add_retention_policy('events', INTERVAL '30 days');

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

CREATE TRIGGER events_notify
    AFTER INSERT ON events
    FOR EACH ROW EXECUTE FUNCTION notify_event();

-- emit_event: inserts an event row; the trigger fires pg_notify automatically
CREATE OR REPLACE FUNCTION emit_event(event_type TEXT, payload JSONB) RETURNS void AS $$
BEGIN
    INSERT INTO events (event_type, payload) VALUES (event_type, payload);
END;
$$ LANGUAGE plpgsql;

-- event_consumer_offsets: tracks per-consumer polling progress for restart recovery
CREATE TABLE event_consumer_offsets (
    consumer_name  TEXT        PRIMARY KEY,
    last_event_id  BIGINT      NOT NULL DEFAULT 0,
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- equity_predictions: model output quantiles (7-day rolling buffer)
-- Columns match the Prediction struct in data_manager/src/data.rs and
-- the predictions_schema pandera definition in ensemble_manager.
-- timestamp is TIMESTAMPTZ; callers convert from Unix milliseconds at write time.
-- Identity is (ticker, timestamp) — the TimescaleDB primary key; no surrogate id column.
CREATE TABLE equity_predictions (
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

SELECT create_hypertable('equity_predictions', by_range('timestamp'));
SELECT add_retention_policy('equity_predictions', INTERVAL '7 days');

-- Intraday rebalance check: every 5 minutes during market hours (14:00–20:55 UTC, weekdays).
-- 5 minutes is a conservative starting point; tighten to 1 minute if signal latency becomes an issue.
-- IMPORTANT: this interval must be >= FLUSH_INTERVAL_SECS in equity_quotes.rs (currently 5s).
-- A compile-time assertion in that file enforces the invariant — update both together.
-- Consumers (e.g., portfolio-manager) listen on the 'events' channel and query equity_quotes directly.
SELECT cron.schedule('intraday-check', '*/5 14-20 * * 1-5', $$SELECT emit_event('intraday_check', '{}')$$);

-- record_end_of_day_snapshot: emits an event for portfolio-manager to record the day's final NAV and compute returns.
CREATE OR REPLACE FUNCTION record_end_of_day_snapshot() RETURNS void AS $$
BEGIN
    PERFORM emit_event('end_of_day_snapshot_requested', '{}');
END;
$$ LANGUAGE plpgsql;

-- Nightly EOD snapshot trigger: weekdays at 21:15 UTC (after market close, after quote archival).
-- Runs first so the snapshot is persisted before export_trading_history runs at 21:45.
SELECT cron.schedule('record-end-of-day-snapshot', '15 21 * * 1-5', $$SELECT record_end_of_day_snapshot()$$);

-- Daily equity quotes export: weekdays at 21:05 UTC (after intraday-check window ends at 20:55 UTC
-- and after 4 PM Eastern market close in both EDT and EST).
-- Triggers a Rust export task in data_manager via the jobs channel.
SELECT cron.schedule('export-equity-quotes', '5 21 * * 1-5', $$SELECT schedule_job('export-equity-quotes')$$);

-- Nightly equity bars export: weekdays at 21:30 UTC.
-- Triggers a Rust export task in data_manager via the jobs channel.
SELECT cron.schedule('export-equity-bars', '30 21 * * 1-5', $$SELECT schedule_job('export-equity-bars')$$);

-- Nightly trading history export: weekdays at 21:45 UTC (after record-end-of-day-snapshot at 21:15
-- so today's snapshot is included).
-- Triggers a Rust export task in data_manager via the jobs channel.
SELECT cron.schedule('export-trading-history', '45 21 * * 1-5', $$SELECT schedule_job('export-trading-history')$$);
