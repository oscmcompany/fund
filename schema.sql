-- Fund platform PostgreSQL schema
-- TimescaleDB operational data layer, model metadata, and job scheduling

CREATE EXTENSION IF NOT EXISTS timescaledb;
CREATE EXTENSION IF NOT EXISTS pg_cron;

-- equity_bars: Rolling buffer for equity bar data (last 90 days; ensemble needs 70-day lookback)
-- Source: Massive API (historical), Alpaca REST (EOD backfill)
CREATE TABLE IF NOT EXISTS equity_bars (
    ticker                        TEXT             NOT NULL,
    timestamp                     TIMESTAMPTZ      NOT NULL,
    open_price                    DOUBLE PRECISION,
    high_price                    DOUBLE PRECISION,
    low_price                     DOUBLE PRECISION,
    close_price                   DOUBLE PRECISION,
    volume                        BIGINT,
    volume_weighted_average_price DOUBLE PRECISION,
    transactions                  BIGINT,
    inserted_at                   TIMESTAMPTZ      NOT NULL DEFAULT now(),
    PRIMARY KEY (ticker, timestamp)
);

SELECT create_hypertable('equity_bars', by_range('timestamp'), if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_equity_bars_inserted_at ON equity_bars (inserted_at);
CREATE INDEX IF NOT EXISTS idx_equity_bars_timestamp ON equity_bars (timestamp DESC);
SELECT add_retention_policy('equity_bars', INTERVAL '90 days', if_not_exists => TRUE);

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

-- equity_rebalance_sessions: groups one full rebalance cycle (allocation to orders)
CREATE TABLE IF NOT EXISTS equity_rebalance_sessions (
    id              UUID        PRIMARY KEY,
    triggered_at    TIMESTAMPTZ NOT NULL,
    trigger_reason  TEXT        NOT NULL,
    model_run_id    TEXT        NOT NULL, -- set by the training pipeline; references model_runs.run_id
    completed_at    TIMESTAMPTZ,
    status          TEXT        NOT NULL
);

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
    holding_days               INTEGER,
    UNIQUE (pair_id, opened_at)
);

-- equity_allocations: one row per ticker leg per rebalance cycle
-- side and action match PositionSide/PositionAction enums in portfolio_schema.py
CREATE TABLE IF NOT EXISTS equity_allocations (
    id               UUID        PRIMARY KEY,
    rebalance_id     UUID        NOT NULL REFERENCES equity_rebalance_sessions(id),
    equity_pair_id   UUID        NOT NULL REFERENCES equity_pairs(id),
    generated_at     TIMESTAMPTZ NOT NULL,
    model_run_id     TEXT        NOT NULL, -- set by the training pipeline; references model_runs.run_id
    ticker           TEXT        NOT NULL,
    side             TEXT        NOT NULL CHECK (side IN ('LONG', 'SHORT')),
    action           TEXT        NOT NULL CHECK (action IN ('OPEN_POSITION', 'CLOSE_POSITION', 'UNSPECIFIED')),
    dollar_amount    NUMERIC     NOT NULL,
    entry_price      NUMERIC
);

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

-- equity_portfolio_snapshots: nightly materialized portfolio state for historical charting
CREATE TABLE IF NOT EXISTS equity_portfolio_snapshots (
    snapshot_date        DATE        NOT NULL PRIMARY KEY,
    net_asset_value      NUMERIC     NOT NULL,
    gross_return         NUMERIC     NOT NULL,
    net_return           NUMERIC     NOT NULL,
    total_slippage_cost  NUMERIC     NOT NULL,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now()
);

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

-- notify_event: fires pg_notify on the 'events' channel after each insert
CREATE OR REPLACE FUNCTION notify_event() RETURNS trigger AS $$
BEGIN
    PERFORM pg_notify('events', NEW.event_type);
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

-- predictions: model output quantiles (7-day rolling buffer)
-- Columns match the Prediction struct in data_manager/src/data.rs and
-- the predictions_schema pandera definition in ensemble_manager.
-- timestamp is TIMESTAMPTZ; callers convert from Unix milliseconds at write time.
CREATE TABLE IF NOT EXISTS predictions (
    id              BIGSERIAL        NOT NULL,
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

SELECT create_hypertable('predictions', by_range('timestamp'), if_not_exists => TRUE);
SELECT add_retention_policy('predictions', INTERVAL '7 days', if_not_exists => TRUE);

-- Daily inference trigger: weekdays at 14:00 UTC
DO $do$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM cron.job WHERE jobname = 'predictions-requested') THEN
        PERFORM cron.schedule(
            'predictions-requested',
            '0 14 * * 1-5',
            $$SELECT emit_event('predictions_requested', '{}')$$
        );
    END IF;
END;
$do$;
