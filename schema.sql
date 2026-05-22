-- Fund platform PostgreSQL schema
-- TimescaleDB operational data layer, model metadata, and job scheduling

CREATE EXTENSION IF NOT EXISTS timescaledb;
CREATE EXTENSION IF NOT EXISTS pg_cron;

-- equity_bars: Hot cache for recent equity bar data (TTL-managed, last 10 days)
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
SELECT add_retention_policy('equity_bars', INTERVAL '10 days', if_not_exists => TRUE);

-- equity_quotes: intraday bid/ask rolling 24-hour buffer
-- Exported to S3 Parquet daily then purged; future use: replay simulation
CREATE TABLE IF NOT EXISTS equity_quotes (
    timestamp   TIMESTAMPTZ NOT NULL,
    ticker      TEXT        NOT NULL,
    bid_price   NUMERIC     NOT NULL,
    ask_price   NUMERIC     NOT NULL,
    bid_size    INTEGER     NOT NULL,
    ask_size    INTEGER     NOT NULL
);
SELECT create_hypertable('equity_quotes', by_range('timestamp'), if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_equity_quotes_ticker_timestamp ON equity_quotes (ticker, timestamp DESC);
SELECT add_retention_policy('equity_quotes', INTERVAL '1 day', if_not_exists => TRUE);

-- equity_orders: orders submitted to Alpaca, linked to allocations
CREATE TABLE IF NOT EXISTS equity_orders (
    id               UUID        PRIMARY KEY,
    allocation_id    UUID        NOT NULL,
    submitted_at     TIMESTAMPTZ NOT NULL,
    ticker           TEXT        NOT NULL,
    side             TEXT        NOT NULL,
    quantity         NUMERIC     NOT NULL,
    order_type       TEXT        NOT NULL,
    limit_price      NUMERIC,
    alpaca_order_id  TEXT        NOT NULL
);

-- equity_allocations: model output (intended target weights) at generation time
CREATE TABLE IF NOT EXISTS equity_allocations (
    id               UUID        PRIMARY KEY,
    rebalance_id     UUID        NOT NULL,
    generated_at     TIMESTAMPTZ NOT NULL,
    model_run_id     TEXT        NOT NULL, -- set by the training pipeline; references model_runs.run_id
    ticker           TEXT        NOT NULL,
    target_weight    NUMERIC     NOT NULL,
    reference_price  NUMERIC     NOT NULL
);

-- equity_rebalance_sessions: groups one full rebalance cycle (allocation to orders)
CREATE TABLE IF NOT EXISTS equity_rebalance_sessions (
    id              UUID        PRIMARY KEY,
    triggered_at    TIMESTAMPTZ NOT NULL,
    trigger_reason  TEXT        NOT NULL,
    model_run_id    TEXT        NOT NULL, -- set by the training pipeline; references model_runs.run_id
    completed_at    TIMESTAMPTZ,
    status          TEXT        NOT NULL
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

-- Remove legacy TTL job: timestamp column is now TIMESTAMPTZ; retention handled by TimescaleDB policy.
DO $do$
BEGIN
    IF EXISTS (SELECT 1 FROM cron.job WHERE jobname = 'equity-bars-ttl') THEN
        PERFORM cron.unschedule('equity-bars-ttl');
    END IF;
END;
$do$;

-- Nightly equity bar sync: weekdays at 5:00 AM UTC (covers EDT 1 AM ET)
DO $do$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM cron.job WHERE jobname = 'equity-bar-sync') THEN
        PERFORM cron.schedule('equity-bar-sync', '0 5 * * 1-5', $$SELECT schedule_job('equity-bar-sync')$$);
    END IF;
END;
$do$;
