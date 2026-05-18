-- Fund platform PostgreSQL schema
-- TimescaleDB operational data layer

CREATE EXTENSION IF NOT EXISTS timescaledb;
CREATE EXTENSION IF NOT EXISTS pg_cron;

-- equity_prices: daily OHLCV bars (TimescaleDB hypertable)
-- Source: Massive API (historical), Alpaca REST (EOD backfill)
CREATE TABLE IF NOT EXISTS equity_prices (
    time    TIMESTAMPTZ      NOT NULL,
    symbol  TEXT             NOT NULL,
    open    DOUBLE PRECISION NOT NULL,
    high    DOUBLE PRECISION NOT NULL,
    low     DOUBLE PRECISION NOT NULL,
    close   DOUBLE PRECISION NOT NULL,
    volume  BIGINT           NOT NULL
);
SELECT create_hypertable('equity_prices', by_range('time'), if_not_exists => TRUE);
CREATE UNIQUE INDEX IF NOT EXISTS idx_equity_prices_symbol_time ON equity_prices (symbol, time DESC);

-- equity_quotes: intraday bid/ask rolling 24-hour buffer
-- Exported to S3 Parquet daily then purged; future use: replay simulation
CREATE TABLE IF NOT EXISTS equity_quotes (
    time        TIMESTAMPTZ NOT NULL,
    symbol      TEXT        NOT NULL,
    bid_price   NUMERIC     NOT NULL,
    ask_price   NUMERIC     NOT NULL,
    bid_size    INTEGER     NOT NULL,
    ask_size    INTEGER     NOT NULL
);
SELECT create_hypertable('equity_quotes', by_range('time'), if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_equity_quotes_symbol_time ON equity_quotes (symbol, time DESC);
SELECT add_retention_policy('equity_quotes', INTERVAL '1 day', if_not_exists => TRUE);

-- equity_orders: orders submitted to Alpaca, linked to allocations
CREATE TABLE IF NOT EXISTS equity_orders (
    id               UUID        PRIMARY KEY,
    allocation_id    UUID        NOT NULL,
    submitted_at     TIMESTAMPTZ NOT NULL,
    symbol           TEXT        NOT NULL,
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
    model_run_id     UUID        NOT NULL,
    symbol           TEXT        NOT NULL,
    target_weight    NUMERIC     NOT NULL,
    reference_price  NUMERIC     NOT NULL
);

-- equity_rebalance_sessions: groups one full rebalance cycle (allocation to orders)
CREATE TABLE IF NOT EXISTS equity_rebalance_sessions (
    id              UUID        PRIMARY KEY,
    triggered_at    TIMESTAMPTZ NOT NULL,
    trigger_reason  TEXT        NOT NULL,
    model_run_id    UUID        NOT NULL,
    completed_at    TIMESTAMPTZ,
    status          TEXT        NOT NULL
);

-- equity_portfolio_snapshots: nightly materialized portfolio state for historical charting
CREATE TABLE IF NOT EXISTS equity_portfolio_snapshots (
    snapshot_date        DATE    NOT NULL PRIMARY KEY,
    nav                  NUMERIC NOT NULL,
    gross_return         NUMERIC NOT NULL,
    net_return           NUMERIC NOT NULL,
    total_slippage_cost  NUMERIC NOT NULL
);

-- equity_trades: fills from Alpaca websocket (Phase 3 — not yet wired)
CREATE TABLE IF NOT EXISTS equity_trades (
    time          TIMESTAMPTZ NOT NULL,
    symbol        TEXT        NOT NULL,
    order_id      UUID        NOT NULL,
    quantity      NUMERIC     NOT NULL,
    price         NUMERIC     NOT NULL,
    side          TEXT        NOT NULL,
    slippage_bps  NUMERIC
);

-- model_runs: training metadata for model artifacts and evaluation metrics
CREATE TABLE IF NOT EXISTS model_runs (
    run_id               TEXT PRIMARY KEY,
    model_name           TEXT NOT NULL DEFAULT 'tide',
    artifact_key         TEXT,
    training_data_key    TEXT,
    start_date           DATE,
    end_date             DATE,
    lookback_days        INTEGER,
    status               TEXT NOT NULL DEFAULT 'started',
    crps                 DOUBLE PRECISION,
    directional_accuracy DOUBLE PRECISION,
    quantile_coverage    DOUBLE PRECISION,
    drift_status         TEXT,
    stage_counts         JSONB,
    started_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at         TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_model_runs_status ON model_runs (status);
CREATE INDEX IF NOT EXISTS idx_model_runs_started_at ON model_runs (started_at DESC);

-- scheduled_jobs: job queue for pg_cron + LISTEN/NOTIFY
CREATE TABLE IF NOT EXISTS scheduled_jobs (
    id           BIGSERIAL    PRIMARY KEY,
    job_name     TEXT         NOT NULL,
    scheduled_at TIMESTAMPTZ  NOT NULL DEFAULT now(),
    claimed_at   TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    status       TEXT         NOT NULL DEFAULT 'pending',
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

-- Nightly equity prices sync: weekdays at 5:00 AM UTC (covers EDT 1 AM ET)
SELECT cron.schedule('equity-prices-sync', '0 5 * * 1-5', $$SELECT schedule_job('equity-prices-sync')$$);
