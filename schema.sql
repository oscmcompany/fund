-- Fund platform PostgreSQL schema
-- Hot cache, model metadata, and job scheduling

-- equity_bars: Hot cache for recent equity bar data (TTL-managed, last 10 days)
CREATE TABLE IF NOT EXISTS equity_bars (
    ticker                        TEXT             NOT NULL,
    timestamp                     BIGINT           NOT NULL,
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

CREATE INDEX IF NOT EXISTS idx_equity_bars_timestamp ON equity_bars (timestamp);
CREATE INDEX IF NOT EXISTS idx_equity_bars_inserted_at ON equity_bars (inserted_at);

-- model_runs: Training metadata for model artifacts and evaluation metrics
CREATE TABLE IF NOT EXISTS model_runs (
    id                   BIGSERIAL PRIMARY KEY,
    run_id               TEXT NOT NULL UNIQUE,
    model_name           TEXT NOT NULL DEFAULT 'tide',
    artifact_key         TEXT,
    training_data_key    TEXT,
    start_date           DATE,
    end_date             DATE,
    lookback_days        INTEGER,
    status               TEXT NOT NULL DEFAULT 'started',
    continuous_ranked_probability_score DOUBLE PRECISION,
    directional_accuracy DOUBLE PRECISION,
    quantile_coverage    DOUBLE PRECISION,
    drift_status         TEXT,
    stage_counts         JSONB,
    started_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at         TIMESTAMPTZ
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

-- pg_cron extension for scheduled jobs
CREATE EXTENSION IF NOT EXISTS pg_cron;

-- Notify function: insert row then send NOTIFY on channel "jobs"
CREATE OR REPLACE FUNCTION schedule_job(name TEXT) RETURNS void AS $$
BEGIN
    INSERT INTO scheduled_jobs (job_name) VALUES (name);
    PERFORM pg_notify('jobs', name);
END;
$$ LANGUAGE plpgsql;

-- Nightly equity bar sync: weekdays at 5:00 AM UTC (covers EDT 1 AM ET)
SELECT cron.schedule('equity-bar-sync', '0 5 * * 1-5', $$SELECT schedule_job('equity-bar-sync')$$);

-- TTL cleanup: delete equity bars older than 10 days, daily at 3:00 AM UTC
SELECT cron.schedule('equity-bars-ttl', '0 3 * * *', $$DELETE FROM equity_bars WHERE timestamp < (EXTRACT(EPOCH FROM now() - interval '10 days') * 1000)::BIGINT$$);
