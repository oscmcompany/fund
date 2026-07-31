-- Fund platform PostgreSQL schema
-- TimescaleDB operational data layer, model metadata, and event coordination

CREATE EXTENSION IF NOT EXISTS timescaledb;
CREATE EXTENSION IF NOT EXISTS pg_cron;

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
CREATE INDEX IF NOT EXISTS idx_equity_bars_inserted_at ON equity_bars (inserted_at); -- noqa: PG01
CREATE INDEX IF NOT EXISTS idx_equity_bars_timestamp ON equity_bars (timestamp DESC); -- noqa: PG01
SELECT add_retention_policy('equity_bars', INTERVAL '90 days', if_not_exists => TRUE);

-- equity_rebalance_sessions: groups one full rebalance cycle (allocation to orders)
CREATE TABLE IF NOT EXISTS equity_rebalance_sessions (
    id              UUID        PRIMARY KEY,
    triggered_at    TIMESTAMPTZ NOT NULL,
    trigger_reason  TEXT        NOT NULL,
    model_run_id    TEXT,       -- set by the training pipeline; references model_runs.run_id; nullable when unavailable
    completed_at    TIMESTAMPTZ,
    status          TEXT        NOT NULL
);

-- equity_pairs: one row per cointegrated pair per rebalance cycle
-- Entry signals (z_score, hedge_ratio, signal_strength) are recorded at the time of opening.
-- Matches the pairs_schema pandera definition and ClosedPair struct in src/data/types.rs.
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
    close_reason               TEXT        CHECK (close_reason IN ('profit_taken', 'stop_loss', 'end_of_day', 'reconciliation_alpaca_missing')),
    UNIQUE (pair_id, opened_at)
);

-- Idempotent constraint replacement: updates close_reason CHECK to include reconciliation_alpaca_missing
-- for existing deployments where CREATE TABLE was a no-op.
DO $do$
BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'equity_pairs_close_reason_check' AND conrelid = 'equity_pairs'::regclass
    ) THEN
        ALTER TABLE equity_pairs DROP CONSTRAINT equity_pairs_close_reason_check;
    END IF;
    ALTER TABLE equity_pairs ADD CONSTRAINT equity_pairs_close_reason_check
        CHECK (close_reason IN ('profit_taken', 'stop_loss', 'end_of_day', 'reconciliation_alpaca_missing')) NOT VALID;
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

CREATE INDEX IF NOT EXISTS idx_equity_allocations_rebalance_id ON equity_allocations (rebalance_id); -- noqa: PG01

-- equity_orders: orders submitted to Alpaca, linked to allocations
-- allocation_id is nullable: submitted orders are tracked before allocations exist.
-- status tracks the order lifecycle: submitted → filled or cancelled.
CREATE TABLE IF NOT EXISTS equity_orders (
    id               UUID        PRIMARY KEY,
    allocation_id    UUID        REFERENCES equity_allocations(id),
    submitted_at     TIMESTAMPTZ NOT NULL,
    ticker           TEXT        NOT NULL,
    side             TEXT        NOT NULL CHECK (side IN ('LONG', 'SHORT')),
    quantity         NUMERIC     NOT NULL,
    order_type       TEXT        NOT NULL,
    limit_price      NUMERIC,
    alpaca_order_id  TEXT        NOT NULL,
    status           TEXT        NOT NULL DEFAULT 'filled' CHECK (status IN ('submitted', 'filled', 'cancelled')),
    filled_at        TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_equity_orders_allocation_id ON equity_orders (allocation_id); -- noqa: PG01

-- Idempotent constraint backfill: adds the side CHECK to existing deployments where CREATE TABLE was a no-op.
-- NOT VALID skips scanning existing rows; safe to re-run.
DO $do$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'equity_orders_side_check' AND conrelid = 'equity_orders'::regclass
    ) THEN
        ALTER TABLE equity_orders ADD CONSTRAINT equity_orders_side_check CHECK (side IN ('LONG', 'SHORT')) NOT VALID;
    END IF;
END;
$do$;

-- Idempotent column backfill: adds status and filled_at columns, relaxes allocation_id NOT NULL
-- for existing deployments where CREATE TABLE was a no-op.
DO $do$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'equity_orders' AND column_name = 'status'
    ) THEN
        ALTER TABLE equity_orders ADD COLUMN status TEXT NOT NULL DEFAULT 'filled';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'equity_orders' AND column_name = 'filled_at'
    ) THEN
        ALTER TABLE equity_orders ADD COLUMN filled_at TIMESTAMPTZ;
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'equity_orders_status_check' AND conrelid = 'equity_orders'::regclass
    ) THEN
        ALTER TABLE equity_orders ADD CONSTRAINT equity_orders_status_check
            CHECK (status IN ('submitted', 'filled', 'cancelled')) NOT VALID;
    END IF;
    -- Relax allocation_id NOT NULL so submitted orders can be tracked before allocations exist.
    ALTER TABLE equity_orders ALTER COLUMN allocation_id DROP NOT NULL;
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

CREATE INDEX IF NOT EXISTS idx_equity_portfolio_snapshots_timestamp -- noqa: PG01
    ON equity_portfolio_snapshots (snapshot_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_equity_portfolio_snapshots_type_timestamp -- noqa: PG01
    ON equity_portfolio_snapshots (snapshot_type, snapshot_timestamp DESC);
CREATE UNIQUE INDEX IF NOT EXISTS uq_equity_portfolio_snapshots_end_of_day_date -- noqa: PG01
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
-- Ongoing updates are owned by the data service when equity details are refreshed.
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

CREATE INDEX IF NOT EXISTS idx_model_runs_status ON model_runs (status); -- noqa: PG01
CREATE INDEX IF NOT EXISTS idx_model_runs_started_at ON model_runs (started_at DESC); -- noqa: PG01

-- equity_reconciliation_events: audit trail for DB-Alpaca state discrepancies.
-- Append-only during detection; resolved_at is updated when corrective action succeeds.
-- Designed for Phase 2b continuous reconciliation without schema migration.
CREATE TABLE IF NOT EXISTS equity_reconciliation_events (
    id                BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    detected_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    event_type        TEXT        NOT NULL,
    ticker            TEXT        NOT NULL,
    expected_quantity NUMERIC,
    actual_quantity   NUMERIC,
    equity_pair_id    UUID        REFERENCES equity_pairs(id),
    alpaca_order_id   TEXT,
    action_taken      TEXT        NOT NULL,
    resolved_at       TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_equity_reconciliation_events_unresolved -- noqa: PG01
    ON equity_reconciliation_events (detected_at)
    WHERE resolved_at IS NULL;

-- Nightly equity bar sync: weekdays at 05:00 UTC
DO $do$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM cron.job WHERE jobname = 'equity-bars-sync-requested') THEN
        PERFORM cron.schedule('equity-bars-sync-requested', '0 5 * * 1-5', $$SELECT emit_event('equity_bars_sync_requested', '{}')$$);
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
CREATE INDEX IF NOT EXISTS idx_events_type_id ON events (event_type, id); -- noqa: PG01
SELECT remove_retention_policy('events', if_exists => TRUE);

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

-- equity_predictions: model output quantiles (purged nightly by unified database purge)
-- Columns match the Prediction struct in src/data/types.rs and
-- the predictions_schema pandera definition in the inference module.
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
SELECT remove_retention_policy('equity_predictions', if_exists => TRUE);

-- Session cron jobs: one pre-market prediction request plus a session-start
-- trigger. Consumers listen on the 'events' channel; live quotes reach the
-- portfolio service over the in-memory broadcast channel and are never
-- persisted, so no table mediates this path.
DO $do$
BEGIN
    -- Pre-market prediction request: weekdays at 09:00 Eastern, 30 minutes ahead
    -- of a regular open so predictions are ready for the first evaluation pass.
    -- Fires in UTC hours 13-14 to cover both EDT and EST, gated on the actual
    -- Eastern time so DST needs no schema re-apply. Holidays are handled by the
    -- inference consumer, not here.
    --
    -- The gate width is lateness tolerance, not a window centred on a target:
    -- it starts at the scheduled minute and runs 20 minutes. pg_cron delayed
    -- past it -- a busy database at startup, a backed-up job queue -- misses the
    -- day entirely. The only hard bound is that a gate must not also match the
    -- other firing an hour later, so anything under 60 minutes is safe.
    --
    -- Every job that emits an event is named for the event it emits.
    IF EXISTS (SELECT 1 FROM cron.job WHERE jobname = 'equity-predictions-requested') THEN
        PERFORM cron.unschedule('equity-predictions-requested');
    END IF;
    PERFORM cron.schedule(
        'equity-predictions-requested',
        '0 13,14 * * 1-5',
        $$SELECT emit_event('equity_predictions_requested', '{"reason": "pre_market"}'::jsonb)
          WHERE (now() AT TIME ZONE 'America/New_York')::time >= TIME '09:00'
            AND (now() AT TIME ZONE 'America/New_York')::time < TIME '09:20'$$
    );

    -- There is deliberately no evaluation job here. A five-minute heartbeat used to
    -- run a full rebalance pass whether or not anything had changed: up to 78 passes
    -- a session, each re-ranking the candidate reservoir and re-pricing every open leg
    -- to usually conclude that nothing should happen. Intraday work is driven by the
    -- live-quote evaluator instead, which emits portfolio_evaluation_requested only
    -- when a spread actually crosses a close threshold. Cron opens and closes the
    -- session; it does not drive the work in between.

    -- Session start: weekdays at 09:25 Eastern, five minutes ahead of a regular
    -- open. The portfolio consumer confirms against Alpaca's clock that the
    -- market actually trades today, so a holiday costs one no-op event; the
    -- schedule itself only needs to exclude weekends. Fires in UTC hours 13-14
    -- to cover both EDT and EST, gated on the actual Eastern time so DST needs
    -- no schema re-apply.
    --
    -- 20 minutes of lateness tolerance, per the note above. The gate start stays
    -- at 09:25: it is five minutes before the bell on purpose, so the consumer
    -- can reconcile, confirm the session, arm the liquidation timer, and build
    -- the portfolio before the open.
    IF EXISTS (SELECT 1 FROM cron.job WHERE jobname = 'trading-session-started') THEN
        PERFORM cron.unschedule('trading-session-started');
    END IF;
    PERFORM cron.schedule(
        'trading-session-started',
        '25 13,14 * * 1-5',
        $$SELECT emit_event('trading_session_started', '{"reason": "scheduled_open"}'::jsonb)
          WHERE (now() AT TIME ZONE 'America/New_York')::time >= TIME '09:25'
            AND (now() AT TIME ZONE 'America/New_York')::time < TIME '09:45'$$
    );
END;
$do$;

-- End-of-day liquidation trigger: weekdays at 3:45 PM Eastern Time (15 minutes before market close).
-- Fires in the UTC range 19-20 (covering 15:45 EDT and 15:45 EST) with an inline WHERE clause
-- that gates on the actual Eastern time, so DST is handled correctly year-round without needing
-- to re-apply the schema after a DST transition.
-- This is the fail-safe path only. On trading_session_started the portfolio consumer reads the real
-- close from Alpaca and arms a one-shot timer for 15 minutes before it, which pulls liquidation
-- forward on early-close days when this fixed 15:45 schedule would fire hours after the market shut.
-- This job still fires unconditionally so an unreachable Alpaca clock cannot leave positions open
-- overnight; liquidation is idempotent, so both paths firing is harmless. The payload names this
-- path so the handler can tell it from the timer: a fail-safe that fires and finds the book already
-- flat is the healthy case and says so, while one that finds open positions means the primary path
-- failed and is worth a warning.
--
-- The gate stops at 15:59 rather than taking the full 20 minutes of tolerance the session jobs get.
-- Neither handle_portfolio_liquidation nor run_end_of_day_liquidation checks the market session, and
-- DELETE /v2/positions submits market orders, so a trigger arriving after 16:00 would attempt
-- liquidation into a shut market -- worse than not firing, because the fail-safe would appear to
-- have run.
DO $do$
BEGIN
    IF EXISTS (SELECT 1 FROM cron.job WHERE jobname = 'portfolio-liquidation-requested') THEN
        PERFORM cron.unschedule('portfolio-liquidation-requested');
    END IF;
    PERFORM cron.schedule(
        'portfolio-liquidation-requested',
        '45 19-20 * * 1-5',
        $$SELECT emit_event('portfolio_liquidation_requested', '{"reason": "fail_safe_schedule"}'::jsonb)
          WHERE (now() AT TIME ZONE 'America/New_York')::time >= TIME '15:45'
            AND (now() AT TIME ZONE 'America/New_York')::time < TIME '15:59'$$
    );
END;
$do$;

-- Nightly database export: weekdays at 21:45 UTC.
-- Exports all ephemeral tables to S3 Parquet, then chains backup and purge via events:
-- database_export → database_backup → database_purge.
DO $do$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM cron.job WHERE jobname = 'database-export-requested') THEN
        PERFORM cron.schedule(
            'database-export-requested',
            '45 21 * * 1-5',
            $$SELECT emit_event('database_export_requested', json_build_object('date', CURRENT_DATE::text)::jsonb)$$
        );
    END IF;
END;
$do$;

-- Scheduled health check: weekdays at 10:00 Eastern.
-- Runs the same pg_cron job validation and event freshness checks the data service runs at startup.
-- Both are needed and they answer different questions: startup asks "did we miss anything while we
-- were down", this asks "has anything stopped while we were up". The application runs under a tmux
-- restart loop and can stay up for weeks, during which a silently stopped cron job produced no
-- warning from anywhere.
-- Timed after the session should have started, so a missed trading_session_started is caught the
-- same morning rather than the next day. Fires in UTC hours 14-15 to cover both EDT and EST, gated
-- on the actual Eastern time so DST needs no schema re-apply.
DO $do$
BEGIN
    IF EXISTS (SELECT 1 FROM cron.job WHERE jobname = 'scheduler-health-check-requested') THEN
        PERFORM cron.unschedule('scheduler-health-check-requested');
    END IF;
    PERFORM cron.schedule(
        'scheduler-health-check-requested',
        '0 14,15 * * 1-5',
        $$SELECT emit_event('scheduler_health_check_requested', '{"reason": "scheduled"}'::jsonb)
          WHERE (now() AT TIME ZONE 'America/New_York')::time >= TIME '10:00'
            AND (now() AT TIME ZONE 'America/New_York')::time < TIME '10:20'$$
    );
END;
$do$;

-- Model artifact check: weekdays at 06:30 UTC, thirty minutes after the trainer's 06:00 crontab
-- entry on its own VM. UTC-anchored like the bars sync rather than gated on Eastern time, because
-- it tracks the trainer's schedule and not the market's.
-- The trainer has no database connection -- PostgreSQL is bound to 127.0.0.1 -- so the application
-- cannot enforce the ordering of sync, training, and prediction across the VM boundary. It observes
-- it instead: this check turns a training run that did not happen into an event rather than silence.
DO $do$
BEGIN
    IF EXISTS (SELECT 1 FROM cron.job WHERE jobname = 'model-artifact-check-requested') THEN
        PERFORM cron.unschedule('model-artifact-check-requested');
    END IF;
    PERFORM cron.schedule(
        'model-artifact-check-requested',
        '30 6 * * 1-5',
        $$SELECT emit_event('model_artifact_check_requested', '{"reason": "scheduled"}'::jsonb)$$
    );
END;
$do$;

-- Daily cleanup of pg_cron run history: retain 7 days to keep the table small
-- while providing enough history for health monitoring and debugging.
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
