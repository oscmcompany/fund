-- One-time production migration: spell out "eod" -> "end_of_day".
--
-- WHY THIS EXISTS
-- This project has no migration runner. `schema.sql` is reapplied idempotently
-- (CREATE ... IF NOT EXISTS, CREATE OR REPLACE) on every data-manager startup, so
-- editing schema.sql alone does NOT reconcile an existing database:
--   * the stored snapshot_type value stays 'eod' and the existing CHECK still
--     forbids 'end_of_day', so portfolio-manager INSERTs would be REJECTED;
--   * `CREATE OR REPLACE FUNCTION record_end_of_day_snapshot` leaves the old
--     record_eod_snapshot() in place;
--   * `cron.schedule('record-end-of-day-snapshot', ...) IF NOT EXISTS` leaves the
--     old 'record-eod-snapshot' job in place, so BOTH jobs fire -> duplicate EOD
--     snapshots and orphaned 'eod_snapshot_requested' events.
--
-- DEPLOY ORDER (important)
--   1. Run this migration against the live `fund` database FIRST.
--   2. THEN deploy the new application code + schema.sql.
-- Run it outside the 21:15 UTC EOD window so no end-of-day event is in flight.
--
-- HOW TO RUN (from the exe.dev VM, or any host with psql access to the fund DB)
--   psql -h localhost -p 5432 -d fund \
--     -f migrations/2026-05-29-spell-out-end-of-day-identifiers.sql \
--     --set ON_ERROR_STOP=on
--
-- This migration is idempotent: re-running it after success is a no-op.

BEGIN;

-- 1. Migrate the stored snapshot_type value on existing rows.
UPDATE equity_portfolio_snapshots
   SET snapshot_type = 'end_of_day'
 WHERE snapshot_type = 'eod';

-- 2. Swap the CHECK constraint to the new allowed value set.
--    (Postgres auto-named the inline CHECK <table>_<column>_check.)
ALTER TABLE equity_portfolio_snapshots
    DROP CONSTRAINT IF EXISTS equity_portfolio_snapshots_snapshot_type_check;
ALTER TABLE equity_portfolio_snapshots
    ADD CONSTRAINT equity_portfolio_snapshots_snapshot_type_check
    CHECK (snapshot_type IN ('intraday', 'end_of_day'));

-- 3. Rebuild the partial unique index under its new name and predicate.
--    Original definition: ON equity_portfolio_snapshots ((snapshot_timestamp::date))
--    WHERE snapshot_type = 'eod'.
DROP INDEX IF EXISTS uq_equity_portfolio_snapshots_eod_date;
CREATE UNIQUE INDEX IF NOT EXISTS uq_equity_portfolio_snapshots_end_of_day_date
    ON equity_portfolio_snapshots ((snapshot_timestamp::date))
    WHERE snapshot_type = 'end_of_day';

-- 4. Remove the old pg_cron job and stored function so the renamed versions do
--    not run alongside them.
DO $unschedule$
BEGIN
    IF EXISTS (SELECT 1 FROM cron.job WHERE jobname = 'record-eod-snapshot') THEN
        PERFORM cron.unschedule('record-eod-snapshot');
    END IF;
END;
$unschedule$;
DROP FUNCTION IF EXISTS record_eod_snapshot();

-- 5. Create the renamed function and cron job now, so they exist immediately and
--    independently of a data-manager restart (matches schema.sql exactly, so a
--    later schema.sql reapply is a no-op).
CREATE OR REPLACE FUNCTION record_end_of_day_snapshot() RETURNS void AS $$
BEGIN
    PERFORM emit_event('end_of_day_snapshot_requested', '{}');
END;
$$ LANGUAGE plpgsql;

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

-- 6. Rename any not-yet-consumed events so the new consumer still processes them.
--    (Harmless on already-consumed rows; consumers track progress by id, not type.)
UPDATE events
   SET event_type = 'end_of_day_snapshot_requested'
 WHERE event_type = 'eod_snapshot_requested';

-- 7. Sanity check: this must report 0 before COMMIT.
DO $check$
DECLARE
    straggler_count integer;
BEGIN
    SELECT count(*) INTO straggler_count
      FROM equity_portfolio_snapshots
     WHERE snapshot_type = 'eod';
    IF straggler_count <> 0 THEN
        RAISE EXCEPTION 'Migration incomplete: % rows still have snapshot_type = ''eod''', straggler_count;
    END IF;
END;
$check$;

COMMIT;
