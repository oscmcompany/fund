-- Idempotent: creates the dashboard_reader Postgres role and grants read-only access to exactly
-- the tables the dashboard queries.
--
-- The role is the reason the dashboard can be a separate process rather than a page served by the
-- service: it holds SELECT and nothing else, so a bug in the rendering path cannot write to the
-- database the strategy trades from. Grant nothing here that the dashboard does not read.
--
-- Applied automatically by the schema process during devenv startup. Can also be run manually:
--   psql -h localhost -p 5432 -d fund -f tools/dashboard_reader_setup.sql

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'dashboard_reader') THEN
        CREATE ROLE dashboard_reader WITH LOGIN;
    END IF;
END
$$;

GRANT CONNECT ON DATABASE fund TO dashboard_reader;
GRANT USAGE ON SCHEMA public TO dashboard_reader;

-- Five tables, one per section of the page. equity_bars, equity_predictions, and events are
-- TimescaleDB hypertables; granting on the hypertable covers its existing chunks and the ones
-- created later, so there is nothing to re-run when a chunk rolls over.
--
-- account_activities is deliberately absent. Realized profit and loss is materialized onto
-- equity_pairs by the post-close sync precisely so the dashboard does not re-join activities to
-- legs on every page load, and a grant for a table nothing reads is a grant nobody will remember to
-- remove.
GRANT SELECT ON account_snapshots   TO dashboard_reader;
GRANT SELECT ON equity_pairs        TO dashboard_reader;
GRANT SELECT ON equity_predictions  TO dashboard_reader;
GRANT SELECT ON equity_bars         TO dashboard_reader;
GRANT SELECT ON events              TO dashboard_reader;
