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

-- equity_bars, equity_predictions, and events are TimescaleDB hypertables; granting on the
-- hypertable covers its existing chunks and the ones created later, so there is nothing to re-run
-- when a chunk rolls over.
--
-- account_activities is read only for its transfer rows. Period returns are raw equity changes and
-- are wrong across a capital flow, so the page must be able to see that one arrived and withhold
-- the number. Realized profit and loss is still materialized onto equity_pairs by the post-close
-- sync, so nothing here re-joins activities to legs on a page load.
GRANT SELECT ON account_snapshots   TO dashboard_reader;
GRANT SELECT ON account_activities  TO dashboard_reader;
GRANT SELECT ON equity_pairs        TO dashboard_reader;
GRANT SELECT ON equity_predictions  TO dashboard_reader;
GRANT SELECT ON equity_bars         TO dashboard_reader;
GRANT SELECT ON events              TO dashboard_reader;
