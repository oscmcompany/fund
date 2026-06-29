-- Idempotent: creates the dashboard_reader Postgres role and grants
-- read-only access to the seven tables queried by dashboard_service.
--
-- Apply against the fund database after PostgreSQL is running:
--   psql -h localhost -p 5432 -d fund -f tools/dashboard_reader_setup.sql
--
-- The role's login password is managed separately via secretspec
-- (profiles.dashboard, key: DATABASE_URL).

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'dashboard_reader') THEN
        CREATE ROLE dashboard_reader WITH LOGIN;
    END IF;
END
$$;

GRANT CONNECT ON DATABASE fund TO dashboard_reader;
GRANT USAGE ON SCHEMA public TO dashboard_reader;

GRANT SELECT ON equity_pairs               TO dashboard_reader;
GRANT SELECT ON equity_allocations         TO dashboard_reader;
GRANT SELECT ON equity_portfolio_snapshots TO dashboard_reader;
GRANT SELECT ON equity_bars                TO dashboard_reader;
GRANT SELECT ON equity_predictions         TO dashboard_reader;
GRANT SELECT ON model_runs                 TO dashboard_reader;
GRANT SELECT ON equity_rebalance_sessions  TO dashboard_reader;
