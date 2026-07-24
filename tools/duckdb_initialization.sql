-- DuckDB initialization script
--
-- Loaded automatically by the start-duckdb devenv script. Creates read-only
-- views over the S3 data lake so production and export data is queryable
-- immediately.
--
-- Requirements:
--   - AWS credentials configured in the environment (e.g. ~/.aws/credentials)
--   - AWS_S3_BUCKET_NAME set (start-duckdb passes the bucket argument)

INSTALL aws;
LOAD aws;
INSTALL httpfs;
LOAD httpfs;
CALL load_aws_credentials();

SET VARIABLE bucket = getenv('AWS_S3_BUCKET_NAME');

.bail off

-- ---------------------------------------------------------------------------
-- Data lake views (raw ingested data)
-- ---------------------------------------------------------------------------

.print 'Loading equity_bars...'
DROP VIEW IF EXISTS equity_bars;
CREATE OR REPLACE VIEW equity_bars AS
SELECT *
FROM read_parquet(
    's3://' || getvariable('bucket') || '/data/equity/bars/**/*.parquet',
    hive_partitioning = true
);

.print 'Loading equity_quotes...'
DROP VIEW IF EXISTS equity_quotes;
CREATE OR REPLACE VIEW equity_quotes AS
SELECT *
FROM read_parquet(
    's3://' || getvariable('bucket') || '/data/equity/quotes/**/*.parquet',
    hive_partitioning = true
);

.print 'Loading equity_details...'
DROP VIEW IF EXISTS equity_details;
CREATE OR REPLACE VIEW equity_details AS
SELECT *
FROM read_csv(
    's3://' || getvariable('bucket') || '/data/equity/details/details.csv',
    auto_detect = true
);

-- ---------------------------------------------------------------------------
-- Export views (daily operational snapshots)
-- ---------------------------------------------------------------------------

.print 'Loading equity_predictions...'
DROP VIEW IF EXISTS equity_predictions;
CREATE OR REPLACE VIEW equity_predictions AS
SELECT *
FROM read_parquet(
    's3://' || getvariable('bucket') || '/exports/equity/predictions/**/*.parquet',
    hive_partitioning = true
);

.print 'Loading equity_rebalance_sessions...'
DROP VIEW IF EXISTS equity_rebalance_sessions;
CREATE OR REPLACE VIEW equity_rebalance_sessions AS
SELECT *
FROM read_parquet(
    's3://' || getvariable('bucket') || '/exports/equity/rebalance-sessions/**/*.parquet',
    hive_partitioning = true
);

.print 'Loading equity_pairs...'
DROP VIEW IF EXISTS equity_pairs;
CREATE OR REPLACE VIEW equity_pairs AS
SELECT *
FROM read_parquet(
    's3://' || getvariable('bucket') || '/exports/equity/pairs/**/*.parquet',
    hive_partitioning = true
);

.print 'Loading equity_allocations...'
DROP VIEW IF EXISTS equity_allocations;
CREATE OR REPLACE VIEW equity_allocations AS
SELECT *
FROM read_parquet(
    's3://' || getvariable('bucket') || '/exports/equity/allocations/**/*.parquet',
    hive_partitioning = true
);

.print 'Loading equity_orders...'
DROP VIEW IF EXISTS equity_orders;
CREATE OR REPLACE VIEW equity_orders AS
SELECT *
FROM read_parquet(
    's3://' || getvariable('bucket') || '/exports/equity/orders/**/*.parquet',
    hive_partitioning = true
);

.print 'Loading equity_portfolio_snapshots...'
DROP VIEW IF EXISTS equity_portfolio_snapshots;
CREATE OR REPLACE VIEW equity_portfolio_snapshots AS
SELECT *
FROM read_parquet(
    's3://' || getvariable('bucket') || '/exports/equity/portfolio-snapshots/**/*.parquet',
    hive_partitioning = true
);

.print 'Loading model_runs...'
DROP VIEW IF EXISTS model_runs;
CREATE OR REPLACE VIEW model_runs AS
SELECT *
FROM read_parquet(
    's3://' || getvariable('bucket') || '/exports/model-runs/**/*.parquet',
    hive_partitioning = true
);

.print 'Loading equity_reconciliation_events...'
DROP VIEW IF EXISTS equity_reconciliation_events;
CREATE OR REPLACE VIEW equity_reconciliation_events AS
SELECT *
FROM read_parquet(
    's3://' || getvariable('bucket') || '/exports/equity/reconciliation-events/**/*.parquet',
    hive_partitioning = true
);

.print ''
.print 'DuckDB initialized. Views with errors above were skipped (no data in S3).'
.print 'Run .help for DuckDB commands, SHOW TABLES to list loaded views.'
