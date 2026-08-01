-- DuckDB initialization script
--
-- Loaded automatically by the start-duckdb devenv script. Creates read-only
-- views over the S3 data lake so production and export data is queryable
-- immediately.
--
-- Two prefixes, and they are not interchangeable:
--
--   data/    the trainer's own archive. Fetched from Alpaca and written by the
--            trainer VM, which has no database. This is the training input and
--            it accumulates for as long as the bucket keeps it.
--   exports/ the application's nightly export of its PostgreSQL tables. This is
--            what the 90-day retention window would otherwise discard.
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
-- Trainer archive (data/) -- the model's training input
-- ---------------------------------------------------------------------------

.print 'Loading training_bars...'
DROP VIEW IF EXISTS training_bars;
CREATE OR REPLACE VIEW training_bars AS
SELECT *
FROM read_parquet(
    's3://' || getvariable('bucket') || '/data/equity/bars/**/*.parquet',
    hive_partitioning = true
);

.print 'Loading training_details...'
DROP VIEW IF EXISTS training_details;
CREATE OR REPLACE VIEW training_details AS
SELECT *
FROM read_csv(
    's3://' || getvariable('bucket') || '/data/equity/details/details.csv',
    auto_detect = true
);

-- ---------------------------------------------------------------------------
-- Nightly exports (exports/) -- one view per exported table
-- ---------------------------------------------------------------------------

.print 'Loading equity_bars...'
DROP VIEW IF EXISTS equity_bars;
CREATE OR REPLACE VIEW equity_bars AS
SELECT *
FROM read_parquet(
    's3://' || getvariable('bucket') || '/exports/equity/bars/**/*.parquet',
    hive_partitioning = true
);

.print 'Loading equity_details...'
DROP VIEW IF EXISTS equity_details;
CREATE OR REPLACE VIEW equity_details AS
SELECT *
FROM read_parquet(
    's3://' || getvariable('bucket') || '/exports/equity/details/**/*.parquet',
    hive_partitioning = true
);

.print 'Loading equity_predictions...'
DROP VIEW IF EXISTS equity_predictions;
CREATE OR REPLACE VIEW equity_predictions AS
SELECT *
FROM read_parquet(
    's3://' || getvariable('bucket') || '/exports/equity/predictions/**/*.parquet',
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

.print 'Loading account_snapshots...'
DROP VIEW IF EXISTS account_snapshots;
CREATE OR REPLACE VIEW account_snapshots AS
SELECT *
FROM read_parquet(
    's3://' || getvariable('bucket') || '/exports/account/snapshots/**/*.parquet',
    hive_partitioning = true
);

.print 'Loading account_activities...'
DROP VIEW IF EXISTS account_activities;
CREATE OR REPLACE VIEW account_activities AS
SELECT *
FROM read_parquet(
    's3://' || getvariable('bucket') || '/exports/account/activities/**/*.parquet',
    hive_partitioning = true
);

-- The event log is the record of every command issued and every outcome reached, and completed
-- payloads carry the per-run summaries. It is the most useful table here for asking what the
-- strategy actually did on a given day.
.print 'Loading events...'
DROP VIEW IF EXISTS events;
CREATE OR REPLACE VIEW events AS
SELECT *
FROM read_parquet(
    's3://' || getvariable('bucket') || '/exports/events/**/*.parquet',
    hive_partitioning = true
);

.print ''
.print 'DuckDB initialized. Views with errors above were skipped (no data in S3).'
.print 'Run .help for DuckDB commands, SHOW TABLES to list loaded views.'
