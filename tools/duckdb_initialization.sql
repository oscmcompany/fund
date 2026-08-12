-- DuckDB initialization script
--
-- Loaded automatically by the start-duckdb devenv script. Creates read-only
-- views over the S3 data lake so production and export data is queryable
-- immediately.
--
-- Two prefixes, one owner each, and they are not interchangeable:
--
--   data/    the trainer's own archive. Fetched from Massive and written by the
--            trainer VM, which has no database. This is the training input and
--            the long-term record of equity bars and ticker metadata; it
--            accumulates for as long as the bucket keeps it.
--   exports/ the application's nightly export of the tables only it knows
--            about -- events, predictions, pairs, account state -- plus the
--            session log, which is the original those tables are derived from.
--            This is what the retention window would otherwise discard.
--
-- Bars and ticker metadata appear under data/ and nowhere else. They used to be
-- exported as well, from the same Massive endpoint by a second path, and two
-- writers of one fact is one too many.
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

-- Bars and ticker metadata are not here. They were exported under this prefix as well as archived
-- under data/, from the same Massive endpoint, and the trainer's archive is now their single
-- owner -- query `training_bars` and `training_details` above for them.

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

-- The session log is the application's own original: what it observed before it acted, appended to
-- local disk as it happened and sealed to Parquet after the close. Everything above is a fold or a
-- query over it, so this is where to go when the tables disagree with each other.
--
-- The envelope is columns and the body is a JSON string, so filter on event_type and reach into
-- payload with the JSON operators:
--
--   SELECT payload->>'ticker', payload->>'outcome'
--   FROM session_log WHERE event_type = 'order_resolved';
--
--   SELECT unnest(from_json(payload->'readings', '["json"]'))->>'ticker'
--   FROM session_log WHERE event_type = 'prices_observed';
--
-- correlation_id threads a command to everything it did: one command_finished, the pass it ran, the
-- prices it read, the orders it sent, the fills they produced. Joining on it is how a duration
-- becomes an answer about where the time went.
--
-- schema_version is 2 for anything written after 2026-08-12. Version 1 carried the pass's prices,
-- screen inputs, exclusions, and open book inline on an evaluation_pass record, and had no
-- command_finished, pair_opened, pair_closed, pair_attributed, or activity_observed.
.print 'Loading session_log...'
DROP VIEW IF EXISTS session_log;
CREATE OR REPLACE VIEW session_log AS
SELECT
    schema_version,
    event_id,
    correlation_id,
    event_type,
    session_date,
    -- Stored as epoch milliseconds so the Parquet column is a plain integer; recovered here, since
    -- every question worth asking of this table is ordered in time.
    epoch_ms(created_at) AS created_at,
    payload
FROM read_parquet(
    's3://' || getvariable('bucket') || '/exports/session_log/**/*.parquet',
    hive_partitioning = true
);

.print ''
.print 'DuckDB initialized. Views with errors above were skipped (no data in S3).'
.print 'Run .help for DuckDB commands, SHOW TABLES to list loaded views.'
