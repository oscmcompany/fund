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
--            journal, which is the original those tables are derived from.
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

-- Every stock split Massive reports, historical and announced, refreshed whole each night. Named
-- in full because "split" alone collides with the train/validation split the trainer beside it
-- means by the word. One object
-- rather than a partition per date: the feed revises and cancels announced splits, so a row that
-- disappears is a cancellation and the current table is the only authoritative answer.
--
-- split_from shares become split_to shares -- a two-for-one forward split is 1 -> 2 and a
-- one-for-three reverse split is 3 -> 1. execution_date is an Eastern calendar date and may be in
-- the future. first_seen is when this job first saw the row, not when the split was announced.
--
-- On a bucket where the trainer has not yet run, the object does not exist and this one CREATE
-- prints an IO error and is skipped; every other view still loads, and this one appears after the
-- first refresh. training_details has the same fixed-key exposure.
.print 'Loading training_stock_splits...'
DROP VIEW IF EXISTS training_stock_splits;
CREATE OR REPLACE VIEW training_stock_splits AS
SELECT
    id,
    ticker,
    CAST(execution_date AS DATE) AS execution_date,
    split_from,
    split_to,
    to_timestamp(first_seen / 1000.0) AS first_seen
FROM read_parquet(
    's3://' || getvariable('bucket') || '/data/equity/corporate_actions/splits.parquet'
);

-- The dates a symbol's series may not be read across. `related_ticker` is where a rename continues
-- or which company a spinoff distributed, and is null for the reasons that name neither.
.print 'Loading training_series_boundaries...'
DROP VIEW IF EXISTS training_series_boundaries;
CREATE OR REPLACE VIEW training_series_boundaries AS
SELECT
    id,
    ticker,
    CAST(date AS DATE) AS boundary_date,
    CAST(process_date AS DATE) AS process_date,
    reason,
    related_ticker,
    to_timestamp(first_seen / 1000.0) AS first_seen
FROM read_parquet(
    's3://' || getvariable('bucket') || '/data/equity/corporate_actions/boundaries.parquet'
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

-- The journal is the application's own original: what it observed before it acted, appended to
-- local disk as it happened and sealed to Parquet after the close. Everything above is a fold or a
-- query over it, so this is where to go when the tables disagree with each other.
--
-- The envelope is columns and the body is a JSON string, so filter on event_type and reach into
-- payload with the JSON operators:
--
--   SELECT payload->>'ticker', payload->>'outcome'
--   FROM journal WHERE event_type = 'order_resolved';
--
--   SELECT unnest(from_json(payload->'readings', '["json"]'))->>'ticker'
--   FROM journal WHERE event_type = 'prices_observed';
--
-- correlation_id threads a command to everything it did: one command_finished, the pass it ran, the
-- prices it read, the orders it sent, the fills they produced. Joining on it is how a duration
-- becomes an answer about where the time went.
--
-- schema_version is 3 or 4; v1 and v2 were development and their objects are gone. Two payload
-- fields changed shape at v4, so a query reaching into either has to branch on the version:
--
--   quote_rejection was the bare reason string 'stale_quote' or 'wide_quote'. It is now an object
--   whose 'reason' key holds that same string alongside the reading that produced it --
--   age_seconds and limit_seconds for a stale quote, relative_spread and limit for a wide one.
--   Under v3 the reading is simply gone; there is nothing to recover it from.
--
--   early_closes[].session_close was 'HH:MM' and is now 'HH:MM:SS'.
--
-- Fields are added within a version as well as across one, and an absent key reads as NULL either
-- way, so "the build predated this field" and "there was nothing to record" are the same answer.
-- Anything counting a field should bound the query by the first session that has it rather than
-- treating an early NULL as a zero.
.print 'Loading journal...'
-- Dropped as well as `journal`: this script runs against a persistent ~/lab.duckdb, so a view
-- created before the rename outlives it and still points at a prefix that no longer exists.
DROP VIEW IF EXISTS session_log;
DROP VIEW IF EXISTS journal;
CREATE OR REPLACE VIEW journal AS
SELECT
    schema_version,
    event_id,
    correlation_id,
    event_type,
    session_date,
    -- Stored as epoch milliseconds so the Parquet column is a plain integer. to_timestamp, not
    -- epoch_ms: the first returns a TIMESTAMPTZ anchored to UTC, the second a bare TIMESTAMP that
    -- reads as whatever zone the session happens to be in. An instant is always UTC here, and
    -- session_date beside it is always the Eastern trading day.
    to_timestamp(timestamp / 1000.0) AS timestamp,
    payload,
    -- From the key layout rather than the file contents, so a filter on them skips whole files.
    -- session_date is inside each Parquet, so filtering on that alone opens every one, and the
    -- archive grows by a file per trading day forever.
    year,
    month,
    day
FROM read_parquet(
    's3://' || getvariable('bucket') || '/exports/journal/**/*.parquet',
    hive_partitioning = true
);

.print ''
.print 'DuckDB initialized. Views with errors above were skipped (no data in S3).'
.print 'Run .help for DuckDB commands, SHOW TABLES to list loaded views.'
