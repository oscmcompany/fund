-- Views over the archive layout of `src/common/storage.rs`, one series per view so no glob unions two cadences or
-- two providers. Reads the buckets named by AWS_S3_ARCHIVE_BUCKET_NAME and AWS_S3_RECORDS_BUCKET_NAME with the local
-- AWS credential chain; `check-views` creates each view alone and fails on any that is empty or does not create.

-- The setup must hold before any view is defined; a view that fails afterwards leaves the others usable.
.bail on
-- Instants print as stored; the local zone once made a correct reading look wrong.
SET TimeZone = 'UTC';
INSTALL httpfs;
LOAD httpfs;
INSTALL aws;
LOAD aws;
CREATE OR REPLACE SECRET archive (TYPE s3, PROVIDER credential_chain);
-- An unset variable reads as empty, which would point every view at `s3:///`.
SET VARIABLE market_data_bucket = CASE
    WHEN getenv('AWS_S3_ARCHIVE_BUCKET_NAME') = '' THEN error('AWS_S3_ARCHIVE_BUCKET_NAME is not set')
    ELSE getenv('AWS_S3_ARCHIVE_BUCKET_NAME')
END;
SET VARIABLE records_bucket = CASE
    WHEN getenv('AWS_S3_RECORDS_BUCKET_NAME') = '' THEN error('AWS_S3_RECORDS_BUCKET_NAME is not set')
    ELSE getenv('AWS_S3_RECORDS_BUCKET_NAME')
END;
.bail off

-- Massive's grouped daily, one bar per symbol per session, stamped at the 16:00 Eastern close.
CREATE OR REPLACE VIEW massive_daily_bars AS
SELECT * EXCLUDE (year, month, day), make_date(year, month, day) AS session
FROM read_parquet(
    's3://' || getvariable('market_data_bucket')
        || '/data/equity/stage=parsed/bars/provider=massive/origin=vendor/interval=one_day/year=*/month=*/day=*/data.parquet',
    hive_partitioning = true,
    hive_types = {'year': BIGINT, 'month': BIGINT, 'day': BIGINT}
);

-- Alpaca's SIP one-minute bars over the whole Eastern day, extended hours included.
CREATE OR REPLACE VIEW alpaca_minute_bars AS
SELECT * EXCLUDE (year, month, day), make_date(year, month, day) AS session
FROM read_parquet(
    's3://' || getvariable('market_data_bucket')
        || '/data/equity/stage=parsed/bars/provider=alpaca/origin=vendor/interval=one_minute/year=*/month=*/day=*/data.parquet',
    hive_partitioning = true,
    hive_types = {'year': BIGINT, 'month': BIGINT, 'day': BIGINT}
);

-- Every host's journal, one row per line; `payload` is JSON text for `json_extract`.
CREATE OR REPLACE VIEW journal AS
SELECT * EXCLUDE (year, month, day), make_date(year, month, day) AS session
FROM read_parquet(
    's3://' || getvariable('records_bucket')
        || '/records/journal/producer=*/year=*/month=*/day=*/data.parquet',
    hive_partitioning = true,
    hive_types = {'year': BIGINT, 'month': BIGINT, 'day': BIGINT}
);

-- Every service's logs, one row per line; a file holds the runs that started in its session.
CREATE OR REPLACE VIEW logs AS
SELECT * EXCLUDE (year, month, day), make_date(year, month, day) AS session
FROM read_parquet(
    's3://' || getvariable('records_bucket')
        || '/records/logs/producer=*/service=*/year=*/month=*/day=*/data.parquet',
    hive_partitioning = true,
    hive_types = {'year': BIGINT, 'month': BIGINT, 'day': BIGINT}
);

-- Every experiment a study journaled, one row each; settings and outputs stay JSON for `json_extract`, and an
-- experiment line this build could not read shows in `journal` with `unreadable` set rather than here. Like every
-- view here, it does not create in a bucket holding no researcher journal, and the error names the empty glob.
CREATE OR REPLACE VIEW experiments AS
SELECT
    timestamp,
    session,
    payload ->> '$.label' AS label,
    payload -> '$.parameters' AS parameters,
    list_sort(list_distinct(CAST(payload ->> '$.fingerprints[*].leg' AS VARCHAR[]))) AS legs,
    CAST(list_min(CAST(payload ->> '$.fingerprints[*].first' AS VARCHAR[])) AS DATE) AS first,
    CAST(list_max(CAST(payload ->> '$.fingerprints[*].last' AS VARCHAR[])) AS DATE) AS last,
    payload -> '$.estimates' AS estimates,
    payload -> '$.metrics' AS metrics,
    "commit",
    payload ->> '$.machine.hostname' AS hostname,
    payload ->> '$.machine.architecture' AS architecture,
    payload ->> '$.machine.operating_system' AS operating_system,
    CAST(payload ->> '$.machine.cores' AS BIGINT) AS cores,
    CAST(payload ->> '$.since_opened' AS BIGINT) AS milliseconds_since_opened,
    run_id
FROM (
    SELECT * EXCLUDE (year, month, day), make_date(year, month, day) AS session
    FROM read_parquet(
        's3://' || getvariable('records_bucket')
            || '/records/journal/producer=researcher/year=*/month=*/day=*/data.parquet',
        hive_partitioning = true,
        hive_types = {'year': BIGINT, 'month': BIGINT, 'day': BIGINT}
    )
    WHERE event_type = 'experiment_ran'
);
