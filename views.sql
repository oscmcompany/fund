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
        || '/data/equity/bars/provider=massive/origin=fetched/interval=one_day/year=*/month=*/day=*/data.parquet',
    hive_partitioning = true,
    hive_types = {'year': BIGINT, 'month': BIGINT, 'day': BIGINT}
);

-- Alpaca's SIP one-minute bars over the whole Eastern day, extended hours included.
CREATE OR REPLACE VIEW alpaca_minute_bars AS
SELECT * EXCLUDE (year, month, day), make_date(year, month, day) AS session
FROM read_parquet(
    's3://' || getvariable('market_data_bucket')
        || '/data/equity/bars/provider=alpaca/origin=fetched/interval=one_minute/year=*/month=*/day=*/data.parquet',
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
