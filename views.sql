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

-- The one-minute trade bars the archive derives from Alpaca's SIP trades over the whole Eastern day, built by the same
-- fold the trader runs live.
CREATE OR REPLACE VIEW alpaca_trade_bars AS
SELECT * EXCLUDE (year, month, day), make_date(year, month, day) AS session
FROM read_parquet(
    's3://' || getvariable('market_data_bucket')
        || '/data/equity/stage=parsed/trades/provider=alpaca/origin=derived/interval=one_minute/year=*/month=*/day=*/data.parquet',
    hive_partitioning = true,
    hive_types = {'year': BIGINT, 'month': BIGINT, 'day': BIGINT}
);

-- Massive's one-minute bars from its flat files, back to 2003, through the session before the nightly's first.
CREATE OR REPLACE VIEW massive_minute_bars AS
SELECT * EXCLUDE (year, month, day), make_date(year, month, day) AS session
FROM read_parquet(
    's3://' || getvariable('market_data_bucket')
        || '/data/equity/stage=parsed/bars/provider=massive/origin=vendor/interval=one_minute/year=*/month=*/day=*/data.parquet',
    hive_partitioning = true,
    hive_types = {'year': BIGINT, 'month': BIGINT, 'day': BIGINT}
);

-- Five-minute bars rolled up from Massive's one-minute bars.
CREATE OR REPLACE VIEW massive_five_minute_bars AS
SELECT * EXCLUDE (year, month, day), make_date(year, month, day) AS session
FROM read_parquet(
    's3://' || getvariable('market_data_bucket')
        || '/data/equity/stage=parsed/bars/provider=massive/origin=derived/interval=five_minute/year=*/month=*/day=*/data.parquet',
    hive_partitioning = true,
    hive_types = {'year': BIGINT, 'month': BIGINT, 'day': BIGINT}
);

-- One-minute quote bars folded from Massive's quote flat files, time-weighted over regular hours.
CREATE OR REPLACE VIEW massive_quote_bars AS
SELECT * EXCLUDE (year, month, day), make_date(year, month, day) AS session
FROM read_parquet(
    's3://' || getvariable('market_data_bucket')
        || '/data/equity/stage=parsed/quotes/provider=massive/origin=derived/interval=one_minute/year=*/month=*/day=*/data.parquet',
    hive_partitioning = true,
    hive_types = {'year': BIGINT, 'month': BIGINT, 'day': BIGINT}
);

-- Five-minute quote bars rolled up from Massive's one-minute quote bars.
CREATE OR REPLACE VIEW massive_five_minute_quote_bars AS
SELECT * EXCLUDE (year, month, day), make_date(year, month, day) AS session
FROM read_parquet(
    's3://' || getvariable('market_data_bucket')
        || '/data/equity/stage=parsed/quotes/provider=massive/origin=derived/interval=five_minute/year=*/month=*/day=*/data.parquet',
    hive_partitioning = true,
    hive_types = {'year': BIGINT, 'month': BIGINT, 'day': BIGINT}
);

-- Daily quote bars rolled up from Massive's one-minute quote bars.
CREATE OR REPLACE VIEW massive_daily_quote_bars AS
SELECT * EXCLUDE (year, month, day), make_date(year, month, day) AS session
FROM read_parquet(
    's3://' || getvariable('market_data_bucket')
        || '/data/equity/stage=parsed/quotes/provider=massive/origin=derived/interval=one_day/year=*/month=*/day=*/data.parquet',
    hive_partitioning = true,
    hive_types = {'year': BIGINT, 'month': BIGINT, 'day': BIGINT}
);

-- One-minute trade bars folded from Massive's trade flat files over the whole Eastern day.
CREATE OR REPLACE VIEW massive_trade_bars AS
SELECT * EXCLUDE (year, month, day), make_date(year, month, day) AS session
FROM read_parquet(
    's3://' || getvariable('market_data_bucket')
        || '/data/equity/stage=parsed/trades/provider=massive/origin=derived/interval=one_minute/year=*/month=*/day=*/data.parquet',
    hive_partitioning = true,
    hive_types = {'year': BIGINT, 'month': BIGINT, 'day': BIGINT}
);

-- Five-minute trade bars rolled up from Massive's one-minute trade bars.
CREATE OR REPLACE VIEW massive_five_minute_trade_bars AS
SELECT * EXCLUDE (year, month, day), make_date(year, month, day) AS session
FROM read_parquet(
    's3://' || getvariable('market_data_bucket')
        || '/data/equity/stage=parsed/trades/provider=massive/origin=derived/interval=five_minute/year=*/month=*/day=*/data.parquet',
    hive_partitioning = true,
    hive_types = {'year': BIGINT, 'month': BIGINT, 'day': BIGINT}
);

-- Daily trade bars rolled up from Massive's one-minute trade bars.
CREATE OR REPLACE VIEW massive_daily_trade_bars AS
SELECT * EXCLUDE (year, month, day), make_date(year, month, day) AS session
FROM read_parquet(
    's3://' || getvariable('market_data_bucket')
        || '/data/equity/stage=parsed/trades/provider=massive/origin=derived/interval=one_day/year=*/month=*/day=*/data.parquet',
    hive_partitioning = true,
    hive_types = {'year': BIGINT, 'month': BIGINT, 'day': BIGINT}
);

-- One-minute quote bars the nightly folds from Alpaca's SIP quotes.
CREATE OR REPLACE VIEW alpaca_quote_bars AS
SELECT * EXCLUDE (year, month, day), make_date(year, month, day) AS session
FROM read_parquet(
    's3://' || getvariable('market_data_bucket')
        || '/data/equity/stage=parsed/quotes/provider=alpaca/origin=derived/interval=one_minute/year=*/month=*/day=*/data.parquet',
    hive_partitioning = true,
    hive_types = {'year': BIGINT, 'month': BIGINT, 'day': BIGINT}
);

-- Five-minute quote bars rolled up from Alpaca's one-minute quote bars.
CREATE OR REPLACE VIEW alpaca_five_minute_quote_bars AS
SELECT * EXCLUDE (year, month, day), make_date(year, month, day) AS session
FROM read_parquet(
    's3://' || getvariable('market_data_bucket')
        || '/data/equity/stage=parsed/quotes/provider=alpaca/origin=derived/interval=five_minute/year=*/month=*/day=*/data.parquet',
    hive_partitioning = true,
    hive_types = {'year': BIGINT, 'month': BIGINT, 'day': BIGINT}
);

-- Daily quote bars rolled up from Alpaca's one-minute quote bars.
CREATE OR REPLACE VIEW alpaca_daily_quote_bars AS
SELECT * EXCLUDE (year, month, day), make_date(year, month, day) AS session
FROM read_parquet(
    's3://' || getvariable('market_data_bucket')
        || '/data/equity/stage=parsed/quotes/provider=alpaca/origin=derived/interval=one_day/year=*/month=*/day=*/data.parquet',
    hive_partitioning = true,
    hive_types = {'year': BIGINT, 'month': BIGINT, 'day': BIGINT}
);

-- Five-minute trade bars rolled up from Alpaca's one-minute trade bars.
CREATE OR REPLACE VIEW alpaca_five_minute_trade_bars AS
SELECT * EXCLUDE (year, month, day), make_date(year, month, day) AS session
FROM read_parquet(
    's3://' || getvariable('market_data_bucket')
        || '/data/equity/stage=parsed/trades/provider=alpaca/origin=derived/interval=five_minute/year=*/month=*/day=*/data.parquet',
    hive_partitioning = true,
    hive_types = {'year': BIGINT, 'month': BIGINT, 'day': BIGINT}
);

-- Daily trade bars rolled up from Alpaca's one-minute trade bars.
CREATE OR REPLACE VIEW alpaca_daily_trade_bars AS
SELECT * EXCLUDE (year, month, day), make_date(year, month, day) AS session
FROM read_parquet(
    's3://' || getvariable('market_data_bucket')
        || '/data/equity/stage=parsed/trades/provider=alpaca/origin=derived/interval=one_day/year=*/month=*/day=*/data.parquet',
    hive_partitioning = true,
    hive_types = {'year': BIGINT, 'month': BIGINT, 'day': BIGINT}
);

-- Each snapshot of the trade conditions table, one row per condition code, dated by `as_of`.
CREATE OR REPLACE VIEW trade_conditions AS
SELECT *
FROM read_parquet(
    's3://' || getvariable('market_data_bucket')
        || '/data/equity/stage=parsed/reference/provider=massive/table=conditions/as_of=*/data.parquet',
    hive_partitioning = true,
    hive_types = {'as_of': DATE}
);

-- Each quarterly snapshot of every symbol's details as Massive held them on `as_of`.
CREATE OR REPLACE VIEW security_details AS
SELECT *
FROM read_parquet(
    's3://' || getvariable('market_data_bucket')
        || '/data/equity/stage=parsed/reference/provider=massive/table=security_details/as_of=*/data.parquet',
    hive_partitioning = true,
    hive_types = {'as_of': DATE}
);

-- Each nightly snapshot of Massive's whole split table, dated by the session it was taken after.
CREATE OR REPLACE VIEW splits AS
SELECT *
FROM read_parquet(
    's3://' || getvariable('market_data_bucket')
        || '/data/equity/stage=parsed/reference/provider=massive/table=splits/as_of=*/data.parquet',
    hive_partitioning = true,
    hive_types = {'as_of': DATE}
);

-- Each nightly snapshot of the dates a symbol's series may not be read across, from Alpaca's corporate actions.
CREATE OR REPLACE VIEW series_boundaries AS
SELECT *
FROM read_parquet(
    's3://' || getvariable('market_data_bucket')
        || '/data/equity/stage=parsed/reference/provider=alpaca/table=series_boundaries/as_of=*/data.parquet',
    hive_partitioning = true,
    hive_types = {'as_of': DATE}
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

-- Each minute bar a trader run built live against the archive's for the same symbol and minute, over the names the run
-- watched from its first journaled bar to its last: `presence` says which side holds the bar and each `*_difference` is
-- trader minus archive in the archive's units, `NULL` where either side lacks the value.
CREATE OR REPLACE VIEW bar_seam AS
SELECT * FROM (
    WITH trader_journal AS (
        SELECT * EXCLUDE (year, month, day), make_date(year, month, day) AS session
        FROM read_parquet(
            's3://' || getvariable('records_bucket')
                || '/records/journal/producer=trader/year=*/month=*/day=*/data.parquet',
            hive_partitioning = true,
            hive_types = {'year': BIGINT, 'month': BIGINT, 'day': BIGINT}
        )
    ),
    trader AS (
        SELECT
            session,
            run_id,
            payload ->> '$.symbol' AS symbol,
            CAST(payload ->> '$.timestamp' AS TIMESTAMPTZ) AS timestamp,
            CAST(payload ->> '$.trade_count' AS UBIGINT) AS trade_count,
            CAST(payload ->> '$.volume' AS DECIMAL(38, 0)) * 0.000001 AS volume,
            CAST(payload ->> '$.dollar_volume' AS DECIMAL(38, 0)) * 0.000000000001 AS dollar_volume,
            CAST(payload ->> '$.opened_at' AS TIMESTAMPTZ) AS opened_at,
            CAST(payload ->> '$.open' AS DECIMAL(18, 0)) * 0.000001 AS open,
            CAST(payload ->> '$.closed_at' AS TIMESTAMPTZ) AS closed_at,
            CAST(payload ->> '$.close' AS DECIMAL(18, 0)) * 0.000001 AS close,
            CAST(payload ->> '$.high' AS DECIMAL(18, 0)) * 0.000001 AS high,
            CAST(payload ->> '$.low' AS DECIMAL(18, 0)) * 0.000001 AS low
        FROM trader_journal
        WHERE event_type = 'bar_built'
    ),
    runs AS (
        SELECT run_id, session, min(timestamp) AS first, max(timestamp) AS last FROM trader GROUP BY run_id, session
    ),
    -- The configured universe plus any held name the run built a bar for.
    watched AS (
        SELECT run_id, unnest(string_split(payload ->> '$.parameters.universe.value', ',')) AS symbol
        FROM trader_journal
        WHERE event_type = 'configuration_resolved'
        UNION
        SELECT run_id, symbol FROM trader
    ),
    archive AS (
        SELECT runs.run_id, bars.*
        FROM read_parquet(
            's3://' || getvariable('market_data_bucket')
                || '/data/equity/stage=parsed/trades/provider=alpaca/origin=derived/interval=one_minute/year=*/month=*/day=*/data.parquet',
            hive_partitioning = true,
            hive_types = {'year': BIGINT, 'month': BIGINT, 'day': BIGINT}
        ) AS bars
        JOIN runs ON runs.session = make_date(bars.year, bars.month, bars.day)
        JOIN watched ON watched.run_id = runs.run_id AND watched.symbol = bars.symbol
        WHERE bars.timestamp BETWEEN runs.first AND runs.last
    )
    SELECT
        coalesce(trader.run_id, archive.run_id) AS run_id,
        coalesce(trader.session, make_date(archive.year, archive.month, archive.day)) AS session,
        coalesce(trader.symbol, archive.symbol) AS symbol,
        coalesce(trader.timestamp, archive.timestamp) AS timestamp,
        CASE
            WHEN archive.symbol IS NULL THEN 'trader_only'
            WHEN trader.symbol IS NULL THEN 'archive_only'
            ELSE 'both'
        END AS presence,
        CAST(trader.trade_count AS BIGINT) - CAST(archive.trade_count AS BIGINT) AS trade_count_difference,
        trader.volume - archive.volume AS volume_difference,
        trader.dollar_volume - archive.dollar_volume AS dollar_volume_difference,
        trader.opened_at - archive.opened_at AS opened_at_difference,
        trader.open - archive.open AS open_difference,
        trader.closed_at - archive.closed_at AS closed_at_difference,
        trader.close - archive.close AS close_difference,
        trader.high - archive.high AS high_difference,
        trader.low - archive.low AS low_difference
    FROM trader
    FULL OUTER JOIN archive
        ON archive.run_id = trader.run_id AND archive.symbol = trader.symbol AND archive.timestamp = trader.timestamp
);
