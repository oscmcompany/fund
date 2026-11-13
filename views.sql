-- Views over the archive layout of `src/common/storage.rs`: one series per provider view, and provider-neutral views
-- that take one provider per session, so no session mixes two cadences or two providers. Reads the buckets named by
-- AWS_S3_ARCHIVE_BUCKET_NAME and AWS_S3_PROFILE_BUCKET_NAME with the local AWS credential chain.

-- The setup must hold before any view is defined; a view that fails afterwards leaves the others usable.
.bail on
-- Instants print in UTC, as stored.
SET TimeZone = 'UTC';
INSTALL httpfs;
LOAD httpfs;
INSTALL aws;
LOAD aws;
CREATE OR REPLACE SECRET archive (TYPE s3, PROVIDER credential_chain);
-- An unset variable reads as empty, which would point every view at `s3:///`.
SET VARIABLE archive_bucket = CASE
    WHEN getenv('AWS_S3_ARCHIVE_BUCKET_NAME') = '' THEN error('AWS_S3_ARCHIVE_BUCKET_NAME is not set')
    ELSE getenv('AWS_S3_ARCHIVE_BUCKET_NAME')
END;
SET VARIABLE profile_bucket = CASE
    WHEN getenv('AWS_S3_PROFILE_BUCKET_NAME') = '' THEN error('AWS_S3_PROFILE_BUCKET_NAME is not set')
    ELSE getenv('AWS_S3_PROFILE_BUCKET_NAME')
END;

-- The partitions of a session-partitioned series that `days` globs, `year`/`month`/`day` folded into `session`.
CREATE OR REPLACE MACRO partitions(bucket, series, days) AS TABLE
SELECT * EXCLUDE (year, month, day), make_date(year, month, day) AS session
FROM read_parquet(
    's3://' || getvariable(bucket) || '/' || series || days || '/data.parquet',
    hive_partitioning = true,
    hive_types = {'year': BIGINT, 'month': BIGINT, 'day': BIGINT}
);

-- Every session of a series; filter on a `session` range, which prunes files, never on `year(session)`, which reads all.
CREATE OR REPLACE MACRO sessions(bucket, series) AS TABLE
SELECT * FROM partitions(bucket, series, 'year=*/month=*/day=*');

-- A nanosecond instant rounded up to the microsecond a `TIMESTAMPTZ` holds, so it is never read as earlier than it was.
CREATE OR REPLACE MACRO microsecond_ceiling(instant) AS make_timestamptz((epoch_ns(instant) + 999) // 1000);

-- A reference table's snapshots, dated by `as_of`, each row with `known_at`, the instant its file was fetched; a file
-- whose metadata lacks that instant fails the read.
CREATE OR REPLACE MACRO snapshots(series) AS TABLE
WITH fetched AS (
    SELECT file_name, microsecond_ceiling(decode(value)::TIMESTAMP_NS) AS known_at
    FROM parquet_kv_metadata('s3://' || getvariable('archive_bucket') || '/' || series || 'as_of=*/data.parquet')
    WHERE decode(key) = 'fund.fetched_at'
)
SELECT
    snapshot.* EXCLUDE (filename),
    CASE
        WHEN fetched.known_at IS NULL THEN error(snapshot.filename || ' has no fund.fetched_at')
        ELSE fetched.known_at
    END AS known_at
FROM read_parquet(
    's3://' || getvariable('archive_bucket') || '/' || series || 'as_of=*/data.parquet',
    hive_partitioning = true,
    hive_types = {'as_of': DATE},
    filename = true
) AS snapshot
LEFT JOIN fetched ON fetched.file_name = snapshot.filename;

-- The sessions a market-data series holds, read from its file listing rather than its rows.
CREATE OR REPLACE MACRO held_sessions(series) AS TABLE
SELECT make_date(parts.year::BIGINT, parts.month::BIGINT, parts.day::BIGINT) AS session
FROM (
    SELECT regexp_extract(file, 'year=(\d+)/month=(\d+)/day=(\d+)/', ['year', 'month', 'day']) AS parts
    FROM glob('s3://' || getvariable('archive_bucket') || '/' || series || 'year=*/month=*/day=*/data.parquet')
);

-- One series both providers carry, Massive's where it holds the session and Alpaca's otherwise; `provider` says which.
CREATE OR REPLACE MACRO preferred(massive, alpaca) AS TABLE
SELECT * FROM sessions('archive_bucket', massive)
UNION ALL
SELECT * FROM sessions('archive_bucket', alpaca)
WHERE session NOT IN (SELECT session FROM held_sessions(massive));

-- A journaled count of millionths, a `Price` or `Shares`, in units.
CREATE OR REPLACE MACRO millionths(value) AS CAST(value AS DECIMAL(38, 0)) * 0.000001;

-- A journaled count of trillionths, a `DollarVolume`, in dollars.
CREATE OR REPLACE MACRO trillionths(value) AS CAST(value AS DECIMAL(38, 0)) * 0.000000000001;
.bail off

-- Each one-minute bar a trader run built live against the archive's for the same symbol and minute, in the partitions
-- `days` globs, over the names the run watched from its first journaled bar to its last: `presence` says which side
-- holds the bar and each `*_difference` is trader minus archive in the archive's units, `NULL` where either side lacks
-- the value.
CREATE OR REPLACE MACRO bar_seam_over(days) AS TABLE
WITH trader_journal AS (
    SELECT * FROM partitions('profile_bucket', 'records/journal/producer=trader/', days)
),
trader AS (
    SELECT
        session,
        run_id,
        payload ->> '$.symbol' AS symbol,
        CAST(payload ->> '$.timestamp' AS TIMESTAMPTZ) AS timestamp,
        CAST(payload ->> '$.trade_count' AS UBIGINT) AS trade_count,
        millionths(payload ->> '$.volume') AS volume,
        trillionths(payload ->> '$.dollar_volume') AS dollar_volume,
        CAST(payload ->> '$.opened_at' AS TIMESTAMPTZ) AS opened_at,
        millionths(payload ->> '$.open') AS open,
        CAST(payload ->> '$.closed_at' AS TIMESTAMPTZ) AS closed_at,
        millionths(payload ->> '$.close') AS close,
        millionths(payload ->> '$.high') AS high,
        millionths(payload ->> '$.low') AS low
    FROM trader_journal
    WHERE event_type = 'bar_built' AND (payload ->> '$.interval') = 'one_minute'
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
    FROM partitions('archive_bucket', 'data/equity/stage=parsed/trades/provider=alpaca/interval=one_minute/', days) AS bars
    JOIN runs ON runs.session = bars.session
    JOIN watched ON watched.run_id = runs.run_id AND watched.symbol = bars.symbol
    WHERE bars.timestamp BETWEEN runs.first AND runs.last
)
SELECT
    coalesce(trader.run_id, archive.run_id) AS run_id,
    coalesce(trader.session, archive.session) AS session,
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
    ON archive.run_id = trader.run_id AND archive.symbol = trader.symbol AND archive.timestamp = trader.timestamp;

-- The seam for one session, reading only that day's journal and trade bars; it fails until both partitions exist.
CREATE OR REPLACE MACRO bar_seam_on(day) AS TABLE
SELECT * FROM bar_seam_over(strftime(day, 'year=%Y/month=%m/day=%d'));

-- One-minute quote bars the nightly folds from Alpaca's SIP quotes.
CREATE OR REPLACE VIEW alpaca_minute_quote_bars AS
SELECT * FROM sessions('archive_bucket', 'data/equity/stage=parsed/quotes/provider=alpaca/interval=one_minute/');

-- Five-minute quote bars rolled up from Alpaca's one-minute quote bars.
CREATE OR REPLACE VIEW alpaca_five_minute_quote_bars AS
SELECT * FROM sessions('archive_bucket', 'data/equity/stage=parsed/quotes/provider=alpaca/interval=five_minute/');

-- Daily quote bars rolled up from Alpaca's one-minute quote bars.
CREATE OR REPLACE VIEW alpaca_daily_quote_bars AS
SELECT * FROM sessions('archive_bucket', 'data/equity/stage=parsed/quotes/provider=alpaca/interval=one_day/');

-- One-minute trade bars derived from Alpaca's SIP trades over the whole Eastern day by the trader's own fold.
CREATE OR REPLACE VIEW alpaca_minute_trade_bars AS
SELECT * FROM sessions('archive_bucket', 'data/equity/stage=parsed/trades/provider=alpaca/interval=one_minute/');

-- Five-minute trade bars rolled up from Alpaca's one-minute trade bars.
CREATE OR REPLACE VIEW alpaca_five_minute_trade_bars AS
SELECT * FROM sessions('archive_bucket', 'data/equity/stage=parsed/trades/provider=alpaca/interval=five_minute/');

-- Daily trade bars rolled up from Alpaca's one-minute trade bars.
CREATE OR REPLACE VIEW alpaca_daily_trade_bars AS
SELECT * FROM sessions('archive_bucket', 'data/equity/stage=parsed/trades/provider=alpaca/interval=one_day/');

-- One-minute quote bars folded from Massive's quote flat files, time-weighted over regular hours.
CREATE OR REPLACE VIEW massive_minute_quote_bars AS
SELECT * FROM sessions('archive_bucket', 'data/equity/stage=parsed/quotes/provider=massive/interval=one_minute/');

-- Five-minute quote bars rolled up from Massive's one-minute quote bars.
CREATE OR REPLACE VIEW massive_five_minute_quote_bars AS
SELECT * FROM sessions('archive_bucket', 'data/equity/stage=parsed/quotes/provider=massive/interval=five_minute/');

-- Daily quote bars rolled up from Massive's one-minute quote bars.
CREATE OR REPLACE VIEW massive_daily_quote_bars AS
SELECT * FROM sessions('archive_bucket', 'data/equity/stage=parsed/quotes/provider=massive/interval=one_day/');

-- One-minute trade bars folded from Massive's trade flat files over the whole Eastern day.
CREATE OR REPLACE VIEW massive_minute_trade_bars AS
SELECT * FROM sessions('archive_bucket', 'data/equity/stage=parsed/trades/provider=massive/interval=one_minute/');

-- Five-minute trade bars rolled up from Massive's one-minute trade bars.
CREATE OR REPLACE VIEW massive_five_minute_trade_bars AS
SELECT * FROM sessions('archive_bucket', 'data/equity/stage=parsed/trades/provider=massive/interval=five_minute/');

-- Daily trade bars rolled up from Massive's one-minute trade bars.
CREATE OR REPLACE VIEW massive_daily_trade_bars AS
SELECT * FROM sessions('archive_bucket', 'data/equity/stage=parsed/trades/provider=massive/interval=one_day/');

-- One-minute quote bars from either provider, one provider per session.
CREATE OR REPLACE VIEW minute_quote_bars AS
SELECT * FROM preferred(
    'data/equity/stage=parsed/quotes/provider=massive/interval=one_minute/',
    'data/equity/stage=parsed/quotes/provider=alpaca/interval=one_minute/'
);

-- Five-minute quote bars from either provider, one provider per session.
CREATE OR REPLACE VIEW five_minute_quote_bars AS
SELECT * FROM preferred(
    'data/equity/stage=parsed/quotes/provider=massive/interval=five_minute/',
    'data/equity/stage=parsed/quotes/provider=alpaca/interval=five_minute/'
);

-- Daily quote bars from either provider, one provider per session.
CREATE OR REPLACE VIEW daily_quote_bars AS
SELECT * FROM preferred(
    'data/equity/stage=parsed/quotes/provider=massive/interval=one_day/',
    'data/equity/stage=parsed/quotes/provider=alpaca/interval=one_day/'
);

-- One-minute trade bars from either provider, one provider per session.
CREATE OR REPLACE VIEW minute_trade_bars AS
SELECT * FROM preferred(
    'data/equity/stage=parsed/trades/provider=massive/interval=one_minute/',
    'data/equity/stage=parsed/trades/provider=alpaca/interval=one_minute/'
);

-- Five-minute trade bars from either provider, one provider per session.
CREATE OR REPLACE VIEW five_minute_trade_bars AS
SELECT * FROM preferred(
    'data/equity/stage=parsed/trades/provider=massive/interval=five_minute/',
    'data/equity/stage=parsed/trades/provider=alpaca/interval=five_minute/'
);

-- Daily trade bars from either provider, one provider per session.
CREATE OR REPLACE VIEW daily_trade_bars AS
SELECT * FROM preferred(
    'data/equity/stage=parsed/trades/provider=massive/interval=one_day/',
    'data/equity/stage=parsed/trades/provider=alpaca/interval=one_day/'
);

-- Each snapshot of the trade conditions table, one row per condition code.
CREATE OR REPLACE VIEW trade_conditions AS
SELECT * FROM snapshots('data/equity/stage=parsed/reference/provider=massive/table=conditions/');

-- Each quarterly snapshot of every symbol's details as Massive held them on `as_of`.
CREATE OR REPLACE VIEW security_details AS
SELECT * FROM snapshots('data/equity/stage=parsed/reference/provider=massive/table=security_details/');

-- Each quarterly snapshot of the industry code each annual report's header assigned a filer of common stock or a
-- depositary receipt Massive gives no code, one row per report; `known_at` is EDGAR's `accepted_at`, the filing's
-- publication as DuckDB reads it to the microsecond, and a null code reads as the filer left unclassified, ending any
-- earlier code.
CREATE OR REPLACE VIEW filed_industry_codes AS
SELECT * REPLACE (accepted_at AS known_at)
FROM snapshots('data/equity/stage=parsed/reference/provider=sec/table=filed_industry_codes/');

-- Each nightly snapshot of Massive's whole split table, dated by the session it was taken after.
CREATE OR REPLACE VIEW splits AS
SELECT * FROM snapshots('data/equity/stage=parsed/reference/provider=massive/table=splits/');

-- Each nightly snapshot of the dates a symbol's series may not be read across, from Alpaca's corporate actions.
CREATE OR REPLACE VIEW series_boundaries AS
SELECT * FROM snapshots('data/equity/stage=parsed/reference/provider=alpaca/table=series_boundaries/');

-- Each nightly snapshot of whether every active asset was shortable and easy to borrow, as Alpaca listed it after the
-- session's close; a session trades on the snapshot dated before it.
CREATE OR REPLACE VIEW borrow_flags AS
SELECT * FROM snapshots('data/equity/stage=parsed/reference/provider=alpaca/table=borrow_flags/');

-- Every producer's journal, the hosts' and the operator's, one row per line; `payload` is JSON text for `json_extract`.
CREATE OR REPLACE VIEW journal AS
SELECT * FROM sessions('profile_bucket', 'records/journal/producer=*/');

-- Every service's logs, one row per line; a file holds the runs that started in its session.
CREATE OR REPLACE VIEW logs AS
SELECT * FROM sessions('profile_bucket', 'records/logs/producer=*/service=*/');

-- Every experiment a study journaled, one row each; settings and outputs stay JSON for `json_extract`.
CREATE OR REPLACE VIEW experiments AS
SELECT
    timestamp,
    session,
    payload ->> '$.label' AS label,
    payload -> '$.parameters' AS parameters,
    payload -> '$.metrics' AS metrics,
    payload,
    "commit",
    run_id
FROM sessions('profile_bucket', 'records/journal/producer=researcher/')
WHERE event_type = 'experiment_ran';

-- A verdict reads the latest declaration of its question made before it, so a later declaration cannot move its bar.
CREATE OR REPLACE VIEW verdicts AS
WITH records AS (
    SELECT * FROM sessions('profile_bucket', 'records/journal/producer=researcher/')
    WHERE event_type IN ('question_declared', 'verdict_reached')
),
declared AS (
    SELECT
        timestamp,
        payload ->> '$.label' AS label,
        payload ->> '$.question' AS question,
        payload AS declaration
    FROM records
    WHERE event_type = 'question_declared'
),
reached AS (
    SELECT *, payload ->> '$.label' AS label, payload ->> '$.question' AS question
    FROM records
    WHERE event_type = 'verdict_reached'
)
SELECT
    reached.timestamp,
    reached.session,
    reached.label,
    reached.question,
    reached.payload ->> '$.outcome.kind' AS outcome,
    CAST(reached.payload ->> '$.outcome.reading' AS DOUBLE) AS reading,
    reached.payload ->> '$.outcome.scope' AS scope,
    reached.payload ->> '$.outcome.reopen' AS reopen,
    reached.payload ->> '$.outcome.cause' AS cause,
    CAST(reached.payload ->> '$.runs' AS VARCHAR[]) AS runs,
    declared.timestamp AS declared_at,
    declared.declaration ->> '$.baseline' AS baseline,
    declared.declaration ->> '$.metric' AS metric,
    declared.declaration -> '$.bar' AS bar,
    CAST(declared.declaration ->> '$.as_of' AS DATE) AS as_of,
    declared.declaration -> '$.parameters' AS parameters,
    reached."commit",
    reached.run_id
FROM reached
ASOF LEFT JOIN declared
    ON declared.label = reached.label AND declared.question = reached.question
    AND declared.timestamp <= reached.timestamp;

COMMENT ON VIEW verdicts IS 'Every question a study closed, one row per verdict, with the declaration it answered.';
COMMENT ON COLUMN verdicts.timestamp IS 'Instant the verdict was journaled, UTC.';
COMMENT ON COLUMN verdicts.session IS 'Session the verdict was journaled in, an America/New_York date.';
COMMENT ON COLUMN verdicts.label IS 'Free text naming what the study is about.';
COMMENT ON COLUMN verdicts.question IS 'What the study set out to answer.';
COMMENT ON COLUMN verdicts.outcome IS 'live, refuted or inconclusive.';
COMMENT ON COLUMN verdicts.reading IS 'The metric reading a live outcome cleared its bar with; null otherwise.';
COMMENT ON COLUMN verdicts.scope IS 'Exactly what a refutation covers; null otherwise.';
COMMENT ON COLUMN verdicts.reopen IS 'What would reopen a refutation; null otherwise.';
COMMENT ON COLUMN verdicts.cause IS 'Why an inconclusive outcome did not decide; null otherwise.';
COMMENT ON COLUMN verdicts.runs IS 'Study runs the verdict rests on; empty for a finding recorded after its journals.';
COMMENT ON COLUMN verdicts.declared_at IS
    'Instant of the latest declaration of the question at or before the verdict, UTC; null when undeclared.';
COMMENT ON COLUMN verdicts.baseline IS 'Trivial arm the declaration quoted skill against; null when undeclared.';
COMMENT ON COLUMN verdicts.metric IS 'Metric the declared bar reads; null when undeclared.';
COMMENT ON COLUMN verdicts.bar IS 'Where the metric had to land, as JSON; null when undeclared.';
COMMENT ON COLUMN verdicts.as_of IS 'Session after which data is the "as of" confirm read; null when undeclared.';
COMMENT ON COLUMN verdicts.parameters IS 'Free choices fixed at declaration, as JSON; null when undeclared.';
COMMENT ON COLUMN verdicts."commit" IS 'Git sha the binary that journaled the verdict was built from.';
COMMENT ON COLUMN verdicts.run_id IS 'The study run that journaled the verdict.';

-- The seam over every session, which reads every partition of both series; `bar_seam_on` reads one.
CREATE OR REPLACE VIEW bar_seam AS
SELECT * FROM bar_seam_over('year=*/month=*/day=*');
