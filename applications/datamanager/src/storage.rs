use crate::data::{
    create_equity_bar_dataframe, create_equity_details_dataframe, create_portfolio_dataframe,
    create_predictions_dataframe, EquityBar, Portfolio, Prediction,
};
use crate::errors::Error;
use crate::state::State;
use aws_credential_types::provider::ProvideCredentials;
use aws_sdk_s3::primitives::ByteStream;
use chrono::{DateTime, Utc};
use duckdb::Connection;
use polars::prelude::*;
use serde::Deserialize;
use std::io::Cursor;
use tracing::{debug, error, info, warn};

pub async fn write_equity_bars_dataframe_to_s3(
    state: &State,
    dataframe: &DataFrame,
    timestamp: &DateTime<Utc>,
) -> Result<String, Error> {
    write_dataframe_to_s3(state, dataframe, timestamp, "bars".to_string()).await
}

pub async fn write_portfolio_dataframe_to_s3(
    state: &State,
    dataframe: &DataFrame,
    timestamp: &DateTime<Utc>,
) -> Result<String, Error> {
    write_dataframe_to_s3(state, dataframe, timestamp, "portfolios".to_string()).await
}

pub async fn write_predictions_dataframe_to_s3(
    state: &State,
    dataframe: &DataFrame,
    timestamp: &DateTime<Utc>,
) -> Result<String, Error> {
    write_dataframe_to_s3(state, dataframe, timestamp, "predictions".to_string()).await
}

pub fn is_valid_ticker(ticker: &str) -> bool {
    !ticker.is_empty()
        && ticker.chars().any(|c| c.is_ascii_alphanumeric())
        && ticker
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '.' || c == '-')
}

pub fn format_s3_key(timestamp: &DateTime<Utc>, dataframe_type: &str) -> String {
    let year = timestamp.format("%Y");
    let month = timestamp.format("%m");
    let day = timestamp.format("%d");

    format!(
        "equity/{}/daily/year={}/month={}/day={}/data.parquet",
        dataframe_type, year, month, day,
    )
}

pub fn date_to_int(timestamp: &DateTime<Utc>) -> Result<i32, Error> {
    timestamp
        .format("%Y%m%d")
        .to_string()
        .parse::<i32>()
        .map_err(|e| Error::Other(format!("Failed to convert date to integer: {}", e)))
}

pub fn escape_sql_ticker(ticker: &str) -> String {
    ticker.replace('\'', "''")
}

pub fn sanitize_duckdb_config_value(value: &str) -> Result<String, Error> {
    if value.is_empty() {
        return Err(Error::Other("Configuration value cannot be empty".into()));
    }

    // Reject SQL metacharacters
    if value.contains('\'') || value.contains('"') || value.contains(';') || value.contains("--") {
        let message = format!(
            "Invalid configuration value contains SQL metacharacters: {}",
            value
        );
        error!("{}", message);
        return Err(Error::Other(message));
    }

    // Reasonable length limit
    if value.len() > 512 {
        let message = format!("Configuration value too long: {} characters", value.len());
        error!("{}", message);
        return Err(Error::Other(message));
    }

    Ok(value.to_string())
}

async fn write_dataframe_to_s3(
    state: &State,
    dataframe: &DataFrame,
    timestamp: &DateTime<Utc>,
    dataframe_type: String,
) -> Result<String, Error> {
    info!("Uploading DataFrame to S3 as parquet");

    let key = format_s3_key(timestamp, &dataframe_type);

    let mut buffer = Vec::new();
    {
        let cursor = Cursor::new(&mut buffer);
        let writer = ParquetWriter::new(cursor);
        match writer.finish(&mut dataframe.clone()) {
            Ok(_) => {
                info!(
                    "DataFrame successfully converted to parquet, size: {} bytes",
                    buffer.len()
                );
            }
            Err(err) => {
                return Err(Error::Other(format!("Failed to write parquet: {}", err)));
            }
        }
    }

    let body = ByteStream::from(buffer);

    match state
        .s3_client
        .put_object()
        .bucket(&state.bucket_name)
        .key(&key)
        .body(body)
        .content_type("application/octet-stream")
        .send()
        .await
    {
        Ok(_) => {
            info!(
                "Successfully uploaded parquet file to s3://{}/{}",
                state.bucket_name, key
            );
            Ok(key)
        }
        Err(err) => Err(Error::Other(format!("Failed to upload to S3: {}", err))),
    }
}

async fn create_duckdb_connection() -> Result<Connection, Error> {
    debug!("Opening in-memory DuckDB connection");
    let connection = Connection::open_in_memory()?;

    debug!("Installing and loading httpfs extension");
    connection.execute_batch("INSTALL httpfs; LOAD httpfs;")?;

    debug!("Loading AWS configuration for DuckDB S3 access");
    let config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
    let provider = config.credentials_provider().ok_or_else(|| {
        error!("No AWS credentials provider found");
        Error::Other("No AWS credentials provider found".into())
    })?;

    debug!("Fetching AWS credentials");
    let credentials = provider.provide_credentials().await?;

    let region = config
        .region()
        .map(|r| r.as_ref().to_string())
        .ok_or_else(|| {
            error!("AWS region not configured");
            Error::Other("AWS region not configured".into())
        })?;

    let has_session_token = credentials.session_token().is_some();
    debug!(
        "AWS credentials loaded: region={}, has_session_token={}",
        region, has_session_token
    );

    let session_token = credentials.session_token().unwrap_or_default();

    let sanitized_region = sanitize_duckdb_config_value(&region)?;
    let sanitized_access_key = sanitize_duckdb_config_value(credentials.access_key_id())?;
    let sanitized_secret_key = sanitize_duckdb_config_value(credentials.secret_access_key())?;
    // Session token can be empty for static credentials (no temporary session)
    let sanitized_session_token = if !session_token.is_empty() {
        sanitize_duckdb_config_value(session_token)?
    } else {
        String::new()
    };

    let mut s3_configuration_statements = vec![
        format!("SET s3_region='{}';", sanitized_region),
        "SET s3_url_style='path';".to_string(),
        format!("SET s3_access_key_id='{}';", sanitized_access_key),
        format!("SET s3_secret_access_key='{}';", sanitized_secret_key),
        format!("SET s3_session_token='{}';", sanitized_session_token),
    ];

    if let Ok(duckdb_s3_endpoint) = std::env::var("DUCKDB_S3_ENDPOINT") {
        debug!("Configuring DuckDB with custom S3 endpoint");
        let sanitized_endpoint = sanitize_duckdb_config_value(&duckdb_s3_endpoint)?;
        s3_configuration_statements.push(format!("SET s3_endpoint='{}';", sanitized_endpoint));

        let duckdb_s3_use_ssl = std::env::var("DUCKDB_S3_USE_SSL")
            .unwrap_or_else(|_| "true".to_string())
            .to_lowercase();

        if duckdb_s3_use_ssl != "true" && duckdb_s3_use_ssl != "false" {
            let message = format!(
                "Invalid DUCKDB_S3_USE_SSL: must be 'true' or 'false', got '{}'",
                duckdb_s3_use_ssl
            );
            error!("{}", message);
            return Err(Error::Other(message));
        }

        s3_configuration_statements.push(format!("SET s3_use_ssl={};", duckdb_s3_use_ssl));
    }

    let s3_configuration_sql = s3_configuration_statements.join("\n");

    debug!("Configuring DuckDB S3 settings");
    connection.execute_batch(&s3_configuration_sql)?;

    info!("DuckDB connection established with S3 access");
    Ok(connection)
}

pub async fn query_equity_bars_parquet_from_s3(
    state: &State,
    tickers: Option<Vec<String>>,
    start_timestamp: Option<DateTime<Utc>>,
    end_timestamp: Option<DateTime<Utc>>,
) -> Result<Vec<u8>, Error> {
    let connection = create_duckdb_connection().await?;

    let (start_timestamp, end_timestamp) = match (start_timestamp, end_timestamp) {
        (Some(start), Some(end)) => (start, end),
        (Some(start), None) => {
            let end_date = chrono::Utc::now();
            info!(
                "No end date specified, defaulting to now: {} to {}",
                start, end_date
            );
            (start, end_date)
        }
        (None, Some(end)) => {
            let start_date = end - chrono::Duration::days(7);
            info!(
                "No start date specified, defaulting to 7 days before end: {} to {}",
                start_date, end
            );
            (start_date, end)
        }
        (None, None) => {
            let end_date = chrono::Utc::now();
            let start_date = end_date - chrono::Duration::days(7);
            info!(
                "No date range specified, using default: {} to {}",
                start_date, end_date
            );
            (start_date, end_date)
        }
    };

    info!(
        "Querying equity bars from {} to {}, bucket: {}",
        start_timestamp, end_timestamp, state.bucket_name
    );

    // Use glob pattern with hive partitioning to handle missing files gracefully
    let s3_glob = format!("s3://{}/equity/bars/daily/**/*.parquet", state.bucket_name);

    info!("Using S3 glob pattern: {}", s3_glob);

    // Build date filter for hive partitions
    let start_date_int = date_to_int(&start_timestamp)?;
    let end_date_int = date_to_int(&end_timestamp)?;

    debug!(
        "Date range filter: {} to {} (as integers)",
        start_date_int, end_date_int
    );

    // Build ticker filter
    let ticker_filter = match &tickers {
        Some(ticker_list) if !ticker_list.is_empty() => {
            debug!("Validating {} tickers for query filter", ticker_list.len());
            for ticker in ticker_list {
                if !is_valid_ticker(ticker) {
                    warn!("Invalid ticker format rejected: {}", ticker);
                    return Err(Error::Other(format!("Invalid ticker format: {}", ticker)));
                }
            }
            debug!("Ticker validation passed: {:?}", ticker_list);
            let ticker_values = ticker_list
                .iter()
                .map(|t| format!("'{}'", escape_sql_ticker(t)))
                .collect::<Vec<_>>()
                .join(", ");
            format!("AND ticker IN ({})", ticker_values)
        }
        _ => {
            debug!("No ticker filter applied, querying all tickers");
            String::new()
        }
    };

    let query_sql = format!(
        "
        SELECT
            ticker,
            timestamp,
            open_price,
            high_price,
            low_price,
            close_price,
            volume,
            volume_weighted_average_price,
            transactions
        FROM read_parquet('{}', hive_partitioning = true)
        WHERE (year::int * 10000 + month::int * 100 + day::int) BETWEEN {} AND {}
        {}
        ORDER BY timestamp, ticker
        ",
        s3_glob, start_date_int, end_date_int, ticker_filter
    );

    debug!("Executing query SQL: {}", query_sql);

    info!("Preparing DuckDB statement");
    let mut statement = connection.prepare(&query_sql)?;

    info!("Executing query and mapping results");
    let equity_bars: Vec<EquityBar> = statement
        .query_map([], |row| {
            Ok(EquityBar {
                ticker: row.get(0)?,
                timestamp: row.get(1)?,
                open_price: row.get(2)?,
                high_price: row.get(3)?,
                low_price: row.get(4)?,
                close_price: row.get(5)?,
                volume: row.get(6)?,
                volume_weighted_average_price: row.get(7)?,
                transactions: row.get(8)?,
            })
        })?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| {
            warn!("Failed to map query results: {}", e);
            Error::Other(format!("Failed to map query results: {}", e))
        })?;

    info!("Query returned {} equity bar records", equity_bars.len());

    if equity_bars.is_empty() {
        warn!(
            "No equity bar data found for date range {} to {}",
            start_timestamp, end_timestamp
        );
    }

    debug!("Creating DataFrame from equity bars");
    let equity_bars_dataframe = create_equity_bar_dataframe(equity_bars);

    let mut buffer = Vec::new();
    {
        let cursor = Cursor::new(&mut buffer);
        let writer = ParquetWriter::new(cursor);
        writer
            .finish(&mut equity_bars_dataframe?.clone())
            .map_err(|e| Error::Other(format!("Failed to write parquet: {}", e)))?;
    }

    info!("Query returned {} bytes of parquet data", buffer.len());

    Ok(buffer)
}

#[derive(Deserialize)]
pub struct PredictionQuery {
    pub ticker: String,
    pub timestamp: f64, // Unix timestamp as float
}

pub async fn query_predictions_dataframe_from_s3(
    state: &State,
    predictions_query: Vec<PredictionQuery>,
) -> Result<DataFrame, Error> {
    info!(
        "Querying predictions for {} ticker/timestamp pairs",
        predictions_query.len()
    );
    let connection = create_duckdb_connection().await?;

    let mut s3_paths = Vec::new();
    let mut tickers = Vec::new();

    for prediction_query in predictions_query.iter() {
        let timestamp_seconds = prediction_query.timestamp;
        let seconds = timestamp_seconds.trunc() as i64;
        let nanos = ((timestamp_seconds.fract()) * 1_000_000_000_f64).round() as u32;
        let timestamp = DateTime::<Utc>::from_timestamp(seconds, nanos)
            .ok_or_else(|| Error::Other("Invalid timestamp".into()))?;
        let year = timestamp.format("%Y");
        let month = timestamp.format("%m");
        let day = timestamp.format("%d");

        let s3_path = format!(
            "s3://{}/equity/predictions/daily/year={}/month={}/day={}/data.parquet",
            state.bucket_name, year, month, day
        );

        debug!(
            "Adding S3 path for ticker {} at {}/{}/{}: {}",
            prediction_query.ticker, year, month, day, s3_path
        );

        s3_paths.push(s3_path);

        tickers.push(prediction_query.ticker.clone());
    }

    if s3_paths.is_empty() {
        warn!("No prediction query positions provided");
        return Err(Error::Other("No positions provided".into()));
    }

    info!(
        "Querying {} S3 files for tickers: {:?}",
        s3_paths.len(),
        tickers
    );

    let s3_paths_query = s3_paths
        .iter()
        .map(|path| format!("SELECT * FROM '{}'", path))
        .collect::<Vec<_>>()
        .join(" UNION ALL ");

    let tickers_query = tickers
        .iter()
        .map(|ticker| format!("'{}'", ticker))
        .collect::<Vec<_>>()
        .join(", ");

    let query = format!(
        "
        SELECT
            ticker,
            timestamp,
            quantile_10,
            quantile_50,
            quantile_90
        FROM ({})
        WHERE ticker IN ({})
        ORDER BY timestamp, ticker
        ",
        s3_paths_query, tickers_query,
    );

    debug!("Executing export SQL: {}", query);

    info!("Preparing predictions query statement");
    let mut statement = connection.prepare(&query)?;

    info!("Executing predictions query and mapping results");
    let predictions: Vec<Prediction> = statement
        .query_map([], |row| {
            Ok(Prediction {
                ticker: row.get(0)?,
                timestamp: row.get(1)?,
                quantile_10: row.get(2)?,
                quantile_50: row.get(3)?,
                quantile_90: row.get(4)?,
            })
        })?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| {
            warn!("Failed to map predictions query results: {}", e);
            Error::Other(format!("Failed to map query results: {}", e))
        })?;

    info!("Query returned {} prediction records", predictions.len());

    debug!("Creating predictions DataFrame");
    let predictions_dataframe = create_predictions_dataframe(predictions)?;

    info!(
        "Predictions DataFrame created with {} rows",
        predictions_dataframe.height()
    );

    Ok(predictions_dataframe)
}

pub async fn query_portfolio_dataframe_from_s3(
    state: &State,
    timestamp: Option<DateTime<Utc>>,
) -> Result<DataFrame, Error> {
    info!(
        "Querying portfolio data, timestamp filter: {:?}",
        timestamp.map(|ts| ts.to_string())
    );
    let connection = create_duckdb_connection().await?;

    let (query_with_action, query_without_action) = match timestamp {
        Some(ts) => {
            let year = ts.format("%Y");
            let month = ts.format("%m");
            let day = ts.format("%d");
            let s3_path = format!(
                "s3://{}/equity/portfolios/daily/year={}/month={}/day={}/data.parquet",
                state.bucket_name, year, month, day
            );
            info!(
                "Querying specific date portfolio: {}/{}/{}",
                year, month, day
            );

            let with_action = format!(
                "
                SELECT
                    ticker,
                    timestamp,
                    side,
                    dollar_amount,
                    action
                FROM '{}'
                ORDER BY timestamp, ticker
                ",
                s3_path
            );

            let without_action = format!(
                "
                SELECT
                    ticker,
                    timestamp,
                    side,
                    dollar_amount
                FROM '{}'
                ORDER BY timestamp, ticker
                ",
                s3_path
            );

            (with_action, without_action)
        }
        None => {
            let s3_wildcard = format!(
                "s3://{}/equity/portfolios/daily/**/*.parquet",
                state.bucket_name
            );
            info!(
                "Querying most recent portfolio using hive partitioning: {}",
                s3_wildcard
            );

            let with_action = format!(
                "
                WITH partitioned_data AS (
                    SELECT
                        ticker,
                        timestamp,
                        side,
                        dollar_amount,
                        action,
                        year,
                        month,
                        day
                    FROM read_parquet('{}', hive_partitioning = true)
                ),
                max_date AS (
                    SELECT MAX(year::int * 10000 + month::int * 100 + day::int) as date_int
                    FROM partitioned_data
                )
                SELECT
                    ticker,
                    timestamp,
                    side,
                    dollar_amount,
                    action
                FROM partitioned_data
                WHERE (year::int * 10000 + month::int * 100 + day::int) = (SELECT date_int FROM max_date)
                ORDER BY timestamp, ticker
                ",
                s3_wildcard
            );

            let without_action = format!(
                "
                WITH partitioned_data AS (
                    SELECT
                        ticker,
                        timestamp,
                        side,
                        dollar_amount,
                        year,
                        month,
                        day
                    FROM read_parquet('{}', hive_partitioning = true)
                ),
                max_date AS (
                    SELECT MAX(year::int * 10000 + month::int * 100 + day::int) as date_int
                    FROM partitioned_data
                )
                SELECT
                    ticker,
                    timestamp,
                    side,
                    dollar_amount
                FROM partitioned_data
                WHERE (year::int * 10000 + month::int * 100 + day::int) = (SELECT date_int FROM max_date)
                ORDER BY timestamp, ticker
                ",
                s3_wildcard
            );

            (with_action, without_action)
        }
    };

    // Try query with action column first, fall back to query without if column doesn't exist
    let portfolios = match execute_portfolio_query_with_action(&connection, &query_with_action) {
        Ok(portfolios) => portfolios,
        Err(e) => {
            let err_str = e.to_string();
            if err_str.contains("action") && err_str.contains("not found") {
                info!(
                    "Action column not found in parquet, using fallback query with default action"
                );
                execute_portfolio_query_without_action(&connection, &query_without_action)?
            } else {
                return Err(e);
            }
        }
    };

    info!("Query returned {} portfolio records", portfolios.len());

    debug!("Creating portfolio DataFrame");
    let portfolio_dataframe = create_portfolio_dataframe(portfolios)?;

    info!(
        "Portfolio DataFrame created with {} rows",
        portfolio_dataframe.height()
    );

    Ok(portfolio_dataframe)
}

fn execute_portfolio_query_with_action(
    connection: &Connection,
    query: &str,
) -> Result<Vec<Portfolio>, Error> {
    debug!("Executing query with action column: {}", query);

    let mut statement = connection.prepare(query)?;

    let portfolios: Vec<Portfolio> = statement
        .query_map([], |row| {
            Ok(Portfolio {
                ticker: row.get::<_, String>(0)?,
                timestamp: row.get::<_, f64>(1)?,
                side: row.get::<_, String>(2)?,
                dollar_amount: row.get::<_, f64>(3)?,
                action: row.get::<_, String>(4)?,
            })
        })?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| {
            warn!("Failed to map portfolio query results: {}", e);
            Error::Other(format!("Failed to map query results: {}", e))
        })?;

    Ok(portfolios)
}

fn execute_portfolio_query_without_action(
    connection: &Connection,
    query: &str,
) -> Result<Vec<Portfolio>, Error> {
    debug!("Executing query without action column: {}", query);

    let mut statement = connection.prepare(query)?;

    let portfolios: Vec<Portfolio> = statement
        .query_map([], |row| {
            Ok(Portfolio {
                ticker: row.get::<_, String>(0)?,
                timestamp: row.get::<_, f64>(1)?,
                side: row.get::<_, String>(2)?,
                dollar_amount: row.get::<_, f64>(3)?,
                action: "UNSPECIFIED".to_string(),
            })
        })?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| {
            warn!("Failed to map portfolio query results: {}", e);
            Error::Other(format!("Failed to map query results: {}", e))
        })?;

    Ok(portfolios)
}

pub async fn read_equity_details_dataframe_from_s3(state: &State) -> Result<DataFrame, Error> {
    info!("Reading equity details CSV from S3");

    let key = "equity/details/categories.csv";

    let response = state
        .s3_client
        .get_object()
        .bucket(&state.bucket_name)
        .key(key)
        .send()
        .await
        .map_err(|e| Error::Other(format!("Failed to get object from S3: {}", e)))?;

    let bytes = response
        .body
        .collect()
        .await
        .map_err(|e| Error::Other(format!("Failed to read response body: {}", e)))?
        .into_bytes();

    let csv_content = String::from_utf8(bytes.to_vec())
        .map_err(|e| Error::Other(format!("Failed to convert bytes to UTF-8: {}", e)))?;

    info!(
        "Successfully read CSV from S3, size: {} bytes",
        csv_content.len()
    );

    let dataframe = create_equity_details_dataframe(csv_content)?;

    info!(
        "Successfully processed DataFrame with {} rows",
        dataframe.height()
    );

    Ok(dataframe)
}
