//! S3 data lake manifest for dataset discoverability.
//!
//! Writes a `data/manifest.json` file to the S3 bucket describing all
//! available datasets, their schemas, partitioning, and formats. This
//! enables researchers and downstream consumers (DuckDB, model trainers)
//! to discover datasets without reading application code.

use aws_sdk_s3::primitives::ByteStream;
use chrono::{DateTime, Utc};
use serde::Serialize;
use tracing::{info, warn};

const MANIFEST_KEY: &str = "data/manifest.json";

/// Top-level manifest describing the data lake contents.
#[derive(Debug, Serialize)]
pub struct Manifest {
    /// Schema version for forward compatibility.
    pub version: u32,
    /// When this manifest was last written.
    pub updated_at: DateTime<Utc>,
    /// All datasets in the data lake.
    pub datasets: Vec<DatasetDescriptor>,
}

/// Description of a single dataset in the data lake.
#[derive(Debug, Serialize)]
pub struct DatasetDescriptor {
    /// Human-readable dataset name.
    pub name: String,
    /// What this dataset contains.
    pub description: String,
    /// S3 key prefix (e.g., "data/equity/bars").
    pub prefix: String,
    /// File format ("parquet" or "csv").
    pub format: String,
    /// Hive partition keys in order (e.g., ["year", "month", "day"]).
    pub partitioning: Vec<String>,
    /// Column schema for the dataset.
    pub columns: Vec<ColumnDescriptor>,
}

/// Description of a single column in a dataset.
#[derive(Debug, Serialize)]
pub struct ColumnDescriptor {
    /// Column name.
    pub name: String,
    /// Data type (e.g., "String", "Int64", "Float64").
    pub data_type: String,
    /// Whether the column can contain null values.
    pub nullable: bool,
    /// Optional description of the column's semantics.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

/// Builds the canonical manifest describing all datasets in the data lake.
pub fn build_manifest() -> Manifest {
    Manifest {
        version: 1,
        updated_at: Utc::now(),
        datasets: vec![
            equity_bars_descriptor(),
            equity_quotes_descriptor(),
            equity_details_descriptor(),
        ],
    }
}

/// Converts a validation column schema into manifest column descriptors.
fn columns_from_schema(
    schema: &[(&str, polars::prelude::DataType, bool)],
    descriptions: &[(&str, &str)],
) -> Vec<ColumnDescriptor> {
    schema
        .iter()
        .map(|(name, data_type, nullable)| {
            let description = descriptions
                .iter()
                .find(|(column_name, _)| column_name == name)
                .map(|(_, description)| description.to_string());
            ColumnDescriptor {
                name: name.to_string(),
                data_type: format!("{:?}", data_type),
                nullable: *nullable,
                description,
            }
        })
        .collect()
}

fn equity_bars_descriptor() -> DatasetDescriptor {
    use crate::data::validation::EQUITY_BARS_COLUMNS;

    let descriptions: &[(&str, &str)] = &[
        ("ticker", "Uppercase ticker symbol"),
        ("timestamp", "Unix timestamp in milliseconds"),
        ("transactions", "Number of trades in the bar"),
    ];

    DatasetDescriptor {
        name: "equity_bars".to_string(),
        description: "Daily OHLCV equity bars from Massive API".to_string(),
        prefix: "data/equity/bars".to_string(),
        format: "parquet".to_string(),
        partitioning: vec!["year".to_string(), "month".to_string(), "day".to_string()],
        columns: columns_from_schema(EQUITY_BARS_COLUMNS, descriptions),
    }
}

fn equity_quotes_descriptor() -> DatasetDescriptor {
    DatasetDescriptor {
        name: "equity_quotes".to_string(),
        description: "Intraday bid/ask equity quotes".to_string(),
        prefix: "data/equity/quotes".to_string(),
        format: "parquet".to_string(),
        partitioning: vec!["year".to_string(), "month".to_string(), "day".to_string()],
        columns: vec![
            ColumnDescriptor {
                name: "timestamp".to_string(),
                data_type: "Int64".to_string(),
                nullable: false,
                description: Some("Unix timestamp in milliseconds".to_string()),
            },
            ColumnDescriptor {
                name: "ticker".to_string(),
                data_type: "String".to_string(),
                nullable: false,
                description: Some("Uppercase ticker symbol".to_string()),
            },
            ColumnDescriptor {
                name: "bid_price".to_string(),
                data_type: "Float64".to_string(),
                nullable: false,
                description: None,
            },
            ColumnDescriptor {
                name: "ask_price".to_string(),
                data_type: "Float64".to_string(),
                nullable: false,
                description: None,
            },
            ColumnDescriptor {
                name: "bid_size".to_string(),
                data_type: "Int32".to_string(),
                nullable: false,
                description: None,
            },
            ColumnDescriptor {
                name: "ask_size".to_string(),
                data_type: "Int32".to_string(),
                nullable: false,
                description: None,
            },
        ],
    }
}

fn equity_details_descriptor() -> DatasetDescriptor {
    DatasetDescriptor {
        name: "equity_details".to_string(),
        description: "Equity reference data (ticker, sector, industry)".to_string(),
        prefix: "data/equity/details".to_string(),
        format: "csv".to_string(),
        partitioning: vec![],
        columns: vec![
            ColumnDescriptor {
                name: "ticker".to_string(),
                data_type: "String".to_string(),
                nullable: false,
                description: Some("Uppercase ticker symbol".to_string()),
            },
            ColumnDescriptor {
                name: "sector".to_string(),
                data_type: "String".to_string(),
                nullable: false,
                description: Some("GICS sector or 'NOT AVAILABLE'".to_string()),
            },
            ColumnDescriptor {
                name: "industry".to_string(),
                data_type: "String".to_string(),
                nullable: false,
                description: Some("Industry classification or 'NOT AVAILABLE'".to_string()),
            },
        ],
    }
}

/// Serializes and uploads the manifest to S3.
pub async fn write_manifest(s3_client: &aws_sdk_s3::Client, bucket_name: &str) {
    let manifest = build_manifest();

    let json = match serde_json::to_string_pretty(&manifest) {
        Ok(json) => json,
        Err(error) => {
            warn!(error = %error, "Failed to serialize manifest");
            return;
        }
    };

    match s3_client
        .put_object()
        .bucket(bucket_name)
        .key(MANIFEST_KEY)
        .content_type("application/json")
        .body(ByteStream::from(json.into_bytes()))
        .send()
        .await
    {
        Ok(_) => info!(key = MANIFEST_KEY, "Wrote data lake manifest to S3"),
        Err(error) => warn!(error = %error, "Failed to upload manifest to S3"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_manifest_has_three_datasets() {
        let manifest = build_manifest();
        assert_eq!(manifest.version, 1);
        assert_eq!(manifest.datasets.len(), 3);
    }

    #[test]
    fn test_equity_bars_descriptor_has_nine_columns() {
        let descriptor = equity_bars_descriptor();
        assert_eq!(descriptor.name, "equity_bars");
        assert_eq!(descriptor.format, "parquet");
        assert_eq!(descriptor.columns.len(), 9);
        assert_eq!(descriptor.partitioning, vec!["year", "month", "day"]);
    }

    #[test]
    fn test_equity_bars_column_names_match_pandera_contract() {
        let descriptor = equity_bars_descriptor();
        let column_names: Vec<&str> = descriptor.columns.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(
            column_names,
            vec![
                "ticker",
                "timestamp",
                "open_price",
                "high_price",
                "low_price",
                "close_price",
                "volume",
                "volume_weighted_average_price",
                "transactions",
            ]
        );
    }

    #[test]
    fn test_equity_quotes_descriptor_has_six_columns() {
        let descriptor = equity_quotes_descriptor();
        assert_eq!(descriptor.name, "equity_quotes");
        assert_eq!(descriptor.columns.len(), 6);
    }

    #[test]
    fn test_equity_details_descriptor_is_csv() {
        let descriptor = equity_details_descriptor();
        assert_eq!(descriptor.format, "csv");
        assert!(descriptor.partitioning.is_empty());
        assert_eq!(descriptor.columns.len(), 3);
    }

    #[test]
    fn test_manifest_serializes_to_json() {
        let manifest = build_manifest();
        let json = serde_json::to_string_pretty(&manifest).unwrap();
        assert!(json.contains("\"version\": 1"));
        assert!(json.contains("\"equity_bars\""));
        assert!(json.contains("\"equity_quotes\""));
        assert!(json.contains("\"equity_details\""));
    }

    #[test]
    fn test_column_description_omitted_when_none() {
        let column = ColumnDescriptor {
            name: "test".to_string(),
            data_type: "String".to_string(),
            nullable: false,
            description: None,
        };
        let json = serde_json::to_string(&column).unwrap();
        assert!(!json.contains("description"));
    }

    #[test]
    fn test_nullable_columns_marked_correctly() {
        let descriptor = equity_bars_descriptor();
        let non_nullable: Vec<&str> = descriptor
            .columns
            .iter()
            .filter(|c| !c.nullable)
            .map(|c| c.name.as_str())
            .collect();
        assert_eq!(
            non_nullable,
            vec![
                "ticker",
                "timestamp",
                "open_price",
                "high_price",
                "low_price",
                "close_price",
                "volume",
            ]
        );
    }
}
