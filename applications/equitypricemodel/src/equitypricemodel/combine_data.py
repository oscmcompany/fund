import sys

import polars as pl
import structlog

from .equity_details_schema import equity_details_schema


def combine_data(
    categories_csv_path: str,
    equity_bars_csv_path: str,
    output_csv_path: str,
) -> None:
    logger = structlog.get_logger()

    try:
        categories_data = pl.read_csv(categories_csv_path)
    except Exception as e:
        logger.exception(
            "Failed to read categories CSV", path=categories_csv_path, error=str(e)
        )
        raise

    try:
        equity_details_data = equity_details_schema.validate(categories_data)
    except Exception as e:
        logger.exception("Categories data validation failed", error=str(e))
        raise

    try:
        equity_bars_data = pl.read_csv(equity_bars_csv_path)
    except Exception as e:
        logger.exception(
            "Failed to read equity bars CSV", path=equity_bars_csv_path, error=str(e)
        )
        raise

    consolidated_data = equity_details_data.join(
        equity_bars_data, on="ticker", how="inner"
    )

    retained_columns = (
        "ticker",
        "timestamp",
        "open_price",
        "high_price",
        "low_price",
        "close_price",
        "volume",
        "volume_weighted_average_price",
        "sector",
        "industry",
    )

    missing_columns = [
        col for col in retained_columns if col not in consolidated_data.columns
    ]
    if missing_columns:
        logger.error(
            "Missing required columns in consolidated data",
            missing_columns=missing_columns,
        )
        message = f"Missing required columns: {', '.join(missing_columns)}"
        raise ValueError(message)

    filtered_data = consolidated_data.select(retained_columns)

    try:
        filtered_data.write_csv(output_csv_path)
    except Exception as e:
        logger.exception(
            "Failed to write output CSV",
            output_csv_path=output_csv_path,
            error=str(e),
        )
        raise


if __name__ == "__main__":
    logger = structlog.get_logger()

    if len(sys.argv) != 4:  # noqa: PLR2004
        logger.error(
            "Requires categories CSV, equity bars CSV, and output CSV paths as arguments",  # noqa: E501
        )
        sys.exit(1)

    combine_data(sys.argv[1], sys.argv[2], sys.argv[3])
