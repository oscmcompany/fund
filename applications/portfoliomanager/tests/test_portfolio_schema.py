from datetime import UTC, datetime
from typing import cast

import polars as pl
import pytest
from pandera.errors import SchemaError
from pandera.polars import PolarsData
from portfoliomanager.portfolio_schema import (
    check_pair_tickers_different,
    check_position_side_counts,
    check_position_side_sums,
    pairs_schema,
    portfolio_schema,
)


class _MockPolarsData:
    def __init__(self, df: pl.DataFrame) -> None:
        self.lazyframe = df.lazy()
        self.key = "side"


def _as_polars_data(df: pl.DataFrame) -> PolarsData:
    return cast("PolarsData", _MockPolarsData(df))


_TICKERS = [
    "AAPL",
    "GOOGL",
    "MSFT",
    "AMZN",
    "TSLA",
    "NVDA",
    "META",
    "NFLX",
    "BABA",
    "CRM",
    "AMD",
    "INTC",
    "ORCL",
    "ADBE",
    "PYPL",
    "SHOP",
    "SPOT",
    "ROKU",
    "ZM",
    "DOCU",
]
_PAIR_IDS = [
    "AAPL-AMD",
    "AAPL-AMD",
    "GOOGL-INTC",
    "GOOGL-INTC",
    "MSFT-ORCL",
    "MSFT-ORCL",
    "AMZN-ADBE",
    "AMZN-ADBE",
    "TSLA-PYPL",
    "TSLA-PYPL",
    "NVDA-SHOP",
    "NVDA-SHOP",
    "META-SPOT",
    "META-SPOT",
    "NFLX-ROKU",
    "NFLX-ROKU",
    "BABA-ZM",
    "BABA-ZM",
    "CRM-DOCU",
    "CRM-DOCU",
]


def test_portfolio_schema_valid_data() -> None:
    valid_data = pl.DataFrame(
        {
            "ticker": _TICKERS,
            "timestamp": [datetime(2025, 1, 1, 0, 0, 0, 0, tzinfo=UTC).timestamp()]
            * 20,
            "side": (["LONG"] * 10) + (["SHORT"] * 10),
            "dollar_amount": [1000.0] * 20,
            "pair_id": _PAIR_IDS,
        }
    )

    validated_df = portfolio_schema.validate(valid_data)
    assert validated_df.shape == (20, 5)


def test_portfolio_schema_ticker_lowercase_fails() -> None:
    data = pl.DataFrame(
        {
            "ticker": ["aapl"],  # lowercase should fail
            "timestamp": [datetime(2025, 1, 1, 0, 0, 0, 0, tzinfo=UTC).timestamp()],
            "side": ["LONG"],
            "dollar_amount": [1000.0],
            "pair_id": ["AAPL-MSFT"],
        }
    )

    with pytest.raises(SchemaError):
        portfolio_schema.validate(data)


def test_portfolio_schema_invalid_side_fails() -> None:
    data = pl.DataFrame(
        {
            "ticker": ["AAPL"],
            "timestamp": [datetime(2025, 1, 1, 0, 0, 0, 0, tzinfo=UTC).timestamp()],
            "side": ["BUY"],  # Invalid side value
            "dollar_amount": [1000.0],
            "pair_id": ["AAPL-MSFT"],
        }
    )

    with pytest.raises(SchemaError):
        portfolio_schema.validate(data)


def test_portfolio_schema_negative_dollar_amount_fails() -> None:
    data = pl.DataFrame(
        {
            "ticker": ["AAPL"],
            "timestamp": [datetime(2025, 1, 1, 0, 0, 0, 0, tzinfo=UTC).timestamp()],
            "side": ["LONG"],
            "dollar_amount": [-1000.0],  # Negative amount should fail
            "pair_id": ["AAPL-MSFT"],
        }
    )

    with pytest.raises(SchemaError):
        portfolio_schema.validate(data)


def test_portfolio_schema_unbalanced_sides_fails() -> None:
    data = pl.DataFrame(
        {
            "ticker": _TICKERS,
            "timestamp": [datetime(2025, 1, 1, 0, 0, 0, 0, tzinfo=UTC).timestamp()]
            * 20,
            "side": ["LONG"] * 15 + ["SHORT"] * 5,  # Unbalanced: 15 LONG, 5 SHORT
            "dollar_amount": [1000.0] * 20,
            "pair_id": _PAIR_IDS,
        }
    )

    with pytest.raises(SchemaError, match="Expected 10 long side positions, found: 15"):
        portfolio_schema.validate(data)


def test_portfolio_schema_imbalanced_dollar_amounts_fails() -> None:
    data = pl.DataFrame(
        {
            "ticker": _TICKERS,
            "timestamp": [datetime(2025, 1, 1, 0, 0, 0, 0, tzinfo=UTC).timestamp()]
            * 20,
            "side": (["LONG"] * 10) + (["SHORT"] * 10),
            "dollar_amount": ([2000.0] * 10)
            + ([500.0] * 10),  # Very imbalanced amounts
            "pair_id": _PAIR_IDS,
        }
    )

    with pytest.raises(SchemaError, match="long and short dollar amount sums"):
        portfolio_schema.validate(data)


def test_portfolio_schema_duplicate_tickers_fails() -> None:
    data = pl.DataFrame(
        {
            "ticker": ["AAPL", "AAPL"],  # Duplicate ticker
            "timestamp": [datetime(2025, 1, 1, 0, 0, 0, 0, tzinfo=UTC).timestamp()] * 2,
            "side": ["LONG", "SHORT"],
            "dollar_amount": [1000.0, 1000.0],
            "pair_id": ["AAPL-MSFT", "AAPL-MSFT"],
        }
    )

    with pytest.raises(SchemaError):
        portfolio_schema.validate(data)


def test_portfolio_schema_zero_timestamp_fails() -> None:
    data = pl.DataFrame(
        {
            "ticker": ["AAPL"],
            "timestamp": [0.0],  # Zero timestamp should fail
            "side": ["LONG"],
            "dollar_amount": [1000.0],
            "pair_id": ["AAPL-MSFT"],
        }
    )

    with pytest.raises(SchemaError):
        portfolio_schema.validate(data)


def test_check_position_side_counts_short_count_mismatch_raises() -> None:
    data = pl.DataFrame(
        {
            "side": ["LONG"] * 10 + ["SHORT"] * 5,
            "dollar_amount": [1000.0] * 15,
        }
    )

    with pytest.raises(ValueError, match="Expected 10 short side positions, found: 5"):
        check_position_side_counts(_as_polars_data(data))


def test_check_position_side_counts_total_count_mismatch_raises() -> None:
    data = pl.DataFrame(
        {
            "side": ["LONG"] * 10 + ["SHORT"] * 10,
            "dollar_amount": [1000.0] * 20,
        }
    )

    with pytest.raises(ValueError, match="Expected 21 total positions, found: 20"):
        check_position_side_counts(_as_polars_data(data), total_positions_count=21)


def test_check_position_side_sums_zero_total_raises() -> None:
    data = pl.DataFrame(
        {
            "side": ["LONG"] * 10 + ["SHORT"] * 10,
            "dollar_amount": [0.0] * 20,
        }
    )

    with pytest.raises(ValueError, match="Total dollar amount must be > 0"):
        check_position_side_sums(_as_polars_data(data))


def test_pairs_schema_validates_valid_pairs() -> None:
    data = pl.DataFrame(
        {
            "pair_id": ["AAPL-MSFT", "GOOG-AMZN"],
            "long_ticker": ["AAPL", "GOOG"],
            "short_ticker": ["MSFT", "AMZN"],
            "z_score": [2.5, 3.1],
            "hedge_ratio": [1.2, 0.8],
            "signal_strength": [0.4, 0.6],
            "long_realized_volatility": [0.02, 0.03],
            "short_realized_volatility": [0.018, 0.025],
        }
    )

    validated = pairs_schema.validate(data)
    assert validated.shape[0] == len(data)


def test_portfolio_schema_missing_pair_id_fails() -> None:
    data = pl.DataFrame(
        {
            "ticker": ["AAPL"],
            "timestamp": [datetime(2025, 1, 1, 0, 0, 0, 0, tzinfo=UTC).timestamp()],
            "side": ["LONG"],
            "dollar_amount": [1000.0],
        }
    )

    with pytest.raises((SchemaError, pl.exceptions.ColumnNotFoundError)):
        portfolio_schema.validate(data)


def test_check_pair_tickers_different_same_ticker_raises() -> None:
    data = pl.DataFrame(
        {
            "long_ticker": ["AAPL", "AAPL"],
            "short_ticker": ["MSFT", "AAPL"],  # second row: same ticker
        }
    )

    with pytest.raises(
        ValueError, match="long_ticker and short_ticker must be different"
    ):
        check_pair_tickers_different(_as_polars_data(data))
