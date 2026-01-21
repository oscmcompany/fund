import polars as pl
import pytest
from equitypricemodel.equity_details_schema import equity_details_schema
from pandera.errors import SchemaError


def test_equity_details_schema_valid_data() -> None:
    valid_data = pl.DataFrame(
        {
            "ticker": ["AAPL"],
            "sector": ["TECHNOLOGY"],
            "industry": ["SOFTWARE"],
        }
    )

    validated_df = equity_details_schema.validate(valid_data)
    assert validated_df.shape == (1, 3)


def test_equity_details_schema_sector_lowercase_fails() -> None:
    data = pl.DataFrame(
        {
            "ticker": ["AAPL"],
            "sector": ["technology"],
            "industry": ["SOFTWARE"],
        }
    )

    with pytest.raises(SchemaError):
        equity_details_schema.validate(data)


def test_equity_details_schema_industry_lowercase_fails() -> None:
    data = pl.DataFrame(
        {
            "ticker": ["AAPL"],
            "sector": ["TECHNOLOGY"],
            "industry": ["software"],
        }
    )

    with pytest.raises(SchemaError):
        equity_details_schema.validate(data)


def test_equity_details_schema_both_fields_uppercase_passes() -> None:
    data = pl.DataFrame(
        {
            "ticker": ["JNJ"],
            "sector": ["HEALTHCARE"],
            "industry": ["PHARMACEUTICALS"],
        }
    )

    validated_df = equity_details_schema.validate(data)
    assert validated_df["sector"][0] == "HEALTHCARE"
    assert validated_df["industry"][0] == "PHARMACEUTICALS"


def test_equity_details_schema_whitespace_fails() -> None:
    data = pl.DataFrame(
        {
            "ticker": ["AAPL"],
            "sector": ["  TECHNOLOGY  "],
            "industry": ["SOFTWARE"],
        }
    )

    with pytest.raises(SchemaError):
        equity_details_schema.validate(data)


def test_equity_details_schema_industry_whitespace_fails() -> None:
    data = pl.DataFrame(
        {
            "ticker": ["AAPL"],
            "sector": ["TECHNOLOGY"],
            "industry": ["  SOFTWARE  "],
        }
    )

    with pytest.raises(SchemaError):
        equity_details_schema.validate(data)


def test_equity_details_schema_no_whitespace_passes() -> None:
    data = pl.DataFrame(
        {
            "ticker": ["JNJ"],
            "sector": ["HEALTHCARE"],
            "industry": ["PHARMACEUTICALS"],
        }
    )

    validated_df = equity_details_schema.validate(data)
    assert validated_df["sector"][0] == "HEALTHCARE"
    assert validated_df["industry"][0] == "PHARMACEUTICALS"


def test_equity_details_schema_null_sector() -> None:
    data = pl.DataFrame(
        {
            "ticker": ["AAPL"],
            "sector": [None],
            "industry": ["SOFTWARE"],
        }
    )

    validated_df = equity_details_schema.validate(data)
    assert validated_df["sector"][0] == "NOT AVAILABLE"
    assert validated_df["industry"][0] == "SOFTWARE"


def test_equity_details_schema_null_industry() -> None:
    data = pl.DataFrame(
        {
            "ticker": ["AAPL"],
            "sector": ["TECHNOLOGY"],
            "industry": [None],
        }
    )

    validated_df = equity_details_schema.validate(data)
    assert validated_df["industry"][0] == "NOT AVAILABLE"
    assert validated_df["sector"][0] == "TECHNOLOGY"


def test_equity_details_schema_missing_sector_column() -> None:
    data = pl.DataFrame(
        {
            "ticker": ["AAPL"],
            "industry": ["SOFTWARE"],
        }
    )

    with pytest.raises((SchemaError, pl.exceptions.ColumnNotFoundError)):
        equity_details_schema.validate(data)


def test_equity_details_schema_missing_industry_column() -> None:
    data = pl.DataFrame(
        {
            "ticker": ["AAPL"],
            "sector": ["TECHNOLOGY"],
        }
    )

    with pytest.raises((SchemaError, pl.exceptions.ColumnNotFoundError)):
        equity_details_schema.validate(data)


def test_equity_details_schema_type_coercion() -> None:
    data = pl.DataFrame(
        {
            "ticker": ["AAPL"],
            "sector": [123],  # coerced to string
            "industry": [456],  # coerced to string
        }
    )

    validated_df = equity_details_schema.validate(data)
    assert validated_df["sector"].dtype == pl.String
    assert validated_df["industry"].dtype == pl.String
    assert validated_df["sector"][0] == "123"
    assert validated_df["industry"][0] == "456"


def test_equity_details_schema_multiple_rows() -> None:
    data = pl.DataFrame(
        {
            "ticker": ["AAPL", "JNJ", "JPM"],
            "sector": ["TECHNOLOGY", "HEALTHCARE", "FINANCE"],
            "industry": ["SOFTWARE", "PHARMACEUTICALS", "BANKING"],
        }
    )

    validated_df = equity_details_schema.validate(data)
    assert validated_df.shape == (3, 3)
    assert validated_df["sector"].to_list() == ["TECHNOLOGY", "HEALTHCARE", "FINANCE"]
    assert validated_df["industry"].to_list() == [
        "SOFTWARE",
        "PHARMACEUTICALS",
        "BANKING",
    ]


def test_equity_details_schema_mixed_case_fails() -> None:
    data = pl.DataFrame(
        {
            "ticker": ["AAPL"],
            "sector": ["TeChnOlOgY"],
            "industry": ["SOFTWARE"],
        }
    )

    with pytest.raises(SchemaError):
        equity_details_schema.validate(data)


def test_equity_details_schema_empty_string() -> None:
    data = pl.DataFrame(
        {
            "ticker": ["AAPL"],
            "sector": [""],
            "industry": ["SOFTWARE"],
        }
    )

    validated_df = equity_details_schema.validate(data)
    assert validated_df["sector"][0] == ""


def test_equity_details_schema_whitespace_only_fails() -> None:
    data = pl.DataFrame(
        {
            "ticker": ["AAPL"],
            "sector": ["   "],
            "industry": ["SOFTWARE"],
        }
    )

    # whitespace-only strings should fail validation
    with pytest.raises(SchemaError):
        equity_details_schema.validate(data)


def test_equity_details_schema_special_characters() -> None:
    data = pl.DataFrame(
        {
            "ticker": ["BRK.A"],
            "sector": ["REAL-ESTATE"],
            "industry": ["RETAIL & WHOLESALE"],
        }
    )

    validated_df = equity_details_schema.validate(data)
    assert validated_df["sector"][0] == "REAL-ESTATE"
    assert validated_df["industry"][0] == "RETAIL & WHOLESALE"
