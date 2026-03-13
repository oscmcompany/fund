import json
import os
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch

from tools.sync_equity_bars_data import (
    sync_equity_bars_data,
    sync_equity_bars_for_date,
    validate_and_parse_dates,
)


def _recent_date_range_json(days_ago_start: int = 10, days_ago_end: int = 9) -> str:
    now = datetime.now(tz=UTC).replace(hour=0, minute=0, second=0, microsecond=0)
    start = (now - timedelta(days=days_ago_start)).strftime("%Y-%m-%d")
    end = (now - timedelta(days=days_ago_end)).strftime("%Y-%m-%d")
    return json.dumps({"start_date": start, "end_date": end})


def test_validate_and_parse_dates_returns_datetime_tuple() -> None:
    date_range_json = _recent_date_range_json()

    start_date, end_date = validate_and_parse_dates(date_range_json)

    assert isinstance(start_date, datetime)
    assert isinstance(end_date, datetime)
    assert start_date.tzinfo is UTC
    assert end_date.tzinfo is UTC
    assert start_date <= end_date


def test_validate_and_parse_dates_clamps_to_current_day() -> None:
    now = datetime.now(tz=UTC).replace(hour=0, minute=0, second=0, microsecond=0)
    date_range_json = json.dumps(
        {
            "start_date": now.strftime("%Y-%m-%d"),
            "end_date": now.strftime("%Y-%m-%d"),
        }
    )

    _, end_date = validate_and_parse_dates(date_range_json)

    assert end_date <= now


def test_sync_equity_bars_for_date_returns_status_and_body() -> None:
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.text = '{"synced": true}'

    target_date = datetime(2025, 6, 2, tzinfo=UTC)

    with patch(
        "tools.sync_equity_bars_data.requests.post", return_value=mock_response
    ) as mock_post:
        status_code, response_text = sync_equity_bars_for_date(
            base_url="http://localhost:8080",
            date_value=target_date,
        )

    assert status_code == 200  # noqa: PLR2004
    assert response_text == '{"synced": true}'
    mock_post.assert_called_once()
    call_kwargs = mock_post.call_args
    assert call_kwargs.args[0] == "http://localhost:8080/equity-bars"
    assert call_kwargs.kwargs["json"]["date"] == "2025-06-02T00:00:00Z"


def test_sync_equity_bars_data_single_date_makes_one_request() -> None:
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.text = "ok"
    target_date = datetime(2025, 6, 2, tzinfo=UTC)

    with patch(
        "tools.sync_equity_bars_data.requests.post", return_value=mock_response
    ) as mock_post:
        sync_equity_bars_data(
            base_url="http://localhost:8080",
            date_range=(target_date, target_date),
        )

    assert mock_post.call_count == 1


@patch("tools.sync_equity_bars_data.time.sleep")
@patch("tools.sync_equity_bars_data.boto3")
def test_sync_equity_bars_data_only_syncs_missing_dates(
    mock_boto3: MagicMock,
    mock_sleep: MagicMock,
) -> None:
    mock_s3 = MagicMock()
    mock_boto3.client.return_value = mock_s3
    mock_s3.list_objects_v2.return_value = {
        "Contents": [
            {
                "Key": "equity/bars/daily/year=2025/month=06/day=02/data.parquet",
            }
        ],
        "IsTruncated": False,
    }

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.text = "ok"

    start_date = datetime(2025, 6, 2, tzinfo=UTC)
    end_date = datetime(2025, 6, 4, tzinfo=UTC)

    with patch.dict(os.environ, {"AWS_S3_DATA_BUCKET_NAME": "data-bucket"}):
        with patch(
            "tools.sync_equity_bars_data.requests.post", return_value=mock_response
        ) as mock_post:
            sync_equity_bars_data(
                base_url="http://localhost:8080",
                date_range=(start_date, end_date),
            )

    assert mock_post.call_count == 2
    synced_dates = [
        call.kwargs["json"]["date"] for call in mock_post.call_args_list
    ]
    assert synced_dates == ["2025-06-03T00:00:00Z", "2025-06-04T00:00:00Z"]
    mock_sleep.assert_called_once_with(1.0)
