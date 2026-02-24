from unittest.mock import MagicMock, patch

from tools.sync_equity_details_data import sync_equity_details, sync_equity_details_data


def test_sync_equity_details_returns_status_and_body() -> None:
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.text = '{"synced": true}'

    with patch(
        "tools.sync_equity_details_data.requests.post", return_value=mock_response
    ) as mock_post:
        status_code, response_text = sync_equity_details(
            base_url="http://localhost:8080"
        )

    assert status_code == 200  # noqa: PLR2004
    assert response_text == '{"synced": true}'
    mock_post.assert_called_once_with(
        "http://localhost:8080/equity-details", timeout=300
    )


def test_sync_equity_details_data_success() -> None:
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.text = "ok"

    with patch(
        "tools.sync_equity_details_data.requests.post", return_value=mock_response
    ):
        sync_equity_details_data(base_url="http://localhost:8080")
