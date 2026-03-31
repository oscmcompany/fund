from datetime import UTC, datetime

import pytest
from internal.timestamps import to_timestamp_milliseconds

_YEAR_2000_MS = 946_684_800_000


def test_to_timestamp_milliseconds_known_value() -> None:
    dt = datetime(2000, 1, 1, 0, 0, 0, tzinfo=UTC)
    assert to_timestamp_milliseconds(dt) == _YEAR_2000_MS


def test_to_timestamp_milliseconds_rejects_naive_datetime() -> None:
    dt = datetime(2000, 1, 1, 0, 0, 0)  # noqa: DTZ001
    with pytest.raises(ValueError, match="timezone-aware"):
        to_timestamp_milliseconds(dt)
