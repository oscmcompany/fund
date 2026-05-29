from datetime import UTC, datetime

_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


def to_timestamp_milliseconds(datetime_value: datetime) -> int:
    """Convert a datetime to a Unix timestamp in milliseconds (ms).

    All timestamps in this project use Unix milliseconds (Python int, Polars
    Int64, Rust i64). This unit was chosen because:
    - Massive sends bar timestamps natively in milliseconds.
    - Alpaca sends RFC-3339 strings; millisecond precision captures the
      meaningful part for OHLCV bar data without padding to nanoseconds.
    - For EOD data a whole-second integer would suffice, but milliseconds
      costs nothing extra and keeps the format consistent with the live-data
      WebSocket feeds we plan to add.

    Uses integer-only timedelta arithmetic to avoid floating-point truncation
    errors. Do not use int(datetime_value.timestamp() * 1000) as the float intermediate
    can yield off-by-1ms results for some datetimes.

    Raises ValueError if datetime_value is not timezone-aware, as naive
    datetimes produce system-timezone-dependent results.
    """
    if (
        datetime_value.tzinfo is None
        or datetime_value.tzinfo.utcoffset(datetime_value) is None
    ):
        message = "Datetime must be timezone-aware"
        raise ValueError(message)
    delta = datetime_value - _EPOCH
    return delta.days * 86_400_000 + delta.seconds * 1000 + delta.microseconds // 1000
