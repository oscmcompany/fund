from datetime import UTC, datetime

_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


def to_timestamp_milliseconds(dt: datetime) -> int:
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
    errors. Do not use int(dt.timestamp() * 1000) as the float intermediate
    can yield off-by-1ms results for some datetimes.

    Raises ValueError if dt is not timezone-aware, as naive datetimes produce
    system-timezone-dependent results.
    """
    if dt.tzinfo is None or dt.tzinfo.utcoffset(dt) is None:
        message = "Datetime must be timezone-aware"
        raise ValueError(message)
    delta = dt - _EPOCH
    return delta.days * 86_400_000 + delta.seconds * 1000 + delta.microseconds // 1000
