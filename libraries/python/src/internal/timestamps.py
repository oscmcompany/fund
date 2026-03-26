from datetime import datetime


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

    Use this helper at every site that converts a Python datetime to a stored
    or transmitted timestamp value. Do not use `int(dt.timestamp())` directly
    as that produces seconds, not milliseconds.

    Raises ValueError if dt is not timezone-aware, as naive datetimes produce
    system-timezone-dependent results.
    """
    if dt.tzinfo is None or dt.tzinfo.utcoffset(dt) is None:
        message = "Datetime must be timezone-aware"
        raise ValueError(message)
    return int(dt.timestamp() * 1000)
