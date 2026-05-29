from dataclasses import dataclass


@dataclass
class Configuration:
    # Minimum account equity required by Alpaca (FINRA rule) to execute short sells.
    minimum_short_equity: float = 2000.0
    # Multiplier Alpaca applies to the ask price when reserving buying power for
    # short market orders (ask * buffer * qty charged against buying power).
    short_buying_power_buffer: float = 1.03
