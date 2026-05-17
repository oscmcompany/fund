from dataclasses import dataclass


@dataclass
class Configuration:
    # When True, positions are held overnight and overnight margin rules apply.
    # Set to False for intraday strategies where positions are closed before
    # market close and no overnight maintenance margin needs to be reserved.
    hold_overnight: bool = True
    # Minimum account equity required by Alpaca (FINRA rule) to execute short sells.
    minimum_short_equity: float = 2000.0
    # Multiplier Alpaca applies to the ask price when reserving buying power for
    # short market orders (ask * buffer * qty charged against buying power).
    short_buying_power_buffer: float = 1.03
    # Maintenance margin rate for stocks priced at or above low_price_threshold.
    # Applied only when hold_overnight=True.
    overnight_margin_rate_standard: float = 0.30
    # Maintenance margin rate for stocks priced below low_price_threshold.
    # Applied only when hold_overnight=True.
    overnight_margin_rate_low_price: float = 1.00
    # Price boundary separating standard and low-price overnight margin rates.
    low_price_threshold: float = 5.00
