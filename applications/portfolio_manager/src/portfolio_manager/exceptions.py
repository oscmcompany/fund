class InsufficientBuyingPowerError(Exception):
    """Raised when there is insufficient buying power to place an order."""


class AssetNotShortableError(Exception):
    """Raised when attempting to short an asset that cannot be shorted."""


class PriceDataUnavailableError(Exception):
    """Raised when historical price data cannot be fetched from data manager."""


class InsufficientPairsError(Exception):
    """Raised when there are insufficient pairs to create a portfolio."""
