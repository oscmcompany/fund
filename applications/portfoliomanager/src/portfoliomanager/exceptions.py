class InsufficientPredictionsError(Exception):
    """Raised when there are insufficient predictions to create a portfolio."""


class InsufficientBuyingPowerError(Exception):
    """Raised when there is insufficient buying power to place an order."""


class AssetNotShortableError(Exception):
    """Raised when attempting to short an asset that cannot be shorted."""


class PortfolioDataError(Exception):
    """Raised when portfolio data preparation fails."""
