import time
from typing import cast

import structlog
from alpaca.common.exceptions import APIError
from alpaca.data import StockHistoricalDataClient
from alpaca.trading import (
    ClosePositionRequest,
    OrderRequest,
    TradeAccount,
    TradingClient,
)
from alpaca.trading.enums import OrderSide, OrderType, TimeInForce

from .enums import TradeSide
from .exceptions import AssetNotShortableError, InsufficientBuyingPowerError

logger = structlog.get_logger(__name__)


class AlpacaAccount:
    def __init__(
        self,
        cash_amount: float,
        buying_power: float,
    ) -> None:
        self.cash_amount = cash_amount
        self.buying_power = buying_power


class AlpacaClient:
    def __init__(
        self,
        api_key: str,
        api_secret: str,
        is_paper: bool,  # noqa: FBT001
    ) -> None:
        self.rate_limit_sleep = 0.5  # seconds

        self.trading_client = TradingClient(
            api_key=api_key,
            secret_key=api_secret,
            paper=is_paper,
        )

        self.data_client = StockHistoricalDataClient(
            api_key=api_key,
            secret_key=api_secret,
        )

        self.is_paper = is_paper

    def get_account(self) -> AlpacaAccount:
        account: TradeAccount = cast("TradeAccount", self.trading_client.get_account())

        time.sleep(self.rate_limit_sleep)

        return AlpacaAccount(
            cash_amount=float(cast("str", account.cash)),
            buying_power=float(cast("str", account.buying_power)),
        )

    def open_position(
        self,
        ticker: str,
        side: TradeSide,
        dollar_amount: float,
    ) -> None:
        # Use notional (dollar amount) for order submission
        # This allows Alpaca to handle fractional shares automatically
        if dollar_amount <= 0:
            message = (
                f"Cannot open position for {ticker}: "
                f"non-positive dollar_amount {dollar_amount}"
            )
            raise ValueError(message)

        try:
            self.trading_client.submit_order(
                order_data=OrderRequest(
                    symbol=ticker.upper(),
                    notional=dollar_amount,
                    side=OrderSide(side.value.lower()),
                    type=OrderType.MARKET,
                    time_in_force=TimeInForce.DAY,
                ),
            )
        except APIError as e:
            error_str = str(e).lower()
            # Handle insufficient buying power
            if "insufficient buying power" in error_str or "buying_power" in error_str:
                message = f"Insufficient buying power for {ticker}: {e}"
                raise InsufficientBuyingPowerError(message) from e
            # Handle non-shortable assets
            if "cannot be sold short" in error_str or "not shortable" in error_str:
                message = f"Asset {ticker} cannot be sold short: {e}"
                raise AssetNotShortableError(message) from e
            # Re-raise other API errors
            raise

        time.sleep(self.rate_limit_sleep)

    def close_position(
        self,
        ticker: str,
    ) -> bool:
        """Close a position for the given ticker.

        Returns True if position was closed, False if position didn't exist.
        """
        try:
            self.trading_client.close_position(
                symbol_or_asset_id=ticker.upper(),
                close_options=ClosePositionRequest(
                    percentage="100",
                ),
            )
            time.sleep(self.rate_limit_sleep)
        except APIError as e:
            # Prefer structured information from the Alpaca API when available,
            # and fall back to matching documented error message fragments for
            # backwards compatibility.
            status_code = getattr(e, "status_code", None)
            error_code = getattr(e, "code", None)
            error_message = getattr(e, "message", None)
            error_str = (
                str(error_message) if error_message is not None else str(e)
            ).lower()

            # Known Alpaca behaviours when closing a non-existent position:
            # - HTTP 404 Not Found
            # - Specific error_code values (e.g. "position_not_found")
            # - Error messages containing "position not found"
            http_not_found = 404
            position_not_found = (
                status_code == http_not_found
                or error_code in {"position_not_found"}
                or "position not found" in error_str
                or "position does not exist" in error_str
            )
            if position_not_found:
                logger.info(
                    "Position already closed or does not exist",
                    ticker=ticker,
                )
                return False
            raise
        return True
