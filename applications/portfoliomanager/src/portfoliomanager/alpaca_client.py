import time
from typing import cast

from alpaca.common.exceptions import APIError
from alpaca.data import StockHistoricalDataClient
from alpaca.data.requests import StockLatestQuoteRequest
from alpaca.trading import (
    ClosePositionRequest,
    OrderRequest,
    TradeAccount,
    TradingClient,
)
from alpaca.trading.enums import OrderSide, OrderType, TimeInForce

from .enums import TradeSide
from .exceptions import AssetNotShortableError, InsufficientBuyingPowerError


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
        price_tolerance_percent: float = 1.0,
    ) -> None:
        self.rate_limit_sleep = 0.5  # seconds
        self.price_tolerance_percent = price_tolerance_percent

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

    def _get_current_price(self, ticker: str, side: TradeSide) -> float:
        """Get current price for a ticker based on trade side.

        For BUY orders, returns ask price (what sellers are asking).
        For SELL orders, returns bid price (what buyers are bidding).
        """
        request = StockLatestQuoteRequest(symbol_or_symbols=ticker.upper())
        quotes = self.data_client.get_stock_latest_quote(request)
        quote = quotes[ticker.upper()]

        if side == TradeSide.BUY:
            # Use ask price for buys (what we'd pay)
            return (
                float(quote.ask_price)
                if quote.ask_price > 0
                else float(quote.bid_price)
            )
        else:
            # Use bid price for sells (what we'd receive)
            return (
                float(quote.bid_price)
                if quote.bid_price > 0
                else float(quote.ask_price)
            )

    def open_position(
        self,
        ticker: str,
        side: TradeSide,
        dollar_amount: float,
    ) -> None:
        # Calculate quantity from dollar amount and current price
        # Allow fractional shares where supported by the brokerage
        current_price = self._get_current_price(ticker, side)
        qty = dollar_amount / current_price

        if qty <= 0:
            message = (
                f"Cannot open position for {ticker}: "
                f"non-positive quantity calculated from dollar_amount {dollar_amount} "
                f"and price {current_price}"
            )
            raise ValueError(message)

        # Calculate limit price with tolerance to prevent execution at unfavorable prices
        # For buys, limit is max price we'll pay (ask + tolerance allows some slippage up)
        # For sells, limit is min price we'll accept (bid - tolerance allows some slippage down)
        tolerance = current_price * (self.price_tolerance_percent / 100.0)
        if side == TradeSide.BUY:
            limit_price = current_price + tolerance
        else:
            limit_price = current_price - tolerance

        try:
            self.trading_client.submit_order(
                order_data=OrderRequest(
                    symbol=ticker.upper(),
                    qty=qty,
                    side=OrderSide(side.value.lower()),
                    type=OrderType.LIMIT,
                    time_in_force=TimeInForce.DAY,
                    limit_price=limit_price,
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
    ) -> None:
        self.trading_client.close_position(
            symbol_or_asset_id=ticker.upper(),
            close_options=ClosePositionRequest(
                percentage="100",
            ),
        )

        time.sleep(self.rate_limit_sleep)
