import time
from typing import TYPE_CHECKING, cast

import structlog
from alpaca.common.exceptions import APIError
from alpaca.data import StockHistoricalDataClient
from alpaca.trading import (
    Asset,
    Clock,
    ClosePositionRequest,
    GetAssetsRequest,
    OrderRequest,
    TradeAccount,
    TradingClient,
)
from alpaca.trading.enums import (
    AssetClass,
    AssetStatus,
    OrderSide,
    OrderType,
    PositionIntent,
    TimeInForce,
)
from tenacity import (
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

if TYPE_CHECKING:
    from tenacity import RetryCallState

from .enums import TradeSide
from .exceptions import AssetNotShortableError, InsufficientBuyingPowerError

logger = structlog.get_logger(__name__)

_TRANSIENT_STATUS_CODES = {429, 500, 502, 503, 504}


def _is_transient_error(error: BaseException) -> bool:
    status_code = getattr(error, "status_code", None)
    if status_code in _TRANSIENT_STATUS_CODES:
        return True
    return isinstance(error, OSError)


def _log_retry(retry_state: "RetryCallState") -> None:
    outcome = retry_state.outcome
    error = outcome.exception() if outcome is not None else None
    logger.warning(
        "Retrying Alpaca API call",
        attempt=retry_state.attempt_number,
        wait_seconds=getattr(retry_state.next_action, "sleep", None),
        error=str(error),
    )


_alpaca_retry = retry(
    retry=retry_if_exception(_is_transient_error),
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=10),
    before_sleep=_log_retry,
    reraise=True,
)


class AlpacaAccount:
    def __init__(
        self,
        cash_amount: float,
        buying_power: float,
        equity: float,
    ) -> None:
        self.cash_amount = cash_amount
        self.buying_power = buying_power
        self.equity = equity


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

    @_alpaca_retry
    def is_market_open(self) -> bool:
        clock: Clock = cast("Clock", self.trading_client.get_clock())
        time.sleep(self.rate_limit_sleep)
        return bool(clock.is_open)

    @_alpaca_retry
    def get_account(self) -> AlpacaAccount:
        account: TradeAccount = cast("TradeAccount", self.trading_client.get_account())

        time.sleep(self.rate_limit_sleep)

        return AlpacaAccount(
            cash_amount=float(cast("str", account.cash)),
            buying_power=float(cast("str", account.buying_power)),
            equity=float(cast("str", account.equity)),
        )

    def _confirm_order_status(
        self,
        ticker: str,
        alpaca_order_id: str,
        order: object,
    ) -> None:
        """Check submitted order status and raise if rejected or stuck pending_new."""
        order_status = str(getattr(order, "status", ""))

        if order_status == "rejected":
            logger.error(
                "Order rejected by Alpaca",
                ticker=ticker,
                alpaca_order_id=alpaca_order_id,
            )
            message = (
                f"Order for {ticker} was rejected by Alpaca"
                f" (order_id={alpaca_order_id})"
            )
            raise RuntimeError(message)

        if order_status == "pending_new":
            time.sleep(1)
            try:
                polled_order = self.trading_client.get_order_by_id(alpaca_order_id)
                order_status = str(getattr(polled_order, "status", ""))
            except Exception:
                logger.exception(
                    "Failed to poll order status after pending_new",
                    ticker=ticker,
                    alpaca_order_id=alpaca_order_id,
                )
            if order_status == "pending_new":
                logger.error(
                    "Order remains pending_new after poll",
                    ticker=ticker,
                    alpaca_order_id=alpaca_order_id,
                )
                message = (
                    f"Order for {ticker} remains pending_new after poll"
                    f" (order_id={alpaca_order_id})"
                )
                raise RuntimeError(message)

    def open_position(
        self,
        ticker: str,
        side: TradeSide,
        dollar_amount: float,
        entry_price: float,
        quantity: int | None = None,
    ) -> str:
        if dollar_amount <= 0:
            message = (
                f"Cannot open position for {ticker}: "
                f"non-positive dollar_amount {dollar_amount}"
            )
            raise ValueError(message)

        if entry_price <= 0:
            message = (
                f"Cannot open position for {ticker}: "
                f"entry_price must be positive, got {entry_price}"
            )
            raise ValueError(message)

        if side == TradeSide.SELL:
            # Alpaca does not support fractional short sells; whole shares only.
            # Use the pre-computed quantity when available to avoid recomputation.
            qty = quantity if quantity is not None else int(dollar_amount / entry_price)
            if qty == 0:
                message = (
                    f"Cannot short {ticker}: dollar_amount {dollar_amount} "
                    f"is less than one share at entry_price {entry_price}"
                )
                raise ValueError(message)
            order_request = OrderRequest(
                symbol=ticker.upper(),
                qty=qty,
                side=OrderSide.SELL,
                type=OrderType.MARKET,
                time_in_force=TimeInForce.DAY,
                position_intent=PositionIntent.SELL_TO_OPEN,
            )
        else:
            # Long buys use notional so Alpaca handles fractional shares automatically.
            order_request = OrderRequest(
                symbol=ticker.upper(),
                notional=round(dollar_amount, 2),
                side=OrderSide.BUY,
                type=OrderType.MARKET,
                time_in_force=TimeInForce.DAY,
                position_intent=PositionIntent.BUY_TO_OPEN,
            )

        try:
            order = self.trading_client.submit_order(order_data=order_request)
            alpaca_order_id = str(getattr(order, "id", ""))
            self._confirm_order_status(ticker, alpaca_order_id, order)
        except APIError as e:
            error_str = str(e).lower()
            if "insufficient buying power" in error_str or "buying_power" in error_str:
                message = f"Insufficient buying power for {ticker}: {e}"
                raise InsufficientBuyingPowerError(message) from e
            if (
                "cannot be sold short" in error_str
                or "not shortable" in error_str
                or "not allowed to short" in error_str
            ):
                message = f"Asset {ticker} cannot be sold short: {e}"
                raise AssetNotShortableError(message) from e
            raise

        time.sleep(self.rate_limit_sleep)
        return alpaca_order_id

    @_alpaca_retry
    def get_shortable_tickers(self, tickers: list[str]) -> set[str]:
        all_assets: list[Asset] = cast(
            "list[Asset]",
            self.trading_client.get_all_assets(
                GetAssetsRequest(
                    asset_class=AssetClass.US_EQUITY,
                    status=AssetStatus.ACTIVE,
                )
            ),
        )
        time.sleep(self.rate_limit_sleep)
        ticker_set = set(tickers)
        return {
            str(asset.symbol)
            for asset in all_assets
            if asset.symbol in ticker_set and asset.shortable and asset.easy_to_borrow
        }

    def get_open_positions(self) -> list[dict[str, object]]:
        positions = cast(
            "list[object]",
            self.trading_client.get_all_positions(),
        )
        time.sleep(self.rate_limit_sleep)
        return [
            {
                "ticker": str(getattr(position, "symbol", "")),
                "side": str(getattr(position, "side", "")),
                "quantity": float(str(getattr(position, "qty", "0"))),
                "market_value": float(str(getattr(position, "market_value", "0"))),
                "unrealized_profit_and_loss": float(
                    str(getattr(position, "unrealized_pl", "0"))
                ),
            }
            for position in positions
        ]

    @_alpaca_retry
    def close_position(
        self,
        ticker: str,
    ) -> bool:
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
                or error_code == "position_not_found"
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
