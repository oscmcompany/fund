from unittest.mock import MagicMock, patch

# Prevent Alpaca SDK credential validation from crashing pytest collection.
# server.py instantiates AlpacaClient at module level; TradingClient and
# StockHistoricalDataClient reject empty credentials, which fails in CI.
# Patching the underlying SDK classes here allows AlpacaClient to be
# instantiated without real credentials while keeping the AlpacaClient
# class itself real for test_alpaca_client.py.
_tc_patcher = patch("portfoliomanager.alpaca_client.TradingClient", MagicMock)
_tc_patcher.start()

_dc_patcher = patch(
    "portfoliomanager.alpaca_client.StockHistoricalDataClient", MagicMock
)
_dc_patcher.start()
