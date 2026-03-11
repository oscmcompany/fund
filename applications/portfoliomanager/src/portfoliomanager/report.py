import polars as pl

from .beta import compute_portfolio_beta
from .regime import RegimeResult
from .statistical_arbitrage import CONFIDENCE_THRESHOLD

_SEPARATOR = "-" * 62

# Column widths for the pairs table — sized to fit both headers and data.
_W_PAIR_ID = 24
_W_TICKER = 12
_W_Z_SCORE = 8
_W_SIGNAL = 10
_W_HEDGE = 8


def format_regime_report(regime: RegimeResult, exposure_scale: float) -> str:
    """Return a formatted string summarising the regime detection output."""
    lines = [
        _SEPARATOR,
        "  REGIME DETECTION",
        _SEPARATOR,
        f"  State:          {regime['state']}",
        f"  Confidence:     {regime['confidence']:.3f}",
        f"  Exposure scale: {exposure_scale}x",
    ]
    return "\n".join(lines)


def format_beta_report(market_betas: pl.DataFrame) -> str:
    """Return a formatted string summarising market beta across the universe."""
    lines = [_SEPARATOR, "  MARKET BETA", _SEPARATOR]

    if market_betas.is_empty():
        lines.append("  No betas computed (insufficient data)")
        return "\n".join(lines)

    beta_values = market_betas["market_beta"]
    lines += [
        f"  Tickers with betas:  {market_betas.height}",
        f"  Beta range:          [{beta_values.min():.3f}, {beta_values.max():.3f}]",
        f"  Mean beta:           {beta_values.mean():.3f}",
    ]

    top_five = market_betas.sort("market_beta", descending=True).head(5)
    bottom_five = market_betas.sort("market_beta").head(5)

    lines.append("  Highest betas:")
    for row in top_five.iter_rows(named=True):
        lines.append(f"    {row['ticker']:<8} {row['market_beta']:.3f}")
    lines.append("  Lowest betas:")
    for row in bottom_five.iter_rows(named=True):
        lines.append(f"    {row['ticker']:<8} {row['market_beta']:.3f}")

    return "\n".join(lines)


def format_consolidation_report(
    signals: pl.DataFrame,
    input_ticker_count: int,
) -> str:
    """Return a formatted string summarising the signal consolidation output."""
    alpha_values = signals["ensemble_alpha"]
    confidence_values = signals["ensemble_confidence"]
    high_confidence_count = signals.filter(
        pl.col("ensemble_confidence") >= CONFIDENCE_THRESHOLD
    ).height
    alpha_min, alpha_max = alpha_values.min(), alpha_values.max()
    conf_min, conf_max = confidence_values.min(), confidence_values.max()

    lines = [
        _SEPARATOR,
        "  SIGNAL CONSOLIDATION",
        _SEPARATOR,
        f"  Input tickers:            {input_ticker_count}",
        f"  Signals computed:         {signals.height}",
        f"  High confidence (>=0.5):  {high_confidence_count}",
        f"  Alpha range:              [{alpha_min:.4f}, {alpha_max:.4f}]",
        f"  Confidence range:         [{conf_min:.3f}, {conf_max:.3f}]",
    ]
    return "\n".join(lines)


def format_pairs_report(candidate_pairs: pl.DataFrame) -> str:
    """Return a formatted string listing the selected stat arb pairs."""
    lines = [_SEPARATOR, "  PAIR SELECTION", _SEPARATOR]

    if candidate_pairs.is_empty():
        lines.append("  No qualifying pairs found.")
        return "\n".join(lines)

    lines.append(f"  Pairs selected: {candidate_pairs.height}")
    lines.append("")

    header = (
        f"  {'pair_id':<{_W_PAIR_ID}} {'long_ticker':<{_W_TICKER}} "
        f"{'short_ticker':<{_W_TICKER}} {'z_score':>{_W_Z_SCORE}} "
        f"{'signal':>{_W_SIGNAL}} {'hedge':>{_W_HEDGE}}"
    )
    separator = (
        f"  {'-' * _W_PAIR_ID} {'-' * _W_TICKER} {'-' * _W_TICKER} "
        f"{'-' * _W_Z_SCORE} {'-' * _W_SIGNAL} {'-' * _W_HEDGE}"
    )
    lines += [header, separator]

    for row in candidate_pairs.iter_rows(named=True):
        lines.append(
            f"  {row['pair_id']:<{_W_PAIR_ID}} {row['long_ticker']:<{_W_TICKER}} "
            f"{row['short_ticker']:<{_W_TICKER}} {row['z_score']:>{_W_Z_SCORE}.3f} "
            f"{row['signal_strength']:>{_W_SIGNAL}.4f} "
            f"{row['hedge_ratio']:>{_W_HEDGE}.3f}"
        )

    return "\n".join(lines)


def format_portfolio_report(
    portfolio: pl.DataFrame,
    candidate_pairs: pl.DataFrame,
    market_betas: pl.DataFrame,
    capital: float,
    exposure_scale: float,
) -> str:
    """Return a formatted string summarising position sizes and portfolio-level risk."""
    long_total = portfolio.filter(pl.col("side") == "LONG")["dollar_amount"].sum()
    short_total = portfolio.filter(pl.col("side") == "SHORT")["dollar_amount"].sum()
    imbalance_pct = abs(long_total - short_total) / max(long_total, short_total) * 100
    portfolio_beta = compute_portfolio_beta(portfolio, market_betas)

    lines = [
        _SEPARATOR,
        "  POSITION SIZING",
        _SEPARATOR,
        f"  Capital:         ${capital:,.0f}",
        f"  Exposure scale:  {exposure_scale}x",
        f"  Long total:      ${long_total:,.2f}",
        f"  Short total:     ${short_total:,.2f}",
        f"  Imbalance:       {imbalance_pct:.2f}%",
        f"  Portfolio beta:  {portfolio_beta:.4f}",
        "",
    ]

    for pair_id in candidate_pairs["pair_id"].to_list():
        pair_rows = portfolio.filter(pl.col("pair_id") == pair_id)
        long_row = pair_rows.filter(pl.col("side") == "LONG").row(0, named=True)
        short_row = pair_rows.filter(pl.col("side") == "SHORT").row(0, named=True)
        lines.append(
            f"  {pair_id:<{_W_PAIR_ID + 2}} "
            f"LONG  {long_row['ticker']:<8} ${long_row['dollar_amount']:>9,.2f}  "
            f"SHORT {short_row['ticker']:<8} ${short_row['dollar_amount']:>9,.2f}"
        )

    return "\n".join(lines)
