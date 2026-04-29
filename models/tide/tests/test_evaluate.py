import polars as pl
import pytest
from tide.evaluate import (
    DriftResult,
    check_drift,
    compute_crps,
    compute_directional_accuracy,
    compute_quantile_coverage,
)

_FLOAT_TOLERANCE = 1e-6


def _make_predictions(
    quantile_10: list[float],
    quantile_50: list[float],
    quantile_90: list[float],
) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "quantile_10": quantile_10,
            "quantile_50": quantile_50,
            "quantile_90": quantile_90,
        }
    )


def _make_actuals(daily_return: list[float]) -> pl.DataFrame:
    return pl.DataFrame({"daily_return": daily_return})


def test_compute_crps_returns_float() -> None:
    predictions = _make_predictions(
        quantile_10=[-0.01, -0.02],
        quantile_50=[0.01, 0.02],
        quantile_90=[0.03, 0.04],
    )
    actuals = _make_actuals([0.015, 0.025])

    result = compute_crps(predictions, actuals)

    assert isinstance(result, float)


def test_compute_crps_perfect_predictions_returns_zero() -> None:
    # When quantile_50 equals actual and quantile_10/90 bracket it tightly,
    # the pinball loss approaches zero
    value = 0.05
    predictions = _make_predictions(
        quantile_10=[value],
        quantile_50=[value],
        quantile_90=[value],
    )
    actuals = _make_actuals([value])

    result = compute_crps(predictions, actuals)

    assert abs(result) < _FLOAT_TOLERANCE


def test_compute_crps_positive_for_imperfect_predictions() -> None:
    predictions = _make_predictions(
        quantile_10=[-0.10],
        quantile_50=[0.10],
        quantile_90=[0.20],
    )
    actuals = _make_actuals([-0.05])

    result = compute_crps(predictions, actuals)

    assert result > 0.0


def test_compute_directional_accuracy_all_matching_signs() -> None:
    predictions = _make_predictions(
        quantile_10=[0.01, 0.02, -0.03],
        quantile_50=[0.05, 0.06, -0.01],
        quantile_90=[0.10, 0.11, 0.01],
    )
    actuals = _make_actuals([0.03, 0.07, -0.02])

    result = compute_directional_accuracy(predictions, actuals)

    assert result == pytest.approx(1.0)


def test_compute_directional_accuracy_all_mismatched_signs() -> None:
    predictions = _make_predictions(
        quantile_10=[0.01, 0.02],
        quantile_50=[0.05, 0.06],
        quantile_90=[0.10, 0.11],
    )
    actuals = _make_actuals([-0.03, -0.07])

    result = compute_directional_accuracy(predictions, actuals)

    assert result == pytest.approx(0.0)


def test_compute_directional_accuracy_returns_float_between_zero_and_one() -> None:
    predictions = _make_predictions(
        quantile_10=[-0.01, 0.01],
        quantile_50=[-0.05, 0.05],
        quantile_90=[0.01, 0.10],
    )
    actuals = _make_actuals([-0.03, -0.02])

    result = compute_directional_accuracy(predictions, actuals)

    assert 0.0 <= result <= 1.0


def test_compute_quantile_coverage_all_within_bounds() -> None:
    predictions = _make_predictions(
        quantile_10=[-0.10, -0.10],
        quantile_50=[0.0, 0.0],
        quantile_90=[0.10, 0.10],
    )
    actuals = _make_actuals([0.05, -0.05])

    result = compute_quantile_coverage(predictions, actuals)

    assert result == pytest.approx(1.0)


def test_compute_quantile_coverage_none_within_bounds() -> None:
    predictions = _make_predictions(
        quantile_10=[0.01, 0.01],
        quantile_50=[0.05, 0.05],
        quantile_90=[0.10, 0.10],
    )
    actuals = _make_actuals([-0.05, -0.10])

    result = compute_quantile_coverage(predictions, actuals)

    assert result == pytest.approx(0.0)


def test_compute_quantile_coverage_returns_float_between_zero_and_one() -> None:
    predictions = _make_predictions(
        quantile_10=[-0.05, 0.01],
        quantile_50=[0.0, 0.05],
        quantile_90=[0.05, 0.10],
    )
    actuals = _make_actuals([0.03, -0.02])

    result = compute_quantile_coverage(predictions, actuals)

    assert 0.0 <= result <= 1.0


def test_check_drift_returns_insufficient_history_when_below_minimum_runs() -> None:
    current_metrics = {"crps": 0.05}
    prior_evaluations = [{"crps": 0.04}, {"crps": 0.045}]

    result = check_drift(
        current_metrics=current_metrics,
        prior_evaluations=prior_evaluations,
        minimum_runs=3,
    )

    assert result.status == "insufficient_history"
    assert result.current_crps == pytest.approx(0.05)
    assert result.baseline_crps is None


def test_check_drift_returns_insufficient_history_when_no_prior_evaluations() -> None:
    current_metrics = {"crps": 0.05}

    result = check_drift(
        current_metrics=current_metrics,
        prior_evaluations=[],
        minimum_runs=3,
    )

    assert result.status == "insufficient_history"
    assert isinstance(result, DriftResult)


def test_check_drift_returns_drift_detected_when_crps_exceeds_threshold() -> None:
    baseline = 0.04
    current_metrics = {"crps": baseline * 1.25}  # 25% degradation, above 20% threshold
    prior_evaluations = [{"crps": baseline}, {"crps": baseline}, {"crps": baseline}]

    result = check_drift(
        current_metrics=current_metrics,
        prior_evaluations=prior_evaluations,
        minimum_runs=3,
        degradation_threshold=0.20,
    )

    assert result.status == "drift_detected"
    assert result.current_crps == pytest.approx(baseline * 1.25)
    assert result.baseline_crps == pytest.approx(baseline)


def test_check_drift_returns_no_drift_when_crps_within_threshold() -> None:
    baseline = 0.04
    current_metrics = {"crps": baseline * 1.10}  # 10% degradation, below 20% threshold
    prior_evaluations = [{"crps": baseline}, {"crps": baseline}, {"crps": baseline}]

    result = check_drift(
        current_metrics=current_metrics,
        prior_evaluations=prior_evaluations,
        minimum_runs=3,
        degradation_threshold=0.20,
    )

    assert result.status == "no_drift"
    assert result.baseline_crps == pytest.approx(baseline)


def test_check_drift_baseline_crps_is_mean_of_prior_evaluations() -> None:
    prior_evaluations = [{"crps": 0.02}, {"crps": 0.04}, {"crps": 0.06}]
    current_metrics = {"crps": 0.04}

    result = check_drift(
        current_metrics=current_metrics,
        prior_evaluations=prior_evaluations,
        minimum_runs=3,
    )

    assert result.baseline_crps == pytest.approx(0.04)
