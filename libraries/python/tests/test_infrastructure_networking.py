from pathlib import Path

REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
INFRASTRUCTURE_NETWORKING_PATH = REPOSITORY_ROOT / "infrastructure" / "networking.py"


def load_infrastructure_networking() -> str:
    return INFRASTRUCTURE_NETWORKING_PATH.read_text(encoding="utf-8")


def test_networking_contains_nat_gateway_baseline_alarm() -> None:
    infrastructure_networking = load_infrastructure_networking()

    assert '"nat_gateway_bytes_out_to_destination_alarm"' in infrastructure_networking
    assert 'metric_name="BytesOutToDestination"' in infrastructure_networking
    assert "threshold=500_000_000" in infrastructure_networking
    assert "period=3600" in infrastructure_networking
    assert "evaluation_periods=2" in infrastructure_networking
