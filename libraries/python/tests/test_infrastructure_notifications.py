from pathlib import Path

REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
INFRASTRUCTURE_NOTIFICATIONS_PATH = (
    REPOSITORY_ROOT / "infrastructure" / "notifications.py"
)


def load_infrastructure_notifications() -> str:
    return INFRASTRUCTURE_NOTIFICATIONS_PATH.read_text(encoding="utf-8")


def test_notifications_contains_cost_anomaly_monitor_resource() -> None:
    infrastructure_notifications = load_infrastructure_notifications()

    assert '"cost_anomaly_monitor"' in infrastructure_notifications


def test_notifications_contains_cost_anomaly_subscription_resource() -> None:
    infrastructure_notifications = load_infrastructure_notifications()

    assert '"cost_anomaly_subscription"' in infrastructure_notifications


def test_notifications_anomaly_subscription_uses_plural_dimensions_key() -> None:
    infrastructure_notifications = load_infrastructure_notifications()

    assert '"Dimensions"' in infrastructure_notifications


def test_notifications_contains_budget_resource() -> None:
    infrastructure_notifications = load_infrastructure_notifications()

    assert '"production_cost_budget"' in infrastructure_notifications
