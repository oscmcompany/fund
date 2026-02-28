from typing import cast
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pendulum
from prefect.client.schemas.objects import Flow, FlowRun, State, StateType
from pydantic_extra_types.pendulum_dt import DateTime
from tools.flows.notifications import send_training_notification


def _make_flow() -> Flow:
    return Flow(id=uuid4(), name="tide-training-pipeline")


def _make_flow_run(
    state_type: StateType,
    start_time: DateTime | None = None,
    end_time: DateTime | None = None,
) -> FlowRun:
    return FlowRun(
        id=uuid4(),
        flow_id=uuid4(),
        name="test-run-1",
        state_type=state_type,
        start_time=start_time,
        end_time=end_time,
    )


def test_send_training_notification_on_completion() -> None:
    flow = _make_flow()
    flow_run = _make_flow_run(
        StateType.COMPLETED,
        start_time=cast("DateTime", pendulum.datetime(2024, 1, 1, 12, 0, 0, tz="UTC")),
        end_time=cast("DateTime", pendulum.datetime(2024, 1, 1, 12, 30, 0, tz="UTC")),
    )
    state = State(type=StateType.COMPLETED, name="Completed")

    mock_ses = MagicMock()

    with (
        patch.dict(
            "os.environ",
            {
                "TRAINING_NOTIFICATION_SENDER_EMAIL": "sender@example.com",
                "TRAINING_NOTIFICATION_RECIPIENT_EMAILS": "recipient@example.com",
            },
        ),
        patch("tools.flows.notifications.boto3") as mock_boto3,
    ):
        mock_boto3.client.return_value = mock_ses
        send_training_notification(flow, flow_run, state)

    mock_boto3.client.assert_called_once_with("ses")
    mock_ses.send_email.assert_called_once()

    call_kwargs = mock_ses.send_email.call_args[1]
    assert call_kwargs["Source"] == "sender@example.com"
    assert call_kwargs["Destination"]["ToAddresses"] == ["recipient@example.com"]
    assert "completed" in call_kwargs["Message"]["Subject"]["Data"]
    assert "1800 seconds" in call_kwargs["Message"]["Body"]["Text"]["Data"]


def test_send_training_notification_on_failure() -> None:
    flow = _make_flow()
    flow_run = _make_flow_run(StateType.FAILED)
    state = State(
        type=StateType.FAILED,
        name="Failed",
        message="Something went wrong",
    )

    mock_ses = MagicMock()

    with (
        patch.dict(
            "os.environ",
            {
                "TRAINING_NOTIFICATION_SENDER_EMAIL": "sender@example.com",
                "TRAINING_NOTIFICATION_RECIPIENT_EMAILS": "a@example.com,b@example.com",
            },
        ),
        patch("tools.flows.notifications.boto3") as mock_boto3,
    ):
        mock_boto3.client.return_value = mock_ses
        send_training_notification(flow, flow_run, state)

    call_kwargs = mock_ses.send_email.call_args[1]
    assert call_kwargs["Destination"]["ToAddresses"] == [
        "a@example.com",
        "b@example.com",
    ]
    assert "FAILED" in call_kwargs["Message"]["Subject"]["Data"]
    assert "Something went wrong" in call_kwargs["Message"]["Body"]["Text"]["Data"]


def test_send_training_notification_skips_when_not_configured() -> None:
    flow = _make_flow()
    flow_run = _make_flow_run(StateType.COMPLETED)
    state = State(type=StateType.COMPLETED, name="Completed")

    with (
        patch.dict(
            "os.environ",
            {
                "TRAINING_NOTIFICATION_SENDER_EMAIL": "",
                "TRAINING_NOTIFICATION_RECIPIENT_EMAILS": "",
            },
        ),
        patch("tools.flows.notifications.boto3") as mock_boto3,
    ):
        send_training_notification(flow, flow_run, state)

    mock_boto3.client.assert_not_called()


def test_send_training_notification_handles_ses_error() -> None:
    flow = _make_flow()
    flow_run = _make_flow_run(StateType.COMPLETED)
    state = State(type=StateType.COMPLETED, name="Completed")

    mock_ses = MagicMock()
    mock_ses.send_email.side_effect = Exception("SES error")

    with (
        patch.dict(
            "os.environ",
            {
                "TRAINING_NOTIFICATION_SENDER_EMAIL": "sender@example.com",
                "TRAINING_NOTIFICATION_RECIPIENT_EMAILS": "recipient@example.com",
            },
        ),
        patch("tools.flows.notifications.boto3") as mock_boto3,
    ):
        mock_boto3.client.return_value = mock_ses
        send_training_notification(flow, flow_run, state)
