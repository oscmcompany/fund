import os
from datetime import UTC, datetime

import boto3
import structlog
from prefect.client.schemas.objects import Flow, FlowRun, State

logger = structlog.get_logger()


def send_training_notification(flow: Flow, flow_run: FlowRun, state: State) -> None:
    """Send email notification via SES on training pipeline completion or failure."""
    sender_email = os.getenv("TRAINING_NOTIFICATION_SENDER_EMAIL", "")
    recipient_emails_raw = os.getenv("TRAINING_NOTIFICATION_RECIPIENT_EMAILS", "")

    if not sender_email or not recipient_emails_raw:
        logger.warning(
            "Notification emails not configured, skipping",
            sender_email=sender_email,
            recipient_emails=recipient_emails_raw,
        )
        return

    recipient_emails = [
        email.strip() for email in recipient_emails_raw.split(",") if email.strip()
    ]

    state_name = state.name or "Unknown"
    is_failure = state.is_failed()

    duration_seconds = None
    if flow_run.start_time and flow_run.end_time:
        duration_seconds = (flow_run.end_time - flow_run.start_time).total_seconds()

    duration_text = f"{duration_seconds:.0f} seconds" if duration_seconds else "unknown"

    subject = (
        f"Training pipeline {'FAILED' if is_failure else 'completed'}: "
        f"{flow.name}/{flow_run.name}"
    )

    body_parts = [
        f"Flow: {flow.name}",
        f"Run: {flow_run.name}",
        f"State: {state_name}",
        f"Duration: {duration_text}",
        f"Timestamp: {datetime.now(tz=UTC).isoformat()}",
    ]

    if is_failure and state.message:
        body_parts.append(f"\nError: {state.message}")

    body = "\n".join(body_parts)

    try:
        ses_client = boto3.client("ses")
        ses_client.send_email(
            Source=sender_email,
            Destination={"ToAddresses": recipient_emails},
            Message={
                "Subject": {"Data": subject, "Charset": "UTF-8"},
                "Body": {"Text": {"Data": body, "Charset": "UTF-8"}},
            },
        )
        logger.info(
            "Training notification sent",
            recipients=recipient_emails,
            state=state_name,
        )
    except Exception:
        logger.exception("Failed to send training notification")
