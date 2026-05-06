"""Artifact watcher that polls S3 for new model artifacts.

When a new artifact is detected under the configured prefix, the watcher
sends SIGTERM to the ensemble-manager process so the devenv process manager
restarts it with the fresh artifact.
"""

import os
import signal
import subprocess
import time
from pathlib import Path

import boto3
import botocore.exceptions
import structlog

logger = structlog.get_logger()

POLL_INTERVAL_SECONDS = 60
STATE_FILE = Path("/tmp/artifact-watcher-last-key")  # noqa: S108


def get_latest_artifact_key(
    bucket: str,
    prefix: str,
) -> str | None:
    """Find the latest model artifact key under a prefix."""
    s3_client = boto3.client("s3")
    paginator = s3_client.get_paginator("list_objects_v2")
    folders: list[str] = []

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter="/"):
        folders.extend(
            common_prefix["Prefix"] for common_prefix in page.get("CommonPrefixes", [])
        )

    if not folders:
        return None

    folders.sort(reverse=True)

    for folder in folders:
        artifact_key = f"{folder}output/model.tar.gz"
        try:
            s3_client.head_object(Bucket=bucket, Key=artifact_key)
        except botocore.exceptions.ClientError:
            logger.debug("Artifact not found", key=artifact_key)
            continue
        else:
            return artifact_key

    return None


def read_last_key() -> str | None:
    """Read the last known artifact key from the state file."""
    if STATE_FILE.exists():
        content = STATE_FILE.read_text().strip()
        return content or None
    return None


def write_last_key(key: str) -> None:
    """Write the current artifact key to the state file."""
    STATE_FILE.write_text(key)


def restart_ensemble_manager() -> None:
    """Send SIGTERM to ensemble-manager process listening on port 8082."""
    try:
        result = subprocess.run(
            ["/usr/bin/lsof", "-ti", "tcp:8082"],
            capture_output=True,
            text=True,
            check=False,
        )
        pids = result.stdout.strip()
        if pids:
            for pid in pids.splitlines():
                os.kill(int(pid), signal.SIGTERM)
                logger.info("Sent SIGTERM to ensemble-manager", pid=int(pid))
        else:
            logger.info("No ensemble-manager process found on port 8082")
    except Exception:
        logger.exception("Failed to restart ensemble-manager")


def run() -> None:
    """Main polling loop."""
    bucket = os.environ.get("AWS_S3_MODEL_ARTIFACTS_BUCKET_NAME", "")
    prefix = os.environ.get("AWS_S3_MODEL_ARTIFACT_PATH", "artifacts/tide/")

    if not bucket:
        logger.error("AWS_S3_MODEL_ARTIFACTS_BUCKET_NAME not set, exiting")
        return

    logger.info(
        "Artifact watcher started",
        bucket=bucket,
        prefix=prefix,
        poll_interval=POLL_INTERVAL_SECONDS,
    )

    last_key = read_last_key()
    if last_key:
        logger.info("Resuming with last known artifact", last_key=last_key)

    while True:
        try:
            current_key = get_latest_artifact_key(bucket=bucket, prefix=prefix)

            if current_key is None:
                logger.debug("No artifacts found yet")
            elif last_key is None:
                logger.info("Initial artifact detected", artifact_key=current_key)
                write_last_key(current_key)
                last_key = current_key
            elif current_key != last_key:
                logger.info(
                    "New artifact detected",
                    previous_key=last_key,
                    new_key=current_key,
                )
                restart_ensemble_manager()
                write_last_key(current_key)
                last_key = current_key
        except Exception:
            logger.exception("Error during artifact poll")

        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    run()
