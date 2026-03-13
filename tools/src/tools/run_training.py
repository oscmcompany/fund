"""Lightweight CLI for running TiDE training locally.

Usage:
    uv run --package tools python -m tools.run_training \
        --data-path results/equitypricemodel/training_data.parquet \
        --config '{"learning_rate": 0.003, "hidden_size": 64}'

Outputs a single JSON line to stdout with the training result.
All log output goes to stderr so stdout is clean for parsing.
"""

import argparse
import json
import sys
import tempfile

import polars as pl
import structlog

structlog.configure(
    processors=[
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.JSONRenderer(),
    ],
    wrapper_class=structlog.BoundLogger,
    context_class=dict,
    logger_factory=structlog.PrintLoggerFactory(file=sys.stderr),
    cache_logger_on_first_use=True,
)

logger = structlog.get_logger()


def main() -> None:
    parser = argparse.ArgumentParser(description="Run TiDE model training")
    parser.add_argument(
        "--data-path",
        required=True,
        help="Path to training data parquet file",
    )
    parser.add_argument(
        "--config",
        default="{}",
        help="JSON string of configuration overrides",
    )
    args = parser.parse_args()

    config_overrides = json.loads(args.config)

    logger.info("Loading training data", path=args.data_path)
    training_data = pl.read_parquet(args.data_path)
    logger.info("Training data loaded", rows=training_data.height)

    from equitypricemodel.trainer import DEFAULT_CONFIGURATION, train_model

    merged_config = dict(DEFAULT_CONFIGURATION)
    merged_config.update(config_overrides)

    logger.info("Starting training", config=merged_config)

    with tempfile.TemporaryDirectory(prefix="train_") as checkpoint_dir:
        _model, _data, losses = train_model(
            training_data,
            configuration=merged_config,
            checkpoint_directory=checkpoint_dir,
        )

    final_loss = losses[-1] if losses else None

    result = {
        "config": merged_config,
        "quantile_loss": final_loss,
        "all_losses": losses,
        "status": "OK",
    }

    print(json.dumps(result))


if __name__ == "__main__":
    main()
