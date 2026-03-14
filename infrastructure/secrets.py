import pulumi_aws as aws
from config import (
    data_manager_secret_name,
    data_manager_secret_values,
    portfolio_manager_secret_name,
    portfolio_manager_secret_values,
    serialize_secret_config_object,
    shared_secret_name,
    shared_secret_values,
    tags,
)

data_manager_secret = aws.secretsmanager.Secret(
    "data_manager_secret",
    name=data_manager_secret_name,
    recovery_window_in_days=0,
    tags=tags,
)

portfolio_manager_secret = aws.secretsmanager.Secret(
    "portfolio_manager_secret",
    name=portfolio_manager_secret_name,
    recovery_window_in_days=0,
    tags=tags,
)

shared_secret = aws.secretsmanager.Secret(
    "shared_secret",
    name=shared_secret_name,
    recovery_window_in_days=0,
    tags=tags,
)

aws.secretsmanager.SecretVersion(
    "data_manager_secret_version",
    secret_id=data_manager_secret.id,
    secret_string=data_manager_secret_values.apply(
        lambda values: serialize_secret_config_object(
            values,
            "datamanagerSecretValue",
            {"MASSIVE_API_KEY"},
        )
    ),
)

aws.secretsmanager.SecretVersion(
    "portfolio_manager_secret_version",
    secret_id=portfolio_manager_secret.id,
    secret_string=portfolio_manager_secret_values.apply(
        lambda values: serialize_secret_config_object(
            values,
            "portfoliomanagerSecretValue",
            {"ALPACA_API_KEY_ID", "ALPACA_API_SECRET", "ALPACA_IS_PAPER"},
        )
    ),
)

aws.secretsmanager.SecretVersion(
    "shared_secret_version",
    secret_id=shared_secret.id,
    secret_string=shared_secret_values.apply(
        lambda values: serialize_secret_config_object(
            values,
            "sharedSecretValue",
            {"SENTRY_DSN"},
        )
    ),
)
