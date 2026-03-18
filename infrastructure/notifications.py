import pulumi_aws as aws
from config import (
    account_id,
    budget_alert_email_addresses,
    monthly_budget_limit_usd,
    tags,
)

infrastructure_alerts_topic = aws.sns.Topic(
    "infrastructure_alerts_topic",
    name="fund-infrastructure-alerts",
    tags=tags,
)

for notification_email_index, notification_email_address in enumerate(
    budget_alert_email_addresses,
    start=1,
):
    aws.sns.TopicSubscription(
        f"infrastructure_alert_email_subscription_{notification_email_index}",
        topic=infrastructure_alerts_topic.arn,
        protocol="email",
        endpoint=notification_email_address,
    )

# This can be updated by setting the monthlyBudgetLimitUsd Pulumi configuration
# variable.
aws.budgets.Budget(
    "production_cost_budget",
    account_id=account_id,
    name="fund-monthly-cost",
    budget_type="COST",
    time_unit="MONTHLY",
    limit_amount=f"{monthly_budget_limit_usd:.2f}",
    limit_unit="USD",
    notifications=[
        aws.budgets.BudgetNotificationArgs(
            comparison_operator="GREATER_THAN",
            notification_type="ACTUAL",
            threshold=monthly_budget_limit_usd,
            threshold_type="ABSOLUTE_VALUE",
            subscriber_email_addresses=budget_alert_email_addresses,
        ),
        aws.budgets.BudgetNotificationArgs(
            comparison_operator="GREATER_THAN",
            notification_type="FORECASTED",
            threshold=monthly_budget_limit_usd,
            threshold_type="ABSOLUTE_VALUE",
            subscriber_email_addresses=budget_alert_email_addresses,
        ),
    ],
)
