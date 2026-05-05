import pulumi_aws as aws
from compute import (
    alb,
    data_manager_log_group,
    data_manager_tg,
    ensemble_manager_tg,
    portfolio_manager_tg,
)
from config import tags
from notifications import infrastructure_alerts_topic
from redeployment import redeployment_lambda

aws.cloudwatch.MetricAlarm(
    "alb_5xx_alarm",
    name="fund-alb-5xx",
    namespace="AWS/ApplicationELB",
    metric_name="HTTPCode_ELB_5XX_Count",
    dimensions={"LoadBalancer": alb.arn_suffix},
    statistic="Sum",
    period=300,
    evaluation_periods=1,
    threshold=5,
    comparison_operator="GreaterThanOrEqualToThreshold",
    treat_missing_data="notBreaching",
    alarm_actions=[infrastructure_alerts_topic.arn],
    ok_actions=[infrastructure_alerts_topic.arn],
    tags=tags,
)

_unhealthy_host_targets = {
    "data_manager": data_manager_tg,
    "portfolio_manager": portfolio_manager_tg,
    "ensemble_manager": ensemble_manager_tg,
}

for service_name, target_group in _unhealthy_host_targets.items():
    aws.cloudwatch.MetricAlarm(
        f"{service_name}_unhealthy_hosts_alarm",
        name=f"fund-{service_name.replace('_', '-')}-unhealthy-hosts",
        namespace="AWS/ApplicationELB",
        metric_name="UnHealthyHostCount",
        dimensions={
            "TargetGroup": target_group.arn_suffix,
            "LoadBalancer": alb.arn_suffix,
        },
        statistic="Maximum",
        period=300,
        evaluation_periods=2,
        threshold=1,
        comparison_operator="GreaterThanOrEqualToThreshold",
        treat_missing_data="breaching",
        alarm_actions=[infrastructure_alerts_topic.arn],
        ok_actions=[infrastructure_alerts_topic.arn],
        tags=tags,
    )

data_sync_metric_filter = aws.cloudwatch.LogMetricFilter(
    "data_sync_success_metric_filter",
    name="fund-data-sync-success",
    log_group_name=data_manager_log_group.name,
    pattern='"Successfully uploaded DataFrame to S3"',
    metric_transformation=aws.cloudwatch.LogMetricFilterMetricTransformationArgs(
        name="DataSyncSuccess",
        namespace="Fund/DataManager",
        value="1",
        default_value="0",
    ),
)

aws.cloudwatch.MetricAlarm(
    "data_sync_staleness_alarm",
    name="fund-data-sync-stale",
    namespace="Fund/DataManager",
    metric_name="DataSyncSuccess",
    statistic="Sum",
    period=86400,
    evaluation_periods=1,
    threshold=1,
    comparison_operator="LessThanThreshold",
    treat_missing_data="breaching",
    alarm_actions=[infrastructure_alerts_topic.arn],
    ok_actions=[infrastructure_alerts_topic.arn],
    tags=tags,
)

aws.cloudwatch.MetricAlarm(
    "redeployment_lambda_errors_alarm",
    name="fund-redeployment-lambda-errors",
    namespace="AWS/Lambda",
    metric_name="Errors",
    dimensions={"FunctionName": redeployment_lambda.name},
    statistic="Sum",
    period=300,
    evaluation_periods=1,
    threshold=1,
    comparison_operator="GreaterThanOrEqualToThreshold",
    treat_missing_data="notBreaching",
    alarm_actions=[infrastructure_alerts_topic.arn],
    ok_actions=[infrastructure_alerts_topic.arn],
    tags=tags,
)
