import pulumi_aws as aws
from compute import alb, data_manager_tg, ensemble_manager_tg, portfolio_manager_tg
from config import tags
from notifications import infrastructure_alerts_topic
from redeployment import redeployment_lambda

aws.cloudwatch.MetricAlarm(
    "alb_5xx_alarm",
    name="fund-alb-target-5xx",
    namespace="AWS/ApplicationELB",
    metric_name="HTTPCode_Target_5XX_Count",
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
        treat_missing_data="notBreaching",
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
