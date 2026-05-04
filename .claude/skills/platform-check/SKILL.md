---
name: platform-check
description: >
  Check fund platform health across all services. Use when the user asks
  "is everything running", "check the platform", "platform status",
  "are services healthy", "check health", "is the fund ok", or
  wants to verify end-to-end system state.
---

# Platform Health Check

Run a comprehensive health check of all fund platform components using the
OSCM AWS account (`--profile oscm --region us-east-1`). All tools (`aws`,
`curl`, `jq`, `prefect`) are available in the devenv shell.

## Check sequence

Run checks 1 through 7 in parallel where possible. Checks 1, 3, 4, 5, 6, and 7
are fully independent. Check 2 requires two sequential steps (get ARNs, then
check health) but can run in parallel with the other checks. Collect all results
before reporting.

### 1. ECS services

```bash
aws ecs describe-services --profile oscm --region us-east-1 \
  --cluster fund-applications \
  --services fund-data-manager-server fund-portfolio-manager-server fund-ensemble-manager-server \
  --query 'services[*].{name:serviceName,status:status,running:runningCount,desired:desiredCount,deployment:deployments[0].rolloutState}' \
  --output table
```

Flag any service where `running < desired` or `deployment != COMPLETED`.

### 1b. ECS recently stopped tasks

Check for recently stopped tasks that could indicate crash-looping:

```bash
aws ecs list-tasks --profile oscm --region us-east-1 \
  --cluster fund-applications --desired-status STOPPED \
  --query 'taskArns' --output json
```

If stopped tasks exist, describe them to get stop reasons:

```bash
aws ecs describe-tasks --profile oscm --region us-east-1 \
  --cluster fund-applications \
  --tasks <TASK_ARNS> \
  --query 'tasks[*].{task:taskArn,status:lastStatus,reason:stoppedReason,stopCode:stopCode,stopped:stoppedAt}' \
  --output table
```

Flag any tasks that stopped due to errors (not routine deployments).

### 2. ALB target group health

Retrieve target group ARNs first, then check health for each:

```bash
aws elbv2 describe-target-groups --profile oscm --region us-east-1 \
  --names fund-data-manager-server fund-portfolio-manager-server fund-ensemble-manager-server \
  --query 'TargetGroups[*].{name:TargetGroupName,arn:TargetGroupArn}' \
  --output json
```

For each target group ARN (run all three health checks in parallel):

```bash
aws elbv2 describe-target-health --profile oscm --region us-east-1 \
  --target-group-arn <ARN> \
  --query 'TargetHealthDescriptions[*].{target:Target.Id,port:Target.Port,state:TargetHealth.State,reason:TargetHealth.Reason}' \
  --output table
```

Flag any target with state other than `healthy`.

### 3. Direct health endpoint checks

Health endpoints are exposed on each service at `/health` (port 8080) but are
not routed through the ALB listener rules. Use the ALB target group health
status from check 2 instead of direct curl. The target group health checks
already hit `/health` on each registered target.

### 4. CloudWatch alarms

```bash
aws cloudwatch describe-alarms --profile oscm --region us-east-1 \
  --state-value ALARM \
  --alarm-name-prefix fund- \
  --query 'MetricAlarms[*].{name:AlarmName,state:StateValue,reason:StateReason}' \
  --output table
```

If no alarms are in ALARM state, report all clear. Otherwise flag each alarm.

### 5. Latest model artifact

Discover the model artifacts bucket name dynamically:

```bash
aws s3api list-buckets --profile oscm --region us-east-1 \
  --query 'Buckets[?starts_with(Name, `fund-model-artifacts`)].Name' --output text
```

Then list recent artifacts:

```bash
aws s3 ls s3://<MODEL_ARTIFACTS_BUCKET>/artifacts/tide/ \
  --profile oscm --region us-east-1 --recursive | sort | tail -3
```

Report the timestamp of the latest artifact. Staleness check accounts for
weekday-only training (Mon-Fri evenings ET):
- If today is Mon, allow up to ~3 days (since Friday night).
- If today is Tue-Fri, flag if older than ~1.5 days (should have run last night).
- If today is Sat-Sun, allow since Friday night.

### 6. S3 data freshness

Check recent equity bar data to verify data-manager syncs are working.
Discover the data bucket name dynamically:

```bash
aws s3api list-buckets --profile oscm --region us-east-1 \
  --query 'Buckets[?starts_with(Name, `fund-data-`)].Name' --output text
```

Then list recent data:

```bash
aws s3 ls s3://<DATA_BUCKET>/equity/bars/ \
  --profile oscm --region us-east-1 --recursive | sort | tail -5
```

Report the latest data timestamp. Apply the same weekday-aware staleness logic
as model artifacts (data syncs run 6 PM ET weekdays).

### 7. Redeployment Lambda recent activity

```bash
aws logs filter-log-events --profile oscm --region us-east-1 \
  --log-group-name /aws/lambda/fund-redeploy-ensemble-manager \
  --start-time $(python3 -c 'import time; print(int((time.time() - 86400) * 1000))') \
  --filter-pattern 'Forced new deployment' \
  --query 'events[*].{time:ingestionTime,message:message}' \
  --output table
```

Report whether the Lambda triggered recently. No recent invocations is normal
if no new model was uploaded.

### 8. Prefect training status (optional)

Only run if `prefect` CLI is available. The Prefect CLI must be connected to
Prefect Cloud (not a local server). Detect this by checking the output -- if
the CLI starts a "temporary server", report DEGRADED with a note that the
Prefect Cloud profile is not configured in this shell. To fix:

```bash
prefect cloud login
# or
prefect profile use cloud
```

When connected to Cloud, run:

```bash
prefect flow-run ls --flow-name tide-trainer --limit 3
```

Report latest training run status. Flag if most recent run failed.

## Output format

Present a summary table:

```text
Component             Status    Details
--------------------  --------  ----------------------------------------
ECS: data-manager     OK        1/1 running, deployment COMPLETED
ECS: portfolio-mgr    OK        1/1 running, deployment COMPLETED
ECS: ensemble-mgr     OK        1/1 running, deployment COMPLETED
ECS: stopped tasks    OK        No error-stopped tasks
ALB: data-manager     OK        1 healthy target (/health 200)
ALB: portfolio-mgr    OK        1 healthy target (/health 200)
ALB: ensemble-mgr     OK        1 healthy target (/health 200)
CloudWatch alarms     OK        No alarms firing
Model artifacts       OK        Latest: 2026-04-29 22:30
Data freshness        OK        Latest bars: 2026-04-29 18:05
Redeploy Lambda       OK        Last triggered: 2026-04-29 22:35
Training (Prefect)    OK        Latest run: SUCCESS
```

Use DEGRADED or ERROR for any failing checks and include the reason.

If everything is healthy, conclude with: "All platform components are healthy."

If any component is unhealthy, list specific remediation steps:
- ECS service unhealthy: "Force redeploy with `aws ecs update-service --profile oscm --region us-east-1 --cluster fund-applications --service <name> --force-new-deployment`"
- Stopped tasks with errors: "Check ECS task logs: `aws ecs describe-tasks` and Sentry for stack traces"
- Health endpoint failures: "Check application logs in CloudWatch: `/ecs/fund-<service>`"
- Model artifacts stale: "Check Prefect training runs and ECS `fund-models` cluster ASG scaling"
- Data freshness stale: "Check data-manager logs and Massive API connectivity"
- Prefect not connected: "Run `prefect cloud login` to configure Cloud profile"
