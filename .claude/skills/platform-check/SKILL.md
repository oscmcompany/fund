---
name: platform-check
description: >
  Check fund platform health across all services. Use when the user asks
  "is everything running", "check the platform", "platform status",
  "are services healthy", "check health", "is the fund ok", or
  wants to verify end-to-end system state.
---

# Platform Health Check

Run a comprehensive health check of all fund platform components. Services run
on a single VM via `devenv up`. S3 buckets are checked via AWS CLI
(`--profile oscm --region us-east-1`).

## Check sequence

Run checks 1 through 4 in parallel. Collect all results before reporting.

### 1. Service health endpoints

Check all three services in parallel:

```bash
curl -sf http://localhost:8080/health | jq .
curl -sf http://localhost:8081/health | jq .
curl -sf http://localhost:8082/health | jq .
```

Flag any service that doesn't return HTTP 200 with `"status": "ok"`.

### 2. Latest model artifact

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

### 3. S3 data freshness

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

### 4. Sentry recent errors

Check Sentry for recent errors (if Sentry CLI is available):

```bash
sentry-cli issues list --project fund --status unresolved --sort date | head -5
```

If Sentry CLI is not available, note it as unchecked.

## Output format

Present a summary table:

```text
Component             Status    Details
--------------------  --------  ----------------------------------------
data-manager          OK        HTTP 200, status: ok
portfolio-manager     OK        HTTP 200, status: ok
ensemble-manager      OK        HTTP 200, model loaded
Model artifacts       OK        Latest: 2026-04-29 22:30
Data freshness        OK        Latest bars: 2026-04-29 18:05
Sentry errors         OK        No unresolved errors
```

Use DEGRADED or ERROR for any failing checks and include the reason.

If everything is healthy, conclude with: "All platform components are healthy."

If any component is unhealthy, list specific remediation steps:
- Service not responding: "Check `devenv up` process output and Sentry for stack traces"
- Model artifacts stale: "Check training pipeline and S3 artifact uploads"
- Data freshness stale: "Check data-manager logs and Massive API connectivity"
- Ensemble-manager model error: "Check model artifact download in logs, verify S3 bucket access"
