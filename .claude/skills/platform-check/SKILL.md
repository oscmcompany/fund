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
on a single exe.dev VM via `devenv --profile application up`. Data is checked
via the PostgreSQL database and service health endpoints.

## Connection

Default to the production application VM: `oscm-fund-production-application.exe.xyz`.

Establish an SSH tunnel for database access and use SSH for health endpoint checks:

```bash
ssh -L 15432:localhost:5432 oscm-fund-production-application.exe.xyz -N &
```

Wait for the tunnel to be ready, then query with:

```bash
for i in $(seq 1 15); do pg_isready -h localhost -p 15432 -q && break; sleep 1; done
pg_isready -h localhost -p 15432 || { echo "SSH tunnel failed"; exit 1; }
psql -h localhost -p 15432 -d fund -c "<QUERY>"
```

If the SSH connection fails, ask the user which VM to target.

## Check sequence

Run checks 1 through 5 in parallel. Collect all results before reporting.

### 1. Service health endpoints

Check all three services via SSH:

```bash
ssh oscm-fund-production-application.exe.xyz 'curl -sf http://localhost:8080/health'
ssh oscm-fund-production-application.exe.xyz 'curl -sf http://localhost:8082/health'
ssh oscm-fund-production-application.exe.xyz 'curl -sf http://localhost:8083/health'
```

Flag any service that doesn't return HTTP 200 with `"status": "ok"`.

- Port 8080: data-manager
- Port 8082: ensemble-manager
- Port 8083: portfolio-manager

### 2. Event bus health

Check that cron-driven events are firing and consumers are keeping up:

```sql
-- Recent events by type (last 24h)
SELECT event_type, count(*) as event_count, max(created_at) as latest_event
FROM events
WHERE created_at >= now() - interval '24 hours'
GROUP BY event_type
ORDER BY latest_event DESC;

-- Consumer offsets (are consumers stuck?)
SELECT consumer_name, last_event_id, updated_at
FROM event_consumer_offsets
ORDER BY updated_at DESC;

-- Latest event ID (for comparison with consumer offsets)
SELECT max(id) as latest_event_id FROM events;
```

Flag if:
- `equity_bars_sync_requested` hasn't fired today (weekdays only, expected at 05:00 UTC)
- `market_session_check` hasn't fired in the last hour (weekdays during market hours)
- Any consumer's `last_event_id` is more than 100 behind the latest event ID

### 3. Model status

```sql
SELECT run_id, model_name, status, continuous_ranked_probability_score,
       directional_accuracy, drift_status, started_at, completed_at
FROM model_runs
ORDER BY started_at DESC
LIMIT 1;
```

Weekday-aware staleness:
- Mon: allow up to ~3 days (since Friday night).
- Tue-Fri: flag if older than ~1.5 days (should have run last night).
- Sat-Sun: allow since Friday night.

Flag if `status` is not `completed` or `drift_status` is `drift_detected`.

### 4. Data freshness

```sql
-- Latest equity bars in the database
SELECT max(timestamp) as latest_bar FROM equity_bars;

-- Latest predictions
SELECT max(timestamp) as latest_prediction FROM equity_predictions;

-- Latest portfolio snapshot
SELECT max(snapshot_timestamp) as latest_snapshot, snapshot_type
FROM equity_portfolio_snapshots
GROUP BY snapshot_type
ORDER BY latest_snapshot DESC;
```

Apply the same weekday-aware staleness logic (data syncs run at 05:00 UTC weekdays).

### 5. S3 artifact freshness (optional double-check)

Verify that S3 artifacts match what the database reports. Discover buckets dynamically:

```bash
aws s3api list-buckets --profile oscm --region us-east-1 \
  --query 'Buckets[?starts_with(Name, `fund-model-artifacts`) || starts_with(Name, `oscm-fund`)].Name' --output text
```

List recent model artifacts:

```bash
aws s3 ls s3://<ARTIFACTS_BUCKET>/models/tide/ --profile oscm --region us-east-1 --recursive | sort | tail -3
```

List recent equity bar exports:

```bash
aws s3 ls s3://<DATA_BUCKET>/data/equity/bars/ --profile oscm --region us-east-1 | sort | tail -5
```

Compare timestamps with the database values from check 3 and 4. Flag discrepancies.
If AWS CLI is not authenticated, skip this check and note it in the report.

## Output format

Present a summary table:

```text
Component             Status    Details
--------------------  --------  ----------------------------------------
data-manager          OK        HTTP 200, port 8080
ensemble-manager      OK        HTTP 200, port 8082
portfolio-manager     OK        HTTP 200, port 8083
PostgreSQL            OK        Connected, latest event 2m ago
Event bus             OK        All consumers current, cron jobs firing
Model training        OK        Latest: tide-2026-07-09, CRPS=0.034, no drift
Equity bars           OK        Latest: 2026-07-09 18:05 UTC
Predictions           OK        Latest: 2026-07-09 22:30 UTC
Portfolio snapshots   OK        Latest EOD: 2026-07-09 20:00 UTC
S3 artifacts          OK        Matches database (or SKIPPED if AWS CLI unavailable)
```

Use DEGRADED or ERROR for any failing checks and include the reason.

If everything is healthy, conclude with: "All platform components are healthy."

If any component is unhealthy, list specific remediation steps:
- Service not responding: "SSH to VM and check `devenv --profile application up` process output"
- Events not firing: "Check pg_cron extension and cron.job table on the VM"
- Consumer stuck: "Check service logs in `/var/log/fund/` for the stuck consumer"
- Model stale: "Check training pipeline on the trainer VM"
- Data stale: "Check data-manager logs and Massive API connectivity"
- S3 mismatch: "Compare S3 artifact timestamps with model_runs table"
