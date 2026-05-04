---
name: fund-briefing
description: >
  Daily fund briefing combining platform health and trading activity.
  Use when the user asks "morning report", "daily report", "fund briefing",
  "how is the fund", "fund status", "trading status", "portfolio summary",
  "what happened overnight", "morning briefing", or "how are we doing".
---

# Daily Fund Briefing

Produce a single morning report covering platform health and Alpaca trading
activity. Uses the OSCM AWS account (`--profile oscm --region us-east-1`) and
the Alpaca REST API. All tools (`aws`, `curl`, `jq`) are available in the
devenv shell.

## Execution order

1. **Phase 1 -- Credential retrieval** (must complete before Phase 2)
2. **Phase 2 -- Data collection** (two parallel groups)
   - Group A: Platform health checks (all independent, run in parallel)
   - Group B: Alpaca API calls (all independent, run in parallel)
3. **Phase 3 -- Render report** (after all data collected)

---

## Phase 1: Credential retrieval

### 1a. Discover portfolio manager secret name

The secret uses a path-based naming convention (`fund/production/portfolio-manager/all`).
The `--filter Key=name` does a **prefix** match, so use the full path prefix:

```bash
aws secretsmanager list-secrets --profile oscm --region us-east-1 \
  --filter Key=name,Values=fund/production/portfolio-manager \
  --query 'SecretList[0].Name' --output text
```

Store the result as `SECRET_NAME`. If this returns `None` or fails, skip all
Alpaca sections and suggest the user run `aws sso login --profile oscm`.

### 1b. Retrieve and parse credentials

```bash
bash -c "aws secretsmanager get-secret-value --profile oscm --region us-east-1 \
  --secret-id '<SECRET_NAME>' \
  --query 'SecretString' --output text \
  | jq -r '{key: .ALPACA_API_KEY_ID, secret: .ALPACA_API_SECRET, paper: .ALPACA_IS_PAPER}'"
```

Extract:
- `ALPACA_KEY` = `.key`
- `ALPACA_SECRET` = `.secret`
- `IS_PAPER` = `.paper` (string `"true"` or `"false"`)

Determine the base URL:
- If `IS_PAPER` is `"true"`: `https://paper-api.alpaca.markets`
- Otherwise: `https://api.alpaca.markets`

All subsequent Alpaca curl calls use these headers:
```
APCA-API-KEY-ID: <ALPACA_KEY>
APCA-API-SECRET-KEY: <ALPACA_SECRET>
```

---

## Phase 2, Group A: Platform health checks

Run all of these in parallel. These are the same checks from the platform-check
skill, inlined here to avoid skill-in-skill invocation.

### A1. ECS services

```bash
aws ecs describe-services --profile oscm --region us-east-1 \
  --cluster fund-applications \
  --services fund-data-manager-server fund-portfolio-manager-server fund-ensemble-manager-server \
  --query 'services[*].{name:serviceName,status:status,running:runningCount,desired:desiredCount,deployment:deployments[0].rolloutState}' \
  --output json
```

Flag any service where `running < desired` or `deployment != COMPLETED`.

### A2. ALB target group health

First get ARNs:

```bash
aws elbv2 describe-target-groups --profile oscm --region us-east-1 \
  --names fund-data-manager-server fund-portfolio-manager-server fund-ensemble-manager-server \
  --query 'TargetGroups[*].{name:TargetGroupName,arn:TargetGroupArn}' \
  --output json
```

Then for each ARN (run in parallel):

```bash
aws elbv2 describe-target-health --profile oscm --region us-east-1 \
  --target-group-arn <ARN> \
  --query 'TargetHealthDescriptions[*].{target:Target.Id,state:TargetHealth.State}' \
  --output json
```

Flag any target with state other than `healthy`.

### A3. CloudWatch alarms

```bash
aws cloudwatch describe-alarms --profile oscm --region us-east-1 \
  --state-value ALARM \
  --alarm-name-prefix fund- \
  --query 'MetricAlarms[*].{name:AlarmName,state:StateValue,reason:StateReason}' \
  --output json
```

### A4. Model artifact freshness

Discover bucket:

```bash
aws s3api list-buckets --profile oscm --region us-east-1 \
  --query 'Buckets[?starts_with(Name, `fund-model-artifacts`)].Name' --output text
```

List recent artifacts:

```bash
bash -c "aws s3 ls s3://<MODEL_ARTIFACTS_BUCKET>/artifacts/tide/ \
  --profile oscm --region us-east-1 --recursive | sort | tail -3"
```

Weekday-aware staleness:
- Mon: allow up to ~3 days (since Friday night).
- Tue-Fri: flag if older than ~1.5 days.
- Sat-Sun: allow since Friday night.

### A5. S3 data freshness

Discover bucket:

```bash
aws s3api list-buckets --profile oscm --region us-east-1 \
  --query 'Buckets[?starts_with(Name, `fund-data-`)].Name' --output text
```

List recent data (path is `equity/bars/`, not `equity_bars/`):

```bash
bash -c "aws s3 ls s3://<DATA_BUCKET>/equity/bars/ \
  --profile oscm --region us-east-1 --recursive | sort | tail -5"
```

Same weekday-aware staleness logic (data syncs at 6 PM ET weekdays).

---

## Phase 2, Group B: Alpaca API calls

Run all of these in parallel. Skip this entire group if credential retrieval
failed. For each call, if the HTTP status is 401 or 403, mark the section as
ERROR and suggest checking the secret values in AWS Secrets Manager.

### B1. Market clock

```bash
curl -s -w '\n%{http_code}' \
  -H 'APCA-API-KEY-ID: <ALPACA_KEY>' \
  -H 'APCA-API-SECRET-KEY: <ALPACA_SECRET>' \
  '<BASE_URL>/v2/clock'
```

Extract: `is_open`, `next_open`, `next_close`.

### B2. Account info

```bash
curl -s -w '\n%{http_code}' \
  -H 'APCA-API-KEY-ID: <ALPACA_KEY>' \
  -H 'APCA-API-SECRET-KEY: <ALPACA_SECRET>' \
  '<BASE_URL>/v2/account'
```

Extract: `equity`, `cash`, `buying_power`, `portfolio_value`, `status`,
`last_equity` (for day change calculation).

Day change = `equity - last_equity`. Day change % = `(equity - last_equity) / last_equity * 100`.

### B3. Current positions

```bash
curl -s -w '\n%{http_code}' \
  -H 'APCA-API-KEY-ID: <ALPACA_KEY>' \
  -H 'APCA-API-SECRET-KEY: <ALPACA_SECRET>' \
  '<BASE_URL>/v2/positions'
```

For each position extract: `symbol`, `side`, `market_value`, `cost_basis`,
`unrealized_pl`, `unrealized_plpc`.

If the array is empty, display "No open positions".

### B4. Recent orders (last 2 days)

```bash
curl -s -w '\n%{http_code}' \
  -H 'APCA-API-KEY-ID: <ALPACA_KEY>' \
  -H 'APCA-API-SECRET-KEY: <ALPACA_SECRET>' \
  '<BASE_URL>/v2/orders?status=all&after=<2_DAYS_AGO_ISO8601>&limit=50&direction=desc'
```

Compute `<2_DAYS_AGO_ISO8601>` as `$(date -u -v-2d +%Y-%m-%dT00:00:00Z)` (macOS)
or `$(date -u -d "2 days ago" +%Y-%m-%dT00:00:00Z)` (Linux).

For each order extract: `submitted_at` (format as `MM/DD HH:MM AM/PM` ET),
`symbol`, `side`, `notional` or `qty`, `status`, `filled_avg_price`.

If the array is empty, display "No orders in the last 2 days".

### B5. Weekly portfolio history

```bash
curl -s -w '\n%{http_code}' \
  -H 'APCA-API-KEY-ID: <ALPACA_KEY>' \
  -H 'APCA-API-SECRET-KEY: <ALPACA_SECRET>' \
  '<BASE_URL>/v2/account/portfolio/history?period=1W&timeframe=1D'
```

Extract the `timestamp` and `equity` arrays. Calculate daily P&L as the
difference between consecutive equity values. Compute week-to-date return
as `(last_equity - first_equity) / first_equity * 100`.

---

## Phase 3: Report output

Render the report using the format below. All dollar values should use commas
and two decimal places (e.g., `$12,345.67`). Percentages use two decimal places
with a sign prefix (e.g., `+1.23%`, `-0.45%`).

```text
================================================================
  DAILY FUND REPORT -- <DATE> <TIME> ET
================================================================

  Market: OPEN / CLOSED (next open/close: <time>)
  Account mode: PAPER / LIVE

--- PLATFORM HEALTH ---

  Component             Status    Details
  --------------------  --------  ----------------------------------------
  ECS: data-manager     OK        1/1 running, deployment COMPLETED
  ECS: portfolio-mgr    OK        1/1 running, deployment COMPLETED
  ECS: ensemble-mgr     OK        1/1 running, deployment COMPLETED
  ALB targets           OK        All 3 target groups healthy
  CloudWatch alarms     OK        No alarms firing
  Model artifacts       OK        Latest: <timestamp>
  Data freshness        OK        Latest bars: <timestamp>

--- ACCOUNT ---

  Equity:          $XX,XXX.XX
  Cash:            $XX,XXX.XX
  Buying power:    $XX,XXX.XX
  Day change:      +$XXX.XX (+X.XX%)

--- POSITIONS ---

  Ticker  Side   Mkt Value    Cost Basis   Unrealized P&L
  ------  -----  ----------   ----------   ---------------
  AAPL    long   $2,345.67    $2,300.00    +$45.67 (+1.99%)
  ...

  (or "No open positions" if empty)

--- RECENT ORDERS (last 2 days) ---

  Time             Symbol  Side   Notional     Status   Fill Price
  ---------------  ------  -----  ----------   -------  ----------
  05/03 10:02 AM   AAPL    buy    $2,300.00    filled   $186.45
  ...

  (or "No orders in the last 2 days" if empty)

--- WEEKLY PERFORMANCE ---

  Date        Equity        Daily P&L
  ----------  -----------   ----------
  2026-04-28  $XX,XXX.XX    --
  2026-04-29  $XX,XXX.XX    +$XX.XX
  ...

  Week-to-date: +X.XX%
```

---

## Error handling

| Failure | Behavior |
|---------|----------|
| AWS CLI / SSO not authenticated | Report error, suggest `aws sso login --profile oscm` |
| Secret not found | Skip all Alpaca sections, show platform health only |
| Alpaca 401/403 | Mark trading sections as ERROR, suggest checking secret values |
| Individual platform check fails | Mark that row DEGRADED, continue with remaining checks |
| No positions | Display "No open positions" |
| No recent orders | Display "No orders in the last 2 days" |
| Portfolio history empty | Display "No history available" |

If platform health is fully healthy but Alpaca sections have errors (or vice
versa), still render the sections that succeeded. Never let one section's failure
prevent the rest of the report from rendering.
