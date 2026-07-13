---
name: generate-fund-report
description: >
  Daily fund briefing combining platform health and trading activity.
  Use when the user asks "morning report", "daily report", "fund briefing",
  "how is the fund", "fund status", "trading status", "portfolio summary",
  "what happened overnight", "morning briefing", or "how are we doing".
---

# Daily Fund Briefing

Produce a single morning report covering platform health and trading activity.
Data comes from the PostgreSQL database on the production VM and the Alpaca REST API.

## Connection

Default to the production application VM: `oscm-fund-production-application.exe.xyz`.
Connect to PostgreSQL via SSH tunnel:

```bash
ssh -L 15432:localhost:5432 oscm-fund-production-application.exe.xyz -N &
```

Wait for the tunnel to be ready before querying:

```bash
for i in $(seq 1 15); do pg_isready -h localhost -p 15432 -q && break; sleep 1; done
pg_isready -h localhost -p 15432 || { echo "SSH tunnel failed"; exit 1; }
```

Then query with:

```bash
psql -h localhost -p 15432 -d fund -c "<QUERY>"
```

If the SSH connection fails, ask the user which VM to target.

## Execution order

1. **Phase 1 -- Credential retrieval** (must complete before Phase 2 Group B)
2. **Phase 2 -- Data collection** (two parallel groups)
   - Group A: Database queries (all independent, run in parallel)
   - Group B: Alpaca API calls (all independent, run in parallel)
3. **Phase 3 -- Render report** (after all data collected)

---

## Phase 1: Credential retrieval

Retrieve Alpaca credentials from AWS Secrets Manager for direct API calls:

```bash
aws secretsmanager get-secret-value --profile oscm --region us-east-1 \
  --secret-id 'fund/production/portfolio-manager/all' \
  --query 'SecretString' --output text \
  | jq -r '{key: .ALPACA_API_KEY_ID, secret: .ALPACA_API_SECRET, paper: .ALPACA_IS_PAPER}'
```

Extract:
- `ALPACA_KEY` = `.key`
- `ALPACA_SECRET` = `.secret`
- `IS_PAPER` = `.paper` (string `"true"` or `"false"`)

Determine the base URL:
- If `IS_PAPER` is `"true"`: `https://paper-api.alpaca.markets`
- Otherwise: `https://api.alpaca.markets`

If credential retrieval fails, skip Group B and suggest `aws sso login --profile oscm`.

---

## Phase 2, Group A: Database queries

Run all of these in parallel via the SSH tunnel.

### A1. Latest portfolio snapshot

```sql
SELECT snapshot_timestamp, snapshot_type, net_asset_value, gross_return, net_return, total_slippage_cost
FROM equity_portfolio_snapshots
ORDER BY snapshot_timestamp DESC
LIMIT 5;
```

### A2. Active pairs

```sql
SELECT p.pair_id, p.long_ticker, p.short_ticker, p.z_score, p.hedge_ratio,
       p.signal_strength, p.opened_at
FROM equity_pairs p
WHERE p.status = 'open'
ORDER BY p.opened_at DESC;
```

### A3. Recent rebalance sessions

```sql
SELECT id, triggered_at, trigger_reason, model_run_id, status, completed_at
FROM equity_rebalance_sessions
ORDER BY triggered_at DESC
LIMIT 5;
```

### A4. Recent orders

```sql
SELECT o.submitted_at, o.ticker, o.side, o.quantity, o.order_type, o.limit_price, o.alpaca_order_id
FROM equity_orders o
ORDER BY o.submitted_at DESC
LIMIT 20;
```

### A5. Latest model run

```sql
SELECT run_id, model_name, status, continuous_ranked_probability_score,
       directional_accuracy, quantile_coverage, drift_status, started_at, completed_at
FROM model_runs
ORDER BY started_at DESC
LIMIT 3;
```

### A6. Recent closed pairs (last 5 days)

```sql
SELECT pair_id, long_ticker, short_ticker, realized_profit_and_loss, return_percent,
       close_reason, opened_at, closed_at
FROM equity_pairs
WHERE status = 'closed' AND closed_at >= now() - interval '5 days'
ORDER BY closed_at DESC;
```

### A7. Event bus health

```sql
SELECT event_type, count(*) as event_count,
       max(created_at) as latest_event
FROM events
WHERE created_at >= now() - interval '24 hours'
GROUP BY event_type
ORDER BY latest_event DESC;
```

---

## Phase 2, Group B: Alpaca API calls

Run all of these in parallel. Skip this group if credential retrieval failed.

### B1. Market clock

```bash
curl -sSf -w '\n%{http_code}' \
  -H 'APCA-API-KEY-ID: <ALPACA_KEY>' \
  -H 'APCA-API-SECRET-KEY: <ALPACA_SECRET>' \
  '<BASE_URL>/v2/clock'
```

Extract: `is_open`, `next_open`, `next_close`.

### B2. Account info

```bash
curl -sSf -w '\n%{http_code}' \
  -H 'APCA-API-KEY-ID: <ALPACA_KEY>' \
  -H 'APCA-API-SECRET-KEY: <ALPACA_SECRET>' \
  '<BASE_URL>/v2/account'
```

Extract: `equity`, `cash`, `buying_power`, `portfolio_value`, `status`,
`last_equity` (for day change calculation).

Day change = `equity - last_equity`. Day change % = `(equity - last_equity) / last_equity * 100`.

### B3. Current positions (Alpaca)

```bash
curl -sSf -w '\n%{http_code}' \
  -H 'APCA-API-KEY-ID: <ALPACA_KEY>' \
  -H 'APCA-API-SECRET-KEY: <ALPACA_SECRET>' \
  '<BASE_URL>/v2/positions'
```

For each position extract: `symbol`, `side`, `market_value`, `cost_basis`,
`unrealized_pl`, `unrealized_plpc`.

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

--- ACCOUNT ---

  Equity:          $XX,XXX.XX
  Cash:            $XX,XXX.XX
  Buying power:    $XX,XXX.XX
  Day change:      +$XXX.XX (+X.XX%)

--- POSITIONS (Alpaca) ---

  Ticker  Side   Mkt Value    Cost Basis   Unrealized P&L
  ------  -----  ----------   ----------   ---------------
  AAPL    long   $2,345.67    $2,300.00    +$45.67 (+1.99%)
  ...

  (or "No open positions" if empty)

--- ACTIVE PAIRS ---

  Pair ID     Long    Short   Z-Score  Signal   Opened
  ----------  ------  ------  -------  -------  ----------------
  AAPL-MSFT   AAPL    MSFT    -2.13    0.85     07/09 09:32 AM
  ...

  (or "No active pairs" if empty)

--- RECENT CLOSED PAIRS (last 5 days) ---

  Pair         P&L          Return    Reason       Closed
  ----------   ----------   -------   ----------   ----------------
  AAPL-MSFT    +$123.45     +1.23%    profit_taken 07/08 03:55 PM
  ...

  (or "No recently closed pairs" if empty)

--- RECENT ORDERS (last 20) ---

  Time             Symbol  Side   Qty      Type     Limit
  ---------------  ------  -----  -------  -------  ----------
  07/09 10:02 AM   AAPL    LONG   50       limit    $186.45
  ...

  (or "No recent orders" if empty)

--- PORTFOLIO SNAPSHOTS ---

  Timestamp            Type       NAV           Gross     Net       Slippage
  -------------------  ---------  -----------   -------   -------   --------
  2026-07-09 20:00     end_of_day $XX,XXX.XX    +0.45%    +0.42%    $12.34
  ...

--- MODEL STATUS ---

  Latest run:    <run_id>
  Status:        completed
  CRPS:          0.0342
  Dir. Accuracy: 0.5812
  Drift:         none
  Started:       2026-07-09 22:00 UTC

--- EVENT BUS (24h) ---

  Event Type                        Count   Latest
  --------------------------------  ------  -------------------
  market_session_check              48      2026-07-10 13:00
  equity_bars_sync_requested        1       2026-07-10 05:00
  ...

--- REBALANCE SESSIONS ---

  Triggered            Reason              Status     Model Run
  -------------------  ------------------  ---------  ----------
  2026-07-09 09:31     market_session       completed  tide-2026-...
  ...
```

---

## Error handling

| Failure | Behavior |
|---------|----------|
| SSH tunnel fails | Ask user which VM to target |
| AWS CLI / SSO not authenticated | Skip Alpaca sections, suggest `aws sso login --profile oscm` |
| Alpaca 401/403 | Mark trading sections as ERROR, suggest checking secret values |
| Database query fails | Mark that section DEGRADED, continue with remaining queries |
| No positions | Display "No open positions" |
| No recent orders | Display "No recent orders" |
| No active pairs | Display "No active pairs" |

If database sections succeed but Alpaca sections fail (or vice versa), still render
the sections that succeeded. Never let one section's failure prevent the rest of the
report from rendering.
