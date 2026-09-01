# The backfill box

> A record of a hand-built fixture, so rebuilding it is reading rather than remembering.

The bulk backfills run on one EC2 instance rather than an exe.dev VM, because they are bandwidth
bound: the range reader holds 128 concurrent connections to Massive and the difference between
same-region and a laptop is roughly 16 MB/s against 22.

Everything here was configured by hand and lives only in AWS. That is a reasonable trade for a
single fixture — see the argument in `create-bucket` for why a per-instance script should not learn
about a singular one — but a fixture nobody can read is one nobody can rebuild. This file is the
reading.

## What exists

| Piece | Value |
| --- | --- |
| Instance | `i-03498f81e917976f0`, `r6i.2xlarge`, `us-east-1` |
| Instance profile | `fund-quote-backfill` |
| Inline policy | `archive-and-secrets` — checked in as `backfill-box-role-policy.json` |
| Managed policy | `AmazonSSMManagedInstanceCore`, so the box needs no SSH key |
| Stall alarm | `fund-quote-backfill-stalled` |
| Runner | `run-backfill`, deployed to `~/run-backfill` |

## The policy

`backfill-box-role-policy.json` is the live document. Apply it with:

```bash
aws iam put-role-policy --role-name fund-quote-backfill \
  --policy-name archive-and-secrets \
  --policy-document file://tools/backfill-box-role-policy.json
```

Two of its grants are less obvious than they look:

- **The archive bucket had to be added.** The role predates the bucket split and named only the
  developer bucket, so the first trade pass failed in 45 ms on `ListBucket`.
- **`s3:AbortMultipartUpload` is load-bearing and silent when absent.** The raw tee aborts a failed
  multipart upload rather than leaving 64 MiB parts billing until the bucket's seven-day rule sweeps
  them, and the abort is best-effort: it warns and carries on. Without this grant the guard is
  defeated by IAM rather than by logic, and the only symptom is a slowly growing bill.

Every other principal in the account is an administrator, which is why this role is the only place a
bucket rename can break anything — and why a break here is worth writing down.

## The stall alarm

`NetworkIn` below 1 MB summed over twelve five-minute periods stops the instance. A backfill holds
NetworkIn far above that for its whole run, so the alarm only fires once the work has genuinely
stopped, and stopping the box is what keeps a dead run from billing for a weekend.

**It must treat missing data as not breaching.** A stopped instance publishes no `NetworkIn` at all,
and counting that silence as a stall banks breaching periods while the box is off — so the alarm
fires within minutes of the next start, before any work can raise the metric. That happened on
2026-09-01 and stopped the box four minutes into a cold start.

```bash
aws cloudwatch put-metric-alarm --alarm-name fund-quote-backfill-stalled \
  --metric-name NetworkIn --namespace AWS/EC2 --statistic Sum \
  --dimensions Name=InstanceId,Value=i-03498f81e917976f0 \
  --period 300 --evaluation-periods 12 --threshold 1000000 \
  --comparison-operator LessThanThreshold --treat-missing-data notBreaching \
  --alarm-actions arn:aws:automate:us-east-1:ec2:stop
```

## Deploying to it

The box has no git checkout — the tree arrives as a tarball, so a build there is of whatever was
last shipped rather than of a commit anyone can name.

```bash
git archive --format=tar.gz -o /tmp/fund-master.tar.gz origin/master
aws s3 cp /tmp/fund-master.tar.gz s3://<records-bucket>/bootstrap/fund-master.tar.gz
# then, on the box, over the existing tree so target/ survives and the rebuild is incremental:
#   tar -xzf fund-master.tar.gz -C ~/fund && cargo build --release --bin seed
```

Commands reach it through SSM rather than SSH. Two things bite:

- **SSM runs the script under `dash`**, so `set -euo pipefail` fails on `pipefail` — use `set -eu`.
- **`HOME` is unset**, which breaks `. ~/.cargo/env`; set it explicitly before building.

## Running a pass

```bash
FUND_PROFILE=development/john.forstmeier ~/run-backfill \
  equity-trades archive --start 2021-08-26 --end 2026-08-28 --tee-raw
```

`archive` reads presence off the daily prefix and skips sessions already summarised, so a pass that
dies is resumed by re-running the identical command. `widen` is the deliberate override that revisits
sessions already written.
