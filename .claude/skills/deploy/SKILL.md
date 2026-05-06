---
name: deploy
description: >
  Set up and manage devenv services on exe.dev VM. Covers both local
  development (devenv up) and production deployment via SSH. Secrets
  are managed through secretspec with the awssm provider. Use when the
  user wants to deploy services, manage the environment, start services,
  check health, or set up local development. Triggers on requests like
  "deploy", "set up production", "devenv up", "start services",
  "deploy failed", "deploy errors", "fix deploy", "check health",
  or environment setup involving devenv.
---

# devenv + exe.dev Deployment

`devenv.nix` drives both local development and production. All services
run via `devenv up` on a single exe.dev VM. Secrets are injected via
`secretspec run --`.

## Architecture

```text
devenv.nix (local development and production)
├── processes.data-manager (Rust, port 8080)
├── processes.ensemble-manager (Python/FastAPI, port 8082)
├── processes.portfolio-manager (Python/FastAPI, port 8081)
├── processes.artifact-watcher (production only, polls S3)
└── env.* (localhost URLs, dev defaults)

AWS (retained services)
├── S3: fund-data-404221e2 (equity bars, predictions, portfolios)
├── S3: fund-model-artifacts-404221e2 (trained model weights)
└── Secrets Manager: secretspec/fund/production/* (via secretspec awssm)
```

## Services

All services communicate over localhost:

| Service | Port | Language | Description |
|---------|------|----------|-------------|
| data-manager | 8080 | Rust/Axum | Equity data fetching and storage |
| portfolio-manager | 8081 | Python/FastAPI | Portfolio rebalancing |
| ensemble-manager | 8082 | Python/FastAPI | Model predictions |
| artifact-watcher | - | Python | Polls S3, restarts ensemble-manager on new artifacts |

## Secrets (secretspec)

Secrets are stored in AWS Secrets Manager under the secretspec naming
convention: `secretspec/fund/production/{KEY}`.

```bash
secretspec check          # validate all production secrets are accessible
secretspec set <KEY>      # set a secret value interactively
```

Required production secrets:
- `MASSIVE_API_KEY` - Massive API key for equity data
- `ALPACA_API_KEY_ID` - Alpaca trading API key ID
- `ALPACA_API_SECRET` - Alpaca trading API secret
- `ALPACA_IS_PAPER` - Alpaca paper trading flag
- `SENTRY_DSN` - Sentry error tracking DSN (optional)

## Deployment

### Local Development

```bash
devenv shell   # enter the development environment
devenv up      # start all services with hot-reload
```

### Production (exe.dev VM)

```bash
# On the VM:
./tools/bootstrap-machine --prod  # first-time setup
devenv up                         # start all services (secretspec injects secrets)
```

Production mode is activated by setting `FUND_ENVIRONMENT=production` in `.envrc`.
This disables hot-reload, enables `cargo run --release`, and starts the
artifact-watcher process.

### Manual Deploy

```bash
ssh exe.dev
cd /path/to/fund
git pull
# devenv up restarts automatically if running under systemd
```

## Health Checks

```bash
curl localhost:8080/health  # data-manager
curl localhost:8081/health  # portfolio-manager
curl localhost:8082/health  # ensemble-manager
```

## Environment Variables (Production .envrc)

```bash
export FUND_ENVIRONMENT=production
export AWS_S3_DATA_BUCKET_NAME=fund-data-404221e2
export AWS_S3_MODEL_ARTIFACTS_BUCKET_NAME=fund-model-artifacts-404221e2
export AWS_S3_MODEL_ARTIFACT_PATH=artifacts/tide/
export MASSIVE_BASE_URL=https://api.massive.com
```

## Common Issues

- **Service won't start**: check `cleanup-services` to kill stale processes
- **Secrets missing**: run `secretspec check` to validate, `secretspec set <KEY>` to fix
- **Model not loading**: check S3 artifacts path and ensemble-manager logs
- **Artifact watcher not restarting**: verify S3 credentials and bucket name
