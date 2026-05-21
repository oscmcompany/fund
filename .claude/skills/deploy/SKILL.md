---
name: deploy
description: >
  Set up and manage devenv services on exe.dev VM. Uses profiles to
  separate app services from ML training. Secrets are managed through
  secretspec with the awssm provider. Use when the user wants to deploy
  services, manage the environment, start services, check health, or
  set up local development. Triggers on requests like "deploy", "set up
  production", "devenv up", "start services", "deploy failed", "deploy
  errors", "fix deploy", "check health", or environment setup involving
  devenv.
---

# devenv + exe.dev Deployment

`devenv.nix` drives both local development and production. Services run
via `devenv --profile apps up` on a single exe.dev VM. ML training uses
`devenv --profile ml shell`. Secrets are injected via `secretspec run --`.

## Architecture

```text
devenv.nix
├── base (always active, no profile needed)
│   ├── languages (Rust, Python, Nix)
│   ├── packages (clang, uv, ruff, cargo-watch, etc.)
│   ├── env (AWS_REGION, CC=clang, S3 bucket names, LIBRARY_PATH)
│   ├── scripts (check scripts, aws utilities, bump-deps)
│   ├── tasks (checks:python, checks:rust, checks:ci)
│   └── git-hooks (all 6 hooks)
├── profiles.apps (devenv --profile apps up)
│   ├── processes.data-manager (Rust, port 8080)
│   ├── processes.ensemble-manager (Python/FastAPI, port 8082)
│   ├── processes.portfolio-manager (Python/FastAPI, port 8081)
│   ├── processes.artifact-watcher (polls S3)
│   ├── env (service URLs, MASSIVE_BASE_URL, DISABLE_DISK_CACHE, BACKFILL_LOOKBACK_DAYS)
│   └── scripts.cleanup-services
└── profiles.ml (devenv --profile ml shell)
    ├── env (FUND_LOOKBACK_DAYS, MLFLOW_TRACKING_URI, PREFECT_API_URL)
    └── scripts (train-local, deploy-training)

AWS (retained services)
├── S3: fund-production-data (equity bars, predictions, portfolios)
├── S3: fund-production-model-artifacts (trained model weights)
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

## Deployment

### Local Development

```bash
devenv shell                    # enter the development environment
devenv --profile apps up        # start application services with hot-reload
devenv --profile ml shell       # ML training environment
```

### Production (exe.dev VM)

```bash
# On the VM:
./tools/bootstrap-machine --prod  # first-time setup
devenv --profile apps up          # start application services (secretspec injects secrets)
```

Production mode is activated by setting `FUND_PROFILE=production` in `.env`.
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

## Environment Variables (Production .env)

```bash
FUND_PROFILE=production
AWS_S3_MODEL_ARTIFACT_PATH=artifacts/tide/
MASSIVE_BASE_URL=https://api.massive.com
```

## Common Issues

- **Service won't start**: check `cleanup-services` to kill stale processes
- **Secrets missing**: run `secretspec check` to validate, `secretspec set <KEY>` to fix
- **Model not loading**: check S3 artifacts path and ensemble-manager logs
- **Artifact watcher not restarting**: verify S3 credentials and bucket name
