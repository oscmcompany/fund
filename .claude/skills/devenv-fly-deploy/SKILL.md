---
name: devenv-fly-deploy
description: >
  Set up and manage devenv + AWS infrastructure from devenv.nix and Pulumi.
  Covers both local development (devenv up) and production deployment to AWS
  ECS/Fargate via Pulumi. Use when the user wants to deploy services, manage
  infrastructure, push Docker images to ECR, update ECS services, manage
  Pulumi stacks, add monitoring/metrics/grafana/prometheus, configure
  deploy commands, or set up local development services. Triggers on requests
  like "deploy to aws", "set up production", "devenv deploy", "add grafana",
  "set up metrics", "add monitoring", "devenv up", "add a local service",
  "deploy failed", "deploy errors", "fix deploy", "ecs logs", "deploy all",
  "infra up", "pulumi up", or infrastructure setup involving devenv and AWS.
---

# devenv + AWS Infrastructure

`devenv.nix` drives local development (`devenv up`). Pulumi (`infrastructure/`)
manages AWS production infrastructure. Docker images are built locally, pushed
to ECR, and ECS services are force-redeployed to pick up new images.

## Architecture

```
devenv.nix (local development)
├── services.postgres (PostgreSQL)
├── services.minio (S3-compatible storage)
├── services.prometheus (metrics scraping)
├── processes.* (application + grafana + ready banner)
└── env.* (localhost URLs, dev credentials)

infrastructure/ (AWS production via Pulumi)
├── __main__.py (exports)
├── compute.py (ECS services, ALB, RDS, ElastiCache)
├── networking.py (VPC, subnets, security groups)
├── storage.py (ECR repos, S3 buckets)
├── iam.py (roles, policies, OIDC)
├── secrets.py (Secrets Manager)
├── parameters.py (SSM parameters)
└── notifications.py (SNS, budgets, SES)
```

Same code, different env vars:
| Concern | Local | AWS |
|---------|-------|-----|
| Storage | MinIO at `localhost:9000` | S3 (native) |
| Database | Local PostgreSQL | RDS PostgreSQL |
| Service discovery | `localhost:PORT` | `<service>.fund.local:PORT` (Cloud Map) |
| Metrics | Local Prometheus | CloudWatch Container Insights |
| MLflow | Not running locally | `http://mlflow.fund.local:8080` |
| Dashboards | Local Grafana + local Prometheus | Grafana ECS + CloudWatch |

## AWS Services

7 ECS Fargate services on a single ALB:
| Service | ALB Port | Health Check | CPU/Memory |
|---------|----------|-------------|------------|
| data-manager | 80/443 (path: `/equity-bars*`, etc.) | `/health` | 256/512 |
| ensemble-manager | 80/443 (path: `/model/*`) | `/health` | 256/512 |
| portfolio-manager | 80/443 (path: `/portfolio*`) | `/health` | 256/512 |
| training-server (Prefect) | 4200 | `/api/health` | 512/1024 |
| training-worker | internal only | N/A | 4096/8192 |
| mlflow | 5000 | `/health` | 512/2048 |
| grafana | 3000 | `/api/health` | 256/512 |

Supporting infrastructure:
- VPC with public/private subnets across 2 AZs
- NAT Gateway for private subnet egress
- RDS PostgreSQL for Prefect + separate RDS for MLflow
- ElastiCache Redis for Prefect
- S3 buckets: fund-data, fund-model-artifacts, fund-mlflow-artifacts
- ECR repos for all 7 services + tide-trainer
- Cloud Map service discovery (fund.local namespace)
- Secrets Manager for app secrets
- SSM Parameter Store for config

## Deployment Commands (devenv scripts)

```bash
# Infrastructure lifecycle
infra-up              # pulumi up --stack production
infra-down            # pulumi down --stack production
infra-outputs         # show all Pulumi outputs as JSON
infra-url             # show ALB base URL

# Docker + ECS deployment
ecr-push <svc|all>   # build Docker image and push to ECR
ecs-deploy <svc|all>  # force ECS service redeployment
deploy <svc|all>      # ecr-push + ecs-deploy combined

# Secrets and config
pull-secrets          # AWS Secrets Manager -> .envrc

# Prefect
prefect-init          # create work pool + register deployment (prod)
prefect-setup         # create work pool + register deployment (local)

# Status
ecs-status            # show ECS service status
```

## Adding a New Service

1. Add ECR repo in `infrastructure/storage.py`
2. Add ECS task definition + service in `infrastructure/compute.py`
3. Add ALB target group + listener rule in `infrastructure/compute.py`
4. Add service discovery entry in `infrastructure/compute.py`
5. Add log group in `infrastructure/compute.py`
6. Add IAM permissions if needed in `infrastructure/iam.py`
7. Add exports in `infrastructure/__main__.py`
8. Add to `ecsServices` in `devenv.nix`
9. Add a corresponding `processes.*` entry for local dev
10. If it exposes `/metrics`, add a prometheus scrape config

## Key Decisions

- **Fargate only**: no EC2 instances to manage
- **Single ALB**: all services share one ALB, routed by port or path
- **Private subnets**: all ECS tasks run in private subnets, only ALB is public
- **Cloud Map**: internal service-to-service communication via DNS
- **Pulumi Python**: infrastructure as code in the same language as the app
- **No auto-scaling**: desired count fixed at 1 (cost optimization)

## Common Issues

- **ECR login expired**: `aws ecr get-login-password` tokens last 12 hours
- **ECS service stuck**: force new deployment with `ecs-deploy <svc>`
- **Pulumi state lock**: another `pulumi up` is running, wait or cancel
- **NAT Gateway costs**: the NAT Gateway runs 24/7, budget alarm at $25/mo
- **RDS snapshots**: deletion protection is on, disable before `infra-down`

## User Workflow

```bash
# First time
cd infrastructure && pulumi stack init production
pulumi config set aws:region us-east-1
# Set all required Pulumi config values (see Pulumi.production.yaml)
infra-up              # create all AWS resources
pull-secrets && direnv allow
deploy all            # build + push + deploy all services
prefect-init          # register Prefect deployment

# Local development
devenv up             # starts all local services
prefect-setup         # register local Prefect deployment

# Ongoing deployment
deploy myservice      # single service
deploy all            # everything
ecs-status            # check health

# Tear down
infra-down            # destroy all AWS resources
```
