---
name: devenv-deploy
description: >
  Set up and manage devenv + AWS infrastructure from devenv.nix and Pulumi.
  Covers both local development (devenv up) and production deployment to AWS
  ECS/Fargate via Pulumi. Training orchestration uses Prefect Cloud with an
  ECS work pool. Use when the user wants to deploy services, manage
  infrastructure, push Docker images to ECR, update ECS services, manage
  Pulumi stacks, add monitoring/metrics, configure deploy commands, or set
  up local development services. Triggers on requests like "deploy to aws",
  "set up production", "devenv deploy", "devenv up", "add a local service",
  "deploy failed", "deploy errors", "fix deploy", "ecs logs", "deploy all",
  "infra up", "pulumi up", or infrastructure setup involving devenv and AWS.
---

# devenv + AWS Infrastructure

`devenv.nix` drives local development (`devenv up`). Pulumi (`infrastructure/`)
manages AWS production infrastructure. Training orchestration uses Prefect Cloud.

## Architecture

```
devenv.nix (local development)
├── processes.prefect-worker (local Prefect worker)
├── tasks.models:tide:deploy (register deployments)
├── tasks.models:tide:train (trigger ECS training)
├── tasks.models:tide:train:local (trigger local training)
└── env.* (localhost URLs, dev credentials)

infrastructure/ (AWS production via Pulumi)
├── __main__.py (exports)
├── compute.py (ECS services, ALB)
├── networking.py (VPC, subnets, security groups)
├── storage.py (ECR repos, S3 buckets)
├── iam.py (roles, policies, OIDC)
├── secrets.py (Secrets Manager)
├── parameters.py (SSM parameters)
└── notifications.py (SNS, budgets, SES)

Prefect Cloud (training orchestration)
├── fund-work-pool-ecs (production training on ECS Fargate)
├── fund-work-pool-local (local dev training)
├── S3Bucket blocks (data-bucket, artifact-bucket)
└── AwsCredentials block (fund-aws)
```

## AWS Services

ECS Fargate services on a single ALB:
| Service | ALB Route | Health Check | CPU/Memory |
|---------|-----------|-------------|------------|
| data-manager | path: `/equity-bars*`, etc. | `/health` | 256/512 |
| ensemble-manager | path: `/model/*` | `/health` | 256/512 |
| portfolio-manager | path: `/portfolio*` | `/health` | 256/512 |

Training runs via Prefect Cloud ECS work pool (not a persistent ECS service).

Supporting infrastructure:
- VPC with public/private subnets across 2 AZs
- NAT Gateway for private subnet egress
- S3 buckets: fund-data, fund-model-artifacts
- ECR repos for all services + tide-runner
- Cloud Map service discovery (fund.local namespace)
- Secrets Manager for app secrets
- SSM Parameter Store for config

## Training Deployment

Training uses Prefect Cloud with two work pools:

- **fund-work-pool-ecs**: Production training on ECS Fargate (4096 CPU, 8192 MB)
  - Image: `<account>.dkr.ecr.us-east-1.amazonaws.com/fund/tide-runner:worker`
  - Configured in Prefect Cloud UI
- **fund-work-pool-local**: Local development training via process worker
  - Started by `devenv up` (runs `prefect worker start`)

Deployments defined in `prefect.yaml`:
- `tide-training`: ECS pool, pulls code via git clone
- `tide-training-local`: Local pool, uses working directory

## Deployment Commands (devenv tasks)

```bash
# Infrastructure lifecycle (from infrastructure/ directory)
mask infrastructure stack up    # pulumi up
mask infrastructure stack down  # pulumi down

# Docker + ECS deployment
mask infrastructure image build <pkg> <stage>   # build Docker image
mask infrastructure image push <pkg> <stage>    # push to ECR
mask infrastructure image deploy <pkg> <stage>  # force ECS redeployment

# Training
devenv tasks run models:tide:deploy       # register Prefect deployments
devenv tasks run models:tide:train        # trigger ECS training run
devenv tasks run models:tide:train:local  # trigger local training run

# Local development
devenv up                                 # start local Prefect worker
```

## Adding a New Service

1. Add ECR repo in `infrastructure/storage.py`
2. Add ECS task definition + service in `infrastructure/compute.py`
3. Add ALB target group + listener rule in `infrastructure/compute.py`
4. Add service discovery entry in `infrastructure/compute.py`
5. Add log group in `infrastructure/compute.py`
6. Add IAM permissions if needed in `infrastructure/iam.py`
7. Add exports in `infrastructure/__main__.py`

## Key Decisions

- **Fargate only**: no EC2 instances to manage
- **Single ALB**: all services share one ALB, routed by path
- **Private subnets**: all ECS tasks run in private subnets, only ALB is public
- **Cloud Map**: internal service-to-service communication via DNS
- **Prefect Cloud**: training orchestration managed externally, not self-hosted
- **ECS work pool**: training runs on-demand ECS tasks, not persistent services
- **Pulumi Python**: infrastructure as code in the same language as the app
- **No auto-scaling**: desired count fixed at 1 (cost optimization)

## Common Issues

- **ECR login expired**: `aws ecr get-login-password` tokens last 12 hours
- **ECS service stuck**: force new deployment with `mask infrastructure image deploy`
- **Pulumi state lock**: another `pulumi up` is running, wait or cancel
- **NAT Gateway costs**: runs 24/7, budget alarm set
- **Prefect Cloud auth**: ensure PREFECT_API_URL in .envrc points to Cloud, not localhost
