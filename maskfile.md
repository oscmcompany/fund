# OSCM Task Manager

## setup

> Initial system setup and prerequisite configuration

```bash
set -euo pipefail

echo "Setting up development environment"

echo "Checking prerequisites"
missing_deps=()

if ! command -v docker >/dev/null >&1; then
    missing_deps+=("Docker")
fi

if [[ ${#missing_deps[@]} -gt 0 ]]; then
    echo "Missing prerequisites: ${missing_deps[*]}"
    echo "Please install the following:"
    for dep in "${missing_deps[@]}"; do
        case $dep in
            "Docker")
                echo "- Docker: https://docs.docker.com/get-docker/"
                ;;
        esac
    done
    exit 1
fi

echo "Prerequisites check completed"

echo "Configuring GitHub CLI"

if ! gh auth status >/dev/null 2>&1; then
    echo "GitHub CLI not authenticated"
    echo "Run 'gh auth login' before setup"
    exit 1
fi

echo "GitHub CLI configuration completed"

echo "Development environment setup completed successfully"
```

## infrastructure

> Manage infrastructure resources

### image

> Manage Docker images for applications

#### build (package_name) (stage_name)

> Build Docker image, optionally pushing to ECR (e.g. `portfolio-manager server`, `tide runner`)

<!-- markdownlint-disable MD036 MD032 MD007 -->
**OPTIONS**
* push
    * flags: --push
    * type: boolean
    * desc: Push image to ECR after building
<!-- markdownlint-enable MD036 MD032 MD007 -->

```bash
set -euo pipefail

PUSH="${push:-"false"}"
if [ -n "${push+x}" ] && [ -z "$push" ]; then
    PUSH="true"
fi

if [ -f "models/${package_name}/Dockerfile" ]; then
    dockerfile="models/${package_name}/Dockerfile"
    build_target="${stage_name}"
    namespace="models"
else
    resolved_name=$(echo "${package_name}" | tr '-' '_')
    dockerfile="applications/${resolved_name}/Dockerfile"
    build_target="${stage_name}"
    namespace="applications"
fi

echo "Setting up Docker Buildx"
if [ -n "${GITHUB_ACTIONS:-}" ]; then
    echo "Using buildx builder configured by docker/setup-buildx-action"
else
    docker buildx create --use --name fund-builder 2>/dev/null || docker buildx use fund-builder || (echo "Using default buildx builder" && docker buildx use default)
fi

if [ "$PUSH" == "true" ]; then
    echo "Building and pushing image"

    aws_account_id=$(aws sts get-caller-identity --query Account --output text)
    aws_region="${AWS_REGION:-}"
    if [ -z "$aws_region" ]; then
        echo "AWS_REGION environment variable is not set"
        exit 1
    fi

    commit_hash=$(git rev-parse --short HEAD)

    repository_name="fund/${namespace}-${package_name}-${stage_name}"
    image_reference="${aws_account_id}.dkr.ecr.${aws_region}.amazonaws.com/${repository_name}"

    echo "Logging into ECR"
    aws ecr get-login-password --region ${aws_region} | docker login \
        --username AWS \
        --password-stdin ${aws_account_id}.dkr.ecr.${aws_region}.amazonaws.com 2>/dev/null || echo "Could not authenticate to ECR (will build without cache)"

    echo "Checking if image for commit ${commit_hash} already exists in ECR"
    existing_image=$(aws ecr describe-images \
        --repository-name "${repository_name}" \
        --image-ids "imageTag=git-${commit_hash}" \
        --query 'imageDetails[0].imageDigest' \
        --output text 2>/dev/null || echo "NONE")
    if [ "$existing_image" != "NONE" ] && [ "$existing_image" != "None" ] && [ -n "$existing_image" ]; then
        echo "Image for commit ${commit_hash} already exists in ECR, skipping build"
        exit 0
    fi

    cache_reference="${image_reference}:buildcache"

    # Use GHA backend for caching when running in GitHub Actions
    if [ -n "${GITHUB_ACTIONS:-}" ]; then
        scope="${namespace}-${package_name}-${stage_name}"
        echo "Running in GitHub Actions - using hybrid cache (gha + registry) with scope: ${scope}"
        cache_from_arguments="--cache-from type=gha,scope=${scope} --cache-from type=registry,ref=${cache_reference}"
        cache_to_arguments="--cache-to type=gha,scope=${scope},mode=max --cache-to type=registry,ref=${cache_reference},mode=max"
    else
        echo "Running locally - using registry cache only"
        cache_from_arguments="--cache-from type=registry,ref=${cache_reference}"
        cache_to_arguments="--cache-to type=registry,ref=${cache_reference},mode=max"
    fi

    docker buildx build \
        --platform linux/amd64 \
        --target ${build_target} \
        --file ${dockerfile} \
        --tag ${image_reference}:latest \
        --tag ${image_reference}:git-${commit_hash} \
        ${cache_from_arguments} \
        ${cache_to_arguments} \
        --push \
        .

    echo "Image built and pushed: ${package_name} ${stage_name} (commit: ${commit_hash})"
else
    echo "Checking image build"

    if [ -n "${GITHUB_ACTIONS:-}" ]; then
        scope="${namespace}-${package_name}-${stage_name}"
        echo "Running in GitHub Actions - using GHA cache with scope: ${scope}"
        cache_arguments="--cache-from type=gha,scope=${scope}"
    else
        echo "Running locally - no cache"
        cache_arguments=""
    fi

    docker buildx build \
        --platform linux/amd64 \
        --target ${build_target} \
        --file ${dockerfile} \
        ${cache_arguments} \
        .

    echo "Image build check passed: ${package_name} ${stage_name}"
fi
```

### stack

> Manage infrastructure stack

#### up

> Launch or update infrastructure stack

<!-- markdownlint-disable MD036 MD032 MD007 -->
**OPTIONS**
* bootstrap
    * flags: --bootstrap
    * type: boolean
    * desc: Run optional bootstrap commands with stack update (e.g. for initial setup from local machine)
<!-- markdownlint-enable MD036 MD032 MD007 -->

```bash
set -euo pipefail

BOOTSTRAP="${bootstrap:-"false"}"
if [ -n "${bootstrap+x}" ] && [ -z "$bootstrap" ]; then
    BOOTSTRAP="true"
fi

cd infrastructure/

echo "Launching infrastructure"

if ! organization_name=$(pulumi org get-default 2>/dev/null) || [ -z "${organization_name}" ]; then
    echo "Unable to determine Pulumi organization name - ensure you are logged in"
    exit 1
fi

pulumi stack select ${organization_name}/fund/production --create

if ! pulumi config get fund:randomSuffix >/dev/null 2>&1; then
  pulumi config set --secret fund:randomSuffix "$(openssl rand -hex 4)"
fi

RANDOM_SUFFIX=$(pulumi config get fund:randomSuffix)

echo "Importing existing resources into Pulumi state (if they exist)"

pulumi import --yes --generate-code=false aws:iam/role:Role github_actions_infrastructure_role fund-github-actions-infrastructure-role 2>/dev/null || true

GITHUB_POLICY_ARN=$(aws iam list-policies --scope Local --query 'Policies[?PolicyName==`fund-github-actions-infrastructure-policy`].Arn' --output text 2>/dev/null || echo "")
if [ -n "$GITHUB_POLICY_ARN" ]; then
  pulumi import --yes --generate-code=false aws:iam/policy:Policy github_actions_infrastructure_policy "$GITHUB_POLICY_ARN" 2>/dev/null || true
fi

pulumi import --yes --generate-code=false aws:s3/bucket:Bucket data_bucket "fund-data-${RANDOM_SUFFIX}" 2>/dev/null || true
pulumi import --yes --generate-code=false aws:s3/bucketServerSideEncryptionConfiguration:BucketServerSideEncryptionConfiguration data_bucket_encryption "fund-data-${RANDOM_SUFFIX}" 2>/dev/null || true
pulumi import --yes --generate-code=false aws:s3/bucketPublicAccessBlock:BucketPublicAccessBlock data_bucket_public_access_block "fund-data-${RANDOM_SUFFIX}" 2>/dev/null || true
pulumi import --yes --generate-code=false aws:s3/bucketVersioning:BucketVersioning data_bucket_versioning "fund-data-${RANDOM_SUFFIX}" 2>/dev/null || true

pulumi import --yes --generate-code=false aws:s3/bucket:Bucket model_artifacts_bucket "fund-model-artifacts-${RANDOM_SUFFIX}" 2>/dev/null || true
pulumi import --yes --generate-code=false aws:s3/bucketServerSideEncryptionConfiguration:BucketServerSideEncryptionConfiguration model_artifacts_bucket_encryption "fund-model-artifacts-${RANDOM_SUFFIX}" 2>/dev/null || true
pulumi import --yes --generate-code=false aws:s3/bucketPublicAccessBlock:BucketPublicAccessBlock model_artifacts_bucket_public_access_block "fund-model-artifacts-${RANDOM_SUFFIX}" 2>/dev/null || true
pulumi import --yes --generate-code=false aws:s3/bucketVersioning:BucketVersioning model_artifacts_bucket_versioning "fund-model-artifacts-${RANDOM_SUFFIX}" 2>/dev/null || true

echo "Importing resources complete"

pulumi up --diff --yes

if [[ "$BOOTSTRAP" == "true" ]]; then
  echo "Configuring GitHub Actions environment"

  if ! gh auth status >/dev/null 2>&1; then
    echo "Warning: GitHub CLI not authenticated - skipping GitHub environment setup"
    echo "Run 'gh auth login' and re-run with --bootstrap to configure GitHub Actions"
  else
    echo "Setting GitHub environment secrets for pulumi environment"

    role_arn=$(pulumi stack output aws_iam_github_actions_infrastructure_role_arn --stack production)
    region=$(pulumi config get aws:region --stack production)
    artifacts_bucket=$(pulumi stack output aws_s3_model_artifacts_bucket_name --stack production)
    data_bucket=$(pulumi stack output aws_s3_data_bucket_name --stack production)

    gh secret set AWS_IAM_INFRASTRUCTURE_ROLE_ARN --env pulumi --body "$role_arn"
    gh secret set AWS_REGION --env pulumi --body "$region"
    gh secret set AWS_S3_MODEL_ARTIFACTS_BUCKET_NAME --env pulumi --body "$artifacts_bucket"
    gh secret set AWS_S3_DATA_BUCKET_NAME --env pulumi --body "$data_bucket"

    echo "GitHub environment secrets updated successfully"
    echo "  - AWS_IAM_INFRASTRUCTURE_ROLE_ARN"
    echo "  - AWS_REGION"
    echo "  - AWS_S3_MODEL_ARTIFACTS_BUCKET_NAME"
    echo "  - AWS_S3_DATA_BUCKET_NAME"
    echo ""
    echo "Note: PULUMI_ACCESS_TOKEN must be set manually"
    echo "Generate token at: https://app.pulumi.com/account/tokens"
    echo "Then run: gh secret set PULUMI_ACCESS_TOKEN --env pulumi --body \"<your-token>\""
  fi
fi

echo "Infrastructure launched successfully"
```

#### down

> Teardown infrastructure stack

```bash
set -euo pipefail

echo "Tearing down infrastructure"

cd infrastructure/

pulumi down --yes --stack production

echo "Infrastructure torn down successfully"
```

### service

> Manage infrastructure services

#### invoke (application_name)

> Invoke service REST endpoint

<!-- markdownlint-disable MD036 MD032 MD007 -->
**OPTIONS**
* date_range
    * flags: --date-range
    * type: string
    * desc: Date range JSON for equity bars sync, e.g. '{"start_date": "2025-01-01", "end_date": "2025-01-01"}'
* data_type
    * flags: --data-type
    * type: string
    * choices: equity-bars, equity-details
    * desc: Data type to sync for datamanager
<!-- markdownlint-enable MD036 MD032 MD007 -->

```bash
set -euo pipefail

echo "Invoking ${application_name} service"

cd infrastructure/

base_url=$(pulumi stack output fund_base_url --stack production 2>/dev/null || echo "")

if [ -z "$base_url" ]; then
    echo "fund_base_url not found - infrastructure might not be deployed"
    exit 1
fi

cd "${MASKFILE_DIR}"

case "$application_name" in
    portfolio-manager)
        full_url="${base_url}/portfolio"
        echo "Creating portfolio: $full_url"

        http_code=$(curl --request POST "$full_url" \
            --header "Content-Type: application/json" \
            --write-out "%{http_code}" \
            --output /dev/stderr)

        echo "HTTP Status: $http_code"

        if [ "$http_code" != "200" ]; then
            echo "Expected status code 200, received $http_code"
            exit 1
        fi
        ;;

    data-manager)
        if [ -z "${data_type:-}" ]; then
            echo "Missing required flag: --data-type"
            echo "Valid choices: equity-bars, equity-details"
            exit 1
        fi

        if [ "$data_type" = "equity-bars" ]; then
            if [ -n "${date_range:-}" ]; then
                uv run python -m tools.sync_equity_bars_data "$base_url" "$date_range"
            else
                current_date=$(date -u +"%Y-%m-%d")
                date_range_json="{\"start_date\": \"$current_date\", \"end_date\": \"$current_date\"}"
                uv run python -m tools.sync_equity_bars_data "$base_url" "$date_range_json"
            fi
        elif [ "$data_type" = "equity-details" ]; then
            uv run python -m tools.sync_equity_details_data "$base_url"
        fi
        ;;

    *)
        echo "Unknown application name: ${application_name}"
        echo "Valid options: portfolio-manager, data-manager"
        exit 1
        ;;
esac
```

#### update (service_name)

> Force redeploy an ECS service with the latest image (e.g. `ensemble-manager`)

```bash
set -euo pipefail

echo "Updating ${service_name} service"

cd infrastructure/

if ! organization_name=$(pulumi org get-default 2>/dev/null) || [ -z "${organization_name}" ]; then
    echo "Error: Pulumi default organization not set. Run: pulumi org set-default <organization>"
    exit 1
fi
pulumi stack select "${organization_name}/fund/production"
cluster=$(pulumi stack output aws_ecs_cluster_name)

cd "${MASKFILE_DIR}"

case "${service_name}" in
    data-manager|portfolio-manager|ensemble-manager) ;;
    *)
        echo "Unknown service: ${service_name}"
        echo "Valid options: data-manager, portfolio-manager, ensemble-manager"
        exit 1
        ;;
esac

service="fund-${service_name}-server"

aws ecs update-service --cluster "$cluster" --service "$service" --force-new-deployment --no-cli-pager > /dev/null
echo "Deployment started: ${service}"

echo "Waiting for ${service} to stabilize"
aws ecs wait services-stable --cluster "$cluster" --services "$service"
echo "Deployment complete: ${service}"
```

### trainer

> Manage Prefect Cloud model training resources

#### initialize (environment)

> Create work pool and register deployments (environment: remote, local)

```bash
set -euo pipefail

case "${environment}" in
    remote)
        unset PREFECT_API_URL

        cd infrastructure/
        pulumi stack select "$(pulumi org get-default)/fund/production"
        models_cluster=$(pulumi stack output aws_ecs_models_cluster_name)
        tide_trainer_task_definition_arn=$(pulumi stack output aws_ecs_tide_trainer_task_definition_arn)
        vpc_id=$(pulumi stack output aws_vpc_id)
        private_subnet_1_id=$(pulumi stack output aws_ecs_private_subnet_1_id)
        private_subnet_2_id=$(pulumi stack output aws_ecs_private_subnet_2_id)
        ecs_security_group_id=$(pulumi stack output aws_ecs_security_group_id)
        cd "${MASKFILE_DIR}"

        echo "Creating fund-models-remote work pool on Prefect Cloud"
        aws_credentials_block_id=$(uv run prefect block inspect "aws-credentials/fund-aws" | grep "Block id" | grep -oE '[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}')
        base_job_template=$(uv run prefect work-pool get-default-base-job-template --type ecs \
            | uv run python -m tools.build_work_pool_template \
                "${models_cluster}" \
                "${aws_credentials_block_id}" \
                "${tide_trainer_task_definition_arn}" \
                "${vpc_id}" \
                "${private_subnet_1_id}" \
                "${private_subnet_2_id}" \
                "${ecs_security_group_id}")
        uv run prefect work-pool create "fund-models-remote" --type ecs \
            --base-job-template <(echo "${base_job_template}") 2>/dev/null \
            || uv run prefect work-pool update "fund-models-remote" \
                --base-job-template <(echo "${base_job_template}")

        echo ""
        echo "Done. Visit Prefect Cloud dashboard to view deployments."
        ;;
    local)
        export PREFECT_API_URL="http://localhost:4200/api"

        echo "Creating fund-models-local work pool"
        uv run prefect work-pool create "fund-models-local" --type process 2>/dev/null \
            || echo "  already exists"

        echo "Registering local training deployment"
        uv run prefect --no-prompt deploy --name tide-trainer-local
        ;;
    *)
        echo "Unknown environment: ${environment}"
        echo "Valid options: remote, local"
        exit 1
        ;;
esac
```

## model

> Model management commands

### train (model_name)

> Trigger model training run via Prefect Cloud deployment

```bash
set -euo pipefail

unset PREFECT_API_URL

lookback_days="${FUND_LOOKBACK_DAYS:-365}"

case "${model_name}" in
    tide)
        deployment="tide-training-pipeline/tide-trainer-remote"
        log_group="/ecs/fund/models"
        log_stream_prefix="tide/prefect"
        ;;
    *)
        echo "Unknown model: ${model_name}"
        echo "Valid options: tide"
        exit 1
        ;;
esac

echo "Triggering training run for ${model_name} (lookback_days=${lookback_days})"
uv run prefect deployment run "${deployment}" --param "lookback_days=${lookback_days}"

echo ""
echo "To find logs once the run starts (GPU provisioning takes ~3-5 minutes):"
echo "  1. Open the flow run in Prefect Cloud and note the ECS task ARN under Infrastructure"
echo "  2. The task ID is the last segment of the ARN (after the final '/')"
echo "  3. Find the log stream in CloudWatch log group '${log_group}':"
echo "     ${log_stream_prefix}/<task-id>"
```

### deploy (model_name)

> Register flow deployment with Prefect Cloud

```bash
set -euo pipefail

echo "Deploying ${model_name} model"

unset PREFECT_API_URL
export FUND_LOOKBACK_DAYS="${FUND_LOOKBACK_DAYS:-365}"

cd infrastructure

if ! organization_name=$(pulumi org get-default 2>/dev/null) || [ -z "${organization_name}" ]; then
    echo "Unable to determine Pulumi organization name - ensure you are logged in"
    exit 1
fi

pulumi stack select "${organization_name}/fund/production"
tide_image_uri=$(pulumi stack output aws_ecr_models_tide_runner_image)

cd "${MASKFILE_DIR}"

case "${model_name}" in
    tide)
        export FUND_TIDE_IMAGE_URI="${tide_image_uri}"
        uv run python -m tide.deploy
        ;;
    *)
        echo "Unknown model: ${model_name}"
        exit 1
        ;;
esac

echo "Deployment complete: ${model_name}"
```

### download (model_name) [run_id]

> Download model artifacts
>
> Optionally pass a run_id to skip interactive selection and download a specific run.

```bash
set -euo pipefail

case "${model_name}" in
    tide)
        export APPLICATION_NAME="${model_name}"
        export FUND_ARTIFACT_RUN_ID="${run_id:-}"
        uv run python -m tools.download_model_artifacts
        ;;
    *)
        echo "Unknown model: ${model_name}"
        exit 1
        ;;
esac
```

## mcp

> MCP server management

### setup

> Set up MCP servers for Claude Code

```bash
set -euo pipefail

need() { command -v "$1" >/dev/null 2>&1 || { echo "Missing required command: $1" >&2; exit 1; }; }

need claude
need uvx

: "${AWS_PROFILE:?AWS_PROFILE is not set}"
: "${AWS_REGION:?AWS_REGION is not set}"

UVX_PATH="$(command -v uvx)"

detect_uvx_exe() {
  local pkg="$1"

  # AWS Labs MCP servers follow a predictable naming convention:
  # awslabs.ecs-mcp-server -> awslabs_ecs_mcp_server
  # awslabs.cloudwatch-mcp-server -> awslabs_cloudwatch_mcp_server
  local base_name="${pkg%@*}"  # Remove @latest suffix
  local exe_name="${base_name//./_}"  # Replace . with _
  exe_name="${exe_name//-/_}"  # Replace - with _

  echo "$exe_name"
}

remove_if_present() {
  local name="$1"
  claude mcp remove "$name" >/dev/null 2>&1 || true
}

echo "Removing existing MCP entries (ignore errors if not present)"
remove_if_present awslabs-ecs-mcp-server
remove_if_present awslabs-cloudwatch-logs-mcp-server
remove_if_present awslabs-cloudwatch-mcp-server

ECS_PKG="awslabs.ecs-mcp-server@latest"
CW_PKG="awslabs.cloudwatch-mcp-server@latest"

echo "Detecting uvx executable for ECS package: $ECS_PKG"
ECS_EXE="$(detect_uvx_exe "$ECS_PKG")"
echo "-> ECS executable: $ECS_EXE"

echo "Detecting uvx executable for CloudWatch package: $CW_PKG"
CW_EXE="$(detect_uvx_exe "$CW_PKG")"
echo "-> CloudWatch executable: $CW_EXE"

echo "Adding ECS MCP server"
claude mcp add awslabs-ecs-mcp-server -s project \
  -e FASTMCP_LOG_LEVEL="${FASTMCP_LOG_LEVEL:-info}" \
  -e AWS_PROFILE="$AWS_PROFILE" \
  -e AWS_REGION="$AWS_REGION" \
  -- "$UVX_PATH" --from "$ECS_PKG" "$ECS_EXE"

echo "Adding CloudWatch MCP server"
claude mcp add awslabs-cloudwatch-mcp-server -s project \
  -e FASTMCP_LOG_LEVEL="${FASTMCP_LOG_LEVEL:-info}" \
  -e AWS_PROFILE="$AWS_PROFILE" \
  -e AWS_REGION="$AWS_REGION" \
  -- "$UVX_PATH" --from "$CW_PKG" "$CW_EXE"

echo
echo "Done. Current MCP status:"
claude mcp list
```
