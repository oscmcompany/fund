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

### images

> Manage Docker images for applications

#### build (application_name) (stage_name)

> Build Docker images with optional cache pull

```bash
set -euo pipefail

echo "Building image"

aws_account_id=$(aws sts get-caller-identity --query Account --output text)
aws_region=${AWS_REGION}
if [ -z "$aws_region" ]; then
    echo "AWS_REGION environment variable is not set"
    exit 1
fi

image_reference="${aws_account_id}.dkr.ecr.${aws_region}.amazonaws.com/fund/${application_name}-${stage_name}"
cache_reference="${image_reference}:buildcache"

# Use GHA backend for caching when running in GitHub Actions 
if [ -n "${GITHUB_ACTIONS:-}" ]; then
    scope="${application_name}-${stage_name}"
    echo "Running in GitHub Actions - using hybrid cache (gha + registry) with scope: ${scope}"
    cache_from_arguments="--cache-from type=gha,scope=${scope} --cache-from type=registry,ref=${cache_reference}"
    cache_to_arguments="--cache-to type=gha,scope=${scope},mode=max --cache-to type=registry,ref=${cache_reference},mode=max"
else
    echo "Running locally - using registry cache only"
    cache_from_arguments="--cache-from type=registry,ref=${cache_reference}"
    cache_to_arguments="--cache-to type=registry,ref=${cache_reference},mode=max"
fi

echo "Setting up Docker Buildx"
if [ -n "${GITHUB_ACTIONS:-}" ]; then
    echo "Using buildx builder configured by docker/setup-buildx-action"
else
    docker buildx create --use --name fund-builder 2>/dev/null || docker buildx use fund-builder || (echo "Using default buildx builder" && docker buildx use default)
fi

echo "Logging into ECR (to pull cache if available)"
aws ecr get-login-password --region ${aws_region} | docker login \
    --username AWS \
    --password-stdin ${aws_account_id}.dkr.ecr.${aws_region}.amazonaws.com 2>/dev/null || echo "Could not authenticate to ECR for cache (will build without cache)"

echo "Building with caching (will continue if cache doesn't exist)"
docker buildx build \
    --platform linux/amd64 \
    --target ${stage_name} \
    --file applications/${application_name}/Dockerfile \
    --tag ${image_reference}:latest \
    ${cache_from_arguments} \
    ${cache_to_arguments} \
    --load \
    .

echo "Image built: ${application_name} ${stage_name}"
```

#### push (application_name) (stage_name)

> Push Docker image to ECR

```bash
set -euo pipefail

echo "Pushing image to ECR"

aws_account_id=$(aws sts get-caller-identity --query Account --output text)
aws_region=${AWS_REGION}
if [ -z "$aws_region" ]; then
    echo "AWS_REGION environment variable is not set"
    exit 1
fi

repository_name="fund/${application_name}-${stage_name}"
image_reference="${aws_account_id}.dkr.ecr.${aws_region}.amazonaws.com/${repository_name}"
commit_hash=$(git rev-parse --short HEAD)

echo "Logging into ECR"
aws ecr get-login-password --region ${aws_region} | docker login \
    --username AWS \
    --password-stdin ${aws_account_id}.dkr.ecr.${aws_region}.amazonaws.com > /dev/null

echo "Checking if image for commit ${commit_hash} already exists in ECR"
existing_tag="NONE"
if image_digest=$(aws ecr describe-images \
    --repository-name "${repository_name}" \
    --image-ids "imageTag=git-${commit_hash}" \
    --query 'imageDetails[0].imageDigest' \
    --output text 2>/dev/null); then
    existing_tag="${image_digest}"
fi

if [ "$existing_tag" != "NONE" ] && [ "$existing_tag" != "None" ] && [ -n "$existing_tag" ]; then
    echo "Image for commit ${commit_hash} already exists in ECR, skipping push"
    echo "Image pushed: ${application_name} ${stage_name} (cached)"
    exit 0
fi

echo "Pushing image"
docker tag "${image_reference}:latest" "${image_reference}:git-${commit_hash}"
docker push "${image_reference}:latest"
docker push "${image_reference}:git-${commit_hash}"

echo "Image pushed: ${application_name} ${stage_name} (commit: ${commit_hash})"
```

### stack

> Manage infrastructure stack

#### up

> Launch or update infrastructure stack

```bash
set -euo pipefail

cd infrastructure/

echo "Launching infrastructure"

organization_name=$(pulumi org get-default)

if [ -z "${organization_name}" ]; then
    echo "Unable to determine Pulumi organization name - ensure you are logged in"
    exit 1
fi

pulumi stack select ${organization_name}/fund/production --create

pulumi up --diff --yes

echo "Forcing ECS service deployments to pull latest images"

cluster=$(pulumi stack output aws_ecs_cluster_name --stack production 2>/dev/null || echo "")

if [ -z "$cluster" ]; then
    echo "Cluster not found - skipping service deployments (initial setup)"
else
    # Note: Service names use 'fund' prefix matching the Pulumi project name.
    # These must exactly match the ECS service names created by the infrastructure code.
    # The AWS account provides environment context (one account = one environment).
    for service in fund-datamanager fund-portfoliomanager fund-equitypricemodel; do
        echo "Checking if $service exists and is ready"

        # Wait up to 60 seconds for service to be active
        retry_count=0
        maximum_retries=12
        retry_wait_seconds=5
        service_is_ready=false

        while [ $retry_count -lt $maximum_retries ]; do
            service_status=$(aws ecs describe-services \
                --cluster "$cluster" \
                --services "$service" \
                --query 'services[0].status' \
                --output text 2>/dev/null || echo "NONE")

            if [ "$service_status" = "ACTIVE" ]; then
                service_is_ready=true
                echo "Service $service is ACTIVE"
                break
            elif [ "$service_status" = "NONE" ]; then
                echo "Service not found, waiting ($((retry_count + 1))/$maximum_retries)"
            else
                echo "Service status: $service_status, waiting ($((retry_count + 1))/$maximum_retries)"
            fi

            sleep $retry_wait_seconds
            retry_count=$((retry_count + 1))
        done

        if [ "$service_is_ready" = true ]; then
            echo "Forcing new deployment for $service"
            aws ecs update-service \
                --cluster "$cluster" \
                --service "$service" \
                --force-new-deployment \
                --no-cli-pager \
                --output text > /dev/null 2>&1 && echo "Deployment initiated" || echo "Failed to force deployment"
        else
            echo "Skipping $service (not ready after 60s - may be initial deployment)"
        fi
    done

    echo "Stack update complete - ECS is performing rolling deployments"
    echo "Monitor progress: aws ecs describe-services --cluster $cluster --services fund-portfoliomanager"
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

### services

> Manage infrastructure services

#### invoke (application_name) [date_range]

> Invoke service REST endpoint

```bash
set -euo pipefail

echo "Invoking ${application_name} service"

cd infrastructure/

base_url=$(pulumi stack output fund_base_url --stack production 2>/dev/null || echo "")

if [ -z "$base_url" ]; then
    echo "fund_base_url not found - infrastructure might not be deployed"
    exit 1
fi

case "$application_name" in
    portfoliomanager)
        full_url="${base_url}/portfolio"
        echo "Creating portfolio: $full_url"

        http_code=$(curl -X POST "$full_url" \
            -H "Content-Type: application/json" \
            -w "%{http_code}" \
            -s -o /dev/stderr)

        echo "HTTP Status: $http_code"

        if [ "$http_code" != "200" ]; then
            echo "Expected status code 200, received $http_code"
            exit 1
        fi
        ;;

    datamanager)
        if [ -n "${date_range:-}" ]; then
            cd "${MASKFILE_DIR}"
            uv run python tools/sync_equity_bars_data.py "$base_url" "$date_range"
        else
            current_date=$(date -u +"%Y-%m-%dT00:00:00Z")
            full_url="${base_url}/equity-bars"
            echo "Syncing equity bars: $full_url"

            curl -X POST "$full_url" \
                -H "Content-Type: application/json" \
                -d "{\"date\": \"$current_date\"}" \
                -w "\nHTTP Status: %{http_code}\n" \
                -s
        fi
        ;;

    *)
        echo "Unknown application name: ${application_name}"
        echo "Valid options: portfoliomanager, datamanager"
        exit 1
        ;;
esac
```

## development

> Python and Rust development tools and code quality checks

### rust

> Rust development workflow commands

#### update

> Update Rust dependencies

```bash
set -euo pipefail

echo "Updating Rust dependencies"

cargo update

echo "Rust dependencies updated successfully"
```

#### check

> Check Rust compilation

```bash
set -euo pipefail

echo "Check Rust compilation"

cargo check 

echo "Rust compiled successfully"
```

#### format

> Format Rust code

```bash
set -euo pipefail

echo "Formatting Rust code"

cargo fmt --all

echo "Rust code formatted successfully"
```

#### lint

> Run Rust code quality checks

```bash
set -euo pipefail

echo "Running Rust lint checks"

cargo clippy

echo "Rust linting completed successfully"
```

#### test

> Run Rust tests with coverage reporting

```bash
set -euo pipefail

echo "Running Rust tests with coverage"

echo "Checking Docker availability for integration tests"
if [[ "${RUN_INTEGRATION_TESTS:-0}" == "1" ]]; then
    echo "RUN_INTEGRATION_TESTS=1 - enforcing Docker availability"
    if command -v docker >/dev/null 2>&1; then
        if ! docker info >/dev/null 2>&1; then
            echo "Error: Docker is installed but daemon is not running"
            echo "Integration tests requiring Docker will fail"
            echo "Start Docker with: open -a Docker (macOS) or sudo systemctl start docker (Linux)"
            exit 1
        fi
        echo "Docker daemon is running"
    else
        echo "Error: Docker is not installed"
        echo "Integration tests requiring Docker will fail"
        echo "Install Docker from: https://docs.docker.com/get-docker/"
        exit 1
    fi
else
    if command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1; then
        echo "Docker is available - integration tests can run"
    else
        echo "Warning: Docker is not available or daemon is not running"
        echo "Integration tests requiring Docker may be skipped or fail"
        echo "To enforce Docker for integration tests, set RUN_INTEGRATION_TESTS=1"
    fi
fi

mkdir -p .coverage_output

if ! command -v cargo-llvm-cov >/dev/null 2>&1; then
    echo "cargo-llvm-cov not available - running tests without coverage"
    cargo test --workspace --verbose
elif ! command -v llvm-cov >/dev/null 2>&1 || ! command -v llvm-profdata >/dev/null 2>&1; then
    echo "LLVM tools (llvm-cov or llvm-profdata) not available - running tests without coverage"
    cargo test --workspace --verbose
else
    export LLVM_COV=$(which llvm-cov)
    export LLVM_PROFDATA=$(which llvm-profdata)
    if cargo llvm-cov --workspace --verbose \
        --cobertura \
        --output-path .coverage_output/rust.xml; then
        echo "Rust tests with coverage completed successfully"
    else
        echo "cargo llvm-cov failed - check test output above"
        exit 1
    fi
fi
```

#### all

> Full Rust development checks

```bash
set -euo pipefail

echo "Running Rust development checks"

# mask development rust update # Temporarily removing for continuous integration speed

mask development rust check

mask development rust format

mask development rust lint

mask development rust test

echo "Rust development checks completed successfully"
```

### python

> Python development workflow commands

#### install

> Install Python dependencies

```bash
set -euo pipefail

echo "Installing Python dependencies"

uv sync --all-packages --all-groups

echo "Python dependencies installed successfully"
```

#### format

> Format Python code

```bash
set -euo pipefail

echo "Formatting Python code"

ruff format

echo "Python code formatted successfully"
```

#### dead-code

> Check for dead Python code

```bash
set -euo pipefail

echo "Running dead code analysis"

uvx vulture \
    --min-confidence 80 \
    --exclude '.flox,.venv,target' \
    . tools/vulture_whitelist.py

echo "Dead code check completed"
```

#### lint

> Run comprehensive Python code quality checks

```bash
set -euo pipefail

echo "Running Python lint checks"

ruff check \
    --output-format=github \
    .

echo "Python linting completed successfully"
```

#### type-check

> Run Python type checks

```bash
set -euo pipefail

echo "Running Python type checks"

uvx ty check
```

#### test

> Run Python tests using coverage reporting

```bash
set -euo pipefail

echo "Running Python tests with coverage"

mkdir -p .coverage_output

uv run coverage run --parallel-mode -m pytest \
    && uv run coverage combine \
    && uv run coverage report \
    && uv run coverage xml -o .coverage_output/python.xml

echo "Python tests completed successfully"
```

#### all

> Full Python development checks

```bash
set -euo pipefail

echo "Running Python development checks"

mask development python install

mask development python format

mask development python lint

mask development python type-check

mask development python dead-code

mask development python test

echo "Python development checks completed successfully"
```

### markdown

> Markdown development workflow commands

#### lint

> Run Markdown lint checks

```bash
set -euo pipefail

echo "Running Markdown lint checks"

markdownlint "**/*.md" --ignore ".flox" --ignore ".venv" --ignore "target"

echo "Markdown linting completed successfully"
```

#### all

> Full Markdown development checks

```bash
set -euo pipefail

echo "Running Markdown development checks"

mask development markdown lint

echo "Markdown development checks completed successfully"
```

## data

> Data management commands

### sync

> Update data in cloud storage

#### equity (data_type)

> Sync equity data to S3

```bash
set -euo pipefail

echo "Syncing equity data: ${data_type}"

cd infrastructure
export AWS_S3_DATA_BUCKET="$(pulumi stack output aws_s3_data_bucket)"

cd ../

if [ "${data_type}" = "categories" ]; then
    echo "Syncing equity categories"

    export MASSIVE_API_KEY=$(aws secretsmanager get-secret-value \
        --secret-id fund/production/datamanager \
        --query 'SecretString' \
        --output text | jq -r '.MASSIVE_API_KEY')

    uv run python tools/sync_equity_categories.py
else
    echo "Unknown data type: ${data_type}"
    echo "Valid options: categories"
    exit 1
fi

echo "Equity data sync complete"
```

## models

> Model management commands

### prepare (application_name)

> Prepare training data by consolidating equity bars with categories

```bash
set -euo pipefail

export APPLICATION_NAME="${application_name}"

cd infrastructure
export AWS_S3_DATA_BUCKET="$(pulumi stack output aws_s3_data_bucket)"
export AWS_S3_MODEL_ARTIFACTS_BUCKET="$(pulumi stack output aws_s3_model_artifacts_bucket)"
export LOOKBACK_DAYS="${LOOKBACK_DAYS:-365}"

cd ../

uv run python tools/prepare_training_data.py
```

### train (application_name) [instance_preset]

> Train model on SageMaker. Presets: testing, standard (default), performance, or custom instance type

```bash
set -euo pipefail

export APPLICATION_NAME="${application_name}"

preset="${instance_preset:-standard}"

case "$preset" in
    testing)
        instance_type="ml.t3.xlarge"
        echo "================================================"
        echo "Training with TESTING architecture (CPU)"
        echo "Instance: ml.t3.xlarge (~\$0.23/hr)"
        echo "Use for: Quick iteration, debugging"
        echo "================================================"
        ;;
    standard)
        instance_type="ml.g5.xlarge"
        echo "================================================"
        echo "Training with STANDARD architecture (GPU)"
        echo "Instance: ml.g5.xlarge - 1x A10G (~\$1.41/hr)"
        echo "Use for: Regular training runs"
        echo "================================================"
        ;;
    performance)
        instance_type="ml.p3.2xlarge"
        echo "================================================"
        echo "Training with PERFORMANCE architecture (GPU)"
        echo "Instance: ml.p3.2xlarge - 1x V100 (~\$3.82/hr)"
        echo "Use for: Large datasets, faster training"
        echo "================================================"
        ;;
    ml.*)
        instance_type="$preset"
        echo "================================================"
        echo "Training with CUSTOM architecture"
        echo "Instance: ${instance_type}"
        echo "================================================"
        ;;
    *)
        echo "Unknown preset: $preset"
        echo ""
        echo "Available presets:"
        echo "testing     - ml.t3.xlarge (CPU, ~\$0.23/hr)"
        echo "standard    - ml.g5.xlarge (GPU, ~\$1.41/hr) [default]"
        echo "performance - ml.p3.2xlarge (GPU, ~\$3.82/hr)"
        echo ""
        echo "Or specify a custom instance type: ml.g4dn.xlarge"
        exit 1
        ;;
esac

export SAGEMAKER_INSTANCE_TYPE="${instance_type}"

cd infrastructure
export AWS_ECR_EQUITY_PRICE_MODEL_TRAINER_IMAGE_ARN="$(pulumi stack output aws_ecr_equitypricemodel_trainer_image)"
export AWS_IAM_SAGEMAKER_ROLE_ARN="$(pulumi stack output aws_iam_sagemaker_role_arn)"
export AWS_S3_MODEL_ARTIFACTS_BUCKET="$(pulumi stack output aws_s3_model_artifacts_bucket)"
export AWS_S3_EQUITY_PRICE_MODEL_ARTIFACT_OUTPUT_PATH="s3://${AWS_S3_MODEL_ARTIFACTS_BUCKET}/artifacts"
export AWS_S3_EQUITY_PRICE_MODEL_TRAINING_DATA_PATH="s3://${AWS_S3_MODEL_ARTIFACTS_BUCKET}/training"

cd ../

uv run python tools/run_training_job.py
```

### artifacts

> Manage model artifacts

#### download (application_name)

> Download model artifacts

```bash
set -euo pipefail

export APPLICATION_NAME="${application_name}"

uv run python tools/download_model_artifacts.py
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
