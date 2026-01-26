# Pocket Size Fund Task Manager

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
                echo "  - Docker: https://docs.docker.com/get-docker/"
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

image_reference="${aws_account_id}.dkr.ecr.${aws_region}.amazonaws.com/oscmcompany/${application_name}-${stage_name}"
cache_reference="${image_reference}:buildcache"

# Use GHA backend for Cargo caching when running in GitHub Actions 
if [ -n "${GITHUB_ACTIONS:-}" ]; then
    echo "Running in GitHub Actions - using hybrid cache (gha + registry)"
    cache_from_arguments="--cache-from type=gha --cache-from type=registry,ref=${cache_reference}"
    cache_to_arguments="--cache-to type=gha,mode=max --cache-to type=registry,ref=${cache_reference},mode=max"
else
    echo "Running locally - using registry cache only"
    cache_from_arguments="--cache-from type=registry,ref=${cache_reference}"
    cache_to_arguments="--cache-to type=registry,ref=${cache_reference},mode=max"
fi

echo "Setting up Docker Buildx"
docker buildx create --use --name psf-builder 2>/dev/null || docker buildx use psf-builder || (echo "Using default buildx builder" && docker buildx use default)

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

image_reference="${aws_account_id}.dkr.ecr.${aws_region}.amazonaws.com/oscmcompany/${application_name}-${stage_name}"

echo "Logging into ECR"
aws ecr get-login-password --region ${aws_region} | docker login \
    --username AWS \
    --password-stdin ${aws_account_id}.dkr.ecr.${aws_region}.amazonaws.com > /dev/null

echo "Pushing image"
docker push ${image_reference}:latest

echo "Image pushed: ${application_name} ${stage_name}"
```

### stack

> Manage infrastructure stack

#### up

> Launch or update infrastructure stack

```bash
set -euo pipefail

cd infrastructure/

echo "Launching infrastructure"

organization_name=$(pulumi whoami)

if [ -z "${organization_name}" ]; then
    echo "Unable to determine Pulumi organization name - ensure you are logged in"
    exit 1
fi

pulumi stack select ${organization_name}/oscmcompany/production --create

pulumi up --diff --yes

echo "Forcing ECS service deployments to pull latest images"

cluster=$(pulumi stack output aws_ecs_cluster_name --stack production 2>/dev/null || echo "")

if [ -z "$cluster" ]; then
    echo "Cluster not found - skipping service deployments (initial setup)"
else
    for service in oscmcompany-datamanager oscmcompany-portfoliomanager oscmcompany-equitypricemodel; do
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
    echo "Monitor progress: aws ecs describe-services --cluster $cluster --services oscmcompany-portfoliomanager"
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

base_url=$(pulumi stack output psf_base_url --stack production 2>/dev/null || echo "")

if [ -z "$base_url" ]; then
    echo "psf_base_url not found - infrastructure might not be deployed"
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

> Run Rust tests

```bash
set -euo pipefail

echo "Running Rust tests"

cargo test --workspace --verbose

echo "Rust tests completed successfully"
```

#### all

> Full Rust development checks

```bash
set -euo pipefail

echo "Running Rust development checks"

mask development rust update

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

echo "Running Python tests"

uv run coverage run --parallel-mode -m pytest && uv run coverage combine && uv run coverage report && uv run coverage xml -o coverage/.python.xml

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
        --secret-id oscmcompany/production/environment_variables \
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
        echo "  testing     - ml.t3.xlarge (CPU, ~\$0.23/hr)"
        echo "  standard    - ml.g5.xlarge (GPU, ~\$1.41/hr) [default]"
        echo "  performance - ml.p3.2xlarge (GPU, ~\$3.82/hr)"
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

echo "Removing existing MCP entries (ignore errors if not present)..."
remove_if_present awslabs-ecs-mcp-server
remove_if_present awslabs-cloudwatch-logs-mcp-server
remove_if_present awslabs-cloudwatch-mcp-server

ECS_PKG="awslabs.ecs-mcp-server@latest"
CW_PKG="awslabs.cloudwatch-mcp-server@latest"

echo "Detecting uvx executable for ECS package: $ECS_PKG"
ECS_EXE="$(detect_uvx_exe "$ECS_PKG")"
echo "  -> ECS executable: $ECS_EXE"

echo "Detecting uvx executable for CloudWatch package: $CW_PKG"
CW_EXE="$(detect_uvx_exe "$CW_PKG")"
echo "  -> CloudWatch executable: $CW_EXE"

echo "Adding ECS MCP server..."
claude mcp add awslabs-ecs-mcp-server -s project \
  -e FASTMCP_LOG_LEVEL="${FASTMCP_LOG_LEVEL:-info}" \
  -e AWS_PROFILE="$AWS_PROFILE" \
  -e AWS_REGION="$AWS_REGION" \
  -- "$UVX_PATH" --from "$ECS_PKG" "$ECS_EXE"

echo "Adding CloudWatch MCP server..."
claude mcp add awslabs-cloudwatch-mcp-server -s project \
  -e FASTMCP_LOG_LEVEL="${FASTMCP_LOG_LEVEL:-info}" \
  -e AWS_PROFILE="$AWS_PROFILE" \
  -e AWS_REGION="$AWS_REGION" \
  -- "$UVX_PATH" --from "$CW_PKG" "$CW_EXE"

echo
echo "Done. Current MCP status:"
claude mcp list
```

## ralph

> Ralph autonomous development workflow

### setup

> Create required labels for Ralph workflow

```bash
set -euo pipefail

echo "Setting up Ralph labels"

if ! command -v gh &> /dev/null; then
    echo "GitHub CLI (gh) is required"
    exit 1
fi

if ! gh auth status &> /dev/null; then
    echo "GitHub CLI not authenticated"
    echo "Run: gh auth login"
    exit 1
fi

labels='[
  {"name": "refining", "color": "ba3eb2", "description": "Spec being built or discussed"},
  {"name": "ready", "color": "0e8a16", "description": "Spec complete, ready for implementation"},
  {"name": "in-progress", "color": "fbca04", "description": "Ralph loop actively working"},
  {"name": "needs-attention", "color": "d93f0b", "description": "Loop hit max iterations or got stuck"},
  {"name": "backlog-review", "color": "7057ff", "description": "Backlog review tracking"}
]'

existing=$(gh label list --json name --jq '.[].name')

echo "$labels" | jq -c '.[]' | while read -r label; do
    name=$(echo "$label" | jq -r '.name')
    color=$(echo "$label" | jq -r '.color')
    desc=$(echo "$label" | jq -r '.description')

    if echo "$existing" | grep -qx "$name"; then
        echo "  Label '$name' already exists"
    else
        gh label create "$name" --color "$color" --description "$desc"
        echo "  Created label '$name'"
    fi
done

echo "Setup complete"
```

### spec [issue_number]

> Build or refine a spec through interactive conversation

```bash
set -euo pipefail

echo "Starting Ralph spec refinement"

if ! command -v gh &> /dev/null; then
    echo "GitHub CLI (gh) is required"
    exit 1
fi

if ! command -v claude &> /dev/null; then
    echo "Claude CLI is required"
    exit 1
fi

if ! command -v jq &> /dev/null; then
    echo "jq is required"
    exit 1
fi

if ! gh auth status &> /dev/null; then
    echo "GitHub CLI not authenticated"
    echo "Run: gh auth login"
    exit 1
fi

if [ -z "${issue_number:-}" ]; then
    echo "Creating new spec issue"

    issue_json=$(gh issue create \
        --template "SPEC.md" \
        --title "New Spec: [TITLE]" \
        --label "refining" \
        --label "feature" \
        --json number)

    issue_number=$(echo "$issue_json" | jq -r '.number')

    echo "Created issue #${issue_number}"
    echo "Opening issue in browser"
    gh issue view "${issue_number}" --web &
fi

echo "Refining issue #${issue_number}"

issue_title=$(gh issue view "${issue_number}" --json title --jq '.title')
issue_body=$(gh issue view "${issue_number}" --json body --jq '.body')

system_prompt="You are helping refine a technical specification in GitHub issue #${issue_number}.

CURRENT SPEC:
Title: ${issue_title}

${issue_body}

YOUR ROLE:
1. Probe the user with questions to refine this spec
2. Ask about: problem clarity, requirements completeness, performance, security, testing, edge cases, dependencies
3. When decisions are made, update the issue incrementally using: gh issue edit ${issue_number} --body \"...\"
4. Keep Open Questions section updated as questions are resolved
5. Move resolved questions to Decisions section with rationale

IMPORTANT:
- You do NOT add the 'ready' label - the human decides when the spec is complete
- Update the issue body incrementally as decisions are made
- Use the spec template format (Problem, Requirements, Open Questions, Decisions, Specification)
- Be thorough but conversational

Start by reviewing the current spec and asking clarifying questions."

claude --system-prompt "$system_prompt"

echo ""
echo "Spec refinement session ended"
echo "When ready, add the 'ready' label:"
echo "  gh issue edit ${issue_number} --add-label ready --remove-label refining"
```

### loop (issue_number)

> Run autonomous loop on a ready spec

```bash
set -euo pipefail

max_iterations="${RALPH_MAX_ITERATIONS:-10}"

echo "Starting Ralph loop for issue #${issue_number}"

echo "Running pre-flight checks"

if ! command -v gh &> /dev/null; then
    echo "GitHub CLI (gh) is required"
    exit 1
fi

if ! command -v claude &> /dev/null; then
    echo "Claude CLI is required"
    exit 1
fi

if ! command -v jq &> /dev/null; then
    echo "jq is required"
    exit 1
fi
echo "  Required tools available"

if [ -n "$(git status --porcelain)" ]; then
    echo "Error: Working directory has uncommitted changes"
    echo "Commit or stash changes before running ralph loop"
    exit 1
fi
echo "  Working directory is clean"

default_branch=$(git remote show origin | grep 'HEAD branch' | cut -d' ' -f5)
current_branch=$(git rev-parse --abbrev-ref HEAD)
if [ "$current_branch" != "$default_branch" ]; then
    echo "Error: Not on default branch ${default_branch} (currently on: ${current_branch})"
    echo "Run: git checkout ${default_branch}"
    exit 1
fi
echo "  On default branch (${default_branch})"

echo "  Pulling latest ${default_branch}"
if ! git pull --ff-only origin "$default_branch"; then
    echo "Error: Could not pull latest ${default_branch}"
    echo "Resolve conflicts or check network/auth"
    exit 1
fi
echo "  ${default_branch} is up to date"

if ! gh auth status &> /dev/null; then
    echo "Error: GitHub CLI not authenticated"
    echo "Run: gh auth login"
    exit 1
fi
echo "  GitHub CLI authenticated"

if ! labels=$(gh issue view "${issue_number}" --json labels --jq '.labels[].name'); then
    echo "Error: Could not fetch issue #${issue_number}"
    echo "Check network connectivity and issue existence"
    exit 1
fi
if ! echo "$labels" | grep -q "^ready$"; then
    echo "Error: Issue #${issue_number} does not have 'ready' label"
    echo "Current labels: ${labels:-none}"
    exit 1
fi
echo "  Issue has 'ready' label"

issue_title=$(gh issue view "${issue_number}" --json title --jq '.title')
short_desc=$(echo "$issue_title" | tr '[:upper:]' '[:lower:]' | tr ' ' '-' | tr -cd 'a-z0-9-' | cut -c1-30)
branch_name="ralph/${issue_number}-${short_desc}"

if git show-ref --verify --quiet "refs/heads/${branch_name}" 2>/dev/null; then
    echo "Error: Local branch '${branch_name}' already exists"
    echo "Delete with: git branch -d ${branch_name}"
    exit 1
fi

if git ls-remote --heads origin "${branch_name}" 2>/dev/null | grep -q .; then
    echo "Error: Remote branch '${branch_name}' already exists"
    echo "Delete with: git push origin --delete ${branch_name}"
    exit 1
fi
echo "  Branch '${branch_name}' does not exist"

echo "Pre-flight checks passed"

echo "Creating branch: ${branch_name}"
git checkout -b "${branch_name}"

echo "Updating labels: removing 'ready', adding 'in-progress'"
gh issue edit "${issue_number}" --remove-label "ready" --add-label "in-progress"

cleanup_on_error() {
    local exit_code=$?
    if [ $exit_code -ne 0 ]; then
        echo ""
        echo "Error: Script failed unexpectedly (exit code: $exit_code)"
        gh issue edit "${issue_number}" --remove-label "in-progress" --add-label "needs-attention" 2>/dev/null || true
        gh issue comment "${issue_number}" --body "## Ralph Loop Error

The loop exited unexpectedly with code $exit_code.

**Branch:** \`${branch_name}\`

Check the terminal output for details. The branch may have partial progress." 2>/dev/null || true
    fi
}
trap cleanup_on_error EXIT

system_prompt="You are executing an autonomous development loop for GitHub issue #${issue_number}.

WORKFLOW:
1. Read the spec: gh issue view ${issue_number}
2. PLAN: Identify unchecked requirements, group logically related ones
3. EXECUTE: Implement the grouped requirements
4. TEST: Run pre-commit hooks (they run mask development python/rust all)
5. UPDATE: Check off completed requirements in the issue
6. DECIDE: If more unchecked requirements remain AND you've completed a logical group, exit to rotate context

COMPLETION:
- When ALL requirement checkboxes are checked, output <promise>COMPLETE</promise>
- This signals the loop is done and triggers PR creation

CONTEXT ROTATION:
- Complete logically related requirements together (same files, same concepts)
- Exit after meaningful progress to allow fresh context on next iteration
- Don't try to do everything in one pass

CHECKBOX UPDATE:
- Use gh issue edit to update the issue body with checked boxes
- Update checkboxes BEFORE exiting to preserve progress

GIT:
- ALWAYS attempt git commit after implementing a requirement
- The commit triggers pre-commit hooks which verify the change
- If commit fails, fix the issues and retry
- If commit succeeds, the requirement is verified - check it off

IMPORTANT:
- Start with planning before any code changes
- Be thorough but exit after completing related requirements
- The commit gate is the verification (pre-commit = mask development all)"

stream_text='select(.type == "assistant").message.content[]? | select(.type == "text").text // empty | gsub("\n"; "\r\n") | . + "\r\n\n"'
final_result='select(.type == "result").result // empty'

tmpfile=$(mktemp)

cleanup_and_fail() {
    rm -f "$tmpfile"
    echo ""
    echo "============================================"
    echo "UNEXPECTED ERROR - cleaning up"
    echo "============================================"
    gh issue edit "${issue_number}" --remove-label "in-progress" --add-label "needs-attention" 2>/dev/null || true
    gh issue comment "${issue_number}" --body "## Ralph Loop Error

The loop exited unexpectedly. Branch: \`${branch_name}\`

Check the logs and retry with: \`mask ralph loop ${issue_number}\`" 2>/dev/null || true
    exit 1
}

trap "rm -f $tmpfile" EXIT
trap cleanup_and_fail ERR

iteration=1
while [ $iteration -le $max_iterations ]; do
    echo ""
    echo "============================================"
    echo "ITERATION ${iteration}/${max_iterations}"
    echo "============================================"

    spec=$(gh issue view "${issue_number}" --json body --jq '.body')

    claude \
        --print \
        --output-format stream-json \
        --system-prompt "${system_prompt}" \
        --dangerously-skip-permissions \
        "Current spec state:\n\n${spec}\n\nBegin iteration ${iteration}. Start with planning." \
    | grep --line-buffered '^{' \
    | tee "$tmpfile" \
    | jq --unbuffered -rj "$stream_text"

    result=$(jq -r "$final_result" "$tmpfile")

    if [[ "$result" == *"<promise>COMPLETE</promise>"* ]]; then
        echo ""
        echo "============================================"
        echo "RALPH COMPLETE after ${iteration} iterations"
        echo "============================================"

        echo "Pushing branch"
        git push -u origin "${branch_name}"

        echo "Creating pull request"
        pr_body=$(cat <<EOF
## Summary

Autonomous implementation of issue #${issue_number}

Closes #${issue_number}

## Implementation

See linked issue for full spec and requirements checklist.

---

Generated by Ralph loop in ${iteration} iteration(s)

<promise>COMPLETE</promise>
EOF
)
        pr_url=$(gh pr create \
            --title "${issue_title}" \
            --body "$pr_body")

        echo "Pull request created: ${pr_url}"
        echo "Issue will auto-close on merge"
        trap - EXIT
        exit 0
    fi

    iteration=$((iteration + 1))
done

echo ""
echo "============================================"
echo "MAX ITERATIONS REACHED (${max_iterations})"
echo "============================================"

echo "Pushing branch for review"
branch_pushed="false"
if git push -u origin "${branch_name}" 2>/dev/null; then
    echo "Branch pushed successfully"
    branch_pushed="true"
else
    echo "Warning: Could not push branch (progress is local only)"
fi

gh issue edit "${issue_number}" --remove-label "in-progress" --add-label "needs-attention"

modified_files=$(git diff --name-only "origin/${default_branch}" 2>/dev/null || echo "none")

if [ "$branch_pushed" = "true" ]; then
    branch_info="**Branch:** [\`${branch_name}\`](../../tree/${branch_name}) (pushed to remote)"
else
    branch_info="**Branch:** \`${branch_name}\` (local only - push failed)"
fi

failure_comment=$(cat <<EOF
## Ralph Loop Failed

**Iterations:** ${max_iterations}/${max_iterations}
${branch_info}

### Current State
Review the branch to see partial progress.

### Files Modified
${modified_files:-none}

### Next Steps
1. Review the branch: \`git checkout ${branch_name}\`
2. Check what requirements remain unchecked
3. Manually complete or debug the issue
4. Consider breaking down remaining requirements into smaller tasks

### To Resume
1. Fix the blocking issue
2. Update the spec if needed
3. Delete the branch: \`git branch -D ${branch_name} && git push origin --delete ${branch_name}\`
4. Re-add the \`ready\` label and run \`mask ralph loop ${issue_number}\` again
EOF
)

gh issue comment "${issue_number}" --body "$failure_comment"

echo "Failure comment posted to issue #${issue_number}"
echo "Label changed to 'needs-attention'"
trap - EXIT
exit 1
```

### backlog

> Review open issues for duplicates, overlaps, and staleness

```bash
set -euo pipefail

echo "Starting Ralph backlog review"

if ! command -v gh &> /dev/null; then
    echo "GitHub CLI (gh) is required"
    exit 1
fi

if ! command -v claude &> /dev/null; then
    echo "Claude CLI is required"
    exit 1
fi

if ! command -v jq &> /dev/null; then
    echo "jq is required"
    exit 1
fi

if ! gh auth status &> /dev/null; then
    echo "GitHub CLI not authenticated"
    echo "Run: gh auth login"
    exit 1
fi

staleness_days=60
tracking_issue_title="Backlog Review"

echo "Checking for existing tracking issue"
existing_issue=$(gh issue list --search "\"${tracking_issue_title}\" in:title" --state open --json number --jq '.[0].number // empty')

if [ -z "$existing_issue" ]; then
    echo "Creating tracking issue: ${tracking_issue_title}"

    tracking_body=$(cat <<'TRACKING_TEMPLATE'
# Backlog Review

This issue tracks periodic backlog review reports generated by `mask ralph backlog`.

Each comment contains an analysis of open issues looking for:
- Potential duplicates or overlapping issues
- Stale issues (no activity for 60+ days)
- Issues that may already be implemented
- Consolidation opportunities

Run `mask ralph backlog` to generate a new report.
TRACKING_TEMPLATE
)

    existing_issue=$(gh issue create \
        --title "${tracking_issue_title}" \
        --body "$tracking_body" \
        --label "backlog-review" \
        --json number -q '.number')

    echo "Created tracking issue #${existing_issue}"
else
    echo "Found existing tracking issue #${existing_issue}"
fi

echo "Fetching open issues"
issues_json=$(gh issue list --state open --limit 500 --json number,title,body,labels,updatedAt,createdAt)
issue_count=$(echo "$issues_json" | jq 'length')
echo "Found ${issue_count} open issues"

echo "Analyzing backlog with Claude"

staleness_date=$(date -v-${staleness_days}d +%Y-%m-%d 2>/dev/null || date -d "-${staleness_days} days" +%Y-%m-%d)
today=$(date +%Y-%m-%d)

system_prompt="You are analyzing a GitHub issue backlog for consolidation opportunities.

TODAY'S DATE: ${today}
STALENESS THRESHOLD: ${staleness_days} days (issues not updated since ${staleness_date} are stale)
TRACKING ISSUE: #${existing_issue} (do NOT include this in analysis)

ANALYSIS TASKS:
1. DUPLICATES: Find issues with similar titles/descriptions that might be duplicates
2. OVERLAPS: Find issues that cover related functionality and could be consolidated
3. STALE: Find issues with updatedAt older than ${staleness_date} (exclude issues with 'ready' or 'in-progress' labels)
4. IMPLEMENTED: Search the codebase for keywords that suggest an issue might already be done

OUTPUT FORMAT:
Generate a markdown report following this exact structure:

## Backlog Review - ${today}

### Potential Duplicates
(List pairs/groups with confidence level and reasoning, or 'None found')

### Stale Issues (60+ days)
(List issue numbers with last activity date, or 'None found')

### Potentially Implemented
(List issues where codebase search found relevant matches, or 'None found')

### Consolidation Suggestions
(Suggest which issues could be merged and why, or 'None')

### Summary
- X open issues reviewed
- Y potential duplicates found
- Z stale issues identified

IMPORTANT:
- Be conservative with duplicate detection - only flag clear matches
- For 'potentially implemented', actually search the codebase using Grep/Glob
- Exclude the tracking issue #${existing_issue} from all analysis
- Use high/medium/low confidence levels
- Keep the report concise and actionable"

report=$(claude \
    --print \
    --dangerously-skip-permissions \
    --system-prompt "${system_prompt}" \
    "Analyze this issue backlog and generate a report:

${issues_json}

Search the codebase as needed to check if issues might already be implemented.")

echo "Posting report to tracking issue #${existing_issue}"
gh issue comment "${existing_issue}" --body "${report}"

echo ""
echo "Backlog review complete"
echo "Report posted to: https://github.com/$(gh repo view --json nameWithOwner -q .nameWithOwner)/issues/${existing_issue}"
```

### pr

> Process PR review feedback interactively

**OPTIONS**
* pr_number
    * flags: --pr
    * type: string
    * desc: PR number (auto-detects from branch if not provided)

```bash
set -euo pipefail

echo "Starting Ralph PR review"

if ! command -v gh &> /dev/null; then
    echo "GitHub CLI (gh) is required"
    exit 1
fi

if ! command -v claude &> /dev/null; then
    echo "Claude CLI is required"
    exit 1
fi

if ! command -v jq &> /dev/null; then
    echo "jq is required"
    exit 1
fi

if ! gh auth status &> /dev/null; then
    echo "GitHub CLI not authenticated"
    echo "Run: gh auth login"
    exit 1
fi

if [ -n "${pr_number:-}" ]; then
    pr_num="$pr_number"
    echo "Using PR #${pr_num}"
else
    echo "Auto-detecting PR from current branch"
    pr_num=$(gh pr view --json number --jq '.number' 2>/dev/null || echo "")
    if [ -z "$pr_num" ]; then
        echo "Error: No PR found for current branch"
        echo "Use --pr <number> to specify a PR"
        exit 1
    fi
    echo "Found PR #${pr_num}"
fi

repo_info=$(gh repo view --json nameWithOwner --jq '.nameWithOwner')
owner=$(echo "$repo_info" | cut -d'/' -f1)
repo=$(echo "$repo_info" | cut -d'/' -f2)

echo "Fetching review comments"

review_threads=$(gh api graphql -f query='
query($owner: String!, $repo: String!, $pr: Int!) {
  repository(owner: $owner, name: $repo) {
    pullRequest(number: $pr) {
      reviewThreads(first: 100) {
        nodes {
          id
          isResolved
          path
          line
          comments(first: 10) {
            nodes {
              id
              body
              author { login }
              createdAt
            }
          }
        }
      }
    }
  }
}' -f owner="$owner" -f repo="$repo" -F pr="$pr_num")

unresolved_threads=$(echo "$review_threads" | jq '[.data.repository.pullRequest.reviewThreads.nodes[] | select(.isResolved == false)]')
thread_count=$(echo "$unresolved_threads" | jq 'length')

if [ "$thread_count" -eq 0 ]; then
    echo "No unresolved review conversations found"
    exit 0
fi

echo "Found ${thread_count} unresolved conversation(s)"
echo ""

plan_file=$(mktemp)
trap "rm -f $plan_file" EXIT
echo "[]" > "$plan_file"

index=0
while [ $index -lt "$thread_count" ]; do
    thread=$(echo "$unresolved_threads" | jq ".[$index]")
    thread_id=$(echo "$thread" | jq -r '.id')
    path=$(echo "$thread" | jq -r '.path // "unknown"')
    line=$(echo "$thread" | jq -r '.line // "?"')
    first_comment=$(echo "$thread" | jq '.comments.nodes[0]')
    author=$(echo "$first_comment" | jq -r '.author.login // "unknown"')
    body=$(echo "$first_comment" | jq -r '.body')

    display_num=$((index + 1))
    echo "============================================"
    echo "[${display_num}/${thread_count}] @${author} on ${path}:${line}"
    echo "============================================"
    echo ""

    # Truncate for display: show text before code blocks, summarize code blocks
    display_body=$(echo "$body" | awk '
        BEGIN { in_code=0; code_lines=0; shown_code=0 }
        /^```/ {
            if (in_code) {
                if (code_lines > 3) printf "  ... (%d lines)\n", code_lines
                print "```"
                in_code=0; code_lines=0; shown_code=0
            } else {
                print
                in_code=1
            }
            next
        }
        in_code {
            code_lines++
            if (shown_code < 3) { print; shown_code++ }
            next
        }
        { print }
    ' | head -25)

    echo "$display_body"

    total_lines=$(echo "$body" | wc -l | tr -d ' ')
    if [ "$total_lines" -gt 25 ]; then
        echo "... (${total_lines} lines total)"
    fi
    echo ""

    while true; do
        printf "[A]ccept / [S]kip / [M]odify / [Q]uit: "
        read -r choice < /dev/tty

        case "$choice" in
            [Aa])
                jq --arg tid "$thread_id" --arg path "$path" --arg line "$line" --arg body "$body" --arg author "$author" \
                    '. += [{"thread_id": $tid, "path": $path, "line": $line, "suggestion": $body, "author": $author, "action": "accept", "modification": null}]' \
                    "$plan_file" > "${plan_file}.tmp" && mv "${plan_file}.tmp" "$plan_file"
                echo "Added to plan"
                break
                ;;
            [Ss])
                echo "Skipped"
                break
                ;;
            [Mm])
                echo "Enter your modification (end with empty line):"
                modification=""
                while IFS= read -r mod_line < /dev/tty; do
                    [ -z "$mod_line" ] && break
                    modification="${modification}${mod_line}\n"
                done
                jq --arg tid "$thread_id" --arg path "$path" --arg line "$line" --arg body "$body" --arg author "$author" --arg mod "$modification" \
                    '. += [{"thread_id": $tid, "path": $path, "line": $line, "suggestion": $body, "author": $author, "action": "modify", "modification": $mod}]' \
                    "$plan_file" > "${plan_file}.tmp" && mv "${plan_file}.tmp" "$plan_file"
                echo "Added to plan with modification"
                break
                ;;
            [Qq])
                echo "Quitting"
                exit 0
                ;;
            *)
                echo "Invalid choice. Use A/S/M/Q"
                ;;
        esac
    done

    echo ""
    index=$((index + 1))
done

plan_count=$(jq 'length' "$plan_file")

if [ "$plan_count" -eq 0 ]; then
    echo "No suggestions accepted. Nothing to do."
    exit 0
fi

echo "============================================"
echo "Plan Summary"
echo "============================================"
echo "Accepted: ${plan_count} suggestion(s)"
echo ""
jq -r '.[] | "- \(.path):\(.line) (\(.action))"' "$plan_file"
echo ""

printf "Proceed with execution? [Y/n]: "
read -r confirm < /dev/tty

if [ "$confirm" = "n" ] || [ "$confirm" = "N" ]; then
    echo "Aborted"
    exit 0
fi

echo ""
echo "Starting execution phase"

plan_json=$(cat "$plan_file")

system_prompt="You are implementing approved PR review suggestions.

TASK:
For each suggestion in the plan, implement the requested change.

PLAN:
${plan_json}

WORKFLOW:
1. For each item in the plan:
   - Read the file at the specified path
   - Implement the suggestion (or the modification if provided)
   - The 'suggestion' field contains the reviewer's comment
   - The 'modification' field (if present) contains the user's adjusted approach
2. After implementing, commit with a message like: 'Address review: <brief description>'
3. Output a JSON array of results for posting replies, format:
   [{\"thread_id\": \"...\", \"reply\": \"Fixed in <commit>. <brief explanation>\"}]

IMPORTANT:
- Make minimal, focused changes
- Don't refactor beyond what's requested
- If a suggestion doesn't make sense, skip it and note why in the reply
- Output the JSON results array at the end, wrapped in <results>...</results> tags"

result=$(claude \
    --print \
    --dangerously-skip-permissions \
    --system-prompt "${system_prompt}" \
    "Implement the suggestions in the plan. Output results JSON when done.")

results_json=$(echo "$result" | awk '/<results>/,/<\/results>/' | sed '1s/.*<results>//; $s/<\/results>.*//' | tr -d '\n')

if [ -z "$results_json" ]; then
    echo "Warning: Could not parse results from Claude output"
    echo "Changes may have been made but replies were not posted"
    exit 1
fi

echo "Pushing changes"
if ! git push; then
    echo "Error: Failed to push changes"
    echo "Replies will not be posted until push succeeds"
    exit 1
fi

echo "Posting replies and resolving conversations"

echo "$results_json" | jq -c '.[]' | while read -r item; do
    thread_id=$(echo "$item" | jq -r '.thread_id')
    reply=$(echo "$item" | jq -r '.reply')

    echo "Replying to thread ${thread_id}"

    gh api graphql -f query='
    mutation($threadId: ID!, $body: String!) {
      addPullRequestReviewThreadReply(input: {pullRequestReviewThreadId: $threadId, body: $body}) {
        comment { id }
      }
    }' -f threadId="$thread_id" -f body="$reply" > /dev/null 2>&1 || echo "Warning: Failed to post reply"

    gh api graphql -f query='
    mutation($threadId: ID!) {
      resolveReviewThread(input: {threadId: $threadId}) {
        thread { isResolved }
      }
    }' -f threadId="$thread_id" > /dev/null 2>&1 || echo "Warning: Failed to resolve thread"
done

echo ""
echo "PR review complete"
echo "View PR: https://github.com/${repo_info}/pull/${pr_num}"
```
