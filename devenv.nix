{
  pkgs,
  lib,
  config,
  inputs,
  ...
}: let
  awsRegion = "us-east-1";

  # ECS service definitions — maps service names to their Docker build context
  ecsServices = {
    data-manager = {
      dockerfile = "applications/data_manager/Dockerfile";
      context = ".";
      ecrRepo = "fund/data-manager-server";
    };
    ensemble-manager = {
      dockerfile = "applications/ensemble_manager/Dockerfile";
      context = ".";
      ecrRepo = "fund/ensemble-manager-server";
    };
    portfolio-manager = {
      dockerfile = "applications/portfolio_manager/Dockerfile";
      context = ".";
      ecrRepo = "fund/portfolio-manager-server";
    };
  };

  deployableServices = builtins.attrNames ecsServices;
in {
  dotenv.enable = true;

  languages = {
    rust.enable = true;
    python.enable = true;
    nix.enable = true;
  };

  git-hooks.hooks = {
    python-install = {
      enable = true;
      name = "Install Python dependencies";
      entry = "python-install";
      files = "(pyproject\\.toml|uv\\.lock)$";
      pass_filenames = false;
      language = "system";
    };
    python-checks = {
      enable = true;
      name = "Check all Python code";
      entry = "python-checks";
      files = "(\\.py|pyproject\\.toml|uv\\.lock)$";
      pass_filenames = false;
      language = "system";
      fail_fast = true;
    };
    rust-checks = {
      enable = true;
      name = "Check all Rust code";
      entry = "rust-checks";
      files = "(\\.rs|Cargo\\.(toml|lock))$";
      pass_filenames = false;
      language = "system";
      fail_fast = true;
    };
    markdown-checks = {
      enable = true;
      name = "Check all Markdown code";
      entry = "markdown-checks";
      files = "\\.md$";
      pass_filenames = false;
      language = "system";
      fail_fast = true;
    };
    yaml-checks = {
      enable = true;
      name = "Check all YAML code";
      entry = "yaml-checks";
      files = "\\.(yaml|yml)$";
      pass_filenames = false;
      language = "system";
      fail_fast = true;
    };
    nix-lint = {
      enable = true;
      name = "Lint all Nix code";
      entry = "nix-lint";
      files = "\\.nix$";
      pass_filenames = false;
      language = "system";
      fail_fast = true;
    };
  };

  env = {
    # DuckDB library path for Rust linker
    LIBRARY_PATH = "${pkgs.duckdb}/lib";

    # AWS region
    AWS_REGION = awsRegion;
    AWS_DEFAULT_REGION = awsRegion;

    # Service URLs (localhost, not container names)
    FUND_DATAMANAGER_BASE_URL = "http://localhost:8080";
    FUND_ENSEMBLEMANAGER_BASE_URL = "http://localhost:8082";
    PREFECT_API_URL = "http://localhost:4200/api";

    # MLflow tracking (production ALB, set after pulumi-up)
    # Override in .envrc with the actual ALB URL: http://<alb-dns>:5000
    MLFLOW_TRACKING_URI = "";

    # Development defaults
    ENVIRONMENT = "development";
    DISABLE_DISK_CACHE = "1";
    BACKFILL_LOOKBACK_DAYS = "730";
  };

  packages = with pkgs; [
    git
    curl
    jq
    rustup
    cargo-watch
    awscli2
    pulumiPackages.pulumi-language-python
    pulumi-bin
    markdownlint-cli
    uv
    xenon
    ruff
    alejandra
    duckdb
    docker-client
  ];

  # PostgreSQL for orchestration server (local)
  services.postgres = {
    enable = true;
    package = pkgs.postgresql_16;
    listen_addresses = "127.0.0.1";
    port = 5432;
    initialDatabases = [
      {
        name = "prefect";
        user = "prefect";
        pass = "prefect";
      }
    ];
  };

  # Prometheus for local metrics scraping
  services.prometheus = {
    enable = true;
    port = 9090;
    scrapeConfigs = [
      {
        job_name = "data-manager";
        static_configs = [{targets = ["localhost:8080"];}];
        metrics_path = "/metrics";
        scrape_interval = "15s";
      }
      {
        job_name = "ensemble-manager";
        static_configs = [{targets = ["localhost:8082"];}];
        metrics_path = "/metrics";
        scrape_interval = "15s";
      }
      {
        job_name = "portfolio-manager";
        static_configs = [{targets = ["localhost:8081"];}];
        metrics_path = "/metrics";
        scrape_interval = "15s";
      }
    ];
  };

  # --- AWS / Pulumi commands ---
  # All infra scripts unset MinIO env vars so the real AWS credentials are used.

  # Bring up AWS infrastructure with Pulumi
  scripts.infra-up.exec = ''
    unset AWS_ENDPOINT_URL
    cd "$DEVENV_ROOT/infrastructure"
    pulumi up --stack production --yes
  '';

  # Tear down AWS infrastructure with Pulumi
  scripts.infra-down.exec = ''
    unset AWS_ENDPOINT_URL
    cd "$DEVENV_ROOT/infrastructure"
    pulumi down --stack production --yes
  '';

  # Show Pulumi stack outputs (ALB URL, ECR repos, bucket names, etc.)
  scripts.infra-outputs.exec = ''
    unset AWS_ENDPOINT_URL
    cd "$DEVENV_ROOT/infrastructure"
    pulumi stack output --stack production --json
  '';

  # Get the ALB base URL from Pulumi outputs
  scripts.infra-url.exec = ''
    unset AWS_ENDPOINT_URL
    cd "$DEVENV_ROOT/infrastructure"
    pulumi stack output --stack production aws_alb_url 2>/dev/null || echo "Not deployed yet"
  '';

  # Build and push a Docker image to ECR
  scripts.ecr-push.exec = ''
    unset AWS_ENDPOINT_URL
    SERVICE="$1"
    if [ -z "$SERVICE" ]; then
      echo "Usage: ecr-push <${lib.concatStringsSep "|" deployableServices}|all>"
      exit 1
    fi

    ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
    REGISTRY="$ACCOUNT_ID.dkr.ecr.${awsRegion}.amazonaws.com"
    GIT_SHA=$(git rev-parse --short HEAD)

    echo "Logging into ECR..."
    aws ecr get-login-password --region ${awsRegion} | docker login --username AWS --password-stdin "$REGISTRY"

    ${lib.concatStringsSep "\n" (lib.mapAttrsToList (svc: def: ''
        push_${builtins.replaceStrings ["-"] ["_"] svc}() {
          local repo="${def.ecrRepo}"
          local sha_tag="git-$GIT_SHA"
          local ctx_dir="$DEVENV_ROOT/${def.context}"

          # Skip if this commit is already pushed
          if aws ecr describe-images --repository-name "$repo" --image-ids imageTag="$sha_tag" --region ${awsRegion} >/dev/null 2>&1; then
            echo "=== Skipping ${svc} (tag $sha_tag already exists) ==="
            return 0
          fi

          echo "=== Building ${svc} ==="
          docker buildx build \
            --platform linux/amd64 \
            --tag "$REGISTRY/$repo:latest" \
            --tag "$REGISTRY/$repo:$sha_tag" \
            --file "$DEVENV_ROOT/${def.dockerfile}" \
            --push \
            "$ctx_dir"
          echo "OK: ${svc}"
        }
      '')
      ecsServices)}

    case "$SERVICE" in
      ${lib.concatStringsSep "\n" (map (svc: ''
      ${svc}) push_${builtins.replaceStrings ["-"] ["_"] svc} ;;'')
    deployableServices)}
      all)
        ${lib.concatStringsSep "\n        " (map (svc: ''
      push_${builtins.replaceStrings ["-"] ["_"] svc}'')
    deployableServices)}
        echo "All images pushed"
        ;;
      *) echo "Unknown service: $SERVICE"; exit 1 ;;
    esac
  '';

  # Force ECS services to redeploy with latest image
  scripts.ecs-deploy.exec = ''
    unset AWS_ENDPOINT_URL
    SERVICE="$1"
    CLUSTER="fund-applications"

    if [ -z "$SERVICE" ]; then
      echo "Usage: ecs-deploy <${lib.concatStringsSep "|" deployableServices}|all>"
      exit 1
    fi

    deploy_ecs_service() {
      local ecs_name="$1"
      echo "=== Redeploying $ecs_name ==="
      aws ecs update-service \
        --cluster "$CLUSTER" \
        --service "$ecs_name" \
        --force-new-deployment \
        --region ${awsRegion} \
        --query 'service.serviceName' \
        --output text
      echo "Waiting for $ecs_name to stabilize..."
      aws ecs wait services-stable \
        --cluster "$CLUSTER" \
        --services "$ecs_name" \
        --region ${awsRegion}
      echo "OK: $ecs_name"
    }

    case "$SERVICE" in
      data-manager) deploy_ecs_service "fund-data-manager-server" ;;
      ensemble-manager) deploy_ecs_service "fund-ensemble-manager-server" ;;
      portfolio-manager) deploy_ecs_service "fund-portfolio-manager-server" ;;
      all)
        for svc in fund-data-manager-server fund-ensemble-manager-server fund-portfolio-manager-server; do
          deploy_ecs_service "$svc"
        done
        echo "All services redeployed"
        ;;
      *) echo "Unknown service: $SERVICE"; exit 1 ;;
    esac
  '';

  # Build, push, and redeploy a service (combines ecr-push + ecs-deploy)
  scripts.deploy.exec = ''
    SERVICE="$1"
    if [ -z "$SERVICE" ]; then
      echo "Usage: deploy <${lib.concatStringsSep "|" deployableServices}|all>"
      exit 1
    fi
    ecr-push "$SERVICE" && ecs-deploy "$SERVICE"
  '';

  # CD pipeline: push images, update infra, deploy services
  scripts.cd-deploy.exec = ''
    set -euo pipefail
    unset AWS_ENDPOINT_URL

    if ! aws sts get-caller-identity >/dev/null 2>&1; then
      echo "ERROR: No AWS credentials available"
      echo "CD deploy requires AWS credentials (OIDC in CI or configured profile locally)"
      exit 1
    fi

    echo "=== Pushing images to ECR ==="
    ecr-push all

    echo "=== Updating infrastructure ==="
    infra-up

    echo "=== Deploying ECS services ==="
    ecs-deploy all

    echo "CD deploy completed successfully"
  '';

  # Show ECS service status
  scripts.ecs-status.exec = ''
    unset AWS_ENDPOINT_URL
    CLUSTER="fund-applications"
    echo "=== ECS Services ==="
    aws ecs list-services --cluster "$CLUSTER" --region ${awsRegion} --query 'serviceArns[*]' --output table 2>/dev/null || echo "Cluster not found"
    echo ""
    for svc in fund-data-manager-server fund-ensemble-manager-server fund-portfolio-manager-server; do
      STATUS=$(aws ecs describe-services --cluster "$CLUSTER" --services "$svc" --region ${awsRegion} --query 'services[0].{status:status,running:runningCount,desired:desiredCount}' --output text 2>/dev/null)
      if [ -n "$STATUS" ]; then
        echo "  $svc: $STATUS"
      else
        echo "  $svc: not found"
      fi
    done
  '';

  # Pull secrets from AWS Secrets Manager into .envrc
  scripts.pull-secrets.exec = ''
    unset AWS_ENDPOINT_URL
    ENVRC="$DEVENV_ROOT/.envrc"
    AWS_CMD="aws --region ${awsRegion}"
    export AWS_PROFILE=default

    echo "Fetching secrets from AWS Secrets Manager..."

    for secret_id in fund/production/portfolio_manager fund/production/data_manager fund/production/shared; do
      echo "  $secret_id"
      json=$($AWS_CMD secretsmanager get-secret-value --secret-id "$secret_id" --query SecretString --output text)
      for key in $(echo "$json" | jq -r 'keys[]'); do
        val=$(echo "$json" | jq -r --arg k "$key" '.[$k]')
        if grep -q "^export $key=" "$ENVRC" 2>/dev/null; then
          echo "    $key (already set, skipping)"
        else
          echo "export $key=\"$val\"" >> "$ENVRC"
          echo "    $key (added)"
        fi
      done
    done

    echo ""
    echo "Done. Run 'direnv allow' to reload."
  '';

  # Create ECS work pool and register training deployment on Prefect Cloud
  scripts.initialize-remote-trainer.exec = ''
    unset PREFECT_API_URL

    echo "Creating fund-models-remote work pool on Prefect Cloud..."
    uv run --package tools prefect work-pool create "fund-models-remote" --type ecs 2>/dev/null \
      || echo "  already exists"

    echo "Registering training deployments..."
    uv run prefect --no-prompt deploy --all

    echo ""
    echo "Done. Visit Prefect Cloud dashboard to view deployments."
  '';

  # --- Local dev commands ---

  # Register Prefect S3Bucket blocks on the local server
  scripts.register-blocks.exec = ''
        echo "Waiting for orchestrator..."
        while ! curl -sf http://localhost:4200/api/health > /dev/null 2>&1; do
          sleep 2
        done

        export PREFECT_API_URL="http://localhost:4200/api"

        echo "Reading bucket names from Pulumi stack outputs..."
        unset AWS_ENDPOINT_URL
        DATA_BUCKET=$(cd "$DEVENV_ROOT/infrastructure" && pulumi stack output --stack production aws_s3_data_bucket_name 2>/dev/null)
        ARTIFACTS_BUCKET=$(cd "$DEVENV_ROOT/infrastructure" && pulumi stack output --stack production aws_s3_model_artifacts_bucket_name 2>/dev/null)

        if [ -z "$DATA_BUCKET" ] || [ -z "$ARTIFACTS_BUCKET" ]; then
          echo "Could not read bucket names from Pulumi. Using defaults."
          DATA_BUCKET="fund-data-404221e2"
          ARTIFACTS_BUCKET="fund-model-artifacts-404221e2"
        fi

        echo "  data bucket:      $DATA_BUCKET"
        echo "  artifacts bucket:  $ARTIFACTS_BUCKET"

        echo "Registering S3Bucket blocks on local Prefect server..."
        uv run --package tide python -c "
    import sys
    from prefect_aws.s3 import S3Bucket
    from prefect_aws.credentials import AwsCredentials

    data_bucket = sys.argv[1]
    artifacts_bucket = sys.argv[2]

    creds = AwsCredentials()
    creds.save('aws-credentials', overwrite=True)

    S3Bucket(bucket_name=data_bucket, credentials=creds).save('data-bucket', overwrite=True)
    S3Bucket(bucket_name=artifacts_bucket, credentials=creds).save('artifact-bucket', overwrite=True)
    print('Blocks registered: data-bucket, artifact-bucket')
    " "$DATA_BUCKET" "$ARTIFACTS_BUCKET"
  '';

  # Create work pool and register training deployment locally
  scripts.initialize-local-trainer.exec = ''
    echo "Waiting for orchestrator..."
    while ! curl -sf http://localhost:4200/api/health > /dev/null 2>&1; do
      sleep 2
    done

    register-blocks

    echo "Creating fund-models-local work pool..."
    uv run --package tools prefect work-pool create "fund-models-local" --type process 2>/dev/null \
      || echo "  already exists"

    echo "Registering local training deployment..."
    uv run prefect --no-prompt deploy --name tide-trainer-local

    echo "Setting pull steps to local project root..."
    DEPLOYMENT_ID=$(curl -sf http://localhost:4200/api/deployments/name/tide-training-pipeline/tide-trainer-local | jq -r '.id')
    curl -sf -X PATCH "http://localhost:4200/api/deployments/$DEPLOYMENT_ID" \
      -H 'Content-Type: application/json' \
      -d "{\"pull_steps\": [{\"prefect.deployments.steps.set_working_directory\": {\"directory\": \"$DEVENV_ROOT\"}}]}"

    echo ""
    echo "Done. Visit http://localhost:4200 to see the orchestrator dashboard."
    echo "Run 'devenv up' to start workers that will pick up scheduled runs."
  '';

  scripts.cleanup-services.exec = ''
    for PORT in 4200 5432 8080 8081 8082 9090; do
      PID=$(lsof -ti tcp:$PORT 2>/dev/null || true)
      if [ -n "$PID" ]; then
        echo "Killing stale process on port $PORT (PID $PID)"
        kill $PID 2>/dev/null || true
      fi
    done
    sleep 1
    PID_FILE="$PGDATA/postmaster.pid"
    if [ -f "$PID_FILE" ]; then
      PID=$(head -1 "$PID_FILE")
      if ! kill -0 "$PID" 2>/dev/null; then
        echo "Removing stale postmaster.pid (PID $PID not running)"
        rm -f "$PID_FILE"
      fi
    fi
  '';

  # --- Development check scripts ---

  scripts.python-install.exec = ''
    set -euo pipefail
    echo "Installing Python dependencies"
    uv sync --all-packages --all-groups
    echo "Python dependencies installed successfully"
  '';

  scripts.python-format.exec = ''
    set -euo pipefail
    echo "Checking Python code formatting"
    ruff format --check
    echo "Python code formatting check passed"
  '';

  scripts.python-lint = {
    description = "Running Python lint checks";
    exec = ''
      set -euo pipefail
      ruff check --output-format=github .
      echo "Python linting completed successfully"
    '';
  };

  scripts.python-type-check.exec = ''
    set -euo pipefail
    echo "Running Python type checks"
    uvx ty check
    echo "Python type checks completed successfully"
  '';

  scripts.python-dead-code.exec = ''
    set -euo pipefail
    echo "Running dead code analysis"
    uvx vulture \
      --min-confidence 80 \
      --exclude '.flox,.venv,target' \
      . tools/src/tools/vulture_whitelist.py
    echo "Dead code check completed"
  '';

  scripts.python-complexity.exec = ''
    set -euo pipefail
    echo "Running Python complexity analysis"
    xenon --max-absolute D --max-modules D --max-average A \
      --ignore '.flox,.venv,target' .
    echo "Python complexity analysis completed successfully"
  '';

  scripts.python-test.exec = ''
    set -euo pipefail
    echo "Running Python tests"
    mkdir -p .coverage_output
    uv run coverage run -m pytest --tb=short -q
    uv run coverage combine 2>/dev/null || true
    uv run coverage xml
    echo "Python tests completed successfully"
  '';

  scripts.python-checks.exec = ''
    devenv tasks run checks:python
  '';

  scripts.rust-format.exec = ''
    set -euo pipefail
    echo "Checking Rust code formatting"
    cargo fmt --all -- --check
    echo "Rust code formatting check passed"
  '';

  scripts.rust-check.exec = ''
    set -euo pipefail
    echo "Check Rust packages"
    cargo check --workspace
    echo "Rust packages checked successfully"
  '';

  scripts.rust-lint.exec = ''
    set -euo pipefail
    echo "Running Rust lint checks"
    cargo clippy
    echo "Rust linting completed successfully"
  '';

  scripts.rust-test.exec = ''
    set -euo pipefail
    echo "Running Rust tests"

    DOCKER_AVAILABLE=0
    if command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1; then
      DOCKER_AVAILABLE=1
    fi

    if [[ "$DOCKER_AVAILABLE" == "1" ]]; then
      echo "Docker available - running all tests including integration"
      TEST_ARGS="--workspace --verbose"
    else
      echo "Docker not available - skipping integration tests"
      TEST_ARGS="--workspace --verbose --lib --bins"
    fi

    mkdir -p .coverage_output
    if ! command -v cargo-llvm-cov >/dev/null 2>&1; then
      echo "cargo-llvm-cov not available - running tests without coverage"
      cargo test $TEST_ARGS
    elif ! command -v llvm-cov >/dev/null 2>&1 || ! command -v llvm-profdata >/dev/null 2>&1; then
      echo "LLVM tools not available - running tests without coverage"
      cargo test $TEST_ARGS
    else
      export LLVM_COV=$(which llvm-cov)
      export LLVM_PROFDATA=$(which llvm-profdata)
      cargo llvm-cov $TEST_ARGS \
        --cobertura \
        --output-path .coverage_output/rust.xml
      echo "Rust tests with coverage completed successfully"
    fi
  '';

  scripts.rust-checks.exec = ''
    devenv tasks run checks:rust
  '';

  scripts.markdown-checks.exec = ''
    set -euo pipefail
    echo "Running Markdown lint checks"
    markdownlint "**/*.md" --ignore ".flox" --ignore ".venv" \
      --ignore "target" --ignore ".scratchpad"
    echo "Markdown checks completed successfully"
  '';

  scripts.yaml-checks.exec = ''
    set -euo pipefail
    echo "Running YAML lint checks"
    uvx yamllint .
    echo "YAML checks completed successfully"
  '';

  scripts.nix-lint.exec = ''
    set -euo pipefail
    echo "Linting Nix files"
    alejandra --check --exclude ./.devenv --exclude ./.flox --exclude ./.venv --exclude ./target .
    echo "Nix lint check passed"
  '';

  scripts.docker-build-check.exec = ''
    set -euo pipefail
    if ! command -v docker >/dev/null 2>&1 || ! docker info >/dev/null 2>&1; then
      echo "Docker not available - skipping build checks"
      exit 0
    fi

    echo "Running Docker build checks"
    ${lib.concatStringsSep "\n" (lib.mapAttrsToList (svc: def: ''
        echo "=== Building ${svc} ==="
        docker buildx build \
          --platform linux/amd64 \
          --file "$DEVENV_ROOT/${def.dockerfile}" \
          "$DEVENV_ROOT/${def.context}"
      '')
      ecsServices)}
    echo "Docker build checks passed"
  '';

  scripts.bump-deps.exec = ''
    set -euo pipefail
    echo "Bumping all dependencies..."
    echo "=== Rust ==="
    cargo update
    echo "=== Python ==="
    uv lock --upgrade
    echo ""
    echo "Dependencies bumped. Review changes:"
    echo "  git diff Cargo.lock uv.lock"
  '';

  tasks = {
    # --- Python checks (install first, then all others in parallel) ---

    "checks:python:install".exec = "python-install";

    "checks:python:format" = {
      exec = "python-format";
      after = ["checks:python:install"];
    };
    "checks:python:lint" = {
      exec = "python-lint";
      status = "python-install";
    };
    "checks:python:type-check" = {
      exec = "python-type-check";
      after = ["checks:python:lint"];
    };
    "checks:python:dead-code" = {
      exec = "python-dead-code";
      after = ["checks:python:type-check"];
    };
    "checks:python:complexity" = {
      exec = "python-complexity";
      after = ["checks:python:dead-code"];
    };
    "checks:python:test" = {
      exec = "python-test";
      after = ["checks:python:complexity"];
    };

    # --- Rust checks (format parallel with check, lint+test after check) ---

    "checks:rust:format".exec = "rust-format";
    "checks:rust:check".exec = "rust-check";

    "checks:rust:lint" = {
      exec = "rust-lint";
      after = ["checks:rust:check"];
    };
    "checks:rust:test" = {
      exec = "rust-test";
      after = ["checks:rust:check"];
    };

    # --- Standalone checks ---

    "checks:markdown".exec = "markdown-checks";
    "checks:yaml".exec = "yaml-checks";
    "checks:nix".exec = "nix-lint";

    # --- Model tasks ---

    "models:tide:deploy".exec = ''
      uv run prefect --no-prompt deploy --all
    '';
    "models:tide:train".exec = ''
      uv run prefect deployment run tide-training-pipeline/tide-trainer-remote
    '';
    "models:tide:train:local".exec = ''
      uv run prefect deployment run tide-training-pipeline/tide-trainer-local
    '';

    "checks:docker".exec = "docker-build-check";

    "checks:ci" = {
      exec = ''
        echo "All CI checks passed"
      '';
      after = [
        "checks:nix"
        "checks:markdown"
        "checks:yaml"
        "checks:python:test"
        "checks:rust:lint"
        "checks:rust:test"
        "checks:docker"
      ];
    };

    "checks:cd:deploy".exec = "cd-deploy";

    "checks:cd" = {
      exec = ''
        echo "CD pipeline completed"
      '';
      after = ["checks:ci" "checks:cd:deploy"];
    };
  };

  # --- Local processes ---

  processes = {
    orchestrator.exec = ''
      while ! pg_isready -h 127.0.0.1 -p 5432 -U prefect -q 2>/dev/null; do
        sleep 1
      done
      export PREFECT_API_DATABASE_CONNECTION_URL="postgresql+asyncpg://prefect:prefect@127.0.0.1:5432/prefect"
      export PREFECT_UI_API_URL="http://localhost:4200/api"
      cd tools
      exec uv run prefect server start --host 0.0.0.0
    '';

    training-worker-1.exec = ''
      while ! curl -sf http://localhost:4200/api/health > /dev/null 2>&1; do
        sleep 2
      done

      # Register S3 blocks, create work pool, and register deployment on first startup
      export PREFECT_API_URL="http://localhost:4200/api"
      register-blocks || true

      uv run --package tools prefect work-pool create "fund-models-local" --type process 2>/dev/null || true

      uv run prefect --no-prompt deploy --name tide-trainer-local 2>/dev/null || true
      DEPLOYMENT_ID=$(curl -sf http://localhost:4200/api/deployments/name/tide-training-pipeline/tide-trainer-local | jq -r '.id') || true
      if [ -n "$DEPLOYMENT_ID" ] && [ "$DEPLOYMENT_ID" != "null" ]; then
        PROJECT_ROOT="''${DEVENV_ROOT:-$(pwd)}"
        curl -sf -X PATCH "http://localhost:4200/api/deployments/$DEPLOYMENT_ID" \
          -H 'Content-Type: application/json' \
          -d "{\"pull_steps\": [{\"prefect.deployments.steps.set_working_directory\": {\"directory\": \"$PROJECT_ROOT\"}}]}" || true
      fi

      cd tools
      exec uv run prefect worker start --pool fund-models-local --name worker-1
    '';

    training-worker-2.exec = ''
      while ! curl -sf http://localhost:4200/api/health > /dev/null 2>&1; do
        sleep 2
      done
      sleep 3
      cd tools
      exec uv run prefect worker start --pool fund-models-local --name worker-2
    '';

    data-manager.exec = ''
      cd applications/data_manager
      exec cargo watch -x run
    '';

    ensemble-manager.exec = ''
      while ! curl -sf http://localhost:8080/health > /dev/null 2>&1; do
        sleep 2
      done
      cd applications/ensemble_manager
      exec uv run uvicorn ensemble_manager.server:application --host 0.0.0.0 --port 8082 --reload
    '';

    portfolio-manager.exec = ''
      while ! curl -sf http://localhost:8080/health > /dev/null 2>&1; do
        sleep 2
      done
      while ! curl -sf http://localhost:8082/health > /dev/null 2>&1; do
        sleep 2
      done
      cd applications/portfolio_manager
      exec uv run uvicorn portfolio_manager.server:application --host 0.0.0.0 --port 8081 --reload
    '';

    ready.exec = ''
      while ! curl -sf http://localhost:8080/health > /dev/null 2>&1; do
        sleep 2
      done
      echo ""
      echo "========================================"
      echo "  Fund development environment ready"
      echo "========================================"
      echo ""
      echo "  PostgreSQL:       localhost:5432"
      echo "  Prometheus:       localhost:9090"
      echo "  Orchestrator UI:  localhost:4200"
      echo "  Data Manager:     localhost:8080"
      echo "  Ensemble Manager: localhost:8082"
      echo "  Portfolio Manager: localhost:8081"
      echo ""

      # Stay alive so devenv doesn't restart this process
      while true; do sleep 3600; done
    '';
  };

  enterShell = ''
    echo "Fund development environment"
    echo ""
    echo "  Local (devenv up):"
    echo "    PostgreSQL:       localhost:5432"
    echo "    Prometheus:       localhost:9090"
    echo "    Orchestrator UI:  localhost:4200"
    echo "    Data Manager:     localhost:8080"
    echo "    Ensemble Manager: localhost:8082"
    echo "    Portfolio Manager: localhost:8081"
    echo ""
    echo "  AWS (Pulumi):"
    echo "    infra-up          Create all AWS infrastructure"
    echo "    infra-down        Tear down all AWS infrastructure"
    echo "    infra-outputs     Show Pulumi stack outputs"
    echo "    infra-url         Show ALB base URL"
    echo "    pull-secrets      AWS Secrets Manager -> .envrc"
    echo "    ecr-push <svc>   Build and push Docker image to ECR"
    echo "    ecs-deploy <svc>  Force ECS service redeployment"
    echo "    deploy <svc|all>  Build, push, and redeploy (ecr-push + ecs-deploy)"
    echo "    ecs-status        Show ECS service status"
    echo "    initialize-remote-trainer  Create work pool + register deployment (prod)"
    echo ""
    echo "  Checks (devenv tasks run):"
    echo "    checks:python       All Python checks (parallel after install)"
    echo "    checks:rust         All Rust checks (parallel after cargo check)"
    echo "    checks:markdown     Markdown lint"
    echo "    checks:yaml         YAML lint"
    echo "    checks:nix          Nix lint (alejandra)"
    echo "    Individual: checks:python:format, checks:rust:lint, etc."
    echo ""
    echo "  Utilities:"
    echo "    bump-deps         Update all dependency lockfiles"
    echo ""
    echo "  Local:"
    echo "    register-blocks            Register S3 blocks on local Prefect server"
    echo "    initialize-local-trainer   Create work pool + register deployment (local)"
    echo "    cleanup-services  Kill stale local processes"
  '';

  enterTest = ''
  '';
}
