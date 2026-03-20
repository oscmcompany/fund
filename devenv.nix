{ pkgs, lib, config, inputs, ... }:

let
  awsRegion = "us-east-1";

  # ECS service definitions — maps service names to their Docker build context
  ecsServices = {
    data-manager = {
      dockerfile = "applications/data_manager/Dockerfile";
      context = ".";
      ecrRepo = "fund/data_manager-server";
    };
    ensemble-manager = {
      dockerfile = "applications/ensemble_manager/Dockerfile";
      context = ".";
      ecrRepo = "fund/ensemble_manager-server";
    };
    portfolio-manager = {
      dockerfile = "applications/portfolio_manager/Dockerfile";
      context = ".";
      ecrRepo = "fund/portfolio_manager-server";
    };
    training-server = {
      dockerfile = "tools/Dockerfile.prefect-server";
      context = ".";
      ecrRepo = "fund/training-server";
    };
    training-worker = {
      dockerfile = "tools/Dockerfile";
      context = ".";
      ecrRepo = "fund/training-worker";
    };
    grafana = {
      dockerfile = "Dockerfile";
      context = "dashboards";
      ecrRepo = "fund/grafana";
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

  env = {
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

  packages = [
    pkgs.git
    pkgs.curl
    pkgs.jq
    pkgs.rustup
    pkgs.cargo-watch
    pkgs.awscli2
    pkgs.grafana
    pkgs.pulumiPackages.pulumi-language-python
    pkgs.pulumi-bin
  ];

  # PostgreSQL for Prefect (local)
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
        static_configs = [{ targets = [ "localhost:8080" ]; }];
        metrics_path = "/metrics";
        scrape_interval = "15s";
      }
      {
        job_name = "ensemble-manager";
        static_configs = [{ targets = [ "localhost:8082" ]; }];
        metrics_path = "/metrics";
        scrape_interval = "15s";
      }
      {
        job_name = "portfolio-manager";
        static_configs = [{ targets = [ "localhost:8081" ]; }];
        metrics_path = "/metrics";
        scrape_interval = "15s";
      }
    ];
  };

  # --- AWS / Pulumi commands ---
  # All infra scripts unset MinIO env vars so the real AWS credentials are used.

  # Bring up AWS infrastructure with Pulumi
  scripts.infra-up.exec = ''
    unset AWS_ENDPOINT_URL AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY
    cd "$DEVENV_ROOT/infrastructure"
    pulumi up --stack production --yes
  '';

  # Tear down AWS infrastructure with Pulumi
  scripts.infra-down.exec = ''
    unset AWS_ENDPOINT_URL AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY
    cd "$DEVENV_ROOT/infrastructure"
    pulumi down --stack production --yes
  '';

  # Show Pulumi stack outputs (ALB URL, ECR repos, bucket names, etc.)
  scripts.infra-outputs.exec = ''
    unset AWS_ENDPOINT_URL AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY
    cd "$DEVENV_ROOT/infrastructure"
    pulumi stack output --stack production --json
  '';

  # Get the ALB base URL from Pulumi outputs
  scripts.infra-url.exec = ''
    unset AWS_ENDPOINT_URL AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY
    cd "$DEVENV_ROOT/infrastructure"
    pulumi stack output --stack production aws_alb_url 2>/dev/null || echo "Not deployed yet"
  '';

  # Build and push a Docker image to ECR
  scripts.ecr-push.exec = ''
    unset AWS_ENDPOINT_URL AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY
    SERVICE="$1"
    if [ -z "$SERVICE" ]; then
      echo "Usage: ecr-push <${lib.concatStringsSep "|" deployableServices}|all>"
      exit 1
    fi

    ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
    REGISTRY="$ACCOUNT_ID.dkr.ecr.${awsRegion}.amazonaws.com"

    echo "Logging into ECR..."
    aws ecr get-login-password --region ${awsRegion} | docker login --username AWS --password-stdin "$REGISTRY"

    ${lib.concatStringsSep "\n" (lib.mapAttrsToList (svc: def: ''
    push_${builtins.replaceStrings ["-"] ["_"] svc}() {
      local repo="${def.ecrRepo}"
      local image="$REGISTRY/$repo:latest"
      local ctx_dir="$DEVENV_ROOT/${def.context}"
      echo "=== Building ${svc} ==="
      docker build -t "$image" -f "$DEVENV_ROOT/${def.dockerfile}" "$ctx_dir"
      echo "=== Pushing ${svc} ==="
      docker push "$image"
      echo "OK: ${svc}"
    }
    '') ecsServices)}

    case "$SERVICE" in
      ${lib.concatStringsSep "\n" (map (svc: ''
      ${svc}) push_${builtins.replaceStrings ["-"] ["_"] svc} ;;'') deployableServices)}
      all)
        ${lib.concatStringsSep "\n        " (map (svc: ''
        push_${builtins.replaceStrings ["-"] ["_"] svc}'') deployableServices)}
        echo "All images pushed"
        ;;
      *) echo "Unknown service: $SERVICE"; exit 1 ;;
    esac
  '';

  # Force ECS services to redeploy with latest image
  scripts.ecs-deploy.exec = ''
    unset AWS_ENDPOINT_URL AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY
    SERVICE="$1"
    CLUSTER="fund-application"

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
      echo "OK: $ecs_name"
    }

    case "$SERVICE" in
      data-manager) deploy_ecs_service "fund-data-manager-server" ;;
      ensemble-manager) deploy_ecs_service "fund-ensemble-manager-server" ;;
      portfolio-manager) deploy_ecs_service "fund-portfolio-manager-server" ;;
      training-server) deploy_ecs_service "fund-training-server" ;;
      training-worker) deploy_ecs_service "fund-training-worker" ;;
      grafana) deploy_ecs_service "fund-grafana" ;;
      all)
        for svc in fund-data-manager-server fund-ensemble-manager-server fund-portfolio-manager-server fund-training-server fund-training-worker fund-grafana; do
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

  # Show ECS service status
  scripts.ecs-status.exec = ''
    unset AWS_ENDPOINT_URL AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY
    CLUSTER="fund-application"
    echo "=== ECS Services ==="
    aws ecs list-services --cluster "$CLUSTER" --region ${awsRegion} --query 'serviceArns[*]' --output table 2>/dev/null || echo "Cluster not found"
    echo ""
    for svc in fund-data-manager-server fund-ensemble-manager-server fund-portfolio-manager-server fund-training-server fund-training-worker fund-grafana; do
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
    unset AWS_ENDPOINT_URL AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY
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

  # Create Prefect work pool and register deployment on production
  scripts.training-init.exec = ''
    unset AWS_ENDPOINT_URL AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY
    cd "$DEVENV_ROOT/infrastructure"
    ALB_URL=$(pulumi stack output --stack production aws_alb_url 2>/dev/null)
    if [ -z "$ALB_URL" ]; then
      echo "ALB URL not found. Run 'infra-up' first."
      exit 1
    fi

    PROD_URL="''${ALB_URL%/}:4200/api"

    echo "Checking Prefect server at $PROD_URL..."
    for i in 1 2 3 4 5; do
      curl -sf "$PROD_URL/health" > /dev/null 2>&1 && break
      echo "  Waiting for Prefect server..."
      sleep 5
    done

    if ! curl -sf "$PROD_URL/health" > /dev/null 2>&1; then
      echo "Prefect server not reachable at $PROD_URL"
      exit 1
    fi

    S3_DATA=$(pulumi stack output --stack production aws_s3_data_bucket_name 2>/dev/null)
    S3_ARTIFACTS=$(pulumi stack output --stack production aws_s3_model_artifacts_bucket_name 2>/dev/null)

    echo "Creating fund-work-pool-local work pool..."
    PREFECT_API_URL="$PROD_URL" \
      uv run --package tools prefect work-pool create "fund-work-pool-local" --type process 2>/dev/null \
      || echo "  already exists"

    echo "Registering daily-training deployment..."
    PREFECT_API_URL="$PROD_URL" \
    AWS_S3_DATA_BUCKET_NAME="$S3_DATA" \
    AWS_S3_MODEL_ARTIFACTS_BUCKET_NAME="$S3_ARTIFACTS" \
      uv run --package tide python -m tide.deploy

    echo ""
    echo "Done. Dashboard: $ALB_URL:4200"
  '';

  # --- Local dev commands ---

  # Create Prefect work pool and register deployment locally
  scripts.training-setup.exec = ''
    echo "Waiting for Prefect server..."
    while ! curl -sf http://localhost:4200/api/health > /dev/null 2>&1; do
      sleep 2
    done

    echo "Creating fund-work-pool-local work pool..."
    PREFECT_API_URL="http://localhost:4200/api" \
      uv run --package tools prefect work-pool create "fund-work-pool-local" --type process 2>/dev/null \
      || echo "  already exists"

    echo "Registering daily-training deployment..."
    PREFECT_API_URL="http://localhost:4200/api" \
      uv run --package tide python -m tide.deploy

    echo ""
    echo "Done. Visit http://localhost:4200 to see the Prefect dashboard."
    echo "Run 'devenv up' to start workers that will pick up scheduled runs."
  '';

  scripts.cleanup-services.exec = ''
    for PORT in 3000 4200 5432 8080 8081 8082 9090; do
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

  tasks = {
    "models:tide:deploy".exec = ''
      uv run prefect --no-prompt deploy --all
      '';
    "models:tide:train".exec = ''
      uv run prefect deployment run tide-training-pipeline/tide-training
      '';
    "models:tide:train:local".exec = ''
      uv run prefect deployment run tide-training-pipeline/tide-training-local
      '';
  };

  # --- Local processes ---

  processes = {
    prefect-server.exec = ''
      while ! pg_isready -h 127.0.0.1 -p 5432 -U prefect -q 2>/dev/null; do
        sleep 1
      done
      export PREFECT_API_DATABASE_CONNECTION_URL="postgresql+asyncpg://prefect:prefect@127.0.0.1:5432/prefect"
      export PREFECT_UI_API_URL="http://localhost:4200/api"
      cd tools
      exec uv run prefect server start --host 0.0.0.0
    '';

    prefect-worker-1.exec = ''
      while ! curl -sf http://localhost:4200/api/health > /dev/null 2>&1; do
        sleep 2
      done

      # Create work pool and register deployment on first startup
      PREFECT_API_URL="http://localhost:4200/api" \
        uv run --package tools prefect work-pool create "fund-work-pool-local" --type process 2>/dev/null || true
      PREFECT_API_URL="http://localhost:4200/api" \
        uv run --package tide python -m tide.deploy 2>/dev/null || true

      cd tools
      exec uv run prefect worker start --pool fund-work-pool-local --name worker-1
    '';

    prefect-worker-2.exec = ''
      while ! curl -sf http://localhost:4200/api/health > /dev/null 2>&1; do
        sleep 2
      done
      sleep 3
      cd tools
      exec uv run prefect worker start --pool fund-work-pool-local --name worker-2
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

    grafana.exec = ''
      GRAFANA_DATA="$DEVENV_STATE/grafana"
      GRAFANA_PROV="$GRAFANA_DATA/provisioning"
      mkdir -p "$GRAFANA_PROV/datasources" "$GRAFANA_PROV/dashboards"

      cp "$DEVENV_ROOT/dashboards/local/datasources/prometheus.yaml" "$GRAFANA_PROV/datasources/"

      cat > "$GRAFANA_PROV/dashboards/dashboards.yaml" << EOF
      apiVersion: 1
      providers:
        - name: fund
          orgId: 1
          folder: Fund
          type: file
          disableDeletion: false
          updateIntervalSeconds: 10
          allowUiUpdates: true
          options:
            path: $DEVENV_ROOT/dashboards
            foldersFromFilesStructure: false
      EOF

      exec grafana server \
        --homepath "${pkgs.grafana}/share/grafana" \
        --config /dev/null \
        cfg:server.http_port=3000 \
        cfg:server.http_addr=0.0.0.0 \
        cfg:paths.data="$GRAFANA_DATA" \
        cfg:paths.provisioning="$GRAFANA_PROV" \
        cfg:security.admin_password=admin \
        cfg:auth.anonymous_enabled=true \
        cfg:auth.anonymous_org_role=Viewer
    '';

    ready.exec = ''
      while ! curl -sf http://localhost:3000/api/health > /dev/null 2>&1; do
        sleep 2
      done
      echo ""
      echo "========================================"
      echo "  Fund development environment ready"
      echo "========================================"
      echo ""
      echo "  PostgreSQL:       localhost:5432"
      echo "  Prometheus:       localhost:9090"
      echo "  Grafana:          localhost:3000  (admin/admin)"
      echo "  Prefect UI:       localhost:4200"
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
    echo "    Grafana:          localhost:3000  (admin/admin)"
    echo "    Prefect UI:       localhost:4200"
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
    echo "    training-init     Create work pool + register deployment (prod)"
    echo ""
    echo "  Local:"
    echo "    training-setup    Create work pool + register deployment (local)"
    echo "    cleanup-services  Kill stale local processes"
  '';

  enterTest = ''
  '';
}
