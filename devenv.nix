{ pkgs, lib, config, inputs, ... }:

let
  flyRegion = "ewr";
  flyOrg = "personal";

  # Fly.io app definitions — single source of truth for all services
  flyApps = {
    data-manager = {
      name = "fund-datamanager";
      dockerfile = "applications/data_manager/Dockerfile";
      context = ".";
      port = 8080;
      metrics = true;
      env = {
        ENVIRONMENT = "production";
        RUST_LOG = "info";
        AWS_REGION = "us-east-1";
      };
      secrets = [
        "AWS_S3_DATA_BUCKET_NAME"
        "AWS_ENDPOINT_URL_S3"
        "AWS_ENDPOINT_URL"
        "AWS_ACCESS_KEY_ID"
        "AWS_SECRET_ACCESS_KEY"
        "MASSIVE_BASE_URL"
        "MASSIVE_API_KEY"
      ];
      vm = { size = "shared-cpu-1x"; memory = "512mb"; };
    };

    ensemble-manager = {
      name = "fund-ensemblemanager";
      dockerfile = "applications/ensemble_manager/Dockerfile";
      context = ".";
      port = 8080;
      metrics = true;
      env = {
        ENVIRONMENT = "production";
        FUND_DATAMANAGER_BASE_URL = "http://fund-datamanager.internal:8080";
      };
      secrets = [
        "AWS_S3_MODEL_ARTIFACTS_BUCKET_NAME"
        "AWS_ENDPOINT_URL_S3"
        "AWS_ACCESS_KEY_ID"
        "AWS_SECRET_ACCESS_KEY"
      ];
      vm = { size = "shared-cpu-1x"; memory = "512mb"; };
    };

    portfolio-manager = {
      name = "fund-portfoliomanager";
      dockerfile = "applications/portfolio_manager/Dockerfile";
      context = ".";
      port = 8080;
      metrics = true;
      env = {
        ENVIRONMENT = "production";
        FUND_DATAMANAGER_BASE_URL = "http://fund-datamanager.internal:8080";
        FUND_ENSEMBLEMANAGER_BASE_URL = "http://fund-ensemblemanager.internal:8080";
      };
      secrets = [
        "ALPACA_API_KEY_ID"
        "ALPACA_API_SECRET"
        "AWS_ACCESS_KEY_ID"
        "AWS_SECRET_ACCESS_KEY"
      ];
      vm = { size = "shared-cpu-1x"; memory = "512mb"; };
    };

    prefect-server = {
      name = "fund-prefect-server";
      dockerfile = "tools/Dockerfile.prefect-server";
      context = ".";
      port = 4200;
      env = {
        PREFECT_UI_API_URL = "http://fund-prefect-server.internal:4200/api";
      };
      secrets = [
        "PREFECT_API_DATABASE_CONNECTION_URL"
      ];
      vm = { size = "shared-cpu-1x"; memory = "1024mb"; };
    };

    prefect-worker = {
      name = "fund-prefect-worker";
      dockerfile = "tools/Dockerfile";
      context = ".";
      port = 8080;
      env = {
        PREFECT_API_URL = "http://fund-prefect-server.internal:4200/api";
        ENVIRONMENT = "production";
      };
      secrets = [
        "AWS_S3_DATA_BUCKET_NAME"
        "AWS_S3_MODEL_ARTIFACTS_BUCKET_NAME"
        "AWS_ENDPOINT_URL_S3"
        "AWS_ACCESS_KEY_ID"
        "AWS_SECRET_ACCESS_KEY"
        "MLFLOW_TRACKING_URI"
      ];
      vm = { size = "performance-2x"; memory = "4096mb"; };
    };

    mlflow = {
      name = "fund-mlflow";
      dockerfile = "tools/Dockerfile.mlflow";
      context = ".";
      port = 8080;
      metrics = false;
      env = {
        MLFLOW_HOST = "0.0.0.0";
        MLFLOW_PORT = "8080";
        MLFLOW_SERVER_DISABLE_SECURITY_MIDDLEWARE = "true";
      };
      secrets = [
        "MLFLOW_BACKEND_STORE_URI"
        "MLFLOW_DEFAULT_ARTIFACT_ROOT"
        "AWS_ACCESS_KEY_ID"
        "AWS_SECRET_ACCESS_KEY"
        "AWS_ENDPOINT_URL_S3"
      ];
      vm = { size = "shared-cpu-2x"; memory = "2048mb"; };
    };

    grafana = {
      name = "fund-grafana";
      dockerfile = "Dockerfile";
      context = "dashboards";
      port = 8080;
      env = {
        GF_SERVER_HTTP_PORT = "8080";
        GF_AUTH_ANONYMOUS_ENABLED = "false";
      };
      secrets = [
        "GF_SECURITY_ADMIN_PASSWORD"
        "FLY_PROMETHEUS_TOKEN"
      ];
      vm = { size = "shared-cpu-1x"; memory = "256mb"; };
    };
  };

  # Services that get deployed via `deploy <name|all>`
  deployableServices = [ "data-manager" "ensemble-manager" "portfolio-manager" "prefect-server" "prefect-worker" "mlflow" "grafana" ];

  # Generate fly.toml content for a service
  mkFlyToml = name: app: let
    envLines = lib.concatStringsSep "\n" (
      lib.mapAttrsToList (k: v: "  ${k} = \"${v}\"") app.env
    );
    autoStop = if name == "prefect-server" || name == "prefect-worker" || name == "mlflow"
      then "off" else "suspend";
    minMachines = if name == "prefect-server" || name == "mlflow" then "1" else "0";
    metricsBlock = if app ? metrics then ''

[metrics]
  port = ${toString app.port}
  path = "/metrics"
'' else "";
    restartBlock = if name == "prefect-worker" then ''

[[restart]]
  policy = "always"
'' else "";
    # All services except prefect-worker get [http_service] for internal routing
    # and fly proxy access. Only Grafana has public IPs allocated (in fly-init),
    # so only Grafana is publicly reachable. Internal services don't force HTTPS
    # since fly proxy uses plain HTTP over WireGuard.
    forceHttps = if name == "grafana" || name == "mlflow" then "true" else "false";
    httpBlock = if name == "prefect-worker" then "" else ''

[http_service]
  internal_port = ${toString app.port}
  force_https = ${forceHttps}
  auto_stop_machines = "${autoStop}"
  auto_start_machines = true
  min_machines_running = ${minMachines}

  [[http_service.checks]]
    grace_period = "60s"
    interval = "30s"
    method = "GET"
    path = "/health"
    timeout = "10s"
'';
  in ''
app = "${app.name}"
primary_region = "${flyRegion}"

[build]
  dockerfile = "${app.dockerfile}"

[env]
${envLines}
${httpBlock}${metricsBlock}${restartBlock}
[[vm]]
  size = "${app.vm.size}"
  memory = "${app.vm.memory}"
  '';

in {
  env = {
    # MinIO credentials
    MINIO_ROOT_USER = "minioadmin";
    MINIO_ROOT_PASSWORD = "minioadmin";

    # AWS env vars pointing to local MinIO
    AWS_REGION = "us-east-1";
    AWS_DEFAULT_REGION = "us-east-1";
    AWS_ACCESS_KEY_ID = "minioadmin";
    AWS_SECRET_ACCESS_KEY = "minioadmin";
    AWS_ENDPOINT_URL = "http://localhost:9000";
    AWS_S3_DATA_BUCKET_NAME = "fund-data";
    AWS_S3_MODEL_ARTIFACTS_BUCKET_NAME = "fund-model-artifacts";

    # Service URLs (localhost, not container names)
    FUND_DATAMANAGER_BASE_URL = "http://localhost:8080";
    FUND_ENSEMBLEMANAGER_BASE_URL = "http://localhost:8082";
    PREFECT_API_URL = "http://localhost:4200/api";

    # MLflow tracking (centralized on Fly.io)
    MLFLOW_TRACKING_URI = "https://fund-mlflow.fly.dev";

    # Development defaults
    ENVIRONMENT = "development";
    DISABLE_DISK_CACHE = "1";
    BACKFILL_LOOKBACK_DAYS = "1460";
  };

  packages = [
    pkgs.git
    pkgs.curl
    pkgs.jq
    pkgs.minio-client
    pkgs.rustup
    pkgs.cargo-watch
    pkgs.flyctl
    pkgs.grafana
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

  # MinIO for S3-compatible object storage (local)
  services.minio = {
    enable = true;
    accessKey = "minioadmin";
    secretKey = "minioadmin";
    buckets = [
      "fund-data"
      "fund-model-artifacts"
    ];
  };

  # --- Fly.io commands ---

  # Deploy services to Fly.io
  scripts.deploy.exec = ''
    SERVICE="$1"
    if [ -z "$SERVICE" ]; then
      echo "Usage: deploy <${lib.concatStringsSep "|" deployableServices}|all>"
      exit 1
    fi

    ${lib.concatStringsSep "\n" (map (svc: let app = flyApps.${svc}; ctx = if app ? context then app.context else "."; in ''
    deploy_${builtins.replaceStrings ["-"] ["_"] svc}() {
      local ctx_dir="$DEVENV_ROOT/${ctx}"
      local fly_toml
      fly_toml=$(mktemp "$ctx_dir/.fly-${svc}-XXXXXX.toml")
      cat > "$fly_toml" << 'FLYEOF'
${mkFlyToml svc app}
FLYEOF
      echo "=== Deploying ${svc} ==="
      cd "$DEVENV_ROOT" && fly deploy "$ctx_dir" --config "$fly_toml"
      local rc=$?
      rm -f "$fly_toml"
      if [ $rc -ne 0 ]; then
        echo "FAILED: ${svc}"
        return $rc
      fi
      echo "OK: ${svc}"
    }
    '') deployableServices)}

    ensure_tigris_buckets() {
      echo "=== Ensuring Tigris storage buckets ==="
      fly storage create -a fund-datamanager -n fund-data 2>/dev/null || echo "  fund-data already exists"
      fly storage create -a fund-prefect-worker -n fund-model-artifacts 2>/dev/null || echo "  fund-model-artifacts already exists"
    }

    case "$SERVICE" in
      ${lib.concatStringsSep "\n" (map (svc: ''
      ${svc}) deploy_${builtins.replaceStrings ["-"] ["_"] svc} ;;'') deployableServices)}
      all)
        ensure_tigris_buckets
        pids=""
        failed=""
        ${lib.concatStringsSep "\n        " (map (svc: ''
        deploy_${builtins.replaceStrings ["-"] ["_"] svc} &
        pids="$pids $!"'') deployableServices)}
        for pid in $pids; do
          if ! wait "$pid"; then
            failed="$failed $pid"
          fi
        done
        if [ -n "$failed" ]; then
          echo "Some deploys failed"
          exit 1
        fi
        echo "All services deployed"

        echo ""
        echo "=== Setting up Prefect ==="
        echo "Starting proxy to Prefect server..."
        fly proxy 4201:4200 -a fund-prefect-server &
        PROXY_PID=$!
        sleep 3
        PROD_URL="http://localhost:4201/api"
        for i in 1 2 3 4 5; do
          curl -sf "$PROD_URL/health" > /dev/null 2>&1 && break
          echo "  Waiting for Prefect server..."
          sleep 5
        done
        PREFECT_API_URL="$PROD_URL" \
          uv run --package tools prefect work-pool create "training-pool" --type process 2>/dev/null \
          || echo "  training-pool already exists"
        PREFECT_API_URL="$PROD_URL" \
        AWS_S3_DATA_BUCKET_NAME="fund-data" \
        AWS_S3_MODEL_ARTIFACTS_BUCKET_NAME="fund-model-artifacts" \
          uv run --package tide python -m tide.deploy
        kill $PROXY_PID 2>/dev/null || true
        echo "Prefect deployment registered"
        ;;
      *) echo "Unknown service: $SERVICE"; exit 1 ;;
    esac
  '';

  # First-time Fly.io setup: create apps, postgres, storage, secrets, and Prefect
  scripts.fly-init.exec = ''
    echo "=== Creating Fly.io apps ==="
    ${lib.concatStringsSep "\n" (lib.mapAttrsToList (svc: app: ''
      echo "  ${app.name}"
      fly apps create "${app.name}" 2>/dev/null || echo "    already exists"
    '') flyApps)}

    echo ""
    echo "=== Allocating public IPs (Grafana + MLflow) ==="
    fly ips allocate-v4 --shared -a fund-grafana 2>/dev/null || echo "  already exists"
    fly ips allocate-v6 -a fund-grafana 2>/dev/null || echo "  already exists"
    fly ips allocate-v4 --shared -a fund-mlflow 2>/dev/null || echo "  already exists"
    fly ips allocate-v6 -a fund-mlflow 2>/dev/null || echo "  already exists"

    echo ""
    echo "=== Creating Fly Postgres (for Prefect) ==="
    pg_output=$(fly postgres create --name fund-prefect-db --region ${flyRegion} --vm-size shared-cpu-1x --vm-memory 1024 --initial-cluster-size 1 --volume-size 1 2>&1) \
      && echo "$pg_output" \
      || echo "  already exists"

    attach_output=$(fly postgres attach fund-prefect-db -a fund-prefect-server 2>&1) \
      && echo "$attach_output" \
      || echo "  already attached"

    # Extract credentials from attach output and set Prefect-compatible connection URL
    db_url=$(echo "$attach_output" | grep DATABASE_URL | sed 's/.*DATABASE_URL=//' | xargs)
    if [ -n "$db_url" ]; then
      # Convert postgres://user:pass@host:port/db?sslmode=disable
      # to postgresql+asyncpg://user:pass@host.internal:5433/db (direct, no SSL)
      pg_user=$(echo "$db_url" | sed 's|postgres://||' | cut -d: -f1)
      pg_pass=$(echo "$db_url" | sed 's|postgres://||' | cut -d: -f2 | cut -d@ -f1)
      pg_db=$(echo "$db_url" | sed 's|.*/||' | cut -d? -f1)
      asyncpg_url="postgresql+asyncpg://''${pg_user}:''${pg_pass}@fund-prefect-db.internal:5433/''${pg_db}"
      echo "  Setting PREFECT_API_DATABASE_CONNECTION_URL"
      fly secrets set -a fund-prefect-server "PREFECT_API_DATABASE_CONNECTION_URL=$asyncpg_url"
    else
      echo "  Postgres already attached, skipping connection URL setup"
    fi

    echo ""
    echo "=== Creating Fly Postgres (for MLflow) ==="
    mlflow_pg_output=$(fly postgres create --name fund-mlflow-db --region ${flyRegion} --vm-size shared-cpu-1x --vm-memory 256 --initial-cluster-size 1 --volume-size 1 2>&1) \
      && echo "$mlflow_pg_output" \
      || echo "  already exists"

    mlflow_attach_output=$(fly postgres attach fund-mlflow-db -a fund-mlflow 2>&1) \
      && echo "$mlflow_attach_output" \
      || echo "  already attached"

    mlflow_db_url=$(echo "$mlflow_attach_output" | grep DATABASE_URL | sed 's/.*DATABASE_URL=//' | sed 's/?sslmode=disable//' | xargs)
    if [ -n "$mlflow_db_url" ]; then
      # MLflow requires postgresql:// scheme, not postgres://
      mlflow_db_url=$(echo "$mlflow_db_url" | sed 's|^postgres://|postgresql://|')
      echo "  Setting MLFLOW_BACKEND_STORE_URI"
      fly secrets set -a fund-mlflow "MLFLOW_BACKEND_STORE_URI=$mlflow_db_url"
    else
      echo "  Postgres already attached, skipping connection URL setup"
    fi

    echo ""
    echo "=== Creating Tigris storage buckets ==="
    tigris_output=$(fly storage create -a fund-datamanager -n fund-data 2>&1) \
      && echo "$tigris_output" \
      || echo "  fund-data already exists"

    fly storage create -a fund-prefect-worker -n fund-model-artifacts 2>/dev/null || echo "  fund-model-artifacts already exists"
    fly storage create -a fund-mlflow -n fund-mlflow-artifacts 2>/dev/null || echo "  fund-mlflow-artifacts already exists"

    # Share Tigris credentials with apps that need S3 access
    tigris_key=$(echo "$tigris_output" | grep AWS_ACCESS_KEY_ID | awk '{print $2}')
    tigris_secret=$(echo "$tigris_output" | grep AWS_SECRET_ACCESS_KEY | awk '{print $2}')
    if [ -n "$tigris_key" ] && [ -n "$tigris_secret" ]; then
      echo ""
      echo "=== Sharing Tigris credentials ==="
      for app in fund-datamanager fund-ensemblemanager fund-prefect-worker fund-mlflow; do
        echo "  $app"
        fly secrets set -a "$app" \
          AWS_ACCESS_KEY_ID="$tigris_key" \
          AWS_SECRET_ACCESS_KEY="$tigris_secret" \
          AWS_ENDPOINT_URL_S3="https://fly.storage.tigris.dev" \
          AWS_REGION="us-east-1" \
          2>/dev/null || true
      done
      fly secrets set -a fund-datamanager \
        AWS_S3_DATA_BUCKET_NAME="fund-data" \
        AWS_ENDPOINT_URL="https://fly.storage.tigris.dev" \
        2>/dev/null || true
      fly secrets set -a fund-ensemblemanager \
        AWS_S3_MODEL_ARTIFACTS_BUCKET_NAME="fund-model-artifacts" \
        2>/dev/null || true
      fly secrets set -a fund-prefect-worker \
        AWS_S3_DATA_BUCKET_NAME="fund-data" \
        AWS_S3_MODEL_ARTIFACTS_BUCKET_NAME="fund-model-artifacts" \
        2>/dev/null || true
      fly secrets set -a fund-mlflow \
        MLFLOW_DEFAULT_ARTIFACT_ROOT="s3://fund-mlflow-artifacts" \
        2>/dev/null || true
      fly secrets set -a fund-prefect-worker \
        MLFLOW_TRACKING_URI="https://fund-mlflow.fly.dev" \
        2>/dev/null || true
    else
      echo "  Tigris already exists, push credentials manually with fly-secrets"
    fi

    echo ""
    echo "=== Setting up Grafana secrets ==="
    PROM_TOKEN=$(echo "${flyOrg}" | fly tokens create readonly -n "grafana-prometheus" 2>/dev/null | tail -1)
    if [ -n "$PROM_TOKEN" ]; then
      fly secrets set -a fund-grafana \
        "GF_SECURITY_ADMIN_PASSWORD=$(openssl rand -base64 24)" \
        "FLY_PROMETHEUS_TOKEN=FlyV1 $PROM_TOKEN" \
        2>/dev/null || echo "  secrets already set"
    else
      echo "  Failed to create Prometheus token, set FLY_PROMETHEUS_TOKEN manually"
    fi

    echo ""
    echo "Done. Next steps:"
    echo "  1. pull-secrets      (fetch secrets from AWS Secrets Manager)"
    echo "  2. direnv allow      (reload environment)"
    echo "  3. fly-secrets       (push remaining secrets to Fly apps)"
    echo "  4. deploy all        (deploy everything)"
    echo "  5. prefect-init      (create work pool and register deployment)"
  '';

  # Pull secrets from AWS Secrets Manager into .envrc
  scripts.pull-secrets.exec = ''
    ENVRC="$DEVENV_ROOT/.envrc"
    AWS_CMD="aws --region us-east-1"
    unset AWS_ENDPOINT_URL AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY
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
          echo "export $key=$val" >> "$ENVRC"
          echo "    $key (added)"
        fi
      done
    done

    echo ""
    echo "Done. Run 'direnv allow' to reload."
  '';

  # Push secrets from current environment to Fly.io apps
  scripts.fly-secrets.exec = ''
    ${lib.concatStringsSep "\n" (lib.mapAttrsToList (svc: app: let
      secretChecks = lib.concatStringsSep "\n" (map (s: ''
        val="''${${s}:-}"
        if [ -z "$val" ]; then
          echo "  WARNING: ${s} is not set in environment, skipping"
          missing=1
        else
          args="$args ${s}=$val"
        fi
      '') app.secrets);
    in ''
      echo "=== ${app.name} ==="
      args=""
      missing=0
      ${secretChecks}
      if [ -n "$args" ]; then
        fly secrets set -a "${app.name}" $args
        echo "  Secrets pushed"
      fi
      if [ "$missing" = "1" ]; then
        echo "  Some secrets missing - set them in your environment and re-run"
      fi
      echo ""
    '') flyApps)}
  '';

  # Show Fly.io app status
  scripts.fly-status.exec = ''
    ${lib.concatStringsSep "\n" (lib.mapAttrsToList (svc: app: ''
      echo "=== ${app.name} ==="
      fly status -a "${app.name}" 2>/dev/null || echo "  not deployed"
      echo ""
    '') flyApps)}
  '';

  # Tear down all Fly.io resources
  scripts.fly-destroy.exec = ''
    echo "This will destroy ALL Fly.io apps, database, and storage. Are you sure? (y/N)"
    read -r confirm
    if [ "$confirm" = "y" ]; then
      ${lib.concatStringsSep "\n" (lib.mapAttrsToList (svc: app: ''
        echo "Destroying ${app.name}..."
        fly apps destroy "${app.name}" --yes 2>/dev/null || true
      '') flyApps)}
      echo "Destroying fund-prefect-db..."
      fly apps destroy fund-prefect-db --yes 2>/dev/null || true
      echo "Destroying fund-mlflow-db..."
      fly apps destroy fund-mlflow-db --yes 2>/dev/null || true
    fi
  '';

  # Deploy Grafana dashboards (shortcut for deploy grafana)
  scripts.dashboards-push.exec = ''
    deploy grafana
  '';

  # Create Prefect work pool and register deployment on production
  # Requires: fly-proxy prefect (running in another terminal)
  scripts.prefect-init.exec = ''
    PROD_URL="http://localhost:4201/api"

    echo "Checking Prefect server via fly proxy..."
    echo "(Make sure 'fly-proxy prefect' is running in another terminal)"
    if ! curl -sf "$PROD_URL/health" > /dev/null 2>&1; then
      echo "Prefect server not reachable at $PROD_URL"
      echo "Run 'fly-proxy prefect' in another terminal first"
      exit 1
    fi

    echo "Creating training-pool work pool..."
    PREFECT_API_URL="$PROD_URL" \
      uv run --package tools prefect work-pool create "training-pool" --type process 2>/dev/null \
      || echo "  already exists"

    echo "Registering daily-training deployment..."
    PREFECT_API_URL="$PROD_URL" \
    AWS_S3_DATA_BUCKET_NAME="fund-data" \
    AWS_S3_MODEL_ARTIFACTS_BUCKET_NAME="fund-model-artifacts" \
      uv run --package tide python -m tide.deploy

    echo ""
    echo "Done. Dashboard: http://localhost:4201 (via fly-proxy prefect)"
  '';

  # Proxy to internal Fly.io services for local access
  scripts.fly-proxy.exec = ''
    SERVICE="$1"
    case "$SERVICE" in
      mlflow)
        echo "MLflow UI: http://localhost:5050"
        fly proxy 5050:8080 -a fund-mlflow
        ;;
      prefect)
        echo "Prefect UI: http://localhost:4201"
        fly proxy 4201:4200 -a fund-prefect-server
        ;;
      data-manager)
        echo "Data Manager: http://localhost:8090"
        fly proxy 8090:8080 -a fund-datamanager
        ;;
      *)
        echo "Usage: fly-proxy <mlflow|prefect|data-manager>"
        echo ""
        echo "Proxies to internal Fly.io services (no public endpoints)."
        echo "MLflow tracking URI is already set to http://localhost:5050"
        echo "so run 'fly-proxy mlflow' before training locally."
        ;;
    esac
  '';

  # --- Local dev commands ---

  # Create Prefect work pool and register deployment locally
  scripts.prefect-setup.exec = ''
    echo "Waiting for Prefect server..."
    while ! curl -sf http://localhost:4200/api/health > /dev/null 2>&1; do
      sleep 2
    done

    echo "Creating training-pool work pool..."
    PREFECT_API_URL="http://localhost:4200/api" \
      uv run --package tools prefect work-pool create "training-pool" --type process 2>/dev/null \
      || echo "  already exists"

    echo "Registering daily-training deployment..."
    PREFECT_API_URL="http://localhost:4200/api" \
      uv run --package tide python -m tide.deploy

    echo ""
    echo "Done. Visit http://localhost:4200 to see the Prefect dashboard."
    echo "Run 'devenv up' to start workers that will pick up scheduled runs."
  '';

  scripts.cleanup-services.exec = ''
    for PORT in 3000 4200 5432 8080 8081 8082 9000 9001 9090; do
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
        uv run --package tools prefect work-pool create "training-pool" --type process 2>/dev/null || true
      PREFECT_API_URL="http://localhost:4200/api" \
        uv run --package tide python -m tide.deploy 2>/dev/null || true

      cd tools
      exec uv run prefect worker start --pool training-pool --name worker-1
    '';

    prefect-worker-2.exec = ''
      while ! curl -sf http://localhost:4200/api/health > /dev/null 2>&1; do
        sleep 2
      done
      sleep 3
      cd tools
      exec uv run prefect worker start --pool training-pool --name worker-2
    '';

    data-manager.exec = ''
      while ! curl -sf http://localhost:9000/minio/health/live > /dev/null 2>&1; do
        sleep 1
      done
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
      echo "  MinIO API:        localhost:9000"
      echo "  MinIO Console:    localhost:9001"
      echo "  Prometheus:       localhost:9090"
      echo "  Grafana:          localhost:3000  (admin/admin)"
      echo "  MLflow:           https://fund-mlflow.fly.dev"
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
    echo "    MinIO API:        localhost:9000"
    echo "    MinIO Console:    localhost:9001"
    echo "    Prometheus:       localhost:9090"
    echo "    Grafana:          localhost:3000  (admin/admin)"
    echo "    Prefect UI:       localhost:4200"
    echo "    Data Manager:     localhost:8080"
    echo "    Ensemble Manager: localhost:8082"
    echo "    Portfolio Manager: localhost:8081"
    echo ""
    echo "    MLflow:           https://fund-mlflow.fly.dev"
    echo ""
    echo "  Fly.io:"
    echo "    fly-init          Create apps, Postgres, Tigris, secrets"
    echo "    pull-secrets      AWS Secrets Manager -> .envrc"
    echo "    fly-secrets       Environment -> Fly.io secrets"
    echo "    deploy <svc|all>  Deploy to production"
    echo "    prefect-init      Create work pool + register deployment (prod)"
    echo "    fly-proxy <svc>   Proxy to internal Fly.io service"
    echo "    fly-status        Show app status"
    echo "    fly-destroy       Tear down everything"
    echo "    dashboards-push   Deploy Grafana with dashboards"
    echo ""
    echo "  Local:"
    echo "    prefect-setup     Create work pool + register deployment (local)"
    echo "    cleanup-services  Kill stale local processes"
  '';
}
