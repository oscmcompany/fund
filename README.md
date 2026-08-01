# fund

> The open source capital management company

## About

The **fund** repository holds the resources for the Open Source Capital Management platform.

The project is actively a work-in-progress.

## Project

Below are resources for the project and repository.

### Setup

#### Local

```sh
# Install devenv (https://devenv.sh/getting-started/), clone the repository
# and enter the directory - available commands are printed on shell entry, and
# start the application services.
git clone git@github.com:oscmcompany/fund.git
cd fund
devenv --profile application up
```

#### Remote

```sh
# Provision the application VM from your local machine then SSH in and
# start services with sync cron.
provision-production-application-vm
ssh oscm-fund-production-application.vm.exe.dev
start-application

# Seed ticker metadata and historical bars into PostgreSQL on the running
# application. Run without arguments for full usage and options. The pair screen
# needs 60 sessions of aligned closes and the model 70, so allow six months.
SEED_START_DATE=YYYY-MM-DD devenv tasks run data:seed

# Share the VM with the team and publish the dashboard externally.
ssh exe.dev share add oscm-fund-production-application team
ssh exe.dev share access allow oscm-fund-production-application
ssh exe.dev publish oscm-fund-production-application 8084:8084

# Provision the trainer VM from your local machine then SSH in and install
# the training cron job.
provision-production-trainer-vm
ssh oscm-fund-production-trainer.vm.exe.dev
start-trainer

# Launch DuckDB for a local query interface against S3 with all data lake views
# pre-loaded. Once in the DuckDB shell, run .help for commands and SHOW TABLES
# to list loaded views.
start-duckdb oscm-fund-production
```

> For development VMs, run the equivalent `development` scripts.
> The provision script handles environment-specific configuration.

#### Notes

- Application services run in a tmux session; attach with `tmux attach -t fund`
- Two processes run there: `fund`, the service, and `dashboard`. The service is one event loop woken
  by pg_cron through `LISTEN`/`NOTIFY`, not a set of per-module workers
- Dashboard is available at `http://<vm-name>.vm.exe.dev:8084`
- Git sync checks for updates every minute on both VMs; view logs at
  `/var/log/fund/sync-application.log` and `/var/log/fund/sync-trainer.log`
- Training runs weekdays at 23:00 UTC — post-close Eastern year-round, so the artifact is ready the
  evening before the session that uses it; view logs at `/var/log/fund/train-tide-model.log`
- The local `~/lab.duckdb` file is scratch space. It can be deleted and rebuilt from S3 at any time.

### Principles

An unordered and non-exhaustive list we work towards:

> Test in production  
> Always roll forward  
> Systems over process  
> No code is good code  
> Never write documentation  
> Be explicit  
> Git is truth  

### Links

Check out [our tasks](https://github.com/orgs/oscmcompany/projects/1) to see what we're working on or
ping [either](https://x.com/forstmeier) of [us](https://x.com/hyperpriorai) for anything else.
