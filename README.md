# fund

> The open source capital management company

## About

The **fund** repository holds the resources for the Open Source Capital Management platform.

The project is actively a work-in-progress.

## Project

Below are resources for the project and repository.

### Setup

#### Local

For local development, you can use the `devenv` tool to spin up a local environment with all dependencies.

```sh
git clone https://github.com/oscmcompany/fund.git && cd fund
bash tools/bootstrap-machine --profile development/first_name.last_name
devenv --profile application up
```

#### Remote

For remote development or production instances, you can provision VMs on `exe.dev`.

```sh
# Include the --production flag for production instances
bash tools/provision-application-vm
bash tools/provision-trainer-vm
```

#### Data

After launching, the database has the schema applied and equity details inserted but historical data must
be manually populated. Run the following commands locally or SSH into the application VM to backfill data.

```sh
# Backfill equity bars to S3 Parquet for model training
BACKFILL_START_DATE=YYYY-MM-DD devenv tasks run database:backfill

# Backfill equity bars to S3 Parquet for training (no PostgreSQL needed)
BACKFILL_START_DATE=YYYY-MM-DD devenv tasks run data:backfill-s3-equity-bars
```

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
