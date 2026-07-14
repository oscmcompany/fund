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
# Development
provision-development-application-vm
provision-development-trainer-vm

# Production
provision-production-application-vm
provision-production-trainer-vm
```

#### Data

After launching, the database has the schema applied but equity details and historical bars must
be manually populated. Use the data seeding tasks to bootstrap both S3 and PostgreSQL.

```sh
# Full bootstrap: seed equity details and bars into S3 and PostgreSQL
SEED_SOURCE=massive SEED_START_DATE=YYYY-MM-DD devenv tasks run data:seed

# Or run individually:

# Seed equity details (embedded CSV) into S3 and/or PostgreSQL
SEED_TARGET=all devenv tasks run data:equity-details

# Seed equity bars from Massive API or S3 into S3 and/or PostgreSQL
SEED_SOURCE=massive SEED_TARGET=s3 SEED_START_DATE=YYYY-MM-DD devenv tasks run data:equity-bars

# Populate PostgreSQL from existing S3 Parquet (avoids re-fetching from Massive)
SEED_SOURCE=s3 SEED_TARGET=postgresql SEED_START_DATE=YYYY-MM-DD devenv tasks run data:equity-bars
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
