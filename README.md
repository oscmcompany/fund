# fund

> The open source capital management company  

<!-- markdownlint-disable-next-line MD013 -->
[![Code checks](https://github.com/oscmcompany/fund/actions/workflows/run_code_checks.yaml/badge.svg)](https://github.com/oscmcompany/fund/actions/workflows/run_code_checks.yaml) [![Test coverage](https://coveralls.io/repos/github/oscmcompany/fund/badge.svg?branch=master)](https://coveralls.io/github/oscmcompany/fund?branch=master)

## About

The **fund** repository holds the resources for the Open Source Capital Management platform.

The project is actively a work-in-progress.  

## Project

Below are resources for the project and repository.

### Setup

On a fresh machine with the repo cloned, run the bootstrap script:

```sh
./tools/bootstrap-machine         # install nix, devenv, and build the environment
./tools/bootstrap-machine --prod  # also pull production secrets from AWS
```

Once bootstrapped:

```sh
devenv shell  # enter the development environment
devenv up     # start local services (postgres, prometheus, prefect, etc.)
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
