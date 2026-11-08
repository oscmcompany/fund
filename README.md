# fund

> The open source capital management company

## About

The **fund** repository holds the resources for the Open Source Capital Management platform.

The project is actively a work-in-progress.

## Project

Below are resources for the project and repository.

### Setup

```sh
# Install Determinate Nix and devenv (https://devenv.sh/getting-started/) at
# the versions in host/versions, then clone the repository.
git clone git@github.com:oscmcompany/fund.git
cd fund

# Name your secretspec profile (add it to secretspec.toml if it is new); its
# secrets are read from AWS Parameter Store with your AWS credentials.
echo "FUND_PROFILE=development/<name>" > .env

# Enter the environment - available tasks are printed on shell entry.
devenv shell

# Run every check.
devenv tasks run checks:all
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
