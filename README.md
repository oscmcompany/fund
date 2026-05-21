# fund

> The open source capital management company

## About

The **fund** repository holds the resources for the Open Source Capital Management platform.

The project is actively a work-in-progress.

## Project

Below are resources for the project and repository.

### Setup

On a fresh machine with the repo cloned, run the bootstrap script:

```sh
./tools/bootstrap-machine         # install nix, devenv, and build the environment
./tools/bootstrap-machine --prod  # also configure production env vars and validate secrets
```

Once bootstrapped:

```sh
devenv shell                    # enter the development environment
devenv --profile apps up        # start application services
devenv --profile ml shell       # ML training environment
```

Production runs on a VM with `devenv --profile apps up` and secretspec for secret injection.

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
