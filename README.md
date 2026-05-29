# fund

> The open source capital management company

## About

The **fund** repository holds the resources for the Open Source Capital Management platform.

The project is actively a work-in-progress.

## Project

Below are resources for the project and repository.

### Setup

On a fresh machine with the repository cloned, run the bootstrap script:

```sh
./tools/bootstrap-machine --profile dev/yourname
./tools/bootstrap-machine --profile production --prod  # production with secret validation
```

Add the `devenv shell` hook to auto-activate the environment on `cd`:

```sh
# zsh (~/.zshrc)
eval "$(devenv hook zsh)"

# bash (~/.bashrc)
eval "$(devenv hook bash)"

# fish (~/.config/fish/config.fish)
devenv hook fish | source

# nushell (config.nu)
devenv hook nu | save --force ~/.cache/devenv/hook.nu
source ~/.cache/devenv/hook.nu
```

Then trust the project directory:

```sh
devenv allow
```

Once bootstrapped:

```sh
# Environment auto-activates on cd with devenv hook configured
devenv shell                    # or: enter manually without hook
devenv --profile apps up        # start application services
devenv --profile ml shell       # machine learning training environment
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
