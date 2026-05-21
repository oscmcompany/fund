#!/usr/bin/env bash
set -euo pipefail

# --------------------------------------------------------------------------- #
# install.sh -- Provision a fund VM on exe.dev from a local machine
#
# Run locally:  bash tools/install.sh
# --------------------------------------------------------------------------- #

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

VM_NAME=""
MODE=""
DEV_NAME=""
AWS_ACCESS_KEY=""
AWS_SECRET_KEY=""
AWS_REGION=""
VM_TAGS=()
VM_BRANCH=""

# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #

step() {
  echo ""
  echo "==> $1"
}

fail() {
  echo "ERROR: $1" >&2
  exit 1
}

prompt() {
  local message="$1"
  local default="${2:-}"
  local result
  if [[ -n "$default" ]]; then
    printf "%s [%s]: " "$message" "$default" >&2
  else
    printf "%s: " "$message" >&2
  fi
  read -r result
  echo "${result:-$default}"
}

confirm() {
  local message="$1"
  local answer
  printf "%s [Y/n]: " "$message"
  read -r answer
  case "${answer:-Y}" in
    [Yy]*) return 0 ;;
    *) return 1 ;;
  esac
}

remote() {
  ssh -o ConnectTimeout=10 -o StrictHostKeyChecking=accept-new "$VM_NAME.exe.xyz" "$@"
}

remote_tty() {
  ssh -t -o ConnectTimeout=10 -o StrictHostKeyChecking=accept-new "$VM_NAME.exe.xyz" "$@"
}

remote_long() {
  ssh -o ConnectTimeout=10 -o StrictHostKeyChecking=accept-new -o ServerAliveInterval=60 "$VM_NAME.exe.xyz" "$@"
}

remote_long_tty() {
  ssh -t -o ConnectTimeout=10 -o StrictHostKeyChecking=accept-new -o ServerAliveInterval=60 "$VM_NAME.exe.xyz" "$@"
}

# Source Nix in a remote command chain
NIX_SOURCE='for p in /nix/var/nix/profiles/default/etc/profile.d/nix-daemon.sh /etc/profile.d/nix.sh; do [ -f "$p" ] && . "$p" && break; done'

# --------------------------------------------------------------------------- #
# Phase 1: Collect local info
# --------------------------------------------------------------------------- #

phase_collect() {
  step "Collecting configuration"

  # AWS credentials
  echo ""
  if [[ -f "$HOME/.aws/credentials" ]] && command -v aws &>/dev/null; then
    local current_key
    current_key="$(aws configure get aws_access_key_id 2>/dev/null || true)"
    if [[ -n "$current_key" ]]; then
      echo "Found local AWS credentials (access key: ${current_key:0:8}...)"
      if confirm "Use these credentials for the VM?"; then
        AWS_ACCESS_KEY="$(aws configure get aws_access_key_id)"
        AWS_SECRET_KEY="$(aws configure get aws_secret_access_key)"
        AWS_REGION="$(aws configure get region 2>/dev/null || echo "us-east-1")"
      fi
    fi
  fi

  if [[ -z "$AWS_ACCESS_KEY" ]]; then
    echo "Enter AWS credentials for the VM:"
    AWS_ACCESS_KEY="$(prompt "  Access Key ID")"
    read -r -s -p "  Secret Access Key: " AWS_SECRET_KEY
    echo ""
    AWS_REGION="$(prompt "  Region" "us-east-1")"
  fi

  [[ -z "$AWS_ACCESS_KEY" ]] && fail "AWS Access Key ID is required"
  [[ -z "$AWS_SECRET_KEY" ]] && fail "AWS Secret Access Key is required"

  # VM name
  local slug
  slug="$(head -c 4 /dev/urandom | xxd -p)"
  echo ""
  VM_NAME="$(prompt "VM name" "oscm-fund-$slug")"

  # Mode
  echo ""
  echo "Select mode:"
  echo "  1) dev   -- development environment"
  echo "  2) prod  -- production with git-sync"
  local mode_choice
  mode_choice="$(prompt "Choice" "1")"
  case "$mode_choice" in
    1|dev)  MODE="dev" ;;
    2|prod) MODE="prod" ;;
    *) fail "Invalid mode selection: $mode_choice" ;;
  esac

  # Developer name (dev mode only)
  if [[ "$MODE" == "dev" ]]; then
    echo ""
    echo "Known developer profiles: chris, john"
    DEV_NAME="$(prompt "Developer name" "chris")"
  fi

  # Tags
  VM_TAGS=("fund")
  if [[ "$MODE" == "prod" ]]; then
    VM_TAGS+=("prod")
  else
    VM_TAGS+=("dev")
  fi
  echo ""
  echo "Default tags: ${VM_TAGS[*]}"
  local extra_tags
  extra_tags="$(prompt "Additional tags (comma-separated, or blank to skip)" "")"
  if [[ -n "$extra_tags" ]]; then
    IFS=',' read -ra parsed_tags <<< "$extra_tags"
    for tag in "${parsed_tags[@]}"; do
      tag="$(echo "$tag" | xargs)"
      if [[ -n "$tag" ]]; then
        VM_TAGS+=("$tag")
      fi
    done
  fi

  # Branch
  echo ""
  VM_BRANCH="$(prompt "Branch" "master")"

  # Summary
  echo ""
  echo "--- Configuration Summary ---"
  echo "  VM name:    $VM_NAME"
  echo "  Mode:       $MODE"
  echo "  AWS key:    ${AWS_ACCESS_KEY:0:8}..."
  echo "  AWS secret: ${AWS_SECRET_KEY:0:4}********************"
  echo "  AWS region: $AWS_REGION"
  if [[ "$MODE" == "dev" ]]; then
    echo "  Developer:  $DEV_NAME"
    echo "  Profile:    dev/$DEV_NAME"
  else
    echo "  Profile:    production"
  fi
  echo "  Branch:     $VM_BRANCH"
  echo "  Tags:       ${VM_TAGS[*]}"
  echo "-----------------------------"
  echo ""

  if ! confirm "Proceed with this configuration?"; then
    echo "Aborted."
    exit 0
  fi
}

# --------------------------------------------------------------------------- #
# Phase 2: Create exe.dev VM
# --------------------------------------------------------------------------- #

phase_create_vm() {
  step "Creating exe.dev VM"

  # Check if VM already exists
  local vm_exists=false
  if command -v ssh &>/dev/null; then
    local vm_list
    vm_list="$(ssh exe.dev ls --json 2>/dev/null || echo '{"vms":[]}')"
    if echo "$vm_list" | jq -e ".vms[] | select(.vm_name == \"$VM_NAME\")" &>/dev/null; then
      local vm_status
      vm_status="$(echo "$vm_list" | jq -r ".vms[] | select(.vm_name == \"$VM_NAME\") | .status")"
      echo "VM '$VM_NAME' already exists (status: $vm_status)"
      vm_exists=true
      if [[ "$vm_status" != "running" ]]; then
        echo "Waiting for VM to reach running state..."
      fi
    fi
  fi

  if [[ "$vm_exists" == false ]]; then
    echo "Creating VM '$VM_NAME'..."
    ssh exe.dev new --name="$VM_NAME"
    echo "VM creation initiated"
  fi

  # Poll until VM is running
  local elapsed=0
  local timeout=300
  while [[ $elapsed -lt $timeout ]]; do
    local status
    status="$(ssh exe.dev ls --json 2>/dev/null | jq -r ".vms[] | select(.vm_name == \"$VM_NAME\") | .status" || echo "unknown")"
    if [[ "$status" == "running" ]]; then
      echo "VM '$VM_NAME' is running"
      break
    fi
    echo "  Status: $status (waiting...)"
    sleep 3
    elapsed=$((elapsed + 3))
  done

  if [[ $elapsed -ge $timeout ]]; then
    fail "Timed out waiting for VM to reach running state after ${timeout}s"
  fi

  # Wait for SSH connectivity
  step "Waiting for SSH connectivity"
  local ssh_elapsed=0
  local ssh_timeout=60
  while [[ $ssh_elapsed -lt $ssh_timeout ]]; do
    if ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=accept-new "$VM_NAME.exe.xyz" true 2>/dev/null; then
      echo "SSH connection established"
      break
    fi
    sleep 3
    ssh_elapsed=$((ssh_elapsed + 3))
  done

  if [[ $ssh_elapsed -ge $ssh_timeout ]]; then
    fail "Timed out waiting for SSH connectivity after ${ssh_timeout}s"
  fi

  # Apply tags
  if [[ ${#VM_TAGS[@]} -gt 0 ]]; then
    step "Tagging VM"
    ssh exe.dev tag "$VM_NAME" "${VM_TAGS[@]}" || true
    echo "Applied tags: ${VM_TAGS[*]}"
  fi
}

# --------------------------------------------------------------------------- #
# Phase 3: Bootstrap the VM
# --------------------------------------------------------------------------- #

phase_bootstrap() {
  # --- 3a: AWS credentials ---
  step "Configuring AWS credentials on VM"

  remote "mkdir -p ~/.aws"

  printf '[default]\naws_access_key_id = %s\naws_secret_access_key = %s\n' \
    "$AWS_ACCESS_KEY" "$AWS_SECRET_KEY" \
    | remote "cat > ~/.aws/credentials"

  printf '[default]\nregion = %s\noutput = json\n' \
    "$AWS_REGION" \
    | remote "cat > ~/.aws/config"

  remote "chmod 600 ~/.aws/credentials ~/.aws/config"

  # Verify -- aws CLI may not be installed yet on a fresh VM, so only
  # check if the files were written. bootstrap-machine will install the
  # CLI and validate credentials itself.
  if remote "test -f ~/.aws/credentials && test -f ~/.aws/config" 2>/dev/null; then
    echo "Credentials written to VM"
  else
    fail "Failed to write AWS credentials on VM"
  fi

  # --- 3b: GitHub auth ---
  step "Authenticating GitHub on VM"

  # Install gh if missing
  if ! remote "command -v gh" &>/dev/null; then
    echo "Installing GitHub CLI on VM..."
    remote "sudo apt-get update -qq && sudo apt-get install -y -qq gh" >/dev/null
  fi

  if remote "gh auth status" &>/dev/null; then
    echo "GitHub CLI already authenticated"
  else
    echo "Starting GitHub device flow authentication..."
    echo "A code will appear below. Open https://github.com/login/device in your browser and enter it."
    echo ""
    remote_long_tty "gh auth login --hostname github.com --git-protocol https --web"

    # Verify
    if ! remote "gh auth status" &>/dev/null; then
      fail "GitHub authentication failed"
    fi
    echo "GitHub authentication successful"
  fi

  # --- 3c: Clone repo ---
  step "Cloning repository on VM"

  if remote "test -d ~/fund/.git" 2>/dev/null; then
    echo "Repository already cloned, fetching latest..."
    remote "cd ~/fund && git fetch --all"
  else
    echo "Cloning oscmcompany/fund..."
    remote "gh repo clone oscmcompany/fund ~/fund"
  fi

  # Verify
  if ! remote "test -d ~/fund/.git" 2>/dev/null; then
    fail "Repository clone verification failed"
  fi

  # Checkout branch
  if [[ "$VM_BRANCH" != "master" ]]; then
    echo "Checking out branch $VM_BRANCH..."
    remote "cd ~/fund && git checkout $VM_BRANCH"
  fi
  echo "Repository ready (branch: $VM_BRANCH)"

  # --- 3d: Run bootstrap-machine ---
  step "Running bootstrap-machine on VM (this will take a while)"

  local bootstrap_args="--noninteractive"
  if [[ "$MODE" == "prod" ]]; then
    bootstrap_args="--profile production --prod $bootstrap_args"
  else
    bootstrap_args="--profile dev/$DEV_NAME $bootstrap_args"
  fi

  # shellcheck disable=SC2086
  remote_long_tty "cd ~/fund && bash tools/bootstrap-machine $bootstrap_args" \
    || fail "bootstrap-machine failed"

  echo "Bootstrap complete"
}

# --------------------------------------------------------------------------- #
# Phase 4: Drop in
# --------------------------------------------------------------------------- #

phase_dropin() {
  step "Rebooting VM"
  remote "sudo reboot" || true
  sleep 5

  local reboot_elapsed=0
  local reboot_timeout=60
  while [[ $reboot_elapsed -lt $reboot_timeout ]]; do
    if ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=accept-new "$VM_NAME.exe.xyz" true 2>/dev/null; then
      echo "VM back up after reboot"
      break
    fi
    sleep 3
    reboot_elapsed=$((reboot_elapsed + 3))
  done

  if [[ $reboot_elapsed -ge $reboot_timeout ]]; then
    fail "Timed out waiting for VM to come back after reboot (${reboot_timeout}s)"
  fi

  # --- Prod-only: start services in tmux session ---
  if [[ "$MODE" == "prod" ]]; then
    step "Starting services in tmux session"
    remote "$NIX_SOURCE && cd ~/fund && tmux new-session -d -s fund 'devenv --profile apps up'"
    echo "Services started in tmux session 'fund'"
  fi

  # --- Share VM with team ---
  step "Sharing VM with team"
  ssh exe.dev share add "$VM_NAME" team 2>/dev/null \
    && echo "Shared $VM_NAME with team" \
    || echo "Warning: could not share VM with team (share manually with: ssh exe.dev share add $VM_NAME team)"
  ssh exe.dev share access allow "$VM_NAME" 2>/dev/null \
    && echo "Enabled team SSH access" \
    || echo "Warning: could not enable team access (run: ssh exe.dev share access allow $VM_NAME)"

  step "Setup complete"

  echo ""
  echo "--- VM Ready ---"
  echo "  VM:       $VM_NAME.exe.xyz"
  if [[ "$MODE" == "prod" ]]; then
    echo "  Profile:  production"
    echo "  Services: running in tmux session 'fund'"
  else
    echo "  Profile:  dev/$DEV_NAME"
  fi
  echo "  Tags:     ${VM_TAGS[*]}"
  echo ""
  echo "Next steps:"
  echo "  ssh $VM_NAME.exe.xyz"
  if [[ "$MODE" == "prod" ]]; then
    echo "  tmux attach -t fund        # attach to services session"
  else
    echo "  cd ~/fund"
    echo "  devenv --profile apps up    # start application services"
  fi
  echo "-----------------"
  echo ""

  if confirm "Drop into VM now?"; then
    if [[ "$MODE" == "prod" ]]; then
      exec ssh -t "$VM_NAME.exe.xyz" "tmux attach -t fund"
    else
      exec ssh -t "$VM_NAME.exe.xyz" "cd ~/fund && exec \$SHELL -l"
    fi
  fi
}

# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #

echo "fund VM provisioning"
echo "===================="

phase_collect
phase_create_vm
phase_bootstrap
phase_dropin
