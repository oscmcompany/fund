#!/usr/bin/env bash
# Ralph preflight checks - shared dependency validation
# Source this script and call ralph_preflight with required tools

ralph_preflight() {
    local require_claude=false
    local require_jq=false

    for arg in "$@"; do
        case "$arg" in
            --claude) require_claude=true ;;
            --jq) require_jq=true ;;
        esac
    done

    if ! command -v gh &> /dev/null; then
        echo "GitHub CLI (gh) is required"
        exit 1
    fi

    if [ "$require_claude" = true ] && ! command -v claude &> /dev/null; then
        echo "Claude CLI is required"
        exit 1
    fi

    if [ "$require_jq" = true ] && ! command -v jq &> /dev/null; then
        echo "jq is required"
        exit 1
    fi

    if ! gh auth status &> /dev/null; then
        echo "GitHub CLI not authenticated"
        echo "Run: gh auth login"
        exit 1
    fi
}
