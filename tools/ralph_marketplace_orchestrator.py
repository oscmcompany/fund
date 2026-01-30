"""Ralph marketplace orchestrator.

Main entry point for marketplace commands: setup, loop, status, reset.
"""

import json
import sys
from pathlib import Path

from tools.ralph_marketplace_budget import allocate_budgets, format_budget_allocation
from tools.ralph_marketplace_state import MarketplaceStateManager
from tools.ralph_marketplace_weights import format_weight_update_summary


def setup_marketplace() -> None:
    """Initialize marketplace state and configuration."""
    print("Initializing Ralph marketplace")

    ralph_dir = Path(".ralph")
    state_manager = MarketplaceStateManager(ralph_dir)

    # Create configuration
    config = state_manager.load_config()
    state_manager.save_config(config)

    print(f"\nConfiguration:")
    print(f"  Number of bots: {config['num_bots']}")
    print(f"  Base budget per bot: {config['base_budget_per_bot']}")
    print(f"  Total budget pool: {config['num_bots'] * config['base_budget_per_bot']}")
    print(f"\nScoring weights:")
    for dimension, weight in config["scoring_weights"].items():
        print(f"  {dimension}: {weight:.2f}")
    print(f"\nWeight constraints:")
    print(f"  Min: {config['weight_constraints']['min']:.2f}")
    print(f"  Max: {config['weight_constraints']['max']:.2f}")

    # Initialize state
    state = state_manager.load_state()
    print(f"\nInitialized {len(state.bots)} smart bots with equal weights:")
    for bot_id, bot in state.bots.items():
        print(f"  {bot_id}: weight={bot.weight:.3f}, efficiency={bot.efficiency:.2%}")

    print(f"\nMarketplace initialized at {ralph_dir}/")
    print("Use 'mask ralph marketplace status' to view current state")


def display_marketplace_status() -> None:
    """Display current marketplace state."""
    ralph_dir = Path(".ralph")
    state_manager = MarketplaceStateManager(ralph_dir)

    try:
        state = state_manager.load_state()
        config = state_manager.load_config()
    except FileNotFoundError:
        print("Error: Marketplace not initialized")
        print("Run: mask ralph marketplace setup")
        sys.exit(1)

    print("Ralph Marketplace Status")
    print("=" * 80)
    print(f"Last Updated: {state.last_updated}")
    print(f"Rounds Completed: {state.rounds_completed}")
    print(f"Total Budget Pool: {state.total_budget_pool} iterations")
    print()

    # Bot statistics
    print("Smart Bot Statistics")
    print("-" * 80)
    print(
        f"{'Bot ID':<15} {'Weight':>8} {'Efficiency':>10} {'Succeeded':>10} {'Failed':>8} {'Accuracy':>10}"
    )
    print("-" * 80)

    # Sort by weight descending
    sorted_bots = sorted(state.bots.items(), key=lambda x: x[1].weight, reverse=True)

    for bot_id, bot in sorted_bots:
        print(
            f"{bot_id:<15} {bot.weight:>8.3f} {bot.efficiency:>10.2%} "
            f"{bot.implementations_succeeded:>10} {bot.implementations_failed:>8} "
            f"{bot.average_accuracy:>10.2%}"
        )

    print()

    # Budget allocation
    allocations = allocate_budgets(state)
    print(format_budget_allocation(state, allocations))
    print()

    # Recent events
    events = state_manager.load_events()
    if events:
        print("Recent Events (last 5)")
        print("-" * 80)
        for event in events[-5:]:
            timestamp = event["timestamp"][:19]  # Trim to seconds
            bot_id = event["bot_id"]
            outcome = event["outcome"]
            weight_delta = event.get("weight_delta", 0.0)
            print(f"{timestamp}  {bot_id:<15} {outcome:<20} (Δw: {weight_delta:+.3f})")
    else:
        print("No events recorded yet")

    print()


def reset_marketplace() -> None:
    """Reset marketplace to initial state."""
    ralph_dir = Path(".ralph")
    state_manager = MarketplaceStateManager(ralph_dir)

    print("Resetting marketplace state")
    print("This will:")
    print("  - Remove all event history")
    print("  - Reset bot weights to equal")
    print("  - Clear cached state")
    print("  - Keep configuration unchanged")
    print()

    state_manager.reset_state()

    print("Marketplace reset complete")
    print("All bots now have equal weights")
    print("Run 'mask ralph marketplace status' to verify")


def run_marketplace_loop(issue_number: str, branch_name: str) -> None:
    """Run marketplace competition loop.

    This is a placeholder for the main loop logic. The actual implementation
    will be handled by the arbiter agent via Claude CLI.

    Args:
        issue_number: GitHub issue number
        branch_name: Git branch name
    """
    ralph_dir = Path(".ralph")
    state_manager = MarketplaceStateManager(ralph_dir)

    try:
        state = state_manager.load_state()
        config = state_manager.load_config()
    except FileNotFoundError:
        print("Error: Marketplace not initialized")
        print("Run: mask ralph marketplace setup")
        sys.exit(1)

    print(f"Starting marketplace loop for issue #{issue_number}")
    print(f"Branch: {branch_name}")
    print()

    # Allocate budgets
    allocations = allocate_budgets(state)
    print(format_budget_allocation(state, allocations))
    print()

    print("Loading arbiter agent...")
    arbiter_prompt_path = Path(".claude/agents/ralph_arbiter.md")
    if not arbiter_prompt_path.exists():
        print(f"Error: Arbiter agent not found at {arbiter_prompt_path}")
        sys.exit(1)

    # Load arbiter prompt template
    with open(arbiter_prompt_path) as f:
        arbiter_prompt = f.read()

    # Inject context variables
    arbiter_prompt = arbiter_prompt.replace("{{issue_number}}", issue_number)
    arbiter_prompt = arbiter_prompt.replace("{{num_bots}}", str(config["num_bots"]))
    arbiter_prompt = arbiter_prompt.replace(
        "{{bot_budgets}}", json.dumps(allocations, indent=2)
    )
    arbiter_prompt = arbiter_prompt.replace(
        "{{total_budget}}", str(state.total_budget_pool)
    )

    print("Arbiter agent loaded and configured")
    print()
    print("=" * 80)
    print("MARKETPLACE LOOP")
    print("=" * 80)
    print()
    print(f"The arbiter will now orchestrate the competition:")
    print(f"  1. Extract requirements from issue #{issue_number}")
    print(f"  2. Spawn {config['num_bots']} smart bots to submit proposals")
    print(f"  3. Evaluate and rank proposals")
    print(f"  4. Implement top-ranked proposal")
    print(f"  5. Update marketplace state based on outcome")
    print()
    print("Note: The actual loop execution requires integration with Claude CLI")
    print("This placeholder demonstrates the orchestration structure")
    print()

    # TODO: Actual implementation would spawn arbiter via Claude CLI:
    # result = subprocess.run([
    #     "claude",
    #     "--system-prompt", arbiter_prompt,
    #     "--dangerously-skip-permissions",
    #     "Begin marketplace loop for issue #{issue_number}"
    # ], capture_output=True)
    #
    # Parse result, update state, record event
    # For now, this is a placeholder showing the structure


def main() -> None:
    """Main entry point for marketplace orchestrator."""
    if len(sys.argv) < 2:
        print("Usage: python ralph_marketplace_orchestrator.py <command> [args]")
        print("Commands:")
        print("  setup                    - Initialize marketplace")
        print("  status                   - Show marketplace state")
        print("  reset                    - Reset marketplace to initial state")
        print("  loop <issue> <branch>    - Run marketplace loop")
        sys.exit(1)

    command = sys.argv[1]

    if command == "setup":
        setup_marketplace()
    elif command == "status":
        display_marketplace_status()
    elif command == "reset":
        reset_marketplace()
    elif command == "loop":
        if len(sys.argv) < 4:
            print("Usage: python ralph_marketplace_orchestrator.py loop <issue_number> <branch_name>")
            sys.exit(1)
        issue_number = sys.argv[2]
        branch_name = sys.argv[3]
        run_marketplace_loop(issue_number, branch_name)
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
