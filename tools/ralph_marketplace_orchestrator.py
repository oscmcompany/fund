"""Ralph marketplace orchestrator.

Main entry point for marketplace commands: setup, status, reset.
"""

import sys
from pathlib import Path

from ralph_marketplace_budget import allocate_budgets, format_budget_allocation
from ralph_marketplace_state import MarketplaceStateManager


def setup_marketplace() -> None:
    """Initialize marketplace state and configuration."""
    print("Initializing Ralph marketplace")

    ralph_dir = Path(".ralph")
    state_manager = MarketplaceStateManager(ralph_dir)

    # Create configuration
    config = state_manager.load_config()
    state_manager.save_config(config)

    print("\nConfiguration:")
    print(f"  Number of bots: {config['num_bots']}")
    print(f"  Base budget per bot: {config['base_budget_per_bot']}")
    print(f"  Total budget pool: {config['num_bots'] * config['base_budget_per_bot']}")
    print("\nScoring weights:")
    for dimension, weight in config["scoring_weights"].items():
        print(f"  {dimension}: {weight:.2f}")
    print("\nWeight constraints:")
    print(f"  Min: {config['weight_constraints']['min']:.2f}")
    print(f"  Max: {config['weight_constraints']['max']:.2f}")

    # Initialize state
    state = state_manager.load_state()
    print(f"\nInitialized {len(state.bots)} bidders with equal weights:")
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
    print("Bidder Statistics")
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
    print("  - Reset bidder weights to equal")
    print("  - Clear cached state")
    print("  - Keep configuration unchanged")
    print()

    state_manager.reset_state()

    print("Marketplace reset complete")
    print("All bidders now have equal weights")
    print("Run 'mask ralph marketplace status' to verify")


def main() -> None:
    """Main entry point for marketplace orchestrator."""
    if len(sys.argv) < 2:
        print("Usage: python ralph_marketplace_orchestrator.py <command> [args]")
        print("Commands:")
        print("  setup                    - Initialize marketplace")
        print("  status                   - Show marketplace state")
        print("  reset                    - Reset marketplace to initial state")
        sys.exit(1)

    command = sys.argv[1]

    if command == "setup":
        setup_marketplace()
    elif command == "status":
        display_marketplace_status()
    elif command == "reset":
        reset_marketplace()
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
