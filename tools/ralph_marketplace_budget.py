"""Ralph marketplace budget allocation.

Handles iteration budget allocation across smart bots based on weights and efficiency.
"""

from tools.ralph_marketplace_state import BotState, MarketplaceState


def allocate_budgets(state: MarketplaceState) -> dict[str, int]:
    """Allocate iteration budgets to bots using fixed pool with efficiency rewards.

    The total budget pool is fixed at (num_bots * base_budget_per_bot).
    Allocation is based on combined score: weight × efficiency.
    This creates zero-sum competition where high performers take from low performers.

    Args:
        state: Current marketplace state

    Returns:
        Dictionary mapping bot_id to allocated iteration budget
    """
    total_budget = state.total_budget_pool

    # Calculate combined scores (weight × efficiency)
    combined_scores = {}
    for bot_id, bot in state.bots.items():
        combined_scores[bot_id] = bot.weight * bot.efficiency

    # Normalize to sum to 1.0
    total_combined = sum(combined_scores.values())
    if total_combined == 0:
        # All bots have zero score, distribute equally
        equal_budget = total_budget / len(state.bots)
        return {bot_id: int(equal_budget) for bot_id in state.bots}

    normalized_scores = {
        bot_id: score / total_combined for bot_id, score in combined_scores.items()
    }

    # Allocate proportionally
    allocations = {
        bot_id: total_budget * normalized_scores[bot_id] for bot_id in state.bots
    }

    # Round to integers while maintaining total sum
    # Use banker's rounding for fairness
    integer_allocations = {}
    total_allocated = 0

    # Sort by fractional part descending to prioritize rounding up
    items = sorted(
        allocations.items(), key=lambda x: x[1] - int(x[1]), reverse=True
    )

    for bot_id, allocation in items:
        integer_allocation = int(allocation)
        integer_allocations[bot_id] = integer_allocation
        total_allocated += integer_allocation

    # Distribute remaining budget due to rounding
    remaining = total_budget - total_allocated
    if remaining > 0:
        # Give remaining iterations to highest-scoring bots
        sorted_bots = sorted(
            combined_scores.items(), key=lambda x: x[1], reverse=True
        )
        for i in range(remaining):
            bot_id = sorted_bots[i % len(sorted_bots)][0]
            integer_allocations[bot_id] += 1

    # Ensure minimum allocation of 1 iteration per bot (can participate)
    for bot_id in integer_allocations:
        if integer_allocations[bot_id] < 1:
            integer_allocations[bot_id] = 1

    return integer_allocations


def format_budget_allocation(
    state: MarketplaceState, allocations: dict[str, int]
) -> str:
    """Format budget allocation for display.

    Args:
        state: Current marketplace state
        allocations: Budget allocations from allocate_budgets()

    Returns:
        Formatted string showing allocation details
    """
    lines = [
        "Budget Allocation",
        "=" * 60,
        f"Total Pool: {state.total_budget_pool} iterations",
        "",
        f"{'Bot ID':<15} {'Weight':>8} {'Efficiency':>10} {'Budget':>10}",
        "-" * 60,
    ]

    # Sort by allocation descending
    sorted_bots = sorted(
        allocations.items(), key=lambda x: x[1], reverse=True
    )

    for bot_id, budget in sorted_bots:
        bot = state.bots[bot_id]
        lines.append(
            f"{bot_id:<15} {bot.weight:>8.3f} {bot.efficiency:>10.2%} {budget:>10}"
        )

    # Verify total
    total_allocated = sum(allocations.values())
    lines.append("-" * 60)
    lines.append(f"{'Total Allocated':<15} {' '*18} {total_allocated:>10}")

    if total_allocated != state.total_budget_pool:
        lines.append(f"WARNING: Total allocated ({total_allocated}) != pool ({state.total_budget_pool})")

    return "\n".join(lines)
