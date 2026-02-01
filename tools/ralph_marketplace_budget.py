"""Ralph marketplace budget allocation.

Handles iteration budget allocation across bidders based on weights and efficiency.
"""

from ralph_marketplace_state import MarketplaceState


def allocate_budgets(state: MarketplaceState) -> dict[str, int]:
    """Allocate iteration budgets to bots using fixed pool with efficiency rewards.

    The total budget pool is fixed at (num_bots * base_budget_per_bot).
    Allocation is based on combined score: weight * efficiency.
    This creates zero-sum competition where high performers take from low performers.

    Args:
        state: Current marketplace state

    Returns:
        Dictionary mapping bot_id to allocated iteration budget
    """
    total_budget = state.total_budget_pool

    # Calculate combined scores (weight * efficiency)
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
    integer_allocations = {}

    # First pass: floor all allocations (without minimum enforcement yet)
    for bot_id, allocation in allocations.items():
        integer_allocations[bot_id] = int(allocation)

    # Calculate remaining budget to distribute
    total_allocated = sum(integer_allocations.values())
    remaining = total_budget - total_allocated

    # Distribute remaining iterations to highest-scoring bots
    # Sort by fractional part descending (who "deserves" rounding up most)
    fractional_parts = [
        (bot_id, allocations[bot_id] - integer_allocations[bot_id])
        for bot_id in allocations
    ]
    sorted_by_fraction = sorted(fractional_parts, key=lambda x: x[1], reverse=True)

    # Give remaining iterations based on fractional parts
    for i in range(remaining):
        bot_id = sorted_by_fraction[i % len(sorted_by_fraction)][0]
        integer_allocations[bot_id] += 1

    # Enforce minimum of 1 iteration per bot while maintaining zero-sum
    # Identify bots with 0 allocations
    zero_bots = [bot_id for bot_id, alloc in integer_allocations.items() if alloc == 0]

    if zero_bots:
        # Need to reallocate from high-scoring bots to ensure minimum
        # Sort by score to take from highest scorers
        sorted_bots = sorted(combined_scores.items(), key=lambda x: x[1], reverse=True)

        for zero_bot in zero_bots:
            # Find a bot with >1 allocation to take from
            for donor_bot_id, _ in sorted_bots:
                if integer_allocations[donor_bot_id] > 1:
                    integer_allocations[donor_bot_id] -= 1
                    integer_allocations[zero_bot] += 1
                    break
            else:
                # No bot has >1, cannot enforce minimum without breaking zero-sum
                # Give 1 to this bot anyway (will slightly exceed budget)
                integer_allocations[zero_bot] = 1

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
    sorted_bots = sorted(allocations.items(), key=lambda x: x[1], reverse=True)

    for bot_id, budget in sorted_bots:
        bot = state.bots[bot_id]
        lines.append(
            f"{bot_id:<15} {bot.weight:>8.3f} {bot.efficiency:>10.2%} {budget:>10}"
        )

    # Verify total
    total_allocated = sum(allocations.values())
    lines.append("-" * 60)
    lines.append(f"{'Total Allocated':<15} {' ' * 18} {total_allocated:>10}")

    if total_allocated != state.total_budget_pool:
        lines.append(
            f"WARNING: Total allocated ({total_allocated}) "
            f"!= pool ({state.total_budget_pool})"
        )

    return "\n".join(lines)
