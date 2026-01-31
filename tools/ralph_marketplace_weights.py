"""Ralph marketplace weight update calculations.

Handles weight adjustments based on proposal and implementation outcomes.
"""

from typing import Literal


OutcomeType = Literal[
    "ranked_first_success",
    "ranked_first_failure",
    "ranked_second_plus_success",
    "ranked_not_tried",
    "replan_new_success",
    "replan_failed_again",
    "replan_resubmitted_same",
]


WEIGHT_DELTAS = {
    "ranked_first_success": 0.10,  # Proposal ranked #1, implemented successfully
    "ranked_first_failure": -0.15,  # Proposal ranked #1, implementation failed
    "ranked_second_plus_success": 0.08,  # Ranked #2+, succeeded after higher rank failed
    "ranked_not_tried": -0.02,  # Ranked but not tried (another succeeded)
    "replan_new_success": 0.12,  # Replan with new proposal succeeded
    "replan_failed_again": -0.20,  # Replan failed again
    "replan_resubmitted_same": -0.05,  # Replan but resubmitted same proposal
}


ACCURACY_BONUS_THRESHOLD = 0.15  # Accuracy within this range gets bonus
ACCURACY_BONUS = 0.05  # Bonus for accurate prediction


def calculate_weight_delta(
    outcome: OutcomeType, accuracy: float | None = None
) -> float:
    """Calculate weight delta for a given outcome.

    Args:
        outcome: Type of outcome
        accuracy: Proposal accuracy (difference between proposal and implementation scores)
                 Only applicable for success outcomes

    Returns:
        Weight delta to apply
    """
    base_delta = WEIGHT_DELTAS[outcome]

    # Add accuracy bonus for successful outcomes
    if (
        accuracy is not None
        and outcome
        in ["ranked_first_success", "ranked_second_plus_success", "replan_new_success"]
        and accuracy <= ACCURACY_BONUS_THRESHOLD
    ):
        return base_delta + ACCURACY_BONUS

    return base_delta


def determine_outcome_type(
    bot_id: str,
    rankings: list[tuple[str, float]],
    implementation_result: str,
    *,
    is_replan: bool = False,
    resubmitted_same: bool = False,
) -> OutcomeType:
    """Determine outcome type for weight calculation.

    Args:
        bot_id: Bot identifier
        rankings: List of (bot_id, score) tuples, sorted by rank
        implementation_result: "success" or "failure"
        is_replan: Whether this is a replan round
        resubmitted_same: Whether bot resubmitted same proposal (replan only)

    Returns:
        Outcome type for weight calculation
    """
    # Find bot's rank
    bot_rank = None
    for i, (ranked_bot_id, _) in enumerate(rankings, start=1):
        if ranked_bot_id == bot_id:
            bot_rank = i
            break

    if bot_rank is None:
        message = f"Bot {bot_id} not found in rankings"
        raise ValueError(message)

    # Replan scenarios
    if is_replan:
        if resubmitted_same:
            return "replan_resubmitted_same"
        if implementation_result == "success":
            return "replan_new_success"
        return "replan_failed_again"

    # Initial round scenarios
    if bot_rank == 1:
        # Top-ranked bot
        if implementation_result == "success":
            return "ranked_first_success"
        return "ranked_first_failure"

    # Lower-ranked bots
    # Note: In current design, only top proposal is implemented
    # So if we're calculating for rank 2+, it means either:
    # 1. Rank 1 failed and we tried this one (success case)
    # 2. Another bot succeeded (not tried case)

    if implementation_result == "success":
        return "ranked_second_plus_success"

    return "ranked_not_tried"


def format_weight_update_summary(
    bot_id: str,
    old_weight: float,
    new_weight: float,
    outcome: OutcomeType,
    delta: float,
) -> str:
    """Format weight update summary for display.

    Args:
        bot_id: Bot identifier
        old_weight: Weight before update
        new_weight: Weight after update
        outcome: Outcome type
        delta: Weight delta applied

    Returns:
        Formatted summary string
    """
    outcome_descriptions = {
        "ranked_first_success": "Ranked #1, implementation succeeded",
        "ranked_first_failure": "Ranked #1, implementation failed",
        "ranked_second_plus_success": "Ranked #2+, succeeded after higher rank failed",
        "ranked_not_tried": "Ranked but not tried (another succeeded)",
        "replan_new_success": "Replan with new proposal succeeded",
        "replan_failed_again": "Replan failed again",
        "replan_resubmitted_same": "Replan resubmitted same proposal",
    }

    lines = [
        f"Weight Update: {bot_id}",
        "-" * 40,
        f"Outcome: {outcome_descriptions[outcome]}",
        f"Old Weight: {old_weight:.3f}",
        f"Delta: {delta:+.3f}",
        f"New Weight: {new_weight:.3f}",
        f"Change: {((new_weight - old_weight) / old_weight * 100):+.1f}%",
    ]

    return "\n".join(lines)
