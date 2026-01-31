"""Ralph marketplace state management.

Handles loading, saving, and computing marketplace state from append-only event log.
"""

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


@dataclass
class BotState:
    """State for a single smart bot."""

    bot_id: str
    weight: float
    efficiency: float
    proposals_submitted: int
    implementations_succeeded: int
    implementations_failed: int
    total_iterations_used: int
    average_accuracy: float


@dataclass
class MarketplaceState:
    """Complete marketplace state."""

    bots: dict[str, BotState]
    total_budget_pool: int
    rounds_completed: int
    last_updated: str


class MarketplaceStateManager:
    """Manages marketplace state persistence and computation."""

    def __init__(self, ralph_dir: Path = Path(".ralph")) -> None:
        """Initialize state manager.

        Args:
            ralph_dir: Directory containing marketplace state and events
        """
        self.ralph_dir = ralph_dir
        self.events_dir = ralph_dir / "events"
        self.state_file = ralph_dir / "marketplace.json"
        self.version_file = ralph_dir / ".state_version"
        self.config_file = ralph_dir / "config.json"

        # Ensure directories exist
        self.events_dir.mkdir(parents=True, exist_ok=True)

    def load_config(self) -> dict[str, Any]:
        """Load marketplace configuration.

        Returns:
            Configuration dictionary
        """
        if not self.config_file.exists():
            # Default configuration
            return {
                "num_bots": 3,
                "base_budget_per_bot": 10,
                "scoring_weights": {
                    "spec_alignment": 0.30,
                    "technical_quality": 0.20,
                    "innovation": 0.15,
                    "risk": 0.20,
                    "efficiency": 0.10,
                    "specialist_validation": 0.05,
                },
                "weight_constraints": {"min": 0.05, "max": 0.60},
            }

        with open(self.config_file, encoding="utf-8") as f:
            return json.load(f)

    def save_config(self, config: dict[str, Any]) -> None:
        """Save marketplace configuration.

        Args:
            config: Configuration dictionary
        """
        with open(self.config_file, "w", encoding="utf-8") as f:
            json.dump(config, f, indent=2)

    def load_events(self) -> list[dict[str, Any]]:
        """Load all events from event log, sorted by timestamp.

        Returns:
            List of event dictionaries
        """
        events = []
        if not self.events_dir.exists():
            return events

        for event_file in self.events_dir.glob("*.json"):
            try:
                with open(event_file, encoding="utf-8") as f:
                    event = json.load(f)
                    events.append(event)
            except json.JSONDecodeError as e:
                # Log warning and skip corrupted file
                print(f"Warning: Skipping corrupted event file {event_file.name}: {e}")
                continue

        # Sort by timestamp
        events.sort(key=lambda e: e["timestamp"])
        return events

    def compute_state_from_events(
        self, events: list[dict[str, Any]], config: dict[str, Any]
    ) -> MarketplaceState:
        """Compute current state from event log.

        Args:
            events: List of events
            config: Configuration dictionary

        Returns:
            Computed marketplace state
        """
        num_bots = config["num_bots"]

        # Initialize bot states with equal weights
        bots = {}
        for i in range(1, num_bots + 1):
            bot_id = f"smart_bot_{i}"
            bots[bot_id] = BotState(
                bot_id=bot_id,
                weight=1.0 / num_bots,  # Equal initial weights
                efficiency=1.0,  # Start at perfect efficiency
                proposals_submitted=0,
                implementations_succeeded=0,
                implementations_failed=0,
                total_iterations_used=0,
                average_accuracy=0.0,
            )

        # Apply events to update state
        for event in events:
            bot_id = event["bot_id"]
            outcome = event["outcome"]
            weight_delta = event.get("weight_delta", 0.0)

            if bot_id not in bots:
                print(
                    f"Warning: Event for unknown bot '{bot_id}' "
                    f"(outcome: {outcome}, weight_delta: {weight_delta})"
                )
                continue

            bot = bots[bot_id]

            # Update based on outcome
            # Check for success outcomes (ends with _success)
            if isinstance(outcome, str) and (
                outcome.endswith("_success") or outcome == "success"
            ):
                bot.implementations_succeeded += 1
            # Check for failure outcomes (contains "failure" or "failed")
            elif isinstance(outcome, str) and (
                "failure" in outcome or "failed" in outcome
            ):
                bot.implementations_failed += 1

            # Update weight
            bot.weight += weight_delta

            # Update iteration count
            bot.total_iterations_used += event.get("iteration_count", 0)

            # Update accuracy tracking
            if "accuracy" in event:
                # Running average (only count events with accuracy)
                # Count how many events with accuracy we've seen so far
                accuracy_count = 0
                for prev_event in events[: events.index(event) + 1]:
                    if prev_event["bot_id"] == bot_id and "accuracy" in prev_event:
                        accuracy_count += 1

                if accuracy_count > 0:
                    # Update running average considering only events with accuracy
                    bot.average_accuracy = (
                        bot.average_accuracy * (accuracy_count - 1) + event["accuracy"]
                    ) / accuracy_count

        # Normalize weights to sum to 1.0
        total_weight = sum(bot.weight for bot in bots.values())
        if total_weight > 0:
            for bot in bots.values():
                bot.weight = bot.weight / total_weight

        # Apply weight constraints
        min_weight = config["weight_constraints"]["min"]
        max_weight = config["weight_constraints"]["max"]
        for bot in bots.values():
            bot.weight = max(min_weight, min(max_weight, bot.weight))

        # Renormalize after constraints
        total_weight = sum(bot.weight for bot in bots.values())
        if total_weight > 0:
            for bot in bots.values():
                bot.weight = bot.weight / total_weight

        # Compute efficiency
        for bot in bots.values():
            total_attempts = bot.implementations_succeeded + bot.implementations_failed
            if total_attempts > 0:
                bot.efficiency = bot.implementations_succeeded / total_attempts
            else:
                bot.efficiency = 1.0  # No attempts yet

        return MarketplaceState(
            bots=bots,
            total_budget_pool=config["num_bots"] * config["base_budget_per_bot"],
            rounds_completed=len(events),
            last_updated=datetime.now(timezone.utc).isoformat(),
        )

    def load_state(self) -> MarketplaceState:
        """Load marketplace state, recomputing from events if needed.

        Returns:
            Current marketplace state
        """
        config = self.load_config()
        events = self.load_events()

        # Check if cached state is current
        cached_version = 0
        if self.version_file.exists():
            cached_version = int(self.version_file.read_text().strip())

        if self.state_file.exists() and cached_version == len(events):
            # Cache is current, load it
            with open(self.state_file, encoding="utf-8") as f:
                state_dict = json.load(f)
                bots = {
                    bot_id: BotState(**bot_data)
                    for bot_id, bot_data in state_dict["bots"].items()
                }
                return MarketplaceState(
                    bots=bots,
                    total_budget_pool=state_dict["total_budget_pool"],
                    rounds_completed=state_dict["rounds_completed"],
                    last_updated=state_dict["last_updated"],
                )

        # Recompute from events
        state = self.compute_state_from_events(events, config)

        # Cache the computed state
        self.save_state(state)

        return state

    def save_state(self, state: MarketplaceState) -> None:
        """Save marketplace state to cache.

        Args:
            state: Marketplace state to save
        """
        state_dict = {
            "bots": {
                bot_id: {
                    "bot_id": bot.bot_id,
                    "weight": bot.weight,
                    "efficiency": bot.efficiency,
                    "proposals_submitted": bot.proposals_submitted,
                    "implementations_succeeded": bot.implementations_succeeded,
                    "implementations_failed": bot.implementations_failed,
                    "total_iterations_used": bot.total_iterations_used,
                    "average_accuracy": bot.average_accuracy,
                }
                for bot_id, bot in state.bots.items()
            },
            "total_budget_pool": state.total_budget_pool,
            "rounds_completed": state.rounds_completed,
            "last_updated": state.last_updated,
        }

        with open(self.state_file, "w", encoding="utf-8") as f:
            json.dump(state_dict, f, indent=2)

        # Update version file
        events = self.load_events()
        self.version_file.write_text(str(len(events)))

    def record_event(self, event: dict[str, Any]) -> None:
        """Record a new event to the event log.

        Args:
            event: Event dictionary containing outcome details
        """
        if "timestamp" not in event:
            event["timestamp"] = datetime.now(timezone.utc).isoformat()

        # Create event filename
        timestamp_str = event["timestamp"].replace(":", "-").replace(".", "-")
        bot_id = event["bot_id"]
        outcome = event["outcome"]
        filename = f"{timestamp_str}-{bot_id}-{outcome}.json"

        # Write event file
        event_file = self.events_dir / filename
        with open(event_file, "w", encoding="utf-8") as f:
            json.dump(event, f, indent=2)

    def reset_state(self) -> None:
        """Reset marketplace state to initial conditions.

        Removes all events and cached state, keeping configuration.
        """
        # Remove all event files
        if self.events_dir.exists():
            for event_file in self.events_dir.glob("*.json"):
                event_file.unlink()

        # Remove cached state
        if self.state_file.exists():
            self.state_file.unlink()

        # Remove version file
        if self.version_file.exists():
            self.version_file.unlink()

    def get_bot_history(self, bot_id: str, last_n: int = 10) -> list[dict[str, Any]]:
        """Get recent history for a specific bot.

        Args:
            bot_id: Bot identifier
            last_n: Number of recent events to return

        Returns:
            List of recent events for the bot
        """
        events = self.load_events()
        bot_events = [e for e in events if e["bot_id"] == bot_id]
        return bot_events[-last_n:]
