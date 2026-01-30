# Marketplace Proposal Followups

Future considerations and functionality deferred from the initial marketplace implementation.

## Phase 1 - Initial Implementation (Current)

The initial implementation focuses on:
- 3 smart bots competing with lightweight proposals
- Arbiter evaluates proposals and implements winner
- Fixed budget pool (10 * num_bots = 30 iterations)
- Budget allocated by weight × efficiency (zero-sum competition)
- Append-only event log for state management
- 6 scoring dimensions with unified proposal/implementation metrics
- Immediate weight updates after each round
- Replan rounds on failure

## Phase 2 - Meta-Arbiter and Adaptive Tuning

### Meta-Arbiter Agent

**Purpose:** Monitor arbiter decisions for bias patterns and tune scoring weights dynamically.

**Functionality:**
- Runs periodically (not every loop) - suggested after every 20-50 rounds
- Analyzes historical accuracy: Did high-ranked proposals succeed more often than low-ranked?
- Identifies poorly calibrated scoring dimensions (e.g., risk scores don't predict failures)
- Detects bias patterns (e.g., always favoring minimal diff over innovation)
- Recommends weight adjustments for scoring dimensions

**Implementation Approach:**
```python
# Meta-arbiter analyzes last N rounds
def analyze_arbiter_performance(last_n_rounds=20):
    # Load events
    events = load_events()[-last_n_rounds:]

    # Compute correlations
    correlations = {
        'spec_alignment': correlation(spec_scores, success_rate),
        'technical_quality': correlation(quality_scores, success_rate),
        'innovation': correlation(innovation_scores, success_rate),
        'risk': correlation(risk_scores, success_rate),
        'efficiency': correlation(efficiency_scores, success_rate),
        'specialist_validation': correlation(specialist_scores, success_rate)
    }

    # Identify weak signals (low correlation with success)
    # Recommend weight adjustments
    # Human reviews and approves changes
```

**Output:** Recommendations stored in `.ralph/meta_analysis.json`, requiring human approval before applying weight changes.

**Why Deferred:** Need 50-100 rounds of data to identify meaningful patterns. Start with fixed weights, tune later.

---

## Phase 3 - Enhanced Learning Signals

### Post-Merge Health Tracking

**Delayed signals** that require tracking over time:

1. **Defect Injection Rate**
   - Track if future bug fixes reference commits from marketplace implementations
   - Implementation: Add "caused-by: <commit-hash>" tags in bug fix commits
   - Alternative: Use `git blame` + issue references to trace defects back

2. **Revert Rate**
   - Track if marketplace implementations get reverted or significantly modified
   - Window: Within 2 weeks of merge suggests poor implementation

3. **Maintenance Burden**
   - Track future changes to files modified by marketplace implementations
   - High churn might indicate brittle or unclear code

**Integration:**
```python
# Weekly background job
def update_post_merge_signals():
    # For each marketplace PR merged in last 4 weeks:
    for pr in recent_marketplace_prs():
        defects = find_linked_bugs(pr)
        reverts = find_reverts(pr)
        churn = calculate_file_churn(pr.files_changed)

        # Update bot weights retroactively
        adjust_weight_for_delayed_signal(pr.winning_bot, defects, reverts, churn)
```

**Weight adjustment:**
- Each defect traced back: -0.02 weight penalty
- Revert within 2 weeks: -0.10 weight penalty
- High churn (>3 edits in 2 weeks): -0.03 weight penalty

**Why Deferred:** Requires integration with issue tracking and sustained monitoring infrastructure.

---

## Phase 4 - Advanced Bot Behaviors

### Smart Bot Hobbies and Token Rewards

**Concept:** Reward high-performing bots with "hobby time" to explore side interests.

**Mechanics:**
- Bots accumulate reward tokens for successful implementations
- Tokens can be "spent" on:
  - Exploratory refactoring (no issue required)
  - Performance optimization experiments
  - Documentation improvements
  - Proof-of-concept implementations of interesting patterns

**Example:**
```json
{
  "smart_bot_1": {
    "reward_tokens": 5000,
    "hobbies": ["performance_optimization", "code_simplification"],
    "hobby_projects": [
      {
        "description": "Explore async/await patterns in datamanager",
        "tokens_spent": 1500,
        "outcome": "PR #456 (performance improvement)"
      }
    ]
  }
}
```

**Why Deferred:** Requires defining what "hobby work" means and how to evaluate value. Complex incentive design.

---

### Smart Bot Specialization Over Time

**Concept:** Allow bots to develop specializations based on success patterns.

**Mechanics:**
- Track which types of issues each bot excels at (infrastructure, refactoring, new features, bug fixes)
- Adjust bot selection probability based on issue type
- Example: If smart_bot_1 has 90% success rate on infrastructure issues, prioritize it for infra work

**Implementation:**
```python
def select_bots_for_competition(issue):
    issue_type = classify_issue(issue)  # "infrastructure", "feature", "refactor", etc.

    # Adjust bot weights based on issue type
    adjusted_weights = {}
    for bot in bots:
        base_weight = bot.weight
        specialization_bonus = bot.success_rate_for_type(issue_type) - bot.overall_success_rate
        adjusted_weights[bot] = base_weight * (1 + specialization_bonus)

    # Select N bots with highest adjusted weights
    return select_top_n(adjusted_weights, n=3)
```

**Why Deferred:** Need substantial history to identify genuine specialization vs. noise.

---

## Phase 5 - External System Integrations

### Dumb Bot External Access

**Approved for future implementation:**

1. **Infrastructure Specialist - AWS APIs (Read-Only)**
   - Query: `aws ecs describe-services`, `aws logs get-log-events`
   - Use case: Check current deployment state before proposing infrastructure changes
   - Permission gate: Requires user approval for AWS API calls

2. **Risk Specialist - Sentry Integration (Read-Only)**
   - Query recent error logs, search patterns, identify frequently occurring issues
   - Use case: Prioritize fixes for high-frequency errors
   - Permission gate: Sentry API key required

3. **Trading Specialist - Alpaca APIs (Read-Only)**
   - Query account status, positions, order history
   - Use case: Validate portfolio state before proposing trading logic changes
   - Permission gate: Alpaca API key required (paper trading only initially)

**Implementation Approach:**
```python
# Specialist agent with external access
def infrastructure_specialist_with_aws(query):
    if "aws" in query.lower():
        # Check if AWS access is permitted
        if not user_approved_aws_access():
            return "AWS access requires user approval"

        # Execute read-only AWS command
        result = subprocess.run(["aws", "ecs", "describe-services", ...])
        return parse_aws_output(result)
    else:
        # Standard codebase query
        return standard_specialist_response(query)
```

**Security Considerations:**
- All external calls are read-only
- API calls logged for auditability
- Rate limiting to prevent API abuse
- Sensitive data filtered from responses

**Why Deferred:** Requires permission system design and API credential management.

---

## Phase 6 - Concurrency and Scale

### Multi-User Conflict Resolution

**Current solution:** Append-only event log handles merge conflicts gracefully.

**Future enhancements:**

1. **Branch-Aware Learning**
   - Each branch maintains separate learning state
   - On merge, branch learnings fold into main via weighted average
   - Prevents one user's bad experiences from immediately penalizing all users

2. **User-Specific Bot Instances**
   - Users can maintain personal bot instances (like config profiles)
   - Local specialization: "My bots are good at Python, yours at Rust"
   - Optional sync: Periodically merge local learnings to shared state

3. **Distributed Marketplace**
   - Multiple marketplace instances running concurrently
   - Cross-instance learning via periodic sync
   - Useful for large teams working on different issue streams

**Why Deferred:** Single-user or small team usage doesn't require this complexity yet.

---

### Scaling to More Bots

**Current:** 3 smart bots competing per round.

**Future:** Dynamic bot count based on issue complexity.

**Heuristics for bot selection:**
```python
def determine_bot_count(issue):
    # Factors:
    # - Checkbox count (more requirements = more bots)
    # - Estimated complexity (from spec keywords)
    # - Historical difficulty (similar issues that failed)

    base_count = 3

    if issue.checkboxes > 5:
        base_count += 1
    if "refactor" in issue.title.lower():
        base_count += 1
    if issue.has_label("complex"):
        base_count += 1

    return min(base_count, 5)  # Cap at 5 bots
```

**Why Deferred:** Need to validate that 3 bots provides sufficient diversity before scaling up.

---

## Phase 7 - Human Feedback Integration

### Interactive Arbiter Review

**Concept:** Allow human to review arbiter's proposal rankings before implementation.

**Workflow:**
```
1. Arbiter evaluates 3 proposals, ranks them
2. Display rankings + scores to user
3. User can:
   - Approve top proposal (proceed with implementation)
   - Override ranking (select different proposal)
   - Request revisions (send feedback to bots, replan)
4. Arbiter implements approved proposal
```

**Learning signal:** Track human overrides. If human frequently overrides arbiter, it suggests scoring weights are miscalibrated.

**Why Deferred:** Current design prioritizes full autonomy. Add human review after validating autonomous performance.

---

### Bot Personality Tuning

**Concept:** Allow users to tune bot personalities/behavior.

**Example tuning parameters:**
```json
{
  "smart_bot_1": {
    "risk_tolerance": "conservative",  // "aggressive", "moderate", "conservative"
    "innovation_preference": "high",   // "high", "medium", "low"
    "verbosity": "detailed"            // "terse", "normal", "detailed"
  }
}
```

**Use case:** Different teams or issue types might prefer different bot behaviors. Infrastructure work might prefer conservative bots, new features might prefer innovative bots.

**Why Deferred:** Fixed personalities initially to establish baseline performance.

---

## Implementation Notes

### Testing Strategy

**Unit Tests:**
- `test_ralph_marketplace_state.py` - State loading, event processing, cache invalidation
- `test_ralph_marketplace_weights.py` - Weight update calculations, normalization, constraints
- `test_ralph_marketplace_budget.py` - Budget allocation, zero-sum guarantee

**Integration Tests:**
- `test_marketplace_loop_success.py` - Full loop with successful implementation
- `test_marketplace_loop_failure.py` - Failure handling, replan rounds
- `test_marketplace_concurrency.py` - Event log merge scenarios

**Test Coverage Goal:** 90% line coverage (per CLAUDE.md standards)

---

### Monitoring and Observability

**Metrics to Track:**
- Round completion time (arbiter evaluation + implementation)
- Proposal evaluation time (per proposal)
- Implementation success rate (overall and per bot)
- Weight distribution over time (are bots converging or staying diverse?)
- Replan frequency (how often do initial proposals fail?)

**Dashboards:**
- Bot performance trends (weight, efficiency, accuracy over time)
- Scoring dimension correlations (which dimensions predict success?)
- Issue complexity vs. success rate

---

### Migration Path

**From existing Ralph to Marketplace:**

1. **Phase 1: Parallel deployment**
   - Keep existing `mask ralph loop` as-is
   - Add new `mask ralph marketplace loop` as opt-in
   - Users choose which to use per issue

2. **Phase 2: Gradual adoption**
   - Tag issues with "marketplace-friendly" for suitable issues
   - Collect data comparing marketplace vs. traditional loop

3. **Phase 3: Default to marketplace**
   - After 50+ successful marketplace completions, make it default
   - `mask ralph loop` becomes alias for `mask ralph marketplace loop`
   - Keep traditional loop available as `mask ralph loop --legacy`

4. **Phase 4: Deprecation (optional)**
   - After 6 months, consider deprecating legacy loop if marketplace proves superior
   - Or maintain both for different use cases (marketplace for complex, legacy for simple)

---

## Open Questions

1. **Bot identity revelation:** When should bots know they're competing vs. collaborating?
   - Current design: Bots are blind to competition during proposal phase
   - Alternative: Bots know they're competing and explicitly try to differentiate

2. **Proposal diversity enforcement:** Should arbiter reject proposals that are too similar?
   - Risk: All 3 bots converge on same approach (wasted compute)
   - Solution: Penalize low-diversity proposal sets?

3. **Arbiter bias detection:** How to detect if arbiter develops systematic biases?
   - Example: Always preferring bot that submits first (despite tie-breaker only for equal scores)
   - Solution: Track if tie-breaker is invoked disproportionately

4. **Failure categorization:** Should different failure types have different weight penalties?
   - Test failure vs. lint failure vs. spec misalignment
   - More severe penalties for spec misalignment (misunderstood requirements)?

5. **Cross-language issues:** How to handle issues touching both Python and Rust?
   - Current: Single arbiter implements everything
   - Alternative: Hybrid approach with language-specific sub-implementations?

---

## Related Work and Inspiration

- **Multi-armed bandit algorithms:** Weight updates resemble bandit exploration/exploitation
- **Prediction markets:** Marketplace mechanism similar to information aggregation markets
- **Ensemble methods:** Multiple models voting/competing for best prediction
- **Test-driven development:** Comprehensive checks as verification gate
- **A/B testing frameworks:** Comparing multiple approaches with statistical rigor

---

## Success Criteria for Phase 1

Before moving to Phase 2, validate:
- [ ] 50+ successful issue completions via marketplace
- [ ] Average iteration count < 15 (efficiency vs. max 30 budget)
- [ ] Replan frequency < 20% (most first proposals succeed)
- [ ] Weight distribution stays diverse (no single bot > 0.60 weight)
- [ ] Zero critical bugs introduced by marketplace implementations
- [ ] User satisfaction (qualitative feedback from team)

---

## Documentation Needed

- User guide: How to interpret proposal rankings and bot performance
- Operator guide: How to tune weights, reset marketplace, debug failures
- Developer guide: How to add new dumb bot specialists
- Architecture diagram: Data flow from issue → proposals → implementation → PR

---

End of followups document.
