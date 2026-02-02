# Update Pull Request

> Address PR feedback and fix failing checks

## Instructions

Analyze and address all feedback and failing checks on a GitHub pull request, then respond to and resolve all comments.

Follow these steps:

### 1. Fetch PR Data

- Accept the pull request ID from $ARGUMENTS; error if no argument is provided with a clear message that a PR number is required.
- Clear any existing content from `.claude/tasks/todos.md` to start fresh for this PR work.
- Fetch comprehensive PR data using these API calls in parallel:
  - `gh api repos/:owner/:repo/pulls/$ARGUMENTS` for PR metadata
  - `gh api repos/:owner/:repo/pulls/$ARGUMENTS/comments` for review comments (code-level feedback)
  - `gh api repos/:owner/:repo/issues/$ARGUMENTS/comments` for issue comments (PR-level conversation)
  - `gh api repos/:owner/:repo/pulls/$ARGUMENTS/reviews` for full reviews with approval states
  - `gh api repos/:owner/:repo/commits/{commit_sha}/check-runs` for CI check statuses (get commit_sha from PR metadata)

### 2. Analyze Check Failures

- Identify failing checks (Python or Rust checks specifically) and attempt to fetch logs using `gh api repos/:owner/:repo/actions/runs/{run_id}/logs`.
- If logs are not accessible via API, run `mask development python all` or `mask development rust all` locally to replicate the errors and capture the failure details.
- Add check failures to the list of items that need fixes.

### 3. Group and Analyze Feedback

- Parse all feedback to identify open, unresolved comments that require action (filter out resolved threads and general approvals).
- For each piece of feedback, capture complete metadata: comment ID, commenter name, comment body, file path, line number (for review comments), and thread/conversation ID.
- Group related feedback using judgement: by file, by theme, by type of change, or whatever makes most sense for the specific PR; ensure each group maintains the full metadata for all comments it contains.
- Analyze dependencies between feedback groups to determine which are independent (can be worked in parallel) and which are interdependent (must be handled sequentially).
- For each piece of feedback, evaluate whether to address it (make code changes) or reject it (explain why the feedback doesn't apply); provide clear reasoning for each decision.

### 4. Enter Plan Mode (CLAUDE.md: "Plan Mode Default")

- Enter plan mode to organize the work.
- Present a high-level overview showing: (1) total number of feedback groups identified, (2) brief description of each group, (3) which groups are independent vs interdependent, (4) check failures that need fixes.
- Wait for user acknowledgment of the overview before proceeding to detailed planning.

### 5. Present Plans Consecutively

- For each feedback group (or set of independent groups that can be parallelized):
  - Present a detailed plan including: grouped feedback items with metadata (comment IDs, commenters, file/line), recommended actions (address vs reject with reasoning), and implementation approach for fixes.
  - For independent groups, explicitly note: "The following groups are independent and will be implemented in parallel using subagents per CLAUDE.md 'Subagent Strategy'."
  - Apply "Demand Elegance" principle (CLAUDE.md): pause and ask "Is there a more elegant solution?" for non-trivial changes.
  - Wait for user approval of this group's plan before proceeding.

### 6. Implement Fixes

- After plan approval for a group (or parallel groups):
  - For independent groups, spawn parallel subagents (CLAUDE.md: "Subagent Strategy") to implement solutions simultaneously, keeping main context clean.
  - For interdependent groups, implement sequentially.
  - Implement all fixes for feedback being addressed and for check failures in this group.

### 7. Verify Changes (CLAUDE.md: "Verification Before Done")

- After implementing each group's fixes, run verification checks locally:
  - Run `mask development python all` if any Python files were modified.
  - Run `mask development rust all` if any Rust files were modified.
  - Skip redundant checks if the next group will touch the same language files (batch them), but always run comprehensive checks at the end.
- If checks fail, fix issues and re-run until passing before moving to the next group.
- Do not proceed to the next feedback group until current group's changes pass verification.

### 8. Iterate Through All Groups

- Repeat steps 5-7 for each feedback group until all have been addressed.
- Always run final comprehensive verification using both `mask development python all` and `mask development rust all` regardless of which files were changed.

### 9. Respond to and Resolve Comments

- For each piece of feedback (both addressed and rejected), draft a response comment explaining what was done or why it was rejected, using the commenter name for personalization.
- Post all response comments to their respective threads:
  - Use `gh api repos/:owner/:repo/pulls/comments/{comment_id}/replies -f body="..."` for review comments (code-level).
  - Use `gh api repos/:owner/:repo/issues/comments -f body="..."` for issue comments (PR-level).
- Auto-resolve all comment threads using the appropriate GitHub mechanism for each comment type:
  - For review comments, use GraphQL API to resolve threads: `gh api graphql -f query='mutation { resolveReviewThread(input: {threadId: "..."}) { thread { isResolved } } }'`.
  - Resolve both addressed feedback and rejected feedback (since rejected feedback includes explanation in response).

### 10. Final Summary

- Provide a comprehensive summary showing:
  - Total feedback items processed (with count of addressed vs rejected).
  - Which checks were fixed.
  - Confirmation that all comments have been responded to and resolved.
  - Final verification status (all checks passing).
- For check failures that were fixed, note that no comments were posted - the fixes will be reflected in re-run checks.
