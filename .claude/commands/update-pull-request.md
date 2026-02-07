# Update Pull Request

> Address PR feedback and fix failing checks

## Important: Context Requirements

**This command requires continuous context throughout execution.** Do not clear context between steps, as this will cause loss of:
- PR data file paths and metadata
- Comment and thread IDs needed for responses
- Mapping between feedback items and their resolution mechanisms

If you need to accept edits during execution:
- Choose "accept edits and continue" (NOT "clear context")
- Or wait until Section 10 (commit stage) to accept all edits at once

## Instructions

Analyze and address all feedback and failing checks on a GitHub pull request, then respond to and resolve all comments.

Follow these steps:

### 1. Fetch PR Data

- Accept the pull request ID from ${ARGUMENTS}; error if no argument is provided with a clear message that a PR number is required.
- Determine the scratchpad directory path from the system reminder message (shown at session start, format: `/private/tmp/claude-*/scratchpad`). Use this for all temporary file storage instead of `/tmp/` to ensure session isolation and automatic cleanup.
- Determine repository owner and name from git remote: extract from `git remote get-url origin` (format: `https://github.com/owner/repo.git` or `git@github.com:owner/repo.git`) and export variables:
  - `OWNER=<extracted_owner>`
  - `REPO=<extracted_repo>`
- Fetch comprehensive PR data using a single GraphQL query, saving to a file to avoid token limit issues:

  ```bash

  gh api graphql -f query='
    query($owner: String!, $repo: String!, $number: Int!) {
      repository(owner: $owner, name: $repo) {
        pullRequest(number: $number) {
          id
          title
          headRefName
          headRefOid
          baseRefName

          reviewThreads(first: 100) {
            nodes {
              id
              isResolved
              isOutdated
              comments(first: 100) {
                nodes {
                  id
                  databaseId
                  body
                  author { login }
                  path
                  position
                  createdAt
                }
              }
            }
          }

          comments(first: 100) {
            nodes {
              id
              databaseId
              body
              author { login }
              createdAt
            }
          }

          reviews(first: 50) {
            nodes {
              id
              state
              body
              author { login }
              submittedAt
            }
          }

          commits(last: 1) {
            nodes {
              commit {
                checkSuites(first: 50) {
                  nodes {
                    workflowRun {
                      id
                      databaseId
                    }
                    checkRuns(first: 50) {
                      nodes {
                        name
                        status
                        conclusion
                        detailsUrl
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
  ' -f owner="${OWNER}" -f repo="${REPO}" -F number=${ARGUMENTS} > ${SCRATCHPAD}/pr_data.json
  ```

- Validate that the PR data was fetched successfully:

  ```bash
  # Check file exists
  if [ ! -f "${SCRATCHPAD}/pr_data.json" ]; then
    echo "Error: Failed to fetch PR data. Check that PR #${ARGUMENTS} exists and gh is authenticated."
    exit 1
  fi

  # Validate JSON structure
  if ! jq empty "${SCRATCHPAD}/pr_data.json" 2>/dev/null; then
    echo "Error: PR data file contains invalid JSON"
    exit 1
  fi
  ```

- This single query replaces multiple REST API calls and includes thread IDs needed for later resolution.
- **Important**: Save output to a file (`${SCRATCHPAD}/pr_data.json`) to avoid token limit errors when reading large responses. Parse this file using `jq` for subsequent processing.
- **Critical**: The PR data file will be too large to read directly with the Read tool. Always use `jq` to parse and extract specific fields. Never attempt to read the entire file.

### 2. Analyze Check Failures

- Identify failing checks (Python or Rust checks specifically). Note that check-runs and workflow runs are distinct; to fetch logs, first obtain the workflow run ID from the check-run's check_suite, then use `gh api repos/${OWNER}/${REPO}/actions/runs/{run_id}/logs` (replacing `{run_id}` with the actual run ID).
- If logs are inaccessible via API, run `mask development python all` or `mask development rust all` locally to replicate the errors and capture the failure details.
- Add check failures to the list of items that need fixes.

### 3. Group and Analyze Feedback

- Parse the saved PR data from `${SCRATCHPAD}/pr_data.json` using these extraction rules:
  - From `data.repository.pullRequest.reviewThreads.nodes[]`:
    - **First, identify outdated threads**: Filter for `isResolved: false` AND `isOutdated: true`
      - Auto-resolve these immediately (see step 3a below) since they're no longer relevant to current code
    - **Then, extract unresolved threads**: Filter for `isResolved: false` AND `isOutdated: false`
    - For each unresolved thread, extract:
      - Thread ID: `.id` (format: `PRRT_*`) for later resolution
      - For each comment in `.comments.nodes[]`:
        - Comment database ID: `.databaseId` (integer)
        - Comment node ID: `.id` (format: `PRRC_*`) for GraphQL replies
        - Comment body: `.body`
        - Author: `.author.login`
        - File location: `.path` and `.position`
  - From `data.repository.pullRequest.comments.nodes[]`:
    - Extract issue-level comments (PR conversation):
      - Comment database ID: `.databaseId`
      - Comment body: `.body`
      - Author: `.author.login`
  - From check runs in `commits.nodes[].commit.checkSuites.nodes[].checkRuns.nodes[]`:
    - Filter where `conclusion: "FAILURE"` or `conclusion: "TIMED_OUT"`
- Store complete metadata for each feedback item to use in later steps.
- Group related feedback using judgement: by file, by theme, by type of change, or whatever makes most sense for the specific PR; ensure each group maintains the full metadata for all comments it contains.
- Analyze dependencies between feedback groups to determine which are independent (can be worked in parallel) and which are interdependent (must be handled sequentially).
- For each piece of feedback, evaluate whether to address it (make code changes) or reject it (explain why the feedback doesn't apply); provide clear reasoning for each decision.

### 3a. Resolve Outdated Threads

- Before processing feedback, auto-resolve all outdated threads that are still unresolved:

  ```bash
  # Log outdated threads before resolution for verification
  echo "=== Auto-resolving outdated threads ==="
  jq -r '.data.repository.pullRequest.reviewThreads.nodes[] | select(.isResolved == false and .isOutdated == true) | "\(.id) | \(.comments.nodes[0].path) | \(.comments.nodes[0].author.login) | \(.comments.nodes[0].body[:80])"' ${SCRATCHPAD}/pr_data.json

  # Extract outdated thread IDs and resolve
  jq -r '.data.repository.pullRequest.reviewThreads.nodes[] | select(.isResolved == false and .isOutdated == true) | .id' ${SCRATCHPAD}/pr_data.json | while read thread_id; do
    gh api graphql -f query="
      mutation {
        resolveReviewThread(input: {threadId: \"$thread_id\"}) {
          thread { id isResolved }
        }
      }
    "
  done
  ```

- Log which threads were auto-resolved as outdated for the final summary.

### 4. Enter Plan Mode

- Enter plan mode to organize the work.
- Present a high-level overview showing: (1) total number of feedback groups identified, (2) brief description of each group, (3) which groups are independent vs interdependent, (4) check failures that need fixes.
- Wait for user acknowledgment of the overview before proceeding to detailed planning.

### 5. Present Plans Consecutively

**Default to consecutive presentation to provide clear, reviewable chunks** - even for superficial fixes. Present each logical group separately and wait for approval before proceeding.

- For each feedback group:
  - Present a detailed plan including: grouped feedback items with metadata (comment IDs, commenters, file/line), recommended actions (address vs reject with reasoning), and implementation approach for fixes.
  - For non-trivial changes, pause and ask "Is there a more elegant solution?" before implementing.
  - Wait for user approval of this group's plan before proceeding.

- Only when explicitly grouping truly independent work that benefits from simultaneous execution:
  - Note: "The following groups are independent and will be implemented in parallel using subagents to keep main context clean."
  - Wait for user approval before spawning parallel subagents.

### 6. Implement Fixes

- After plan approval for a group (or parallel groups):
  - For independent groups, spawn parallel subagents to implement solutions simultaneously, keeping main context clean.
  - For interdependent groups, implement sequentially.
  - Implement all fixes for feedback being addressed and for check failures in this group.

### 7. Verify Changes

- After implementing each group's fixes, run verification checks locally:
  - Run `mask development python all` if any Python files were modified.
  - Run `mask development rust all` if any Rust files were modified.
  - Skip redundant checks if the next group will touch the same language files (batch them), but always run comprehensive checks at the end.
  - **Note**: Local verification confirms fixes work in the development environment, but remote CI on the PR will not re-run or reflect these results until changes are pushed in Section 10.
- If checks fail, resolve issues and re-run until passing before moving to the next group.
- Do not proceed to the next feedback group until current group's changes pass verification.

### 8. Iterate Through All Groups

- Repeat steps 5-7 for each feedback group until all have been addressed.
- Always run final comprehensive verification using both `mask development python all` and `mask development rust all` regardless of which files were changed.

### 9. Respond to and Resolve Comments

- For each piece of feedback (both addressed and rejected), draft a response comment explaining what was done or why it was rejected, using the commenter name for personalization.
- Post all response comments to their respective threads:
  - For review comments (code-level), use GraphQL `addPullRequestReviewComment` mutation:

    ```bash
    # IMPORTANT: Keep response text simple - avoid newlines, code blocks, and special characters
    # GraphQL string literals cannot contain raw newlines; use spaces or simple sentences
    # If complex formatting is needed, save response to a variable first and ensure proper escaping

    gh api graphql -f query='
      mutation {
        addPullRequestReviewComment(input: {
          pullRequestId: "<pr_node_id>",
          body: "<response_text>",
          inReplyTo: "<comment_node_id>"
        }) {
          comment { id }
        }
      }
    '
    ```

    Use the PR's node ID from step 1's query (`data.repository.pullRequest.id`) for `pullRequestId`.
    Use the comment's node ID (format: `PRRC_*`) for `inReplyTo` parameter.

    **Response formatting guidelines**:
    - Keep responses concise and single-line when possible
    - Avoid embedding code blocks or complex markdown in mutation strings
    - Use simple sentences: "Fixed in step X" or "Updated to use GraphQL approach"
    - For longer responses, reference line numbers or file paths instead of quoting code

  - For issue comments (PR-level), use REST API:

    ```bash
    gh api repos/${OWNER}/${REPO}/issues/"${ARGUMENTS}"/comments -f body="<response_text>"
    ```

- For each response posted, capture the returned comment ID for verification.
- Auto-resolve all comment threads after posting responses:
  - For review comment threads:
    - Use the thread ID (format: `PRRT_*`) captured during parsing from GraphQL response's `reviewThreads.nodes[].id` field.
    - Resolve thread using GraphQL mutation:

      ```bash
      gh api graphql -f query='
        mutation {
          resolveReviewThread(input: {threadId: "<thread_id>"}) {
            thread {
              id
              isResolved
            }
          }
        }
      '
      ```

    - Map each comment back to its parent thread using the data structure from step 3 parsing.
    - Resolve both addressed and rejected feedback threads (explanation provided in response).
    - **Note**: Threads are resolved based on local verification. The fixes will not appear on the remote PR branch until Section 10's commit and push. Remote CI will not re-run until changes are pushed.

  - For issue comments (PR-level):
    - No resolution mechanism (issue comments don't have thread states).
    - Only post response; no resolution step needed.

- **Do not create any git commits yet.** All changes remain unstaged.

### 10. Commit Changes (After User Confirmation)

- After user confirms that responses and resolutions are correct, create a git commit:
  - Stage all modified files: `git add <files>`
  - Create commit with descriptive message following CLAUDE.md conventions:
    - Include detailed summary of what was fixed and why
    - Reference PR number
    - Add co-author line: extract model name from system context (format: "You are powered by the model named X") and use `Co-Authored-By: Claude X <noreply@anthropic.com>`
  - Example:

    ```bash
    git add file1.py file2.md
    git commit -m "$(cat <<'EOF'
    Address PR #<number> feedback: <brief summary>

    <Detailed explanation of changes made>

    Co-Authored-By: Claude <model_name> <noreply@anthropic.com>
    EOF
    )"
    ```

- Ask user: "Ready to push these changes to the remote branch to update the PR? This will trigger remote CI to re-run and make the fixes visible to other reviewers."

### 11. Final Summary

- Provide a comprehensive summary showing:
  - Outdated threads auto-resolved (count and thread IDs).
  - Total feedback items processed (with count of addressed vs rejected).
  - Which checks were fixed.
  - Confirmation that all comments have been responded to and resolved.
  - Final verification status (all local checks passing; note that remote CI status will update after pushing changes).
- For check failures that were fixed, note that no comments were posted - the fixes will be reflected in re-run checks after pushing to the remote branch.
