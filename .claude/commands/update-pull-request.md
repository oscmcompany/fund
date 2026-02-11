# Update Pull Request

> Address pull request feedback and fix failing checks

## Important: Context Requirements

**This command requires continuous context throughout execution.** Do not clear context between steps, as this will cause loss of:
- Pull request data file paths and metadata
- Comment and thread IDs needed for responses
- Mapping between feedback items and their resolution mechanisms

If you need to accept edits during execution:
- Choose "accept edits and continue" (NOT "clear context")
- Or wait until the "Commit Changes" step to accept all edits at once

## Instructions

Analyze and address all feedback and failing checks on a GitHub pull request, then respond to and resolve all comments.

Follow these steps:

### 1. Fetch Pull Request Data

- Accept the pull request ID from ${ARGUMENTS}; error if no argument is provided with a clear message that a pull request number is required.
- **CRITICAL: Set up SCRATCHPAD environment variable FIRST** before any file operations:

  ```bash
  # Determine scratchpad from system or use default
  SCRATCHPAD="${SCRATCHPAD:-/tmp/claude-scratchpad}"
  mkdir -p "${SCRATCHPAD}"
  export SCRATCHPAD

  # Verify scratchpad is writable
  if [ ! -w "${SCRATCHPAD}" ]; then
    echo "Error: Scratchpad directory ${SCRATCHPAD} is not writable"
    exit 1
  fi

  echo "Using scratchpad: ${SCRATCHPAD}"
  ```

- Determine repository owner and name from git remote and export as environment variables:

  ```bash
  export OWNER=$(git remote get-url origin | sed -E 's|.*[:/]([^/]+)/([^/]+)\.git|\1|')
  export REPO=$(git remote get-url origin | sed -E 's|.*[:/]([^/]+)/([^/]+)\.git|\2|')

  echo "Repository: ${OWNER}/${REPO}"
  ```

- Fetch comprehensive pull request data using a single GraphQL query, saving to a file to avoid token limit issues:

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

- Validate that the pull request data was fetched successfully:

  ```bash
  # Check jq is installed
  if ! command -v jq >/dev/null 2>&1; then
    echo "Error: jq is required but not installed. Install with: brew install jq (macOS) or apt-get install jq (Linux)"
    exit 1
  fi

  # Check file exists
  if [ ! -f "${SCRATCHPAD}/pr_data.json" ]; then
    echo "Error: Failed to fetch pull request data to ${SCRATCHPAD}/pr_data.json using 'gh api'. Check that pull request #${ARGUMENTS} exists, run 'gh auth status' to verify authentication, then retry the fetch command."
    exit 1
  fi

  # Validate JSON structure and GraphQL response
  if ! jq empty "${SCRATCHPAD}/pr_data.json" 2>/dev/null; then
    echo "Error: pull request data file ${SCRATCHPAD}/pr_data.json contains invalid JSON. Try re-running the 'gh api' fetch command; if the problem persists, run 'gh auth status' and verify network access before retrying."
    exit 1
  fi

  # Validate GraphQL response structure
  if ! jq -e '.data.repository.pullRequest.id and (.errors | not)' "${SCRATCHPAD}/pr_data.json" >/dev/null 2>&1; then
    echo "Error: pull request data missing expected fields or contains GraphQL errors. Check pull request #${ARGUMENTS} exists and you have access."
    jq -r '.errors[]?.message // "No specific error message available"' "${SCRATCHPAD}/pr_data.json"
    exit 1
  fi
  ```

- This single query replaces multiple REST API calls and includes thread IDs needed for later resolution.
- **Important**: Save output to a file (`${SCRATCHPAD}/pr_data.json`) to avoid token limit errors when reading large responses. Parse this file using `jq` for subsequent processing.
- **Critical**: The pull request data file will be too large to read directly with the Read tool. Extract structured data once into smaller files (see below).

- **Extract structured data into focused files** (replaces all scattered jq calls throughout command):

  ```bash
  echo "Extracting structured data from PR response..."

  # Extract PR metadata
  jq '.data.repository.pullRequest | {
    id: .id,
    title: .title,
    headRefName: .headRefName,
    baseRefName: .baseRefName
  }' "${SCRATCHPAD}/pr_data.json" > "${SCRATCHPAD}/metadata.json"

  # Extract unresolved review threads
  jq '[.data.repository.pullRequest.reviewThreads.nodes[] |
    select(.isResolved == false and .isOutdated == false) | {
    threadId: .id,
    comments: [.comments.nodes[] | {
      id: .id,
      databaseId: .databaseId,
      body: .body,
      author: .author.login,
      path: .path,
      position: .position
    }]
  }]' "${SCRATCHPAD}/pr_data.json" > "${SCRATCHPAD}/review_threads.json"

  # Extract outdated threads
  jq '[.data.repository.pullRequest.reviewThreads.nodes[] |
    select(.isResolved == false and .isOutdated == true) | {
    threadId: .id,
    comments: [.comments.nodes[] | {
      id: .id,
      body: .body,
      author: .author.login,
      path: .path
    }]
  }]' "${SCRATCHPAD}/pr_data.json" > "${SCRATCHPAD}/outdated_threads.json"

  # Extract PR-level comments
  jq '[.data.repository.pullRequest.comments.nodes[] | {
    id: .id,
    databaseId: .databaseId,
    body: .body,
    author: .author.login
  }]' "${SCRATCHPAD}/pr_data.json" > "${SCRATCHPAD}/pr_comments.json"

  # Extract check failures
  jq '[.data.repository.pullRequest.commits.nodes[].commit.checkSuites.nodes[] |
    .checkRuns.nodes[] |
    select(.conclusion == "FAILURE" or .conclusion == "TIMED_OUT") | {
    name: .name,
    conclusion: .conclusion,
    detailsUrl: .detailsUrl,
    workflowRunId: (.checkSuite.workflowRun.databaseId // null)
  }] | unique_by(.name)' "${SCRATCHPAD}/pr_data.json" > "${SCRATCHPAD}/check_failures.json"

  echo "Data extraction complete:"
  echo "  - metadata.json (PR info)"
  echo "  - review_threads.json ($(jq 'length' ${SCRATCHPAD}/review_threads.json) unresolved threads)"
  echo "  - outdated_threads.json ($(jq 'length' ${SCRATCHPAD}/outdated_threads.json) outdated threads)"
  echo "  - pr_comments.json ($(jq 'length' ${SCRATCHPAD}/pr_comments.json) PR comments)"
  echo "  - check_failures.json ($(jq 'length' ${SCRATCHPAD}/check_failures.json) failed checks)"
  ```

- These smaller structured files can be read with Read tool if needed, and eliminate redundant jq parsing throughout the command.

### 2. Analyze Check Failures

- Review failing checks from the extracted data:

  ```bash
  echo "=== Check Failures ==="
  jq -r '.[] | "[\(.conclusion)] \(.name) - \(.detailsUrl)"' "${SCRATCHPAD}/check_failures.json"
  ```

- Note that check-runs and workflow runs are distinct; to fetch logs, first obtain the workflow run ID, then use `gh api repos/${OWNER}/${REPO}/actions/runs/{run_id}/logs`.
- If logs are inaccessible via API, run `mask development python all` or `mask development rust all` locally to replicate the errors and capture the failure details.
- Add check failures to the list of items that need fixes.

### 3. Group and Analyze Feedback

- Review the extracted feedback data from structured files:

  ```bash
  echo "=== Unresolved Review Threads ==="
  jq -r '.[] | "\(.threadId) | \(.comments[0].path // "N/A") | \(.comments[0].author)"' "${SCRATCHPAD}/review_threads.json"

  echo ""
  echo "=== PR-level Comments ==="
  jq -r '.[] | "\(.databaseId) | \(.author) | \(.body[:80])"' "${SCRATCHPAD}/pr_comments.json"
  ```

- For detailed review of specific threads, use:

  ```bash
  # Get full details of a specific thread
  jq '.[] | select(.threadId == "PRRT_xxx")' "${SCRATCHPAD}/review_threads.json"
  ```

- The structured files contain all necessary metadata:
  - `review_threads.json`: Thread ID, comment IDs (both node and database), body, author, file path/position
  - `pr_comments.json`: Comment IDs, body, author
  - `check_failures.json`: Check name, conclusion, details URL

- Group related feedback using judgement: by file, by theme, by type of change, or whatever makes most sense for the specific pull request; ensure each group maintains the full metadata for all comments it contains.
- Analyze dependencies between feedback groups to determine which are independent (can be worked in parallel) and which are interdependent (must be handled sequentially).
- For each piece of feedback, evaluate whether to address it (make code changes) or reject it (explain why the feedback doesn't apply); provide clear reasoning for each decision.

### 3a. Identify Outdated Threads

- Before processing feedback, identify outdated threads that are still unresolved:

  ```bash
  echo "=== Outdated threads (require manual review) ==="
  jq -r '.[] | "\(.threadId) | \(.comments[0].path // "N/A") | \(.comments[0].author) | \(.comments[0].body[:80])"' "${SCRATCHPAD}/outdated_threads.json"
  ```

- **Important**: "Outdated" means the code was modified, not that feedback is irrelevant. Review each outdated thread manually during Step 3 to determine if the feedback still applies or should be addressed.
- Include outdated threads in your feedback grouping and analysis - they may still require responses or code changes.

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
  - **Note**: Local verification confirms fixes work in the development environment, but remote continuous integration on the pull request will not re-run or reflect these results until changes are pushed in the "Commit Changes" step.
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
    # Extract PR node ID from metadata
    PR_ID=$(jq -r '.id' "${SCRATCHPAD}/metadata.json")

    # IMPORTANT: Keep response text simple - avoid newlines, code blocks, and special characters
    # GraphQL string literals cannot contain raw newlines; use spaces or simple sentences
    # If complex formatting is needed, save response to a variable first and ensure proper escaping

    gh api graphql -f query='
      mutation {
        addPullRequestReviewComment(input: {
          pullRequestId: "'"${PR_ID}"'",
          body: "<response_text>",
          inReplyTo: "<comment_node_id>"
        }) {
          comment { id }
        }
      }
    '
    ```

    Use the PR node ID from `metadata.json` for `pullRequestId`.
    Use the comment node ID (format: `PRRC_*`) from `review_threads.json` for `inReplyTo` parameter.

    Example to get comment node ID:
    ```bash
    # Get first comment's node ID from a specific thread
    COMMENT_ID=$(jq -r '.[] | select(.threadId == "PRRT_xxx") | .comments[0].id' "${SCRATCHPAD}/review_threads.json")
    ```

    **Response formatting guidelines**:
    - Keep responses concise and single-line when possible
    - Avoid embedding code blocks or complex markdown in mutation strings
    - Use simple sentences: "Fixed in step X" or "Updated to use GraphQL approach"
    - For longer responses, reference line numbers or file paths instead of quoting code

  - For issue comments (pull request-level), use REST API:

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
    - **Note**: Threads are resolved based on local verification. The fixes will not appear on the remote pull request branch until the "Commit Changes" step. Remote continuous integration will not re-run until changes are pushed.

  - For issue comments (pull request-level):
    - No resolution mechanism (issue comments don't have thread states).
    - Only post response; no resolution step needed.

- **Do not create any git commits yet.** All changes remain unstaged.

### 10. Commit Changes (After User Confirmation)

- After user confirms that responses and resolutions are correct, create a git commit:
  - Stage all modified files: `git add <files>`
  - Create commit with descriptive message following CLAUDE.md conventions:
    - Include detailed summary of what was fixed and why
    - Reference pull request number
    - Add co-author line: extract model name from system context (format: "You are powered by the model named X") and use `Co-Authored-By: Claude X <noreply@anthropic.com>`
  - Example:

    ```bash
    git add file1.py file2.md
    git commit -m "$(cat <<'EOF'
    Address pull request #<number> feedback: <brief summary>

    <Detailed explanation of changes made>

    Co-Authored-By: Claude <model_name> <noreply@anthropic.com>
    EOF
    )"
    ```

- Ask user: "Ready to push these changes to the remote branch to update the pull request? This will trigger remote continuous integration to re-run and make the fixes visible to other reviewers."

### 11. Final Summary

- Provide a comprehensive summary showing:
  - Total feedback items processed (with count of addressed vs rejected), including any outdated threads that were reviewed.
  - Which checks were fixed.
  - Confirmation that all comments have been responded to and resolved.
  - Final verification status (all local checks passing; note that remote continuous integration status will update after pushing changes).
- For check failures that were fixed, note that no comments were posted - the fixes will be reflected in re-run checks after pushing to the remote branch.
