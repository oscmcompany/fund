# Update Pull Request

> Address pull request feedback and fix failing checks

## Important: Context Requirements

**This command requires continuous context throughout execution.** Do not clear context between steps, as this will cause loss of:
- Pull request data file paths and metadata
- Comment and thread IDs needed for responses
- Mapping between feedback items and their resolution mechanisms

If you need to accept edits during execution:
- Choose "accept edits and continue" (NOT "clear context")
- Or wait until the "Commit and Push Changes" step to accept all edits at once

## Important: API Usage

**Do NOT use MCP GitHub tools during this command.** They have shown session reliability issues. All GitHub API interactions must use `gh api` (REST) or Python `urllib.request` (GraphQL mutations only). See step 8 for details on which operations require Python.

## Instructions

Analyze and address all feedback and failing checks on a GitHub pull request, then respond to and resolve all comments.

Follow these steps:

### 1. Fetch Pull Request Data

- Accept the pull request ID from ${ARGUMENTS}; error if no argument is provided with a clear message that a pull request number is required.
- **IMPORTANT: Environment variable persistence** - When using the Bash tool, environment variables do not persist between separate tool invocations. You must explicitly re-declare `SCRATCHPAD`, `OWNER`, and `REPO` at the top of each subsequent bash block that references them.
- Set up the scratchpad directory, determine the repository owner and name, and fetch PR data in a single bash execution:

  ```bash
  # Set up scratchpad directory with session isolation
  : "${TMPDIR:=/tmp}"
  _old_umask=$(umask)
  umask 077
  SCRATCHPAD="$(mktemp -d "${TMPDIR%/}/claude-scratchpad.XXXXXX")" || {
    echo "Error: Failed to create scratchpad directory under ${TMPDIR}"
    exit 1
  }
  umask "${_old_umask}"
  echo "Using scratchpad: ${SCRATCHPAD}"

  # Determine repository owner and name
  _remote_url=$(git remote get-url origin)
  OWNER=$(echo "${_remote_url}" | sed -E 's|.*[:/]([^/]+)/([^/]+)(\.git)?$|\1|')
  REPO=$(echo "${_remote_url}" | sed -E 's|.*[:/]([^/]+)/([^/]+)(\.git)?$|\2|' | sed 's/\.git$//')

  if [ -z "${OWNER}" ] || [ -z "${REPO}" ]; then
    echo "Error: Failed to determine repository owner and name from git remote \"origin\""
    exit 1
  fi

  echo "Repository: ${OWNER}/${REPO}"

  # Fetch PR data via REST — raw responses saved to scratchpad, never read directly into context
  gh api "repos/${OWNER}/${REPO}/pulls/${ARGUMENTS}"          > "${SCRATCHPAD}/pr_meta_raw.json"
  gh api "repos/${OWNER}/${REPO}/issues/${ARGUMENTS}/comments" > "${SCRATCHPAD}/pr_comments_raw.json"
  gh api "repos/${OWNER}/${REPO}/pulls/${ARGUMENTS}/reviews"   > "${SCRATCHPAD}/pr_reviews_raw.json"
  gh api "repos/${OWNER}/${REPO}/pulls/${ARGUMENTS}/comments"  > "${SCRATCHPAD}/pr_review_comments_raw.json"

  HEAD_SHA=$(jq -r '.head.sha' "${SCRATCHPAD}/pr_meta_raw.json")
  gh api "repos/${OWNER}/${REPO}/commits/${HEAD_SHA}/check-runs" > "${SCRATCHPAD}/check_runs_raw.json"

  # Fetch review thread node IDs (PRRT_* format) needed for resolving threads later.
  # Uses an inline GraphQL query with values embedded as literals (no $variable syntax),
  # which avoids the bash ! history-expansion issue that breaks parameterized GraphQL queries.
  INLINE_QUERY=$(printf '{repository(owner:"%s",name:"%s"){pullRequest(number:%s){reviewThreads(first:100){nodes{id isResolved comments(first:1){nodes{databaseId}}}}}}}' \
    "${OWNER}" "${REPO}" "${ARGUMENTS}")
  gh api graphql -f query="${INLINE_QUERY}" > "${SCRATCHPAD}/thread_ids_raw.json"

  echo "PR data fetched to ${SCRATCHPAD}/"
  echo "SCRATCHPAD=${SCRATCHPAD}"
  ```

- Validate the fetched data:

  ```bash
  SCRATCHPAD="<value printed above>"

  if ! command -v jq >/dev/null 2>&1; then
    echo "Error: jq is required but not installed. Install with: brew install jq (macOS) or apt-get install jq (Linux)"
    exit 1
  fi

  for f in pr_meta_raw pr_comments_raw pr_reviews_raw pr_review_comments_raw check_runs_raw thread_ids_raw; do
    if [ ! -f "${SCRATCHPAD}/${f}.json" ] || ! jq empty "${SCRATCHPAD}/${f}.json" 2>/dev/null; then
      echo "Error: Failed to fetch ${f}.json. Check that pull request #${ARGUMENTS} exists, run 'gh auth status' to verify authentication, then retry."
      exit 1
    fi
  done

  if ! jq -e '.number' "${SCRATCHPAD}/pr_meta_raw.json" >/dev/null 2>&1; then
    echo "Error: Pull request data missing expected fields. Check pull request #${ARGUMENTS} exists and you have access."
    exit 1
  fi

  echo "Validation passed"
  ```

- **Critical**: The raw files will be too large to read directly with the Read tool. Extract structured data once into smaller files:

  ```bash
  SCRATCHPAD="<value printed above>"

  echo "Extracting structured data from raw responses..."

  # PR metadata
  jq '{
    number: .number,
    title: .title,
    headRefName: .head.ref,
    baseRefName: .base.ref,
    headSha: .head.sha
  }' "${SCRATCHPAD}/pr_meta_raw.json" > "${SCRATCHPAD}/metadata.json"

  # PR-level (issue) comments — REST uses .user.login, not .author.login
  jq '[.[] | {id: .id, databaseId: .id, body: .body, author: .user.login}]' \
    "${SCRATCHPAD}/pr_comments_raw.json" > "${SCRATCHPAD}/pr_comments.json"

  # Inline review comments — root threads only (in_reply_to_id == null)
  # rootCommentId is the integer ID used to post replies via REST
  jq '[.[] | select(.in_reply_to_id == null) | {
    rootCommentId: .id,
    path: .path,
    line: (.line // .original_line),
    body: .body,
    author: .user.login
  }]' "${SCRATCHPAD}/pr_review_comments_raw.json" > "${SCRATCHPAD}/review_comments.json"

  # Check failures — REST uses lowercase conclusion values ("failure", "timed_out")
  jq '[.check_runs[] |
    select(.conclusion == "failure" or .conclusion == "timed_out") | {
    name: .name,
    conclusion: .conclusion,
    detailsUrl: .details_url
  }] | unique_by(.name)' "${SCRATCHPAD}/check_runs_raw.json" > "${SCRATCHPAD}/check_failures.json"

  # Thread node IDs — links PRRT_* IDs to root comment database IDs for resolution
  jq '[.data.repository.pullRequest.reviewThreads.nodes[] | {
    threadId: .id,
    isResolved: .isResolved,
    rootCommentId: .comments.nodes[0].databaseId
  }]' "${SCRATCHPAD}/thread_ids_raw.json" > "${SCRATCHPAD}/thread_ids.json"

  echo "Data extraction complete:"
  echo "  - metadata.json"
  echo "  - pr_comments.json        ($(jq 'length' "${SCRATCHPAD}/pr_comments.json") comments)"
  echo "  - review_comments.json    ($(jq 'length' "${SCRATCHPAD}/review_comments.json") root review threads)"
  echo "  - check_failures.json     ($(jq 'length' "${SCRATCHPAD}/check_failures.json") failed checks)"
  echo "  - thread_ids.json         ($(jq 'length' "${SCRATCHPAD}/thread_ids.json") threads)"
  ```

- These smaller structured files can be read with the Read tool if needed, and eliminate redundant jq parsing throughout the command.

### 2. Analyze PR State

- Review all extracted data to build a complete picture of what needs to be addressed:

  ```bash
  SCRATCHPAD="<value printed above>"

  echo "=== Check Failures ==="
  jq -r '.[] | "[\(.conclusion)] \(.name) - \(.detailsUrl)"' "${SCRATCHPAD}/check_failures.json"

  echo ""
  echo "=== Review Comments (root threads only) ==="
  jq -r '.[] | "[\(.rootCommentId)] @\(.author) on \(.path):\(.line // "?"): \(.body[:80])"' \
    "${SCRATCHPAD}/review_comments.json"

  echo ""
  echo "=== PR-level Comments ==="
  jq -r '.[] | "\(.databaseId) | \(.author) | \(.body[:80])"' "${SCRATCHPAD}/pr_comments.json"

  echo ""
  echo "=== Thread Resolution Status ==="
  jq -r '.[] | "\(.threadId) | rootComment=\(.rootCommentId) | resolved=\(.isResolved)"' \
    "${SCRATCHPAD}/thread_ids.json"
  ```

- For full comment bodies:

  ```bash
  SCRATCHPAD="<value printed above>"
  jq -r '.[] | "=== [\(.rootCommentId)] @\(.author) on \(.path) ===\n\(.body)\n"' \
    "${SCRATCHPAD}/review_comments.json"
  ```

- The structured files contain all necessary metadata:
  - `review_comments.json`: Root comment ID (integer), path, line, body, author — one entry per thread
  - `thread_ids.json`: Thread node IDs (`PRRT_*`), resolution status, root comment database ID — join with `review_comments.json` on `rootCommentId`
  - `pr_comments.json`: Comment IDs, body, author
  - `check_failures.json`: Check name, conclusion, details URL
  - `pr_review_comments_raw.json`: Full flat array of all review comments including replies (use for full context if needed)

- Note that check-runs and workflow runs are distinct; to fetch logs, obtain the workflow run ID from `check_runs_raw.json` and use `gh api repos/${OWNER}/${REPO}/actions/runs/{run_id}/logs`. If logs are inaccessible via API, run `mask development python all` or `mask development rust all` locally to replicate the errors.
- Group all feedback (check failures, review threads, outdated threads, PR-level comments) using judgement: by file, by theme, by type of change, or whatever makes most sense for the specific pull request; ensure each group maintains the full metadata for all items it contains.
- Analyze dependencies between feedback groups to determine which are independent (can be worked in parallel) and which are interdependent (must be handled sequentially).
- For each piece of feedback, evaluate whether to address it (make code changes) or reject it (explain why the feedback doesn't apply); provide clear reasoning for each decision.

### 3. Enter Plan Mode

- Enter plan mode to organize the work.
- Present a high-level overview showing: (1) total number of feedback groups identified, (2) brief description of each group, (3) which groups are independent vs interdependent, (4) check failures that need fixes.
- Wait for user acknowledgment of the overview before proceeding to detailed planning.

### 4. Present Plans Consecutively

**Default to consecutive presentation to provide clear, reviewable chunks** - even for superficial fixes. Present each logical group separately and wait for approval before proceeding.

- For each feedback group:
  - Present a detailed plan including: grouped feedback items with metadata (comment IDs, commenters, file/line), recommended actions (address vs reject with reasoning), and implementation approach for fixes.
  - For non-trivial changes, pause and ask "Is there a more elegant solution?" before implementing.
  - Wait for user approval of this group's plan before proceeding.

- Only when explicitly grouping truly independent work that benefits from simultaneous execution:
  - Note: "The following groups are independent and will be implemented in parallel using subagents to keep main context clean."
  - Wait for user approval before spawning parallel subagents.

### 5. Implement Fixes

- After plan approval for a group (or parallel groups):
  - For independent groups, spawn parallel subagents to implement solutions simultaneously, keeping main context clean.
  - For interdependent groups, implement sequentially.
  - Implement all fixes for feedback being addressed and for check failures in this group.

### 6. Verify Changes

- After implementing each group's fixes, run verification checks locally:
  - Run `mask development python all` if any Python files were modified.
  - Run `mask development rust all` if any Rust files were modified.
  - **Note**: Local verification confirms fixes work in the development environment. Remote continuous integration will re-run after changes are pushed in the "Commit and Push Changes" step.
- If checks fail, resolve issues and re-run until passing before moving to the next group.
- Do not proceed to the next feedback group until current group's changes pass verification.
- Repeat steps 4-6 for each feedback group until all have been addressed.
- Always run final comprehensive verification using both `mask development python all` and `mask development rust all` before proceeding.

### 7. Commit and Push Changes

- After all fixes are verified, confirm with the user that changes are ready to commit and push.
- Stage all modified files and create a commit:

  ```bash
  git add file1.py file2.md
  git commit -m "$(cat <<'EOF'
  Address pull request #<number> feedback: <brief summary>

  <Detailed explanation of changes made>

  Co-Authored-By: Claude <noreply@anthropic.com>
  EOF
  )"
  ```

- Push to the remote branch:

  ```bash
  git push
  ```

- Confirm the push succeeded before proceeding. Pushing is required before responding to and resolving comments, so that thread resolutions reflect changes that are live on the branch.

### 8. Respond to and Resolve Comments

- For each piece of feedback (both addressed and rejected), draft a response comment explaining what was done or why it was rejected, using the commenter name for personalization.

#### Posting replies to review threads

Use the REST reply endpoint with the root comment's integer ID from `review_comments.json`:

```bash
SCRATCHPAD="<value printed above>"
OWNER="..."; REPO="..."; PR="${ARGUMENTS}"

post_reply() {
    local comment_id="$1"
    local body="$2"
    gh api "repos/${OWNER}/${REPO}/pulls/${PR}/comments/${comment_id}/replies" \
        -f body="${body}" --jq '.id'
}

# Example — call once per thread:
post_reply 1234567 "Fixed in abc1234. Updated the Dockerfile path to translate hyphens to underscores."
post_reply 1234568 "Intentional — kept as-is by design."
```

**Response formatting guidelines**:
- Keep responses concise and single-line when possible
- Avoid newlines, code blocks, or special characters in the body string
- Reference commit hashes or file paths rather than quoting code inline

#### Posting PR-level (issue) comments

```bash
gh api "repos/${OWNER}/${REPO}/issues/${ARGUMENTS}/comments" -f body="<response_text>"
```

Issue comments have no thread state and do not need a resolution step.

#### Resolving review threads

**Use Python `urllib.request`** — `gh api graphql` is broken for parameterized GraphQL mutations because bash history expansion mangles the `!` in type annotations like `ID!`. Python string literals are not subject to this, making it the reliable path for mutations.

```bash
SCRATCHPAD="<value printed above>"

python3 - << 'PYEOF'
import json, urllib.request, subprocess

token = subprocess.check_output(["gh", "auth", "token"]).decode().strip()
mutation = "mutation($threadId:ID!){resolveReviewThread(input:{threadId:$threadId}){thread{id isResolved}}}"

# Populate from thread_ids.json — unresolved threads to resolve
threads = [
    "PRRT_xxx",
    "PRRT_yyy",
]

for thread_id in threads:
    body = json.dumps({"query": mutation, "variables": {"threadId": thread_id}}).encode()
    req = urllib.request.Request(
        "https://api.github.com/graphql",
        data=body,
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req) as resp:
        data = json.loads(resp.read())
    thread = data.get("data", {}).get("resolveReviewThread", {}).get("thread", {})
    print(f"{thread_id}: isResolved={thread.get('isResolved')}")
PYEOF
```

To extract the list of unresolved thread IDs from the scratchpad:

```bash
jq -r '.[] | select(.isResolved == false) | .threadId' "${SCRATCHPAD}/thread_ids.json"
```

Resolve both addressed and rejected threads — the response comment explains the outcome either way.

### 9. Final Summary

- Provide a comprehensive summary showing:
  - Total feedback items processed (with count of addressed vs rejected).
  - Which checks were fixed.
  - Confirmation that all comments have been responded to and resolved.
  - Final verification status (all local checks passing; remote continuous integration is now running against the pushed changes).
- For check failures that were fixed, note that no comments were posted — the fixes will be reflected in re-run checks which are now in progress.
