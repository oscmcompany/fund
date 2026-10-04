---
name: address-feedback
description: Address review feedback and failing checks on the current branch's pull request, then push and resolve the threads
---

# Address Feedback

> Fix what the bots and reviewers found on this branch's pull request, push, then answer and resolve every thread

Run every step to the end without pausing for approval. Invoking this skill is the approval to commit and push,
which overrides the global rule against pushing unasked. Stop and ask only if the branch has no pull request or a
fix would change the pull request's purpose.

Use `gh` and `git` only, never the GitHub MCP tools. Shell variables do not persist between Bash calls, so
re-declare `SCRATCHPAD`, `OWNER`, `REPO` and `PR` at the top of each block that uses them.

## 1. Fetch

```bash
: "${TMPDIR:=/tmp}"
SCRATCHPAD="$(umask 077 && mktemp -d "${TMPDIR%/}/address-feedback.XXXXXX")"
_remote_url=$(git remote get-url origin)
OWNER=$(echo "${_remote_url}" | sed -E 's|.*[:/]([^/]+)/([^/]+)(\.git)?$|\1|')
REPO=$(echo "${_remote_url}" | sed -E 's|.*[:/]([^/]+)/([^/]+)(\.git)?$|\2|' | sed 's/\.git$//')
PR=$(gh pr view --json number --jq .number) || { echo "Error: no pull request for this branch"; exit 1; }

gh api "repos/${OWNER}/${REPO}/pulls/${PR}" > "${SCRATCHPAD}/pr_meta_raw.json"
gh api --paginate "repos/${OWNER}/${REPO}/issues/${PR}/comments" | jq -s 'add' > "${SCRATCHPAD}/pr_comments_raw.json"
gh api --paginate "repos/${OWNER}/${REPO}/pulls/${PR}/reviews" | jq -s 'add' > "${SCRATCHPAD}/pr_reviews_raw.json"
HEAD_SHA=$(jq -r '.head.sha' "${SCRATCHPAD}/pr_meta_raw.json")
gh api --paginate "repos/${OWNER}/${REPO}/commits/${HEAD_SHA}/check-runs" | jq -s '{check_runs: map(.check_runs) | add}' > "${SCRATCHPAD}/check_runs_raw.json"

# The query lives in a file so `$variables` and `!` never pass through the shell.
cat > "${SCRATCHPAD}/threads.graphql" << 'EOF'
query($owner: String!, $name: String!, $number: Int!, $endCursor: String) {
  repository(owner: $owner, name: $name) {
    pullRequest(number: $number) {
      reviewThreads(first: 50, after: $endCursor) {
        pageInfo { hasNextPage endCursor }
        totalCount
        nodes {
          id isResolved isOutdated path line originalLine
          comments(first: 50) { nodes { databaseId author { login } body } }
        }
      }
    }
  }
}
EOF
gh api graphql --paginate --slurp -F query=@"${SCRATCHPAD}/threads.graphql" \
  -F owner="${OWNER}" -F name="${REPO}" -F number="${PR}" > "${SCRATCHPAD}/threads_raw.json"

echo "SCRATCHPAD=${SCRATCHPAD} OWNER=${OWNER} REPO=${REPO} PR=${PR}"
```

## 2. Extract

The raw files are too large to read; extract once and read only the extracts.

```bash
SCRATCHPAD="..."

jq '[.[].data.repository.pullRequest.reviewThreads.nodes[] | select(.isResolved | not) | {
  threadId: .id, outdated: .isOutdated, path: .path, line: (.line // .originalLine),
  rootCommentId: .comments.nodes[0].databaseId,
  comments: [.comments.nodes[] | {author: .author.login, body: .body}]
}]' "${SCRATCHPAD}/threads_raw.json" > "${SCRATCHPAD}/threads.json"

jq '[.[] | {id: .id, author: .user.login, body: .body}]' \
  "${SCRATCHPAD}/pr_comments_raw.json" > "${SCRATCHPAD}/pr_comments.json"

# Bots put nitpicks and outside-diff findings in the review body, not in a thread.
jq '[.[] | select((.body // "") != "") | {author: .user.login, state: .state, body: .body}]' \
  "${SCRATCHPAD}/pr_reviews_raw.json" > "${SCRATCHPAD}/reviews.json"

jq '[.check_runs[] | select(.conclusion == "failure" or .conclusion == "timed_out")
  | {name: .name, conclusion: .conclusion, detailsUrl: .details_url}] | unique_by(.name)' \
  "${SCRATCHPAD}/check_runs_raw.json" > "${SCRATCHPAD}/check_failures.json"

total=$(jq '[.[].data.repository.pullRequest.reviewThreads.nodes[]] | length' "${SCRATCHPAD}/threads_raw.json")
expected=$(jq '.[0].data.repository.pullRequest.reviewThreads.totalCount' "${SCRATCHPAD}/threads_raw.json")
[ "${total}" = "${expected}" ] || { echo "Error: fetched ${total} of ${expected} threads"; exit 1; }
echo "threads ${total} total, $(jq length "${SCRATCHPAD}/threads.json") unresolved"
echo "pr comments $(jq length "${SCRATCHPAD}/pr_comments.json"), reviews $(jq length "${SCRATCHPAD}/reviews.json")"
echo "check failures $(jq length "${SCRATCHPAD}/check_failures.json")"
```

Then read `threads.json`, `reviews.json`, `pr_comments.json` and `check_failures.json` in full, never a truncated
summary, and list every unresolved thread ID before deciding anything. For a failing check, fetch its logs with
`gh api repos/${OWNER}/${REPO}/actions/runs/{run_id}/logs`, or reproduce it locally with the matching
`devenv tasks run checks:*` task.

## 3. Decide

- Merge duplicates: several bots often flag the same line, and one fix answers all of them
- Group the rest by whatever makes the work cleanest (file, theme or kind of change)
- For each item, decide to fix it or decline it with a one-line reason. Decline anything that contradicts
  `CLAUDE.md`, a docstring false positive where the code is right, and scope that belongs in a later pull request
- Skip bot walkthroughs and summaries that ask for nothing

## 4. Fix

- Implement the fixes, writing a failing test first for any bug, with independent groups in parallel subagents
- Run targeted `cargo test` while iterating
- Then spawn the `reviewer` subagent over the uncommitted diff, and let it fix what it finds
- Run `devenv tasks run checks:all` once and fix anything it reports

## 5. Commit and push

```bash
git add <files>
git commit -m "$(cat << 'EOF'
Address pull request #<number> feedback: <brief summary>

<what changed and why, by theme>

<attribution lines the session specifies>
EOF
)"
git push
```

Confirm the push succeeded before answering any thread, so every resolution points at code on the branch.

## 6. Reply and resolve

Reply to every thread you fixed or declined, one line each, naming the commit or the reason:

```bash
OWNER="..."; REPO="..."; PR="..."
gh api "repos/${OWNER}/${REPO}/pulls/${PR}/comments/<rootCommentId>/replies" -f body="Fixed in <sha>: <what>" --jq .id
```

Answer review bodies and pull request comments that asked for something in one pull request comment:

```bash
gh api "repos/${OWNER}/${REPO}/issues/${PR}/comments" -f body="<response>" --jq .id
```

Resolve only the threads you replied to:

```bash
SCRATCHPAD="..."
cat > "${SCRATCHPAD}/resolve.graphql" << 'EOF'
mutation($threadId: ID!) { resolveReviewThread(input: {threadId: $threadId}) { thread { id isResolved } } }
EOF
for thread_id in <threadId> <threadId>; do
  gh api graphql -F query=@"${SCRATCHPAD}/resolve.graphql" -F threadId="${thread_id}" \
    --jq '.data.resolveReviewThread.thread | "\(.id) \(.isResolved)"'
done
```

Keep replies to one line with no code blocks, referencing a commit or path rather than quoting code.

## 7. Report

In under ten lines: threads fixed, declined and resolved as counts against the unresolved total, what the fresh
review changed, the `checks:all` result, the pushed commit, and anything declined that needs the user's call.
