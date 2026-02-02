# Create or Update Issue

> Create or update GitHub issue specifications

## Instructions

Create a new GitHub issue or update an existing one with detailed specifications for implementation.

Follow these steps:

- Accept optional issue ID from $ARGUMENTS; if no argument provided, treat this as creating a new issue.
- Clear any existing content from `.claude/tasks/todos.md` to start fresh for this issue specification work.
- If issue ID is provided, fetch the existing issue using `gh api repos/:owner/:repo/issues/$ARGUMENTS` and also fetch related items using `gh api repos/:owner/:repo/issues/$ARGUMENTS/timeline` to understand context.
- Read `.github/ISSUE_TEMPLATE/ISSUE_TEMPLATE.md` to understand the template structure and commented instructions, regardless of whether creating new or updating existing issue.
- If updating existing issue and sections have been removed, reference the template file for the commented instructions on what should go in each section.
- Initialize `.claude/tasks/todos.md` with a checklist of specification sections that need to be filled out: Overview, Context (bug/feature/task description), and Changes (solutions/recommendations as bullets, action items as checkboxes).
- Work interactively with the user to collect information for each section, marking todo items as in_progress while gathering info, then completed once the user approves that section.
- As sections are filled out, apply the "Demand Elegance" principle: pause and ask "Is there a clearer, more precise way to specify this?" especially for complex requirements or decisions.
- Once all sections are complete, present the full issue content for user review and approval.
- After approval, determine repository owner and name from git remote, then either create a new issue using `gh api repos/:owner/:repo/issues -f title="..." -f body="..."` or update existing using `gh api repos/:owner/:repo/issues/$ARGUMENTS -X PATCH -f body="..."`.
- When updating existing issues, preserve any existing labels by fetching them first and including them in the update.
- Add the issue to the project specified in template frontmatter (oscmcompany/1) using GraphQL API: first get the project and issue node IDs, then use the addProjectV2ItemById mutation.
- Provide the final issue URL to the user and summarize what was created/updated.
