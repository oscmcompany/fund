# Create or Update Issue

> Create or update GitHub issue specifications

## Instructions

Create a new GitHub issue or update an existing one with detailed specifications for implementation.

Follow these steps:

- Determine repository owner and name from git remote: extract from `git remote get-url origin` (format: `https://github.com/owner/repo.git` or `git@github.com:owner/repo.git`) and export variables:
  - `OWNER=<extracted_owner>`
  - `REPO=<extracted_repo>`
- Accept optional issue ID from ${ARGUMENTS}; if no argument provided, treat this as creating a new issue.
- If issue ID is provided, fetch the existing issue using `gh api repos/${OWNER}/${REPO}/issues/"${ARGUMENTS}"` and also fetch related items using `gh api repos/${OWNER}/${REPO}/issues/"${ARGUMENTS}"/timeline` to understand context.
- Read `.github/ISSUE_TEMPLATE/ISSUE_TEMPLATE.md` to understand the template structure and commented instructions, regardless of whether creating new or updating existing issue.
- If updating existing issue and sections have been removed, reference the template file for the commented instructions on what should go in each section.
- Use TaskCreate to create tasks for each specification section that needs to be filled out: Overview, Context (bug/feature/task description), and Changes (solutions/recommendations as bullets, action items as checkboxes).
- Work interactively with the user to collect information for each section. Use TaskUpdate to set tasks to in_progress while gathering info, then completed once the user approves that section.
- As sections are filled out, pause and ask "Is there a clearer, more precise way to specify this?" especially for complex requirements or decisions.
- Once all sections are complete, present the full issue content for user review and approval.
- After approval, either create a new issue using `gh api repos/${OWNER}/${REPO}/issues -f title="..." -f body="..."` or update existing using `gh api repos/${OWNER}/${REPO}/issues/"${ARGUMENTS}" -X PATCH -f body="..."`.
- When updating existing issues, preserve any existing labels by fetching them first and including them in the update.
- Read `.github/ISSUE_TEMPLATE/ISSUE_TEMPLATE.md` to extract the project from frontmatter `projects:` field (format: `["org/number"]`), parse the organization name and project number, then add the issue using GraphQL API: first get the project node ID with `query { organization(login: "<org>") { projectV2(number: <number>) { id } } }` and the issue node ID, then use the addProjectV2ItemById mutation.
- Provide the final issue URL to the user and summarize what was created/updated.
