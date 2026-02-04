# Update Notes

> Consolidate lessons into CLAUDE.md

## Instructions

Review and consolidate lesson patterns from `.claude/tasks/lessons.md` into the `CLAUDE.md` Notes section.

Follow these steps:

- Check if `.claude/tasks/lessons.md` exists; if not, create it with a header and explanation that it tracks mistake patterns with timestamps for consolidation.
- Read the entire contents of `.claude/tasks/lessons.md` to identify all rules and their timestamps.
- Calculate the date that was 7 days ago from the current date when this command is run.
- Identify rules with timestamps older than that 7-day threshold.
- For each rule older than 7 days, draft a consolidated bullet point suitable for the `CLAUDE.md` "## Notes" section following the existing style and conventions in that file.
- Present the consolidation plan to the user showing: (1) rules to be consolidated with their timestamps, (2) proposed consolidated text for CLAUDE.md, (3) confirmation that these will be removed from lessons.md.
- Wait for user approval before proceeding.
- After approval, append the consolidated bullet points to the `CLAUDE.md` "## Notes" section maintaining logical grouping with existing notes.
- Remove the consolidated rules from `.claude/tasks/lessons.md`, preserving any rules newer than 7 days.
- Provide a summary of how many rules were consolidated and how many remain in lessons.md for future consolidation.
