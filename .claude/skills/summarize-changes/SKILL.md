---
name: summarize-changes
description: Point the user at the few ranges of the current branch's changes that deserve a human read, with links
---

# Summarize Changes

> Pick the production lines on this branch worth a human read, and say why each one is worth it

The bots and the checks already cover line-level correctness, test bodies and rule compliance. This skill covers
what they cannot judge: whether the design, names and assumptions are the ones the user wants. Print the result
to the terminal; post nothing.

## 1. Gather

```bash
gh pr view --json number,url,baseRefName,headRefOid 2>/dev/null || echo "no pull request"
git fetch -q origin
python3 .claude/skills/summarize-changes/production_lines.py origin/<baseRefName, or master without a pull request>
git diff --merge-base origin/<base> HEAD --stat
```

The script prints each file's added production lines (Rust outside `src_old/`, `tests/` and `#[cfg(test)]`)
and the budget: a quarter of the production lines added, capped at 250. Highlight at most that many lines.
Highlight nothing if nothing qualifies, and say so.

Links point at `https://github.com/<owner>/<repo>/blob/<sha>/<path>#L<start>-L<end>` with the pull request's
`headRefOid`. If local `HEAD` differs from it, the newest commits are not pushed: say so, and give `path:start-end`
for ranges that exist only locally.

## 2. Choose

Read the diff and rank candidate ranges by what the user reviews for, roughly in this order:

- New or renamed public names: our vocabulary rather than the vendor's, and agreement with sibling names
- Types at boundaries: struct fields and their visibility, validated constructors, adjacent same-typed arguments
  a caller could swap, `Option` and refusal types that carry their cause
- Module, file and storage layout: partitioning, keys, what a path or bucket name commits to
- Units, scales and constants: money and share scales, thresholds, windows, the divisor behind a ratio
- Composition: whether a combine has an identity and is associative, whether a round trip returns the original
- Assumptions about time, sessions, providers and calendars
- Risk, measurement and model choices: thresholds, baselines, populations, anything a study's answer rests on
- Infrastructure scope: IAM grants, schedules, anything that runs without a person watching
- A docstring that claims two things agree, or anything a reader would act on without checking

Skip tests, fixtures, `Cargo.lock`, mechanical renames and moves, and lines that only answer a bot comment.
Prefer a whole definition over a fragment of one, and fewer ranges read closely over many skimmed.

## 3. Print

```text
Highlighted <n> of <total> production lines (budget <budget>) across <k> ranges

1. <short title> (one-way door | two-way door)
   <link>
   <one or two sentences: what changed and the specific question the user should answer>

2. ...

Skipped: <one line naming what was left out, such as tests, lockfile and bot fixes>
```

A one-way door is a change that is costly to undo once it ships: a stored schema, a bucket key, a public name
other code will spread, an order that runs without a person. Everything else is a two-way door. List one-way
doors first.
