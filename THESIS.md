# thesis

> This is how we build the fund

- At every moment and for every instrument, the system decides whether to act, and how, from what it expects to
  earn net of what acting costs and what risk it adds
- Everything else is an output: how long we hold, how many names we carry and how often we trade are not settings,
  they fall out of the decision
- Rigor in the laboratory buys risk in the book
- Any idea may be tried; only the reading is checked
- Organic means outputs, not settings
- The machinery is public and the state is private
- Minimal overhead; just code and money
- An event arrives, the forecast updates, cost and risk are priced, a decision is made, a fill comes back
- The fill tightens the cost model that gates the next decision
- The system corrects itself from its own fills, and it widens as it earns the right to
- Rebuild it from its records and it lands in the same state; feed it noise and it reports nothing
- Risk-adjusted return over the index on the same sessions, net of every cost
- The target is the index's Sharpe plus two, read on the sessions we traded and nowhere else
- An edge inside the measurement's error does not count, however large the sample
- Net per session is the goal, and every trade must beat cash
- We think, we talk, we tinker
- A hypothesis becomes a comparison against its baseline in a day, and the comparison carries its own null
- The question is declared before the answer is read
- Work runs in parallel, on a laptop or a box, and the box runs what the laptop ran
- Claude sits in every session, in the same headspace as us
- Not a process-heavy shop
- The targets a result is gated on, its margin over the index's Sharpe and its mean daily net return, are
  `SHARPE_MARGIN_OVER_INDEX` and `DAILY_RETURN_TARGET_BASIS_POINTS` in `src/common/laboratory/sharpe.rs`
- A reading's error is the largest among its well-supported block groupings, and the bar it is read against rises
  with every test in its family: `QUOTED_BLOCKS` in `src/common/laboratory/blocks.rs` and
  `FAMILY_WISE_ERROR_RATE` in `src/common/laboratory/haircut.rs`
- The rules for writing code live in `CLAUDE.md`, and the rules for running a study in the `study` skill at
  `.claude/skills/study/SKILL.md`
