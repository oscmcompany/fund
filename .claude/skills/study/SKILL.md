---
name: study
description: The research method for a study in the oscmcompany/fund repository — controls, baselines, nulls, confirm reads, standard errors, kills and screens. Load before designing, writing, running or reading a study (a `studies/src/bin/` binary, a `studies/jobs/` script, or anything journaled through a `Study`); not for ordinary coding work.
---

# Study

> How a study is built and read, so that a reading survives its own null

These rules sit beside CLAUDE.md, which still holds where studies live, how they journal, and how a data source is
promoted into the archive.

- Prove an instrument can see before trusting it: before reading a measurement that reports nothing, confirm it
  reports something when something is there; a model change shows it can see by recovering what
  `common::laboratory::synthetic` planted at a stated strength, against the null arm that planted nothing, before it
  is read on real sessions
- Before believing a result, try to produce it from nothing — permute the labels, shuffle the target, feed the
  pipeline noise, or run it on an input that cannot contain the effect — and confirm the effect disappears;
  whatever it still reports is the machinery's own bias, and every real reading has to clear it
- A control must differ from the treatment in exactly one respect, and it must be able to vary — a control
  that cannot fail is not a control
- State the trivial baseline before reading any metric, journaling it with its metric and bar through
  `Study::declare`, and quote skill against it rather than as a raw score; measure a replaceable component through
  `laboratory::compare` against its trivial arm (the null strategy or the component it replaces) over the same
  replayed sessions, so the paired difference lands in one experiment
- A baseline's free choices are fixed by an external specification or swept across a declared range, never
  chosen by us, and every arm carries its own parameters in its name
- Prefer the instrument that can lower your own number — a screen that costs a fifth of the panel and
  changes no answer is a result, and an instrument that can only flatter is not a measurement
- Start a study as simply as you can provided the main effects are in, get the insights, then evolve it to the
  fully detailed run — a full-scale simulation disguises the system, and fewer runs read carefully beat many runs
  summarized
- Unusually low variance across seeds is a warning rather than a quality signal — it means the metric is
  pinned by the shape of the data rather than by anything the model learned
- Magnitude before significance: ask whether the effect could pay for itself at measured cost before asking
  whether it is real
- Quote a candidate's net edge with the cost and fill models' error rather than the signal's standard error alone,
  since a net figure is only as reliable as its least reliable input and an edge inside the cost error must not
  pass; spend measurement effort on the inputs nothing corrects — the cost and fill models are open loop, while a
  parameter refit every session is corrected by its own feedback
- Judge a component by the output of the system it feeds rather than by its own metric — a forecast by net P&L
  through the decision rule and costs, not its information coefficient; an archive by what the trader and studies
  could read in time, not the objects written
- Assume an automated search or fit has exploited its metric's blind spots, and before reading a winner's score look
  for how it could have — names with an undefined or zero cost, sessions a filter dropped; for the same reason a
  performance target is a gate a result passes, never a quantity to optimize toward
- Read the worst session and the loss tail beside Sharpe for any strategy that supplies liquidity or sells
  insurance, since its returns are smooth until the session they are not
- State a new direction's admission criteria before opening it — a returns bar, a reachable size, an underserved
  gap, and a capability we actually hold — and record which of them it fails, because failing three of four is a
  legitimate answer; admit a dataset on the same footing: does it raise the average information, is it
  re-derivable, and does it carry a dimension nothing else does
- Fix every free choice before looking at the result and record each one with the number it produced; a confirm
  set, designed per study, answers a question only if reading it could not have shaped that question, so data that
  suggested a hypothesis may train or validate it, never confirm it
- Split a study's data by session, never by row, since names on one session share the market move, and confirm on
  a contiguous block later than everything fit or chosen on, since interleaved weeks share a regime; split the
  sample and test the difference between the halves, never whether both halves point the same way
- Record a confirm read "as of": declare the question with its as-of session through `Study::declare` before
  reading, so the journal stamps what was asked with its commit and time, and re-check the declared metric on the
  sessions after that one rather than counting candidates against a raised bar; a verdict answers the latest
  declaration before it, so re-declaring moves the bar; an automated search never reads a confirm set
- Beside any confirm reading, report how many declared questions were re-checked over the same sessions and the
  standard errors that count requires through `common::laboratory::haircut`, since at a 5% bar at least one of ten
  independent questions read on one block passes by luck about 40% of the time; the count informs and gates nothing
- Scope a kill precisely — say exactly what was refuted and under what conditions, because over-claiming a
  refutation closes doors that are still open and under-claiming reopens ones that are shut; record what would reopen
  it (a cost level, a data source, a latency), since a change in what we can do undoes a refutation that rested on it;
  journal it through `Study::verdict`, whose refuted outcome carries both
- Correcting an instance does not generalise the lesson; run the check deliberately against each new
  measurement rather than remembering it as a past mistake
- Standard errors assume independent observations, which financial data never is — group readings into blocks that
  move together, by time (minute up to session), by name, by name within a session, and by session and name at once,
  through `common::laboratory::blocks::coarsen`; quote the largest error among groupings with at least 20 blocks,
  week shown only as a check, since fewer leaves the error itself noisy
- A measurement reports its population: beside any estimate or difference print the population, the undefined share
  and the boundary values, and the effective sample size rather than the row count — `1254 / 1254 / 0 / 0` is a
  proof and a bare `0` is not, and an estimator silently defined on 60% of its inputs reads exactly like a complete
  measurement; journal the population, the undefined count and the null's reading with the experiment through
  `Outputs::checks`
- Screen on the quantity being paid for rather than a correlate of it, and check a screen's units before
  checking its threshold; a cost charged per share is compared with the spread in cents per share and converted
  to basis points only to report, since the same fee is five times larger in basis points at $5 than at $25
- Levels drift and ranks do not — a threshold fixed in absolute units is a moving quantile, so scale
  windows and cutoffs to the distribution's own width
- Screen first, then average: a subset's cost read off the whole distribution's deciles is the wrong
  number, and so is costing a strategy at an average over buckets it does not trade
- Never quantile-bin an unordered category — nominal is not ordinal, and binning one merges unrelated
  groups on the basis of the order they happened to arrive in
- Compare mutual information as a share of the target's entropy rather than in raw bits, and subtract a
  permutation-null baseline before reading the table
- The universe is part of the answer, so journal it beside the result — the set of instruments collected
  and the set measured over are different questions with different answers
- Before reading a vendor field as a signal, record what the vendor produces it for (billing, display, record keeping),
  since a field gathered for another purpose measures that purpose
