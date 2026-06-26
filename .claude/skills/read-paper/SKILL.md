---
name: read-paper
description: >
  Read and analyze a research paper using the three-pass method tailored for
  quantitative finance, machine learning, and technology papers. Walks through
  three named passes — Orientation, Comprehension, and Implementation — pausing
  between each. Use when the user provides a paper URL and wants structured
  analysis. Trigger on "read this paper", "analyze this paper", "/read-paper",
  or when a URL to a research paper is provided.
---

# Read a Research Paper

Analyze a research paper using the three-pass method. The user invokes this
skill with a URL: `/read-paper <URL>`.

Fetch the paper from the URL. If the page is paywalled or the fetch fails,
tell the user and ask them to paste the paper text or provide a local file
path.

The user is a quantitative finance practitioner building a hedge fund. Their
math background is calculus-level; they want to build mathematical intuition
while staying focused on practical implementation. Papers will span quant
finance, machine learning, and technology.

Work through all three passes sequentially, pausing for confirmation between
each. At the start and end of each pass, print a clear header so the user
always knows where they are.

---

## Pass 1: Orientation

**Header on start:** `--- BEGIN PASS 1: ORIENTATION ---`

**Goal:** Determine whether this paper is worth the user's time.

Produce the following structured output:

### Paper type
One of: empirical study, theoretical/mathematical, survey/review, system
description, replication study, vendor/practitioner whitepaper.

### Core claim
One sentence in plain English. No jargon. What is this paper actually saying?

### Relevance to the fund
Which area(s) does this touch? Choose from: statistical arbitrage, volatility
arbitrage, regime detection, signal generation, portfolio construction,
execution/microstructure, risk management, machine learning/modeling,
infrastructure/systems. Briefly explain why.

### Key assumptions
List the assumptions the authors make that the entire paper depends on. Flag
any that seem unrealistic in live markets.

### Red flags

**Hard flags** — these undermine the validity of the results. Flag any that
are present:
- Look-ahead bias in feature construction or signal design
- Survivorship bias in the dataset
- Parameters fit on the full dataset with no hold-out or walk-forward test
- Multiple hypothesis testing without correction (many signals tested, best reported)

**Soft flags** — worth noting but not disqualifying. Flag any that apply and
explain briefly:
- Transaction costs ignored (only flag if the authors do not acknowledge this)
- Scope limited to a specific asset class or market (note the limitation)
- Non-peer-reviewed venue (note it, do not treat it as disqualifying)
- Backtest period is short or covers only one market regime (note with context)
- No benchmark comparison or benchmark is inappropriate

### Verdict
One of: **Continue** (worth Pass 2) or **Stop** (explain why).

**Header on end:** `--- END PASS 1: ORIENTATION ---`

Then ask: "Continue to Pass 2: Comprehension?"

---

## Pass 2: Comprehension

**Header on start:** `--- BEGIN PASS 2: COMPREHENSION ---`

**Goal:** Understand what the paper actually says — methodology, evidence, and
results.

Produce the following structured output:

### Methodology walkthrough
Step-by-step plain English description of what the authors did. No math yet.
Write it so someone familiar with trading but not this specific technique can
follow it.

### Equations
For each significant equation in the paper:
1. **Plain English:** What does this compute? What question does it answer?
2. **Symbol definitions:** Define every symbol used.
3. **Symbolic breakdown:** Show the equation and walk through it term by term.
4. **Intuition:** Why does it take this form? What would change if a term were
   removed or modified? Build the mathematical muscle here.

Label each section clearly so the user can read just the plain English
descriptions if they want, or engage with the symbolic breakdown when they
want to go deeper.

### Figures and tables critique
For each significant figure or table:
- What is it showing?
- Are axes labeled and scales appropriate?
- Are results shown with error bars or confidence intervals where needed?
- Is this in-sample, out-of-sample, or unspecified?
- Are transaction costs and slippage reflected?
- Is the benchmark appropriate?

### Glossary
For every piece of jargon or domain-specific term encountered, produce an
entry:

**Term:** `<term>`
**Definition:** Plain English definition.
**Context:** How it was used in this paper and why it matters for a quant
fund.

If a term appeared in a previous paper during this session, note that and add
any new context this paper provides.

### References worth following
List any cited papers or authors that seem important to the ideas in this
paper and worth reading next. One sentence each on why.

**Header on end:** `--- END PASS 2: COMPREHENSION ---`

Then ask: "Continue to Pass 3: Implementation?"

---

## Pass 3: Implementation

**Header on start:** `--- BEGIN PASS 3: IMPLEMENTATION ---`

**Goal:** Determine whether and how the fund could use this.

Open with a structured setup block, then transition to dialogue.

### Setup block

**Data requirements**
What data would be needed to implement this? Be specific: asset classes,
resolution (tick/minute/daily), history length, derived fields. Flag any data
we likely do not have (the fund uses Alpaca for trading and market data,
Massive for options data, and S3 Parquet for historical bars).

**Compute requirements**
What computational resources does this need? Real-time vs batch, latency
sensitivity, model training costs, memory footprint.

**Math prerequisites**
What mathematical concepts would need to be understood to implement this
confidently? List them plainly — e.g., "eigenvalue decomposition",
"stochastic calculus", "maximum likelihood estimation". Do not assume the
user knows these; flag which ones are essential vs nice-to-have.

**Implementation complexity estimate**
Rough assessment: days, weeks, or months of engineering effort and why.

### Dialogue

After the setup block, ask targeted questions to drive the implementation
discussion. Ground questions in the fund's actual state where possible. For
example:

- If the paper requires tick data: "We have Alpaca OPRA quotes — does the
  resolution and latency match what this strategy needs?"
- If the paper assumes daily rebalancing: "Our rebalancer triggers on
  intraday events — would this strategy need a separate scheduling
  mechanism?"
- If the paper assumes a large universe: "The paper uses 500 stocks — our
  current universe is smaller. How sensitive are the results to universe
  size?"

Continue the dialogue until the user is satisfied or explicitly ends the
session. If at any point the user says they want to save something, write it
out in a format they can copy or save to a file.

**Header on end:** `--- END PASS 3: IMPLEMENTATION ---`
