# Systematic parity test harness

**Status:** open
**Source:** research findings §9 (open question #6)

## Problem
Each test asserts native results against hand-computed expectations. There is
no harness that runs the *same* SQL on the host engine alone and on the native
path and asserts the two agree. As coverage grows (types, aggregates, windows)
we want parity checked mechanically, the way the batch accelerators do
(`checkSparkAnswerAndOperator` / `findFirstNonCometOperator`).

## Goal
A reusable harness that, for a given SQL + bounded watermarked source:
1. runs it host-only and collects the result,
2. runs it with native substitution installed and collects the result,
3. asserts the two are equal, and
4. asserts the native run actually substituted (so we are not silently
   falling back and calling it parity).

## Acceptance criteria
- Existing window/projection cases expressed as parity cases.
- Adding a new supported pattern is a one-line case.
- A case that should fall back is flagged (substitution count 0) rather than
  silently passing.

## Pointers
- Comet `CometTestBase` / `CometPlanChecker` are the model.
- `PhysicalPlanScan.substitutions()` already exposes the substitution count.
