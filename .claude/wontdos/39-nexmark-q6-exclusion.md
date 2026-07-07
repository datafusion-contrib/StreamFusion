# Nexmark q6: why it stays outside the accelerating set

**Status:** Documented exclusion (not a bug, not planned). Recorded 2026-07-01; re-verified on Flink
2.2.1 (StreamFusion's target — newer than the Flink 2.0-preview1 the nexmark repo targets).

Every Nexmark query that Flink 2.2.1 can run, StreamFusion now accelerates fully and by default
(q0–q5, q7–q23, including q13's lookup join). q6 is the one query that stays out, and
it stays out because Flink itself cannot execute it — not because of a missing native operator.

## q6 — `AVG(price) OVER (ROWS BETWEEN 10 PRECEDING AND CURRENT ROW)` over winning bids

q6 takes the winning bid per auction (a retracting Top-1: `ROW_NUMBER() … = 1` over an interval join,
q4's core) and averages the last 10 per seller with a row-count-bounded `OVER` window.

Re-verified on Flink 2.2.1 (host, no StreamFusion) — q6 does **not** run, but the nexmark README's
stated reason is now stale:

- **FLINK-19059 ("OVER window consuming retractions"), the issue the nexmark README links q6 to, is
  fixed in Flink 2.1.0.** That barrier is gone on 2.2.1.
- q6.sql **as written is invalid SQL**: its inner block is `SELECT *, ROW_NUMBER() … AS rownum FROM …
  WHERE rownum <= 1` — `WHERE` can't see the same-level projection alias, so Flink rejects it with
  `Column 'rownum' not found` (host and native alike). The canonical Top-N idiom must wrap the
  `ROW_NUMBER` in a subquery and filter `rownum` in the outer query.
- Wrapped correctly, it clears both of the above and then hits a **different, still-present** Flink
  limit: `Non-time attribute sort is not supported for bounded OVER window`. The winning-bid Top-N
  consumes `dateTime`'s rowtime-attribute property, and a `ROWS BETWEEN 10 PRECEDING` frame requires
  ordering by a time attribute — so Flink refuses to plan it.

**Why StreamFusion can't accelerate it.** StreamFusion overrides Flink's *physical plan* and verifies
each operator byte-for-byte against the host. Flink produces no plan and no output for q6, so there is
nothing to override and nothing to check parity against. (Making q6 run would mean emitting a
bounded-`OVER` operator over a non-time-attribute sort that Flink itself declines to build — new
semantics with no host reference, unverifiable by the parity harness.)
