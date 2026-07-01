# Nexmark q6 and q13: why they stay outside the accelerating set

**Status:** Documented exclusion (not a bug, not planned). Recorded 2026-07-01; reasons re-verified
against Flink 2.2.1 (StreamFusion's target — newer than the Flink 2.0-preview1 the nexmark repo targets).

Of the 24 Nexmark queries, every one that Flink 2.2.1 can run, StreamFusion now accelerates fully and by
default (q0–q5, q7–q23; see `NexmarkExplainTest`). Two remain outside. Neither is a StreamFusion
fallback we can close by writing a native operator — the reasons live in Flink and in the query.

## q6 — `AVG(price) OVER (ROWS BETWEEN 10 PRECEDING AND CURRENT ROW)` over winning bids

q6 takes the winning bid per auction (a retracting Top-1: `ROW_NUMBER() … = 1` over an interval join,
q4's core) and averages the last 10 per seller with a row-count-bounded `OVER` window.

Re-verified on Flink 2.2.1 (host, no StreamFusion) — q6 does **not** run, but the nexmark README's stated
reason is now stale:

- **FLINK-19059 ("OVER window consuming retractions"), the issue the nexmark README links q6 to, is
  fixed in Flink 2.1.0.** That barrier is gone on 2.2.1.
- q6.sql **as written is invalid SQL**: its inner block is `SELECT *, ROW_NUMBER() … AS rownum FROM … 
  WHERE rownum <= 1` — `WHERE` can't see the same-level projection alias, so Flink rejects it with
  `Column 'rownum' not found` (host and native alike). The canonical Top-N idiom must wrap the
  `ROW_NUMBER` in a subquery and filter `rownum` in the outer query.
- Wrapped correctly, it clears both of the above and then hits a **different, still-present** Flink limit:
  `Non-time attribute sort is not supported for bounded OVER window`. The winning-bid Top-N consumes
  `dateTime`'s rowtime-attribute property, and a `ROWS BETWEEN 10 PRECEDING` frame requires ordering by a
  time attribute — so Flink refuses to plan it.

**Why StreamFusion can't accelerate it.** StreamFusion overrides Flink's *physical plan* and verifies each
operator byte-for-byte against the host. Flink produces no plan and no output for q6, so there is nothing
to override and nothing to check parity against. (Making q6 run would mean emitting a bounded-`OVER`
operator over a non-time-attribute sort that Flink itself declines to build — new semantics with no host
reference, unverifiable by the parity harness.)

## q13 — bounded side-input lookup join (`FOR SYSTEM_TIME AS OF B.p_time`)

q13 enriches the bid stream by joining it, **at processing time**, against a bounded side-input table
(`JOIN side_input FOR SYSTEM_TIME AS OF B.p_time`, `p_time = PROCTIME()`). Upstream marks it ✅ — it does
run in Flink, planned as a `StreamPhysicalLookupJoin` (Flink's async `LookupJoinRunner`).

**Why StreamFusion doesn't accelerate it.** A lookup join is an async external-I/O operator: per probe
row it looks the key up in the connector-backed table. That is source/sink-class I/O, not columnar
compute over in-flight Arrow batches, and its cost is the lookup, not CPU — so there is little to
accelerate even in principle. StreamFusion's temporal-join support (`TemporalJoinMatcher`) deliberately
covers only **event-time versioned-table** joins (`StreamPhysicalTemporalJoin`, probe `FOR SYSTEM_TIME AS
OF probe.rowtime`), which are keyed columnar state joins; it leaves processing-time lookup joins on Flink
by design. (In `NexmarkExplainTest` q13 errors earlier still — the stand-in `side_input` is a datagen
table with no primary key, which a temporal join requires; the real nexmark uses a filesystem lookup
connector. Fixing the DDL makes it plan as the lookup join above, which the native planner then leaves on
Flink.)

**Reconsider if:** a native in-memory broadcast/hash-join against a *bounded, static* side table is ever
built (load the small table once, probe columnar) — a genuine native operator, worth it only if lookup
joins land on a hot path. For Nexmark they do not.
