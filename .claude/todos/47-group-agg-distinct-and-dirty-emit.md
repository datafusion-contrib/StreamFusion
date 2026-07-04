# Group aggregate: DISTINCT without ScalarValue churn; emit only dirty groups

**Status:** TODO (2026-07-04 profiling round,
`.claude/research/nexmark-operator-profiles-2026-07.md`, levers 5+8). Related but distinct from
ticket 41 (mini-batch planner coverage): this is the non-mini-batch native operator's own state
and emit cost.

**Measured:** the multi-`DISTINCT` day/channel aggregates own the largest native islands —
q16 30.8 CPU samples/iteration (its whole query is 1.14x), q17 19.4, q15 18.6 — and the hot leaves
are `drop_in_place<ScalarValue>` + `ScalarValue::clone` + `ScalarValue::hash` +
`ScalarValue::try_from_array` + allocator traffic (mimalloc TLS `_tlv_get_addr` + `mi_free` +
malloc) ≈ half the operator. The DISTINCT sets store boxed `ScalarValue`s per (group, value).

References give two composable fixes:

1. **DISTINCT dedup as visibility masking (RisingWave `aggregate/distinct.rs:67-197`).** Dedup the
   batch *before* the accumulators: per (group key, distinct column value) — one memcmp-encoded
   compact key — keep a `Box<[i64]>` of reference counts, one slot per distinct agg call on that
   column (COUNT(DISTINCT bidder) FILTER(...) x4 share one entry). New value → visible to the
   accumulator; repeat → hide the row by clearing its slot in a per-call visibility bitmask.
   Accumulators then run plain vectorized updates over the masked batch; no per-row ScalarValue is
   ever materialized. Retract decrements the count, erases at zero.
2. **Dirty-group emit (Proton `TrackingUpdatesData.h`; Arroyo no-op suppression).** Flush currently
   walks state; Proton prefixes each group state with an update counter + dirty flag, wipes
   retract snapshots in a bulk arena per finalization, and iterates only ingest-touched keys when
   emitting. Arroyo additionally suppresses retract==append pairs. Our GROUP BY flush should walk
   the per-batch touched-key set (we already group updates per batch), and skip emit when the
   value is unchanged (Flink parity: Flink's GroupAggFunction also suppresses no-change updates
   when the agg result is identical — verify semantics against the host on FILTER/DISTINCT edge
   cases).

Expected effect: q16 from 1.14x toward the 2x+ its Flink-side GC profile implies; q15/q17 similar
direction (Alibaba's Java engine reports 4.3x on q15 with distinct-split+mini-batch).

Acceptance: q15/q16/q17 profiles show ScalarValue drop/clone/hash out of the top leaves; parity
matrix (aggregate-type support incl. FILTER + DISTINCT combinations) green; matrix re-quoted.
