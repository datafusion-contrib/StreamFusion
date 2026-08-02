# Sub-plan reuse scoped by digest barriers

**Applies to:** every query with a shared source or self-join (multi-view/self-join queries)

Installing the native planner used to disable Flink's sub-plan reuse outright — the safe-but-blunt
way to keep any Arrow batch from fanning out to two consumers (the hand-off is zero-copy; the
consumer closes the buffers). That also un-shared the *rowwise* prefix ahead of the island, so every
multi-view/self-join query generated and converted its source stream once per branch: the profiling
round measured an exactly-2x `Row→RowData` conversion tax on q3/q4/q5/q7/q8/q9/q20.

The fix keeps reuse enabled and adds a per-instance term to every native rel's digest (emitted only
at the digest explain level, so `EXPLAIN` output is unchanged). Flink's post-optimize reuse pass
merges by digest, so it can now merge the shared rowwise prefix under the islands exactly as stock
Flink does — but it can never merge a columnar subtree, since each native instance's digest is
unique.

Measured on the generator profile loop: q3 +17%, q9 +9%, q20 +6%, with the conversion cost per
iteration restored to parity with stock Flink's.
