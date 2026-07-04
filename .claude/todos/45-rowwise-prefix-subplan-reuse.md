# Stop forking the rowwise prefix: scope the sub-plan-reuse ban to columnar edges

**Status:** TODO (found by the 2026-07-04 profiling round,
`.claude/research/nexmark-operator-profiles-2026-07.md`, lever 1 — the largest single systemic cost
it surfaced).

`NativePlanner.install` disables Flink sub-plan reuse globally because an island's zero-copy
hand-off assumes each Arrow batch is consumed once (the consumer closes the off-heap buffers); a
reused branch fanning one batch to two consumers would double-free. But that invariant only
concerns **ArrowBatch-typed edges**. Disabling reuse globally also forks the **rowwise prefix** —
the source function plus the external-Row→`RowData` conversion node — so every query that reads
two views of one stream converts the whole stream **twice**.

**Measured** (generator loop, `RowRowConverter` CPU samples/iteration, native vs stock Flink):
q0/q19 (single view) 2.2/2.2 and 2.5/2.5 — equal; q3 4.8/2.3, q4 4.7/2.5, q5 4.6/2.2, q7 5.0/2.2,
q8 4.6/2.2, q9 4.4/2.5, q20 4.6/2.2 — an exactly-2x tax on every multi-view/self-join query
(~10% of q3's total job CPU; the datagen source itself is NOT doubled — the fork sits at the
conversion node). On the Kafka rungs the same fork presumably duplicates consume+decode — verify
and quantify there too (it may be much larger).

## What to do

Re-enable sub-plan reuse, then enforce the single-consumer invariant only where it actually
applies, either by
- scoping the reuse pass exclusion to sub-plans whose output type is `ArrowBatch` (the transpose
  and everything downstream), keeping the rowwise prefix shared; or
- letting Arrow edges fan out behind an explicit share/clone barrier (a defensive per-consumer
  batch retain — refcount the C-Data buffers or IPC-copy at the fan-out point only).

The first is simpler and captures the measured win; the second is only needed if a *columnar*
sub-plan ever profitably fans out (none does today — the fan-outs are all at the rowwise prefix).

Expected effect: removes ~2.3 samples/iter from 7+ Nexmark queries on the generator rung
(q3/q20/q8 gain the most, all currently below 1x there); Kafka effect to be measured.

Acceptance: q3/q4/q5/q7/q8/q9/q20 `RowRowConverter` per-iteration cost equal to stock Flink's in
the profile loop; matrix numbers re-quoted; parity suite green (reuse must not reintroduce the
double-free — add a fan-out-under-island test).
