# Logical mini-batches, decoupled from physical Arrow batches

**Applies to:** the two-phase local aggregate, changelog normalize, keep-last dedup, single-phase
GROUP BY, unique-key updating joins, and retracting [Top-N](../operators/top-n.md). Gated by
`table.exec.mini-batch.enabled` — with mini-batch off, each operator keeps its original per-row
(or per-physical-batch) changelog behavior byte-for-byte.

## One mechanism, five operators

Flink's mini-batch model groups input by a *logical* boundary — a row-count trigger, a marker, a
watermark — not by however the physical Arrow batches happen to arrive. The local two-phase
aggregate established the pattern: it counts rows across input batches and splits a batch exactly
at the configured Flink count trigger. That split is an Arrow reference-counted view, so enforcing
the latency/state-size boundary copies no row buffers, and a physical batch can no longer silently
enlarge the logical bundle. Marker, watermark, checkpoint, and end-of-input flushes all reset the
same shared boundary controller, so every mini-batch operator drains at the same points Flink
itself would. Criterion finds no measurable overhead when the logical and physical sizes coincide
at 4,096 rows (17.75 vs. 17.83 M rows/s), while coalescing 32, 256, and 4,096 rows is respectively
**4.43×, 10.29×, and 18.42× faster** than the size-1 immediate-flush baseline for a 64-key local
`SUM`. Flink metrics expose bundle/input/output counts, flush reasons, physical-batch splits,
touched keys, cancelled changes, and the last/peak bundle shape, so a profile can explain a gain or
regression without guessing at bundle behavior.

The second half of the mechanism is what happens *inside* a bundle: a native transition substrate
folds each key's insert/delete/update chain down to its first preimage and final postimage, in
deterministic first-touch order. Inverse changes and equal updates disappear entirely at flush.
That one tested changelog algebra is what every operator below reuses — each applies it to its own
state shape (a durable row, a group accumulator, a join side, a ranked buffer), but none of them
re-derive the fold themselves. The byte-level plumbing underneath — borrowed-key probes into arrow
row state, encoded once per batch — is the same discipline described on the
[arrow-row byte state](arrow-row-state.md) and [borrowed key probes](borrowed-key-probes.md) pages;
this page is about the boundary and the fold, not the encoding.

## Changelog normalize: a logical transition frontier

[Changelog normalize](../operators/changelog-normalize.md) mutates its durable keep-last state on
every input, but in mini-batch mode retains only the first preimage and final postimage per unique
key. A 4,096-row replacement storm over 64 keys emits 64 rows instead of 8,128 — a 127x output cut.
The first release profile found `ScalarValue` extraction/clone/drop and per-replacement
transition-map probes behind the initially modest gain; full rows now use one Arrow-row encoding
pass and compact shared byte payloads, with an embedded dirty bit that stages only the first touch
and reads the durable final value at flush.

Criterion measures **20.79 M** input rows/s versus **15.10 M** for immediate materialization and
**12.06 M** when flushed every 256 physical rows — **1.38x** and **1.72x** faster, respectively,
while cutting output 127x. A second profile reduces transition-map work to a handful of samples;
payload allocation, `BinaryRow` key encoding, and the durable hash probe are the remaining push-path
costs. Logical bundling is independent of Arrow chunking, and transient preimages are included in
[task off-heap accounting](memory-accounting-off-hot-path.md).

## Keep-last dedup: finalize only the winning row per key

Proctime and rowtime keep-last dedup retain the first accepted preimage and continue applying
arrival-order/maximum-rowtime selection to durable state; stale rowtime candidates remain invisible.
Flush reads the final winner once. Keep-first stays on its immediate insert-only path, since it has
no replacement churn to remove.

On the same 4,096-row, 64-key replacement workload, Criterion measures **27.30 M** input rows/s for
the logical bundle versus **17.83 M** immediate and **14.17 M** with 256-row physical flushes —
**1.53x** and **1.93x** faster. The dirty frontier and retained preimages are included in
task off-heap accounting.

## GROUP BY: finalize only the dirty-key frontier

The single-phase [GROUP BY](../operators/group-by.md) aggregate's mini-batch state retains the first
emitted tuple and an Arrow key-row reference on a group's first touch, continues mutating the
durable accumulator without constructing intermediate outputs, and gathers one compact changelog at
the logical boundary — its "dirty-key frontier."

On 4,096 rows over 64 hot keys this is **3.25×** faster than per-row emission and **2.37×** faster
than flushing a diff after every 256-row physical batch (**23.41 vs. 7.21 and 9.89 M rows/s**
respectively). Equal pre/post tuples and groups created then deleted within the bundle emit
nothing; immediate mode remains byte-for-byte unchanged. The Flink operator uses the shared exact
row-count splitter and drains before watermarks, checkpoints, and finish. Retained Arrow key buffers
and first preimages are included in task off-heap accounting and the common bundle metrics.

A post-integration release profile attributes roughly **91%** of samples to state update and **6%**
to finalization; key gathering itself is negligible. The push path therefore skips constructing
empty key/result arrays and leaves the remaining frontier in `BinaryRow` encode/probe and
accumulator fold.

## Unique-key updating joins: fold both inputs to net transitions

When Flink metadata proves that each side's join key contains an upsert key, the two-input updating
join operator shares one exact row-count boundary and retains the durable preimage/final postimage
per side and key. At flush it replays only those compact transitions through the existing
INNER/outer/semi/anti state machine, preserving predicate and degree semantics; non-unique joins
remain immediate.

A 4,096-row replacement storm over 64 joined keys measures **27.69 M** input rows/s for one logical
bundle versus **8.98 M** immediate and **7.57 M** with 256-row physical flushes — **3.08x** and
**3.66x** faster. Release profiling initially found owned-key allocation/free on every staged
replacement; probing the transition frontier by borrowed encoded key — the same discipline as
[borrowed key probes](borrowed-key-probes.md) — improved the logical path from 15.03 to
27.69 M rows/s. Staged keys and rows are charged to task off-heap memory, and count, aligned-watermark,
checkpoint, and finish boundaries drain both sides.

Retracting [Top-N](../operators/top-n.md) applies the same first-preimage/final-postimage algebra
to its ranked buffer; its own numbers and the append-only ranker's distinct-row emit optimization
are covered on
[Top-N emit decodes distinct rows](topn-emit-decodes-distinct-rows.md).
