# Logical mini-batches, decoupled from physical Arrow batches

**Applies to:** the two-phase local aggregate, changelog normalize, keep-last dedup, single-phase
GROUP BY, append-only and unique-key updating joins, and retracting
[Top-N](../operators/top-n.md). Gated by
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

## Updating joins: retain append-only Arrow batches or fold net transitions

For an append-only regular INNER/outer join, there is no valid reduction inside either input
bundle: duplicate rows must retain their multiplicity. StreamFusion therefore clones only the
Arrow batch references until the logical boundary, then processes Flink's side order (right before
left for INNER/LEFT/FULL, left before right for RIGHT). This avoids the otherwise pointless
Arrow-row encode/decode staging round trip while giving a dimension-like side the complete bundle
to become resident before the other side probes it.

On Q3 at 2 million events (best of two), the production-shaped Kafka JSON to exactly-once Kafka
JSON run improves from **1.044 M to 1.200 M events/s** with mini-batching, a **1.15x** gain; stock
Flink improves from 0.600 M to 1.076 M events/s in the same adjacent run, leaving StreamFusion
**1.12x faster** with mini-batching enabled. The generator-only isolation improves from 0.608 M to
0.658 M events/s (**1.08x**). Its remaining engine gap is not the join: a 4,346-sample CPU profile
attributes only 35 inclusive samples to join ingestion and one to Arrow bundle concatenation, while
the generator's RowData-to-Arrow perimeter accounts for 1,415 inclusive samples. The Kafka
front-page path decodes directly to Arrow and does not pay that generator-only transpose.

When Flink metadata proves that each side's join key contains an upsert key, the two-input updating
join operator shares one exact row-count boundary and retains the durable preimage/final postimage
per side and key. At flush it replays only those compact transitions through the existing
INNER/outer join state machine, preserving predicate and degree semantics; non-unique changelog,
SEMI, and ANTI joins remain immediate.

A 4,096-row replacement storm over 64 joined keys measures **27.69 M** input rows/s for one logical
bundle versus **8.98 M** immediate and **7.57 M** with 256-row physical flushes — **3.08x** and
**3.66x** faster. Release profiling initially found owned-key allocation/free on every staged
replacement; probing the transition frontier by borrowed encoded key — the same discipline as
[borrowed key probes](borrowed-key-probes.md) — improved the logical path from 15.03 to
27.69 M rows/s. Staged keys, rows, and retained append-only Arrow buffers are charged to task
off-heap memory. Count, either input watermark, checkpoint, and finish boundaries drain both sides.
Flink's two remaining changelog bundle shapes (an upsert key outside the join key, and no unique
key) are not yet native and retain immediate execution.

Retracting [Top-N](../operators/top-n.md) applies the same first-preimage/final-postimage algebra
to its ranked buffer; its own numbers and the append-only ranker's distinct-row emit optimization
are covered on
[Top-N emit decodes distinct rows](topn-emit-decodes-distinct-rows.md).
