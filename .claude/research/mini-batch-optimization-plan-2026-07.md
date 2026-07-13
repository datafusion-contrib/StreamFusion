# Native mini-batch optimization plan

Status: phases 0-3 implemented and benchmarked; phase 4 remains separately gated, 2026-07-13.

Implemented checkpoints: Flink assigner semantics, exact cross-batch row boundaries, common bundle
metrics, deterministic transition folding, local/single-phase aggregate finalization, append-only
and retracting Top-N, changelog normalization, and keep-last dedup. Keep-first remains deliberately
profile-gated because it has no replacement churn to coalesce. Metadata-proven unique-key updating
joins now fold both sides through one shared logical boundary. General joins and time-sensitive
operators retain the separate correctness gates below.

The objective is to make a Flink logical mini-batch, rather than an individual Arrow
`RecordBatch`, the unit over which native stateful operators amortize key lookup, accumulator work,
changelog construction, and output materialization. Every optimization must preserve the final
materialized result and Flink's operator-specific mini-batch contract. Raw per-record changelog
parity remains the contract when mini-batch is disabled.

This work is performance work, not new SQL coverage. Each shipping commit must update
`docs/optimizations.md`; changes to planner coverage or fallback conditions must also update
`docs/coverage-and-fallbacks.md`. Semantic deviations from stock Flink, including the existing
Top-N collapsed-changelog behavior, belong in `divergences/`.

## Contract to implement

Mini-batch is not a global license to discard changelog records. It is an operator-specific
optimization enabled by `table.exec.mini-batch.*`. An operator may coalesce changes only by a key
for which intermediate changes are not part of that operator's required result. A flush exposes
the bundle atomically to downstream operators.

A native logical bundle has these boundaries:

1. The configured count trigger. Count rows exactly across Arrow batches. If an Arrow batch crosses
   the limit, process a zero-copy `RecordBatch::slice` up to the limit, flush, then process the
   remainder. Never let a physical batch silently enlarge the configured logical bundle.
2. A mini-batch marker from the processing-time or row-time assigner.
3. Before forwarding an ordinary watermark from an operator that buffers records.
4. `prepareSnapshotPreBarrier`, before the checkpoint barrier is forwarded.
5. End of input / `finish`.
6. Close only as cleanup; correctness must not depend on close being called.

For a two-input operator, the count trigger counts accepted rows from both inputs, matching the
intent of Flink's co-bundle trigger. Input watermarks remain aligned using the normal two-input
minimum-watermark rule; flush before forwarding the newly effective watermark. A checkpoint flushes
all pending changes from both inputs before snapshotting durable state.

The processing-time assigner must ignore ordinary upstream watermarks and forward only the terminal
`Long.MAX_VALUE` watermark. Its own periodic markers are generated from processing time. The
row-time assigner continues to derive markers from upstream event-time watermarks. Tests must pin
these behaviors against the corresponding Flink operators.

The transient bundle itself is never checkpointed: it is drained before every checkpoint. Durable
operator state is checkpointed exactly as today.

## Shared implementation substrate

Implement the substrate before changing individual operators.

### `MiniBatchBoundary`

One boundary controller owns the row count and splitting rules used by every one-input native
mini-batch operator. A two-input wrapper applies the same counter to both inputs. It should expose
only `accept(batch) -> slices and flush points`, `on_marker`, `on_watermark`, `on_checkpoint`, and
`on_finish`; operators should not independently reimplement trigger arithmetic.

The JVM operator owns the incoming `VectorSchemaRoot`, so count-trigger slicing should use Arrow
Java's reference-counted `VectorSchemaRoot.slice` before exporting each slice through C Data. The
original root is closed only after all slices have been consumed. Rust still receives ordinary
`RecordBatch` views; no row copying or new cross-language wire format is needed. Verify this
ownership sequence with allocator/leak tests before sharing the helper across operators.

Required metrics:

- logical bundles flushed, classified by count, marker, watermark, checkpoint, and finish;
- input rows and physical Arrow batches per logical bundle;
- output rows and cancelled changelog records;
- touched keys and peak transient bytes;
- count-trigger splits of physical Arrow batches.

### `MiniBatchChanges<K, R>`

Add a deterministic transition buffer inspired by RisingWave's `ChangeBuffer`:

| Existing transition | New input | Net transition |
|---|---|---|
| none | insert/delete/update | input transition |
| insert | delete | cancel |
| insert | update | insert final postimage |
| delete | insert | update first preimage to final postimage |
| update | update | update first preimage to final postimage |
| update | delete | delete first preimage |

Equal preimage/postimage updates are removed at flush. A change whose semantic key changes is a
delete under the old key plus an insert under the new key. Preserve deterministic first-touch order
with a hash map of slots plus an explicit touched-key vector; do not depend on hash-table iteration
order. Operators may impose a stronger deterministic output order, such as rank order for Top-N.

The row representation is operator-selected: borrowed encoded bytes while processing, `Arc<[u8]>`
or an Arrow batch row reference once retained. Do not reintroduce `ScalarValue` state.

### Dirty-key frontier

For aggregate-like state, embed a generation/dirty marker and live-record count in the group state,
following Proton's `TrackingUpdates`. Keep a compact touched-key vector per bundle. On the first
mutation of a group in a generation, capture its preimage lazily and append its key once. Flush
walks only that vector. Prefer this over a second `HashSet`/`HashMap` of dirty groups.

### Adaptive Arrow compaction

For transient input/output, benchmark two representations:

- retain Arrow batches and compact with operation/visibility bitmaps or `take`;
- reconstruct a dense output batch from the surviving rows.

Inline visibility is attractive when most rows survive; reconstruction avoids retaining a large
batch for a sparse result. Choose adaptively only after Criterion establishes a stable crossover.

## Operator matrix

| Operator family | Current mini-batch behavior | Safe/useful target | Priority |
|---|---|---|---|
| Processing-time assigner | Generates periodic markers but currently forwards ordinary upstream watermarks | Ignore non-terminal upstream watermarks; pin marker and terminal behavior to Flink | P0 correctness |
| Local group aggregate | Bundles rows, but a physical Arrow batch may overshoot the count trigger; fold is largely row-oriented | Exact count splitting; one key encoding pass; dirty groups; per-group visibility/sub-batches; one partial per touched key | P0/P1 |
| Global and single-phase group aggregate | Applies partial/input rows and constructs changelog during each push; cached preimage exists | Retain first preimage and final postimage per touched key across the logical bundle; emit once per changed key | P1 |
| Append-only Top-N | Mini-batch mode snapshots/diffs per physical Arrow batch | Keep mutation staging per partition across the logical bundle; emit the final rank-ordered net diff | P1 |
| Retracting Top-N | Diffs around each input/batch; intermediate rank churn remains | Use the shared transition buffer for membership/rank mutations across the bundle; preserve rank-projection updates | P2 |
| Keep-last dedup | Durable latest-row state is efficient, but replacement changelog can be emitted for every physical batch | Retain first old row and final winning row per unique key; suppress stale and no-op replacements | P2 |
| Keep-first dedup | Naturally emits at most one accepted row per durable key | Batch key encoding/probing and cancel insert/delete pairs where input mode permits; expect limited output reduction | P3/profile-gated |
| Changelog normalization | Scalar-keyed pending hotspot; normalizes local transitions | Move to `MiniBatchChanges`; visibility-mask or dense-output compaction | P2 |
| Updating equi-join with declared unique keys | Processes each side immediately; repeated replacements amplify probes and join changelog | Coalesce each unique-key side to its first preimage/final postimage, then apply deterministic net changes at flush | P3 |
| General/non-unique updating join | Each row may alter match multiplicity and outer/semi/anti degree state | Dirty join-key staging and final result diff only after a correctness model covers multiplicity, outer NULL rows, and input interleaving | P4, separately gated |
| Running/bounded `OVER` | Processes Arrow batches but each logical row normally has an observable result | Do not collapse rows. Improve only physical columnar kernels, grouped row positions, and state access | Not semantic mini-batch work |
| Window aggregate and window Top-N | Watermarks/window closure define visibility; already consume physical batches | Do not delay across closure. Use physical batching, dirty panes/windows, and vector kernels only | Separate profiling work |
| Interval/temporal/window join | Watermark, time bounds, and arrival order affect matches/expiry | Do not changelog-coalesce across semantic time boundaries; batch probes and output assembly only | Separate profiling work |
| Lookup join | Connector calls and miss dedup dominate | Deduplicate lookup keys within a physical/logical request where connector semantics permit; not changelog coalescing | Separate profiling work |
| Calc, filter, exchange, transpose, sinks/sources | Stateless or boundary work | Mini-batch configuration must add no work; optimize physical Arrow batches independently | Out of semantic scope |

Never coalesce temporal versions, rows whose processing order is part of the result, or distinct
logical rows merely because they share a partition key.

## Implementation sequence

### Phase 0: correctness and infrastructure

1. Add focused parity tests for processing-time and row-time assigner watermark behavior.
2. Fix the processing-time assigner to discard ordinary upstream watermarks.
3. Introduce the shared exact-count boundary controller, including zero-copy batch splitting.
4. Convert the local group aggregate to the controller and eliminate count-trigger overshoot.
5. Add checkpoint, finish, empty-bundle, terminal-watermark, and physical-batch-straddling tests.
6. Add bundle metrics. Establish a no-regression Criterion baseline before operator work.

Phase 0 is complete only when randomized input chunking produces identical logical bundles and
outputs for the same rows and trigger sequence.

### Phase 1: highest-confidence wins

#### Top-N

Replace full old/new snapshot work where possible with RisingWave-style mutation staging. A cache
entry entering or leaving the visible zone records an insert/delete in the partition's staging
buffer; inverse mutations cancel. Staging persists until the Flink logical boundary, not merely the
end of `push(RecordBatch)`.

For rank projection, flush compares/finalizes positions so surviving rows whose rank changed still
produce the required `-U/+U`. Without rank projection, emit only membership changes. Mini-batch-off
continues to emit the current per-record Flink-compatible cascade.

Benchmark mutation staging against the current physical-batch snapshot/diff strategy; do not assume
staging wins for very small `N` and one-row batches.

#### Group aggregate

Add the dirty-generation/touched-key frontier to global and single-phase aggregate state. Capture
the cached old tuple only on the first mutation in the bundle and finalize only touched groups.
Suppress equal old/new results exactly as Flink does. Then separately prototype row-position
grouping and per-group visibility masks so accumulators receive columnar subsets rather than one
row at a time.

Ship dirty-key finalization independently from vectorized group folding so their gains remain
measurable.

### Phase 2: generic changelog folding

1. Land `MiniBatchChanges` with exhaustive transition-table and property tests.
2. Move changelog normalization to it.
3. Move keep-last dedup to first-preimage/final-postimage staging.
4. Apply it to retracting Top-N membership/rank staging.
5. Evaluate adaptive Arrow visibility versus reconstruction for these operators.

Property tests should compare every short sequence of insert/delete/update operations against a
reference materialized map, including key-changing updates and inconsistent input modes. Test both
upsert and retract output forms.

### Phase 3: unique-key updating joins

Admit this optimization only when planner metadata proves the coalescing key is unique for that
input. Buffer the first preimage and final postimage independently for each side while preserving a
deterministic first-touch sequence. At flush, update durable state and emit the net join result.

Test all join kinds already supported, repeated updates on either side, both sides changing in one
bundle, key-changing updates, null join keys, residual predicates, and checkpoint/watermark flushes.
Compare the materialized result after every flush with the non-mini-batched operator. If raw
changelog ordering becomes a documented divergence, record it explicitly before shipping.

Proton's retained columnar block plus `{block, row}` representation is not the default durable join
design. Existing borrowed-byte state removed stored-row decode from profiles. It may be benchmarked
only as a transient bundle representation and reconsidered for durable state only if profiles show
payload copying/decoding or allocator pressure becoming dominant again.

### Phase 4: non-unique joins and physical vectorization

General join coalescing requires a formal multiset/degree model. Implement it only after the unique
case is correct and profiles show enough remaining join cost. In parallel, profile the operators
that cannot change changelog granularity (`OVER`, temporal/window joins, windows) and optimize their
physical Arrow kernels without calling those changes semantic mini-batching.

Precomputed hashes, fixed-width stack keys, and saved hash cells are profile-gated here. Internal
transient maps may reuse a hash computed during key encoding; Flink key-group routing must retain
its existing hash contract.

## Criterion design

Every optimized operator gets an A/B benchmark that runs the same generated changelog through:

1. mini-batch disabled / immediate behavior;
2. current physical-Arrow-batch behavior, where distinct;
3. logical mini-batch implementation;
4. strategy variants being evaluated, such as snapshot/diff versus mutation staging.

Run native Criterion with `cd native && cargo bench`; Cargo benches use the release bench profile.
Run JVM/Flink integration and Nexmark benchmarks through Maven's `-Pbench` profile. Record time per
input row, output rows, state bytes, transient bytes, and cancellation ratio. Do not report
debug-build numbers.

Standard matrix:

- logical bundle sizes: 1, 32, 256, 4,096, and 50,000 rows;
- physical Arrow batch sizes deliberately both smaller and larger than the logical limit;
- key cardinalities: 1, 64, 4,096, and nearly unique;
- distributions: uniform, 90/10 skew, and a single hot key;
- changelogs: append-only, replacement-heavy, cancelling insert/delete, no-op update, and mixed
  retract/update;
- payloads: fixed-width narrow, mixed Nexmark shape, and string-heavy;
- Top-N: `N = 1, 10, 100`, with/without projected rank and high/low boundary churn;
- joins: hit/miss ratios, match multiplicity, alternating sides, and unique-key replacement storms;
- aggregates: plain, filtered, DISTINCT, and retraction-bearing partials.

Required named comparisons:

| Operator | Strategies |
|---|---|
| Top-N | per-record cascade; physical-batch snapshot/diff; logical-bundle snapshot/diff; mutation staging |
| Aggregate | immediate emit; dirty hash set; embedded generation+touched vector; grouped visibility masks |
| Normalize/dedup | owned rows; retained Arrow+visibility; reconstructed compact output |
| Unique-key join | immediate state mutation; owned-row bundle folding; retained Arrow row references |
| Keyed transient maps | current borrowed-byte hash; precomputed hash, only if profiles justify it |

Criterion must include a size-1 case so the feature cannot hide unacceptable overhead when a bundle
does not coalesce anything.

## Parity and integration testing

For every operator:

- compare mini-batch size 1 with the non-mini-batched output contract;
- compare final materialized state after every logical flush;
- randomize physical Arrow chunking while keeping rows and logical triggers fixed;
- test count, timer marker, ordinary watermark, terminal watermark, checkpoint, and finish flushes;
- restore from a checkpoint taken immediately after a forced flush and compare the continuation;
- run null, key-change, retraction, and equal-update cases;
- keep mini-batch-disabled byte-level parity tests intact.

For operators with a collapsed-changelog contract, compare both the expected collapsed changelog
and downstream materialization. Never weaken the global parity harness merely to admit an
optimization.

Nexmark validation targets:

- q19: append-only Top-N;
- q4, q15, q16, q17: group/global aggregates, filters, and DISTINCT;
- q18: keep-last dedup / rank-one path;
- q3, q9, q20, q23: updating joins and join-to-rank pipelines.

Run the complete tuned mini-batch matrix afterward to catch planner fallbacks and cross-operator
boundary bugs.

## Profiling loop and shipping gates

Profile before implementation, after the isolated Criterion win, and after end-to-end integration.
Use release+mimalloc builds. For Nexmark, report both CPU samples per completed iteration and
iterations completed in the fixed wall-time window. The rowwise generator perimeter can mask a
large isolated operator gain, so retain both measurements rather than substituting one for the
other.

For each phase:

1. Capture the current operator benchmark and representative Nexmark flame graph.
2. Implement the smallest independently measurable technique.
3. Run Criterion with confidence intervals and retain the comparison artifact.
4. Reprofile to verify the intended hotspot shrank and that work did not move into allocation,
   hashing, Arrow `take`, or output reconstruction.
5. Run parity tests and the relevant Nexmark queries.
6. Keep the change only if it improves the intended workloads without a material regression in
   size-1/nearly-unique cases, or if it is necessary substrate for a measured next phase.

The primary success metrics are operator CPU per input row, emitted changelog rows per input row,
and end-to-end CPU per completed Nexmark iteration. Throughput alone is insufficient when job setup,
timers, or the benchmark perimeter dominate.

## Explicit non-goals and stop conditions

- Do not change Flink's configured latency/size semantics to match convenient Arrow batch sizes.
- Do not checkpoint transient mini-batch buffers.
- Do not coalesce across watermarks, checkpoint barriers, window closure, or temporal boundaries.
- Do not use mini-batch mode to collapse results for operators whose per-row output is observable.
- Do not replace the updating join's durable byte state with a block store without new profile
  evidence invalidating the existing rejection.
- Do not add fixed-width/prehashed key specializations speculatively.
- Do not claim SIMD merely because input is Arrow; require a vector kernel or profile evidence of
  reduced per-row dispatch.
- Stop or revise an operator strategy when output construction, memory retention, or size-1
  overhead consumes the coalescing gain.

## Definition of done

The project is complete when:

1. The processing-time watermark and exact-count bugs are fixed and parity-tested.
2. All operators marked P1-P3 have an explicit logical-bundle implementation or a benchmark-backed
   decision not to ship one.
3. Every changed operator has Criterion A/B results against mini-batch-disabled behavior across the
   standard matrix.
4. Profiles demonstrate where the speedup came from and show no newly dominant avoidable hotspot.
5. Mini-batch-disabled raw changelog parity remains intact; enabled-mode divergences are documented.
6. Checkpoint, watermark, end-of-input, randomized chunking, and restore tests pass.
7. Relevant Nexmark queries and the full tuned matrix pass without native coverage regressions.
8. `docs/optimizations.md`, `docs/benchmarks.md`, coverage documentation, divergences, and the
   canonical GitHub backlog reflect the shipped result rather than this plan.

## Reference implementations and prior decisions

- Flink is authoritative for trigger, watermark, checkpoint, and output semantics (`~/data/flink`).
- Arroyo remains the required first reference for operator structure (`~/data/arroyo`).
- RisingWave supplies `ChangeBuffer`, inline `StreamChunkCompactor`, dirty hash aggregation, and
  `TopNStaging` (`~/data/risingwave`).
- Proton supplies accumulator-adjacent `TrackingUpdates` and the profile-gated ref-counted block/row
  reference design (`~/data/proton`).
- StreamFusion's existing borrowed-byte state and the rejected durable join block store remain the
  baseline; see `.claude/wontdos/48-updating-join-block-state.md` and `docs/optimizations.md`.
