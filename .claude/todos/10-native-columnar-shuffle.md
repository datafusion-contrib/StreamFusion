# Native columnar shuffle: keep Arrow across the exchange

**Status:** DONE for the single-phase window case. A native columnar exchange
(`StreamPhysicalNativeColumnarExchange` + `NativeColumnarExchangeExecNode`) splits each
batch by key (`SplitByKeyGroupOperator`) and routes it via a `PartitionTransformation` +
`ColumnarKeyGroupPartitioner`, feeding the columnar window with no row transpose.
`PhysicalPlanScan` substitutes the exchange + columnar window together when the exchange's
input is columnar (`FlinkColumnarWindowSqlHarnessTest`: no-key SINGLETON and keyed HASH both
parity-green). The shuffle uses its own co-locating hash, not Flink's key-group hash — safe
because the window re-groups in operator state ([divergences/10](../../divergences/10-columnar-exchange-own-hash.md)).
The `ArrowBatchSerializer` now releases the batch after serializing it onto the network edge
(the write-side leak this surfaced).

**Remaining:** two-phase local→global columnar (the exchange between a native local and global
window aggregate); higher-parallelism shuffle coverage (tests are p=1, where the split is one
channel; multi-channel split is unit-tested in `SplitByKeyGroupOperatorTest`).

**Source:** user direction — "I don't want to transpose all the time"

## Why this is the gate now
The columnar-flow mechanism (ticket 21) is built and proven for shuffle-free segments
(`ArrowBatch` edges, transpose operators, transition pass). The remaining big unlock is
carrying `ArrowBatch` across a keyed exchange. It blocks **two things**: keeping two-phase
local→global columnar, and making the **window operators** columnar at all (they sit downstream
of a keyBy, so their input edge is an exchange — ticket 21 windows are gated here).

## Approach to use (validated against Arroyo)
Arroyo's pattern, to adapt: inter-operator message is `Data(RecordBatch) | Signal(watermark |
barrier | …)`; the network edge serializes batches with **Arrow IPC**; a keyed shuffle splits a
batch by key with `sort_to_indices → take → slice` into one sub-batch per destination partition,
then sends each. Our `ArrowBatchSerializer` already does the IPC half (it is invoked exactly on a
non-chained/network edge). The new work is the by-key batch partitioner on the columnar stream.

## Problem
When a shuffle sits between two native operators (e.g. two-phase aggregation:
local -> exchange -> global), the data is transposed Arrow -> RowData before
the host's row-based shuffle and RowData -> Arrow after it. That is two
transpositions plus a row-encoded network hop per shuffle, on the hottest path.
Keeping the data columnar across the shuffle would remove both.

## Goal
A columnar exchange that carries Arrow batches (partitioned by key) between
native operators without round-tripping through RowData, so a native chain
stays columnar across a shuffle.

## Design (researched against Flink 2.2, Comet, and our own operators)

Three findings shape it:

1. **Our native window operators use ZERO Flink keyed state.** They are plain
   `AbstractStreamOperator`s holding state in the native handle (+ operator `ListState<byte[]>`
   for checkpoints); the grouping key is read positionally and grouped *inside* native code.
   No `setCurrentKey`, no keyed-state backend. So keying is purely (a) a *distribution*
   requirement met by an upstream exchange + (b) internal native grouping — the operators slot
   onto a columnar exchange unmodified.
2. **At parallelism 1, a keyed exchange routes every record to channel 0** (pass-through). So a
   columnar window at parallelism 1 needs **no by-key split** — just a single-channel columnar
   exchange. This is the simple first step.
3. **Comet does the by-key split** (for parallelism > 1) with scatter-append, not sort: vectorized
   hash of the key columns → partition id; one counting pass + prefix sum → per-partition offsets;
   `interleave_record_batch` to assemble each partition's sub-batch. Comet matches Spark's hash for
   *interop* with Spark operators; we do **not** need to match Flink's hash — see Phase 2 — because
   our shuffle feeds only our own native operator. Any consistent hash works.

Reuse Flink's exchange/network (credit-based backpressure, barriers) — do **not** build a native
transport. The columnar batch rides as the record payload, serialized by our existing
`ArrowBatchSerializer` (Arrow IPC, invoked only on the network edge). A custom
`StreamPartitioner<ArrowBatch>` selects the channel.

### Phase 1 — parallelism 1 (no split)
A columnar keyed exchange that carries `ArrowBatch` with a trivial `StreamPartitioner` returning
channel 0. Mark it so the transition pass treats source/window edges as columnar. Unblocks
**columnar windows** and **two-phase local→global staying columnar** at parallelism 1. No hash, no
split — smallest slice that delivers the columnar window.

### Phase 2 — parallelism > 1 (by-key split)
A split operator partitions an `ArrowBatch` into per-channel sub-batches (Comet scatter-append +
`interleave`), each tagged with its destination channel; a custom `StreamPartitioner<ArrowBatch>`
routes each sub-batch by that tag.

**We do NOT need to reproduce Flink's key-group hash.** The shuffle hash only decides *which
subtask handles which key* — it does not affect the result. The only correctness requirement is
**same key → same channel** (any deterministic hash of the key columns; reuse the existing
`GroupKey`/`ScalarValue` hashing or ahash). This is sound because the keyed consumer downstream is
*our* native operator, which holds **operator** state (not Flink keyed state) and re-groups
internally — so it never depends on Flink's key→subtask mapping. The final result set is identical
under any consistent partitioning, and the parity harness compares sorted result sets.

**The one guardrail:** substitute a columnar exchange **only when its downstream keyed operator is
also substituted natively**. A fallen-back Flink window *would* assume Flink's distribution, so a
columnar exchange must never feed a Flink operator. By construction it only ever feeds a native
columnar window, so this holds — but the substitution must couple the two (don't replace the
exchange if the window isn't replaced).

(Contrast: Comet reproduces Spark's Murmur3+pmod because it runs *inside* Spark's plan, where
partitioning is a contract other Spark operators / joins / bucketing / AQE depend on — an interop
requirement, not a standalone-result one. We have no such cross-engine contract for a fully-native
keyed segment.)

### Flink references (mechanism only — no hash parity needed)
`StreamExecExchange` (the HASH exchange we replace); `PartitionTransformation` +
`StreamPartitioner<ArrowBatch>` (custom partitioner reading the sub-batch's destination channel;
return `SubtaskStateMapper.FULL`). `numberOfChannels` (downstream parallelism) is supplied to the
partitioner at runtime; the split uses our own `hash(key) % numberOfChannels`.

## Constraints
- Reuse Flink's exchange/network — preserve barrier alignment and backpressure.
- Partitioning must match Flink's key-group distribution exactly (Phase 2) so results are
  identical; verify with the parity harness at parallelism > 1.

## Build slices
- **Native by-key split** ✅ DONE — `partition_batch(batch, key_cols, num_partitions)` →
  `Vec<(partition, batch)>` via a fixed-seed consistent hash (no Flink-hash reproduction). Rust-
  tested: a key never splits across partitions, all rows preserved, any partition count.
- **Columnar exchange mechanism** ✅ DONE — `SplitByKeyGroupOperator` (Arrow→Arrow, emits per-channel
  sub-batches tagged with destination via the native split over JNI) + `ColumnarKeyGroupPartitioner`
  (`StreamPartitioner<ArrowBatch>` routing by the tag). Harness-tested: key→single channel, rows
  preserved, partitioner routes by tag.
- **Columnar window operator** ✅ DONE (ticket 21) — `NativeColumnarWindowAggregateOperator`
  consumes Arrow batches and produces identical window aggregates; harness-verified.
- **Planner integration (remaining) — blocked on a columnar watermark-assigner.** Wiring was drafted
  (a `columnar` flag on the window exec node that drops the keyed exchange and does the
  split/partition itself, a `ColumnarInput` window rel, and a rewrite that goes columnar when the
  window's input is an exchange over a columnar source) but **reverted unbuilt**, because it cannot
  trigger end to end yet, and the fix must stay Flink-faithful:
  - **A Flink Parquet/filesystem source does not emit watermarks.** Verified: `FileSystemTableSource`
    implements projection/limit/partition/filter/metadata pushdown but **not** `SupportsWatermarkPushDown`,
    and no `flink-connectors/` source implements it. A windowed filesystem query's `WATERMARK FOR rt
    AS <expr>` is realized as a separate **rowwise `WatermarkAssignerOperator`** between source and
    window. So making our native source emit watermarks would diverge from Flink — do not do it.
  - **Prerequisite: a columnar watermark-assigner** — the Arrow→Arrow analog of Flink's
    `WatermarkAssignerOperator` (`flink-table-runtime/.../operators/wmassigners/`): compute the
    watermark from the time column and pass batches through, so the columnar chain reaches the window
    without transposing. With it, the window's input edge stays columnar and the drafted wiring
    triggers; then parity-test `GROUP BY key, window` over a columnar Parquet source at parallelism 1
    (single channel) and > 1 (split).

## Interaction
- Enables ticket 09 (a native chain spans a shuffle and stays columnar) and ticket 21's
  **columnar windows** (gated here). First concrete uses: the windowed input edge and the
  two-phase local→global exchange.
