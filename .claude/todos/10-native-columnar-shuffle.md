# Native columnar shuffle: keep Arrow across the exchange

**Status:** open (design) — now the central gate (see [00-roadmap](00-roadmap.md) #1).
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
   `interleave_record_batch` to assemble each partition's sub-batch. It matches Spark's hash for
   parity; we must match **Flink's** key-group assignment instead (below).

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
A split operator partitions an `ArrowBatch` into per-keygroup sub-batches (Comet scatter-append +
`interleave`), each tagged with its destination keygroup; a custom `StreamPartitioner<ArrowBatch>`
routes each sub-batch by that tag. **Parity-critical:** reproduce Flink's chain exactly —
`key.hashCode()` (a `BinaryRowData` key → Murmur3 **seed 42**, `BinarySegmentUtils.hashByWords`)
→ `MathUtils.murmurHash` + `bitMix` → `% maxParallelism` (default **128**) → keygroup →
`keygroup * parallelism / maxParallelism` → subtask. The split runs natively; the keygroup→channel
selection runs in the Java partitioner.

### Exact Flink references for parity
`StreamExecExchange` (HASH → `KeyGroupStreamPartitioner` + `RowDataKeySelector`);
`KeyGroupRangeAssignment.{assignKeyToParallelOperator, computeKeyGroupForKeyHash,
computeOperatorIndexForKeyGroup}`; `MathUtils.{murmurHash, bitMix}`; `BinaryRowData.hashCode` →
`BinarySegmentUtils.hashByWords` → `MurmurHashUtils` (seed 42); `PartitionTransformation` +
`StreamPartitioner<ArrowBatch>` (return `SubtaskStateMapper.FULL`).

## Constraints
- Reuse Flink's exchange/network — preserve barrier alignment and backpressure.
- Partitioning must match Flink's key-group distribution exactly (Phase 2) so results are
  identical; verify with the parity harness at parallelism > 1.

## Build slices
1. **Phase 1**: columnar single-channel keyed exchange (parallelism 1) + route a columnar window
   onto it; parity-test a `GROUP BY key, window` query at parallelism 1 (columnar source/filter →
   window). This is also what ticket 21's columnar-windows item is gated on.
2. **Phase 2a**: native `partition_batch(batch, key_cols, key_types, num_partitions)` →
   `Vec<(partition, batch)>` matching Flink's keygroup hash; unit-test parity against the Flink
   formula in isolation.
3. **Phase 2b**: the split operator + custom partitioner; parity-test at parallelism > 1.

## Interaction
- Enables ticket 09 (a native chain spans a shuffle and stays columnar) and ticket 21's
  **columnar windows** (gated here). First concrete uses: the windowed input edge and the
  two-phase local→global exchange.
