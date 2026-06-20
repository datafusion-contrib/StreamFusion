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

## Approaches to evaluate
- **Arrow-as-payload over Flink's network**: keep using Flink's shuffle/network
  stack but ship a serialized Arrow batch (e.g. IPC bytes) as the record
  payload, partitioned by the key column. Transposition becomes batch
  (de)serialization rather than per-row Arrow<->RowData; still uses Flink's
  exchange, so checkpoint/credit-based backpressure are unchanged. Simplest
  first step.
- **Custom partitioner on a columnar stream**: a Flink partitioner that routes
  whole Arrow batches by a hash of the key column, splitting a batch per
  partition. Needs batch slicing/splitting by partition.
- **Fully native exchange**: native code owns partitioning + transport. Most
  divergent from Flink; conflicts with "Flink is the control plane" and its
  network/credit/checkpoint machinery. Likely out of scope.

## Constraints
- Must preserve Flink's checkpoint/barrier alignment and backpressure — favor
  reusing Flink's exchange with an Arrow payload over bypassing it.
- Partitioning must match the host's hash distribution so results stay
  identical (the exchange currently hashes by the key column).
- Verify identical results via the parity harness.

## Interaction
- Enables ticket 09: a native chain can span a shuffle and stay columnar.
- First concrete use: the two-phase local -> global exchange.
