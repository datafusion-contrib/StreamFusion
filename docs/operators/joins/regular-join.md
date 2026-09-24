# Regular join

**Status:** Native with the mode and payload restrictions below. An ordinary equi-join over two full changelog inputs — the only one of the five
join shapes that accepts a retracting/updating stream on *both* sides rather than requiring
insert-only input (see the [insert-only guard](../index.md#global-switches)). Each side is held as
keyed state so a later update or delete on either input can retract and re-emit downstream.

## Admission

The native matcher requires:

- an **equi-key** of a supported type on both sides, or a keyless INNER join over two insert-only
  inputs (including `CROSS JOIN`);
- each key uses Flink's ordinary or null-safe equality policy; mixed policies in composite keys
  are supported for INNER, LEFT/RIGHT/FULL, SEMI and ANTI joins;
- any residual non-equi predicate must be **expressible by the native expression engine**;
- every input column type must be one the Arrow converter and retained-row codec can carry.
  MAP and MULTISET fields fall back, including those nested inside ARRAY or ROW and those used
  only as payloads. The Arrow row codec does not support these types.

## Keyless INNER joins

`CROSS JOIN` and INNER joins whose entire condition is a supported non-equi predicate run
natively when both physical inputs are insert-only and each retains at least one payload column.
The columnar exchanges collapse both inputs onto one task and one key group. Empty key arrays
identify one shared state bucket; no equality predicate or synthetic key is added. Each arriving
row probes the other input's retained multiset, preserving duplicate multiplicities, NULL payloads,
and the predicate's three-valued logic. Immediate and insert-only mini-batch execution use the
same existing join state, including checkpoint restore and memory/RocksDB savepoint transitions.

This initial scope excludes keyless outer, SEMI/ANTI and updating joins. Admission follows the
physical plan: Flink can rewrite `COUNT(*)` over a cross product into a join of two updating
counts, which falls back under that rule even when both original sources are insert-only.
Zero-column physical inputs also fall back because the retained-row codec requires a payload.

This is a singleton streaming join, not a broadcast or time-bounded join. Without configured
state TTL it retains both sides indefinitely, as Flink does. It uses the existing native state
memory budget; it has no separate cardinality cap or spillable candidate-pair buffer. A cross
product can emit `left_count * right_count` rows, and a residual predicate is evaluated after
candidate generation. The RocksDB backend does not remove this per-bucket matching cost.
Its persistent state also retains whole-bucket values, coalesced at bundle boundaries. A
per-record layout with complete-bucket hydration reduced write volume but regressed release
state-microbenchmark throughput, so it was not adopted. See the [backend tradeoff](../../backends/rocksdb.md)
for the measurements and remaining access-pattern constraint.

[Interval join](interval-join.md), [window join](window-join.md), [temporal table
join](temporal-join.md), and [lookup join](lookup-join.md) all state their admission conditions as a
variant of this same list — this page is the fullest treatment; the others cross-reference it rather
than repeating it.

## Null-safe equality

`IS NOT DISTINCT FROM` and equality expanded as `a.k = b.k OR (a.k IS NULL AND b.k IS NULL)`
can match two NULL keys. A NULL in any ordinary `=` key still prevents a match. NULL-bearing
keys keep the same BinaryRow encoding and key-group assignment on both sides; only the
per-key match filter changes. Residual predicates and duplicate match counts retain their
existing behavior. The policy travels with the operator through raw and RocksDB checkpoint
restore, including transitions between the two state backends.

Flink may decorrelate a null-safe EXISTS/NOT EXISTS query into INNER/outer joins and aggregation.
Those plans are admitted under the same rules; native coverage is checked on the actual physical
plan. Differential tests cover STRING, INT/BIGINT, DECIMAL and TIMESTAMP(9), mixed keys,
residual predicates, duplicate matches and changelog updates/deletes at parallelism 2.

## Mini-batch coalescing

Equality joins canonicalize floating NaN elements inside ARRAY keys, matching Flink's binary
array serializer. Different NaN payloads therefore match across inputs. Scalar floating keys
retain their existing raw-bit equality, and signed-zero array elements remain distinct.

Under mini-batch execution, a regular INNER/outer join uses one shared count boundary across both
inputs and drains before either input watermark, a checkpoint, or end of input, matching Flink's
two-input bundle contract. Two input shapes are native:

- For two insert-only inputs, the operator retains the physical Arrow batches by reference and
  replays the complete right-side bundle before the left side (left first for RIGHT joins). No row
  can be cancelled in an append-only bundle, so this preserves multiplicity without staging rows in
  an encoded changelog map.
- When planner metadata **proves both join keys contain an input upsert key**, replacement events
  are folded to the first preimage and final postimage per join key before replay.

Flink also reduces a changelog input whose upsert key is not contained in the join key, and cancels
equal opposing records for a changelog input with no unique key. Those two non-unique changelog
bundle shapes remain on StreamFusion's immediate path; they are not silently given the unique-key
contract. SEMI and ANTI joins also remain immediate, as in Flink's regular-join translation.

## Idle-state TTL

`table.exec.state.ttl` runs natively here, per side, with the same semantics as every other
TTL-bearing operator: each stored row carries its last-**write** wall-clock timestamp, expires at
`last_write + ttl` inclusive, and reads as absent (deleted on read) once expired. See [Idle-state
TTL](../index.md#idle-state-ttl) and [Configuration](../../configuration.md) for the flag surface.

## Falls back to Flink when

- the join type isn't one the native operator covers;
- there is no equi key and the join is not INNER over two insert-only, nonempty-payload inputs;
- the non-equi residual isn't expressible by the native expression engine;
- an input column has a type the Arrow converter or retained-row codec can't carry, including
  MAP/MULTISET at any nesting depth;
- `table.optimizer.delta-join.strategy` is `FORCE` and the optimizer block containing this join has
  no delta join — that block is left unchanged for Flink's later statement-wide validation, even
  if a delta join exists in another block. A block containing a delta join is not rejected by the
  `FORCE` guard; ordinary admission and island checks still apply, and `DeltaJoin` remains
  unsupported. See [Global switches](../index.md#global-switches).

## Null-safe join benchmark

A release/mimalloc run on September 16, 2026 (Apple M1 Max, JDK 17, Flink 2.2.1) joined
1,000,000 probe rows against 1,024 build keys, including NULL on both sides. Parallelism 1,
two warmups and five interleaved trials per engine; row sources, native join, both row/Arrow
transposes and a row blackhole sink are asserted in the measured plan. Median complete-job
time was **0.940s Flink / 0.469s native (2.00x)**. This measures the admitted INNER shape;
it does not establish the same speedup for SEMI/ANTI, outer, or retracting joins.

Run `SF_BENCHMARK=true mvn -pl streamfusion-runtime -am test -Pbench -Dtest=NullSafeJoinBenchmark`.

## Keyless join benchmark

`CrossJoinBenchmark` measures 100,000 left rows crossed with 16 right rows (1.6 million output
rows), parallelism 2 feeding the singleton join, row sources and a rowwise blackhole sink.
The native plan asserts the join, columnar exchange and both row/Arrow transposes. A local
release/mimalloc build against Flink 2.2.1, two warmups and five interleaved measured runs gave
medians of **0.225364s Flink / 0.248805s native (0.906x)**. This standalone shape is slower than
Flink. The initial admission adds verified composition with other native operators; it is not a
standalone throughput optimization.

Run `SF_BENCHMARK=true mvn -pl streamfusion-runtime -am test -Pbench -Dtest=CrossJoinBenchmark`.
