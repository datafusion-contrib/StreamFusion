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
- every input column type must be one the Arrow converter can carry. MAP and MULTISET payloads,
  including nested ARRAY/ROW/map descendants, use the tagged List-of-entry-struct view already
  used for IPC-safe map transport. NULL parents, keys and values remain distinct from empties.
  MAP-bearing inputs must be insert-only. Updating such an input needs Flink's retained-row
  representation and retraction contracts ([#437](https://github.com/datafusion-contrib/StreamFusion/issues/437)).
  Join-key fields still exclude MAP/MULTISET descendants.

Insert-only inputs may carry an explicit INSERT `$row_kind$` sidecar from an upstream
native join. Mini-batch buffering uses the planner's join-key uniqueness proof, rather
than sidecar presence, to preserve every row on a non-unique input. This fixes chained
append-only joins such as Nexmark q23; repeated keys remain independent rows across
bundle boundaries. Non-unique mini-batch inputs reject non-INSERT sidecars as a violated
admission contract. Updating non-unique inputs continue to execute immediately.

## Retraction identity

When the planner proves that a side's join key contains a unique key, retained state has Flink's
ValueState identity: UPDATE_BEFORE or DELETE clears the current row at that join key, without
comparing the incoming payload. A retraction with a changed non-key value must not leave an old
row available for a later match. The rule applies to INNER, LEFT/RIGHT/FULL, SEMI and ANTI joins
on both sides. Non-unique sides retain full-row equality and duplicate occurrence counts.

Heap state releases the complete unique-key entry and its memory reservation. Direct RocksDB
INNER state deletes that key's record prefix, including earlier points in the current batch;
whole-bucket outer/SEMI/ANTI state uses the same unique-key clearing rule. The existing key and
checkpoint encodings are unchanged. This fixes retained state behavior within existing admission;
updating MAP payloads retain the independent restriction above until their complete contracts
are qualified in [#437](https://github.com/datafusion-contrib/StreamFusion/issues/437).

The [released-operator evidence](../../benchmarks/results/unique-join-retractions-2026-10-20.json)
records 24 unique-key failures in each preceding 48-case heap baseline. The oracle
declares column zero as the unique key, matching its keyed input partition. All 96 heap/Rocks
comparisons now pass on both released lines. Existing Java/SQL regressions total 141 passes on
2.2 and 140 with one host-capability skip on 1.18; 106 native join tests pass with two existing
ignores. The 32 native state fixtures are correctness smokes, not throughput comparisons.

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
memory budget; it has no separate state-cardinality cap or spillable candidate-pair buffer. A cross
product can emit `left_count * right_count` rows. Candidate generation and residual filtering use
the bounded INNER output chunks described below. The RocksDB backend does not remove the total
per-bucket matching work.
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

## Bounded INNER output

INNER joins gather at most 4,096 candidate pairs or approximately 8 MiB of estimated decoding and
filtering work per chunk. The estimate includes both payload rows, offsets, null masks and room
for the filtered result. A single oversized pair is processed alone only if its reservation fits
the task budget. Reservations grow before candidate buffers and decoding allocations; budget
pressure can trigger an earlier flush, and a pair that cannot fit fails with the native memory
limit exception. These are internal physical chunk limits, not Flink logical mini-batch settings.

Each chunk is filtered and synchronously handed through JNI to the downstream Arrow consumer.
Immediate input and mini-batch flush use the same streaming output path; they never concatenate
all matching output into one batch. Candidate reservations are released on error, cancellation
unwind and completion, while imported output buffers belong to the downstream Arrow allocator.
Row kinds, duplicate multiplicities, TTL, state layout and checkpoint boundaries are unchanged.

This bounds candidate expansion, not total retained join state or arbitrary residual-UDF scratch
memory. Input payload encoding and RocksDB bucket hydration remain batch-level operations.
Outer, SEMI and ANTI joins retain their existing output construction.

## Mini-batch coalescing

With mini-batching enabled, translation preserves the selected Flink release's validation
of `table.exec.mini-batch.size`. Flink 2.2 rejects unset, zero and negative sizes for
INNER/LEFT/RIGHT/FULL joins, including updating non-unique inputs for which the native
runtime executes immediately. Flink 1.18 joins do not construct a mini-batch trigger and
therefore do not validate this unused size. SEMI/ANTI joins and mini-batch-disabled joins
also retain the host's behavior. Invalid sizes fail during planning with the host exception
and message; the internal timer-only boundary sentinel remains unchanged.

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
- an input column has a type the Arrow converter cannot carry, or an equi-key contains
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

### Immediate inner-join disk state

When mini-batching is disabled, RocksDB inner joins store individual rows and their
counts. Input updates use point reads; opposite-side probes use a memory-accounted,
per-bundle cache or a lazy iterator with bounded output chunks. This preserves
retractions, unique-key replacement, null-key policies, and independent per-side
write-based TTL without hydrating both full histories on each batch.

Old bucket checkpoints migrate lazily. Canonical savepoints retain their existing
logical representation for memory/disk transitions and key-group rescaling. Native
checkpoints require a reader supporting the record format and the same join kind
and mini-batching configuration on restore.
Other join families and mini-batched inner joins continue to use bucket state.


The controlled [record-layout and probe-reuse measurements](../../optimizations/rocksdb-write-through.md#separating-record-layout-from-probe-reuse-2026-09-30)
show that bounded reuse is a memory safeguard, not a universal speedup. Unique-key
inputs favored the old bucket store in the state-level diagnostic; groups larger
than the former 8 MiB allowance paid repeated lazy-scan costs.
That fixed ceiling has been removed; probe reuse is governed by reservations
from the shared native memory pool. Immediate inner joins
still persist only changed records, independent of whether probe reuse fits.

### Upsert-key metadata during substitution

Join input uniqueness is resolved against the original Flink physical plan, before its children
are replaced by native nodes. A native source or Calc must not erase a proven upsert key: when
the join key contains that key, a later update replaces the stored row. Treating it as multiset
state can re-emit obsolete values when another input changes, including overwriting a newer
value in a Paimon partial-update sink. Non-unique inputs retain multiset state.


## MAP/MULTISET retained payloads

Eight scalar/nested map and multiset payload families pass inner/outer/semi/anti SQL comparisons.
Memory and RocksDB recovery cover two failures. Updating/deleting/reinserting MAP-bearing
inputs retain verified stock fallback, including equivalent maps with colliding keys reordered
on retraction. Those reordered inputs exposed a native retained-row mismatch before submission.
Full payloads and result types are compared; outer and independently interleaved changelogs
compare final multiplicities.
Map-list output reaching unverified native Arrow sinks uses the existing row-sink route.
[Shared evidence](../../benchmarks/results/retained-map-payloads-2026-10-20.json) includes the
production Top-N small-function gate, which makes no join-throughput claim.

Regular join state materializes represented scalar strings to the declared UTF-8 payload,
matching Flink's binary-row serialization boundary. UTF-16 caches remain outside persisted
join payloads, and parent NULLs remain NULL. Immediate, minibatch and legacy decoded-restore
paths use the same conversion before row encoding; the declared output schema and outer-join
NULL padding remain unchanged.


## Direct stock canonical state adapter

An explicitly bound Flink 2.2.1 adapter projects the native join's actual left/right retained
rows, multiplicities, association degrees and original TTL write timestamps into released stock
state descriptors and serializers. It handles independent scalar, unique-key map and occurrence
map layouts. Both input sides and every transfer batch validate before any native insertion;
a malformed right side cannot leave valid left rows imported. Native export visits live state,
including indexed RocksDB records and legacy buckets, without an intermediate native snapshot.

The clean direct-operator component passes 78 Java controls across six join kinds, three side
shapes, keyed/keyless paths and both native stores. It covers continued stock/native execution,
multi-batch transfer, original TTL ages and invalid state, including forced late-batch rejection.
These BIGINT fixtures use stock heap projections. Automatic binding is described below;
complete job/connector/type/backend/version/rescale/failure qualification remains open in
[#569](https://github.com/datafusion-contrib/StreamFusion/issues/569). Existing native processing
and checkpoints retain their paths. Benchmarking is deferred.


### Automatic planner binding

The deployed Flink 2.2.1 planner binds this adapter for qualified stock compiled plans and ordinary
SQL jobs, preserving stock side schemas, unique-key selectors, serializers, retention, actual
operator identities and resolved/default maximum parallelism. Ordinary SQL retains its existing
native mini-batch/input mode. Explicit UIDs and manual mappings are unnecessary. Native filters
and mini-batch assigners retain original logical roles in the metadata projection. Shared stock
calculation, filter and watermark roles retain one execution owner, with Arrow reference-count
sharing rebuilt for their actual consumers.

Fifty-six planner controls and four initial clean-process INNER multiset round trips pass across
both stores/submission forms. The jobs retain custom changelog sources, the transactional sink
and both transposes, and compare external materialized results with stock and an independent
oracle. All 15 unchanged released regular-join restore programs pass per engine with actual native
state imports; 14 also process new join input, and the literal-aggregate fixture verifies restored
native execution. Eight additional shared-calculation round trips pass across both engine orders,
both native stores and both submission forms. All 164 scenarios in the frozen regular-binding job
matrix also pass: 144 continued-input handoffs, eight no-input re-exports, four native checkpoint
recoveries and eight rescalings at 1→3→2/maxP128. The later shared-role fix has separate job and
planner qualification.

Constructed retained payloads, unaligned exchange reassembly, async/changelog state backends and
unqualified ordinary wide keys are outside this binding scope. Existing native processing and
checkpoints retain their paths. Complete variant/type/backend/version/TTL/failure/connector and
broader rescale qualification remains open under [#569](https://github.com/datafusion-contrib/StreamFusion/issues/569).
Benchmarking stays deferred. See [automatic regular binding](../../backends/canonical-state.md#automatic-regular-join-binding).


Keyless canonical binding preserves the stock singleton/empty-row state context and maximum
parallelism one. Compiled-plan loading retains INNER and the verified keyless ANTI path with
payload columns and original source changelogs proven insert-only through known row-preserving
stages. Four additional process cases cover nullable-left `NOT IN`, pre-savepoint unmatched rows
retracted by later right input, both native stores and both engine orders. The released ANTI
restore fixture also requires actual native execution. Other outer/updating keyless joins retain
stock owners; ordinary native matching still uses the keyless INNER admission described above.


Eight keyless INNER clean-process round trips pass across both engine orders, native stores and
submission forms. They retain duplicate rows and NULL payload keys, and check the stock singleton
state, external committed effects and an independent nested-loop oracle. The planner selection
passes 56 controls, including keyless INNER binding and preserved stock owners for unsupported
keyless outer/updating inputs.

Mini-batch FULL joins with a changing residual predicate remain unqualified. In a new eight-case
job cohort, every switched result matches the independent oracle, but three uninterrupted stock
baselines omit an unmatched opposite row. A deterministic released-stock-only control reproduces
this: after a matched left value changes from 10 to 15 under a `MOD(left,3)<>0` residual,
immediate FULL join emits both unmatched rows, while mini-batch suppression omits the right row.
Both stock behavior controls pass; they provide no native interoperability credit. The discrepant
job evidence is retained, and stock-baseline assertions have not been relaxed. Complete mini-batch
qualification remains open in [#569](https://github.com/datafusion-contrib/StreamFusion/issues/569).


When both inputs reuse the same first-N subplan, ordinary canonical binding retains one
upstream counter owner and the join's own independent state owner. Two different rank limits
retain two counters. Batch sharing is rebuilt around those owners. All 32 compiled/ordinary cross-engine job scenarios pass across both stores/orders, including
no-input re-export and rescaling at 1→3→2/maxP128. Both families prove new native input after
the barrier, including downstream rows initially pending in a partial transpose. Wider qualification remains in
[#569](https://github.com/datafusion-contrib/StreamFusion/issues/569).


Canonical normalizer/join handoffs now preserve upsert input whose join value differs from its
primary key. The upstream full-row state supplies the old join value for updates and key-only
tombstones; both state owners bind to their stock identities. The 52 compiled/ordinary process
scenarios cover both stores/orders, mini-batch barriers, no-input returns, native checkpoints,
empty state and fixed-max rescaling. Other unqualified upstream native families remain part of
[#569](https://github.com/datafusion-contrib/StreamFusion/issues/569).
