# Deduplication

**Status:** Native — all four variants (rowtime/proctime × keep-first/keep-last), mini-batch
included, idle-state TTL included.

Flink recognizes `ROW_NUMBER() OVER (PARTITION BY key ORDER BY time_col ASC|DESC) = 1` as its
`Deduplicate` operator rather than a general rank filter whenever the order key is exactly one time
column (rowtime or `PROCTIME()`) and the rank is 1. A rank-1 filter ordered by anything else is a
value-ordered dedup, which Flink plans — and StreamFusion runs — as [Top-N](top-n.md) instead.

## The four variants

- **Rowtime keep-first** (`ORDER BY rowtime ASC`) — insert-only, watermark-released: a key's first
  row is held until the watermark passes it, then emitted once.
- **Rowtime keep-last** (`ORDER BY rowtime DESC`) — retracting: each later row for a key replaces
  the previous emission.
- **Proctime keep-first** / **keep-last** — arrival order, no watermark involved. The order key
  itself is materialized by the native `PROCTIME()` expression, so both proctime shapes run as
  ordinary time-ordered dedup over that generated column.

Rowtime keep-first emits at the watermark; the other variants emit eagerly. Proctime dedup needs
no wall-clock timer, unlike the windowed operators that fire on a processing-time clock.

## Mini-batch

All four mini-batch variants validate `table.exec.mini-batch.size > 0` during translation,
with Flink's exception and message. This includes proctime keep-first even though its
native implementation emits eagerly. Disabled mini-batching leaves the unused size alone.

Every mini-batch shape replicates Flink's per-mode emission exactly:

- Under mini-batch, a **rowtime** dedup — keep-first included — becomes Flink's bundled retracting
  function. Its flush emits every kept row's transition by default, or one net transition per key
  per bundle under `table.exec.deduplicate.mini-batch.compact-changes-enabled`.
- A **proctime** flush emits one net transition per key.

## Insert-sensitivity

`table.exec.deduplicate.insert-update-after-sensitive-enabled` (default `true`) is replicated too.
With the option off, under a consumer that requests only `UPDATE_AFTER` (an upsert sink), every
emission becomes a bare `+U` — a fresh key's first row included — and the proctime identical-row
suppression is disabled, exactly as Flink's own helpers behave.

## Idle-state TTL

Idle-state TTL runs natively here — see [TTL semantics](index.md#idle-state-ttl) and
[Configuration](../configuration.md) for the flag surface. One shape needs a wrinkle beyond the
standard last-write-timestamp rule: the watermark-buffered **rowtime keep-first** dedup TTLs only
its emitted *markers*. The buffered candidate row itself mirrors Flink's deliberately un-TTL'd timer
state — the watermark is what cleans it up, and expiring it early would lose data. The marker,
written once when a key fires and never refreshed afterward, expires a fixed retention after that
firing — which is what lets the key emit a second "first" row once its earlier marker has aged out.

## Gap

A rank-1 filter that is not time-ordered — i.e. ordered by a value column rather than rowtime or
`PROCTIME()` — is not a fallback for Deduplication; it is a different query shape, handled by
[Top-N](top-n.md).

Processing-time ascending ranks beyond rank 1 are also handled by
[Top-N's first-N counter](top-n.md#processing-time-first-n). They preserve arrival order and
do not change this operator's rank-1 fast path.

## Pending keep-first state

Memory rowtime keep-first reduces small candidate sets in one contiguous Arrow batch. Above
1,024 winners it promotes immediately to an index of minimum-time rows in retained Arrow chunks.
An arrival batch probes its own keys without copying or reducing all standing candidates. Equal
timestamps retain the incumbent; a strictly earlier replacement takes that arrival's output position.
Watermark and checkpoint boundaries gather live winners in this order. Fully replaced chunks release
immediately, and half-empty chunks compact independently. A few replaced wide values can retain
buffers below that row threshold; if the off-heap budget rejects growth, sparse chunks compact
before the reservation is retried. Live state that still exceeds the budget fails normally. The
budget includes retained Arrow buffers and index metadata. Snapshots retain the existing row-batch format; restoration indexes
that batch lazily. Persistent keep-first continues to use RocksDB point lookups.

See [pending-state measurements](../optimizations/pending-dedup-index.md#measurement) for the
production arrival boundary and complete stock-versus-native SQL comparison.
The measured 16,384-key delayed-watermark workload improved 22.33–321.75× in arrival kernels
and 1.82× in complete native SQL jobs; the candidate's same-run stock comparison was 1.63×.
These are bounded workload measurements with startup and both transposes retained in the SQL
comparison, not a promise for small state, checkpoints, or the RocksDB path.

Keep-last row-state decoding restores the exact nested Arrow field names and metadata
before emitting a batch, using the same schema alignment as Top-N. This includes cached
UTF-16 string representations and retained values after checkpoint restoration; changing a
list child name during Arrow row decoding must not invalidate the output schema.


## Canonical state adapter

Eager value-state exports can use a pinned native RocksDB snapshot with asynchronous stock
serialization. Memory exports capture independent native state and convert asynchronously through a
temporary stock backend. Planner binding and focused state
interchange controls also run on Flink 1.18.1. Buffered, window and other timer-bearing layouts
retain separate projection and qualification requirements. This does not qualify every 1.18
emission variant or cross-version upgrades.

Direct Flink 2.2.1 eager-state conversion is partial. Processing-time keep-first transfers a
Boolean presence marker; processing-time keep-last and eager event-time keep modes transfer
the retained full row, stock serializers, actual key and original TTL age. The proctime
suppression kind survives conversion. Memory and typed RocksDB export live state directly,
without a serialized native snapshot. Whole-view validation precedes insertion into a fresh target.

All 66 direct controls pass across the four eager modes, both native stores, TTL on/off and
nullable/NOT NULL/keyless keys, with explicit expiry, late-corruption and PROCTIME-placeholder
checks. Thirty-two existing processing/checkpoint controls also pass on each Flink line; this
does not qualify stock canonical interchange on 1.18. The full native suite passes 1,216 tests with two existing ignores.

Ordinary SQL and portable stock compiled plans now bind eager canonical state automatically.
The binding captures the actual released sync/bundle function, emission flags, TTL, key serializer,
operator identity and resolved maximum parallelism. Eager versus watermark-buffered keep-first is
selected from the stock function; Boolean presence is used only for processing-time first-row state.
Processing-time bare-update mode registers Flink's row-state schema but exports no retained rows:
its outputs do not depend on previous rows. Restore rejects unexpected saved rows before native
insertion. Buffered rowtime-first uses its distinct candidate/timer contract described below.

Qualification includes 28 eager plan checks and 152 isolated-process job scenarios (76 per
submission form) on frozen binding commit `1ec00607`. They cover both native stores and switch
orders; proctime first/last and eager rowtime last; mini-batch proctime modes and rowtime first/last;
full-chain/compact rowtime bundles; long TTL; and keyless jobs. Empty state, repeated no-input
re-export, native checkpoint recovery and fixed-maxP128 rescaling through 1→3→2 tasks are checked
for both marker and row state. Rescale fixtures keep connectors at one task and disable chaining
from the initial graph. The custom source and transactional sink preserve state, both transposes
execute, pending native bundles flush at canonical barriers, and effects match an independent
winner oracle and uninterrupted stock Flink. Stock classpaths and metadata contain no StreamFusion
runtime types. The two released eager programs each import two rows and consume four new native
rows. All 374 checks in the frozen cohort pass. Sixteen eager-switch cases also pass after the
subsequent empty-state/sink-helper changes; overlapping counts are not cumulative.

Remaining buffered qualification, emission/type variants, old graph/backend/version upgrades
and the complete connector/failure matrix remain open in
[#569](https://github.com/datafusion-contrib/StreamFusion/issues/569). Benchmarking is deferred.


Bare-update canonical qualification adds 16 direct controls (both native stores, TTL on/off,
nullable/NOT NULL/keyless keys and rejection of unexpected retained rows), eight plan checks and
32 isolated-process jobs across both submission forms, stores, switch orders, mini-batch and TTL
settings. A custom upsert sink selects the released false/false emission flags. Immediate jobs
commit exactly one `+U` per input, including identical duplicates; bundled jobs preserve final
per-key upsert effects across savepoint flushes. Both forms check stock metadata/classpaths,
connector state and both transposes. The same process cohort passes 16 eager-switch regressions
and 32 existing processing/checkpoint controls. These overlapping counts do not qualify the full
schema/backend/version/failure matrix or the separate buffered rowtime-first path.


### Buffered event-time first-row conversion

Ordinary SQL and stock compiled plans bind Flink 2.2.1 waiting rows, emitted Boolean markers and
per-key event timers directly. Pending rows remain exempt from TTL; markers keep their original
write ages. Memory/RocksDB conversion uses live state, without a native snapshot or input replay.
Whole-view validation rejects inconsistent timers, keys, values and overlapping states before
insertion; imported stock entries clear only after success. Arrow transport companions are
validated and removed before buffering, and keyless markers survive native checkpoint recovery.

Buffered qualification includes 30 continued-state/recovery cases, 25 direct corruption/age
controls, 12 ordinary/compiled binding checks and 96 complete jobs across partitioned cohorts.
Both submission forms/stores/orders cover continued input, long TTL, nullable/NOT NULL/keyless
keys, empty state, repeated no-input returns, native checkpoints and fixed-maximum rescaling.
Custom connectors, transactional effects, both transposes, clean stock dependencies and independent
watermark/winner results are checked. Retained-winner cases prove partial transpose flushing and
state preservation across both switches, plus late/equal-watermark handling. Fixture/build failures remain archived and receive no
qualification credit; corrected affected cases pass. Wider version, variant, connector and failure
coverage remains open; consult [buffered canonical coverage](../backends/canonical-state.md#buffered-event-time-first-row-state).
Benchmarking remains deferred.


Forty-eight actual-operator controls additionally retain nested payloads across twelve scalar,
ROW, ARRAY and composite key layouts, including non-leading root key columns. Both TTL settings
preserve an original pending winner through both switches and reject newer replacements.
Continued serialized output matches stock byte for byte. These schemas test memory and
RocksDB's snapshot-backed native state; they do not claim direct typed RocksDB coverage for
nested payloads. Canonical conversion is guarded against intermediate native snapshots.


## Window deduplication canonical interchange (partial)

Normal SQL and portable stock compiled plans bind released Flink 2.2.1 event-time window
deduplication for TUMBLE/HOP/CUMULATE. The actual processor and combiner supply the input serializer,
time zone, rowtime column and first/last policy. The original key serializer, identity and maximum
parallelism accompany the runtime owner. Async/unaligned event-time state retains stock ownership. Explicit UIDs are optional; portable JSON remains stock-compatible.

Direct live export writes the released `window_deduplicate` VALUE row under its Long window-end
namespace, event timers and UNION watermark. Keep-first preserves the earliest arrival on equal
time; keep-last replaces it. Both policy and calendar configuration precede restore-time import.
The input watermark survives canonical and ordinary native checkpoints, including an empty
state and immediate canonical re-export. Complete source validation precedes row installation,
and source state remains until installation succeeds.

The shared 425-check cohort includes 152 dedup controls across both stores, keyed/global state,
three shapes, four zones, first/last ties, watermark/late-input recovery and malformed state/timers.
The separate 384-check planner cohort also covers both submission forms and UID policies.
See [window rank interchange](top-n.md#window-rank-canonical-interchange-partial) for the shared
protocol and qualification boundaries. Broader types, resource/receiver failures, full connector
delivery and cross-version qualification remain in
[#569](https://github.com/datafusion-contrib/StreamFusion/issues/569). Benchmarking is deferred.


All eight unchanged released WindowDeduplicateEventTimeRestoreTest programs pass on each engine.
Every native-side invocation requires nonempty canonical winner import and subsequent native
input, covering selected keyed/global first/last TUMBLE/HOP/CUMULATE programs with the original
released assertions. The full paired rank/dedup selection is fourteen invocations per engine;
these are separate from the shared operator/planner cohorts. A clean shared Flink 1.18.1 run
passes 27 processing/checkpoint controls without claiming canonical interchange on that line.


The separate 56-job matrix includes 28 keyed TUMBLE first-row dedup scenarios with BIGINT payloads:
eight continued-input, eight no-input, four native-checkpoint recovery and eight 64-key rescale
cases. It crosses submission forms, stores and engine orders (checkpoint starts native), retains
both transposes/custom stock source/committer state, and compares full external effects with stock
and an independent winner oracle. Source/sink remain singleton while the keyed owner rescales
1→3→2 at maxP128. Last-row dedup and HOP/CUMULATE recovery/rescaling remain outside that job matrix.
A combined 2,374-check regression cohort passes; its controls overlap the earlier cohorts.
Processing-time native window dedup has no released stock counterpart and receives no stock
canonical qualification. Benchmarking remains deferred.

Processing-time ordinary SQL retains its existing native owner rather than passing through the
released stock translator, which rejects that shape. Six dedicated keyed/global rank/first/last
planner controls check actual owner routing and the stock rejection separately.
