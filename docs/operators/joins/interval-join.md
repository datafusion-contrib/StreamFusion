# Interval join

**Status:** Partial. A time-bounded join — a `BETWEEN` predicate on rowtime or proctime instead of
(or alongside) an equi-key match window. Unlike the [regular join](regular-join.md), both inputs
must be insert-only; it is not one of the changelog-aware operators, so a retracting/updating input
falls it back per the [insert-only guard](../index.md#global-switches).

Both **event-time and proctime** bounds are native. An event-time interval join times rows by
rowtime and evicts on the watermark; a proctime interval join times rows by the processing clock and
evicts on a processing-time timer instead.

Interval membership uses **epoch milliseconds**, matching Flink's interval-join
runtime. It does not compare sub-millisecond fractions or scale the interval
into nanoseconds. A runtime TIMESTAMP(3) can still carry such a fraction: with
left time `2.000999999s`, right time `1.000000001s`, and inclusive bounds
`[-1s, +1s]`, Flink matches the pair. Native execution now does too. The timestamp
payload retains its original components; only the interval lookup uses milliseconds.
The direction of the lookup follows the arriving input, including Java long
arithmetic at overflowing bounds.

The native lookup accepts the default millisecond/fraction pair, the four primitive Arrow
timestamp units, and BIGINT milliseconds. Memory snapshots retain
the original payload schema and outer-join match flags; INNER restore does not
interpret a payload column as an outer-join row id. Timestamp payloads and keys preserve both
components across checkpoints, including wide dates and sub-millisecond fractions.

Incoming rows first probe retained opposite-side state. They enter the cache only while their
matching horizon remains ahead of the watermark (or processing clock); an unmatched outer row
whose horizon already closed emits its null padding immediately. The rowtime frontier starts
at zero and resets on recovery, matching Flink; restored cached rows retain their match flags.

Each key and input side retains its first registered cleanup timer. Later out-of-order arrivals
leave that deadline unchanged; when it fires, cleanup removes the expired rows and schedules the
next remaining timestamp. These timers survive native checkpoints, canonical savepoints, and
backend transitions independently of the row buffers. Incoming batches preserve per-arrival
ordering when combining matched pairs and immediate outer padding.

For zero or negative surviving epoch-millisecond timestamps, released Flink cleanup can depend
on the state backend's `MapState` iteration order: its negative timestamp sentinel can either
retain the remaining cache or clear it silently. Native memory and RocksDB execution both use
the released RocksDB cleanup behavior, which clears the remaining cache when its minimum
surviving timestamp is nonpositive. This keeps native behavior consistent across checkpoints
and backend transitions; it can differ from released heap state on pre-epoch timestamps. For
example, with bounds `[-100ms, +100ms]`, left timestamps `[-99, -1, 10]`, then watermark `2`,
released RocksDB and native execution clear the cache, while released heap state retains the
last two rows and can join them with a later right timestamp `-1`. Cleanup parity for this edge
case is defined against released RocksDB rather than heap state.

## Admission

Retained ROW/ARRAY/MAP payloads use the shared native RocksDB row codec. Qualified canonical
contracts pin their native read view and convert bounded frames asynchronously, without a whole
heap projection. Legacy blob-backed checkpoints migrate one key-group partition at a time,
preserving outer match flags and the processing deadline through aligned or clipped restore.
Partition-key and residual-expression admission remain independent of the payload codec.

Equi-keys use the [verified scalar whitelist](../window-aggregate.md#scalar-grouping-and-partition-keys):
all integer widths, FLOAT/DOUBLE, character, BOOLEAN, DATE, TIME, TIMESTAMP/LTZ, DECIMAL, binary and intervals,
including multiple scalar key columns. Collection/ROW/object key columns
retain fallback. Non-INNER joins require null-dropping keys, and the native expression engine
must express every non-equi residual. All four join types — INNER, LEFT, RIGHT, and FULL — are native.

## Falls back to Flink when

- the join type isn't INNER, LEFT, RIGHT, or FULL;
- there's no equi key;
- the key columns aren't null-dropping for a non-INNER join;
- the equi-key type is outside the supported set;
- the non-equi residual (the interval bound plus any extra condition) isn't expressible by the
  native expression engine.

## Cleanup configuration

`table.exec.interval-join.min-cleanup-interval` must be zero (the Flink default) or negative,
which Flink also treats as zero. A positive value keeps the join and its whole native island
on Flink: delaying cleanup changes how long late arrivals can match retained rows and when
unmatched outer rows are emitted relative to downstream watermarks. Native admission preserves
those configured semantics through fallback instead of silently using immediate cleanup.

## Output watermarks

Event-time joins advance their state cleanup using the original combined input watermark,
then delay the outgoing watermark by `max(-lower_bound, upper_bound)` milliseconds, matching
Flink. This keeps downstream event-time windows open while the join can still emit rows for
them. Zero delay forwards the watermark unchanged; processing-time joins also forward it
unchanged. Subtraction uses Flink's Java long arithmetic, including the delayed end-of-input
watermark, while the original terminal watermark still drains the join's state.

## Selective probes

Above 1,024 buffered rows on a side, the memory backend indexes that side by canonical equi-key
and gathers only rows for enabled incoming probe keys before joining. Smaller states use the
original full-batch join. Cleanup and restoration rebuild derived locators; checkpoint
frames retain their existing format. Index metadata counts toward the off-heap budget. Buffer and
timer accounting and cleanup rebuilds still inspect retained state; the RocksDB path is unchanged. See
[join measurements](../../optimizations/datafusion-hash-joins.md) for the measured boundary.
Fixed selective probes against 16,384 retained keys improved 2.84–9.20× in the native kernel.
The complete row-fed SQL workload demonstrated no full-job gain or stable regression: native
and stock medians moved similarly between runs. Both conversion boundaries, startup and task
budget accounting remain in that comparison; the kernel result does not predict SQL throughput.

## Direct canonical state transfer

The unmerged [#569 implementation](https://github.com/datafusion-contrib/StreamFusion/issues/569)
has a direct native transfer layer for event-time interval caches. It visits live rows in bounded
Arrow batches, preserves outer-join match flags and per-key/timestamp list order, and transfers
cleanup timers independently, including keys whose last cached row was already removed. Import
retains owned buffers or writes individual persistent records without probing, joining or emitting
input. No native snapshot is serialized before export. The key binding carries the actual stock
projection layout and timestamp precision. INNER match bits have no effect on future output; the
native memory representation normalizes those unused bits.

Four native controls cover both stores and transfer directions, continued joins and cleanup,
chunking, ownership, ordinary native recovery, malformed input and memory-denial cleanup. The
clean component passes 1,158 native tests with two existing ignores and 29 Java checks, including
18 direct JNI handoff/failure controls and 11 ordinary interval operator/watermark controls.

### Released stock event-time state adapter

The Flink 2.2.1 event-time adapter now writes the stock `IntervalJoinLeftCache` and
`IntervalJoinRightCache` maps. Their long timestamp buckets contain lists of `(RowData, Boolean)`
tuples with the original row serializer and outer-match flags. The two stock long value states
retain each side's actual cleanup deadline; their names refer to the opposite input. The shared
`user-timers` event-time queue and void namespaces remain stock-compatible. Timers whose cached
rows were eagerly removed transfer independently. Canonical export visits live native state into
the stock writer, without a native snapshot or input replay.

Import validates the complete descriptor/namespace inventory, both cache serializers, keys, row
timestamps/kinds, stored deadlines and callback ownership before mutating native rows. Unknown
state or pending processing-time callbacks fail closed. The event-time contract cannot bind to a
native processing-time operator. Scheduled and observed cleanup relationships now use the shared
scoped disk spool rather than job-sized Java sets, on both release lines and in export preflight.
Both sides can share one physical callback; reverse scans still reject callbacks without an owned
deadline. The configured spool write-buffer reservation and scratch files release on every exit.
Export stages tuple entries in the shared disk spool and lets the released list serializer stream
each timestamp bucket into the projection once, preserving peer order and match flags. Restore
reads released map/list bytes directly from snapshot resources and decodes individual tuples in
both validation and apply passes, without materializing a complete Java bucket list. Row and timer
transfers share a 1,024-row and four-MiB payload estimate, allowing one oversized value. The largest
encoded stock KV remains a working-memory floor. The same transport applies to event-time and
processing-time contracts on both release lines. The original stock row/key/timer serializer snapshots load with
StreamFusion and planner classes blocked.

An isolated component passes 240 positive controls and 45 rejection/guard controls. These cover
both native stores and switch directions, immediate re-export, timer-only and empty state, NULL
payloads/keys, all four join kinds and BIGINT or compact TIMESTAMP event-time payloads. A separate
144-case typed-key cohort also resumes ordinary native checkpoints before continuation, covering
BIGINT, INT, VARCHAR, DECIMAL(38,2), compact/noncompact TIMESTAMP, BOOLEAN, DOUBLE and binary keys.
The combined component/regression run passes 618 Java controls; shared Flink 1.18 operator tests
pass without claiming stock canonical qualification for that line. These cohorts overlap.

The stock compiled-plan loader now binds this event-time state contract automatically, using the
original stock serializer/UID and resolved maximum parallelism for both exchanges and the native
state owner. The portable JSON remains stock-only. Twenty-two planner controls and two maximum
parallelism controls cover the binding and unsupported-mode boundaries.

All 26 isolated complete-job cases pass: 16 continued-input switches across four join kinds,
both stores and engine orders; four no-input re-exports; two ordinary native-checkpoint recoveries;
and four FULL-join 64-key rescale cases through 1 → 3 → 2 tasks at maximum parallelism 128. They
retain two custom sources, a transactional sink and both transposes, require actual native input,
exclude StreamFusion from stock workers, and match committed sink effects to a stock baseline.
The combined clean workflow passes 198 controls, including four shared-runner window-join checks.

The unchanged released event-time and processing-time interval programs each import ten pending
rows and consume five new native rows on restore. The three interval and eight window programs
all pass on both engines. Eight programs execute native state owners; two window SEMI/ANTI paths
retain verified stock owners. The remaining impossible-interval LEFT join is already stateless
in Flink: its pinned contract proves a complete submitted graph without a join/CoProcess owner,
no state import or fallback, and actual native Calc work. Its upstream assertions still verify
the null-padded output. This is graph elision, not native interval-join coverage.

Ordinary SQL identities, wider type/version/failure coverage, additional rescale shapes and
connector delivery protocols remain open. Nonpositive/overflow event-time cleanup remains an explicitly unqualified interchange edge;
the released heap/Rocks distinction described above still applies. Benchmarking is deferred.

### Logical input and retained native frames

Interval and window joins share the insert-only input normalization boundary. It retains the
declared logical fields, validates and removes trailing INSERT row-kind/string-cache transport
companions, and rejects retractions or unknown companions before changing state or clock
frontiers. A logical user field with a reserved transport name remains a user field by position.

Older native interval IPC frames normalize the same companions during memory restore and
persistent import while retaining outer row identities and match flags. The native snapshot
framing and independent timer sections are unchanged. Three new native regressions cover
transport input, rejection without mutation, and older tagged frames on both restore routes.
The isolated component passes 1,161 native tests with two existing ignores and 618 Java
component/regression controls. Canonical benchmarking remains deferred.

### Released stock processing-time state adapter

The processing-time adapter preserves SQL rows independently of the cache's arrival clock. Native
state appends a private BIGINT clock column and projects it out of joined output; canonical export
writes only the original payload and matched flag into the stock tuple while using the clock for
its timestamp bucket. PROCTIME's physical NULL placeholder survives even when its logical type
is NOT NULL. Residual expressions still reference their original logical fields.

The stock processing `user-timers` queue transfers independent cleanup deadlines, including
keys with no cached rows. The opposite event queue must be empty. Both adapters reject a native
operator with a different clock mode before importing any rows. Imported deadlines, including
already-due timers, rearm without new input; native checkpoint recovery retains the same behavior.
End of input does not advance the processing clock to infinity or emit premature outer padding.

The isolated controlled-clock component passes 208 positive cases and 17 rejection/guard cases,
covering both stores/orders, four join kinds, nine scalar key layouts, original/NULL payloads,
no-input re-export, timer-only keys, overdue callbacks, EOF and native checkpoint continuation.
The combined clean component/regression selection passes 843 Java controls, while the clean native
suite passes 1,164 tests with two existing ignores. These counts overlap earlier cohorts.
The stock compiled-plan loader automatically binds either clock mode to its matching native
adapter, preserving the original UID, serializer and resolved maximum parallelism. The clean
planner/regression suite passes 175 checks, including 29 interval plan controls. Expanded
complete-job switching is qualified below; the full interoperability matrix remains open.
Canonical export remains a direct live-state transfer. Benchmarking is deferred.

### Complete-job handoff matrix

All 84 clean-process interval job cases pass against uninterrupted stock baselines. The matrix
contains 32 continued-input switches and 32 residual-predicate switches across both clock modes,
four join kinds, both native stores and both engine orders; eight FULL-join no-input re-exports;
four INNER-join native checkpoint recoveries; and eight FULL-join 64-key rescale cases through
1 → 3 → 2 tasks at maximum parallelism 128. The checkpoint cases trigger a real checkpoint,
cancel, restore and then export a canonical savepoint. Native execution is verified on every
claimed continued-input native leg. Each job retains two custom sources, a transactional sink
and both transposes; stock processes exclude StreamFusion, portable metadata contains no
StreamFusion serializer classes and committed sink effects match stock.

Processing-time complete jobs use a one-day matching range so process startup does not determine
expiry. The separate controlled-clock cases verify cleanup at exact deadlines, original/NULL
payloads, idle/overdue callbacks and EOF. The combined clean job/control selection passes 457
checks with no failures, errors or skips. These overlap the component and planner cohorts.
Wider type/version/failure and connector-protocol requirements remain open
in [#569](https://github.com/datafusion-contrib/StreamFusion/issues/569). Benchmarking is deferred.


Qualified compiled plans can disable explicit SQL UID generation. All 84 existing generated-ID
job cases pass: 32 continued-input switches, 32 residual-predicate cases, eight no-input
re-exports, four native checkpoint recoveries and eight 64-key rescale cases through 1 → 3 → 2
tasks. These cover both clocks, native stores and initial engine orders. The runtime state
contract uses the actual native primary ID and canonical metadata retains the original stock
identities automatically. Wider type/version/failure requirements remain open. Ordinary SQL
binding is described below
under [#569](https://github.com/datafusion-contrib/StreamFusion/issues/569); see
[generated operator IDs](../../backends/canonical-state.md#generated-operator-ids-in-qualified-compiled-plans).

Generated-ID rescale fixtures disable chaining before the first savepoint, matching the
ordinary-SQL fixture. Flink's generated IDs depend on chain boundaries; a singleton source and
1 → 3 → 2 downstream tasks otherwise change those IDs even for stock-to-stock restore. Both
clocks, native stores and initial engine orders retain the same 64 keys and maximum parallelism
128. This setup preserves the existing unmatched-state rejection; it neither invents identities
nor drops state. The full existing generated-ID job matrix also retains its continuation,
residual, no-input and native-checkpoint controls.



### Ordinary SQL submission

Normal SQL submission now retains the original stock logical identity and binds this join's
stock state contract without compiling or loading a plan. The initial ordinary SQL qualification
and exact key-layout/admission limits are documented in
[ordinary SQL canonical handoffs](../../backends/canonical-state.md#ordinary-sql-canonical-handoffs).
This does not qualify the full ordinary variant/type/rescale/failure/connector matrix; the family
remains partial under [#569](https://github.com/datafusion-contrib/StreamFusion/issues/569).
