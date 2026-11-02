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
native processing-time operator. The original stock row/key/timer serializer snapshots load with
StreamFusion and planner classes blocked.

An isolated component passes 240 positive controls and 45 rejection/guard controls. These cover
both native stores and switch directions, immediate re-export, timer-only and empty state, NULL
payloads/keys, all four join kinds and BIGINT or compact TIMESTAMP event-time payloads. A separate
144-case typed-key cohort also resumes ordinary native checkpoints before continuation, covering
BIGINT, INT, VARCHAR, DECIMAL(38,2), compact/noncompact TIMESTAMP, BOOLEAN, DOUBLE and binary keys.
The combined component/regression run passes 618 Java controls; shared Flink 1.18 operator tests
pass without claiming stock canonical qualification for that line. These cohorts overlap.

Binding is currently explicit at the component boundary. Automatic compiled-plan binding,
unchanged upstream interval restore programs, clean-classpath complete jobs, residual predicates,
processing-time payload preservation and the wider type/version/rescale/failure matrix remain
open. Nonpositive/overflow event-time cleanup remains an explicitly unqualified interchange edge;
the released heap/Rocks distinction described above still applies. Benchmarking is deferred.
