# Event-time sort

**Status:** Native — `ORDER BY` on a single leading ascending rowtime; a secondary order key is
the one gap.

This is Flink's temporal `Sort`: a full re-ordering of the stream by rowtime, needed because
arbitrary-lateness elements can only be placed once the watermark has passed them, so the operator
must hold and globally order a buffer rather than emit in arrival order.

The admitted shape is exactly Flink's own: the leading order key must be an ascending rowtime. A
descending or non-time leading key isn't a fallback at all — it's a non-temporal `Sort`, which
Flink itself rejects in streaming, so declining it here is parity, not a gap.

## Gap

Any **secondary** order key beyond that leading ascending rowtime falls back.

## Late rows

After emitting rows, the sorter drops subsequent arrivals at or before the last emitted
timestamp. An empty watermark does not advance this cutoff: a row behind the watermark can
still be admitted if it is newer than the last emitted row. Rows tied at a timestamp and
buffered before emission are all retained, in arrival order. This matches Flink's temporal sort.

## State layout

Because a full time order can't be partitioned by any real key, the buffer is raw keyed state
addressed under Flink's own one canonical empty key. That makes it checkpointable and restorable
exactly as Flink restores its own temporal sort, but it deliberately **cannot shard across
subtasks** — the same singleton limitation Flink's implementation has. See
[#22](https://github.com/datafusion-contrib/StreamFusion/issues/22) for that constraint.

The last emitted timestamp survives checkpoints even when the pending buffer is empty.
Native memory checkpoints and native-format savepoints include it in a versioned prefix before
the pending Arrow IPC data; legacy snapshots containing only IPC remain readable. RocksDB
checkpoints retain it as reserved metadata. Recovery and memory/RocksDB backend transitions
preserve both the late-row cutoff and any pending rows.


## Stock-compatible canonical interchange

The #569 integration adapter exports directly to released Flink 2.2.1 `dataState` timestamp buckets
of full original `RowData` rows, `lastTriggeringTsState` and the Void event-timer queue. It iterates
live native input batches or the persistent row store in chunks; it does not first write or decode
a native checkpoint. The cutoff is independent of buffered rows, including an empty buffer and
valid MIN or MAX timestamps. Stock serializers encode the actual empty key and rows, so exported
state does not require StreamFusion classes on the stock classpath.

Import validates descriptor ownership, the empty key, row/time/INSERT consistency and exact timer
membership before installing native state. Nullable and nested payloads retain their full original
values, and equal-time peers retain their arrival order. User column names that resemble internal
metadata remain payload. A failed export leaves live native state available for retry.

The draft portable planner retains the stock submission graph and binds only the admitted single
ascending rowtime sort. It preserves stock maximum parallelism, including the normal larger key-group
space for a single running task; the logical empty key is assigned by Flink's stock serializer and
hashing. The native raw buffer remains one internal group and cannot split across tasks. Its raw
layout is separate from stock canonical state. Secondary order keys continue to use the stock sort.

Focused qualification covers both heap/RocksDB stores, maximum parallelism 1 and 128, pending-only
and mixed state, an empty cutoff-only state, negative/extreme timestamps, empty watermarks, NULLs,
nested rows, arrays, maps, binary values, non-finite doubles, malformed state and unchanged re-export.
Full job, upstream restore-suite and remaining matrix qualification are tracked in
[#569](https://github.com/datafusion-contrib/StreamFusion/issues/569). The portable planner is still
under consolidation. Benchmarking remains deferred under the functionality-first requirement.
