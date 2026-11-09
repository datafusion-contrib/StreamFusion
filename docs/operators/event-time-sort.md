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
state does not require StreamFusion classes on the stock classpath. Export stages each original
row once in the shared temporary disk spool, then the released ListSerializer streams one
complete timestamp bucket into the RocksDB projection. It does not repeatedly read and rewrite
a growing Java list. Heap component backends receive owned lists. Scratch storage closes and
deletes on success or failure; no helper descriptors enter stock metadata. Restore stages opaque
canonical map values and decodes rows individually with the released serializer, avoiding an
additional complete Java list. Bucket order and timer membership are compared in disk storage;
transfers use at most 1,024 rows and a four-MiB payload estimate, plus one oversized row. Flink's
largest encoded KV and the heap source backend's resident state remain intrinsic working sets.

Nested ROW/ARRAY/MAP payloads now use direct native RocksDB rows as well as scalar payloads.
Canonical captures pin the same database and convert bounded frames on the asynchronous writer;
live writes and concurrent captures keep independent read views. The shared retained-row codec
uses Arrow's existing row format and restores the declared nested field metadata. Map payloads
retain entry order and duplicate entries through their physical list/struct representation.
Scalar checkpoint bytes stay unchanged. Old snapshot-backed checkpoints migrate one key-group
blob at a time when opened; that legacy conversion retains a one-partition memory floor.

Import validates descriptor ownership, the empty key, row/time/INSERT consistency and exact timer
membership before installing native state. Nullable and nested payloads retain their full original
values, and equal-time peers retain their arrival order. User column names that resemble internal
metadata remain payload. A failed export leaves live native state available for retry.

Ordinary SQL and portable stock plans capture the actual released rowtime sorter, empty-key
serializer, row schema, time column, identity and maximum parallelism. Binding admits one ascending
TIMESTAMP(p) or TIMESTAMP_LTZ(p) rowtime key at the released precisions p=0..3 with fields supported by the row/Arrow converter.
Secondary order keys, asynchronous state and unaligned checkpoint
configurations retain the stock sort path. Released Flink rejects watermark attributes above
precision three; native enablement preserves that validation. A processing-time-only ordering has no native sort
owner. The submitted SQL or portable artifact remains stock Flink.

The runtime preserves the original maximum parallelism, including a larger key-group space for
one running task. Its representation-only input transpose retains that same maximum so it cannot
impose a different key-group space on the chained task. Flink's stock serializer and hashing assign
the logical empty key. The native raw buffer remains one internal group and cannot split across
tasks; its raw checkpoint layout remains separate from stock canonical state.

Focused qualification covers both heap/RocksDB stores, maximum parallelism 1 and 128, pending-only
and mixed state, an empty cutoff-only state, negative/extreme timestamps, empty watermarks, NULLs,
nested rows, arrays, maps, binary values, non-finite doubles, malformed state and unchanged re-export.
Complete-job controls preserve both transposes, custom source and transactional sink state, stock
workers without StreamFusion dependencies, unchanged-state re-export and ordinary native-checkpoint
recovery. Wider version, source/sink failure and schema/rescaling matrices
remain tracked in
[#569](https://github.com/datafusion-contrib/StreamFusion/issues/569). Benchmarking remains deferred under the functionality-first requirement.

The unchanged released `TemporalSortRestoreTest` rowtime program includes a secondary DOUBLE
order key. It therefore remains a stock-state compatibility control: the execution witness checks
the real stock state's initialization, secondary comparator and nonempty input alongside native
Calc work and an explicit fallback reason. It does not count as native sort state coverage.
The processing-time program has no sort operator; its submitted graph is checked separately.


Restore requires a fresh native target. Existing pending rows or a cutoff-only target are errors,
and rejection preserves both sides. Allocation denial retains all source entries and reports the
original native memory-limit exception; the failed target is discarded before retry. Timer queues
are registered once per restored projection backend using its captured serializer snapshots.
The failure controls use fresh restored projections, matching normal initialization, rather than
re-registering serializers on a running stock operator. Callback failures preserve the original
Java exception before or after Arrow import, and release the buffers.


The consolidated Flink 2.2.1 run passes 1,533 controls in twelve classes with no failures, errors
or skips: 698 sort state controls (542 retained-row/cursor/layout scenarios, 128 precision/backend
cases and 28 failure/retry controls), 276 sort planning controls (256 positive, sixteen stock
fallbacks and four released watermark-validation checks), and 559 existing OVER/rank/identity/SQL
regressions. State controls cover both timestamp kinds at precisions zero through three and
maximum parallelism one/128. All original state assertions remain in the expanded fixture.

A separate 28-case complete-job cohort passes both engine orders, native stores and submission
forms: eight continued-input switches, eight no-input round trips, four ordinary native-checkpoint
recoveries and eight source rescalings through one, three and two tasks. The sorter and committing
sink remain singleton. Every stage's sorter maximum must exactly match the uninterrupted stock
baseline. Pending-only and emitted/pending cuts, equal-time peers, a late drop and acceptance
between the actual cursor and a later empty watermark are checked. Full external output retains
global timestamp order and equal-time arrival order; an unordered multiset is insufficient.
Jobs retain both transposes, require actual native input/storage, preserve custom stock source and
committer state, and exclude StreamFusion from stock classpaths and savepoint metadata. These
job fixtures use BIGINT payloads and TIMESTAMP(3); wider schemas/precisions are operator/planner
qualification, and native connector/writer delivery and failure matrices remain open.

The native suite passes 1,300 tests with two existing ignores, and the no-RocksDB build passes.
The shared map-list spool cohort additionally passes 103 selected actual-operator interchange
cases, 28 malformed-state/failure cases and all 128 temporal precision cases on Flink 1.18.
These qualify the exercised component layouts, rather than the complete 2.2 job matrix on that
line. Earlier shared processing/checkpoint controls overlap this evidence. Both unchanged released temporal-sort restore programs
also pass as stock-only controls; their secondary comparison/proctime shapes remain outside the
native sort contract. Earlier cohorts overlap these runs and are not cumulative coverage.

Eight existing global GROUP BY rescaling jobs also pass with the extended singleton-owner harness,
retaining its prior source/aggregate/sink constraints. This separate regression cohort is not
added to the 28 sort-job count.
