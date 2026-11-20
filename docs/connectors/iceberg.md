# Apache Iceberg

**Status: native Parquet payloads for admitted batch and streaming SQL sources and sinks,
plus data-file maintenance.**

Supported batch and streaming SQL sources and sinks use the native paths when the optional
module is installed and the native planner is enabled. Unsupported contracts retain
the stock connector through the normal admission checks.
The admitted path is the normal legacy sink, unencrypted
format-v1/v2/v3 Parquet, distribution `none`/`hash`/`range`, and verified integer, long, string,
boolean, binary, date, float, double, decimal, time, and microsecond timestamp
columns (with and without a time zone), recursively inside structs, lists and maps. Java fanout handles partitioned append; a
four-bucket test verifies interleaved Arrow selections and independent rolling.
Java's original partition/key selector assigns destinations through borrowed Arrow
views. Rust splits the batches, then the existing columnar key-group exchange
transports them; no row payload crosses that shuffle. Unaligned checkpoints retain
actual key-group labels for channel-state rescaling. Format-v2/v3 tables with
identifier fields also admit INSERT/UPDATE_BEFORE/UPDATE_AFTER/DELETE and the
normal upsert option. SQL primary keys must match the table identifier fields;
upsert materialization retains the stock sink. Java retains equality/position
delete encoding, partition validation, and key bookkeeping. For keyed tables,
`range` keeps Iceberg's normal hash fallback. Fixed-width binary/UUID,
nanosecond timestamps, arbitrary dynamic
payload generators, normal Sink V2, and unknown writer-affecting options retain the
stock SQL sink. The dynamic DataStream API is described below.

The optional `streamfusion-iceberg` module targets the published Iceberg 1.12.0
Flink 2.2 runtime and requires Java 17 or later. Build it with `-Piceberg`.
It is separate from connector-neutral core and uses Apache iceberg-rust
0.11.0 from crates.io with Arrow/Parquet 58.4. The published release replaces the
temporary RC3 Git dependency and contains the same upstream source commit
(`20eb0feddac8f40bb9e9108a0744b7f356d69480`). Rust 1.95 is required.
No sibling Iceberg build is required.
The normal Flink 2.2 release build stages and packages its own platform native
library, includes the connector in deployment archives and Maven publication,
and checks package/JNI isolation. The ordinary optional-module CI matrix runs its
unit and upstream batch/streaming selection on JDK 17. Flink 1.18 release bundles omit it.
Javadoc uses the same Iceberg source selection as compilation, so the inherited
Flink compatibility source directory cannot introduce unrelated connector APIs.

## Bounded batch source and sink

The deployed batch SQL planner can replace either Iceberg boundary independently. When the
query between them runs columnar, the scan, computation and write exchange Arrow batches.
Use `streamfusion.operator.icebergSource.enabled` or `streamfusion.operator.icebergSink.enabled`
to disable a boundary.

Bounded FLIP-27 reads preserve the Iceberg Java snapshot or branch selection, split planning,
parallelism, split assignment, and serializers. The same reader as streaming decodes admitted
Parquet tasks with iceberg-rust, applies deletes, and emits Arrow. This includes v1/v2/v3
snapshots, selected columns and zero-column counts, supported predicates, nested payloads and
schema evolution, Parquet equality/position deletes and v3 Puffin deletion vectors. The
per-file Java fallback and native task counters apply in batch too.

Bounded legacy writes use the same Parquet encoder, Java task writers and Java committers as
streaming. Iceberg Java still chooses partitions, handles file rolling and branch commits,
encodes deletes, and uses `none`, `hash`, or `range` distribution. The writer and committer
finish on end of input, including without periodic checkpoints. Batch input is insert-only.
Identifier-key writes on v2/v3 still support Iceberg upserts, which can write deletes against
existing rows. Bounded `INSERT OVERWRITE` uses Iceberg Java's replacement of the partitions
written by the job. Top-level `NOT NULL` errors use the Flink error policy.

Batch shares the streaming physical-format, type and option exclusions. Pushed source limits,
legacy source APIs, metadata tables and unsupported source abilities retain stock SQL scans.
Normal Sink V2, dynamic SQL sinks, unsupported schema conversions or Flink constraint policies,
and unsupported sink abilities retain stock SQL writes. Iceberg's direct dynamic Arrow sink and
maintenance jobs have their own entry points described below.

### Nested batch hash join with native Iceberg I/O

The bounded join can carry Iceberg ROW, ARRAY, and MAP payloads with scalar join
keys. The Arrow execution schema carries the SQL fields used by the file reader
and writer. Iceberg's hash router continues to select the writer channel. The
stateless bounded writer receives that channel directly without a keyed-input sort.

An Apple M4 release run joined two tables of one million rows each, at parallelism
four with four input files per table and four output partitions. The job committed
one million rows in **11.378 s** with **256 MiB** of task off-heap memory. The hash
join wrote 256 spill files totaling **3,106,212,320 bytes**. The test checked both
native source scans, the native Parquet footer on every output file, and every
committed nested value, including null rows, null list elements, map values, and
microsecond timestamps. The default regression uses 96,000 rows per side and
96 MiB. The same test accepts `-Dsf.iceberg.join.rowsPerFile=250000` and
`-Dsf.iceberg.join.memoryMiB=256` to reproduce the larger fixture.

The spilling path also completed TPC-DS Q72 at **1 GiB**, a budget the earlier
hash join exhausted. With the same budget on stock Flink, three measured pairs
had medians of **38.519 s stock** and **44.607 s StreamFusion**. StreamFusion
was 15.8% slower. All eight trials, including warmups, committed the same
100 rows. The result occupied one of the four configured output partitions.
Every native trial used 40 native scan tasks, wrote one native Parquet file,
and recorded 256 hash partition spills totaling about 978 MB. The measured
ranges were 38.486–38.918 s stock and 44.437–45.684 s StreamFusion.
[Per-run results](../benchmarks/results/iceberg-tpcds-q72-spill-20261120.csv).

### TPC-DS scale 1 comparison, 2026-11-20

This comparison uses all 103 SQL variants and 24 table definitions from the
[Flink 2.2.1 TPC-DS workload](../benchmarks/tpcds.md), with the Trino 1.4 generator at
SF1. Both engines read the same Iceberg v2 Snappy Parquet files. Every input table has
four occupied Iceberg partitions and four data files, for 19,557,335 rows across 96 files.
The sales and returns facts bucket on order or ticket number. Other tables bucket on
their first key, except the five-row warehouse dimension, which uses four explicit shards
and a view exposing the original columns.

Each unchanged TPC-DS query runs inside an outer projection that hashes one result field
to an integer in `0..3`, with a fixed value for NULL. Both engines commit those rows to an
Iceberg v2 table partitioned by that integer and configured for hash distribution. A query
can populate fewer than four output partitions when it produces few rows or repeated
keys. The verifier checks the populated partitions and the complete typed row multiset in
every committed result with the Iceberg Java reader.

The local Apple M4 run uses Temurin 17.0.17, Flink 2.2.1, Iceberg 1.12.0 and release
StreamFusion libraries, with reader and writer parallelism four, 512 MiB task off-heap,
1 GiB Flink managed memory and 8,192-row transposes. Adaptive parallelism is disabled.
Both engines use the same join-reordering and 10 MiB broadcast settings and production
shuffle serialization. The timer spans SQL submission through successful final commit.
Fixture creation, output DDL, and committed readback are outside the timer. Each variant
has one warmup and three measured runs per engine, alternating which engine goes first.

The default StreamFusion configuration matched stock on **102 of 103 variants**. The sum
of median latencies on those 102 variants was **163.634 s for stock Flink** and
**202.605 s for StreamFusion**, or **23.8% more elapsed time**. StreamFusion's median was
lower on 76 variants. The geometric mean of the per-variant stock-to-StreamFusion ratios
was 1.055×.

| Variant | Stock Flink median | StreamFusion median |
| --- | ---: | ---: |
| Q14a | 2.324 s | 29.546 s |
| Q14b | 2.853 s | 18.884 s |
| Q72 | 36.848 s | 36.304 s |
| Q97 | 0.933 s | 0.402 s |

The 306 measured StreamFusion jobs with matching results decoded 10,734 native Iceberg
file tasks and recorded zero Java file-task fallbacks. All 786 committed StreamFusion data
files were written by parquet-rs. Native query computation ran on 10 variants. The other
92 used Flink query computation. Output occupancy was four partitions for 43 variants,
three for 12, two for seven, and one for 40.

In that run, Q28 failed with `bounded task input schema changed` as NativeExpand handed a batch to
the bounded native operator. The failure reproduced with the native scan and stock sink.
A stock scan with native computation and the native sink committed the exact stock result.
Keeping both native Iceberg boundaries and using Flink computation also matched the stock
result, with 24 native Iceberg read tasks and one native output file. These single-run
controls isolate the failure to the native scan and native compute combination. The default
timing totals cover the 102 variants with complete pairs.
The [per-variant report](../benchmarks/results/iceberg-tpcds-sf1-4x4-20261120.csv)
includes warmup and measured latencies, committed result hashes, native execution witnesses
and the failed variant.

### TPC-DS regression follow-up

Flink's Q14a execution graph had six scans and 15 joins. StreamFusion's graph had
57 connector scans and 48 joins. JFR recorded 878 samples in the native join
operator, 860 of them under three copies of the same NULL-safe hash semi-join.
Native stack sampling found Arrow gathers and DataFusion's hash match enumeration
at the busiest leaves. Retaining shared subplans cut Q14a file reads from 228 to 24
and Q14b from 160 to 32. In a separate one-pair check, Q14a still took 30.403 s
after the scan reduction. DataFusion now groups the non-preserved build keys of
composite NULL-safe hash semi-joins whose only predicate is the key match. This
removes duplicate build matches before they reach the join.

Q23a and Q23b originally ran Flink compute. JFR showed row hash joins and the
conversions at the Arrow boundaries. The Iceberg batch adapter now advertises the
format's UTF-8 guarantee, and the common Arrow bridge validates the bytes. Both
queries now run native compute, and file reads fell from 48 and 40 to 28 each.

Q28 exposed a mismatch between the file-reader schema, which carried Parquet field
IDs, and the execution schema of NULL literals emitted by grouping sets. The
adapter gives file IDs to the Rust reader for projection and schema evolution, then
emits the canonical Arrow execution schema. Q28 now passes exact committed parity
with native compute, 24 native read tasks and one native output file per run.

The focused, unprofiled release comparison used one warmup and three alternating
measured pairs per query. These focused binaries used the system allocator.
[Individual trials and result hashes](../benchmarks/results/iceberg-tpcds-sf1-4x4-focused-20261120.csv)
are available for both cohorts.

| Variant | Stock Flink before, s | StreamFusion before, s | Stock Flink after, s | StreamFusion after, s |
| --- | ---: | ---: | ---: | ---: |
| Q14a | 2.701 | 29.758 | 2.769 | 2.146 |
| Q14b | 3.033 | 19.105 | 3.003 | 1.734 |
| Q23a | 2.584 | 4.858 | 2.624 | 1.434 |
| Q23b | 2.529 | 4.834 | 2.642 | 1.489 |
| Q28 | | failed in the original suite | 1.015 | 0.450 |
| Q78 | 2.125 | 1.950 | 2.158 | 2.393 |

Q78 originally measured 4.234 s versus 2.049 s for stock Flink. Its focused
StreamFusion trials varied from 0.935 to 2.290 s before and 1.129 to 3.246 s after.
The native LZ4 decoder produced about 574 MB of Arrow shuffle data per job in both cohorts.

The first production-allocator sweep kept the 512 MiB task off-heap budget. It matched
100 variants and won 99, with summed medians of 68.610 s for StreamFusion and 127.114 s
for stock Flink. All 99 successful variants shared with the original sweep retained the
same committed result hash. Q24a, Q24b and Q72 exhausted the native hash-join budget.
Q88 was the sole completed loss at 8.315 s versus 4.218 s. Its eight branches shared one
store-sales scan but built separate copies of the same hash shuffle.
[512 MiB sweep](../benchmarks/results/iceberg-tpcds-sf1-4x4-512mib-20261120.csv).

Sharing the producer below the hash exchange reduced each Q24 variant from 11 to seven
joins and let both complete under the same 512 MiB budget. Q88 went from 63 to 42
exchanges. In one-pair checks, StreamFusion took 1.662 s on Q24a, 1.620 s on Q24b and
2.727 s on Q88. Stock Flink took 2.249 s, 2.065 s and 4.714 s in those pairs.
Q72 still exhausted 1 GiB, with one large build spread across four subtasks. At 2 GiB
both engines committed the original exact result. That sizing pair took 47.422 s on
StreamFusion and 41.144 s on stock Flink. Native operator counters recorded 56.2 million
rows from the sales/inventory join, 11.9 million after the sold-date join, and 68,349
after the inventory-week join. The native LZ4 decoder produced 5.46 GB of Arrow shuffle data.

The final production sweep used 2 GiB task off-heap on both engines and retained the
same four-partition fixture, output contract, and trial schedule. All **103 variants**
matched, including all warmups. The 102 successful variants from the original sweep
also retained their original result hashes. The sum of the three-trial medians was
**172.230 s for stock Flink** and **108.419 s for StreamFusion**, a **37.0% reduction**.
StreamFusion won 102 variants and the per-query speedup geometric mean was 2.042×.

| Variant | Stock Flink median, s | StreamFusion median, s |
| --- | ---: | ---: |
| Q14a | 2.614 | 2.104 |
| Q14b | 2.918 | 1.735 |
| Q23a | 2.617 | 1.089 |
| Q23b | 2.559 | 1.145 |
| Q24a | 1.842 | 1.449 |
| Q24b | 1.863 | 1.534 |
| Q28 | 1.061 | 0.410 |
| Q72 | 40.480 | 45.548 |
| Q78 | 2.081 | 0.847 |
| Q88 | 4.066 | 2.228 |

All 309 measured StreamFusion jobs showed native computation and native Iceberg
reads. They decoded 6,849 native file tasks without a Java file-task fallback. All
789 StreamFusion output files were written by parquet-rs. The output occupied four
partitions on 43 variants, three on 12, two on seven and one on 41.
[Final per-variant trials and result hashes](../benchmarks/results/iceberg-tpcds-sf1-4x4-final-20261120.csv).

With JDK 17 and Cargo on `PATH`, run the same comparison locally with the
`tpcds` and `bench` Maven profiles and a fresh scratch directory.

```sh
tpcds_dir=$(mktemp -d)
SF_TPCDS=true mvn -Piceberg,bench,tpcds -pl streamfusion-iceberg -am \
  -Dtest=IcebergTpcdsBenchmark -Dsurefire.failIfNoSpecifiedTests=false \
  -Dsf.tpcds.directory="$tpcds_dir" test
```

Use `-Dsf.tpcds.queries=14a,14b,23a,23b,28,78` for the focused cohort. Add
`-Dsf.tpcds.profile=true` for separate JFR recordings. The scratch directory records
execution plans, trial times, file counters and exact-result hashes. The default
task off-heap budget is 512 MiB, and `-Dsf.tpcds.offHeapMiB=2048` selects 2 GiB for both
engines.

### Bounded copy timing

A local Apple M4 run used Flink 2.2.1, Java 17 and release native libraries with parallelism two.
Four Iceberg v2 Parquet files held consecutive integer IDs, a 37-value integer category and
a bigint quantity. The SQL kept the upper half of IDs, added one to each ID, doubled the
quantity, and committed to an unpartitioned Iceberg v2 table using Zstd level 3. Timings include
SQL planning, file I/O, job startup, and commit. Fixture creation and stock readback of every
committed row were outside the timer.

The previous StreamFusion path ran native computation with the Java Iceberg source and sink.
The new path ran the same computation with two native file tasks, no fallback reads, and
native Parquet output. One warmup preceded three measured runs per size, with modes in rotating
order. The first native warmup for 200,000 inputs took 1.318 s, compared with 0.713 s for stock
Flink and 0.624 s for the previous path.

| Input rows | Stock Flink | Previous StreamFusion | Native Iceberg and compute |
| ---: | ---: | ---: | ---: |
| 200,000 | 0.536 s (0.529–0.574) | 0.557 s (0.534–0.565) | 0.508 s (0.507–0.523) |
| 2,000,000 | 0.809 s (0.801–0.890) | 0.892 s (0.838–0.927) | 0.617 s (0.603–0.667) |

The table reports median and observed range for this local numeric workload.

## Streaming sink and shared data writer

The first implementation boundary is the normal legacy streaming sink. A
version-gated adapter replaces its task-writer factory before job serialization,
while retaining Iceberg's writer operator, checkpoint flushes, committer, table
loader, and downstream graph. With unaligned checkpoints enabled, the data exchange
still captures Arrow channel state; the legacy committable edge is aligned explicitly,
following Flink's Sink V2 rule that write results precede their checkpoint barrier. The host task writer traverses borrowed Arrow row
views; the data appender records only accepted positions. Those selections drain
into parquet-rs's single-file Arrow encoder before releasing each input batch,
at memory thresholds, at size checks, and before close. Java's per-row counting
and 1,000-row rolling checks remain in place. Different encoders can produce
different byte sizes and file boundaries under the same target-size policy.
Each input batch exports one shared native Arrow lease across file drains; that lease
ends before the borrowed root is released, including abort paths. Hash routing caches
the host key-group-to-writer mapping when exact recovery labels are unnecessary.
Integer partition tuples also cache their exact Java-selected key groups, avoiding
repeated bucket-path formatting. Other key selectors remain unchanged. Java retains
the partition/hash semantics and controls every rolling check. The Rust encoder
consumes the existing selection allocation for sparse indices and skips index
construction for contiguous slices. Dynamic generators reuse bounded parsed table
identifiers. The original Java router and metadata processor still execute for every
row; the adapter reuses only immutable resolved target metadata, with bounded caches
and fresh checks after schema/spec refresh.

Append range distribution retains the released statistics operator, coordinator,
union state, checkpoint events and range partitioner. Java observes borrowed Arrow
views and chooses each destination; the existing Rust splitter emits selected Arrow
batches. Statistics update the producer's original partitioner before the corresponding
checkpoint barrier, where the stock output gate normally applies them. The normal
custom exchange keeps unaligned channel capture disabled. No file-local sorting is
added. Map/sketch tests cover nullable sort keys in formats 1/2/3, traffic after learned
assignments, SQL entry points, and restoration of learned statistics at parallelism 2→3.
`nativeRangeStatisticsUpdates` exposes applied host assignments.

The `NativeIcebergDynamicSink.append` API accepts Arrow input, one fixed
payload schema and a routing-only callback. The callback returns destination,
branch, spec, distribution, parallelism and key settings; it cannot replace the payload.
The original dynamic processor/updater perform catalog operations, table/branch creation
and Java routing. Both forward and shuffled paths carry target metadata plus Arrow IPC
batches. The normal Sink V2 writers, pre-commit union/aggregator and committer remain.
A wrapper around the original LRU cache decorates each new task-writer factory before
initialization or file creation, changing only data-file encoding. Java writes all deletes.
`nativeDynamicFactories`/`stockDynamicFactories` report target-factory admission.

Dynamic controls use typed handles for the pinned released record contract. A task-local
cache holds at most 1,024 immutable resolved targets, keyed by actual schema/spec identity,
table, branch, writer key, upsert mode and copied equality IDs. It retains no payload
views. Schema/spec encodings and row types remain batch-scoped, so target-cache eviction
does not repeatedly encode the same batch metadata. Route labels use one lookup per row. The host selector LRU also keeps its original
capacity and metadata invalidation: only bucket selectors whose maximum tuple cardinality (including nulls) fits
1,024 entries gain bounded exact-result reuse. Cache misses call the original writer-key selector;
round-robin, equality-only, unbounded and large-cardinality selectors remain unchanged. Java still applies
the original partition transforms to every row. Catalog refresh and the user's callback
are never cached away.

Resolved target layouts must match the input field names, order, types and nullability;
ID reassignment is allowed. Reordering, widening or filling payload columns requires a
future columnar adapter, so such direct-API contracts are rejected before data-file
writing. Arbitrary Java payload generators and the dynamic SQL connector stay stock.
Unsupported target encoding settings retain the original entire target factory, before
any file opens; there is no mid-file encoder switch. Deferred Java keyed updates retain
incoming equality-field IDs, so those IDs must already match the target table. This is
a released host constraint. Keyed dynamic Arrow writes additionally require
`dynamic-sink.immediate-table-update=true`: the deferred updater and direct output
are unioned, so metadata refresh can reorder updates across those branches. The direct
API rejects deferred keyed contracts rather than changing the requested setting.
Tests cover auto-created tables,
forward/shuffle interleaving, cache size one, immediate/deferred append updates,
partitioned immediate v2/v3 upserts and branches, deferred-keyed rejection, mixed
native/stock target policies, plus continuous
source/dynamic-sink 2→3 savepoint restore with aligned and unaligned checkpoint
configurations, with both ordinary and frequent periodic checkpoints. Source progress is
read from the exact restored savepoint ID; a newer checkpoint interrupted by cancellation
may contain only an idle task's zero-row snapshot. Both source tasks must have recorded
the chosen savepoint, and final readback still verifies every committed row exactly once.
Captured-channel fixtures cover 2→3 and 3→2 append/keyed recovery
for both sink APIs and 1↔2 for the normal sink. They keep the source live, inject a
failure after a completed checkpoint with actual retained Arrow channel state, then
verify changing values and exact committed readback after rescaling. A
startup gate waits for every task before releasing input; failure injection verifies
the completed checkpoint's channel handles and restores that exact checkpoint,
rather than assuming the newest checkpoint necessarily buffered records. The fixture
retains two checkpoints while completion callbacks select the restore input. An explicit
non-draining savepoint separates completed streaming writes from the terminal flush.
Published Java dynamic
sink/processor/writer/cache/serializer/aggregation/committer tests retain all 97 cases
in both configurations; these validate delegated host controls alongside native witnesses.

The Rust encoder writes through a bounded reusable JNI byte buffer into the
Java-selected Iceberg output stream. Java owns filesystem configuration and stream
close. Footer metrics use Java's metrics policy, supplemented by native NaN counts;
Java constructs the final data files and commits. Delete encoders remain Java-owned.
For format-v3 sinks, Java also assigns inherited row IDs, records snapshot lineage,
and writes Puffin deletion vectors; the native boundary still covers data files only.
Tests verify snapshot/file row IDs, vector blob references/offsets/lengths, and stock
readback after updates and restarts. The phased keyed recovery fixture waits until all
initial rows are committed, then waits for a completed checkpoint taken after that
observation before sending updates. An unaligned checkpoint can produce a partial initial
snapshot while another writer's rows remain in channel state; even when all rows become
committed, an earlier recovery checkpoint may still retain those records. The later boundary
separates cross-checkpoint equality deletes from same-writer position deletes. Both delete
kinds, exact readback without duplicate keys and native-handle cleanup remain asserted.
After emitting the update batch, the fixture advances its source progress under the checkpoint
lock before throwing the injected failure. A queued checkpoint may acquire the lock while the
legacy source thread propagates its exception to the task; its source offset must agree with
the emitted batch, including when that batch remains in captured channel state. A deterministic
fixture regression checks that progress immediately after the injected failure.
Duplicate-row, final-value and delete-kind failures include source checkpoint events and table
snapshot summaries. The keyed recovery stress selection alternates both upsert modes for
20 repetitions by default; increase it locally with `-Dsf.iceberg.recovery.repetitions=100`.
Live format upgrades, variant/geometry/geography,
and nanosecond schemas retain their existing exclusions. Native v3 reads apply Puffin
deletion vectors through the same Java-planned task reader as v2 deletes.

Timestamp conversion reuses the common Arrow component representation and checked
unit conversion: negative epochs floor correctly and the full supported calendar
range is preserved. Parquet time units and decimals convert column-wise. Native
read output adapts to Flink's Arrow schema after Iceberg projection/deletes; no
payload rows are reconstructed in Java. Tests cover pre-epoch microseconds,
years 0001/9999, precision-38 decimals, NaN metrics, signed zero, infinity, and nulls.

Writer settings use Iceberg Java's effective precedence and defaults. The encoder
maps uncompressed, Snappy, Gzip, and Zstd (including Java's default Zstd level 3),
row-group/page/dictionary byte targets, page row limits, dictionary enablement,
and Parquet v1/v2 pages. Unknown controls, custom size-check heuristics,
per-column controls, bloom filters, content-defined chunking, and Hadoop Parquet
overrides fall back until verified. Metadata metrics modes remain Java-owned.

Validation includes stock Iceberg file readback across those codecs/page versions,
nullable primitive values, field IDs, split offsets and final sizes, sparse
partition selections, within-batch rolling, empty writer cleanup, native handle
reclamation, and SQL append/update native-file witnesses. Keyed tests verify
committed stock readback across multiple checkpoints and injected restarts, with
aligned and unaligned checkpoints enabled. Interrupted batch processing discards
pending selections and closes native resources before borrowed Arrow views expire.
The adapter preserves Flink's keyed context with a representative integer assigned
to each batch's exact key group. Removing that context reproduced stale committed
state in one of ten unaligned fault runs; retaining it passed the stress run and
combined suite. Captured-channel tests also verify changing keyed values after rescaling.
`bin/flink-suite.sh iceberg` requires
matching published upstream streaming cases on stock and StreamFusion. The
[earlier streaming validation](../benchmarks/iceberg-boundary-round2-20261017/validation/summary.json) passes 303 tests
with 13 batch exclusions, plus six release Rust unit tests. Native
Criterion's `iceberg_io` suite separately exercises 126 release fixtures across
encoding, routing, task reads, and scalar adaptation;
it does not establish an end-to-end speedup.

The internal source task-reader foundation now translates Java-planned Parquet
file tasks explicitly into the released iceberg-rust reader. Host-backed positional
I/O preserves Java FileIO and authenticates each data/delete read. JNI workers attach
to the JVM for callbacks; seek/read pairs are synchronized and streams close with the
task. Cancellation closes host streams without holding their seek/read locks, allowing
blocked reads to unwind before the fetcher releases its native handle. Field-ID projection, nullable strings, Parquet equality/position deletes
(including projected-away keys), zero-column row counts, sequential task ordinals,
and logical-row resume with changed batch sizes have direct readback tests. Queued
batch recycling and iterator cancellation reclaim prefetched Arrow roots; injected
host read errors propagate and native handles are reclaimed.
The cancellation registry owns fetched batches until the emitter claims them, so
shutdown also releases records left in Flink's fetch queue after its last fetcher
exits. Claimed batches remain owned by downstream consumers. Recovery fixtures wait
for every task to run before requesting savepoints and require zero Arrow/task
reservations after cancellation and restore.

Task metadata now carries field-ID-bound source predicates, partition specs/tuples,
and legacy name mappings. Tests cover projected-away filter keys, identity constants
omitted from physical files, renamed no-ID columns, null-inclusive Java comparisons,
NaN/infinity and signed-zero set membership. Native page-index selection remains off:
the released reader rejects fixed-width decimal indexes. TIME and binary predicates
are rejected explicitly; residual-only task reads require the complete source filters
because Java uses residuals for pruning and full source filters for exact row filtering.
Partial-file tasks translate physical Parquet row-group midpoints to the released
reader's cumulative offset model; exact-boundary tests preserve Java ownership with
no omitted or duplicated rows.
The source reuses the task's verified Arrow output schema instead of exchanging a
C Data schema for every decoded batch. Source tasks and sink file closes share a
configured-once JSON mapper, retaining metadata serializer caches across files.
The [boundary optimization ledger](../optimizations/iceberg-boundary-overhead.md)
records the ownership contract and measured limits. The
[fast reader/writer comparison](../benchmarks/iceberg-io.md) isolates file-task costs
for iteration before full SQL/recovery and Nexmark validation. Its
[paired observations](../benchmarks/iceberg-boundary-20261017/README.md) retain
component gains, focused streaming confirmation and slower dynamic writer results.

The SQL source now retains the released FLIP-27 streaming source,
including snapshot discovery, assigners, watermarks, split serializers and coordinator
state. It emits Arrow batches with logical row offsets and counts rows in source IO
metrics. SQL admission covers plaintext v1/v2/v3 tables, verified scalar leaves inside
struct/list/map payloads, pruned nested projection (including zero columns), case
sensitivity, top-level scalar source filters and partition constants. Nested layouts evolve
by Iceberg field ID through rename, reorder, nullable additions and supported primitive
promotions. The 0.11.0 decoder retains the physical nested shape; a columnar adapter then
aligns it to the requested logical Arrow schema. Null struct parents mask child validity
before nested equality/sort expressions read the children. Value buffers remain shared
where no cast or gather is needed. Legacy name mappings remain admitted for flat schemas. Host split watermarks retain the original emitter and ordered assigner.
Matching scan contracts can share one source through the normal planner reuse pass.

Each file task has an explicit dispatch contract: admitted plaintext Parquet data/delete
files and v3 Puffin deletion vectors decode in Rust; non-Parquet/encrypted tasks, TIME equality-delete keys and tasks
without complete exact source filters use the released Java task reader and transpose
its output to Arrow. Source counters `nativeParquetTasks` and `stockFallbackTasks` make
that distinction visible. Mixed Parquet/Avro/ORC tasks preserve file ordinals and restored
logical offsets with changed batch sizes. This fallback is not native read coverage.

Pushed limits, metadata tables, nested predicate pushdown, nested partition
source fields, nested schemas with legacy name mappings, non-null initial defaults,
TIME/binary predicates and unsupported source abilities keep the stock
SQL source. Java-owned streaming
snapshot policy remains unchanged; this does not add CDC semantics to append scans.

Continuous SQL tests verify initial discovery, later appends, filters/projection, final
draining savepoints and committed stock readback on both engines. A partially emitted
source task restores from a savepoint at parallelism 2→3 without duplicate committed
rows. Field IDs survive rename, integer promotion and nullable-column additions,
including historical delete files. Source and sink DataStream APIs accept normal explicit
UIDs (the sink overload takes a host UID prefix); rescaling must also preserve compatible
host chaining/max-parallelism boundaries.

The encoder charges parquet-rs's reported retained memory, an estimate for accumulated
row-group metadata, and its 1 MiB JNI output buffer to the normal task off-heap budget
per open file. Reservations update after each batch and release on close or failure;
a quota failure aborts pending appends. This is retention accounting, not a bound on
all transient codec allocations. The released iceberg-rust writer hides this memory
API, so encoding uses its underlying parquet-rs Arrow writer directly; byte-for-byte
fixtures compare file bytes, rolling-size estimates, NaN counts and split offsets with
the released wrapper across codecs/page versions. Iceberg schema conversion, FileIO
and source task decoding use the pinned Apache 0.11.0 release. Shared Arrow allocations use the same
budget; internal decoder retention remains opaque. Queued batches and native handles
have explicit cleanup. Supported paths accelerate by default; the standard per-operator
switches remain available for controlled comparisons. The source follows
Comet's explicit task handoff and Paimon's host-FileIO/C Data bridge.
The implementation is available in [PR #386](https://github.com/datafusion-contrib/StreamFusion/pull/386),
with the completed [four-partition Iceberg-to-Iceberg Nexmark comparison](../benchmarks/iceberg-optimized-20261017/README.md).
The original design and reference implementations are recorded in
[issue #373](https://github.com/datafusion-contrib/StreamFusion/issues/373).
Batch overwrite remains outside scope. Data-file maintenance uses the separate API below.

The keyed checkpoint fixture waits for all six physical inserted rows to appear in committed
Iceberg metadata and for the final keyed rows to be visible before its synthetic streaming source
returns. The row check also covers delete-only batches, which do not advance the inserted-row
count. A completed source checkpoint can
leave update batches in unaligned channel state; observing that source callback alone does not
establish that every writer has committed. Partial update commits keep the fixture running, and a
deadline fails with source events. Final logical rows, delete-file kinds and injected recovery
assertions remain unchanged.

## Data-file maintenance

`NativeIcebergMaintenance.append(env, maintenanceBuilder)` builds Iceberg's normal
Flink `TableMaintenance` graph and substitutes only its data-file rewrite worker.
Use the overload with `sort=true` and a sort-memory budget to order each rewrite
admitted native group by the table's declared sort order. The existing Iceberg planner still chooses
the files, groups, target size, output spec and parallel work. Its trigger, locks,
checkpoint flow, commit batching, conflict checks and cleanup remain Java-owned.
There is no StreamFusion bin-packing implementation or metadata maintenance engine.

```java
var maintenance = TableMaintenance.forTable(env, tableLoader)
    .add(RewriteDataFiles.builder().scheduleOnDataFileCount(10));
NativeIcebergMaintenance.append(env, maintenance, true, 64L << 20);
env.execute();
```

The default overload does not sort and reserves no sort capacity. The sorted overload
uses the supplied per-worker budget; 64 MiB is the one-shot API's default.
The adapter is version-gated to the published Iceberg 1.12.0 / Flink 2.2 layout.
Unsupported file groups retain the original worker before any native files open,
including its stock ordering behavior. Configure sufficient task off-heap memory
for the reader, writer, equality sets and optional sort (for example 256 MiB).

For a one-shot job, `NativeIcebergMaintenance.rewriteDataFiles(env, table, options,
sort)` delegates to Iceberg's `BinPackRewriteFilePlanner`, executes its selected
groups as Flink tasks, and passes the resulting groups to Iceberg's
`RewriteDataFilesCommitManager`. The options belong to that existing planner.
The API rejects unsupported native contracts before submission.

Rust reads Parquet into Arrow, applies v2 position/equality deletes or v3 Puffin
vectors/equality deletes, and writes Parquet again through the existing native sink
appender. Sequence-number applicability is decided by Iceberg's planned tasks.
For integer, long or string equality keys, maintenance uses the existing Iceberg
decoder plus DataFusion's static membership filter, including composite tuples and
struct-descendant leaves. Composite arrays form Arrow struct views; inner nulls compare
equal, including null parent structs. This avoids 0.11.0's large per-delete predicate
tree without converting payload rows. Nested-schema source tasks use the same filter.
Flat sources and other admitted flat key types retain Iceberg's normal delete path.
Keys inside lists/maps and other nested-key types retain the stock task reader. Null matching and delete-sequence applicability remain
covered by stock-reader comparisons. The membership filter’s retained arrays and
per-key objects have an estimated task off-heap charge; internal decoder and
transient build allocations remain opaque.
Iceberg TIME payloads keep their full microsecond precision in Arrow through reading,
sorting and writing. Borrowed Flink TIME views expose milliseconds for control
access only; output partition specs using TIME fields remain outside native
maintenance admission because their Java routing wrapper cannot preserve microseconds.
V3 rewrites read and persist `_row_id` and `_last_updated_sequence_number` alongside
the payload so physical row reordering does not assign new identities. Java's
original partition and rolling writer traverses borrowed Arrow views; only selected
row positions return to Rust. Payloads remain columnar throughout file processing.
Filtering, gathering, sorting and encoding can allocate new column buffers: this is
not a claim of zero copying. No row-to-column or column-to-row payload transpose is
introduced. Flink edges transport file-group metadata rather than payload rows.

Maintenance shares source/sink admission and effective Parquet settings, including
codecs, compression levels, page/row-group sizing, field IDs and Java metrics policy.
It admits verified scalar leaves recursively inside struct/list/map payloads, plaintext
Parquet files, table versions 1–3 and verified delete-key types. Nested name mappings,
nested partition source fields, non-null initial defaults, encryption, unsupported
Parquet controls, TIME partition routing and TIME equality-delete keys remain outside
maintenance coverage. Nested equality keys must be integer/long/string struct descendants;
unsupported groups retain the original worker before any native file opens.
Sorting currently supports identity fields, including struct-descendant scalar keys,
with ascending/descending and explicit null placement; transformed sort keys and Z-order are outside coverage. DataFusion's
existing external sort owns ordering, merge and spill. Its configured memory pool is
reserved against task off-heap memory, with source/output Arrow retention additionally
accounted. Spill files use Flink's task spill directory. Transient codec allocations
retain the source/sink accounting limits described above. Worker counters
`nativeRewriteSortSpills` and `nativeRewriteSortSpillBytes` expose actual sort spills;
group/error counters retain one registered counter across successive groups.

Upstream rewrite SQL action tests preserve their planner/commit implementation and
assertions under a test-only file-worker adapter, including filters and stale-snapshot
equality-delete conflicts. The published streaming rewrite suite runs against stock
and native workers, covering branches, metrics and V3 lineage. See the
[upstream validation scope](../upstream-flink-suite.md) for the Hadoop fixture scope,
format fallbacks and the distinction between SQL setup and streaming maintenance.

The [sorted/equality maintenance comparison](../benchmarks/iceberg-nested-20261029/README.md)
separates complete continuous-job commit latency from file-worker throughput and
includes forced spilling and nested payloads.

The [release rewrite comparison](../benchmarks/iceberg-rewrite-20261028/README.md)
measures unsorted file processing against stock Flink at 400k and 2M rows, including
JNI, Java writer traversal, Parquet IO and close/metrics. It does not measure complete
maintenance-job startup or metadata commits.

## Separate Iceberg Nexmark comparison

`JAVA_HOME`/`PATH` must select JDK 17. Run `bin/bench-iceberg.sh` to build and stage
isolated release+mimalloc libraries and execute the separate streaming comparison.
This leaves the headline Kafka benchmark sources and SQL corpus unchanged.
`SF_ROWS`, `SF_PARALLELISM`, `SF_MATRIX_QUERIES`, `SF_WARMUP`, `SF_RUNS`,
`SF_ICEBERG_MODES`, `SF_ICEBERG_VARIANTS` and `SF_ICEBERG_ARTIFACTS` select the run.
`SF_ICEBERG_JAVA_HEAP` defaults to `4g` for both engines. The untimed Maven launcher
defaults to a 512 MiB heap unless `MAVEN_OPTS` is set. `SF_ICEBERG_DISTRIBUTION`
defaults to `hash`; `range` adds the same append sort order to both output tables.
Defaults are 2M events, parallelism four, all 23 queries, one warmup and three measured
pairs, all four state/mini-batch modes and the existing labelled expression variants.
Small/subset runs validate the harness; they are not final throughput results.

The harness directly reuses the existing event generator, query definitions, UDF,
lookup fixture and updating-result keys through a test-only corpus artifact. It seeds
three normalized `person`/`auction`/`bid` tables outside timing, with four physical bucket
partitions each, using the original deterministic events and field order. Logical SQL
views retain the original TIMESTAMP(3) rowtimes over Iceberg's microsecond storage
types. Both engines use `SOURCE_WATERMARK()` from Iceberg's ordered file lower bounds,
with the headline's four-second delay applied once per split (plus the host's 1 ms
inclusive-boundary adjustment). The benchmark wraps the released split extractor,
leaving split ordering and emission in Java. Stripes contain 50,000 original events by default; each
stripe closes its partition files. Both readers use 128 MiB split/open-file costs and
Iceberg's timestamp-statistics ordered assigner. This avoids combining files into tasks
that traverse event-time stripes out of order. Recomputing a periodic watermark from
decoded rows can advance past unfinished files and produced intermittent late-window
loss in both engines; the file watermark preserves the source's conservative progress
contract. This is an Iceberg-source setting shared by both engines; the original
query corpus and headline harness stay unchanged.

Every trial writes a fresh four-bucket output table. Updating outputs declare the actual
result identifier fields and enable Iceberg upserts; Java writes all deletes. The bucket
source is an identifier field for keyed outputs. A low-cardinality query may occupy fewer
than four buckets; artifacts report the actual count. Both engines receive the same
Zstd level 3, 128 MiB row-group, 1 MiB page, 2 MiB dictionary, 512 MiB file target, hash
distribution and one-second checkpoints. State modes are Flink heap versus native memory,
and Flink RocksDB versus native RocksDB; mini-batching is off/on with two seconds and
50,000 rows. The benchmark records RocksDB engagement, with an explicit q4 assertion.
Arrow IPC shuffle remains enabled through the normal `bench` profile.

The timer includes planning/submission, Java discovery, file IO, decode, query and exchange,
JNI, data/delete encoding, checkpoints and final drain/commit. A benchmark-only delegating
source observer waits for the seeded snapshot to be enumerated, no pending assignments,
and acknowledgements that all assigned splits have been consumed. It requests an
ordinary configured checkpoint to settle the completed streaming writes, followed by a
draining savepoint in the backend's native format, and waits for job completion.
Both remain timed. Checkpoints use the shared MiniCluster's filesystem default,
as the Kafka headline does; the final savepoint and Iceberg files are also written
to disk. The benchmark retains
completed checkpoint IDs, state bytes, durations and final savepoint file sizes.
Flink's statistics snapshot may lag or evict a completed checkpoint; its optional
size/duration fields are left absent in that case. The completed checkpoint future
and committed-result validation remain mandatory, and the client wait stays timed.
This replaces an initial diagnostic protocol that exported two canonical migration
savepoints: kernel sampling found substantial ext4 journal/writeback waits in both
engines. Those observations remain separate and are excluded from the final matrix.
The explicit checkpoint separation avoids merging a pending regular upsert flush with
the terminal flush in the released dynamic aggregator. Sources remain continuous and
use their normal checkpoint serializers. For q12, waiting for the last processing-time
window is included; validation conserves every bidder's count instead of comparing
inherently different wall-clock window timestamps.

Outside timing, stock Iceberg reads the committed output and computes a multiset
fingerprint of declared table columns. Delete-position metadata is excluded from payload
comparison. Runtime source counters, native writer-factory witnesses, Rust data-file
footers and Java delete-file footers distinguish native work from fallback. Stock trials
assert zero native source/writer factories and Java data-file footers. Empty outputs
are reported without inventing file witnesses. Artifacts retain all samples/warmups,
paired validation, plans, stream graphs, table locations, hardware/build metadata and
library hashes. Per-trial observations sample shared task reservations, Arrow allocations
and Linux process RSS at 100 ms intervals during progress polling; these are sampled
peaks, not exhaustive allocation maxima. RSS includes Java and opaque native retention;
baseline RSS and effective maximum Java heap are recorded as well.
`bin/report-iceberg.py <artifact-directory>` reports medians and ranges
and rejects incomplete/unpaired measured repetitions or missing declared matrix cells.
It also reports a separate aggregate using the README's existing variant selection
and sampling convention. `metrics.csv` retains per-cell phase/checkpoint medians,
sampled task/Arrow/RSS peaks and file counts; phase medians need not sum to the total.

For interruption-safe execution, `bin/checkpoint-iceberg.py <evidence-directory>` runs
the same matrix and retains each complete cell (its warmup and all three paired
measurements). `--push upstream` commits and pushes these checkpoints on the current
branch. Resume with the same command: completed cells are skipped, while an interrupted
cell is rerun in full, including its warmup. Each cohort records its own input snapshot
IDs and environment; both engines within a pair share the same files. Plans, graphs,
table metadata and checkpoint sizes are retained without committing the Parquet corpus
or savepoint payloads. Final reporting requires every declared cell across the cohorts.
`SF_ICEBERG_IO_BASELINE=true` separately compares native query execution with stock
Iceberg IO against native query execution with native Iceberg IO. The stock-IO leg
keeps both row/Arrow transposes where a native query island exists; it is labelled
separately from stock Flink and excluded from stock-Flink aggregates. Observations
also split total time into corpus/window progress, the settling checkpoint, and final
drain/commit. `SF_ICEBERG_DYNAMIC_FORMAT_VERSION=3` selects v3 for the separate dynamic
writer diagnostic; keyed runs with repeated keys require actual Java Puffin deletion
vectors and stock readback. These historical diagnostics predate native v3 reads.
For separate diagnostics, `SF_ICEBERG_PROFILE_TOOL` can point to the released
async-profiler `profiler.sh`; `SF_ICEBERG_PROFILE_EVENT` selects `itimer` or `wall`.
Each trial starts profiling before its timer and stops before stock readback, retaining
thread-labelled collapsed stacks beside the plan. Profiled observations are diagnostic
and excluded from the final performance matrix.
`SF_ICEBERG_SNAPSHOT_COMPRESSION=true` selects Flink's standard
`execution.checkpointing.snapshot-compression` option for both engines in a
separate tuning comparison. It defaults to false to retain the headline setting;
compressed and uncompressed observations are never combined in one matrix.
Exact-query aggregates report geometric mean ratios per mode and nominal corpus-event
throughput; labelled expression variants remain separate.
It also reports the headline sampling convention (first memory sample, best of the first
two disk samples) alongside all repeated medians/ranges. A 10k-event smoke matrix passed
120 stock/native pairs across all 23 queries, four modes and labelled variants; q8 also
passed ten repeated disk/mini-batch pairs with file watermarks. These are correctness
checks, not throughput results. The final 2M-event matrix passes all 480 paired
validations across 120 cells (one warmup and three measured pairs).

### Final streaming Nexmark results

The [latest complete matrix](../benchmarks/iceberg-optimized-20261017/README.md)
passes 120 cells / 480 paired validations. Using the README expression-variant
selection, geometric means of repeated stock/native median elapsed-time ratios are:

| State | Mini-batching off | Mini-batching on |
|---|---:|---:|
| Memory | 1.22× | 1.81× |
| RocksDB | 2.80× | 2.94× |

Exact-SQL geometric means are 1.23× / 1.69× memory and 2.51× / 2.74× RocksDB.
All 23 queries contribute once per mode; incompatible expression variants remain
explicit. This i7-12650H WSL2/ext4 comparison uses equal 4 GiB job heaps, two million
events, four input/output buckets, one warmup and three measured pairs per cell.
Timing includes configured checkpoints and final drain/commit. All 480 native graphs
have no row/Arrow transposes or operator fallback; native source/writer/task/footer
witnesses and committed output parity pass. All prior matrix settings match and all
480 input files have identical data pages and normalized decoded footers.

The [previous full matrix](../benchmarks/iceberg-final-20261017/README.md) reports
1.24× / 1.44× memory and 2.63× / 2.97× RocksDB. Current native medians improve
geometrically 1.03× / 1.19× / 1.22× in the first three modes and regress to 0.90×
previous/current in RocksDB/on. Cohort timing drift and large per-cell ranges remain
visible. These are combined boundary changes, not a causal measurement of each edit.

Default-on regressions remain: q18 memory/off is 0.14× stock and q20 memory/off/on
is 0.10× / 0.11×; q23 RocksDB/on is 0.51×. The
[report](../benchmarks/iceberg-optimized-20261017/README.md) retains full ranges and
phase timings: native q18/q20 corpus medians are about 0.66–2.31 seconds, while
settling/draining medians reach 8–16 seconds. Separate phase medians need not sum
to the total. [Earlier CPU/wall/kernel profiles](../benchmarks/iceberg-profile-20261017/README.md)
identify substantial state/checkpoint storage work;
[issue #391](https://github.com/datafusion-contrib/StreamFusion/issues/391) remains open.
Snapshot-compression tuning and the earlier
[same-query stock-I/O ablation](../benchmarks/iceberg-io-baseline-20261017/README.md)
remain separate historical measurements. Supported acceleration stays on by default.

### Current dynamic writer diagnostics

The separate dynamic writer comparison starts both engines with the same generated
8,192-row Arrow batches (integer ID and nullable Unicode payload), writes 2M rows to
two four-bucket targets, and uses one warmup and three alternating measured pairs.
Both engines use parallelism four, 4 GiB heaps, Zstd 3, one-second checkpoints and
immediate metadata updates. Java handles routing, deletes and commits. Timing includes
encoding plus non-draining and draining canonical savepoints; stock readback is outside
timing. These writer-only diagnostics are separate from the main Nexmark protocol.

| Format / mode | Stock median [range], s | Native median [range], s | Ratio |
|---|---:|---:|---:|
| v2 / mixed forward-hash append | 2.249 [2.179–2.301] | 2.313 [2.303–2.322] | 0.97× |
| v2 / keyed upsert | 3.206 [2.951–3.641] | 3.371 [3.057–3.505] | 0.95× |
| v3 / mixed forward-hash append | 2.315 [2.170–2.486] | 2.258 [2.203–2.305] | 1.03× |
| v3 / keyed upsert | 2.689 [2.528–3.072] | 2.745 [2.539–3.076] | 0.98× |

All 32 trials pass payload/readback and native data-file/handle/reservation checks.
Keyed writes repeat 100k keys; v3 trials require actual Java Puffin deletion vectors.
[Version 2 evidence](../benchmarks/iceberg-dynamic-v2-20261017/README.md) and
[version 3 evidence](../benchmarks/iceberg-dynamic-v3-20261017/README.md) retain all
warmups, plans, file counts, metadata and build hashes. These measurements remain
near parity; dynamic acceleration does not establish an end-to-end speedup.

An earlier release CPU diagnostic found routing/control in 52.9% of 1,699 sampled
stream-task stacks, native Parquet writes in 9.2%, and host prepare-commit in 22.0%.
Table-identifier parsing appears in 10.2%. These inclusive shares overlap and are
not wall-time shares. [Counts](../benchmarks/iceberg-dynamic-profile-20261016.csv)
and the [routing follow-up](https://github.com/datafusion-contrib/StreamFusion/issues/387)
retain that work. Earlier [8 GiB](../benchmarks/iceberg-dynamic-20261016.jsonl) and
[resource-accounted](../benchmarks/iceberg-dynamic-accounted-20261016.jsonl) cohorts
remain separate, including the slower 0.94× / 0.98× accounted measurements on a
shared host; they are not a controlled estimate of the accounting overhead.

A separate [RANGE q0 release comparison](../benchmarks/iceberg-range-20261016.jsonl)
uses 2M events, memory state, no mini-batching, four buckets, one warmup and three
measured pairs. Stock median is 1.028 s [0.984–1.223], native 0.741 s [0.651–0.763],
a 1.39× ratio. This short stateless case measures startup/I/O/drain as well as
processing; learned post-checkpoint range routing is proved by the live correctness
fixtures rather than inferred from this timing.

The [Kafka-to-Iceberg diagnostic](../benchmarks.md#iceberg-streaming-comparison)
reuses the headline Kafka JSON input at four partitions and four-bucket Iceberg
outputs, including real identifier-field upserts for updating queries. It supports
the same exact and separately labelled expression variants. Its finite-offset
streaming completion includes final sink commits and differs from the continuous
Iceberg-input suite's explicit checkpoint and draining savepoint.

The fresh [Kafka-to-Iceberg RocksDB/off suite](../benchmarks/kafka-iceberg-final-20261017/README.md)
passes all 23 queries and 120 paired validations, with median-speedup geometric
means **1.44× exact SQL / 1.47× README variants**. It uses four Kafka partitions,
four-bucket outputs, equal 4 GiB heaps and 512 MiB managed memory across four slots.
The report retains q0/q5/q19 regressions and large state-heavy timing ranges.

The separate [q0/q19 CPU diagnostics](../benchmarks/kafka-iceberg-final-20261017/profiles/README.md)
do not reproduce the primary elapsed-time losses. In the profiled q19 pair,
Rust data-file write/finish accounts for 8.0% of execution CPU samples and retained
Java position-delete finalization for 21.0%; inclusive shares overlap and are not
wall-time attribution. The primary benchmark retains its original repeated medians.

## Nested ROW execution metadata

Internal nested RowKind children remain outside persisted SQL schemas. File schemas and
reader projections contain only declared fields; Arrow file writes retain the original
value buffers while excluding these execution sidecars. Nested ROWs loaded from files have
INSERT kind, matching Flink. Top-level changelog columns retain their existing contract.
See [nested Java scalar results](../operators/calc-filter.md#nested-java-scalar-results).

The released 1.12.0 dynamic controller can cache a missing branch while another writer
creates it. Its next create call then throws `Ref main already exists` before emitting
the row. The Arrow generator handles this exact upstream branch-creation failure by
checking current catalog metadata, invalidating the stale negative lookup, and retrying
the same control record once. It requires a real branch with the requested name; other
validation errors and repeated failures remain errors. The router still runs once, and
no payload or previously emitted output is replayed. All original parallel writer,
format, branch, file-writer and committed-row assertions remain required.
