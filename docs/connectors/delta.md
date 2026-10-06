# Delta Lake

The optional `streamfusion-delta` module accelerates data-file writes for the published Delta 4.4
Flink connector. Delta Kernel and the connector still own table creation and discovery, logical-to-
physical schema transforms, optimistic commits, transaction-log actions, primary-key lookup, and
merge-on-read deletion vectors. StreamFusion replaces only the path table's Parquet data-file
writer and the batch-preserving handoff into it.

Both unpartitioned and partitioned path tables are supported in `append` and `upsert` write modes.
This includes direct paths on every Hadoop filesystem supported by the Delta connector (local,
HDFS, S3A, ABFS, and GCS). Catalog-managed tables currently stay on the stock connector path because
the published connector API does not expose a supported engine replacement for them. The released
Delta connector retains its normal table and commit-coordination behavior; StreamFusion decorates
the published path-table API rather than carrying a fork or vendored Delta implementation.
SQL path tables accept connector-specific `fs.*` options such as S3A endpoints, credentials, and
path-style access, or can use the normal ambient/core-site configuration.

For example, an S3 path table can be declared directly in SQL:

```sql
CREATE TABLE delta_sink (
  id BIGINT,
  payload ROW<name STRING, scores ARRAY<INT>>,
  dt STRING
)
WITH (
  'connector' = 'delta',
  'table_path' = 's3a://my-bucket/events',
  'partitions' = 'dt',
  'file_rolling.strategy' = 'count',
  'file_rolling.count' = '-1',
  'fs.s3a.endpoint' = 'https://s3.us-east-1.amazonaws.com',
  'fs.s3a.path.style.access' = 'false'
);
```

AWS workload identity, instance roles, or Hadoop `core-site.xml` remain the preferred credential
sources. For S3-compatible stores, the usual `fs.s3a.access.key`, `fs.s3a.secret.key`, endpoint,
SSL, credentials-provider, and path-style settings can instead be supplied as table options.

In the partitioned path, Arrow batches are split by partition and exchanged between tasks while
they are still Arrow. After the exchange, one ownership-carrying batch record enters StreamFusion's
Delta writer instead of one Flink `StreamRecord` and Java wrapper per row. Java uses lightweight row
views only for Delta's RowKind, partition-value, and primary-key bookkeeping. It records the row
positions that survive the merge and hands the original Arrow buffers plus those positions to Rust;
the data-file payload is never transposed row-by-row. Dense selections pass through unchanged, while
sparse selections gather each Arrow column once immediately before the standard parquet-rs
`ArrowWriter` encodes it. Ignored update-before and key-only delete records never reach a data file.
When an upsert or delete removes the last live row from a buffered selection, the writer releases
its Arrow reference immediately and removes the selection metadata. Empty partition buffers are
also removed. Row positions use growable primitive integer arrays and live positions use a bitset,
avoiding one boxed integer per buffered row; draining visits only live positions. Existing
partition buffers use a map lookup without allocating a creation callback per row. Partially live batches stay retained until their remaining rows are superseded or
the checkpoint writes them; append-only batches retain the same checkpoint lifetime. A replacement
within the same input batch keeps that batch alive before retiring the previous row.

The view operator and Delta writer run at the same sink parallelism. For unpartitioned tables,
view creation starts a new operator chain: Flink's Sink V2 writer cannot chain behind a legacy
source such as SQL datagen. This places any network boundary before view creation, while records
are still serializable Arrow batches. Partitioned tables already have that boundary at their
partition exchange. The Arrow-backed views stay within the writer task.
Disabling operator chaining through the execution environment or
`pipeline.operator-chaining.enabled=false` keeps the Delta writer on the stock path.

The Arrow schema crosses the C Data Interface once when each data-file encoder opens; subsequent
batches export only their arrays. Java opens and owns the Hadoop output stream, while encoded bytes
return through the same bounded one-MiB bridge used by the plain Parquet sink. After Rust finalizes
the standard Arrow writer and its footer, Java flushes and closes the stream. Delta Kernel's footer
reader then derives the typed row count, minimum, maximum, and null-count statistics, and Kernel
creates and commits the resulting data-file actions. StreamFusion does not implement Delta log,
statistics, or commit semantics independently.

The native path is whitelist-first. It accepts Boolean, integer, floating-point, decimal, string,
binary, date, timestamp, `ROW`, `ARRAY`, and `MAP` columns recursively. Schema evolution,
`INSERT OVERWRITE`, intervals, and types the Delta connector itself cannot write stay on the stock
connector path. Delta Lake data files remain Parquet: the transaction log and deletion-vector
sidecars are protocol files, not alternative table data formats.

Delta Kernel views read the engine's millisecond/fraction timestamp pair as microseconds, preserving
Delta's supported timestamp range without an i64 nanosecond intermediate. The Parquet boundary
uses Kernel's physical column names, including nested struct fields, and restores TIMESTAMP versus
TIMESTAMP_NTZ timezone metadata before writing. SQL insertion binds columns by position, so input
aliases and generated expression names must not become names in the table's Parquet files.

Sink constraints remain Flink-owned. A nullable query field assigned to a `NOT NULL` target keeps
the Delta sink on the stock path so `table.exec.sink.not-null-enforcer` can fail or drop the row.
When `table.exec.sink.type-length-enforcer` is enabled, bounded `CHAR`/`VARCHAR` and
`BINARY`/`VARBINARY` targets likewise stay on the stock path for Flink's trim, pad, or error
behavior. Statically non-null inputs and the default type-length setting (`IGNORE`) remain eligible
for native writing.

Count- and size-based file rolling remain on the native path. The connector default is size rolling
at 100 MiB. Count rolling uses exact row boundaries. Size rolling uses parquet-rs' encoded-byte
estimate and checks it every 1,024 rows, so a file can exceed the configured size by one check
interval. Negative limits disable the selected strategy. Rolling opens a new Java-owned Hadoop
stream and a new standard Arrow writer; it does not move table or commit ownership into Rust.

The native data-file writer honors Delta's `delta.parquet.compression.codec` **table property** and
the standard Hadoop `parquet.compression`, block/page/dictionary sizes, dictionary setting, and
writer version. Delta table properties must be established through Delta's table API or existing
table metadata; the published SQL connector does not accept arbitrary `delta.*` table properties as
connector options. Unsupported codecs or writer behavior (validation, custom padding,
multithreaded Zstandard, disabled Zstandard buffer pooling, or a custom Delta Kernel target file
size) delegates that data-file write to Delta Kernel's stock Parquet handler.

Build with the `delta` Maven profile and deploy `streamfusion-delta`, `streamfusion-parquet`, and
published `io.delta:delta-flink_2.2:4.4.0` together. The module has no snapshot, local-Maven, path, or
forked Delta dependency.

`bin/flink-suite.sh delta` runs all four unchanged portable SQL sink tests from Delta `v4.4.0`
against its published connector and Kernel artifacts. It preserves upstream batch aggregation,
committed-row, partition, and file-statistics assertions. The two supported streaming loads must
also prove that native Parquet encoding processed rows; the many-types case includes `TIME(0)`
and must prove stock fallback with that reason. The suite is part of the blocking upstream CI
matrix. The separate remote Databricks/Unity Catalog integration test requires credentials and
is outside this portable suite. See [Upstream Flink suite](../upstream-flink-suite.md).

On the 2M-event, four-partition Kafka JSON Nexmark sink diagnostic (memory state, mini-batching off,
one warmup, best of three), all 23 queries supported by Flink completed and StreamFusion's suite
geomean was **1.522×** the stock published-Delta path. Updating queries used Delta 4.4 merge-on-read
upserts; naturally append-only queries used append mode. See
[Benchmarks](../benchmarks.md#parquet-delta-and-paimon-sink-diagnostics) for the exact method and
reproduction commands.

## Flink 1.18 compatibility audit

The released `delta-flink:3.3.3` connector predates the Kernel-based integration above and declares
Flink 1.16.2 dependencies. Its 70 unchanged portable SQL tests pass against released Flink 1.18.1
in the isolated host audit:

```sh
bin/delta-legacy-audit.sh
```

This runs the in-memory catalog, SQL source, sink and end-to-end suites, preserving their fixtures,
checkpoint/commit checks and expected results. Hive catalog tests require separate infrastructure
and are outside this portable audit. Reports are written under
`.flink-suite/delta-legacy-1.18/target/surefire-reports`. The host audit is a separate blocking
CI job included in `All upstream integration tests`; the existing native Delta 4.4 suite still runs.

The audit loads no StreamFusion planner or native library. It proves the selected host connector
cases, not native Delta acceleration or a complete connector compatibility matrix. Delta 3.3.3
uses its Standalone transaction API and lacks the Kernel path-table engine replacement used by
StreamFusion's 4.4 integration. No Flink 1.18 Delta acceleration artifact is admitted; that remaining
work is tracked in [the connector compatibility issue](https://github.com/datafusion-contrib/StreamFusion/issues/187).

The opt-in `DeltaBufferingBenchmark` measures only the production sink writer's `write` calls
while a checkpoint is held. Arrow conversion, table creation, `prepareCommit`, commit publication,
and a released-Kernel scan that verifies every final key/value run outside the timer. Cases compare
append input with repeated upserts over 16 keys, 1,024-row Arrow batches, 4,096/32,768 input rows,
and 16/256-byte payloads. Each case warms up once and reports three observations; retain all
observations rather than selecting the fastest. Run the Delta module's test with
`SF_DELTA_BUFFER_BENCHMARK=true`, `-Pbench`, and `-Dtest=DeltaBufferingBenchmark`; optional
`SF_DELTA_BUFFER_ROWS` and `SF_DELTA_BUFFER_REPEATS` narrow the fixture matrix.

The report separates thread heap allocation bytes from Arrow allocation requests and retained
Arrow bytes after all input was consumed. Arrow requests are counted by the input allocator's
listener, not inferred from heap bytes; neither request count nor retained bytes measures copied
bytes. Setup allocates the complete input outside timing, so retained bytes identify input buffers
still owned by the writer before `prepareCommit`, without including unconsumed input. This
boundary isolates checkpoint buffering and does not establish an end-to-end stock/native speedup;
`NexmarkDeltaSinkBenchmark.mergeOnReadUpsertComparison` retains the full Kafka/JSON/Delta pipeline
for that comparison.

The final paired release buffering run used seven observations per shape after one warmup.
All eight write medians improved (1.09–3.81×), with substantial JIT/order and range variation.
The 32,768-row, 256-byte upsert median changed 6.933→6.376 ms, median JVM allocation
27,113,312→25,445,936 bytes, and retained input Arrow 10,551,296→329,728 bytes (32×).
Append input retention stays unchanged. Write-phase Arrow allocation requests were zero before
and after; this does not imply zero copied bytes. The
[optimization ledger](../optimizations/delta-live-buffer-retirement.md) retains every shape's
median, range, IQR, allocation/retention totals and a link to all raw observations. The first
retirement-only candidate regressed timing and was revised with primitive positions and bitsets
before this final measurement; no full-job throughput claim follows from this arrival timer.


The buffering benchmark and ownership regression fixture retry directory cleanup for at most one
second when released Hadoop checksum background work races directory walking or removal.
Only missing-file and nonempty-directory errors are retried; other I/O errors fail immediately.
Cleanup remains outside measured execution, and the same helper is used for before/after runs.
