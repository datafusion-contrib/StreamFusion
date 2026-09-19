# Apache Paimon

**Status:** experimental. The optional `streamfusion-paimon` module accelerates streaming
`SELECT` reads and `INSERT INTO` jobs on Paimon **append-only tables** and **primary-key tables with fixed,
dynamic, or postpone buckets** on the published Paimon `2.0.0` Flink 2.2 connector.
Paimon keeps every table-level responsibility:
schema and catalog, bucket assignment rules, sequence numbering rules, file rolling, statistics,
manifests, snapshots, commits, and compaction. StreamFusion replaces the per-row shuffle in front
of the writers, append buffering and spilling, the Parquet/ORC encoding of each data file, and, for
primary-key tables, the sort and merge that turns a bucket's changelog into a level-0 file. Postpone
staging retains every accepted change for Paimon's separate compactor.

Both file formats share the same Java lifecycle, native routing and merge paths. ORC's physical
encoding options and UTC timestamp restrictions are detailed on the [ORC page](orc.md).

Timestamp bundles use lossless millisecond/fraction columns, including years 0001–9999. Parquet
unit selection still follows each Paimon field's precision, and ORC retains the stock writer's
historical calendar and LTZ rules. Existing format/precision admission limits remain in force.

## Streaming source

Streaming Parquet and ORC reads retain the released Java Paimon client for table/catalog resolution,
snapshot and manifest discovery, split assignment, filesystem credentials, and enumerator
checkpoint serialization. A task-side reader emits Arrow batches into the native pipeline:

| Read phase | Implementation |
|---|---|
| Append snapshot and subsequent committed data | Native Parquet/ORC decoding into Arrow |
| Primary-key initial snapshot | Native Parquet/ORC for raw-convertible files; native sorted-run merge for admitted deduplication/first-row splits; Java merge-to-Arrow for other splits |
| Primary-key changelog tailing | Native Parquet/ORC decoding of value columns and the stored row-kind byte |

Primary-key tailing requires `changelog-producer = input`, `lookup`, or `full-compaction`, and
Parquet or ORC changelog files. The producer has already generated the changes; the source preserves
`+I`, `-U`, `+U`, and `-D` without another merge. Default startup reads the current snapshot before
following commits. `scan.mode = latest` starts with new commits only. Remaining snapshot merge coverage
is tracked in [#53](https://github.com/datafusion-contrib/StreamFusion/issues/53). The
[snapshot-merge design](https://github.com/datafusion-contrib/StreamFusion/blob/main/.claude/research/paimon-native-snapshot-merge-design.md)
records the Java planning and Arrow batch interface.

The native decoder calls Paimon's seekable `FileIO` through a reusable 64 KiB transfer buffer.
parquet-rs or orc-rust reads the projected columns and decodes batches of up to 4,096 rows. It does not
load an entire file into Java memory. Arrow C Data exports transfer ownership to the source batch;
closing the reader releases the native decoder and Java input stream. Memory includes compressed
column chunks, decoder working buffers, and the source's bounded fetch queue; the batch row count
is not a hard byte budget.

Top-level projection, reordered columns, nested values, and supported periodic source watermarks
are preserved. Query predicates remain Flink residual filters; this first source does not push
query predicates into its Java scan or native decoder. Checkpoints save the number of emitted
logical rows per split with Paimon's existing serializer. Recovery skips precisely that many rows,
including positions within a new batch size. Prefetched rows do not advance the checkpoint.
`source.operator-uid.suffix` uses Paimon's released UID generation, preserving explicit source
identity when restoring savepoints after job graph changes.

The reader deliberately retains Java decoding and row-to-Arrow conversion for splits requiring
schema evolution, deletion-vector selection, or historical files without an installed native codec, and for specialized
split representations. This preserves those files' Java read semantics within an admitted source.
Primary-key snapshot splits needing unsupported merge semantics retain Java, including its merge
engine and delete handling.

### Native initial snapshot merge

Java Paimon's released `IntervalPartition` groups each snapshot split into disjoint key sections
and sorted runs. Native code merges the runs in a section while opening each run's files in order.
The optional Paimon native library adapts paimon-rust's Arrow key comparison, cell selection and
batch output gathering. Its loser tree ports released Java Paimon's traversal and grouping rules. It requests Arrow batches from the separate Parquet or ORC library through a Java
callback that forwards C Data addresses. No Java row or Java Arrow vector is materialized between
decoding and merging. Java still owns file access, schemas, discovery and checkpoints.

The native merger admits fixed and dynamic hash buckets with `deduplicate`, `first-row`, or basic `partial-update`,
the default loser-tree sort engine, and nonempty stored scalar keys: `BOOLEAN`, `TINYINT` through
`BIGINT`, `FLOAT`/`DOUBLE`, `DECIMAL`, `CHAR`/`VARCHAR`, `BINARY`/`VARBINARY`, `DATE`, and
`TIMESTAMP`/`TIMESTAMP_LTZ` through precision 6. Composite keys use those same types after Java
removes table partition columns from the stored key. Floating comparisons canonicalize NaN payloads
and distinguish signed zeros like Java; this source coverage does not broaden the sink's key
whitelist. Timestamps above precision 6 retain the stock source. Values use the supported source
types. Files must use the current schema and Parquet/ORC without deletion vectors.

Deduplication compares optional `sequence.field` columns in the configured
`sequence.field.sort-order`, with nulls first in either direction, then stored sequence numbers.
Sequence columns use the comparable types above and are decoded even when projected away.
First-row selects the earliest stored sequence. Both engines honor `ignore-delete`; otherwise
first-row requires every file to report zero retracts, leaving Java to handle unsupported input.
Exact stored-sequence ties and overlapping file sequence intervals use Java's loser-tree leaf
states, reverse initialization and equal-key traversal. Each group consumes one current record per
run before any run advances, including at batch boundaries. Deduplication and first-row retain one
winning row reference per key.

Basic `partial-update` selects the latest non-null cell for each column. It supports `ignore-delete`
and `partial-update.remove-record-on-delete`, including stored historical retracts and later
reinsertion. Without either delete policy, files must have a known zero delete count. Multi-record
partial reductions emit inserts or remove the key; singleton groups preserve their original row
kind, matching Java's reducer wrapper. Nested values are selected as whole cells, preserving their
internal nulls. Cell references are gathered into an output Arrow batch without Java rows or a
one-row Arrow allocation per key. Sequence groups and configured field aggregates retain Java.

ORC retains Java for fractional timestamp key bounds that can lose sorted order through its
last-negative-second alias, and for non-leading fractional timestamp keys; see the
[ORC timestamp restrictions](orc.md#configuration-and-types). These snapshot combinations retain Java:

- Aggregation; partial updates with sequence groups, removal by sequence group, or configured
  field aggregates; postpone and cross-partition key-dynamic buckets; unsupported key/sequence
  types; and specialized split representations.
- First-row files with nonzero or unknown delete counts when `ignore-delete` is false, and basic
  partial-update files with those counts when neither supported delete policy is enabled.
- Sections exceeding `sort-spill-threshold`, or the encoded row-group admission budget below.

For raw-convertible primary-key snapshot splits with known delete counts and no deletion vectors,
the source follows Java's raw reader: native decoded values emit insert rows. This path does not
need a merge. Selection merges drop winning retracts and retain the winning add kind;
partial-update reductions follow the row-kind rules above. Changelog tailing continues to preserve all stored changes.

`sort-spill-buffer-size` supplies the snapshot merger's retained-buffer budget. Before emission,
the reader inspects file footers and requires the sum of each run's largest compressed-plus-
uncompressed row-group size (Parquet), or its conservative stripe/dictionary estimate (ORC), to fit half that budget. Larger splits retain Java's spill-capable
reader. Native merging accounts for retained Arrow inputs, encoded keys, the current winner and
pending output references; exceeding the budget fails the read. Output flushes by bytes as well
as rows, and completed batches are reclaimed even during long stretches of deleted keys.

These are encoded-data admission and retained-buffer limits, not a strict total-process or Flink
managed-memory reservation. Transient decoder/output allocations, selection-index vectors, an individual wide record,
Java stream buffers and the source handover queue add memory. A resource or storage failure after
emission fails the attempt; it never silently switches readers in the middle of a split.

Recovery still checkpoints emitted logical rows after merging and delete removal. Replaying the
same split and skipping that prefix supports changes in batch size and Java/native restoration
in either direction. It rereads the prefix rather than persisting native pointers or per-file
version offsets.

`PaimonSnapshotMergeTest` checks stock- and native-written overlapping snapshots, nested values,
projections omitting keys, partitioned composite keys, Java/native restoration and admission
fallbacks. `PaimonSnapshotKeyTypesTest` covers admitted key types across Parquet and ORC with
stock/native writers (floating keys use stock writers), signed integer extremes, all three Parquet
decimal physical encodings, Unicode and binary prefix ordering, pre-epoch dates/timestamps, timestamp precisions 0–6,
projections and restoration in both Java/native directions. Nanosecond keys exercise Java fallback
even when projected away. `PaimonSnapshotModesTest` compares fixed/dynamic buckets, first-row,
stored retracts with ignore-delete, ascending/descending compound sequences, nulls, floating edge
cases and scalar sequence types against Java, including projections and restoration in both directions.
`PaimonSnapshotMergeOracleTest` compares the same records directly with released Java's merge reader
across 378 combinations of merge/delete policies, user sequences, 1–16 runs and batch sizes, including
exact ties, empty runs and repeated keys within one run. `PaimonSnapshotPartialUpdateTest` covers
actual Parquet/ORC files with tied stored sequences, different payloads, all supported scalar and
nested value types, fixed/dynamic buckets, projections and restoration in both directions.
`PaimonSourceSqlTest` follows scalar keys, first-row, partial updates, user sequences and dynamic buckets from
snapshot into subsequent commits for both formats. Lookup changelog fixtures allow compaction
and supply Java's temporary IO manager, as normal Flink writers do.
`PaimonSourceRecoveryTest` restores the asynchronous reader before and after emission for
snapshot and tail splits, including first-row, partial updates and user sequences. `PaimonSnapshotCallbackTest`
checks partial C Data export cleanup and preservation of the original storage exception.
The Rust tests cover keys spanning input batches, 100,000-version and delete-only inputs (including
partial-update groups),
byte-triggered output flushes, and budget/storage failures. The SQL harness requires a marker
proving that a native snapshot merger emitted a batch.

This extension passes 653 Paimon regression cases, including the added Parquet/ORC value and
restore cases, plus eight unchanged upstream continuous-reader, partial-update and read/write SQL
cases with required native snapshot and sink markers. The complete Paimon 2.0.0 SQL harness
previously passed all 265 result, plan and recovery cases with the
complete-plan native hook, including the unchanged source-reuse assertion. A focused subset is
shown below. Include the continuous-reader cases when checking the harness's required native
snapshot-emission marker; the other suites can finish without needing that path.

```bash
FLINK_SUITE_TEST='org.apache.paimon.flink.ReadWriteTableITCase,org.apache.paimon.flink.PrimaryKeyFileStoreTableITCase,org.apache.paimon.flink.FlinkJobRecoveryITCase,org.apache.paimon.flink.ContinuousFileStoreITCase#testWithPrimaryKey+testProjectionWithPrimaryKey' \
  bin/flink-suite.sh paimon
```

### Snapshot catch-up diagnostic

`PaimonSnapshotMergeBenchmark` writes 65,536 keys over one, four and eight commits for `INT`,
and four commits for other scalar keys and first-row, partial-update, sequence and dynamic-bucket
cases, including updates, deletes, strings and nested arrays, before timing. It compares the existing Java
merge-to-Arrow path with native snapshot reading, including Java run planning and footer checks,
file open/close, byte reads, decoding, merging, final Arrow import and an integer payload checksum.
Keys span negative and positive values; a separate integer payload identifies both key and commit
so the checksum checks version selection and uses the same work across key types. Both paths
produce Arrow. One warmup and three measured runs alternate engine order and report best times.
Partial-update cases alternate null string and array columns so output combines different commits,
with default, ignore-delete and whole-record-delete policies. The sequence case orders by this
payload so earlier commits can win; first-row ignores input retracts, and the dynamic case uses two explicitly assigned buckets. Counts and checksums must
match, and every native run must exercise the merger.

| Parquet case | Commits | Java merge to Arrow | Native merge to Arrow | Throughput ratio |
|---|---:|---:|---:|---:|
| INT | 1 | 0.042 s | 0.018 s | 2.40× |
| INT | 4 | 0.072 s | 0.041 s | 1.75× |
| INT | 8 | 0.122 s | 0.078 s | 1.57× |
| DECIMAL(38,2) | 4 | 0.088 s | 0.044 s | 1.99× |
| TIMESTAMP(6) | 4 | 0.065 s | 0.042 s | 1.55× |
| VARBINARY(4) | 4 | 0.079 s | 0.046 s | 1.72× |
| DATE | 4 | 0.070 s | 0.039 s | 1.77× |
| FLOAT | 4 | 0.067 s | 0.040 s | 1.69× |
| DOUBLE | 4 | 0.067 s | 0.043 s | 1.58× |
| First-row + ignore-delete (INT) | 4 | 0.074 s | 0.038 s | 1.96× |
| User sequence (INT) | 4 | 0.082 s | 0.036 s | 2.24× |
| Dynamic buckets (INT) | 4 | 0.072 s | 0.037 s | 1.97× |
| Partial update (INT) | 4 | 0.071 s | 0.040 s | 1.78× |
| Partial update + remove-record-on-delete (INT) | 4 | 0.069 s | 0.038 s | 1.82× |
| Partial update + ignore-delete (INT) | 4 | 0.069 s | 0.039 s | 1.79× |

Four-commit cases with ORC and a `256 mb` admission budget:

| ORC case | Java merge to Arrow | Native merge to Arrow | Throughput ratio |
|---|---:|---:|---:|
| FLOAT | 0.064 s | 0.038 s | 1.68× |
| DOUBLE | 0.062 s | 0.036 s | 1.72× |
| First-row + ignore-delete (INT) | 0.066 s | 0.032 s | 2.06× |
| User sequence (INT) | 0.066 s | 0.032 s | 2.03× |
| Dynamic buckets (INT) | 0.059 s | 0.027 s | 2.18× |
| Partial update (INT) | 0.063 s | 0.035 s | 1.79× |
| Partial update + remove-record-on-delete (INT) | 0.061 s | 0.033 s | 1.83× |
| Partial update + ignore-delete (INT) | 0.064 s | 0.034 s | 1.89× |

These release measurements describe local snapshot catch-up, not whole-job or remote-storage
performance. Run with:

```bash
SF_PAIMON_SNAPSHOT_BENCHMARK=true mvn test -Pbench,paimon \
  -pl :streamfusion-paimon -am -Dtest=PaimonSnapshotMergeBenchmark \
  -Dsurefire.failIfNoSpecifiedTests=false
```

`SF_PAIMON_SNAPSHOT_ROWS` changes the key count; `SF_PAIMON_SNAPSHOT_CASES` selects comma-separated
case names (for example `partial-update,partial-delete,partial-ignore` for the new partial-update cases).
`SF_PAIMON_FILE_FORMAT=orc` selects ORC. `SF_PAIMON_SNAPSHOT_BUDGET` sets
`sort-spill-buffer-size` for both readers (default `64 mb`). The 65,536-key multi-run ORC fixture
exceeds the default footer admission budget; the ORC measurements use `256 mb`. Native defaults
and fallback gates are unchanged; the eight-run ORC control still falls back at `256 mb`.
A benchmark fails if its native run falls back to Java.

### Source admission and fallbacks

Only streaming data-table reads enter this path. It uses the sink's supported value types below
(including nested values and timestamps up to precision 6). These configurations retain the stock
source at planning time:

- Primary-key tables without a changelog producer, table/changelog formats without an installed native Parquet or ORC module, thin data
  files, data evolution, row tracking, chain tables, query authorization, and exposing internal
  key-value sequence numbers.
- Nested subfield pruning, zero-column projections, metadata columns, pushed limits/aggregates,
  and source abilities other than top-level projection, residual filters, and supported watermarks.
- Consumer retention (`consumer-id`), dedicated split generation, checkpoint/snapshot alignment,
  and `postpone.merge-on-read`.
- Source watermarks outside the shared periodic constant-interval expression contract, including on-event
  emission and watermark alignment. Non-negative day-time and YEAR/MONTH/YEAR TO MONTH delays
  are supported, including chained subtractions; calendar intervals use Flink's month-end and
  leap-year arithmetic. Serialized expression plans are evaluated by each reader through the
  shared watermark evaluator, with native handles closed when the reader closes.
- Unverified `scan.*`, `streaming-read-*`, `log.*`, and custom `parquet.*`/`orc.*` settings. ORC also admits the boolean `orc.timestamp-ltz.legacy.type`; timestamp schemas require a UTC JVM timezone. The admitted scan settings are
  `scan.mode`, `scan.snapshot-id`, `scan.timestamp-millis`, `scan.timestamp`, `scan.tag-name`,
  `scan.watermark`, `scan.bounded.watermark`, `scan.parallelism`, `scan.infer-parallelism`,
  `scan.infer-parallelism.max`, `scan.remove-normalize`, and supported watermark idle/emit settings.

`streamfusion.operator.paimonSource.enabled=false` disables the substitution. The selected Parquet or ORC module
must be installed alongside the Paimon module, as for the sink. Native format-factory discovery
is not required for this reader: it calls the decoder directly on Java-planned files.

Repeated compatible native scans share one reader, including scans with different top-level
projections and scans feeding different sinks in a statement set. The deployed streaming planner
lets Flink union their projected columns before native substitution, then places the reader under
an explicit Arrow share operator. Every branch takes a retained buffer view, preserving row kinds
and the normal source watermark/checkpoint flow. Parquet and ORC use the same sharing path.
When a watermarked split finishes, the reader flushes its final maximum watermark candidate
through Flink's split output before releasing it. Candidates are calculated per row before taking
the maximum, including for calendar delays, and carried separately from event timestamps.
Otherwise a tiny native file
can finish before a periodic tick and lose its final watermark. Active files retain periodic
emission, and Flink still combines concurrent splits and handles idleness.
Filters, limits, startup hints, table options, schemas and other scan semantics must agree;
different scans remain independent. A branch that falls back to Flink is excluded from the native
consumer count. Disabling `streamfusion.plan.shareSources`, `table.optimizer.reuse-source-enabled`
or `table.optimizer.reuse-sub-plan-enabled` restores independent native readers.

`PaimonSourceSharingTest` checks three-sink projection union, identical projections, repeated
compilation, disabled sharing, distinct scans, mixed native/Flink branches, and snapshot-to-tail
results including deletes and nulls for both formats, with chained and network edges. It also
checks window closure on every shared branch after a new commit.
Paimon's unchanged `ContinuousFileStoreITCase.testSourceReuseWithScanPushDown` passes its `Reused`
assertion and its filter/limit separation assertions. Cross-sink sharing requires the deployed
planner hook; installing a program into an already constructed stock planner still optimizes each
root separately. Other source gaps remain in [#27](https://github.com/datafusion-contrib/StreamFusion/issues/27).

`PaimonSourceReadTest` checks append/primary-key twin readers, row kinds, nested values, partition
values, historical schema mapping, projections, `latest` startup and within-batch resume.
`PaimonSourceRecoveryTest` checkpoints the asynchronous source reader after emission and restores
with Paimon's serialized split state. `PaimonSourceSqlTest` follows an initial snapshot and a later
commit through streaming SQL, including source watermarks; `PaimonSourceAdmissionTest` checks fallback
and watermark admission.

### Source file-reader diagnostic

The opt-in `PaimonSourceBenchmark` compares three paths on 262,144 rows and four projected columns,
including a nested array: released Java reading to rows, Java reading followed by conversion to
Arrow, and native Parquet/ORC reading to Arrow. Files are generated before timing. Each read
includes file open/close, decode, and an id-column checksum; native reads also include the Java
FileIO/JNI transfer and Arrow import. The Java-to-Arrow baseline uses the existing fallback's row
ownership copy and converter, with the same 4,096-row batch size and changelog sidecar as the native
reader. Both Arrow paths materialize every projected column and checksum the resulting id vector.
The Java row scan checksums the id directly and does not construct Arrow output.
It verifies row counts and checksums across all three paths. One warmup and three measured runs
rotate execution order and report each path's best time. `speedup` compares native with Java rows;
`arrow_speedup` compares native with Java-to-Arrow. Both Arrow endpoints in this older diagnostic
are **Java Arrow**, including a native-to-Java import; they do not directly measure feeding our Rust
operators. The [ORC reader comparison](orc.md#reading-into-arrow-rs) ends the production Rust reader and Java baseline at
an arrow-rs `RecordBatch` consumed in Rust.
This is a local file-reader diagnostic, not an end-to-end Flink or remote-storage benchmark.

Local Apple Silicon measurements with release libraries and mimalloc:

| Format | Path | Java rows | Java to Arrow | Native to Arrow | Native / Java-to-Arrow throughput |
|---|---|---:|---:|---:|---:|
| Parquet | Append files | 0.032 s | 0.130 s | 0.026 s | 5.00× |
| Parquet | Primary-key changelog files | 0.035 s | 0.124 s | 0.019 s | 6.70× |
| ORC | Append files | 0.027 s | 0.104 s | 0.057 s | 1.82× |
| ORC | Primary-key changelog files | 0.020 s | 0.095 s | 0.047 s | 2.01× |

ORC remains slower than Java when the consumer only needs the row checksum. At the Arrow output
boundary both native formats beat the current Java-to-Arrow path in this diagnostic. This does not
isolate codec speed from representation conversion or establish whole-job throughput.

Run with release native libraries:

```bash
SF_PAIMON_SOURCE_BENCHMARK=true mvn test -Pbench,paimon \
  -pl :streamfusion-paimon -am -Dtest=PaimonSourceBenchmark \
  -Dsurefire.failIfNoSpecifiedTests=false
```

`SF_PAIMON_SOURCE_ROWS` changes the input count; `SF_PAIMON_FILE_FORMAT=orc` selects ORC.

## Streaming sink

A sink's Arrow batches are split natively into one sub-batch per `(partition, bucket)` pair. Rust
computes the partition `BinaryRow` and Paimon's default bucket hash column-wise (Paimon's row
layout and Murmur hash are Flink's, so the native key encoder already produces both), and the
routed batches are shuffled while still Arrow with Paimon's own channel formula. Each batch then
enters Paimon's bundle write entry point for its bucket and reaches a StreamFusion
`FileFormatFactory` registered under the selected `parquet` or `orc` identifier, whose writer encodes the whole batch
with parquet-rs or Paimon's Java vectorized ORC writer over Paimon's output stream. Paimon reads statistics
from the resulting footer. Native Parquet writers additionally collect top-level floating-point
bounds with Paimon's collector and pass them through its writer-metadata API: parquet-rs excludes
NaN from footer bounds, whereas Paimon's manifest bounds include it. No Java rows are materialized
for this column-wise collection. Fixed-length `BINARY(n)` uses Paimon's
`BYTE_ARRAY` encoding, including when nested; Arrow's fixed-size buffers are converted at the
writer boundary.

Supported:

- Bucket-unaware tables (`bucket = -1`), partitioned or not, with `partition.sink-strategy` `none`
  or `hash`, including Paimon's in-job compaction coordinator and workers.
- Fixed-bucket append tables (`bucket > 0` with `bucket-key`), partitioned or not, with the
  `sink.parallelism` and small-bucket-count parallelism rules of the stock sink.
- `file.format = orc` with NONE/ZLIB/SNAPPY/LZ4/ZSTD compression, Paimon block/level mappings,
  and the [verified ORC writer settings](orc.md#configuration-and-types).
- `file.format = parquet` with `file.compression` `none`/`snappy`/`gzip`/`zstd` (and
  `file.compression.zstd-level`), `file.block-size`, and the `parquet.*` writer keys the stock
  writer honours: page and dictionary-page size, dictionary encoding, writer version.
- Column types `BOOLEAN`, `TINYINT`..`BIGINT`, `FLOAT`, `DOUBLE`, `DECIMAL`, `CHAR`/`VARCHAR`,
  `BINARY`/`VARBINARY`, `DATE`, `TIMESTAMP`/`TIMESTAMP_LTZ` up to precision 6, and `ARRAY`, `MAP`,
  `ROW` of those, recursively, with Paimon's field ids on every column.
- Hint options (`/*+ OPTIONS(...) */`) and the `paimon.<catalog>.<db>.<table>.<option>` dynamic
  options from the job configuration, resolved the way Paimon's own factory resolves them.
  Statement hints are read explicitly from the physical sink, including on Flink 1.18 where
  the resolved catalog table retains its original options. They affect both admission and the
  writer destination: a `branch` hint must leave the main branch unchanged. Table-scoped job
  configuration overrides the hinted options, matching Paimon's factory precedence.
- `sink.writer-refresh-detectors`: the writer re-reads the refreshed option groups (external data
  paths) after each checkpoint's commit preparation, exactly when the stock operator does.
- Any insert-only query shape: columns bind to the table by position as in Flink's own sink, so
  aliased projections (`SELECT a AS x ...`) and casts the planner inserts are written under the
  table's names and nullability, and an insert-only stream coming out of a changelog-capable
  operator (a join, an aggregate) is accepted with its hidden row-kind column dropped.

### Append buffering and local spill

Each active table-partition/bucket pair needs a file writer. To limit encoder memory when a task
touches many destinations, Paimon switches append writers to buffering when opening a writer would
exceed `write-max-writers-to-spill` (default 10). StreamFusion uses the same threshold, finishes the
existing native files, and retains subsequent Arrow batches in Rust. The mode stays enabled for
the rest of the writer's life, including after checkpoints and writer-option refreshes.

When retained Arrow memory exceeds `write-buffer-size`, the largest bucket spills to an Arrow IPC
stream in Flink's task-local spilling directories. Spills support `spill-compression = lz4`, `lzo`,
and `zstd` (the default), including `spill-compression.zstd-level` (default 1). LZO uses Paimon's
Java compressor on 64 KiB IPC byte blocks and a native decoder, with reusable codec buffers;
the data stays columnar and a whole spill need not fit in memory. These settings control
temporary spill files; `file.compression` controls final data files. At a checkpoint the writer drains each
bucket's spills and then its in-memory batches in arrival order, through the native Parquet encoder.
Only one bucket's encoder is open during this drain. Paimon still owns file metadata, compaction,
checkpoint state, and commits. Local spills are temporary: drain and cancellation delete them;
recovery uses checkpointed Paimon files and source replay.

`write-buffer-spill.max-disk-size` is a soft per-bucket limit, checked before another spill as in
Paimon. A full disk allowance flushes the bucket to final data files. Memory pressure is checked
after accepting a batch, so the budget can temporarily be exceeded by one incoming batch and
encoding/IPC working memory. Automatic writer-count spilling enables disk spill even when
`write-buffer-spillable = false`, as Paimon's automatic transition does.

`sink.use-managed-memory-allocator` defaults to `false`, giving the sink an independent buffer
budget controlled by `write-buffer-size`. Enabling it obtains the buffer budget from Flink's
managed memory pool. Retained native Arrow buffers reserve their byte counts through
Flink's `MemoryManager`, use the operator's assigned managed-memory share, and spill the largest
bucket if the share or the available reservation is exhausted. Checkpoint drains and close return
the reservations. This accounts for retained buffers; incoming batches, sort/encode scratch space,
and file-encoder allocations remain transient off-heap allocations. Paimon's Java compactor retains
its own managed-memory allocator. Both append and primary-key sinks support this setting.

Paimon rereads and rewrites unfinished files at the transition; StreamFusion retains those valid
files. This avoids a row conversion and repeated encoding, but file boundaries, per-file statistics,
and absolute sequence numbers can differ from a stock run after the transition. Rows, destination
buckets, schema, codecs, and sequence-number progression within each bucket writer are preserved.
The focused `NativeAppendSinkWriteTest` checks multiple checkpoints, writer refresh/reopen, disk
limits, native footers, and spill cleanup; `PaimonSinkParityTest` covers SQL, coordinator commits,
and checkpoint failure/recovery.
The [release spill diagnostic](../optimizations/paimon-append-spill.md) measured **1.12× throughput**
against the previous native path that reverted to Java spilling and encoding.

### Primary-key tables

A fixed- or dynamic-bucket primary-key table takes the changelog Flink infers for it (`+I`/`+U`/`-D`; Paimon's
sink declares that it needs no `UPDATE_BEFORE`). The routed batches keep their row kinds and are
held per bucket in a native buffer. At a checkpoint, or once a task's buffers exceed
`write-buffer-size` (largest bucket first, as Paimon's memory pool spills), a bucket's rows are
sorted by key, optional user sequence, and arrival, reduced by the table's merge engine,
and written straight into level-0 data files in Paimon's
key-value layout with the native Parquet encoder, rolled at `target-file-size`. Every file carries
the metadata Paimon's own writer records: key bounds, key and value statistics from the format,
sequence range, delete count, level 0. Sequence numbers continue from the bucket's committed files
exactly as a restored Paimon writer's do, so a native run numbers its rows like a stock run.

Compaction stays Paimon's, in the same job: the new files are handed to Paimon's merge-tree writer
for the bucket before it prepares each checkpoint's commit, through the entry Paimon's dedicated
compaction operator uses for files written elsewhere, so the writer compacts them with the table's
own strategy (`num-sorted-run.compaction-trigger`, `compaction.*`, `commit.force-compact`) and the
rewrites run through Paimon's stock Parquet writer as they do today. Paimon's writer sees no rows,
only files; when it is idle across checkpoints Paimon closes it and the next hand-off recreates it
with a scan of the bucket's committed files, the same cost Paimon's dedicated compactor pays. With
`write-only = true` the hand-off is inert and a dedicated compaction job (`CALL sys.compact` or a
compaction action) picks the level-0 files up unchanged.

Supported: `bucket >= 1` with the default or an explicit `bucket-key`, or dynamic `bucket = -1`, `merge-engine =
deduplicate`/`first-row`/`partial-update`/`aggregation`,
`changelog-producer` `none`/`input`/`lookup`/`full-compaction`, `ignore-delete`, `ignore-update-before`,
`write-only`, `file.compression*` and
the selected format's writer keys as for append tables, and key columns of type `BOOLEAN`,
`TINYINT`..`BIGINT`, `DECIMAL`, `CHAR`/`VARCHAR`, `BINARY`/`VARBINARY`, `DATE`, `TIMESTAMP`, and
`TIMESTAMP_LTZ` (the native sort orders keys by their Arrow byte encoding, which agrees with
Paimon's key comparator for exactly these types). An insert-only stream into a primary-key table
is taken as all inserts.

#### Primary-key writer options

`data-file.thin-mode = true` writes only sequence number, row kind, and table values, omitting
duplicate `_KEY_*` columns from both data and input-changelog files. The encoder borrows the
existing value vectors; key bounds still come from the sorted keys, and key statistics map to the
corresponding value fields with Paimon's key-statistics policy. Java compaction reads and rewrites
these files normally. Native source admission for thin files remains a separate source limitation.

`data-file.external-paths` and `data-file.external-paths.strategy` use Paimon's path factory and
persist the full external path in each file's metadata. Writer option refresh takes effect after
checkpoints. `write.sequence-number-init-mode = snapshot` starts from the larger of the restored
bucket maximum and the snapshot's generated-sequence property. Write-only mode skips the bucket
scan when that property exists, and scans older snapshots that lack it, matching Paimon.

`sink.key-only-deletes.enabled` retains Paimon's changelog negotiation: supported upsert inputs
may supply keys with null value fields on deletes. Paimon ignores this negotiation flag for input
changelogs, aggregation, and partial updates with aggregates. `precommit-compact` retains the
stock changelog compaction coordinator, workers, and creation-time sort before commit. Its buffer
and thread settings are parsed by those Java operators.

`local-merge-buffer-size` combines updates by the full table primary key, including partition
columns, before the bucket shuffle. With `changelog-producer=none`, `deduplicate` and `first-row`
retain Arrow batches in the existing native sink merger, compare keys and user/arrival sequences,
and gather surviving values directly into Arrow columns. The outgoing vectors transfer ownership
without a Java row conversion. The configured size bounds retained input at batch boundaries;
the batch that reaches the limit triggers a flush, so buffering can exceed the target by one input
batch. Arrow storage can produce different intermediate flush groups from Java's row buffer.

Field merges (`partial-update`, `aggregation`) and every changelog-producing mode retain Paimon's
Java local merger and its exact buffer grouping. Those inputs are exposed as Arrow row views,
copied into Paimon's row buffer, and merged results are written into new Arrow batches. This pays
a row conversion cost to preserve aggregation/retraction results and intermediate input changelogs.
Both paths keep row kinds, hold watermarks behind pending rows, flush before checkpoint barriers
and end of input, and release uncommitted buffers on cancellation. Batch clustering remains on stock
Paimon. `PaimonLocalMergeTest`, `PaimonLocalMergeBoundaryTest`, and `PaimonLocalMergeNativeTest` cover
SQL writes and reopened jobs, released Java buffer boundaries, and Arrow ownership/checkpoint restore.

### Merge engines and input ordering

- `deduplicate` keeps the last row in merge order; `first-row` keeps the first. Paimon's normal
  first-row restrictions still apply, including its lookup producer and delete handling.
- For `deduplicate`, `sequence.field` compares one or more value columns before the arrival number, including
  ascending or descending `sequence.field.sort-order`. Nulls sort first in both directions;
  equal user sequences are resolved by arrival. The stored `_SEQUENCE_NUMBER` continues to count
  accepted arrivals. Sequence columns admit the primary-key types above plus `FLOAT` and `DOUBLE`.
  Floating comparisons match Java's ordering of signed zero, infinities, and canonicalized NaNs.
- `rowkind.field` reads `+I`, `-U`, `+U`, or `-D` from a string column. Filtering of ignored deletes
  and update-before rows happens after this override and before assigning sequence numbers.
  Null or invalid kind strings fail as in Paimon.
- `partial-update` merges non-null values. Sequence groups (`fields.<sequence columns>.sequence-group`)
  can replace protected values with null, retract only their columns, and use the supported field
  aggregates. Both `partial-update.remove-record-on-delete` and
  `partial-update.remove-record-on-sequence-group` follow the released writer's behavior.
- `aggregation` reduces each field with its configured aggregate. Supported functions are `sum`
  and `product` on integer, floating-point and decimal columns;
  `min`/`max` on the comparable types above, including floats; `bool_and`/`bool_or`; string `listagg`
  with `fields.<column>.list-agg-delimiter` and `fields.<column>.distinct`; and `first_value`, `last_value`,
  `first_non_null_value` (also `first_not_null_value`), and `last_non_null_value` on all supported
  value types, including nested columns. Paimon's default aggregate selection,
  `fields.<column>.ignore-retract`, and `aggregation.remove-record-on-delete` are preserved.
- Specialized `collect`, `merge_map`, `merge_map_with_keytime`, `nested_update`,
  `nested_partial_update`, `hll_sketch`, `theta_sketch`, `rbm32`, and `rbm64` use the released Java
  field aggregators. Rust retains key grouping and ordinary field reductions, and passes each
  specialized column's operations to Java in one C Data call per flush. This preserves Paimon's
  collection ordering, nested-field options, and serialized sketch formats. It adds no dependency
  on paimon-rust and does not claim these Java field computations are native.
  Callback failures retain the original Java exception across Arrow cleanup, following Comet's
  retained-throwable pattern. Unsupported retractions and malformed sketches fail as in Paimon,
  even when a later delete would discard the intermediate result.
- Column defaults are parsed by released Java Paimon and fill null values in Arrow before merging.
  Defaults on primary-key, partition or bucket-key columns fall back because they affect routing.

As in Paimon's level-0 writer, a key with just one buffered row passes through without invoking
the merge function. This preserves its original row kind and values. Multiple-row partial and
aggregate reductions produce the same insert/delete kind as Paimon. Compaction and subsequent
reads apply Paimon's merge functions to these files.

`PaimonMergeEngineTest` compares native and stock writers over checkpoints and restarts, including
file sequence ranges, delete counts, nested values, defaults, input changelogs and compaction.
`PaimonSinkParityTest` also checks streaming SQL admission and results for each feature.
`PaimonMergeAdditionalTypesTest` covers floating ordering and distinct concatenation;
`PaimonSpecializedAggregateTest` compares collections, nested values, maps, and serialized sketches
across checkpoints, compaction, and reopened writers on both file formats.
`PaimonFieldAggregatorTest` checks released exceptions and Arrow cleanup on failure.
A focused run of Paimon's unchanged SQL tests also covers collection retractions with lookup and
full-compaction changelogs, map retractions, and the stock streaming-read restrictions for product
and listagg tables without a changelog producer.

### Dynamic buckets

For primary-key tables with `bucket = -1`, the first native shuffle computes Paimon's assigner
channel from the partition and trimmed primary-key hashes. Paimon's released `HashBucketAssigner`
assigns bucket IDs from those hashes, and a second native gather and Arrow shuffle sends each
partition/bucket batch to its writer. `dynamic-bucket.target-row-num`,
`dynamic-bucket.assigner-parallelism`, `dynamic-bucket.initial-buckets`, and
`dynamic-bucket.max-buckets` retain Paimon's behavior.

Each native flush also supplies its distinct keys to Paimon's `DynamicBucketIndexMaintainer`.
The hash-index files are committed with the data files, so updates after restart or rescaling
return to the persisted buckets. Assigner commit-user state and checkpoint cleanup follow
Paimon's Flink operator. The changelog producers, deletion vectors, and writer options supported
for fixed buckets also apply here. Cross-partition primary keys (`KEY_DYNAMIC`) still fall back.

### Postpone buckets

Streaming `bucket = -2` tables write every accepted change in arrival order, without sorting or
deduplication, with Paimon's unknown sequence number (`-1`). The shuffle follows Paimon's
partition/primary-key channel formula, or its partition-only routing with
`partition.sink-strategy = hash`. Files retain Paimon's commit-user and writer-ID prefix so its
compactor can replay each writer's input in order. `ignore-delete` still filters retracts.
Creation times increase by at least one millisecond per partition/writer, even when files roll
within one clock tick. Recovery continues after that writer's committed staging files, so neither
manifest scan order nor a stalled clock can reorder its changes.

Native postpone files use the configured Parquet or ORC format. Stock Paimon 2.0.0 normally
chooses Avro internally for these staging files; Paimon's released readers and compactor
accept the configured native formats too. Native staging therefore differs in physical encoding while preserving the records,
row kinds, replay order, and commit protocol. It uses the selected native format's option whitelist.

Postpone ingestion does not produce the final merged table or changelog. Paimon's dedicated
compaction job (`CALL sys.compact` in batch mode) assigns real buckets and applies the configured
merge, changelog, and deletion-vector behavior. Default reads omit uncompacted postpone files;
Paimon's batch `postpone.merge-on-read` reader can include them. The native writer preserves the
stock stateless writer and restore-only committer lifecycle. Batch writes and primary keys that
omit partition columns remain outside the native whitelist.

### Changelog producers and deletion vectors

For fixed and dynamic buckets:

- **Input:** the same native sort returns every retained input row before deduplication, ordered
  by primary key, user sequence when configured, and then arrival sequence. These rows, including `UPDATE_BEFORE` and deletes,
  are encoded into separate Parquet changelog files and committed through Paimon's changelog
  manifests. They are not data-file extras, and their rolling boundaries are independent of the
  merged data files. `ignore-delete` removes retracts before both outputs and before numbering.
  `changelog-file.format` matching the table format (`parquet` or `orc`), `changelog-file.compression`, and
  `changelog-file.stats-mode` follow Paimon's writer settings; compression has the same native
  whitelist as data files.
- **Lookup and force-lookup:** native level-0 files enter Paimon's selected lookup writer.
  Paimon manages previous values and compaction-produced changelogs, and applies `lookup-wait`
  and its lookup compaction strategy. Its active-bucket checkpoint state is preserved, so recovery resumes
  unfinished lookup even if no more input arrives.
- **Full compaction:** Paimon's own global full-compaction writer tracks modified buckets and
  schedules full compaction using `full-compaction.delta-commits` or
  `changelog-producer.compaction-interval`, including idle checkpoints and restored buckets.
  These scheduling options also work with `changelog-producer = none` or `input`.
  Ordinary table compaction can still run between scheduled full compactions, as in stock Paimon.
- **Deletion vectors:** for `deduplicate` tables with `none`, `input`, or `lookup`, Paimon's
  lookup compactor produces the deletion vectors and its index metadata is retained in the
  checkpoint commit. Read visibility follows Paimon's options: uncompacted level-0 files are
  hidden by default in deletion-vector tables; `deletion-vectors.merge-on-read` includes them.

`write-only` retains Paimon's normal behavior: input changelog is written immediately, while
lookup, full compaction, and deletion-vector maintenance are left to a separate compaction job.
The native sink uses the released Java connector for these lifecycles; see
[the writer boundary and paimon-rust assessment](https://github.com/datafusion-contrib/StreamFusion/blob/main/divergences/32-paimon-pk-l0-through-the-compactor-hook.md).

### Parity

Twin-table tests compare logical rows, key/value statistics, sequence and row-kind metadata,
and footer schemas against the stock writer in `PaimonSinkParityTest`,
`NativePaimonParquetWriterTest`, `NativePaimonOrcTest`, `NativePaimonKeyValueFileWriterTest`,
`NativeKeyValueSinkWriteTest`, and `PaimonChangelogSinkWriteTest`. Compressed sizes can differ;
size-based rolling and compaction still follow Paimon's decisions on the actual files. Postpone
staging, append-buffer transitions and random `PARTITION_DYNAMIC` routing have the additional
physical-file differences described on this page.
`bin/flink-suite.sh paimon` runs Paimon's own unchanged append-table SQL integration tests with the
native sink installed (see [the upstream suite](../upstream-flink-suite.md)). Parquet's physical
floating-point footer bounds can differ for NaN and signed zero; Paimon manifest statistics use
the collector described above. `PaimonValueTypesTest` checks 40 scalar edge-value cases across
both formats, including native snapshot reads, NaN/infinities, signed zero, integer extremes,
precision-38 decimals, Unicode, binary and dates.

The regular CI Paimon job runs SQL parity for dynamic and postpone buckets, including reopened
jobs, assigner/writer rescaling, and Paimon's SQL compaction of native postpone files. A recovery
harness discards an uncommitted dynamic checkpoint, restores the assigner state, replays the
input, and compares the resulting hash-to-bucket index and changelog with stock Paimon. A larger
SQL fixture verifies postpone replay across rolled files. These are ordinary correctness tests;
the performance diagnostics below are opt-in.
That rolled-file fixture uses stock fixed-bucket ingestion as its arrival-order oracle: released
Java postpone writers can give two files the same millisecond creation time and replay them out
of order. Native postpone files use strictly increasing creation times as described above.

### Dynamic partition routing and clustering options

Partitioned bucket-unaware append tables support `partition.sink-strategy = PARTITION_DYNAMIC`.
Paimon's Java statistics operator reports partition row counts at checkpoints; its existing
coordinator aggregates and broadcasts them, and its weighted channel selector distributes each
partition across writers. Before statistics arrive, a partition uses up to four writers. Recovery
starts collecting fresh statistics, just as in stock Paimon. Only the partition keys enter this
Java logic; payloads stay in Arrow batches, split natively by the selected channel. The writer
still receives Paimon's unaware bucket 0, independently of its shuffle channel.

The choices are random in stock Paimon, so individual file contents, sizes, and statistics may
differ between equivalent executions. Table rows, partition keys, bucket identity, footer schema,
and codecs retain parity. Tests cover statistics replacement, null partitions, row order within
each channel, different writer parallelisms, watermarks, and recovery after a completed checkpoint.
Other bucket modes and unpartitioned tables retain their normal routing, as in stock Paimon.

`sink.clustering.*` (including the `clustering.columns` and `clustering.strategy` names) does not
force fallback. Paimon 2.0.0 skips range clustering in **STREAMING execution mode**, even for a
bounded source, so these options do not insert a shuffle or sort. Typed option values are still
parsed as in Paimon, so malformed booleans and sample factors remain errors. Incremental-clustering
table settings retain Paimon's Java write and compaction behavior. StreamFusion accelerates only the
streaming planner; batch range clustering and separate clustering jobs remain stock Paimon.

### Writer and commit coordinators

Both coordinator options retain Paimon 2.0.0's Java implementations while the data writer consumes
Arrow batches. No coordinator logic is implemented in Rust.

`sink.writer-coordinator.enabled = true` selects Paimon's coordinated restore factory for fixed
and dynamic buckets. The JobManager serves restored files through Paimon's paged requests and
metadata cache. The native primary-key writer uses this same restore service to continue sequence
numbers; Paimon's selected writer retains compaction, changelog, and deletion-vector restoration.
As in stock Paimon, unaware append and postpone writers do not use the writer restore coordinator.

`sink.coordinator-commit.enabled = true` selects Paimon's committing writer coordinator for
bucket-unaware append tables. Its existing writer lifecycle owns pending committables, checkpoint
events, watermark and idle status, replay, and the deliberate restart after recovering an
uncommitted snapshot. The native subclass replaces only record ingestion with bundle ingestion.
Paimon's sink builder removes the downstream global committer and retains its configuration checks:
streaming checkpointing must be enabled, `write-only = true`, `precommit-compact = false`,
`sink.savepoint.auto-tag = false`, and at most one checkpoint may run concurrently. This does
not add bounded end-input commit support beyond the released connector. The option is ignored by
Paimon's fixed-bucket and primary-key sinks, and StreamFusion preserves that behavior.

SQL parity tests reopen fixed and dynamic primary-key tables with paged coordinated restoration,
including input/lookup/full-compaction changelogs and deletion vectors, and compare append-file
metadata and sequence numbers across jobs. SQL failover tests replay append and fixed/dynamic
primary-key writes after a completed checkpoint. The commit recovery harness compares stock and native
rows and snapshot histories after a lost checkpoint and replay, including an already-committed
snapshot whose acknowledgement was lost. The upstream SQL suite includes Paimon's unchanged
`CoordinatorCommitITCase` for topology, commit metrics, committed rows, and idle-watermark parity.

## Sink falls back to stock Paimon on

Each of these declines at planning time with a reason visible in `NativePlanner.explain`:

- Cross-partition dynamic keys (`KEY_DYNAMIC`) and postpone primary keys that omit partition columns.
- A changelog (retracting or updating) input into an append table, a sink Flink plans with a
  `SinkUpsertMaterializer` (`table.exec.sink.upsert-materialize`; Paimon itself refuses that
  operator), `INSERT OVERWRITE`, and batch-mode inserts (the substitution only exists in the
  streaming planner).
- Primary-key tables with field aggregate/type combinations outside the merge whitelist above;
  sequence fields or sequence groups outside the
  comparable-type whitelist; `sequence.field` combined with `partial-update` or `aggregation`;
  defaults on routing columns;
  input changelog with `changelog-file.format` differing from the table format or unsupported changelog
  compression; primary-key vector, full-text, BTree, or bitmap indexes;
  or a `FLOAT`/`DOUBLE` key column.
- `file.format` other than installed `parquet` or `orc`, `file.format.per.level`, `write-buffer-for-append = true`,
  file indexes (`file-index.*`), `row-tracking.enabled`, `data-evolution.enabled`, `BLOB` columns.
- Append tables with `spill-compression` other than `lz4`/`lzo`/`zstd`.
  Released Paimon 2.0.0 fails when actually spilling with `none`; it remains on the stock path.
- A nullable query field assigned to a `NOT NULL` target, or a bounded `CHAR`/`VARCHAR` or
  `BINARY`/`VARBINARY` target while `table.exec.sink.type-length-enforcer` is enabled. The stock
  sink path preserves Flink's configured fail/drop and trim/pad/error behavior.
- `TIMESTAMP` precision above 6 (Paimon writes INT96 there), `VARIANT`, vector, and geospatial
  types.
- `parquet.*` keys the native writer cannot honour: bloom filters, page validation, custom
  padding, page row-count limits, statistics/column-index truncation, page-size row checks,
  multithreaded zstd, and any per-column (`parquet.*#column`) or unrecognised writer key.
- The selected format identifier resolving to Paimon's own factory (see deployment below).

Compaction rewrites (in-job or from a dedicated compaction job) still use Paimon's stock Parquet
writer. Append buffer spilling retains native encoding as described above. Primary-key buckets
use their existing native buffers, which flush by size into level-0 files.

## Benchmark

The measurements below use Parquet files. [ORC diagnostics](orc.md#build-and-verification)
report that format's reader and writer results separately.

On the 2M-event, four-partition Kafka JSON Nexmark sink diagnostic (memory state, mini-batching off,
one warmup, best of three), the 16 append-only queries completed on both engines and StreamFusion's
suite geomean was **1.47×** the stock published-Paimon path for bucket-unaware tables with in-job
compaction and **1.47×** for four fixed buckets, from 1.08× on a join that emits a few hundred rows
to 2.04× on the query that writes 5.5 M joined rows. Row counts read back through Paimon's snapshots
agree on every query except the processing-time window q12, whose output is non-deterministic by
construction.

The seven updating queries (q4, q9, q15–q19) ran against `deduplicate` primary-key tables with four
fixed buckets and Paimon's default in-job compaction on the same diagnostic: StreamFusion's suite
geomean was **1.64×** the stock path, from 1.18× on q9 (updates concentrated on 120 K keys) to
2.38× on q16 (eight keys rewritten a million times), and the merged row counts read back through
Paimon agree on every query. Stock Paimon pays for its row-at-a-time sort buffer; the native sink
merges each bucket's routed Arrow batches by key in Rust and writes the level-0 files directly. See
[Benchmarks](../benchmarks.md#parquet-delta-and-paimon-sink-diagnostics) for the method and
reproduction command.

### Changelog writer diagnostic

The opt-in writer diagnostic exercises `input`, `lookup`, `full-compaction`, and deletion-vector
writes against stock Paimon, with one warmup and best of three measured runs. It starts with the
same row fixture, includes routing and the native path's RowData-to-Arrow conversion, and measures
writing plus commit preparation and commit. Each run checks the merged table contents. This is a
writer diagnostic, not an end-to-end Nexmark result; the Nexmark harness is unchanged.
It measures one checkpoint into an empty table. It does not measure ongoing lookup against older
files or sparse-update deletion-vector maintenance; those are covered by the recovery parity tests.

```bash
SF_PAIMON_CHANGELOG_BENCHMARK=true mvn test -Ppaimon,bench \
  -pl :streamfusion-paimon -am -Dtest=PaimonChangelogSinkBenchmark \
  -Dsurefire.failIfNoSpecifiedTests=false
```

The default input is 200,000 changes over 20,000 keys, four buckets, and batches of 4,096 rows.
`SF_PAIMON_CHANGELOG_ROWS` changes the input size while retaining the ten-to-one change/key ratio.

Local release measurements with Paimon 2.0.0: the 1M-change run is from 2026-09-11;
the 200K-change run was repeated on 2026-09-13 with the shared file writer and floating statistics.

| Mode | Stock, 1M changes | Native, 1M changes | Speedup, 1M | Speedup, 200K |
|---|---:|---:|---:|---:|
| Input changelog | 3.716 s | 2.319 s | 1.60× | 1.92× |
| Lookup changelog | 2.387 s | 1.856 s | 1.29× | 1.28× |
| Full-compaction changelog | 1.276 s | 1.343 s | 0.95× | 0.93× |
| Deletion vectors enabled | 1.498 s | 1.122 s | 1.34× | 1.34× |

Full compaction is slower in this row-fed diagnostic. It retains Paimon's rowwise compaction and
adds the native path's conversion and file hand-off costs. Its admission adds coverage for columnar
pipelines; these measurements do not establish a throughput improvement for that mode.

### Merge-engine writer diagnostic

`PaimonMergeBenchmark` measures row routing, the RowData-to-Arrow conversion, merging, level-0
file encoding and commit against the released stock writer. It uses 131,072 rows, 16,384 keys,
two buckets and nine columns including nulls and nested arrays. `write-only` isolates ingestion
from compaction. Writer setup, close and result verification are outside the timer. After one
warmup per mode, three measured runs alternate engine order and report the best of each.

On the same local machine with release native libraries and Parquet:

| Mode | Stock | Native | Throughput ratio |
|---|---:|---:|---:|
| First row | 0.116 s | 0.093 s | 1.24× |
| Partial update with a sequence group | 0.145 s | 0.120 s | 1.21× |
| Aggregation with sum | 0.127 s | 0.106 s | 1.20× |
| Deduplicate with two sequence fields | 0.108 s | 0.085 s | 1.27× |

With `SF_PAIMON_MERGE_THIN=true` on the same 131,072-row fixture, stock/native seconds were
0.109/0.092 for first row, 0.140/0.118 for partial update, 0.124/0.106 for aggregation, and
0.104/0.087 for sequence-based deduplication: **1.17–1.20×** stock throughput with matching rows.

With `SF_PAIMON_MERGE_MODES=collect`, distinct array collection uses the released Java field kernel
inside the native writer. The final release fixture measured **0.212 s stock / 0.179 s native
(1.18×)** with Parquet and **0.190 / 0.187 s (1.02×)** with ORC, which is roughly tied.
Both include the ingress conversion and column callback and verify identical array contents and
ordering. These measure the whole writer path, not a faster implementation of Java's collection
function or a benchmark of every specialized aggregate. The environment variable accepts a
comma-separated subset of the modes above plus `collect`.

Every run verifies the final table contents against its stock twin. This is a writer diagnostic,
not an end-to-end Flink job benchmark. Run it with:

```bash
SF_PAIMON_MERGE_BENCHMARK=true mvn test -Pbench,paimon \
  -pl :streamfusion-paimon -am -Dtest=PaimonMergeBenchmark \
  -Dsurefire.failIfNoSpecifiedTests=false
```

`SF_PAIMON_MERGE_ROWS` changes the input count; `SF_PAIMON_FILE_FORMAT=orc` selects ORC.

### Dynamic and postpone SQL diagnostic

The opt-in bucket-mode diagnostic measures streaming SQL ingestion from the same row fixture,
including table creation, planning, job startup, RowData-to-Arrow conversion, shuffles, and commit.
It uses one source task, two sink writers, ten changes per key, one warmup, and the best of three
measured runs, alternating engine order. Dynamic buckets target 5,000 rows per bucket. Each run
compares the resulting table contents. For postpone tables, SQL compaction makes the rows visible
and validates replay **outside the ingestion timer**; these numbers do not measure the complete
ingestion-and-compaction lifecycle. Stock postpone staging uses Avro, while native staging uses
Parquet, as described above.

```bash
SF_PAIMON_BUCKET_BENCHMARK=true SF_PAIMON_BUCKET_ROWS=1000000 \
  mvn test -Ppaimon,bench -pl :streamfusion-paimon -am \
  -Dtest=PaimonBucketModeBenchmark -Dsurefire.failIfNoSpecifiedTests=false
```

Local release measurements on 2026-09-11 with Paimon 2.0.0:

| Mode | Stock, 1M changes | Native, 1M changes | Speedup, 1M | Speedup, 200K |
|---|---:|---:|---:|---:|
| Dynamic buckets | 3.025 s | 3.139 s | 0.96× | 1.05× |
| Postpone buckets | 2.618 s | 3.229 s | 0.81× | 0.89× |

These timings precede the final postpone creation-time ordering fix; its metadata counter and
restoration scan were validated separately and have not been rebenchmarked.

These row-fed results do not establish a throughput improvement. Dynamic assignment still calls
Paimon's Java index per key, and postpone staging pays for conversion and Parquet encoding without
the reduction in output rows that native deduplication normally provides. Admission adds coverage
for existing columnar pipelines and a boundary for future optimization; faster ingestion from an
Arrow source has not been measured. The default fixture size is 200,000 changes. The Nexmark
harness is unchanged.

### Local merge diagnostic

`PaimonLocalMergeBenchmark` compares local merge with identical Arrow input and output boundaries,
including Java's row-buffer conversion on its path. A release run over 131,072 rows, nine columns
(including an array), 16,384 keys and a 4 MiB local buffer measured **0.107 s Java / 0.022 s native
for deduplication (4.78×)** and **0.050 s / 0.017 s for first-row (2.93×)**. Input Arrow construction
and output verification are outside this operator timer; both paths verify the selected values.

The same benchmark separately runs whole streaming SQL jobs over 131,072 eleven-column changelog
rows with two sink writers. This includes SQL planning, job startup, row-to-Arrow ingress, local
merge, shuffle, file encoding and commit. Stock/native times were **0.712 / 0.594 s with Parquet
(1.20×)** and **0.762 / 0.649 s with ORC (1.17×)**, with matching final tables. Both measurements
use one warmup, three measured iterations and alternating engine order, reporting the best time.
These are local fixtures, not distributed throughput claims. Run with:

```bash
SF_PAIMON_LOCAL_MERGE_BENCHMARK=true mvn -Pbench,paimon \
  -pl :streamfusion-paimon -am -Dtest=PaimonLocalMergeBenchmark \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

Set `SF_PAIMON_FILE_FORMAT=orc` for the ORC SQL run, and `SF_PAIMON_LOCAL_MERGE_ROWS` to change row
count. The Arrow-only comparison performs no file IO and does not depend on the file format.

## Cross-format validation

`SF_PAIMON_FILE_FORMAT=orc` runs the shared Paimon table, SQL, spill, merge, bucket and recovery
fixtures with ORC defaults. Explicit format-specific tests keep their own format. CI runs the
suite for both defaults, with UTC test JVMs; timezone-fallback tests set and restore other zones.
The source/key tests also parameterize the formats directly, and native append files are checked
for their encoder's footer writer ID.
The ORC-default run passes 436 Paimon cases, alongside the Parquet regression suite. Shared
filesystem tests pass 52 cases covering both codecs, including failed-footer cleanup. The option tests inspect
actual bloom-filter streams, index strides, compression blocks and file versions using Paimon's
released reader.

```bash
SF_PAIMON_FILE_FORMAT=orc mvn test -Ppaimon -pl :streamfusion-paimon -am \
  '-Dtest=tech.streamfusion.paimon.*Test' -Dsurefire.failIfNoSpecifiedTests=false
```

The merge diagnostic also accepts `SF_PAIMON_MERGE_THIN=true` to measure thin data files against
stock Paimon with the same setting, including ingress conversion, routing, merge, encoding, and
commit. Use the release `bench` profile for timing.

The source and changelog diagnostics accept the same environment variable with `-Pbench,paimon`.

## Deployment

Install the published `paimon-flink-2.2-2.0.0.jar`, the selected `streamfusion-parquet` or `streamfusion-orc` module, and
`streamfusion-paimon` in Flink's `lib/`. Paimon resolves a file format by taking the **first**
`FileFormatFactory` on the classpath that claims the identifier, and Flink adds `lib/` JARs in
sorted name order, so the StreamFusion JAR must sort before `paimon-flink-*`: name it
`01-streamfusion-paimon.jar` (the same convention as `00-streamfusion-loader.jar`). The planner
checks at planning time that the selected format resolves to the StreamFusion factory and declines the sink
with an explicit reason otherwise; it never enters the native topology only to discover the stock
format at runtime. Removing the StreamFusion Paimon JAR restores the stock connector entirely.

Batch inserts, compaction jobs, and any other row-fed path see the same factory: for rows it
delegates each file to Paimon's own writer, so a mixed deployment stays byte-compatible with stock
Paimon.

## Outlook

The sequence/merge combinations, complex sequence types, and routing-column defaults listed above
deliberately retain the stock sink. Their acceleration would require further ordering and routing
work beyond the small ports and released-kernel reuse chosen for this implementation
([decision](https://github.com/datafusion-contrib/StreamFusion/blob/main/.claude/wontdos/60-paimon-merge-fallbacks.md)).

[A Paimon bundle entry for the merge-tree writer](https://github.com/datafusion-contrib/StreamFusion/issues/49)
would remove the compaction hand-off and the idle-writer rescan.
Released Paimon 2.0.0 walks a bundle row by row before the format writer; a Paimon release that
passes bundles through takes the same writer's direct path with no change here
([#39](https://github.com/datafusion-contrib/StreamFusion/issues/39)). The jar-ordering requirement
goes away once Paimon's format discovery gains a priority, which
[#38](https://github.com/datafusion-contrib/StreamFusion/issues/38) proposes upstream. Remaining streaming
source coverage is [issue #27](https://github.com/datafusion-contrib/StreamFusion/issues/27).

Build with the `paimon` Maven profile. The module has no snapshot, local-Maven, path, or forked
Paimon dependency.

### Flink 1.18 nested fallback batches

The experimental 1.18 Java reader copies each fallback row into owned binary storage before
collecting an Arrow batch. Flink 1.18's serializer can otherwise reuse a nested custom-array
buffer across rows, corrupting values when restoring a split between Java and native readers.
Snapshot parity checks cover nested arrays, projections, and restore offsets on both lines.
Compaction parity fixtures pass all eight positional procedure arguments on both lines, using
empty strings for optional filters and the explicit `full` batch-compaction strategy. This avoids depending on named/optional
argument annotations and null argument conversion that Flink 1.18 cannot interpret. Batch mode
is explicit in the table configuration because 1.18 constructs the procedure’s execution
environment from that configuration.
