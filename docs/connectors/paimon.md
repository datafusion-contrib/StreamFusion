# Apache Paimon

**Status:** experimental. The optional `streamfusion-paimon` module accelerates streaming
`INSERT INTO` jobs into Paimon **append-only tables** and **primary-key tables with fixed,
dynamic, or postpone buckets** on the published Paimon `2.0.0` Flink 2.2 connector.
Paimon keeps every table-level responsibility:
schema and catalog, bucket assignment rules, sequence numbering rules, file rolling, statistics,
manifests, snapshots, commits, and compaction. StreamFusion replaces the per-row shuffle in front
of the writers, append buffering and spilling, the Parquet encoding of each data file, and, for
primary-key tables, the sort and merge that turns a bucket's changelog into a level-0 file. Postpone
staging retains every accepted change for Paimon's separate compactor.

## What runs natively

A sink's Arrow batches are split natively into one sub-batch per `(partition, bucket)` pair. Rust
computes the partition `BinaryRow` and Paimon's default bucket hash column-wise (Paimon's row
layout and Murmur hash are Flink's, so the native key encoder already produces both), and the
routed batches are shuffled while still Arrow with Paimon's own channel formula. Each batch then
enters Paimon's bundle write entry point for its bucket and reaches a StreamFusion
`FileFormatFactory` registered under the `parquet` identifier, whose writer encodes the whole batch
with the standard parquet-rs `ArrowWriter` over Paimon's output stream. Paimon reads statistics
from the resulting footer exactly as from its own files.

Supported:

- Bucket-unaware tables (`bucket = -1`), partitioned or not, with `partition.sink-strategy` `none`
  or `hash`, including Paimon's in-job compaction coordinator and workers.
- Fixed-bucket append tables (`bucket > 0` with `bucket-key`), partitioned or not, with the
  `sink.parallelism` and small-bucket-count parallelism rules of the stock sink.
- `file.format = parquet` with `file.compression` `none`/`snappy`/`gzip`/`zstd` (and
  `file.compression.zstd-level`), `file.block-size`, and the `parquet.*` writer keys the stock
  writer honours: page and dictionary-page size, dictionary encoding, writer version.
- Column types `BOOLEAN`, `TINYINT`..`BIGINT`, `FLOAT`, `DOUBLE`, `DECIMAL`, `CHAR`/`VARCHAR`,
  `BINARY`/`VARBINARY`, `DATE`, `TIMESTAMP`/`TIMESTAMP_LTZ` up to precision 6, and `ARRAY`, `MAP`,
  `ROW` of those, recursively, with Paimon's field ids on every column.
- Hint options (`/*+ OPTIONS(...) */`) and the `paimon.<catalog>.<db>.<table>.<option>` dynamic
  options from the job configuration, resolved the way Paimon's own factory resolves them.
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
budget controlled by `write-buffer-size`. Enabling it obtains the buffer budget and memory segments
from Flink's managed memory pool. Native Arrow allocations do not participate in that pool, so
append sinks with this option enabled use stock Paimon.

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
sorted by key and arrival, reduced to the last row per key (Paimon's `deduplicate` merge engine;
`ignore-delete` drops the retracts first), and written straight into level-0 data files in Paimon's
key-value layout with the native Parquet encoder, rolled at `target-file-size`. Every file carries
the metadata Paimon's own writer records: key bounds, key and value statistics from the footer,
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
deduplicate`, `changelog-producer` `none`/`input`/`lookup`/`full-compaction`, `ignore-delete`,
`write-only`, `file.compression*` and
the `parquet.*` writer keys as for append tables, and key columns of type `BOOLEAN`,
`TINYINT`..`BIGINT`, `DECIMAL`, `CHAR`/`VARCHAR`, `BINARY`/`VARBINARY`, `DATE`, `TIMESTAMP`, and
`TIMESTAMP_LTZ` (the native sort orders keys by their Arrow byte encoding, which agrees with
Paimon's key comparator for exactly these types). An insert-only stream into a primary-key table
is taken as all inserts.

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

The native postpone files use Parquet. Stock Paimon 2.0.0 normally chooses Avro internally for
these staging files, even when `file.format = parquet`; Paimon's released readers and compactor
accept both. Native staging therefore differs in physical encoding while preserving the records,
row kinds, replay order, and commit protocol. It uses the normal native Parquet option whitelist.

Postpone ingestion does not produce the final merged table or changelog. Paimon's dedicated
compaction job (`CALL sys.compact` in batch mode) assigns real buckets and applies the configured
merge, changelog, and deletion-vector behavior. Default reads omit uncompacted postpone files;
Paimon's batch `postpone.merge-on-read` reader can include them. The native writer preserves the
stock stateless writer and restore-only committer lifecycle. Batch writes and primary keys that
omit partition columns remain outside the native whitelist.

### Changelog producers and deletion vectors

For fixed and dynamic buckets:

- **Input:** the same native sort returns every retained input row before deduplication, ordered
  by primary key and then arrival sequence. These rows, including `UPDATE_BEFORE` and deletes,
  are encoded into separate Parquet changelog files and committed through Paimon's changelog
  manifests. They are not data-file extras, and their rolling boundaries are independent of the
  merged data files. `ignore-delete` removes retracts before both outputs and before numbering.
  `changelog-file.format = parquet`, `changelog-file.compression`, and
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

Files written natively are row-, metadata-, statistics-, and footer-schema-identical to the stock
writer's (verified against twin tables in `PaimonSinkParityTest`, `NativePaimonParquetWriterTest`,
`NativePaimonKeyValueFileWriterTest`, `NativeKeyValueSinkWriteTest`, and
`PaimonChangelogSinkWriteTest`), except for the postpone staging encoding and append-buffer
transition described above, and the random per-file row distribution with `PARTITION_DYNAMIC`
described below.
`bin/flink-suite.sh paimon` runs Paimon's own unchanged append-table SQL integration tests with the
native sink installed (see [the upstream suite](../upstream-flink-suite.md)). The
one known statistics difference: a `DOUBLE`/`FLOAT` column whose minimum is a negative zero is
recorded as `-0.0` by parquet-rs and `0.0` by parquet-mr.

The regular CI Paimon job runs SQL parity for dynamic and postpone buckets, including reopened
jobs, assigner/writer rescaling, and Paimon's SQL compaction of native postpone files. A recovery
harness discards an uncommitted dynamic checkpoint, restores the assigner state, replays the
input, and compares the resulting hash-to-bucket index and changelog with stock Paimon. A larger
SQL fixture verifies postpone replay across rolled files. These are ordinary correctness tests;
the performance diagnostics below are opt-in.

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

## Falls back to stock Paimon on

Each of these declines at planning time with a reason visible in `NativePlanner.explain`:

- Cross-partition dynamic keys (`KEY_DYNAMIC`) and postpone primary keys that omit partition columns.
- A changelog (retracting or updating) input into an append table, a sink Flink plans with a
  `SinkUpsertMaterializer` (`table.exec.sink.upsert-materialize`; Paimon itself refuses that
  operator), `INSERT OVERWRITE`, and batch-mode inserts (the substitution only exists in the
  streaming planner).
- Primary-key tables with `merge-engine` `first-row`, `partial-update`, or `aggregation`;
  input changelog with `changelog-file.format` other than `parquet` or unsupported changelog
  compression; primary-key vector, full-text, BTree, or bitmap indexes; `sequence.field`,
  `rowkind.field`, `local-merge-buffer-size`,
  `data-file.thin-mode`, `data-file.external-paths`, `sink.key-only-deletes.enabled`,
  `precommit-compact`, `write.sequence-number-init-mode = snapshot`,
  `sink.use-managed-memory-allocator`; or a `FLOAT`/`DOUBLE` key column.
- `file.format` other than `parquet`, `file.format.per.level`, `write-buffer-for-append = true`,
  file indexes (`file-index.*`), `row-tracking.enabled`, `data-evolution.enabled`, `BLOB` columns.
- Append tables with `spill-compression` other than `lz4`/`lzo`/`zstd`, or
  `sink.use-managed-memory-allocator = true` (the native buffer uses Arrow memory).
  Released Paimon 2.0.0 fails when actually spilling with `none`; it remains on the stock path.
- A nullable query field assigned to a `NOT NULL` target, or a bounded `CHAR`/`VARCHAR` or
  `BINARY`/`VARBINARY` target while `table.exec.sink.type-length-enforcer` is enabled. The stock
  sink path preserves Flink's configured fail/drop and trim/pad/error behavior.
- `TIMESTAMP` precision above 6 (Paimon writes INT96 there), `VARIANT`, vector, and geospatial
  types.
- `parquet.*` keys the native writer cannot honour: bloom filters, page validation, custom
  padding, page row-count limits, statistics/column-index truncation, page-size row checks,
  multithreaded zstd, and any per-column (`parquet.*#column`) or unrecognised writer key.
- The `parquet` format identifier resolving to Paimon's own factory (see deployment below).

Compaction rewrites (in-job or from a dedicated compaction job) still use Paimon's stock Parquet
writer. Append buffer spilling retains native encoding as described above. Primary-key buckets
use their existing native buffers, which flush by size into level-0 files.

## Benchmark

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

Local release measurements on 2026-09-11 with Paimon 2.0.0:

| Mode | Stock, 1M changes | Native, 1M changes | Speedup, 1M | Speedup, 200K |
|---|---:|---:|---:|---:|
| Input changelog | 3.716 s | 2.319 s | 1.60× | 1.83× |
| Lookup changelog | 2.387 s | 1.856 s | 1.29× | 1.18× |
| Full-compaction changelog | 1.276 s | 1.343 s | 0.95× | 0.78× |
| Deletion vectors enabled | 1.498 s | 1.122 s | 1.34× | 1.31× |

Full compaction is slower in this row-fed diagnostic. It retains Paimon's rowwise compaction and
adds the native path's conversion and file hand-off costs. Its admission adds coverage for columnar
pipelines; these measurements do not establish a throughput improvement for that mode.

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

## Deployment

Install the published `paimon-flink-2.2-2.0.0.jar`, `streamfusion-parquet`, and
`streamfusion-paimon` in Flink's `lib/`. Paimon resolves a file format by taking the **first**
`FileFormatFactory` on the classpath that claims the identifier, and Flink adds `lib/` JARs in
sorted name order, so the StreamFusion JAR must sort before `paimon-flink-*`: name it
`01-streamfusion-paimon.jar` (the same convention as `00-streamfusion-loader.jar`). The planner
checks at planning time that `parquet` resolves to the StreamFusion factory and declines the sink
with an explicit reason otherwise; it never enters the native topology only to discover the stock
format at runtime. Removing the StreamFusion Paimon JAR restores the stock connector entirely.

Batch inserts, compaction jobs, and any other row-fed path see the same factory: for rows it
delegates each file to Paimon's own writer, so a mixed deployment stays byte-compatible with stock
Paimon.

## Outlook

Each remaining gap has its own issue:
[the other merge engines, `sequence.field`, and `rowkind.field`](https://github.com/datafusion-contrib/StreamFusion/issues/47),
[the remaining primary-key writer options](https://github.com/datafusion-contrib/StreamFusion/issues/48)
(thin mode, key-only deletes, local merge, external paths, managed memory, snapshot sequence init),
[a Paimon bundle entry for the merge-tree writer](https://github.com/datafusion-contrib/StreamFusion/issues/49)
that would remove the compaction hand-off and the idle-writer rescan,
[ORC data files](https://github.com/datafusion-contrib/StreamFusion/issues/35).
Released Paimon 2.0.0 walks a bundle row by row before the format writer; a Paimon release that
passes bundles through takes the same writer's direct path with no change here
([#39](https://github.com/datafusion-contrib/StreamFusion/issues/39)). The jar-ordering requirement
goes away once Paimon's format discovery gains a priority, which
[#38](https://github.com/datafusion-contrib/StreamFusion/issues/38) proposes upstream. A native
Paimon source is [issue #27](https://github.com/datafusion-contrib/StreamFusion/issues/27).

Build with the `paimon` Maven profile. The module has no snapshot, local-Maven, path, or forked
Paimon dependency.
