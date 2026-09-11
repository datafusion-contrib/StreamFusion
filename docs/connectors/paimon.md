# Apache Paimon

**Status:** experimental. The optional `streamfusion-paimon` module accelerates streaming
`INSERT INTO` jobs into Paimon **append-only tables** and **fixed-bucket primary-key tables** on
the published Paimon `2.0.0` Flink 2.2 connector. Paimon keeps every table-level responsibility:
schema and catalog, bucket assignment rules, sequence numbering rules, file rolling, statistics,
manifests, snapshots, commits, and compaction. StreamFusion replaces the per-row shuffle in front
of the writers, the Parquet encoding of each data file, and, for primary-key tables, the sort and
merge that turns a bucket's changelog into a level-0 file.

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

### Primary-key tables

A fixed-bucket primary-key table takes the changelog Flink infers for it (`+I`/`+U`/`-D`; Paimon's
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

Supported: `bucket >= 1` with the default or an explicit `bucket-key`, `merge-engine =
deduplicate`, `changelog-producer` `none`/`input`/`lookup`/`full-compaction`, `ignore-delete`,
`write-only`, `file.compression*` and
the `parquet.*` writer keys as for append tables, and key columns of type `BOOLEAN`,
`TINYINT`..`BIGINT`, `DECIMAL`, `CHAR`/`VARCHAR`, `BINARY`/`VARBINARY`, `DATE`, `TIMESTAMP`, and
`TIMESTAMP_LTZ` (the native sort orders keys by their Arrow byte encoding, which agrees with
Paimon's key comparator for exactly these types). An insert-only stream into a primary-key table
is taken as all inserts.

### Changelog producers and deletion vectors

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
`PaimonChangelogSinkWriteTest`), and
`bin/flink-suite.sh paimon` runs Paimon's own unchanged append-table SQL integration tests with the
native sink installed (see [the upstream suite](../upstream-flink-suite.md)). The
one known statistics difference: a `DOUBLE`/`FLOAT` column whose minimum is a negative zero is
recorded as `-0.0` by parquet-rs and `0.0` by parquet-mr.

## Falls back to stock Paimon on

Each of these declines at planning time with a reason visible in `NativePlanner.explain`:

- Dynamic-bucket, cross-partition, and postpone-bucket modes (so every primary-key table with
  `bucket = -1`).
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
- `sink.clustering.*`, `partition.sink-strategy = PARTITION_DYNAMIC`,
  `sink.writer-coordinator.enabled`, `sink.coordinator-commit.enabled`.
- A nullable query field assigned to a `NOT NULL` target, or a bounded `CHAR`/`VARCHAR` or
  `BINARY`/`VARBINARY` target while `table.exec.sink.type-length-enforcer` is enabled. The stock
  sink path preserves Flink's configured fail/drop and trim/pad/error behavior.
- `TIMESTAMP` precision above 6 (Paimon writes INT96 there), `VARIANT`, vector, and geospatial
  types.
- `parquet.*` keys the native writer cannot honour: bloom filters, page validation, custom
  padding, page row-count limits, statistics/column-index truncation, page-size row checks,
  multithreaded zstd, and any per-column (`parquet.*#column`) or unrecognised writer key.
- The `parquet` format identifier resolving to Paimon's own factory (see deployment below).

Two runtime situations route rows through Paimon's stock Parquet writer inside an otherwise native
job, keeping the output identical to stock Paimon at the cost of the native speed-up for those
files: compaction rewrites (in-job or from a dedicated compaction job), and, for append tables,
Paimon's buffer-spill mode, which a writer task enters once it holds more than
`write-max-writers-to-spill` (default 10) partition-bucket writers and which re-buffers and
rewrites what those writers had already written. In spill mode the absolute sequence numbers in
file metadata differ from a stock run (Paimon reassigns them on the rewrite, and it triggers after
whole routed batches rather than single rows); rows, statistics, and footers are unchanged.
Primary-key buckets never enter that mode: their rows live in the native buffers, which spill by
size into level-0 files.

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
[dynamic and postpone buckets](https://github.com/datafusion-contrib/StreamFusion/issues/34),
[ORC data files](https://github.com/datafusion-contrib/StreamFusion/issues/35),
[the writer and commit coordinators](https://github.com/datafusion-contrib/StreamFusion/issues/36),
[clustering and the dynamic partition sink strategy](https://github.com/datafusion-contrib/StreamFusion/issues/37),
and [native encoding through the buffered spill mode](https://github.com/datafusion-contrib/StreamFusion/issues/40).
Released Paimon 2.0.0 walks a bundle row by row before the format writer; a Paimon release that
passes bundles through takes the same writer's direct path with no change here
([#39](https://github.com/datafusion-contrib/StreamFusion/issues/39)). The jar-ordering requirement
goes away once Paimon's format discovery gains a priority, which
[#38](https://github.com/datafusion-contrib/StreamFusion/issues/38) proposes upstream. A native
Paimon source is [issue #27](https://github.com/datafusion-contrib/StreamFusion/issues/27).

Build with the `paimon` Maven profile. The module has no snapshot, local-Maven, path, or forked
Paimon dependency.
