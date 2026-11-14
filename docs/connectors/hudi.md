# Apache Hudi

**Status: experimental, partial sources and Parquet append/MOR upsert sinks.**

The optional `streamfusion-hudi-flink1.18` module targets the released Hudi 1.2.1
Flink 1.18 bundle. There is no released Hudi Flink 2.2 bundle. Build with
`-Phudi,flink-1.18`; the module rejects other Flink lines. Install the matching
StreamFusion loader/core, `streamfusion-hudi-flink1.18`,
`streamfusion-parquet-flink1.18`, and Hudi's normal
`hudi-flink1.18-bundle-1.2.1` JAR. The connector adds no native library or Rust
storage client; it uses the independently deployed Parquet module.
Its Java-only JAR includes the project license and planner service registration.

## Bounded snapshot source

In the streaming SQL runtime, native planning admits bounded COPY_ON_WRITE
Parquet snapshot reads using Hudi's Source V2 (`read.source-v2.enabled=true`).
Payload types include BOOLEAN, INT, BIGINT, FLOAT, DOUBLE, DECIMAL,
STRING/VARCHAR, BINARY/VARBINARY, DATE, TIME through millisecond precision,
and TIMESTAMP/TIMESTAMP_LTZ at precision 3 or 6.
ROW, ARRAY and MAP payloads can nest these types. TINYINT and SMALLINT remain
stock because Hudi's Avro schema normalizes them to INT. Top-level projection,
reordered columns and NULLs remain columnar. SQL admission requires
an immediate columnar consumer, such as a native Calc or aggregate; a bare scan
returning rows retains stock Hudi to avoid a decode-to-Arrow-to-row detour. Every selected base file must have the requested physical
column names and matching types. Java Hudi resolves the visible base files and
checks footer schemas once during admission; mismatches retain the stock source.
Partitioned tables retain Hudi's partition discovery and pruning. SQL filter
pushdown is admitted because the pinned Hudi source leaves every input filter
in Flink's remaining-filter list; the downstream Calc evaluates the residual
predicate. Direct DataStream admission rejects unapplied predicates.
Hudi's binary/string physical normalization is shared by fixed-length binary,
bounded VARCHAR and VARBINARY declarations. BINARY uses the shared variable
Arrow binary transport; disabling both experimental binary transport features
retains stock Hudi for BINARY columns. Other timestamp precisions stay stock
because Hudi normalizes their table schemas differently from their file encoding.

Hudi's original factory constructs its source before the task reader is replaced,
following the existing version-gated Iceberg adapter pattern. The factory method
is accessed only for the pinned released implementation. Java retains timeline
visibility, discovery, assignment, split state and checkpoint serializers. The
native Parquet decoder requests byte ranges through Hudi's authenticated Java
seekable storage stream and exports Arrow batches into the native pipeline.

Checkpoint offsets count emitted logical rows, not prefetched batches. Restoration
can change decoder batch size and resume inside a batch. Unclaimed fetched batches
are explicitly released during reader close; emitted batches remain owned by the
downstream consumer and survive decoder close. Native decoder memory has a
conservative compressed-plus-uncompressed row-group reservation and a 64 KiB Java
transfer buffer; Arrow output is separately charged to the shared allocator.
These estimates are not a strict bound on all transient decoder/footer allocations.
`nativeHudiBatches` and `nativeHudiRows` witness task-side native output. The source
input-record counter counts logical rows rather than Arrow batches.

The public `NativeHudiSource.from` entry point applies the same source admission
policy for direct DataStream use. Ordinary SQL retains its normal stock fallback
when the optional module, native Parquet module or verified source contract is absent.

## Continuous COW source

The same columnar reader also admits continuous COW reads on version-9 tables
with stored Hudi metadata. Java's original continuous enumerator selects completed
commits, handles the requested start commit and owns discovery/checkpoint state.
`read.streaming.enabled=true`, `read.streaming.check-interval`, `read.start-commit`
and `read.commits.limit` are admitted. An absent start retains Hudi's latest-commit
behavior; `earliest` includes the initial snapshot. An end-commit bound stays stock.

For each changed file Java supplies its exact set of selected requested instants.
The reader adds `_hoodie_commit_time` to its physical projection and translates
that set into the existing Rust Calc filter. The hidden column is removed before
emission. This prevents a rewritten COW file from re-emitting its unchanged rows;
payloads remain Arrow throughout. Checkpoint offsets count rows after this filter,
so recovery can change decode batch size without changing the resumed row sequence.
The public adapter also accepts Hudi's inferred incremental query type in streaming
mode. It retains the stock insert-only post-image behavior; delete/change emission
is outside this admission.

Tables without metadata, other table versions, CDC/changelog mode, bounded
incremental queries, end-commit windows and other unverified discovery options
retain the stock source. The same source schema/projection restrictions apply.

## MOR snapshot and continuous sources

Version-9 MOR tables, including partitioned tables, can use the same Rust Parquet decoder for
base files and Parquet log blocks. Java Hudi's existing file-group reader owns
log framing, completed-instant visibility, rollback blocks, update/delete winner
selection and operation tags. Its reader context supplies stable views into Arrow
vectors. The original Hudi merge map and spill implementation retain keys,
ordering values and Arrow batch/row references. Payloads stay columnar throughout
merging and projection. Terminal field projection is deferred to the Arrow batch
gather, avoiding per-row projection wrappers. Rust's Arrow interleave kernel
gathers the winning rows into bounded output batches.
Gathers retain the decoded vectors' actual field layouts and metadata. In
particular, stored nested rows omit execution-only row-kind children.

Admission covers standard full-row event-time or commit-time merging, a single
INT/BIGINT ordering field, the same payload types as COW reads and
`hoodie.datasource.merge.type=payload_combine`. Existing base schemas and
Parquet-log headers are checked during planning. Avro logs, partial updates, custom payloads and
unverified schema evolution retain the stock source. The same SQL requirement
for an immediate columnar consumer applies. Java storage supplies all input bytes.

Continuous MOR reads use Hudi's original enumerator, fetcher, split ordering and
checkpoint serializers. With `changelog.enabled=true`, Hudi chooses the winning
operation, including key-only deletes. Generated delete-key metadata is encoded
into Arrow key columns, with NULLs in other projected columns. Existing payloads
remain Arrow views. Start-commit and discovery options follow continuous COW
admission. CDC and stored intermediate-change logs retain the stock path.
Row kinds follow Hudi's public engine-record iterator. Updates can appear as
`INSERT` post-images under this released source contract.

Decoding and merging run on Hudi's original split fetcher and feed its bounded
queue with Arrow batches. The shared COW/MOR emitter checkpoints logical rows
after winner selection. Restoration replays the merger and skips those rows,
following Hudi's original split-reader contract, and can change decoder batch
size. This also overlaps source work with downstream Parquet encoding. Native
MOR checkpoints require the same admitted merge and spill configuration on restore.

`compaction.max_memory` reserves the file-group payload/output budget. Base batches
are released after their selected rows are emitted. Log payload batches remain
retained through the merge, while Hudi may spill its key/version/reference controls
to its configured local spill implementation. The budget accounts for twice the
retained Arrow bytes to cover output gathering. Exceeding it fails the task.
Payload spilling is outside the current adapter. Forced-spill tests verify that
the same Arrow batch survives control serialization and projection.

## Immutable COW append sink

The native append sink admits `write.operation=insert` on partitioned or unpartitioned COW tables
with `hoodie.populate.meta.fields=false`, `metadata.enabled=false`, and Hudi's
unbuffered append path. It accepts insert-only input with the same scalar and nested
payload types as the source. Updating, overwrite, buffered and
metadata-bearing writes retain stock Hudi. Bloom metadata, ignored write failures,
unknown Parquet overrides and unverified constraint policies also retain stock.

Admission checks Hudi's effective metadata-table setting as well as the Flink
option. A raw `hoodie.metadata.enable=true` override retains the stock sink.

The pinned released sink builds its normal graph first. Following the Iceberg
adapter, StreamFusion replaces only its append operator factory, preserving Hudi's
coordinator, operator identities, markers, write statuses and commit/checkpoint
protocol. The original append function still processes key/counter access through
borrowed views of Arrow columns. Each partition writer accumulates selected row positions for the batch;
those selections reach the existing Rust encoder without a row transpose.
Hudi's Java storage creates the output stream and writes its bytes.

The original Java write client resolves the effective Parquet configuration.
Compression, row-group/page sizes and dictionary settings pass to the shared codec.
A validated plan carries admission into translation without repeating filesystem
checks. NOT NULL errors reuse the existing columnar constraint enforcer. File
rolling occurs between complete batches; the configured target is approximate
within one batch. Encoder accounting reserves twice the row-group size plus its
1 MiB transfer buffer; this is a conservative estimate, not a strict bound on all
transient allocations. Checkpoints close encoded files before Java acknowledges
their write statuses. Native file footers identify `parquet-rs`; stock Hudi reads
those files unchanged.

## MOR primary-key sink

The MOR sink admits insert/update-before/update-after/delete SQL changes and the public
`NativeHudiMorSink.append` boundary. The one-writer primitive workload measured
below passes the stock/previous-StreamFusion performance gate at 200k and one million rows.
Admission requires version 9, a MOR upsert table, the SIMPLE BUCKET index, explicit positive `write.tasks`,
Parquet log blocks, on-heap Hudi buffering and positive writer/batch budgets.
Metadata-table writes, CDC/changelog storage, write-rate limiting,
compaction and clustering are disabled. Standard full-row commit-time merging or
one declared NOT NULL INT/BIGINT event-ordering field is admitted, with the scalar payload types above
and four Parquet codecs as append. Existing targets must match the declared
schema, keys, merge mode and table options. Partial/custom merges, duplicate
insert policy, independent concurrent writer jobs and nested payloads
retain stock Hudi. SQL NOT NULL constraints use the existing columnar enforcer.
Admission also checks Hudi's effective settings, so raw `hoodie.*` overrides cannot
enable metadata-table writes or an extra operation metadata column behind the Flink flags.
Nullable event-ordering schemas stay stock: a stock-oracle delete/reinsert test
observed Hudi writing the previous row's version for a NULL delete version, while
the Arrow view preserves NULL. The adapter does not reproduce stale field bytes.

The Parquet log boundary uses Hudi's original `HoodieFlinkParquetDataBlock`. The adapter
buffers up to 4,096 row references in Hudi iterator order. The shared Arrow
interleave kernel gathers their payload columns in Rust, preserving order across
source batches and repeated positions. Java builds only the five generated
metadata columns for these dense rows; Rust encodes the combined Arrow batch. Hudi retains log
framing, delete-block construction, append I/O and commits. Actual encoded footer
metadata supplies column statistics; footer inspection borrows the in-memory
encoded bytes through Hudi's existing in-memory input adapter. Tests cover nullable
Boolean/string payloads, metadata alignment,
selected row order, four codecs and iterator/payload failure cleanup. The released
1.2.1 log serialization helper has no writer cleanup on iterator failure, so the
adapter retains its small control loop with scoped encoder ownership.
Log encoding retains Hudi's default Parquet row-group and page sizes. Each
simultaneous encoder reserves twice its row-group size plus a transfer buffer;
four active writers therefore need a larger task off-heap budget than one.

The graph adapter retains Hudi's original SIMPLE bucket index and writer coordinator.
When Flink requires sink materialization, the existing native sink materializer
consumes and emits Arrow over the shared columnar keyed exchange. It uses Flink's
inferred input upsert key and state retention setting; Hudi then receives the
materialized changelog. The planner retains its ordinary materialization decision.
Java's original routing assigns file groups using borrowed key views. Each Java
bucket retains Arrow batch references and a bitset of its selected row positions.
Hudi's buffer map and bucket metadata are accessed only for the pinned release.
With precombine enabled, Hudi's original deduplication helper and buffered record
merger select winners before log encoding. The shared Arrow record context retains
stable payload references through `seal` and binary-row conversion requests.
No payload is copied into binary rows. This also preserves Hudi's ordinary
delete/reinsert behavior when precombine is disabled.
Original batch/checkpoint flushes drain stable borrowed row views into
the existing Java write client, which generates metadata and ordered key-delete
blocks. Payload encoding remains Rust Parquet. No merge, index or spill algorithm
is copied. Hudi's actual buffer pool and Flink's paged serializer retain control
of page allocation, fullness, preemption and retry boundaries. A version-pinned
serializer hook skips payload writes; encoded string lengths come from Arrow
offsets, without decoding strings. Java allocates the same control pages as stock,
while Arrow payloads remain in their original batches until encoding. Each input
key enters Hudi's original bucket function once in arrival order. Its flushes can
occur midway through a batch, preserving delete/reinsert winner behavior.
Retained Arrow buffers are additionally charged to task off-heap memory; exhausting
that budget fails the task rather than introducing a different flush policy.

The graph adapter removes only the bypassed row preprocessing roots it created;
Flink otherwise still executes disconnected registered roots. Existing upstream
inputs and the Java downstream lifecycle remain intact. A single writer connects directly. Multiple writers use Hudi's original bucket
partitioner to choose a channel through borrowed key views, then the existing
Rust bucket-routing operation gathers Arrow batches for Flink's channel shuffle.
Hudi retains bucket assignment and file-group reuse across commits. Tests verify
four partition writers, two-input routing to one writer, and stock Hudi readback across
two commits, unchanged file-group IDs, Rust Parquet footers,
Java key-delete blocks, early flushes under a 1 MiB buffer budget, and recovery
after a completed checkpoint with final update/delete parity at one, two, three
and four configured writers. Storage partition count is independent of writer
parallelism. Buffer tests compare
stock/control occupancy and exhaustion at inline-string, UTF-8, null-bitset and
page boundaries. SQL tests require the native sink and verified fallback plans;
stock-oracle tests cover equal-version, commit-time delete/reinsert and
update-before behavior. NULL variable fields consume the same page space as
Flink's original serializer; their Arrow payloads are never serialized to pages.

Timestamp encoding derives physical units and UTC annotations from Hudi's own
Parquet schema converter. The shared Rust codec receives that schema metadata,
including timestamps nested in COW payloads.

On recovery the adapter uses Hudi's existing inflight-instant request and wait helper
to await older checkpoint commits
before the first restored batch reaches bucket assignment. The coordinator can
still be completing the restored checkpoint's commit when tasks start accepting
input. Waiting prevents bootstrapping new file groups before those committed
groups become visible; Hudi retains all commit and index decisions.

`HudiMorSinkBenchmarkTest` measures the complete COW Parquet source → Calc → MOR
upsert pipeline against stock Flink and the previous native-source/Calc plus stock
sink. It retains planning, storage I/O, routing, Java metadata, native encoding and
commit costs, checks nine stock-reader aggregates, and verifies actual Parquet
footers. The Java-only sink uses the existing engine and Parquet libraries; it
has no dependency on the Rust MOR reader or its separate DSO.

Two warmups and five rotating trials per engine use the documented release
libraries, one writer, 8 GiB heap and 1 GiB task off-heap. Each input has two event
versions per key, nullable VARCHAR/INT/BOOLEAN payloads and a BIGINT event version.
One-second process sampling found no unrelated Maven, Surefire, Cargo or Rust
compiler processes during either complete run.

| Input rows / keys | Stock median ms (range) | Previous SF median ms (range) | Native median ms (range) |
| ---: | ---: | ---: | ---: |
| 200,000 / 100,000 | 983.9 (944.4–1223.4) | 990.5 (918.9–1017.3) | 812.1 (737.8–833.3) |
| 1,000,000 / 500,000 | 2990.0 (2584.3–3363.5) | 2605.6 (2515.3–3278.1) | 1892.0 (1631.0–2283.8) |

The Java-held Arrow buffer's million-row sink pipeline is 1.58× faster than stock
and 1.38× faster than the previous native-source/Calc plus stock-sink path. At
200k the ratios are 1.21× and 1.22×. The trial evidence retains the previous
Rust-buffer measurements, current values, configuration and checks. Measurements
cover complete local read/Calc/upsert jobs.
[Trial evidence](../benchmarks/hudi-upsert-20261110.json)

Earlier contended 200k/1m runs are retained but excluded from speedup claims. An
initial million-row attempt returned no verification aggregate after 760.9 seconds;
that empty collection has not recurred in two later full million-row comparisons,
and its cause remains unresolved. Per-engine/trial markers identify any recurrence.
The first Java-buffer 200k run also had a large mid-run host-load shift and is
excluded. A fresh complete 200k comparison supplies the table above.

## Stock paths

The following retain the stock connector:

- Read-optimized and bounded incremental queries, CDC and stored intermediate-change logs.
- Continuous reads without version-9 metadata, end-commit windows and unverified discovery options.
- Legacy sources, bootstrap tables, source watermarks and metadata columns.
- Tables without an initialized, completed snapshot.
- Pushed limits, nested or zero-column projections, and other payload types.
- Requested schema-on-read evolution, mismatched physical file schemas, non-UTC read
  conversion, bounded commit-range options, and unverified read/Parquet reader options.
- Batch-runtime SQL scans and portable compiled-plan source lowering.
- COW upserts and other unverified sinks;
  state/Bloom/consistent-hashing indexes and unverified payload policies.
  Index operations, compaction and clustering remain Java-owned.
- Sinks that drop partition columns from their physical payload, or override the
  write schema with columns/types different from the declared table. Native reads
  also require each requested partition column to exist in its physical files.

Both MOR directions use released Java Hudi's merge/index/transaction machinery
with Arrow payload references and the existing Rust Parquet codec. The earlier
released-`hudi-core 0.6.0` local-view prototype passed snapshot/recovery checks but
remained slower than the previous SF reader pipeline. Its continuous delete
emission is explicitly unimplemented. The Java reader context supplies that
operation contract through Hudi's existing merger.

## Validation

`bin/hudi-suite.py` runs the released Hudi 1.2.1 Flink test bytecode unchanged
against stock Flink and StreamFusion. The corpus includes every concrete
table-level SQL integration class in the released Flink test JAR: the main
datasource, DFS/HMS SQL schema evolution, data schema evolution, dynamic buckets,
blob writes and Spark vector fixture compatibility. It also includes
`TestHoodieTableSource` and `TestHoodieFileGroupReaderOnFlink`.
The manifest has 350 invocations (349 enabled), including
Hudi's disabled `testMultipleLogBlocksWithDataSkipping`. The manifest detects missing cases and changed
skips. This is a SQL/source/file-group-reader corpus, not the entire Hudi test repository.

`ITTestVariantCrossEngineCompatibility` is retained in the fixture JAR but is an
explicit baseline exclusion: all three unchanged tests fail in stock Flink 1.18
with `Unknown identifier 'VARIANT'`. This is not counted as passing or skipped
coverage. Reproduce it with `--mode stock --tests ITTestVariantCrossEngineCompatibility`.

The fixture build uses published test JARs and matches the released bundle's
package relocations. Hudi does not publish the Flink 1.18 adapter test JAR, so
six unchanged adapter helpers are vendored from the exact release commit.
`dev/flink-suite/hudi-fixtures/provenance.json` pins their source paths and
SHA-256 values, plus the published fixture JAR checksums. Test dependencies
include Hive and Lance so the original mixed-format/catalog cases can execute.
These do not enter the production connector artifact.

```bash
python3 bin/hudi-suite.py
```

The runner requires JDK 17, builds the fixture and observation agent in a scoped
temporary directory, and removes runtime data afterwards. Use `--tests` for a
focused Surefire selection, `--mode stock|native|both` for engines and
`--native-library-dir` to reuse prebuilt libraries. The default report is
`streamfusion-hudi/target/hudi-upstream.json`. It records every result and native
work/fallback observations per JUnit invocation, rejects native work in stock,
and compares both engines' invocation inventories. All 349 enabled invocations
have maintained execution contracts that reject missing expected work,
unexpected routes and changed fallback reasons. The unchanged upstream
cases retain their original legacy-source and row-result choices, so many Hudi
boundaries correctly stay stock while native Calcs execute. The expanded tests
and Nexmark harness separately require native Hudi source/sink execution.
CI runs the default corpus
in the Hudi JDK 17 lane and uploads the concise report.
CI also runs the runner's failure-path regressions: missing or duplicate invocations,
fixture checksum failures, lost native work and unexpected fallback cannot produce
a passing report.
Maintainers can collect routes for new focused selections with `--inventory`;
those reports have `inventory` status and do not qualify as a passing gate.

`HudiExpandedCoverageTest` covers partitioned reads, residual predicates, nested
COW payloads and four MOR writers over two commits. `HudiMorSinkTest` compares
update-before and precombine behavior with the original writer and exercises the
shared native sink materializer. `NativeHudiMorSourceTest` compares partitioned
log-only and base-plus-log merges with stock Hudi, including nested ROW/ARRAY/MAP
NULLs, microsecond timestamps, DECIMAL, TIME and binary payloads. It verifies
projection and that an older event version cannot replace the latest payload.
The [four-partition Hudi Nexmark harness](../benchmarks/hudi-nexmark.md)
adds full-row stock readback and checks the actual Parquet encoder. Expanded
matrix performance validation is ongoing; the earlier one-writer measurements
above do not establish performance for the expanded matrix. The completed
heap/mini-batching-off configuration has a 0.55× geometric mean of median
speedups over stock Flink: simple I/O improves, but window and lookup paths
remain substantially slower. The remaining three configurations are pending;
the benchmark page retains all trial ranges and the draft's open performance
disposition in [issue #663](https://github.com/datafusion-contrib/StreamFusion/issues/663).

The terminal sink does not observe watermarks. When only native Calcs without
`CURRENT_WATERMARK` or user functions separate it from an assigner, the planner retains whole
Arrow batches instead of splitting at unobservable eager emissions. Append sinks
also admit native exchanges, mini-batch assigners and inner joins with insert-only
inputs. For MOR with explicit commit-time ordering and precombine, this proof also crosses native
exchanges, mini-batch assigners, regular joins, non-windowed aggregates/ranking
and deduplication that does not observe watermarks, without user functions.
Rowtime keep-first deduplication retains fine boundaries. Event-time ordering and
precombine-disabled writes retain their flush positions. Windows, unknown
intervening operators and watermark-observing Calcs retain the exact
per-row boundaries. The watermark expression and NULL validation remain intact.

`NativeHudiSourceTest` uses stock Hudi SQL writes, compares stock/native SQL results,
and requires the native source plan. It checks projection/NULLs, restored logical
row offsets with changed batch sizes, stock fallback admission, and Arrow validity
after reader close, and the fixed admitted snapshot across later commits. Continuous tests restore the
original Java enumerator and reader state, check a rewritten COW file against
unchanged rows, and compare SQL post-images across new commits. The
optional CI lake-module matrix includes this Flink 1.18 module. Append tests verify supported scalar/nested types and NULLs, batch-boundary rolling,
stock readback, native SQL source/sink plans without transposes, and recovery after
a completed checkpoint with no duplicates. Other connectors' normal tests cover
the shared file-batch import helper.

`NativeHudiMorSourceTest` compares Java-written base/log and log-only groups with
stock Hudi. It verifies stale updates, ordered/default-version deletes, ties,
commit-time ordering, nullable primitive columns, reordered projections, key-only
tombstones with declared NOT NULL non-key fields, SQL operation tags through a
native Calc, ownership after reader close and forced control spilling. Recovery
tests restore merged logical-row offsets and the original enumerator across a
new commit, change batch sizes, and restart a real pipeline after a completed checkpoint.
A wide fully deleted base file verifies prompt release under a 1 MiB budget.
An actual table with stored intermediate operations verifies planning fallback.

The opt-in `sf.hudi.benchmark` tests compare the same Java-written files using
release native libraries, one source task, a 4096-row decoder batch, 1 GiB task
off-heap and an 8 GiB JVM heap. Each benchmark uses two warmups and five alternating
measured trials. Fixture writes are outside timing. Native plans and checksummed
results are required. Complete pipelines retain planning, admission, Java I/O,
JNI, all intrinsic conversions, and result collection. The aggregate pipeline uses
`SELECT SUM(id), COUNT(name) FROM h` with a 4096-row / 50 ms mini-batch. Its previous
StreamFusion baseline disables only `streamfusion.operator.hudiSource.enabled`,
leaving the same native aggregate over stock Hudi input. The row-result pipeline
uses `SELECT name, id + 1 FROM h`, retaining its final Arrow-to-row boundary.
The reader-only diagnostic excludes planning/admission and checksums equivalent
values directly in RowData or Arrow; it is not a complete-job claim.

Run with prebuilt release engine and Parquet libraries:

```sh
mvn -Phudi,flink-1.18,bench -pl streamfusion-hudi -am test \
  -Dnative.build.skip=true -Dtest=NativeHudiSourceTest \
  -Dsurefire.failIfNoSpecifiedTests=false -Dsf.testForks=1 \
  -Dsf.hudi.benchmark=true -Dsf.hudi.benchmark.rows=1000000
```

Measurements on an Apple M4 (10 cores), macOS 15.3.1, Temurin 17:

| Workload | Rows | Stock median ms (range) | Previous SF median ms (range) | Native median ms (range) |
| --- | ---: | ---: | ---: | ---: |
| aggregate | 200,000 | 490.9 (469.1–573.3) | 477.3 (465.7–575.6) | 487.3 (483.0–580.3) |
| sql | 200,000 | 965.8 (935.5–1063.5) | — | 1047.9 (956.7–1091.2) |
| reader | 200,000 | 22.4 (22.0–30.6) | — | 8.3 (8.3–9.3) |
| aggregate | 1,000,000 | 722.7 (614.9–736.8) | 605.4 (586.7–779.7) | 528.8 (497.3–643.1) |
| sql | 1,000,000 | 2672.5 (2627.8–2722.8) | — | 2636.4 (2591.0–2681.5) |
| reader | 1,000,000 | 134.4 (93.7–147.1) | — | 27.7 (27.2–48.8) |

At one million rows, the aggregate pipeline is 1.37× faster than stock Flink
and 1.14× faster than the previous StreamFusion path in this run. The reader-only
boundary is faster at both sizes. Small complete jobs show no consistent gain:
the 200k aggregate is approximately flat and the row-result Calc is about 9%
slower. Returning every row to Java spends most time outside file decoding;
these measurements do not establish a general SQL speedup. Standalone scans
retain stock Hudi, and the native source's intended use is an Arrow pipeline.
The [trial evidence](../benchmarks/hudi-source-20261109.json) includes variability,
earlier unfavorable results, and the change that removed repeated admission I/O.

### Complete read/write pipeline

The append benchmark runs `INSERT INTO target SELECT name,id+1 FROM h` against
fresh, identically configured immutable COW targets. It times SQL execution through
Hudi's completed commit, including planning/admission, Java file I/O, JNI and all
intrinsic conversions. Source fixture writes, target DDL, plan assertions and
stock-Hudi aggregate readback are outside timing. The previous StreamFusion engine
disables only `hudiSink`, retaining the native source/Calc and its row sink boundary.
Two warmups and five rotating trials use the same release libraries/resources as
above. The native plan must contain both source and sink and no Arrow-to-row edge.
Readback verifies count, id sum and NULL count for every trial.

| Rows | Stock median ms (range) | Previous SF median ms (range) | Native median ms (range) |
| ---: | ---: | ---: | ---: |
| 200,000 | 697.6 (668.3–825.3) | 831.5 (739.6–911.4) | 643.6 (562.1–668.7) |
| 1,000,000 | 1564.7 (1500.7–1714.8) | 1481.9 (1381.8–1565.4) | 831.9 (770.8–886.7) |

At one million rows this run is 1.88× faster than stock Flink and 1.78× faster than
the previous StreamFusion sink path. At 200k rows the respective ratios are 1.08×
and 1.29×; startup and commit costs account for much more of the job. The
[append trial evidence](../benchmarks/hudi-append-20261109.json) retains all trials,
configuration and earlier investigation results. Run the existing benchmark command
with `-Dtest=NativeHudiSourceTest#benchmarkCompleteArrowReadWritePipeline`.

### Continuous COW catch-up

`benchmarkContinuousAggregatePipeline` runs `SELECT SUM(id),COUNT(name),COUNT(*)
FROM h` through each continuous source and waits for the aggregate containing all
rows from one completed commit. It retains SQL planning/admission, Java discovery
and storage I/O, JNI, filtering and result collection. Fixture writes and plan
assertions precede timing; cancellation and task termination follow timing. Hudi's
absent-start default selects the latest completed commit, exercising the exact
instant-set filter. The previous SF baseline keeps its native aggregate over the
stock continuous source. Resources, warmups and rotating trial counts match the
bounded benchmark; every trial verifies all three aggregate values and native plans.

| Rows | Stock median ms (range) | Previous SF median ms (range) | Native median ms (range) |
| ---: | ---: | ---: | ---: |
| 200,000 | 650.9 (588.1–718.7) | 697.2 (607.3–813.9) | 666.0 (600.3–722.2) |
| 1,000,000 | 876.2 (704.1–953.7) | 820.0 (715.2–980.5) | 614.0 (598.2–872.1) |

At one million rows the native median is 1.43× faster than stock and 1.34× faster
than previous SF. At 200k rows it is about 2% slower than stock and 4% faster than
previous SF, with overlapping ranges. These are continuous-source catch-up results;
they do not establish steady-state latency across many commits. The
[continuous trial evidence](../benchmarks/hudi-continuous-20261110.json) retains all
trials and boundaries. Run the benchmark command above with
`-Dtest=NativeHudiSourceTest#benchmarkContinuousAggregatePipeline`.

### MOR read and changelog pipelines

The bounded MOR benchmark runs `INSERT INTO output SELECT id,name,ts FROM h`
through a committed immutable COW append target. Its previous SF baseline uses
stock Hudi MOR reads and the native append sink. The continuous benchmark runs
`SELECT id+1,name,ts FROM h` through a Calc and row collector. Its previous SF
baseline uses the stock source, native Calc and both perimeter conversions.
Timing includes SQL planning/admission, discovery, Java storage I/O, JNI, merging,
and encoding or final row collection. Fixture writes, EXPLAIN, stock readback and
continuous cancellation are outside timing.

Inputs have a Parquet base plus 25% full-row event-time updates in Parquet logs,
with nullable VARCHAR payloads and BIGINT keys/versions. Continuous inputs add
1% key-only deletes in a third completed commit. Every bounded trial verifies
stock-read count, key/version sums and non-null name count. Continuous trials
verify key/version sums, name count/hash, delete count and every operation kind. Native plans require
columnar source-to-consumer flow. Resources, warmups and trial counts match the
other connector comparisons above.

| Workload | Rows | Stock median ms (range) | Previous SF median ms (range) | Native median ms (range) |
| --- | ---: | ---: | ---: | ---: |
| MOR to COW append | 200,000 | 825.5 (697.9–903.0) | 649.7 (622.6–778.3) | 693.7 (563.7–776.1) |
| MOR to COW append | 1,000,000 | 1811.8 (1631.9–1883.7) | 1137.4 (957.7–1349.5) | 1077.0 (1031.7–1121.9) |
| Continuous MOR Calc | 200,000 | 1024.4 (993.8–1053.1) | 995.8 (978.8–1100.2) | 1066.5 (1002.5–1196.5) |
| Continuous MOR Calc | 1,000,000 | 3333.1 (3174.4–3532.3) | 3406.8 (3098.9–3746.1) | 3200.6 (3018.9–3403.2) |

The million-row bounded pipeline is 1.68× faster than stock and 1.06× faster than
previous SF. At 200k it is 1.19× faster than stock and 6.8% slower than previous SF,
with overlapping ranges. The million-row continuous pipeline is 1.04× faster
than stock and 1.06× faster than previous SF. At 200k it is 4.1% slower than stock
and 7.1% slower than previous SF, with overlapping ranges. These continuous runs
measure initial catch-up over three completed commits, including the final row boundary.

The first Java-reader implementation and the per-row projection experiments remain
in the evidence with their unfavorable comparisons. The current reader defers
terminal projection to Arrow gathering, reuses transient base cursors and restores
Hudi's fetch-thread overlap with the sink. The JFR diagnostic identified per-row
projection allocations and is excluded from uninstrumented timing claims.
[Trial and profile evidence](../benchmarks/hudi-mor-read-20261111.json)

Run the benchmark command above with
`-Dtest=NativeHudiMorSourceTest#benchmarkCompleteMorAppendPipeline` or
`-Dtest=NativeHudiMorSourceTest#benchmarkContinuousMorCalcPipeline`.
