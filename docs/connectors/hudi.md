# Apache Hudi

**Status: experimental, partial source and immutable COW append sink.**

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
The current payload types are BIGINT, INT, BOOLEAN and STRING/VARCHAR. Top-level
projection, reordered columns and NULLs remain columnar. SQL admission requires
an immediate columnar consumer, such as a native Calc or aggregate; a bare scan
returning rows retains stock Hudi to avoid a decode-to-Arrow-to-row detour. The table must be
unpartitioned and every selected base file must have the requested physical
column names and matching types. Java Hudi resolves the visible base files and
checks footer schemas once during admission; mismatches retain the stock source.

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

## Immutable COW append sink

The first native sink admits `write.operation=insert` on unpartitioned COW tables
with `hoodie.populate.meta.fields=false`, `metadata.enabled=false`, and Hudi's
unbuffered append path. It accepts insert-only input with the same four primitive
payload types as the source. Updating, overwrite, partitioned, buffered and
metadata-bearing writes retain stock Hudi. Bloom metadata, ignored write failures,
unknown Parquet overrides and unverified constraint policies also retain stock.

The pinned released sink builds its normal graph first. Following the Iceberg
adapter, StreamFusion replaces only its append operator factory, preserving Hudi's
coordinator, operator identities, markers, write statuses and commit/checkpoint
protocol. The original append function still processes key/counter access through
borrowed views of Arrow columns. The Parquet writer recognizes each batch once;
payload vectors are exported to the existing Rust encoder without a row transpose.
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

## Stock paths

The following retain the stock connector:

- Read-optimized and bounded incremental queries, MOR tables and changelog/CDC.
- Continuous reads without version-9 metadata, end-commit windows and unverified discovery options.
- Legacy sources, partitioned or bootstrap tables, source watermarks and metadata columns.
- Tables without an initialized, completed snapshot.
- Pushed filters and limits, nested or zero-column projections, and other payload types.
- Requested schema-on-read evolution, mismatched physical file schemas, non-UTC read
  conversion, bounded commit-range options, and unverified read/Parquet reader options.
- Batch-runtime SQL scans and portable compiled-plan source lowering.
- Updating/metadata-bearing and other unverified sinks, every index operation,
  compaction and clustering worker.

The released hudi-rs merge, buffer and spill algorithms are not duplicated here.
Direct Java I/O injection is not public. An unmodified `hudi-core 0.6.0` prototype
has instead used Java-selected/fetched files in a temporary local view, with real
schema/options supplied through the public table factory. Snapshot results matched
Java Hudi on version-9 Parquet and Avro logs, including stale updates and ordered
and stale key deletes. This probe is not enabled connector coverage: spill,
checkpoint ordering and cache-inclusive performance remain to be established.
It does not expose private sink buffers or add missing changelog delete emission.

## Validation

`NativeHudiSourceTest` uses stock Hudi SQL writes, compares stock/native SQL results,
and requires the native source plan. It checks projection/NULLs, restored logical
row offsets with changed batch sizes, stock fallback admission, and Arrow validity
after reader close, and the fixed admitted snapshot across later commits. Continuous tests restore the
original Java enumerator and reader state, check a rewritten COW file against
unchanged rows, and compare SQL post-images across new commits. The
optional CI lake-module matrix includes this Flink 1.18 module. Append tests verify all four types and NULLs, batch-boundary rolling,
stock readback, native SQL source/sink plans without transposes, and recovery after
a completed checkpoint with no duplicates. Other connectors' normal tests cover
the shared file-batch import helper.

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
