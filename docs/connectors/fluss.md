# Apache Fluss

**Status:** Experimental, enabled by default when installed. The optional `streamfusion-fluss`
module uses the released Apache Fluss Java connector 1.0.0. Disable its verified planner
substitutions with `-Dstreamfusion.fluss.enabled=false`. All 23 runnable Nexmark queries pass
with mini-batching off and matching deterministic output. The q23 mini-batch sidecar bug found by the
[four-mode sweep](#readme-compatible-benchmark-matrix) is fixed; its
[post-fix two-million-event reruns](#q23-mini-batch-fix-validation-2026-10-08)
pass exact parity with memory and RocksDB state.
The admission whitelist keeps unsupported connector combinations
on the stock connector. Benchmark evidence remains dominated by short jobs and does not
establish uniform sustained speedups.
[Final 2M-event results](#final-nexmark-validation) include all append-only trials and
separate primary-key correctness pairs.

The integration uses the existing Java connection for metadata, security, routing and
broker RPCs. Concurrent Arrow readers and writers with identical complete client
configuration share a reference-counted connection within the module's classloader.
Each reader keeps its own split/checkpoint state and each writer keeps its own ID,
sequences and acknowledgement queue. Closing one owner leaves the other owners usable;
the last owner removes and closes the connection synchronously, with no idle cache or
background teardown. Its Netty event loop uses zero quiet time on last-owner close,
and termination is awaited. `-Dstreamfusion.fluss.fast-close.enabled=false` restores
the SDK quiet period for diagnostic comparisons. Configuration is copied before
the Java client can mutate it.

It reads schema-less Arrow IPC messages from `FetchLog`, preserving the
primary-key log's per-row change-type sidecar, and hands owned Arrow buffers to the
existing StreamFusion Java-to-Rust bridge. Compatible vector layouts share buffers;
timestamp input is converted to StreamFusion's component representation. Millisecond
input retains the received millis data and parent validity buffers, adding a zero
fractional-nanos buffer and a small all-valid child bitmap without rebuilding timestamps row by row.
Millisecond timestamp output shares the existing millis and validity buffers. It does not depend
on fluss-rust or require broker changes. The data plane uses version-specific Java
RPC implementation APIs, pinned to connector 1.0.0; this is not a stable public Arrow
polling contract. [Issue #25](https://github.com/datafusion-contrib/StreamFusion/issues/25)
tracks the future public-API migration.

The accelerated connection selects direct receive allocations instead of the released
SDK's default heap-preferring accumulator. Its length-aware accumulator reserves a
complete direct frame once the four-byte length is available, avoiding repeated growth
copies for fragmented large replies. It preserves the released channel initializer,
handshake, security and idle handlers. Installing it accesses the pinned SDK's private
connection/bootstrap fields; unavailable fields or reflective access cause planner
fallback before acceleration. This is an explicit version-specific transport hook,
not a new public Fluss API.

Direct RPC receive allocations are retained through Arrow's foreign-allocation API and
charged to its allocator until the last vector reference closes. IPC metadata is parsed
separately, and the body buffers reference the received allocation. Uncompressed bodies
avoid a network-buffer-to-Arrow body copy; compressed bodies still need decompression
allocations. Non-direct buffers and remote files use the copying decoder. Rust may copy
under-aligned buffers during import, and high-precision timestamp conversion also allocates;
the complete path is not claimed to be zero copy. Append serialization uses one exactly
sized wire byte array, avoiding growing streams and their final array copy; the released Java request encoder still copies it into the outbound RPC buffer.
Fluss's memory-segment zero-copy send path is used for server responses, not client requests.
LZ4 uses the released Fluss block streams with a bounded 8 KiB adapter scratch buffer
and Arrow-backed output, eliminating whole-vector heap staging and growing output
streams. Compression temporarily reserves an Arrow output sized for the uncompressed
input plus frame overhead, then serializes only its written extent. This reservation
is allocator-accounted even for highly compressible input. The released compressor
still allocates its block workspace; decompression
still creates new Arrow buffers. NONE and ZSTD retain their released codecs.

## Current implementation boundary

The source admits streaming `ARROW` log tables, including partitioned and primary-key
logs, with earliest, latest or timestamp startup (append-only `full` is an earliest log
scan) and the upstream enumerator's bounded stopping offsets. Fluss's enumerator owns
partition discovery, partition pruning, and each partition's actual bucket range. Existing
partitions retain their bucket count after `ALTER TABLE ... SET ('bucket.num'=...)`;
new partitions use the new default, including partitions created during a running job.
The reader acknowledges partition removal after releasing queued data and finishing an
outstanding fetch, and removes those splits from checkpoint state. Until acknowledgement,
retiring splits remain in checkpoints so recovery can repeat the removal protocol.
Earliest-offset sentinels are resolved through the SDK, including empty buckets; bounded
empty splits finish without repeated fetches. Fetch and produce requests include the
actual partition routing bucket count, rather than the current table default.

Nonempty top-level projections on schema-version-1 tables are sent to the broker.
On evolved tables, complete batches are selected locally, filling historical missing
nullable trailing columns. Projected structs retain their complete nested fields.
CDC kinds stay aligned. Repeated scans share a source only when their partition and
record-batch predicates match, using their serialized structure rather than display text.
Checkpoint offsets advance after downstream collection;
recovery inside a batch slices away previously consumed rows.

Streaming append-log batch predicates accepted by Fluss are serialized with the released
predicate encoder and sent with the full table schema ID, independently of projection.
Broker statistics skip whole batches; Flink's residual predicate remains in the native
query. Filtered-end offsets are queued after returned batches, including offset-only
entries when nothing matches, so checkpoint progress never overtakes uncollected data.
Tiered logs use Fluss's downloader and local column selection; selective remote range
reads are not implemented.

Primary-key initial snapshots, batch-mode scans, data-lake hybrid reads, merge engines,
point-lookup/limit/count pushdowns, empty projections and unsupported watermarks retain
the stock source.

The [client settings audit](fluss-client-settings.md) distinguishes settings delegated to
the released Java connection from policies implemented by the Arrow reader/writer.
Connection identity, connect/request timeout, network threads, heap/direct reception,
and released plaintext/SASL configuration are forwarded. Dynamic partition creation's
explicit enable/disable option is also supported. Unsupported scanner/writer policies,
filesystem settings and unknown keys retain the stock endpoint, even when explicitly
set to their defaults. Diagnostics name settings without their values.

The Arrow sink is limited to insert-only input and explicitly resolved `ARROW` append-only tables. Partitioned
inputs are split using the existing Rust Arrow partition kernel; payloads remain columnar,
and only each group's first-row partition keys are exposed to Fluss's own partition-name
getter. The released dynamic partition creator owns creation and auto-partition validation.
Each partition uses its metadata bucket count. Tables without bucket keys use their own sticky
assigner. Scalar bucket keys use a connector-owned Rust encoder matching Fluss 1.0's compacted
key bytes and two-stage Murmur hash, verified against the released SDK. The existing partition
kernel groups rows by the resulting bucket number in stable order, with one payload permutation;
a single-bucket batch shares its input buffers. Composite keys, including timestamp components,
are supported; complex bucket keys stay stock. Fluss forbids partition columns in bucket
keys. Writer sequences,
request coalescing and acknowledgement chains are keyed by the complete table/partition/bucket. Input fields map positionally to destination columns. Production uses
`ProduceLog` with the table's Arrow compression setting: matching that setting is needed
because broker projection reconstructs IPC compression metadata from table configuration.
The writer encodes on the task thread and pipelines a bounded queue across independent
buckets, using the released Java client's sticky no-key bucket assigner at sealed
Arrow-batch boundaries. Requests to each bucket remain sequential, including retries, to preserve batch
sequence order. Queued batches for one bucket can share a request through a composite
byte view, with at most five batches and the smaller of 1 MiB and the configured request-size limit.
Larger batches use individual requests; grouping wide batches into multi-megabyte
messages regressed the uncompressed transport benchmark. Five matches
the released broker's deduplication history, so retrying a complete grouped request does
not replay batches that have already fallen out of that history. Larger input roots are
split into columnar slices toward the writer's batch size; a single oversized record
still fails like the stock writer. Checkpoint and end-of-input flushes wait for every acknowledgement.
The queue obeys the Java client's writer-buffer limit and a 64-batch ceiling; production
queue reservations are charged to the shared TaskManager memory budget. Primary-key production,
batch sinks, explicit undo-recovery identities, explicit bucket/dynamic-partition shuffle,
merge engines, lake writes, row modifications and sink materialization use the stock
Flink sink.
Statistics-enabled streaming writes emit V1 log batches. Rust scans the configured Arrow columns
for minimum/maximum row indexes and null counts; only two extrema per column are converted for
the released SDK's schema-aware statistics serializer. This preserves Java NaN/signed-zero,
string, decimal and timestamp ordering without materializing every input row. Unsupported
min/max types retain the SDK's null-count-only behavior. Statistics are recomputed after each
bucket split and size split, so each batch describes precisely its own rows. The broker can
prune these batches during streaming reads; residual row filters remain necessary.
Tables without statistics continue to emit V0 batches. The optional `streamfusion-fluss` artifact
now bundles its own `libstreamfusion_fluss` extension for hashing and statistics; the core
remains connector-neutral. Java SDK connections, routing metadata and RPCs remain unchanged.

Append delivery matches the released connector's default at-least-once recovery contract.
Writer IDs and per-bucket sequences deduplicate retries within one writer lifetime, and
duplicate-sequence replies acknowledge already committed batches like the SDK.
Checkpoint flush waits for acknowledgements. Replaying data after a job restart can append
duplicates; this path does not implement a transactional or checkpoint-aware undo protocol.
The Kafka README suite uses transactional exactly-once output, so cross-connector timing
comparisons must state this difference rather than imply equal delivery guarantees.

The admission whitelist and integration tests cover the contracts described above;
other combinations retain the stock connector. The Fluss-to-Fluss Nexmark harness
reuses the Kafka suite's queries and corpus. Its completed results and isolated
transport measurements are below; they establish improvements within the measured
boundaries, rather than uniform speedups for every query or deployment.

## Build and validation

Install Flink's normal `fluss-flink-2.2:1.0.0` connector alongside `streamfusion-core`
and `streamfusion-fluss`. The optional integration adds no Rust library and reuses the
core's Arrow handoff. Fluss classes are excluded from the core artifact. Shared-source
Javadoc generation resolves the released Fluss API as a documentation-only dependency,
following the other optional connectors; it does not add Fluss to the core consumer POM.

Compilation and Arrow schema/framing/ownership/admission contracts also pass with
Flink 1.18. The full end-to-end benchmark and packaging validation use Flink 2.2.

The module's Docker integration fixture runs released Fluss 1.0.0 and ZooKeeper 3.9.2.
The Fluss brokers use a 1 GiB JVM heap and a 512 MiB direct-memory
limit. Fluss also requires its coordinator and ZooKeeper; their overhead remains part
of the topology and is not claimed to be identical to Kafka's:

```sh
mvn -pl streamfusion-fluss -am test \
  -Dtest=FlussArrowClientTest,FlussArrowLogBatchTest,FlussArrowSchemaTest \
  -Dsurefire.failIfNoSpecifiedTests=false
```

These tests exercise official-client interoperability, projected primary-key changelogs,
resuming within an append batch, reader checkpoint/restore with queued batches, failed
downstream collection, historical nullable columns, paused-broker append flush,
splitting large Arrow roots at the configured request limit, repeated grouped-request
deduplication, wire checksums and shared-buffer lifetime. They are
correctness checks, not performance measurements.

The opt-in matrix harness runs with the release profile. It accepts the same query selector
as the Kafka matrix and reports all individual repetitions rather than a single best run. Each native plan
must contain native interior operators, with a row boundary permitted only for the
intentionally stock primary-key sink. Its not-null constraint and stream-record
timestamp insertion operators are permitted only downstream of that transpose:

```sh
SF_FLUSS_BENCH=true SF_MATRIX_QUERIES=q0 SF_ROWS=1000000 mvn -Pbench -pl streamfusion-fluss -am test \
  -Dtest=NexmarkFlussBenchmark -Dsurefire.failIfNoSpecifiedTests=false \
  -Dnexmark.warmups=1 -Dnexmark.runs=3
```

Input seeding, output validation and table cleanup occur outside the measured job duration.
Both runs use the same preloaded four-bucket input, queries, watermarks, parallelism and
checkpoint interval. Primary-key output remains the stock sink in the native run. The
harness compares exact output multisets using bounded-memory external sorting outside
the timed execution, including duplicate counts; q12's processing-time windows cannot use wall-clock output equality as a parity assertion.
The static lookup result in q13 is deterministic and remains subject to output parity.

Kafka references use the existing headline results in `readme.md`; this integration
does not rerun Kafka benchmarks. Comparisons cover only append-only output queries.
The published Kafka table reports StreamFusion/Flink speedups on an Apple M1 Max;
this machine's Fluss measurements have their own configuration and cannot establish
an absolute cross-machine Fluss/Kafka throughput ratio. The README's expression variants
and exactly-once Kafka delivery also differ from the exact Fluss fixture's settings.

The input event table is append-only. Output tables are primary-key tables only for
q4, q9, q15, q16, q17, q18 and q19, following the Kafka suite's existing upsert keys.
The remaining queries write append-only tables and must engage the Arrow append sink.
The harness checks the actual broker table modes as well as the executed source/sink plans.

### README-compatible benchmark matrix

To run all four README state/mini-batch combinations against stock Flink using
Fluss at both boundaries, run the following sequentially with Java 17 (release builds only):

```sh
for backend in memory rocksdb; do
  for mini_batch in false true; do
    SF_FLUSS_BENCH=true SF_ROWS=2000000 SF_FLUSS_STATE_BACKEND="$backend" \
      SF_FLUSS_MINI_BATCH="$mini_batch" \
      mvn -Pbench -pl streamfusion-fluss -am test \
        -Dtest=NexmarkFlussBenchmark -Dsurefire.failIfNoSpecifiedTests=false \
        -Dnexmark.warmups=1 -Dnexmark.runs=3 -Dsf.extraJvmArgs=-Xmx2g
  done
done
```

`SF_FLUSS_MINI_BATCH` defaults to `false`. When enabled, both engines use the
headline Kafka settings: two seconds allowed latency and 50,000 rows per batch.
Each result line identifies the backend and mini-batch mode and retains every
measured duration. Memory means heap/native memory state; RocksDB means disk
state, rather than disabling all memory use. Four Fluss buckets on both boundaries
correspond to Kafka's four topic partitions; these are unpartitioned Fluss tables.
SQL and deterministic parity checks remain exact, including queries whose Kafka
headline native runs enable incompatible expression variants. Thus these runs
match workload, state and mini-batch settings, but do not reproduce those semantic
variants or Kafka's JSON encoding and exactly-once output guarantees.

The original mini-batch-on sweep exposed a regular-join correctness gap in q23:
native buffering treated sidecar-bearing input with non-unique join keys as
replacement rows and invented deletes, which the append-only sink rejected.
The fix retains every INSERT row on non-unique inputs even when an upstream join
attaches a row-kind sidecar. Unique updating inputs keep replacement folding;
non-unique updating inputs keep immediate execution. The reproduction uses
`SF_MATRIX_QUERIES=q23 SF_ROWS=100000 SF_FLUSS_MINI_BATCH=true`.
q23 remains an append-only output table. The failure logs and original matrix
below are retained as pre-fix evidence. See the
[regular-join page](../operators/joins/regular-join.md#mini-batch-coalescing) for the buffering contract.

### Four-mode results (2026-10-08)

The complete local sweep uses Flink 2.2.1, Fluss 1.0.0, Java 17 and a
release/mimalloc build on Linux with an Intel Core i7-12650H (16 exposed logical
CPUs) and 9.7 GiB host RAM. Each mode uses two million events, parallelism four,
four input and output buckets, UTC, four-second watermarks and one-second
checkpoints. The test JVM has a 2 GiB heap; each Fluss server has a 1 GiB heap and
512 MiB direct-memory limit. Disk mode gives both engines fixed 128 MiB RocksDB
state pools per slot. Mini-batching on uses two seconds allowed latency and
50,000 rows. The benchmark profile disables same-JVM zero-copy exchange.

Each query has one stock/native warmup pair and three measured pairs, in stock
then native order, with a fresh JVM and broker cluster per mode. The source is
append-only; only q4, q9 and q15–q19 have primary-key outputs. Their writer remains
stock in both engines. Setup, input seeding, parity, sorting and output table
cleanup are outside timing; SQL execution, checkpoint/drain and synchronous job
teardown remain inside. All deterministic output checks pass for completed cells;
q12's processing-time output is exempt from exact equality. No expression
variants are enabled and Kafka is not rerun.

The four sweeps produce **90 valid cells and 540 measured durations**. Both
mini-batch-on sweeps fail during q23's native warmup with
`Append-only production received a changelog`; that cell has no speedup. The
100,000-event isolated reproduction fails identically. All seven primary-key
queries pass in all four modes. Each table cell is stock median job duration /
native median job duration, equivalent to relative input throughput for the same
corpus. The geomean uses the same 22 queries in every column (q23 remains visible
but is excluded from that common aggregate).

| Query | Memory, off | Memory, on | Disk, off | Disk, on |
| --- | ---: | ---: | ---: | ---: |
| q0 | 9.64× | 10.11× | 10.22× | 11.29× |
| q1 | 9.14× | 22.04× | 11.29× | 11.62× |
| q2 | 13.07× | 14.50× | 15.50× | 16.52× |
| q3 | 6.96× | 8.76× | 7.87× | 8.32× |
| q4 | 0.70× | 2.55× | 1.87× | 1.45× |
| q5 | 4.77× | 4.65× | 9.18× | 4.50× |
| q7 | 1.00× | 1.21× | 3.15× | 4.19× |
| q8 | 8.23× | 8.51× | 5.26× | 10.70× |
| q9 | 4.68× | 2.05× | 1.38× | 3.08× |
| q10 | 6.22× | 6.43× | 6.21× | 6.65× |
| q11 | 14.16× | 14.54× | 31.59× | 19.94× |
| q12 | 14.83× | 14.91× | 11.87× | 13.06× |
| q13 | 11.39× | 10.54× | 12.26× | 10.14× |
| q14 | 6.41× | 5.52× | 5.70× | 5.40× |
| q15 | 1.33× | 1.82× | 1.53× | 1.64× |
| q16 | 1.77× | 0.88× | 1.48× | 1.31× |
| q17 | 1.41× | 1.69× | 1.56× | 1.68× |
| q18 | 2.30× | 1.83× | 1.40× | 2.44× |
| q19 | 2.21× | 2.45× | 1.29× | 2.53× |
| q20 | 3.78× | 6.04× | 2.64× | 7.51× |
| q21 | 5.91× | 5.84× | 6.09× | 6.34× |
| q22 | 13.49× | 13.32× | 12.22× | 27.04× |
| q23 | 7.17× | **FAILED** | 4.48× | **FAILED** |
| **Geomean, common 22 queries** | **4.66×** | **5.07×** | **4.83×** | **5.71×** |

Including q23 in the successful mini-batch-off columns gives geomeans of
**4.75× memory/off** and **4.82× disk/off** over all 23 queries. Memory/off q4
regresses to **0.70×**, and memory/on q16 regresses to **0.88×**; neither is removed.
There are no median regressions in the disk columns in this sweep. These are
short full-job measurements: stock Fluss client teardown contributes materially,
and stateful queries retain large variability. Host memory use exceeded 8 GiB
near updating queries; no continuous host-pressure recording was made. These
results do not establish uniform sustained-throughput gains or an absolute
comparison with the README's Apple M1 Max Kafka results.

The [individual trials](../benchmarks/fluss-headline-2026-10-08/trials.csv),
[min/median/max, sample standard deviation and speedups](../benchmarks/fluss-headline-2026-10-08/summary.csv),
[all 92 cell statuses](../benchmarks/fluss-headline-2026-10-08/status.csv) and
[environment](../benchmarks/fluss-headline-2026-10-08/environment.json) retain the
complete configuration and variability. The
[executed plan archive](../benchmarks/fluss-headline-2026-10-08/plans.tar.gz) contains
stock/native plans for every query and mode, including failed q23 plans.
Compressed logs and JUnit reports for each mode, plus the isolated q23 failure,
live beside the CSVs. Run `python3 docs/benchmarks/fluss-headline-2026-10-08/summarize.py`
to regenerate the trial and summary CSVs from the archived logs.

### q23 mini-batch fix validation (2026-10-08)

The fix uses planner-proven join-key uniqueness when choosing buffering. A
non-unique insert-only input retains every Arrow batch even when the preceding
native join attaches an INSERT row-kind sidecar. This preserves duplicate rows
and avoids invented replacement deletes. Unique updating inputs still fold
first/preimage and final/postimage changes, while non-unique updating plans keep
immediate execution. No Nexmark SQL, schemas, data, output table mode or planner
admission was changed for the fix.

Both q23 mini-batch-on cells were rerun at commit `1402a3a0`, using the original
release/mimalloc configuration: two million events, parallelism four, four input
and output buckets, append-only output, two-second/50,000-row mini-batching,
one warmup pair and three measured stock/native pairs per backend. Every pair
passes exact output multiset parity. Plans retain both native non-unique regular
joins and the native append-only Fluss endpoints; RocksDB engagement is checked
for both engines. The previous implementation fails this configuration before
producing valid timing samples, so there is no pre-fix speedup to compare.

| State, batching on | Stock trials (s) | Native trials (s) | Stock median (s) | Native median (s) | Median speedup |
| --- | --- | --- | ---: | ---: | ---: |
| Memory | 9.434 / 11.238 / 11.183 | 1.692 / 1.906 / 1.453 | 11.183 | 1.692 | **6.61×** |
| RocksDB | 15.402 / 17.551 / 37.746 | 4.324 / 7.986 / 3.205 | 17.551 | 4.324 | **4.06×** |

These are q23-only reruns, not a new complete four-mode sweep. The original
90 valid cells, two failed cells and common-22-query geomeans above remain
unchanged as pre-fix evidence. The new dataset adds 12 measured durations.
Disk stock and native trials have substantial variability; the full-job timer
includes teardown and these results do not establish sustained throughput.

The [post-fix samples](../benchmarks/q23-mini-batch-fix-2026-10-08/trials.csv),
[summary including ranges and sample standard deviations](../benchmarks/q23-mini-batch-fix-2026-10-08/summary.csv)
and [environment and source hashes](../benchmarks/q23-mini-batch-fix-2026-10-08/environment.json)
are retained with both logs, JUnit reports and executed plans. Run
`python3 docs/benchmarks/q23-mini-batch-fix-2026-10-08/summarize.py` to regenerate
these CSVs. The same directory retains the failing pre-fix unit regression,
the passing 658-test native run (two existing ignores), 35 Java join tests,
16 release Criterion join fixtures and 18 join allocation profiles. SQL parity
covers duplicate three-way joins with batching off and sizes one/four; direct
operator tests cover sidecar combinations, subsequent bundles, shared count,
watermark flushes and checkpoint/restore.

The direct five-batch retry test waits up to 30 seconds for the newly created
bucket's leader assignment before opening its writer gateway. Table creation
finishes before asynchronous assignment; an immediate metadata lookup can fail
before the retry request itself is sent. The wait only establishes the test
precondition. Replaying the same five batches three times must still produce
exactly five offsets and the five expected rows.

## Partitioning and streaming-filter validation (2026-10-07)

The expanded Fluss suite passes on released Flink 2.2.1 and 1.18.1: 119 tests per
line, including the full suite and targeted final regressions on 2.2, and a clean
119-test run on 1.18. Coverage includes mixed partition bucket counts, discovery,
removal with an outstanding fetch, checkpoint restore inside a partitioned batch,
filtered offset-only progress, distinct pushed predicates, SASL/PLAIN, explicit heap
reception, disabled dynamic creation, and propagation of fetch errors on the task
thread. The existing release Criterion partition-split fixtures also pass; their
[allocation/Arrow-sharing inventory](../benchmarks/fluss-coverage-2026-10-07/partition-kernel-allocations.csv)
is a fixture check rather than a new kernel timing result.

`FlussCoverageBenchmark` measures bounded Fluss-to-Fluss `INSERT ... SELECT` jobs
using the release native library, parallelism four, UTC, a one-second checkpoint
interval, and a 2 GiB test JVM on a 16-logical-CPU Intel i7-12650H host. Each case
has one warmup per mode and three measured runs, rotating stock/previous/native
execution order. Inputs and output tables are prepared outside timing; the timer
covers `executeSql(...).await()`, including planning, task startup, Arrow conversion,
JNI, routing, RPC, acknowledgement flushes and task teardown before job completion.
Catalog closure and exact sorted output-multiset checks run outside timing. Every
warmup and measured job passes parity, and native plans assert the intended endpoints
and partition splitter or batch filter.

The three-column corpus is `(id BIGINT, metric BIGINT, part STRING)`, with
`metric=floor(id/4096)%16` and `part='p'+(id%partitions)`, flushed by the released
writer every 4,096 rows. Unpartitioned inputs have four buckets and statistics on all
columns; filtered reads select `metric=7` while retaining the residual filter.
Partitioned read inputs and write destinations start with a two-bucket `p0` partition,
then change the table default to four for newly created partitions. Partitioned reads
and filtered reads write unpartitioned two-bucket tables. All log encoding is ARROW/NONE.
Previous reproduces earlier planner coverage by disabling only the affected endpoint,
retaining current transport and interior operators: stock sink for partitioned writes,
stock source for partitioned/filtered reads. The 500,000-row write/filter and
partitioned-read sweeps used separate fixtures; the 100,000-row sweep preseeded both
input forms and ran all three workloads in one fixture.

Values below are median seconds with the complete three-run min–max range. Raw
[100,000-row trials](../benchmarks/fluss-coverage-2026-10-07/100k.csv),
[500,000-row trials](../benchmarks/fluss-coverage-2026-10-07/500k.csv), and
[representative executed plans](../benchmarks/fluss-coverage-2026-10-07/plans/500k-output_16_partitioned_write_native_1.plan.txt)
are retained. No slower trial is discarded.

| Rows | Workload | Partitions | Stock seconds | Previous seconds | Native seconds | vs. stock | vs. previous |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 100,000 | filtered-read | 2 | 9.475 [4.659–9.497] | 5.256 [5.255–5.319] | 0.653 [0.651–0.668] | 14.52× | 8.05× |
| 100,000 | filtered-read | 16 | 9.312 [4.758–9.458] | 2.644 [2.640–5.179] | 0.641 [0.631–0.644] | 14.52× | 4.12× |
| 100,000 | partitioned-read | 2 | 4.255 [4.200–4.562] | 2.191 [2.176–2.192] | 0.185 [0.172–0.197] | 23.01× | 11.85× |
| 100,000 | partitioned-read | 16 | 4.288 [4.242–4.447] | 2.168 [2.167–2.169] | 0.333 [0.319–0.339] | 12.88× | 6.51× |
| 100,000 | partitioned-write | 2 | 4.536 [4.409–4.601] | 2.433 [2.413–2.448] | 0.386 [0.339–0.433] | 11.74× | 6.30× |
| 100,000 | partitioned-write | 16 | 4.849 [4.565–9.238] | 2.603 [2.546–2.611] | 1.660 [0.870–1.739] | 2.92× | 1.57× |
| 500,000 | filtered-read | 2 | 4.411 [4.316–9.317] | 2.207 [2.186–2.223] | 0.176 [0.162–0.184] | 25.02× | 12.52× |
| 500,000 | filtered-read | 16 | 4.676 [4.660–9.656] | 5.500 [5.315–5.553] | 0.651 [0.648–1.181] | 7.19× | 8.45× |
| 500,000 | partitioned-read | 2 | 4.395 [4.318–9.198] | 2.237 [2.229–2.270] | 0.231 [0.218–0.251] | 19.04× | 9.69× |
| 500,000 | partitioned-read | 16 | 4.350 [4.284–4.462] | 2.227 [2.219–5.249] | 0.392 [0.364–0.496] | 11.11× | 5.69× |
| 500,000 | partitioned-write | 2 | 4.415 [4.378–4.598] | 2.448 [2.295–2.539] | 0.378 [0.367–0.387] | 11.69× | 6.48× |
| 500,000 | partitioned-write | 16 | 4.761 [4.597–9.532] | 2.733 [2.624–2.778] | 0.927 [0.839–1.130] | 5.13× | 2.95× |

All measured native medians beat stock and the prior fallback paths. These are
short-job totals, influenced strongly by connection shutdown, partition creation,
metadata readiness and the default 500 ms broker fetch wait. The larger input is
sometimes faster than the smaller input; independently prepared bucket contents and
lifecycle costs prevent treating these sweeps as a sustained-throughput scaling curve.
They establish acceleration within the measured boundary, rather than a uniform
speedup for arbitrary workloads, partition counts or security configurations.

```sh
SF_FLUSS_COVERAGE_BENCH=true mvn -Pbench -pl streamfusion-fluss -am test \
  -Dtest=FlussCoverageBenchmark -Dsurefire.failIfNoSpecifiedTests=false \
  -Dsf.extraJvmArgs=-Xmx2g -Dfluss.coverage.rows=500000 -Dfluss.coverage.runs=3
```

Use `fluss.coverage.rows=100000` for the smaller corpus and
`fluss.coverage.workloads=partitioned-read` for an isolated partitioned-read sweep.
The benchmark is disabled in ordinary local and CI test runs.

## Bucket-key and streaming-statistics validation (2026-10-08)

A clean released Flink 1.18.1 build passes all 132 Fluss tests without skips. Flink
2.2.1 passes the initial 130-test full suite and a clean final 92-test writer, planner,
compression, framing and ownership run, including all five expanded SDK/type tests.
Javadocs and native-payload JAR packaging also pass.

Released Fluss 1.0 SDK parity tests cover scalar and composite compacted keys,
signed hash tails, UTF-8, fixed-width character/binary values, positive/negative
large decimals, NaNs, signed zero and fractional/local timestamps. Bucket groups
preserve row order and match SDK assignment for one, seven and sixteen buckets.
Statistics match the SDK's serialized bytes, including reordered mappings,
all-null columns, sliced inputs and null-count-only binary/nested columns. Component
metadata distinguishes timestamps from user rows with identical child names.

Integration tests verify actual keyed log placement with NONE/LZ4/ZSTD, forced
request-size splits, broker pruning of native-produced statistics, partitioned SQL
parity with mixed bucket counts, and native dynamic partition creation after changing
the bucket default. Writer IDs, per-bucket acknowledgement ordering and delivery
guarantees are unchanged.

The release `FlussCoverageBenchmark` uses the same full-job timer and corpus described
above: Flink 2.2.1, Fluss 1.0.0, Java 17, parallelism four, 1 s checkpoints, 2 GiB
test heap, ARROW/NONE, one warmup per mode and three rotated measured trials. The
source has four buckets and statistics on all three columns. Stock uses Flink's
source and sink; previous disables only the native sink, reproducing the earlier
fallback while retaining the current native source and interior operators.

`bucketed-write` routes `(id, metric)` keys; `statistics-write` uses sticky buckets
and statistics on all columns; `bucketed-statistics-write` combines those features.
These destinations have two or sixteen partitions: `p0` retains two buckets and the
remaining partitions have four. Fluss forbids partition columns in bucket keys,
so `bucketed-string-write` uses an unpartitioned two-bucket destination with keys
`(id, part)` and two or sixteen distinct strings. The cardinality column below
therefore means partitions for the first three workloads and strings for the last.

The released stock keyed writer can fail buffered records if a newly created
partition's actual bucket count differs from its temporary default. All keyed
comparison modes pre-create identical partitions outside timing. A separate native
integration test exercises creation with actual two/four-bucket counts; pre-creation
is not used to claim dynamic-creation throughput.

Every warmup and measured job checks exact output multiset parity outside timing;
native plans assert the endpoints and partition splitter. The timer retains planning,
startup, Arrow conversion, JNI, routing, RPC, acknowledgement flush and task teardown.
The final [100,000-row trials](../benchmarks/fluss-write-2026-10-08/100k.csv),
[500,000-row trials](../benchmarks/fluss-write-2026-10-08/500k.csv),
[configuration](../benchmarks/fluss-write-2026-10-08/environment.json), and
[representative executed plan](../benchmarks/fluss-write-2026-10-08/plans/500k-output_16_bucketed_statistics_write_native_1.plan.txt)
retain all 144 final measured samples. [Every warmup/measured plan, including earlier
sweeps](../benchmarks/fluss-write-2026-10-08/all-plans.tar.gz) is archived; representative
final plans also remain directly readable. Values are median seconds [complete min–max].

| Rows | Workload | Partitions / strings | Stock seconds | Previous seconds | Native seconds | vs. stock | vs. previous |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 100,000 | bucketed-statistics-write | 2 | 4.283 [4.184–7.220] | 2.192 [2.192–2.192] | 0.199 [0.184–0.213] | 21.52× | 11.01× |
| 100,000 | bucketed-statistics-write | 16 | 4.375 [4.358–4.398] | 2.265 [2.212–2.570] | 0.565 [0.482–0.575] | 7.74× | 4.01× |
| 100,000 | bucketed-string-write | 2 strings | 4.197 [4.186–4.286] | 2.282 [2.179–2.369] | 0.172 [0.161–0.174] | 24.41× | 13.28× |
| 100,000 | bucketed-string-write | 16 strings | 4.179 [4.177–4.190] | 2.230 [2.160–2.558] | 0.150 [0.149–0.158] | 27.90× | 14.89× |
| 100,000 | bucketed-write | 2 | 4.321 [4.319–4.330] | 2.238 [2.214–2.295] | 0.221 [0.216–0.246] | 19.54× | 10.13× |
| 100,000 | bucketed-write | 16 | 4.421 [4.254–4.510] | 2.429 [2.230–2.579] | 0.620 [0.593–0.676] | 7.14× | 3.92× |
| 100,000 | statistics-write | 2 | 4.457 [4.411–9.201] | 2.407 [2.397–2.601] | 0.317 [0.315–0.338] | 14.04× | 7.58× |
| 100,000 | statistics-write | 16 | 4.722 [4.552–9.395] | 2.582 [2.562–2.715] | 0.740 [0.707–0.940] | 6.38× | 3.49× |
| 500,000 | bucketed-statistics-write | 2 | 4.293 [4.288–4.416] | 2.374 [2.364–2.722] | 0.229 [0.227–0.235] | 18.78× | 10.39× |
| 500,000 | bucketed-statistics-write | 16 | 4.519 [4.474–4.530] | 2.510 [2.435–2.677] | 0.660 [0.632–0.680] | 6.85× | 3.81× |
| 500,000 | bucketed-string-write | 2 strings | 4.262 [4.258–4.276] | 2.266 [2.260–2.275] | 0.169 [0.167–0.194] | 25.16× | 13.38× |
| 500,000 | bucketed-string-write | 16 strings | 4.302 [4.269–4.336] | 2.247 [2.244–2.362] | 0.165 [0.164–0.174] | 26.10× | 13.63× |
| 500,000 | bucketed-write | 2 | 4.424 [4.313–4.440] | 2.336 [2.333–2.389] | 0.261 [0.255–0.287] | 16.97× | 8.96× |
| 500,000 | bucketed-write | 16 | 4.510 [4.468–4.519] | 2.570 [2.482–2.825] | 0.670 [0.659–0.749] | 6.73× | 3.83× |
| 500,000 | statistics-write | 2 | 4.706 [4.458–9.426] | 2.311 [2.311–2.335] | 0.316 [0.268–0.344] | 14.88× | 7.31× |
| 500,000 | statistics-write | 16 | 5.190 [4.720–9.737] | 2.897 [2.667–2.918] | 0.723 [0.704–1.294] | 7.18× | 4.01× |

All final native medians beat stock by **6.38–27.90×** and previous by
**3.49–14.89×**. These are short-job totals influenced by metadata readiness,
connection lifecycle, scheduling and fetch waits; the larger corpus can finish
sooner. They establish gains within this boundary, not a sustained-throughput
scaling curve or a uniform gain for arbitrary types, bucket counts or workload shapes.

The earlier [complete 100k matrix](../benchmarks/fluss-write-2026-10-08/pre-metadata-fix-100k.csv)
retains a sixteen-partition keyed native median of 2.661 s versus previous 2.308 s.
Wall profiling exposed duplicate cold partition metadata requests among writers
sharing the SDK updater. Coordinating the cold lookup under its own update lock
moves the final unprofiled native median to 0.620 s; no independent routing cache
was added. All [before wall trials](../benchmarks/fluss-write-2026-10-08/before-wall.csv),
[after wall trials](../benchmarks/fluss-write-2026-10-08/after-wall.csv), and
[inclusive sample scopes](../benchmarks/fluss-write-2026-10-08/wall-inclusive-scopes.csv)
remain separate from the main matrix. Instrumented medians are 1.599/0.639 s;
metadata readiness and scheduling still vary. Wall counts overlap and sum across
threads, so they are not elapsed job time.

A [control on the final rebuilt helper](../benchmarks/fluss-write-2026-10-08/final-helper-control.csv)
checks the last timestamp-identity guard with the same sixteen-partition integer-key workload:
native median 0.629 s [0.496–0.664], stock 4.383 s and previous 2.284 s. All nine
control trials pass parity and remain separate from the 144-sample matrix.

The [initial partial sweep](../benchmarks/fluss-write-2026-10-08/initial-100k-partial.csv)
also retains its 27 measured samples. It stopped at invalid string-key table DDL
before that workload's warmup: the partition column was also specified as a bucket
key. No measured sample is discarded. The final matrix uses the valid unpartitioned
string-key fixture described above.

Release Criterion fixtures call production key grouping and statistics collection
at 256/4,096 rows, string widths 8/256 and cardinalities 1/128, including null payloads.
The [allocation/Arrow-sharing report](../benchmarks/fluss-write-2026-10-08/kernel-allocations.csv)
confirms shared payload buffers for one-bucket groups and newly allocated permutation
buffers for multiple buckets. Allocation requests are not copied-byte measurements;
these fixture smoke checks are not Criterion timing claims.

```sh
SF_FLUSS_COVERAGE_BENCH=true mvn -Pbench -pl streamfusion-fluss -am test \
  -Dtest=FlussCoverageBenchmark -Dsurefire.failIfNoSpecifiedTests=false \
  -Dsf.extraJvmArgs=-Xmx2g -Dfluss.coverage.rows=500000 -Dfluss.coverage.runs=3 \
  -Dfluss.coverage.workloads=bucketed-write,statistics-write,bucketed-statistics-write,bucketed-string-write
```

Use `fluss.coverage.rows=100000` for the smaller sweep. `fluss.coverage.partitions`
selects comma-separated cardinalities (default `2,16`). Optional `profile.asprof`
points to an installed async-profiler executable; `profile.event=wall` records each
measured job separately, with profiler start/stop outside its timer. Profiled results
must remain separate from uninstrumented comparisons.

## SQL regression tests and CI

`FlussSqlTest` adapts the SQL scenarios from the released Apache Fluss
[v1.0.0 source integration tests](https://github.com/apache/fluss/blob/v1.0.0/fluss-flink/fluss-flink-common/src/test/java/org/apache/fluss/flink/source/FlinkTableSourceITCase.java)
and [sink integration tests](https://github.com/apache/fluss/blob/v1.0.0/fluss-flink/fluss-flink-common/src/test/java/org/apache/fluss/flink/sink/FlinkTableSinkITCase.java)
to the released Docker broker fixture. It does not import the upstream in-process
mini-cluster or claim to port every upstream partition, security, tiering or failover
case. The supported/fallback scenarios are:

- Append projection/reordering for ARROW and INDEXED (INDEXED stays stock).
- Primary-key projected log changes with FULL/WAL images, updates and deletes.
- Append production with NONE/LZ4_FRAME/ZSTD and the upstream example rows; bounded
  Fluss input exercises the native Arrow source and sink instead of a literal source.
- The upstream literal `INSERT ... VALUES` append/primary-key cases; unsupported
  literal sources and primary-key writes retain stock production.
- Added nullable columns with historical rows: full-column stock/native SQL parity,
  native projected null filling, and an explicit expected stock 1.0 projection failure
  (`INVALID_COLUMN_PROJECTION`) when a new column is absent from an older batch.

The original 10 adapted cases passed locally on Flink 2.2 and 1.18. Expanded coverage adds
partitioned append reads/writes with mixed bucket counts, live discovery, partition pruning,
projected partitioned primary-key changelogs, delegated connection settings, statistics batch
filtering with residual/projection checks, and distinct filtered scans. Tests compare output with
stock Flink, assert expected values/changelog kinds, and save
actual physical plans while checking acceleration or fallback. The ordinary CI Java
reactor runs these tests on Flink 2.2 and 1.18. The optimized Flink 2.2 image job also
explicitly enables the full 23-query Nexmark parity smoke at 8,192 events, with no
performance assertions. It reuses that job's staged release library, runs zero warmups
and one pair per query, and uploads SQL/Nexmark plans and test reports. The environment
opt-in still protects normal local builds from unexpectedly running the full matrix.

```sh
mvn -Pbench -pl streamfusion-fluss -am test \
  -Dtest=FlussSqlTest -Dsurefire.failIfNoSpecifiedTests=false
```

## Isolated transport profiling

```sh
SF_FLUSS_TRANSPORT_BENCH=true mvn -Pbench -pl streamfusion-fluss -am test \
  -Dtest=FlussTransportBenchmark -Dsurefire.failIfNoSpecifiedTests=false \
  -Dstreamfusion.fluss.profile=true \
  -Dfluss.transport.batchRows=8192 -Dfluss.transport.batches=128 -Dfluss.transport.runs=3
```

This fixture uses a released single-tablet broker, one bucket, two nullable BIGINT columns,
one warmup and repeated trials for NONE, LZ4_FRAME and ZSTD compression. Connections,
table creation, fixture generation and teardown are outside timing. Append timing retains
serialization, compression, routing, RPC and acknowledgement for every batch. Read timing
retains broker fetch, framing, decode, vector ownership and a checksum of the first column;
the stock reader computes the same checksum through row polling. It excludes Flink,
JNI and query operators, so it cannot establish full-job performance or a Kafka speedup.
Fetch replies ending in a partial batch resume at the last complete batch's offset.
The fixture also times the released stock Java append writer using prebuilt rows,
including its buffering, serialization and final acknowledgement flush. A second
table verifies its checksum outside timing. `-Dfluss.transport.wide=true` adds a
128-character string, millisecond timestamp and decimal, with independent null patterns;
`-Dfluss.transport.async=true` exercises the production acknowledgement pipeline.
Outside timing, a warm fetched batch is also round-tripped through the production JNI
bridge. `FLUSS_HANDOFF` reports buffer bytes whose addresses remain shared and whose
addresses change, and verifies every returned value. Address changes identify ownership
or alignment work; this metric is not a general count of copied bytes. The fixture's
reader now requests StreamFusion's production timestamp component layout, and the
writer receives that same layout; these conversions remain inside transport timing.
Earlier wide measurements below used the wire timestamp layout and exclude that
conversion. The current handoff check counts nested component buffers recursively.
The optional profile switch reports serialization/compression, acknowledged produce RPC,
fetch RPC and decode/ownership durations separately. It is disabled in normal execution.
The produce-request count includes retries and distinguishes request coalescing from
faster serialization of the same number of requests. `receivedRecordBytes` counts
received record spans, including log headers, compressed payloads and possible partial
batch tails; `borrowedRecordBytes` counts those spans backed by retained direct
allocations. They diagnose the heap/direct receive boundary, not copied-byte volume
or Java-to-Rust sharing.
For a controlled comparison with the previous pipeline, set
`-Dstreamfusion.fluss.coalesce.enabled=false`; this diagnostic switch disables joining
queued batches into a request without changing encoding, slicing or acknowledgement.
`-Dstreamfusion.fluss.millis-sharing.enabled=false` restores the previous rowwise
millisecond timestamp input conversion for a controlled layout comparison.

The initial copying implementation measured these milliseconds for 1,048,576 rows on the
development machine (three trials, release profile, with JFR profiling enabled):

| Compression | Acknowledged Arrow append | Arrow fetch/decode | Stock Java row fetch |
| --- | --- | --- | --- |
| NONE | 173.719 / 115.782 / 94.891 | 64.249 / 41.479 / 48.606 | 118.573 / 107.845 / 109.337 |
| LZ4_FRAME | 119.865 / 141.752 / 122.959 | 46.448 / 45.212 / 43.626 | 94.244 / 87.903 / 93.503 |
| ZSTD | 135.384 / 130.868 / 131.151 | 27.613 / 24.983 / 23.687 | 78.842 / 81.883 / 68.122 |

These are historical measurements before direct receive was configured, as well as
before receive-buffer retention and exactly sized append
serialization. JFR heap allocation samples identified growing byte streams and their final
array copies as producer costs; sampled allocation weights are not copied-byte counts.

After receive-buffer retention and exactly sized serialization, still using heap
receive before the current direct receiver, the same fixture measured the following milliseconds. Profiling counters were enabled;
all trial checksums matched the stock reader.

| Compression | Acknowledged Arrow append | Arrow fetch/decode | Stock Java row fetch |
| --- | --- | --- | --- |
| NONE | 130.581 / 100.550 / 83.531 | 62.139 / 38.891 / 43.232 | 111.354 / 104.720 / 135.006 |
| LZ4_FRAME | 114.449 / 110.522 / 106.581 | 45.522 / 45.001 / 39.632 | 90.030 / 136.708 / 83.410 |
| ZSTD | 119.923 / 116.550 / 107.761 | 28.709 / 25.949 / 23.852 | 72.615 / 71.343 / 69.164 |

The median component durations identify the remaining costs:

| Compression | Encode/compress | Acknowledged produce RPC | Fetch RPC | Decode/ownership |
| --- | ---: | ---: | ---: | ---: |
| NONE | 9.810 | 89.499 | 34.582 | 7.089 |
| LZ4_FRAME | 37.376 | 71.445 | 15.235 | 26.491 |
| ZSTD | 50.155 | 65.128 | 6.923 | 18.298 |

Component medians are independent and do not sum exactly to the median trial duration.
RPC time dominates uncompressed append; compression and decompression remain material
for the compressed modes. These small fixed-width batches are transport diagnostics,
not representative Nexmark headline results. The
measurements above precede bounded acknowledgement pipelining.

The first complete 8,192-event sweep passed every runnable query (q0–q23 except q6),
including deterministic output parity. With pipelining enabled, the isolated ZSTD append
fixture measured 66.704 / 57.219 / 62.394 ms, versus 119.923 / 116.550 / 107.761 ms for
sequential acknowledgements. Encoding and RPC durations overlap in the pipelined path.
The completed two-million-event suite is recorded below; these early smoke results
are not used as throughput headlines.
End-to-end duration includes Java client teardown. Released Fluss 1.0 uses Netty's
default graceful event-loop shutdown for each connection; source and sink teardown can
therefore dominate short jobs. The isolated transport fixture excludes that teardown
and is reported separately rather than subtracted from job durations.

The wider fixture was then measured with 1,048,576 rows, batches of 8,192, asynchronous
acknowledgements, one warmup and three trials using the worktree's release build. Its
prebuilt input contains two BIGINTs, a 128-character string, a millisecond timestamp
and DECIMAL(18,3), with nullable string, timestamp and decimal values. Milliseconds:

| Compression | Arrow append | Stock Java append | Arrow read | Stock Java read |
| --- | --- | --- | --- | --- |
| NONE | 342.339 / 309.518 / 251.579 | 296.559 / 2302.171 / 258.526 | 325.077 / 250.203 / 183.806 | 509.313 / 442.982 / 360.554 |
| LZ4_FRAME | 277.284 / 263.920 / 260.015 | 437.020 / 421.786 / 418.525 | 205.704 / 191.920 / 185.501 | 326.823 / 331.868 / 307.326 |
| ZSTD | 230.621 / 228.045 / 266.841 | 331.756 / 365.448 / 456.675 | 254.891 / 280.214 / 282.637 | 348.604 / 392.360 / 423.462 |

Compressed production improves on the stock writer in this fixture; the uncompressed
median is slightly slower and has a large stock-writer outlier. The stock writer produced
73 / 62 / 87 log batches for NONE / LZ4 / ZSTD respectively, versus 128 Arrow batches.
This identifies request batching as a producer optimization to investigate. These remain
isolated transport measurements, not full-job or Kafka performance claims.

## Bounded transport measurements

The final transport runs use 1,048,576 rows, 8,192-row input batches, one warmup,
three measured trials, asynchronous acknowledgement and a release build. The test
JVM has a 2 GiB heap; each Fluss broker has a 1 GiB heap and a 512 MiB direct-memory
limit. These are local Linux measurements, not the README's Apple M1 Max Kafka
measurements. All trials are retained below, including the stock wide NONE read outlier.
The measured boundaries and fixtures are described above. Milliseconds:

| Fixture / codec | Arrow append | Stock append | Arrow read | Stock read |
| --- | --- | --- | --- | --- |
| Narrow / NONE | 66.327 / 47.177 / 44.180 | 135.042 / 164.185 / 106.030 | 60.739 / 38.077 / 44.049 | 113.405 / 95.694 / 173.970 |
| Narrow / LZ4_FRAME | 38.935 / 38.812 / 39.225 | 117.236 / 116.310 / 130.303 | 42.266 / 43.669 / 41.097 | 98.108 / 90.364 / 91.421 |
| Narrow / ZSTD | 53.376 / 49.086 / 55.228 | 94.476 / 91.916 / 111.386 | 27.223 / 27.129 / 26.927 | 75.410 / 76.352 / 63.827 |
| Wide / NONE | 244.941 / 251.571 / 217.394 | 257.472 / 260.633 / 285.012 | 237.527 / 174.942 / 186.097 | 1025.163 / 379.704 / 441.669 |
| Wide / LZ4_FRAME | 273.601 / 262.686 / 276.463 | 420.114 / 439.836 / 418.922 | 200.588 / 207.727 / 189.878 | 322.537 / 324.273 / 321.983 |
| Wide / ZSTD | 226.819 / 231.149 / 224.852 | 335.954 / 354.661 / 327.245 | 249.606 / 248.428 / 237.400 | 374.748 / 342.893 / 347.975 |

Narrow append medians improve on the stock Java writer by 2.86× / 3.01× / 1.77×;
read medians improve by 2.57× / 2.16× / 2.78× for NONE / LZ4_FRAME / ZSTD.
For the wide fixture, append gains are 1.06× / 1.54× / 1.48×. The wide NONE append
advantage is small, and its stock read timings vary substantially. These measurements
establish transport behavior, not a full-job or absolute Kafka comparison.

The controlled pipeline comparison disables only request coalescing; encoding,
batching and acknowledgement limits remain the same:

| Narrow codec | Previous pipeline append, ms | Small-request coalescing append, ms | Median improvement |
| --- | --- | --- | ---: |
| NONE | 127.698 / 85.911 / 77.724 | 66.327 / 47.177 / 44.180 | 1.82× |
| LZ4_FRAME | 68.136 / 61.420 / 81.813 | 38.935 / 38.812 / 39.225 | 1.75× |
| ZSTD | 54.109 / 70.249 / 56.207 | 53.376 / 49.086 / 55.228 | 1.05× |

Uncapped grouping of the wide NONE fixture produced 299.487 / 273.179 / 276.437 ms,
versus 266.728 / 197.292 / 215.678 ms with grouping disabled. It reduced RPC count
from 128 to 27 but increased elapsed time. The production 1 MiB cap keeps these wide
batches in individual requests; the capped trials appear above. Narrow batches remain
below the cap, so their grouping behavior is unchanged.

The warm JNI identity projection shares 131,072 data-buffer bytes for the narrow batch,
with 2,048 validity-buffer bytes changing address. For the wide batch, NONE shares
1,131,140 bytes and changes 133,120 bytes; LZ4_FRAME and ZSTD share 1,262,212 bytes
and change 2,048 bytes. The larger NONE change includes decimal alignment. All returned
values are checked; address changes are not reported as a general copied-byte count.

## Production timestamp layout measurements

The final wide fixture uses the production component timestamp layout for both read
and append. Configuration remains 1,048,576 rows, batches of 8,192, one bucket,
one warmup, three release-mode trials and asynchronous acknowledgements. All timings
are milliseconds; input generation and JNI identity validation remain outside timing.
Timestamp layout conversion is inside the timed transport path.

| Compression | Arrow append | Stock append | Arrow read | Stock row read |
| --- | --- | --- | --- | --- |
| NONE | 216.040 / 194.969 / 243.136 | 252.576 / 275.966 / 227.067 | 241.374 / 163.302 / 189.965 | 385.416 / 348.421 / 336.603 |
| LZ4_FRAME | 268.242 / 258.254 / 247.766 | 408.204 / 402.672 / 401.493 | 204.920 / 193.621 / 179.113 | 337.219 / 333.334 / 308.096 |
| ZSTD | 225.885 / 226.571 / 222.308 | 315.006 / 326.738 / 326.360 | 250.082 / 244.093 / 253.717 | 346.826 / 354.268 / 365.917 |

Median append speedups over stock are 1.17× / 1.56× / 1.44×; read speedups are
1.83× / 1.72× / 1.42×, respectively. A separate controlled run disabled borrowing
millisecond input buffers, retaining the previous rowwise timestamp conversion:

| Compression | Rowwise-layout Arrow read | Rowwise decode/ownership | Borrowed-layout decode/ownership |
| --- | --- | --- | --- |
| NONE | 293.177 / 172.685 / 192.602 | 70.541 / 28.358 / 30.714 | 41.460 / 21.851 / 25.784 |
| LZ4_FRAME | 200.635 / 192.904 / 199.970 | 68.375 / 66.488 / 69.260 | 66.572 / 61.651 / 58.509 |
| ZSTD | 239.674 / 231.196 / 255.752 | 106.584 / 103.271 / 106.667 | 100.595 / 96.964 / 100.563 |

Borrowing improves median decode/ownership by 1.19× / 1.11× / 1.06×. Whole-read
medians improve by 1% and 3% for NONE and LZ4, while ZSTD is 4% slower in this pair
of runs despite lower decode cost; RPC variability remains material. The optimization
removes the millis copy, but these trials do not establish a whole-read ZSTD win.
The recursive JNI handoff check observes 1,163,908 shared buffer bytes and 135,168
changed-address bytes for NONE, and 1,294,980 shared / 4,096 changed for LZ4 and ZSTD.
All returned values, including timestamp components and nulls, are validated. These
are address observations, not inferred copied-byte counts.

## Completed Nexmark validation

All 23 runnable queries pass at 2,000,000 events with native source and query-interior
checks. Append-only outputs also require the Arrow append sink. Primary-key outputs
retain their stock sink and validate its transpose/constraint boundary. Exact output
multisets, including duplicate counts, match for every deterministic query. q12's
processing-time output is exempt; q13's static lookup result remains checked. q6 is
excluded by the shared Kafka fixture because Flink SQL cannot run it.

The full-matrix runs below precede connection sharing and direct receive/codec
optimizations. Focused current results follow them. These historical runs use released Fluss 1.0.0, four buckets, parallelism four, UTC, a one-second
checkpoint interval, default memory state and mini-batching off. The test JVM uses a
2 GiB heap; each broker uses a 1 GiB heap and a 512 MiB direct-memory limit. Each query
starts in a fresh test JVM and broker cluster. Append-only queries have one stock/native
warmup pair and three measured pairs. Primary-key queries have one final correctness
pair, following their earlier repeated validation; these are not performance estimates
or Kafka comparisons. Setup, input seeding, output sorting/parity and cleanup are outside
timing. Each duration includes executeSql planning/startup, execution and client teardown.

### Append-only output trials

| Query | Stock Fluss seconds | StreamFusion Fluss seconds | Median SF/Flink |
| --- | --- | --- | ---: |
| q0 | 4.896173 / 4.983536 / 9.820781 | 4.728846 / 4.557351 / 4.506870 | 1.09× |
| q1 | 4.979650 / 4.908936 / 4.850540 | 4.584251 / 9.651440 / 4.518153 | 1.07× |
| q2 | 4.790409 / 4.628008 / 4.587244 | 4.456956 / 4.628614 / 4.356201 | 1.04× |
| q3 | 7.608857 / 4.618437 / 7.526865 | 4.661656 / 4.662505 / 4.582065 | 1.61× |
| q5 | 5.653263 / 5.329129 / 5.092166 | 5.377029 / 5.324281 / 5.123020 | 1.00× |
| q7 | 12.943312 / 14.161689 / 5.806671 | 21.144914 / 5.959881 / 6.000944 | 2.16× |
| q8 | 5.029525 / 7.691730 / 4.689957 | 5.095309 / 4.712600 / 4.812821 | 1.05× |
| q10 | 5.400998 / 5.156140 / 5.451437 | 10.185726 / 4.914559 / 4.924163 | 1.10× |
| q11 | 5.244802 / 5.057556 / 5.004168 | 4.836226 / 4.679192 / 4.644174 | 1.08× |
| q12 | 5.168656 / 4.763380 / 4.729435 | 4.498994 / 4.436776 / 4.397046 | 1.07× |
| q13 | 4.978735 / 4.903931 / 4.862256 | 4.704918 / 4.595521 / 4.582493 | 1.07× |
| q14 | 5.195299 / 5.138232 / 5.144532 | 5.239781 / 10.053488 / 5.137880 | 0.98× |
| q20 | 8.263257 / 8.294414 / 5.316719 | 5.948705 / 8.441064 / 5.663681 | 1.39× |
| q21 | 5.083205 / 4.946829 / 5.146156 | 5.028118 / 4.938488 / 4.926485 | 1.03× |
| q22 | 5.149269 / 10.012365 / 5.282074 | 4.591372 / 4.573737 / 4.575941 | 1.15× |
| q23 | 11.314178 / 8.466644 / 11.235197 | 5.621627 / 5.599494 / 5.559362 | 2.01× |

The historical append-only geomean of median speedups is 1.20×. Its q14 regresses by about 2%;
several queries have outliers, including q7's native 21.145-second trial. No trials
are discarded. These results demonstrate a working columnar pipeline, with modest
startup-dominated gains on many queries; they do not establish that Fluss is uniformly
faster than the README's Kafka measurements. The Kafka values in the landing page are
existing published references on another machine, with the documented expression and
delivery differences. The integration is enabled by default when its optional module is installed. The focused reruns below resolve the measured q14 regression. [Follow-up #301](https://github.com/datafusion-contrib/StreamFusion/issues/301) tracks the remaining lifecycle floor and broader sustained validation.

### Primary-key output correctness runs

These final single pairs pass exact output parity and native query-interior assertions.
They use the stock primary-key writer and are recorded without a Kafka comparison:

| Query | Stock Fluss seconds | StreamFusion query / stock writer seconds |
| --- | ---: | ---: |
| q4 | 10.415635 | 7.399418 |
| q9 | 10.449697 | 11.439946 |
| q15 | 9.082497 | 7.831368 |
| q16 | 10.899953 | 9.680526 |
| q17 | 10.850807 | 9.934540 |
| q18 | 9.373285 | 8.692731 |
| q19 | 35.793688 | 16.450879 |

## Matched hot-path profiling and connection reuse

JFR profiles of stock and native q0/q14 at 2M events separate source-task/fetcher
threads from fixture setup and output validation. Stock execution samples prominently
include boxed integers, string materialization and row copies. Native q14 prominently
includes generated Java expression evaluation and its Arrow row views; native plan
coverage does not imply that every expression is evaluated in Rust.

Each native source/sink task recorded two approximately 2.005-second waits inside
`NettyClient.close`, while stock source fetching closes on its separate fetcher thread.
The native chain was serially shutting down independent source and sink transports.
Reference-counted connection reuse removes redundant clients and shutdowns without
subtracting cleanup or moving it outside the job's measured lifetime.

The control disables only `streamfusion.fluss.connection-sharing.enabled`. Released
Fluss 1.0.0, 2M events, four buckets/parallelism four, memory state, mini-batching off,
one warmup and three measured pairs remain identical. All deterministic output and
native-plan assertions pass. Every trial is retained, including q20's large outliers.
Seconds:

| Query / transport lifetime | Stock Flink | StreamFusion |
| --- | --- | --- |
| q0 / independent connections | 5.016760 / 4.978390 / 9.903302 | 6.444252 / 4.530517 / 4.540318 |
| q0 / shared connection | 5.207928 / 4.922891 / 4.954520 | 2.487257 / 2.445463 / 2.409509 |
| q14 / independent connections | 5.085205 / 5.138027 / 5.027056 | 4.940219 / 4.976899 / 4.954791 |
| q14 / shared connection | 5.012289 / 5.096018 / 5.051965 | 2.909993 / 2.919888 / 2.898332 |
| q20 / independent connections | 5.465149 / 29.356056 / 14.207900 | 12.521662 / 30.652372 / 5.827588 |
| q20 / shared connection | 5.546512 / 8.048414 / 8.065166 | 3.705364 / 3.440408 / 3.398679 |

Native medians improve over independent connections by 1.86× / 1.70× / 3.64× for
q0 / q14 / q20. Against matched stock Flink, shared-connection speedups are 2.03× /
1.74× / 2.34×. q20's comparison has substantial variability; its improvement must not
be attributed entirely to lifecycle. These focused trials supersede the earlier
lifecycle behavior, but are not a new full-suite geomean.

To capture matched profiles, add a JVM recording to the same release harness:

```sh
SF_FLUSS_BENCH=true SF_MATRIX_QUERIES=q0,q14 SF_ROWS=2000000 mvn -Pbench -pl streamfusion-fluss -am test \
  -Dtest=NexmarkFlussBenchmark -Dsurefire.failIfNoSpecifiedTests=false \
  -Dnexmark.warmups=1 -Dnexmark.runs=1 \
  '-Dsf.extraJvmArgs=-XX:StartFlightRecording=filename=fluss-hotpaths.jfr,settings=profile,dumponexit=true'
```

Inspect execution samples, allocation samples and thread parks. Attribute samples by
source/fetcher thread and stack; seeding, stock output scanning and sorting are outside
timing and must not be mistaken for native query costs. JFR trials diagnose costs;
headline timings use repeated runs without recording overhead.

## Direct receive and bounded LZ4 controls

These transport controls use the same released broker, one bucket, 1,048,576 rows in
8,192-row batches, a 2 GiB test JVM, one warmup and three measured trials. Append uses
the production acknowledgement pipeline. Wide inputs include the string, decimal and
production timestamp component layout described above. Read timing retains its checksum;
JNI address checks run outside timing. No Flink lifecycle or query costs are included.
[All 90 trials and stage durations](../benchmarks/fluss-transport-controls.csv) retain
stock comparisons and unfavorable measurements. `run=0` is the first measured trial;
the warmup is separate. Diagnostic switches, applied inside the optional connector, are:

- `streamfusion.fluss.direct-receive.enabled=false`: released heap-preferring receive.
- `streamfusion.fluss.frame-aware-receive.enabled=false`: generic direct accumulation.
- `streamfusion.fluss.lz4-streaming.enabled=false`: released whole-vector LZ4 adapter.

All three default to true. The receive/codec controls were captured before the
frame-aware receiver was installed; their `directTrue` mode is generic direct receive.
The frame-aware controls hold direct receive and streaming LZ4 enabled, changing only
the accumulator. Median milliseconds from the final frame-aware control:

| Input / codec | Generic direct read | Frame-aware direct read | Stock row read | Arrow append | Stock row append |
| --- | ---: | ---: | ---: | ---: | ---: |
| narrow / NONE | 47.909 | 51.501 | 187.436 | 44.111 | 111.880 |
| narrow / LZ4_FRAME | 40.813 | 35.034 | 88.774 | 32.184 | 108.836 |
| narrow / ZSTD | 26.790 | 25.728 | 64.686 | 56.676 | 100.110 |
| wide / NONE | 203.473 | 173.883 | 350.841 | 212.937 | 236.606 |
| wide / LZ4_FRAME | 218.841 | 189.095 | 319.633 | 183.737 | 401.091 |
| wide / ZSTD | 273.821 | 229.987 | 334.174 | 220.574 | 338.870 |

Wide read medians improve by 1.17× / 1.16× / 1.19× over generic direct accumulation.
Narrow NONE whole-read median regresses 8%, although its median fetch stage falls
from 39.405 to 34.628 ms; this regression is retained. Narrow LZ4/ZSTD improve by
1.16×/1.04×. Against the matched stock Java reader, current wide read speedups are
2.02×/1.69×/1.45×. Wide append speedups are 1.11×/2.18×/1.54×. These are isolated
transport results, not Nexmark or cross-machine Kafka throughput claims.

With direct receive held fixed in the earlier codec controls, streaming LZ4 reduces
median narrow append from 38.835 to 32.530 ms and wide append from 264.357 to
178.357 ms (1.19×/1.48×); wide decode falls from 60.215 to 50.566 ms. Narrow whole
LZ4 read regresses from 43.338 to 46.030 ms in those controls. Turning on generic
direct receive alone also regresses some wide reads; the length-aware receiver above
addresses the fragmentation cost rather than assuming direct memory is inherently faster.

The broker integration test proves `borrowedRecordBytes == receivedRecordBytes > 0`
for direct receive, versus zero borrowed bytes for heap receive, with identical rows
and projection. This does not prove end-to-end zero-copy import. In the current real
broker JNI identity check, narrow NONE changes 133,120 buffer bytes and shares none:
its numeric buffers are under-aligned at their RPC offsets. Narrow LZ4/ZSTD share
131,072 bytes and change 2,048 after required decompression. Wide NONE shares 934,528
bytes and changes 364,548; wide LZ4/ZSTD share 1,294,980 and change 4,096. These are
recursive address observations, not copied-byte counters. Earlier heap-receive sharing
observations were downstream of an already-copied Java Arrow body.

### Remaining hot-path costs and rejected work

Released Fluss and Kafka clients group requests by broker and pipeline bounded work.
The four-bucket/four-reader Nexmark topology assigns one bucket per reader, so a
multi-bucket fetch grouping rewrite would not improve this workload. A bounded
one-request source prefetch prototype produced only about 1% q0 and 0.3% q14 median
improvement at 2M events, within run variability. It was removed, including its extra
queue/checkpoint state; [the rejection and every trial](https://github.com/datafusion-contrib/StreamFusion/blob/main/.claude/wontdos/fluss-source-prefetch.md)
are retained.

Remaining substantial costs are Java expression/decimal/string evaluation in q14,
required native alignment work, compression/decompression, the released request
encoder's copy into its direct outbound buffer, and the final connection's roughly
two-second Netty quiet shutdown. Cleanup stays inside end-to-end timing. Removing
these costs requires broader expression work, a compatible alignment/wire solution,
or an upstream transport/lifecycle API; no speculative fork or idle-client cache is
introduced by this optimization pass.

## Focused profile-validation reruns

After connection reuse, direct frame-aware receive and bounded LZ4, a fresh release
run selects q0/q14/q20 at 2M events, with one warmup and three measured stock/native
pairs per query. The selection shares one test JVM, broker cluster and seeded corpus;
per-query warmups remain enabled. Resources, four buckets/parallelism four, memory
state, mini-batching off and exact SQL are unchanged. No JFR recording is enabled;
planning/startup, execution and client teardown remain timed. Every exact output and
native source/interior/append-sink assertion passes. Seconds:

| Query | Stock Flink trials | StreamFusion trials | Median SF/Flink |
| --- | --- | --- | ---: |
| q0 | 4.982779 / 4.798983 / 4.764074 | 2.516900 / 2.482839 / 2.436736 | 1.93× |
| q14 | 5.026582 / 5.039969 / 5.022448 | 2.916753 / 2.911917 / 2.914523 | 1.72× |
| q20 | 5.377090 / 8.183366 / 5.317126 | 3.555080 / 3.480970 / 3.606993 | 1.51× |

These profile-driven trials preceded the complete final matrix below; they do not
define a full-suite geomean. q20 retains a stock 8.183-second outlier. The complete
23-query matrix passed again at 8,192 events after these optimizations, including
stock primary-key sinks; this correctness sweep is not a throughput measurement.
The connector/schema/ownership/admission/output contract sweep passed 41 tests.

A fresh matched q0/q14 JFR recording at 2M events includes one warmup and one measured
pair. It records one approximately 2.01-second native task close wait per job, down
from two waits in each of four native task chains before sharing. Stock task chains
still each record one close wait, with source-fetcher cleanup on its own thread. This
verifies the lifetime change without subtracting cleanup. The earlier recording had
no warmup, so raw sample totals cannot serve as normalized before/after CPU speedups.

In the current recording's captured source-task stacks, stock top samples include
string materialization (45), row copying (38), boxed longs (35) and accumulator append
(31). Native stacks prominently include generated Java expression evaluation (31),
JNI invocation (24), timestamp extraction (24), row copying (19) and boxing (14).
These are sampling observations, not exact per-stage times; JFR does not resolve Rust
execution below the JNI frame. They support the remaining expression/representation
work above, rather than attributing every native sample to network overhead.

## Final Nexmark validation

The complete final matrix uses released Fluss 1.0.0, Flink 2.2, 2,000,000 events,
four buckets, parallelism four, UTC, one-second checkpoints, memory state and
mini-batching off. The test JVM has a 2 GiB heap, each broker has a 1 GiB heap and
512 MiB direct-memory limit, and the native library is the release/mimalloc build.
All 16 append-only queries share one freshly started test JVM, cluster and seeded
corpus, with a stock/native warmup pair and three measured pairs for each query.
The seven primary-key output queries run in a separate fresh JVM and cluster with
one correctness pair each. These differ from the historical per-query-JVM runs;
they are not an identical before/after control. The focused lifecycle/codec controls
above isolate the optimizations independently.

Planning/startup, execution and client teardown stay inside job timing. Cluster
setup, input seeding, output scanning/sorting/parity and cluster cleanup remain outside.
No recording overhead is included. All deterministic output multisets, including
duplicates, match stock Flink; q12 retains its processing-time exemption. Every
native source/interior assertion passes. All append-only outputs execute the Arrow
sink, and primary-key production remains stock. q6 remains excluded by the shared
fixture because Flink SQL cannot execute it. [All 55 final measured pairs](../benchmarks/fluss-nexmark-final.csv)
are retained; no trial is discarded. Complete job seconds:

| Append-only query | Stock Flink trials | StreamFusion trials | Median SF/Flink |
| --- | --- | --- | ---: |
| q0 | 4.862733 / 4.794690 / 4.736671 | 2.518370 / 2.469245 / 2.440789 | 1.94× |
| q1 | 4.818989 / 4.771118 / 4.742557 | 2.426037 / 2.413878 / 2.456827 | 1.97× |
| q2 | 9.456386 / 4.540128 / 9.595717 | 2.311209 / 2.289842 / 2.302503 | 4.11× |
| q3 | 4.522304 / 4.523334 / 4.492609 | 2.523294 / 2.533506 / 2.514319 | 1.79× |
| q5 | 5.118286 / 4.942575 / 4.940154 | 3.080059 / 3.087738 / 3.081525 | 1.60× |
| q7 | 5.803485 / 5.623989 / 5.574316 | 4.017014 / 3.974940 / 3.893690 | 1.41× |
| q8 | 4.542199 / 4.550724 / 4.561198 | 2.521938 / 2.521738 / 2.544414 | 1.80× |
| q10 | 5.083663 / 5.067742 / 4.967778 | 2.826109 / 2.820590 / 2.785411 | 1.80× |
| q11 | 5.015348 / 4.932876 / 4.937438 | 2.340103 / 2.344385 / 2.334384 | 2.11× |
| q12 | 4.612266 / 4.599478 / 4.647023 | 2.312819 / 2.302554 / 2.305954 | 2.00× |
| q13 | 4.782113 / 4.824035 / 4.925995 | 2.419755 / 2.400287 / 2.381811 | 2.01× |
| q14 | 5.006960 / 5.017084 / 4.995941 | 2.905087 / 2.883766 / 2.861918 | 1.74× |
| q20 | 31.841183 / 5.312361 / 7.847848 | 8.953495 / 3.498312 / 3.467522 | 2.24× |
| q21 | 4.904971 / 4.806890 / 4.761857 | 2.838190 / 2.758646 / 2.764158 | 1.74× |
| q22 | 4.970074 / 4.951053 / 5.006223 | 2.383224 / 2.410302 / 2.369573 | 2.09× |
| q23 | 28.147132 / 10.862695 / 8.496231 | 3.843080 / 3.525013 / 3.481148 | 3.08× |

The geomean of append-only median speedups is **2.02×**, with all 16 medians
faster than matched stock Flink. q14 is 1.74×, resolving the previous regression.
q2, q20 and q23 have large stock outliers; q20 also has an 8.953-second native trial.
Their elevated ratios must not be interpreted as clean steady-state processing gains.
The jobs remain short and lifecycle-heavy: the measured removal of a redundant
approximately two-second shutdown is the largest end-to-end improvement. The isolated
transport controls demonstrate the additional receive/compression improvements.
No Kafka benchmarks were rerun; published README references are comparisons of
relative speedup on another machine, with different expression/delivery settings.

Primary-key output timings below are single correctness observations, not performance
estimates or Kafka comparisons. The source and query interior are native; the writer
remains stock. Exact output parity passes for every pair:

| Primary-key output query | Stock Flink seconds | StreamFusion interior / stock writer seconds |
| --- | ---: | ---: |
| q4 | 10.224197 | 7.140303 |
| q9 | 8.435984 | 7.567584 |
| q15 | 10.294327 | 7.452906 |
| q16 | 6.724553 | 6.778204 |
| q17 | 9.055959 | 6.310909 |
| q18 | 10.539551 | 8.246340 |
| q19 | 39.652596 | 15.865008 |

### CI packaging and offset alignment

The optional Fluss artifact resolves its published coordinates with the same `ossrh`
POM flattening as the other connectors. Flink 1.18's overridden Javadoc dependency
list includes the released Fluss connector so the shared source path remains resolvable;
this does not add Fluss to the runtime or core consumer dependencies.

Borrowed uncompressed string/binary offset buffers may start at an unaligned RPC address.
The decoder copies only an offset buffer whose address is not aligned to four bytes,
before exporting it to native code. Arrow-rs 58 reads the final variable-width offset
inside FFI import, before the bridge's existing `align_buffers` repair can run.
Validity and variable-width data buffers remain borrowed. This check is required in
release builds as well as debug builds; release execution succeeding did not prove
that an unaligned dereference was safe. A regression exercises all four address residues,
null values, and retained data-buffer sharing.

### Selecting disk state for the Fluss matrix

`SF_FLUSS_STATE_BACKEND=rocksdb` selects Flink's released RocksDB backend for stock
jobs and `RocksDBNativeStateBackendFactory` for native jobs, matching the existing
persistent-state comparison. Both engines receive a fixed 128 MiB RocksDB memory
pool per slot. Native RocksDB and any JVM fallback delegate retain their separate
resource pools, as in the production backend; this is not a combined process-memory
cap. This avoids the local mini-cluster default allocating roughly 3.3 MiB of
write-buffer memory for the q4 preflight, which produced repeated tiny flushes and
write stalls. That initial untimed attempt was stopped before any measured pair. The default remains `memory`; mini-batching is explicitly
disabled for both engines. Before measured disk trials, an untimed q4 preflight verifies
stock RocksDB working files and a live native RocksDB handle. Stateless queries still
use the selected job configuration without manufacturing state. A temporary local
state directory is cleaned after the cluster closes.

```sh
SF_FLUSS_BENCH=true SF_FLUSS_STATE_BACKEND=rocksdb SF_ROWS=2000000 \
mvn -Pbench -pl streamfusion-fluss -am test -Dtest=NexmarkFlussBenchmark \
  -Dsurefire.failIfNoSpecifiedTests=false -Dsf.extraJvmArgs=-Xmx2g \
  -Dnexmark.warmups=1 -Dnexmark.runs=3
```

Use `SF_MATRIX_QUERIES` to select the same append-only or primary-key output groups
listed above. Queries, schemas, event corpus, watermarks, source/sink semantics and
parity checks are unchanged by the backend selector. Disk state is an engine setting;
it does not change the Fluss broker's storage configuration. CI also exercises q4
and q5 with disk state in the optimized Flink 2.2 job.

### Retaining buffers across offset repair and failed emission

Offset realignment retains all incoming field buffers while reloading a vector.
The Arrow loader releases old fields before retaining replacements; a compressed
data column can have a sole owner even when its small offset column stayed
uncompressed and needs alignment. Without temporary retained references, reload can
free and then reuse that data allocation. The regression covers a compressed data
column with raw offsets, every offset-address residue, nulls and an actual JNI
round trip. This adds reference operations, not another data-buffer copy.

Failed source emission discards only a batch whose root no downstream consumer has
taken. Once a consumer takes it, that consumer owns closure even if collection
throws. The discard is idempotent and marks the cleaner backstop as handled; the
source does not close the same root again after conversion has already failed or
a consumer has already closed it. Broker tests cover failure before and after
consumption without advancing the consumed checkpoint offset.

## RocksDB Nexmark validation

The final measured code is commit `5cfb8199` (release/mimalloc).
The disk matrix uses the same released Fluss 1.0.0/Flink 2.2, release/mimalloc,
2,000,000-event corpus, four buckets, parallelism four, UTC, one-second checkpoints,
2 GiB test JVM heap and broker limits as the memory matrix. Mini-batching stays off.
Stock jobs use Flink RocksDB; native jobs use StreamFusion's native RocksDB backend.
Both have a fixed 128 MiB RocksDB pool per slot, with native and JVM delegate pools
remaining separate when the native backend needs both. The untimed q4 preflight
observes real stock working files and a live native store handle. The earlier local
mini-cluster default-budget preflight was stopped after tiny flush/write stalls,
before any measured pair; the explicit budget is part of these results.

All 16 append-only queries share a fresh JVM, cluster and seeded corpus, with one
stock/native warmup pair and three measured pairs each. The seven primary-key output
queries use a separate fresh JVM/cluster and one correctness pair each. Job timing
includes planning/startup, execution and synchronous client teardown; setup, seeding,
preflight, output scans/sorting/parity and cluster cleanup are outside timing. There
is no profiler in measured runs. This session ran no concurrent local validation;
a separate session launched a Maven/Javadoc build on the same host around q20.
The run therefore cannot be described as an isolated-host measurement.
Every deterministic output multiset and native
source/interior/sink boundary check passes. q12 keeps its processing-time exemption;
q6 remains excluded by the unchanged shared fixture. Primary-key writes stay stock.

[All 55 measured disk pairs](../benchmarks/fluss-nexmark-rocksdb.csv) are retained.
The [first 48 append-only pairs](../benchmarks/fluss-nexmark-rocksdb-first.csv),
whose geometric mean was 2.30×, are also retained. They preceded the final ownership
repair and integration of current main and overlapped local validation work. The
final matrix below uses the merged code with corrected buffer lifetimes and no
concurrent local validation from this session (the separate-session build noted
above still overlapped part of the run). It is a fresh measurement, not an outlier filter.
The append-only geometric mean is computed over the 16 ratios of stock median to
StreamFusion median, with no query or outlier removed.

| Append-only query | Stock RocksDB trials, s | StreamFusion RocksDB trials, s | Median speedup |
| --- | --- | --- | ---: |
| q0 | 4.919251 / 4.817975 / 4.862707 | 2.509191 / 2.379785 / 2.400867 | 2.03× |
| q1 | 4.940872 / 4.923377 / 4.814450 | 2.400765 / 2.410894 / 2.426384 | 2.04× |
| q2 | 4.578269 / 4.667894 / 4.703881 | 2.276834 / 2.286172 / 2.296290 | 2.04× |
| q3 | 4.568776 / 4.572067 / 4.552211 | 2.548523 / 2.549859 / 2.558247 | 1.79× |
| q5 | 24.816966 / 27.095601 / 24.392623 | 6.176894 / 6.082664 / 6.260626 | 4.02× |
| q7 | 12.811515 / 26.354596 / 9.847945 | 5.126424 / 15.946859 / 5.252235 | 2.44× |
| q8 | 17.070013 / 28.843333 / 11.052540 | 3.587196 / 10.478058 / 2.767388 | 4.76× |
| q10 | 10.230286 / 5.520005 / 5.288573 | 3.208818 / 3.199233 / 3.026487 | 1.73× |
| q11 | 12.235849 / 9.445565 / 12.159917 | 2.440060 / 2.427105 / 2.384007 | 5.01× |
| q12 | 7.674205 / 4.682806 / 4.711676 | 2.392962 / 2.398837 / 2.413223 | 1.96× |
| q13 | 4.860003 / 4.831920 / 4.856924 | 2.446450 / 2.409260 / 2.399621 | 2.02× |
| q14 | 5.048139 / 5.024717 / 5.047472 | 2.903404 / 2.897183 / 2.906660 | 1.74× |
| q20 | 10.559941 / 10.103125 / 10.597151 | 6.033249 / 6.389589 / 7.104098 | 1.65× |
| q21 | 9.897371 / 5.000404 / 4.887539 | 2.846612 / 2.857967 / 2.872555 | 1.75× |
| q22 | 5.541504 / 5.388507 / 5.526269 | 2.432800 / 2.439111 / 2.511234 | 2.27× |
| q23 | 16.931457 / 17.619244 / 18.429306 | 19.717781 / 5.723733 / 5.865591 | 3.00× |

The append-only geometric mean of median speedups is **2.34×**. These are
end-to-end bounded-job results, including the lifecycle floor, rather than sustained
CDC throughput. The earlier memory matrix is a separate reference and predates the
CI-required Java offset-alignment repair; it is not an isolated causal measurement
of the backend switch. Queries without keyed state still participate in the geometric
mean with the disk backend configured; they do not manufacture RocksDB work.

Queries with at least a 1.5× maximum/minimum trial spread in either engine are q7, q8, q10, q12, q21, q23. All those trials are included; three measured pairs do not establish a stable long-run distribution.

| Primary-key output | Stock RocksDB, s | StreamFusion interior with stock writer, s |
| --- | ---: | ---: |
| q4 | 13.123166 | 9.691318 |
| q9 | 51.503496 | 11.296820 |
| q15 | 12.252335 | 10.342385 |
| q16 | 11.708422 | 9.232895 |
| q17 | 6.715397 | 6.440399 |
| q18 | 12.355967 | 8.163145 |
| q19 | 27.484399 | 23.413155 |

Primary-key timings are single-pair correctness evidence, excluded from the append-only
geometric mean and from Kafka comparisons. No Kafka benchmark was rerun.

The untimed RocksDB engagement observer retries when an unrelated temporary file
disappears during directory traversal. RocksDB can rename OPTIONS files while the
job starts; that race is not evidence that the backend failed to engage. Other I/O
errors still fail the observer, and the stock CURRENT/native live-handle assertions
remain required. This observer correction does not change any measured job boundary.

### Matched transport measurements

For a Kafka/Fluss comparison, use the same event count, parallelism, repeated
trials, RocksDB budget, and mini-batch setting. Set `SF_KAFKA_NATIVE_VARIANTS=false`
for the Kafka state-backend benchmark to use the exact expression semantics used
by the Fluss matrix. The Kafka default remains the existing opt-in variant
measurement. Its `[kafka-trial]` lines retain precise execution times for every
warmup and measured run; compute medians from measured runs rather than its
legacy best-of summary. Compare absolute execution times as well as speedups
against each transport's stock Flink baseline. Kafka uses exactly-once sinks;
Fluss uses its SDK's at-least-once append path, so these delivery guarantees differ.

The Kafka RocksDB preflight permits q4's stock constraint-enforcing primary-key
sink while checking native state engagement. Timed append-only measurements still
require both the native Kafka source and native Kafka serialization sink.

For separate CPU profiles (excluded from reported timings), pass
`-Dprofile.asprof=/path/to/asprof -Dprofile.outputDir=target/profiles/fluss`
to the Fluss matrix. Each execution is recorded separately by engine and query;
fixture generation, output validation, and planning are outside the recording.
The Kafka `exactlyOnceKafkaSinkProfileAll` helper accepts `-Dprofile.backend=rocksdb`
with the same fixed 128 MiB budget and mini-batching disabled.

Use `-Dprofile.event=wall` with a separate output directory to include blocked
source, sink, and transport threads when diagnosing waits and synchronous teardown.

The matched local configuration is 2,000,000 shared Nexmark events, four source
partitions/buckets and four sink partitions/buckets, parallelism four, UTC, a
four-second event-time watermark delay, one-second checkpoints, mini-batching off,
and a 128 MiB RocksDB budget per slot. Both test JVMs use `-Xmx2g`; benchmarks use
the release native library with mimalloc. The shared deterministic event generator,
queries, schemas, and watermark expressions are unchanged. Kafka retains its JSON
wire encoding and Fluss retains Arrow, which is the transport difference being
measured. The budget applies independently to native and delegated JVM RocksDB
state; it is not a cap on total process memory.

Both transports run sequentially on the same Linux host. Kafka uses the harness's
single `confluentinc/cp-kafka:7.6.1` container. Fluss uses released
`apache/fluss:1.0.0` coordinator and tablet containers with replication factor one,
plus `zookeeper:3.9.2`; each Fluss server has a 1 GiB Java heap and 512 MiB direct
memory limit. Broker topology and total broker memory are not identical, and these
are local test clusters rather than a distributed deployment comparison. The
matched limits described above apply to the Flink/StreamFusion execution JVM and
state pools, not a common cap on all broker processes.

Use one warmup and three measured executions per engine and query. Keep all samples,
including stalls, and calculate each query's speedup as stock median divided by
native median. The append-only geomean covers exactly q0, q1, q2, q3, q5, q7, q8,
q10, q11, q12, q13, q14, q20, q21, q22, and q23. Also report Kafka-native median
divided by Fluss-native median to compare absolute connector runtimes. The Kafka
harness groups an engine's repetitions together; Fluss alternates stock/native
pairs and validates output between pairs. Both time SQL execution through job
completion, including startup, checkpoints, flushes, and synchronous transport
cleanup. Broker startup, corpus seeding, planning, and output validation are outside
the reported execution time. Profile runs are separate from timing runs.

The default immediate shutdown requests
zero quiet time from the already owned Netty event loop when its last connection
lease closes. The released Java connection still closes synchronously, and event
loop termination is awaited. Set `-Dstreamfusion.fluss.fast-close.enabled=false`
to restore the SDK quiet period for baseline controls. Immediate shutdown does not
replace checkpoint/end-of-input acknowledgement flushes or change primary-key production.

Run the timing sweeps sequentially on an otherwise idle machine:

```sh
export SF_ROWS=2000000 SF_PARALLELISM=4 SF_KAFKA_PARTITIONS=4
export SF_MATRIX_QUERIES=q0,q1,q2,q3,q5,q7,q8,q10,q11,q12,q13,q14,q20,q21,q22,q23
export SF_WARMUP=1 SF_RUNS=3 SF_KAFKA_NATIVE_VARIANTS=false
export SF_BENCHMARK=true SF_MATRIX_STATE_BACKENDS=true
export SF_STATE_BACKENDS_MINI_BATCH=false
export SF_STATE_BACKENDS_OPTIONS='state.backend.rocksdb.memory.fixed-per-slot=128 mb;table.exec.mini-batch.enabled=false'
mvn -Pbench -pl streamfusion-runtime -am test \
  -Dtest=NexmarkMatrixBenchmark#stateBackendComparison \
  -Dsurefire.failIfNoSpecifiedTests=false -Dsf.extraJvmArgs=-Xmx2g

export SF_FLUSS_BENCH=true SF_FLUSS_STATE_BACKEND=rocksdb
mvn -Pbench -pl streamfusion-fluss -am test \
  -Dtest=NexmarkFlussBenchmark -Dnexmark.warmups=1 -Dnexmark.runs=3 \
  -Dsurefire.failIfNoSpecifiedTests=false -Dsf.extraJvmArgs=-Xmx2g
```

#### Local matched baseline (2026-10-07)

The complete initial sweep uses the configuration above and exact expression
semantics. The 16-query median-speedup geomeans are **1.28×** for Kafka and
**2.45×** for Fluss against their respective stock Flink baselines. The geomean
of Kafka-native median divided by Fluss-native median is **0.668×**: Fluss native
execution takes **1.50×** as long in this bounded, startup-inclusive baseline.
These are different stock baselines, so their within-transport speedups alone
do not establish which native transport finishes sooner.

The [192 measured samples](../benchmarks/fluss-kafka-rocksdb-baseline.csv) retain
all three trials for both engines and all 16 queries. Native plan assertions pass
for both transports. Fluss checks output multisets outside timing; q12 observes
processing time and is exempt from exact multiset equality. Kafka's timed matrix
checks native source/sink engagement rather than scanning its output for parity.
Kafka q5 and q7 regress to stock in this sweep; Fluss q7, q8, q20 and q23 also
show large outliers. All remain in the median and geomean calculations. The
shutdown optimization was disabled throughout this baseline.

#### Source and sink profile findings

Separate async-profiler 4.5 CPU and wall recordings cover stock/native q0, q14,
q20 and the q5/q7 state regressions. Fluss's CPU recordings include cold first
query executions; Kafka's helper warms each query first. The wall recordings warm
both engines before recording. Kafka recordings surround the helper invocation
(including planning and topic setup/cleanup); Fluss recordings surround timed SQL
execution only. Total sample counts are therefore not a matched CPU-cost ratio. The [sampled inclusive scopes](../benchmarks/fluss-kafka-profile-scopes.csv)
retain scope counts; a stack may match multiple scopes, and wall counts summed
over threads are not elapsed job time or percentages of the critical path.

Native Fluss q0 records 2,822 CPU samples versus Kafka's 7,751 with the same 1 ms
interval. Fluss spends samples in Zstd compression/decompression and Arrow
import/export; Kafka also pays JSON decoding and serialization. These scopes identify different
perimeter work, but the differing recording boundaries and warmup prevent attributing
the total sample-count difference solely to the wire representation.
The warmed Fluss q0 wall recording contains approximately 2,004 samples in its
last connection lease close, corresponding to the fixed two-second Netty quiet
period. Kafka has no matching Netty quiet-period wait.

q20's native CPU recordings are dominated by RocksDB state work for both
transports. One stock Fluss recording also shows a heavily skewed join subtask
and extensive RocksDB/Snappy decompression, rather than a transport decoder
bottleneck. This helps explain the stateful query's large variance; its entire
slowdown cannot be assigned to source or sink copying. Request encoding still
copies through the released SDK, and compressed Arrow vectors still allocate on
decompression. The profiling evidence does not justify a claim of complete
end-to-end zero-copy transport or a uniform sustained-throughput gain from
removing shutdown time.

#### Final matched RocksDB results (2026-10-07)

With immediate shutdown enabled, the complete repeated sweep produces median-speedup
geomeans of **8.56× for Fluss** and **1.57× for Kafka** against their respective
stock Flink baselines. The geomean of Kafka-native median / Fluss-native median is
**2.12×**, so native Fluss finishes sooner geometrically across the 16 queries.
The [192 final measured samples](../benchmarks/fluss-kafka-rocksdb-final.csv) include
all three trials per engine and query. Both timing matrices finish successfully with
native perimeter assertions; all deterministic Fluss output multiset checks pass.

| Query | Stock Kafka, s | Native Kafka, s | Stock Fluss, s | Native Fluss, s | Kafka native / Fluss native |
| --- | ---: | ---: | ---: | ---: | ---: |
| q0 | 2.361 | 1.381 | 4.884 | 0.397 | 3.47× |
| q1 | 2.295 | 1.364 | 4.845 | 0.402 | 3.40× |
| q2 | 1.342 | 1.194 | 4.519 | 0.301 | 3.97× |
| q3 | 1.210 | 1.164 | 4.521 | 0.592 | 1.97× |
| q5 | 3.392 | 4.593 | 54.040 | 4.384 | 1.05× |
| q7 | 6.093 | 3.433 | 11.994 | 3.015 | 1.14× |
| q8 | 1.347 | 1.271 | 11.832 | 0.725 | 1.75× |
| q10 | 2.489 | 1.891 | 5.095 | 0.770 | 2.45× |
| q11 | 5.210 | 1.172 | 11.923 | 0.407 | 2.88× |
| q12 | 1.486 | 1.230 | 4.691 | 0.422 | 2.91× |
| q13 | 2.159 | 1.480 | 4.872 | 0.395 | 3.74× |
| q14 | 2.487 | 1.931 | 5.185 | 0.903 | 2.14× |
| q20 | 15.271 | 4.194 | 10.042 | 3.785 | 1.11× |
| q21 | 1.638 | 1.540 | 5.163 | 0.899 | 1.71× |
| q22 | 2.228 | 1.372 | 5.075 | 0.375 | 3.66× |
| q23 | 25.795 | 7.333 | 18.927 | 9.699 | 0.76× |

Native Fluss q0 moves from 2.400 s in the initial complete sweep to 0.397 s in
this sweep. The isolated q0/q14 shutdown controls support attribution of the
approximately two-second short-job improvement to lifecycle overhead. Different
stock stateful timings contribute to the final stock/native geomean: q5's stock
Fluss trials are 54.040, 55.954 and 29.845 s. Kafka's repeated geomean also changes
from 1.28× to 1.57× without a production Kafka optimization. Neither sweep is
discarded, and the final 8.56× does not measure the shutdown change in isolation.

q23's native Fluss trials are 3.923, 9.699 and 27.773 s, versus a Kafka native
median of 7.333 s. It remains in every comparison despite losing on the median.
q8 and q20 also retain stock outliers. Stateful skew, RocksDB work and scheduling
can dominate these runs; removing transport teardown does not eliminate them.
Kafka's timed matrix asserts native execution but does not rescan sink outputs;
Fluss's deterministic parity checks run outside timing. Delivery guarantees and
repetition ordering differ as described above.

The separate warmed post-change wall profiles confirm the fixed wait is removed:
native q0's final lease-close scope drops from 2,004 sampled milliseconds to 1,
and q20's from 2,005 to 2. Stock q0 still accumulates 16,001 SDK-close wall samples
across its closing owners; that sum is not 16 seconds of elapsed job time. These
post-change recordings are included in the inclusive-scope CSV. Full network
termination is still awaited, as the last-owner regression test verifies.

The shutdown change also passes a clean Flink 1.18 build against its released
Fluss artifact: all 37 transport contract tests and all 10 ported SQL tests pass
without skips. The existing CI jobs discover these tests and run the Fluss SQL
and full Nexmark smoke; primary-key production remains stock.

A subsequent clean Flink 2.2 run with no fast-close override also passes all 47
transport and ported SQL cases, confirming the new default. The last-owner test
covers shared-lease lifetime, completed network termination, retained Arrow vector
reads after shutdown, and allocator release.

A separate warmed q23 CPU recording confirms that state dominates the observed
execution: 18,135 of 23,527 native CPU samples include RocksDB, versus 50,912 of
75,995 stock samples. Native fetch, Arrow decode, Arrow encode and compression
scopes contain 459, 404, 504 and 820 samples respectively; inclusive scopes overlap.
The profiled native run takes 3.829 s and passes output parity, but is excluded
from the three-trial timing matrix and does not explain where every earlier
outlier spent time. This recording does not expose another fixed transport wait
or dominant copy scope. Remaining compression, SDK request serialization and Arrow
interop are real costs; reducing them requires more than removing an idle delay.
State layout/skew work belongs to the join path rather than a broker-network fix.
