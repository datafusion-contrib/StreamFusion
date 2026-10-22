# Apache Doris

**Status:** partial — Arrow snapshot and incremental sources, and Arrow Stream Load sinks.
Incremental execution is implemented against connector 26.3.0 but is not server-validated. The optional `streamfusion-doris` module adapts
the released `org.apache.doris:flink-doris-connector-2.2:26.3.0` connector. Install both JARs
alongside StreamFusion's loader and core. Doris's connector requires JDK 17. The adapter has
no separate native library: it uses the core's existing Arrow C Data Interface and operators.
No sibling checkout, locally built Doris connector, or new ADBC driver is required at runtime.
The planner leaves the adapter inactive when its stock connector dependency is absent.
Set `-Dstreamfusion.doris.enabled=false` to retain stock Doris perimeters while allowing native
SQL operators between them. This is the existing-behavior control used by the benchmark.
The stock connector's Arrow row serializer also needs the matching Flink `flink-python` JAR
from `opt/` available at runtime for queries that retain the stock Arrow sink.

## Source

The source reuses the released connector's split enumeration, split/checkpoint serializers,
SQL generation, authentication and clients. Both Flight SQL and the default Thrift scanner
return Arrow. The adapter consumes these batches before the connector materializes Java rows.
Both the FLIP-27 source and `source.use-old-api=true` retain their stock split machinery.
Flight port discovery and snapshot fallback to Thrift follow the stock transport selection.

```sql
'source.use-flight-sql' = 'true',
'source.flight-sql-port' = '9611'
```

Projection, explicit read-field expressions, filters and limits are prepared by a copy of the
stock table source. Scalar types, timestamps, arrays, maps, rows and empty projections are
admitted when the actual Arrow schema can be checked against the declared Flink schema.
Decimal rescaling and zoned timestamp interpretation are pushed into the Doris projection,
so their result buffers can still be shared with Rust. Nested ROW aliases retain the stock positional interpretation; internal
StreamFusion metadata is removed without changing business values.

The `initial`, `latest` and `from-timestamp` scan modes reuse the released connector's
snapshot/stream phase assignment and polling. Incremental metadata is validated and converted
to Flink's four row kinds; business columns keep their original buffers. Reader checkpoints
retain unfinished splits, and only completed windows are published to the configured offset
table after a completed checkpoint, through the stock offset publisher. An unfinished split
is replayed after recovery, matching the stock connector; there is no per-row offset checkpoint.

Flink batch execution retains the stock planner, as with other StreamFusion connectors.

Remaining source fallbacks are other connector versions/Flink lines, MULTISET (unsupported by the released connector), source watermark
pushdown, unavailable Flight for an incremental scan, an empty Thrift response with no
verifiable schema, and physical Arrow layouts that cannot represent the declared Flink type.
The schema check is recursive; it does not relabel incompatible physical types. Legacy
string-encoded dates/timestamps and LARGEINT-as-STRING are examples requiring stock conversion.
Normal Flink watermark assignment above the source remains available.

## Sink

The serializer consumes native Arrow batches and gives Arrow IPC to the released `DorisSink`.
Stream Load and batch Stream Load writers, HTTP routing, buffering, labels, checkpoint state,
two-phase commit, transaction recovery and committers remain stock implementations.

```sql
'sink.properties.format' = 'arrow',
'sink.properties.compress_type' = ''
```

Insert, update and delete input is supported. The stock connector negotiates update-before
handling; a delete-sign column is generated when delete support is enabled for a Unique Key
table. Scalar and nested values, timestamp precision, destination aliases, explicit sink
parallelism and Flink's primary-key shuffle policy are preserved. NOT NULL error enforcement
uses StreamFusion's existing constraint operator.

Connector options are passed to stock readers, enumerators and writers, including security,
load properties, retries, buffering and commit settings. Options specific to row conversion
are bypassed with that conversion. Compression is passed through rather than silently changed;
its acceptance depends on the Doris server's Arrow Stream Load support. The verified fixture
uses uncompressed Arrow.

Non-Arrow formats, JDBC lookup functions and COPY/TVF transports retain stock execution. Shared Flink sink fallbacks
also remain for required upsert materialization, unsupported sink abilities/overwrite, NOT NULL
DROP and string/binary length trimming. These require Flink's corresponding runtime stages,
rather than a different Arrow transport.

## Buffer ownership and copies

Doris's Flight client shades its Arrow Java classes. The adapter wraps their buffers as foreign
allocations in StreamFusion's allocator, retaining the original reference managers and charging
the retained capacity to StreamFusion's off-heap budget. The reader's resources remain alive
until the last exported allocation is released. The JNI handoff shares compatible buffers.
The real Flight-response regression checks a mixed nullable BIGINT/INT/STRING/DECIMAL(18,2)
batch after reader close. All eight checked value, offset and nullable-bitmap buffers retain
their addresses through Rust (114 logical buffer bytes in this fixture), and values match.
All-valid validity masks are excluded from this address assertion because importers can
reconstruct them. The [buffer observations](../benchmarks/doris-arrow-2026-10-21/flight-buffer-sharing.csv)
record sizes, alignment and sharing without treating allocation counts as copied bytes.

This is a buffer-sharing contract after Flight has decoded its response, not a claim that
network reception, compression, or every Arrow alignment/type conversion is zero copy. Sink
IPC framing and the stock writer's `byte[]` buffering copy bytes. Native output reaches the
serializer as Arrow vectors. Timestamp output is normalized columnarly; specialized nested
physical representations use the core's reusable row views when needed to construct the wire
layout. Output serialization and these conversions may copy, as permitted by the sink contract.
Thrift IPC decoding allocates Arrow buffers once; subsequent handoffs retain those buffers.
The sink serializes the logical column prefix; internal batch sidecars stay local and are released
with the owned root.

## Validation

Build the optional module with `mvn -Pdoris -pl streamfusion-doris -am test`. With Docker and
JDK 17, `bin/doris-suite.sh` builds a fixture using the released Doris 4.1.4.1 FE/BE images and the
published 4.1.3 all-in-one launcher, with image digests pinned in `dev/doris/Dockerfile`.
The newer FE fixes hidden delete-sign resolution in Arrow loads. The script runs:

- buffer-address, JNI batch-echo, lifetime, cancellation and failed-reservation checks;
- SQL comparisons with stock Flink, including a Doris-to-Doris Arrow pipeline, saved plans,
  scalar extremes/nulls, source/sink table models, both two-phase commit settings,
  nested/timestamp round trips, both source APIs, update/delete writers,
  checkpointed sink recovery and allocation checks after completion/cancellation;
- upstream `DorisSourceITCase`, `DorisSinkITCase`, `DorisCatalogITCase`,
  `DorisLookupTableITCase` and `DorisRowDataJdbcLookupFunctionITCase` from release 26.3.0,
  compiled against the released connector and run with StreamFusion's upstream-suite agent.

The upstream checkout is pinned to commit `3a4cae3a56afd5b9a8cbf901aeff129d73a57377` and
must be clean. SQL, test methods and assertions are preserved. One prepared source copy changes
the Guava `Cache` import to the connector's published shaded package; a hash manifest records
both versions. Existing upstream ignores remain visible in Surefire reports.

The CI Doris job runs the same script and retains server logs, SQL plans, Surefire assertions
and the agent's SQL inventory. An upstream run without StreamFusion activation fails the job. Each upstream streaming SQL source
method also requires a positive nonempty Arrow-read witness for its selected Flight/Thrift
transport. Schema probes cannot satisfy these witnesses. These reader witnesses are joined to
JUnit invocations; readers do not expose Flink operator metrics, so they do not independently
establish job-level attribution or a fully native pipeline.
The audit requires all five test classes and joins each executed JUnit invocation to its
SQL/planner/job observations; stale or missing inventory records fail validation.
One unchanged upstream batch timestamp assertion expects the legacy Thrift timezone shift,
although 4.1.4.1 returns timezone-naive timestamps on both transports. The suite runs the
original method without StreamFusion first. Only the identical, exact assertion failure may
be recorded as expected in the accelerated run; errors, missing cases and different failures
are rejected. Both fixture variants still run, and the raw stock/native reports are retained.
A 4.1.3 control with Asia/Shanghai server timezone instead fails the Flight expectation. Native streaming timestamp parity
is covered separately across both transports and both source APIs.

The suite uses local fixture ports 8030, 9030, 8040, 9060, 9611 and 8050. Set
`SF_DORIS_REUSE=true` to use an already running test fixture on those ports. This switch does
not create or remove an external Doris instance.

Incremental execution has unit coverage for row-kind mapping, buffer ownership, split
serialization, boundedness and completed-window checkpoint state, plus SQL planning coverage
for all three scan modes. There is no released compatible server fixture, so incremental tests
are outside the server CI denominator. Connector 26.3.0 shipped the earlier Row Binlog API;
Doris's evolving 5.0 development documentation now describes Changelog with newer connector
requirements. This implementation does not certify compatibility with the final 5.0 protocol.
With a server implementing the pinned connector's protocol,
`SF_DORIS_REUSE=true bin/doris-suite.sh incremental` runs the original
`DorisIncrementalSourceITCase` and `DorisIncrementalSourceFailoverITCase`.

## Release-mode measurements

The final adapter was compared on core base `717b8d94`, including main's Calc optimization.
Each mode uses the same core, two warmups and six measured trials, rotated order and validated
output. Medians in seconds are:

| Transport / values | Rows | Stock | Previous | Arrow | Stock / Arrow | Previous / Arrow |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Flight / scalar | 1,000,000 | 1.706 | 2.437 | 1.341 | 1.27× | 1.82× |
| Flight / scalar | 10,000,000 | 5.844 | 5.934 | 5.886 | 0.99× | 1.01× |
| Flight / nested | 1,000,000 | 1.628 | 1.827 | 1.102 | 1.48× | 1.66× |
| Thrift / scalar | 1,000,000 | 2.134 | 2.330 | 1.898 | 1.12× | 1.23× |
| Thrift / scalar | 10,000,000 | 6.064 | 5.608 | 5.574 | 1.09× | 1.01× |
| Legacy Thrift / scalar | 1,000,000 | 1.086 | 1.160 | 0.943 | 1.15× | 1.23× |

The nested case adds TIMESTAMP(6), ARRAY<INT> with null elements and ROW<INT,STRING> to
the nullable scalar/Unicode/decimal payload. MAP is excluded from speedup claims because
the stock Arrow formatter fails after its 1,000-row batch is reused; `complex=true` and
`maps=true` reproduce the failed control. The native regression validates 5,000 map rows
across 512-row Thrift batches and committed Arrow output.

All modes use a 2 GiB task JVM, two tasks, four buckets and uncompressed two-phase Stream Load.
The fixture caps FE heap at 1 GiB, BE memory at 3 GiB and storage page cache at 256 MiB.
The [complete 144 trials, ranges, configuration and representative plans](../benchmarks/doris-arrow-2026-10-22/results.json)
retain warmups and every unfavorable timing. The [initial-core comparison](../benchmarks/doris-arrow-2026-10-22/initial-core/results.json)
keeps its 192 trials, startup-dominated 100k cases and interrupted memory-limit diagnostics.
The 10M-row Flight result is at parity (no speedup established); the Thrift gain is modest,
and the ranges overlap.

Use `sf.doris.benchmark.complex=true` for nested values, `sf.doris.benchmark.flight=false`
for Thrift, and `sf.doris.benchmark.legacy=true` for the legacy API. The benchmark always
uses a release build with mimalloc and UTC in all three client modes.

### Historical snapshot baseline


The complete-job fixture reads Doris through Flight SQL, projects and filters in Flink SQL,
and writes committed, queryable output through uncompressed Arrow Stream Load. Stock Flink
uses its normal row source, SQL operators and Arrow sink. The previous-behavior control uses
the same StreamFusion core with stock Doris perimeters, native Calc and both row transposes.
The Arrow plan has native Doris source/Calc/sink and no row transposes.

```sh
bin/doris-suite.sh benchmark \
  -Dsf.doris.benchmark.rows=100000,1000000,10000000 \
  -Dsf.doris.benchmark.warmups=2 -Dsf.doris.benchmark.trials=6
```

All modes use JDK 17, a 2 GiB JVM heap, parallelism two, four buckets, replication one, and
WARN logging with HTTP wire debug disabled. The query retains nullable Unicode strings and
DECIMAL(18,2), increments BIGINT/INT columns, and selects half the input. Each trial verifies
counts, numeric/decimal sums, non-null counts and a string CRC32 aggregate. The timer includes
SQL submission, planning, startup, JNI, IPC, network I/O, commit and visibility wait. Table/data
preparation and final aggregate validation are outside timing.

The earlier 2026-10-21 snapshot implementation on Doris 4.1.3 measured the following.
These historical numbers do not establish performance for the expanded coverage.
On Linux x86-64/Core i7-12650H, medians and full measured ranges in seconds were:

| Input rows | Stock Flink | Previous StreamFusion | Arrow source/sink | Stock / Arrow | Previous / Arrow |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 100,000 | 0.809 [0.763–21.908] | 0.775 [0.756–2.051] | 0.795 [0.728–1.864] | 1.02× | 0.98× |
| 1,000,000 | 1.187 [1.097–1.355] | 1.079 [1.017–1.098] | 0.882 [0.837–1.006] | 1.35× | 1.22× |
| 10,000,000 | 7.041 [5.848–20.864] | 6.876 [5.031–8.046] | 5.609 [4.636–15.694] | 1.26× | 1.23× |

The 100k-row case is dominated by startup/publish work: its Arrow median is slightly slower
than the previous control and ranges overlap, so it does not establish a small-job speedup.
Large-job ranges also overlap and retain substantial slow trials. A JFR diagnostic identifies
stock row materialization and conversion as substantial CPU work; these profiles include all
three modes and are separate from the unprofiled timings above.

The [complete measurements and configuration](../benchmarks/doris-arrow-2026-10-21/results.json)
retain every warmup and measured trial, earlier diagnostics and the interrupted memory-pressure
run. The fixture rotates mode order and uses release native code with mimalloc. These are bounded
jobs; they do not establish infinite-stream throughput or a Nexmark result.
