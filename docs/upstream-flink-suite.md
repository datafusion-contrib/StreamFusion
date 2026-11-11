# Upstream Flink suite

## Batch SQL development

The planner factory and suite agent now install a mode-specific batch adapter.
The observer instruments `tech.streamfusion.planner.batch.BatchPlanScan`, where
`optimizeRoots` is declared. The root-package compatibility facade is not observed
separately, so one completed rewrite produces one inventory event. Historical
inventory records with the earlier planner package remain readable.
[Batch SQL](operators/batch.md) documents its current narrow admission. The published
planner test JAR can be scanned directly with the runtime module's `batch-upstream`
Maven profile, avoiding another source checkout or Maven repository for focused
batch iterations. The profile uses released test bytecode with its original SQL,
fixtures and assertions; the ordinary source-suite runner remains available.
The profile also includes the released `flink-table-common` test JAR: the Table
API aggregate fixture uses its `LegacyRowExtension`. This remains test-only.
Flink 1.18 additionally needs its released Scala Table bridge on the test classpath.

The batch Calc contracts require stock fallback for `testManySelectWithFilter` and
`testFilterOnInteger` under the measured scalar-boundary rule, and positive native
work for `testUserDefinedScalarFunction`. Both runtime-route scope files retain
those host/native expectations. Pass the current suite agent through
`sf.extraJvmArgs`, with fresh `streamfusion.flink-suite.native-reports` and
`streamfusion.flink-suite.sql-inventory` directories. A passing upstream assertion
without those native/fallback witnesses is not acceleration evidence.

The unchanged released batch SQL `MiscITCase` adds two positive sort contracts:
`testNullOrdering` consumes 18 native rows across six queries and
`testGlobalSorting` consumes 24 across four mixed-direction queries.
`testNullOrderingOnMultiColumn` includes a STRING key and now requires positive
native sort work (56 rows). Both host lines retain these native routes. The observer records native sort input after
successful processing; merely constructing a native plan is insufficient.
[Sort validation evidence](benchmarks/results/batch-sort-sql-20261109.json) retains
per-job success, batch graph types and positive native work from the focused runs.

The released `SortLimitITCase` runs unchanged on both lines. Four signed-integer
methods and the STRING-key `testOrderByLimitBehindField` require the standalone
sorted-limit performance fallback. `testOrderBehindField` requires positive native
sort work (21 rows) on both host lines. Candidate runs before the cost
gate passed all assertions with positive bounded-sort work; those witnesses are
labelled candidate-only, not current admission. Local composed SHA2/sorted-limit
controls exercise the admitted native path. Private helper overloads of test method
names are excluded from execution-contract advice, so the outer JUnit invocation
owns one scope. No helper SQL or assertion is changed.

Released `HashAggITCase` and `SortAggITCase` now each contract the inherited
`testAggWithoutGroups`, `testAggregationWithoutGroupby` and `testCountStar` methods.
They require the exact standalone aggregate performance fallback; runtime routes
remain `batch_host_only`. Candidate native observations are retained separately
from current admission. The agent instruments the declaring aggregate base, then
resolves each witness to the concrete JUnit fixture identity, so inherited methods
cannot silently escape the contract or attach to the wrong class. Empty SUM/AVG
fixtures are parity controls without fabricated positive-input credit.

The unchanged batch Table API `AggregationITCase.testAggregationTypes` requires
`batch aggregate: standalone pipeline has no verified speedup` on both lines.
`testWorkingAggregationDataTypes` includes unsupported floating-point AVG and requires
`batch aggregate: SUM/SUM0/AVG require signed integers or DECIMAL; MIN/MAX require signed integers, BOOLEAN, DECIMAL, DATE or TIME`.
Both are `batch_host_only` controls. Separate local SQL/operator controls establish
positive native work for admitted fixed-width extrema and typed blocking-shuffle
replay against the stock result.

Released HashAgg `testTwoPhasesAggregation`, `testNullGroupKeyAggregation` and
`testGroupByOnly` require positive `NativeBatchAggregateOperator` work on both
lines. They cover ordinary grouped reductions, NULL keys and key-only DISTINCT
without rewriting upstream SQL or assertions. The same unchanged SortAgg methods
now require positive native aggregate work; the native stage restores the host's
ordered group output through DataFusion sorting. Empty grouped input remains a
parity control and receives no fabricated
positive-input requirement. Native local SQL adds signed composite keys, both
phases, filters, typed partial-state replay and optimizer DISTINCT rewrites.


The bounded operator observer attaches to the shared bounded lifecycle and records
the actual concrete operator after successful Arrow consumption. Runtime route
classification now recognizes completed batch Calc, sort and aggregate work with
matching operator graphs; streaming-only native operators still cannot establish
a batch route. Local admitted SHA2/aggregate controls require positive aggregate
work and consumer retry of completed Arrow partial states on both host lines.

The earlier batch-host-only measurements later in this page describe their original
revision; current batch admission is defined by the batch operator page and contracts.

| Task | Start here |
| --- | --- |
| Run the upstream suites | [Runner and suite selection](#runner-and-suite-selection) |
| Understand build reuse and CI shards | [Shared builds and runtime shards](#shared-builds-and-runtime-shards) |
| Distinguish a host limitation from a native failure | [Expected host failures](#expected-host-failures-in-sql-parity-audits), [comparing failed executions](#comparing-failed-sql-executions) |
| Interpret reported SQL coverage | [Complete invocation inventory](#complete-sql-invocation-inventory), [published inventory](sql-inventory/index.md) |
| Find connector and compatibility fixtures | [Flink 1.18 state fixtures](#flink-118-state-fixtures), [Iceberg streaming SQL](#iceberg-streaming-sql) |

CI relies on these upstream assertions and declared native-execution expectations for
correctness and routing coverage on both supported Flink lines. When support changes, update
the execution contracts and runtime-route scope so newly admitted paths require positive
native work and unexpected fallback fails. The exhaustive native Criterion smoke suite is
not a CI gate; see the [CI policy](ci.md#native-execution-checks-instead-of-exhaustive-benchmark-smoke).
Native execution evidence does not measure throughput.

Window witnesses register operators at their concrete or inherited `open()` implementation,
then count completed Arrow input at the shared update boundary. Instrumenting an abstract core
alone does not register an instance when its lifecycle method is declared elsewhere. Delegating
attached-window overloads count input only at the terminal call. Each of the twenty-four released
event-time restore programs has an exact execution contract. Split programs require positive
local input and separate global witnesses for slice merging and final-window merging; repeated
subtasks of one mode cannot satisfy both. The unmerged #569 integration passes all twenty-four
original programs in both stock and StreamFusion runs: twenty require native window aggregates,
three unpartitioned SESSION plans require both their unchanged stock session table function and
native group aggregation, and one retracting SESSION program requires its stock window aggregate,
positive native Calc work and the exact unsupported-native-session fallback. The released plans
have different state owners; grouping an unpartitioned session separately by business key would
change its semantics. Stock witnesses are registered only after their real state initializes and
count completed input separately from native rows. Missing either side of a mixed route fails.

A mixed contract can name `StockSessionTableFunction` alongside a native operator. A family
fallback can append `&&NativeCalcOperator+StockSessionWindowAggregate` to require its actual stock
state owner and an admitted native boundary as well as the exact reason and absent native family.
These stock labels never satisfy native rows or restore-only evidence. Native evidence keeps its
original four columns, an optional fifth native-import count column, and an optional sixth stock
state-owner count column (with an empty fifth column when necessary). Both verifiers reject stock
labels placed in the native-input column; the audit reports mixed paths separately. Use the
portable `bin/canonical-savepoints.sh` entry point: it tests and packages the current agent before
running either engine, so changed contracts cannot silently use an older agent JAR.

The unchanged `AggregateITCase.testDistinctGroupBy` and `testDistinctWithMultiFilter`
fixtures require consumed native group-aggregate work, including both local and global
stages when their released fixture enables splitting. Runtime-route scopes retain every
backend/phase invocation and Flink 1.18's explicit changelog-backend fallback. These upstream
fixtures exercise shared DISTINCT infrastructure with integer/string inputs. Floating
SUM/AVG DISTINCT additionally uses focused SQL, map-order and repeated-recovery contracts
against each released Flink line; the upstream fixtures alone do not establish floating parity.
The unchanged released `testDistinctWithMultiFilter` fixture has also been executed locally
against this ordered-retraction extension: seven invocations pass on Flink 2.2 and six on 1.18, all with positive native work.

The unchanged Table API `CorrelateITCase.testRowType` requires consumed native table-function
work on both released lines. Its nested external Row argument, collector output and original
result assertions remain upstream's. Runtime-route scopes include both HEAP and ROCKSDB
invocations so an unrelated native node or a silent fallback cannot satisfy the witness.
The DISTINCT JVM aggregate/DataView methods `testDistinctUDAGG` and
`testDistinctUDAGGMixedWithNonDistinctUsage` likewise require the JVM aggregate operator and both
backend routes, retaining upstream's positive idle retention and failure/recovery fixtures.
Flink 1.18's randomized changelog-state setting retains its established full-fallback contract;
the exact observed variant selects native work or that explicit fallback reason. Both JVM
operators are also listed in the runtime-route classifier, which still requires matching
nonempty work from successful jobs.
The agent records the JVM aggregate and table-function operators after successful Arrow-batch
consumption, using the same consumed-row recorder as the other columnar operators.

The unchanged Table API `OverAggregateITCase` methods containing admitted scalar JVM
aggregates require consumed `NativeColumnarJvmOverOperator` input. Constants, shared
instances, managed DataViews, original source failures and retention settings remain
upstream's. Scalar-only SQL `testRowTimeUnBoundedPartitionedRowsOver` and
`testRowTimeUnBoundedPartitionedRangeOver` additionally require consumed JVM OVER work,
including both 2.2 unbounded implementations. Both backend invocations remain in the scopes.
Flink 1.18's observed changelog-state setting keeps its existing explicit full fallback.

The four Table API DISTINCT methods contain builtin aggregates and a direct FLOAT AVG;
they retain the precise existing OVER fallback contract. The Table API unbounded event-time
RANGE fixture also produces an ARRAY above its UDAFs. It retains the existing collection
boundary: `collection value representation requires a final projection or fused consumer`
on 1.18, regardless of changelog state, and `Calc: collection input representation is unverified
for this columnar producer` on 2.2. The scalar-only SQL witnesses qualify the same unbounded
frames without expanding those collection contracts. Their upstream queries are unchanged. Timer, ownership,
scalar-type, late-row and repeated checkpoint/rescaling tests supplement these witnesses;
plan labels alone cannot satisfy the execution contract.

The event-time fixtures retain upstream's host `TimeAssigner` before the SQL input boundary,
so their complete-job route is explicitly `mixed`, with consumed JVM OVER work mandatory.
The route classifier continues to expose that source helper. Their scope additionally uses
`forbidden_operators_on_native_routes` to reject a stock `KeyedProcessOperator` in admitted
runtime graphs, proving the temporary unopened OVER transformation never becomes an edge.
This optional graph check applies to declared native/mixed routes; explicit fallback variants
retain their released host operator. Missing graph evidence cannot satisfy the guard.

Focused MAP/MULTISET collection-aggregate fixtures additionally require released values,
resolved types, per-key ordered changes and consumed JVM group work, including two
nonempty checkpoint recoveries on heap/native RocksDB. They reuse the recursive
preserving container bridge. The unchanged scalar COLLECT fixture below remains its
own execution contract; it does not establish nested-element parity by itself. ARRAY,
ROW and STRUCTURED aggregate elements keep the stock route after rejected representation
prototypes; see [the nested-element gates](operators/group-by.md#nested-collection-elements).

The unchanged SQL `AggregateITCase.testUnboundedGroupByCollect` now has explicit contracts
and route scopes for all seven 2.2 and six 1.18 variants. Synchronous HEAP/ROCKSDB variants
require consumed input in the Rust group aggregate itself, not merely a Calc; the local runs observe 21
rows each. The local candidate run
matches two native/five fallback cases on 2.2 and two native/four fallback cases on 1.18.
Mini-batch/local-global and async-state variants retain exact fallback reasons; 1.18 additionally
follows the fixture's observed changelog-state setting. These runs preserve the original fixture
configuration and source recovery. The suite agent reads the pinned mini-batch mode's `on()`
boolean through a `miniBatch=true|false` selector; its display string contains an equals sign
and is not used as a selector value. No upstream test or setting is changed to force a route.

The unchanged streaming SQL `CalcITCase.testNonPrimitiveMapType` requires consumed NativeCalc
work on Flink 2.2. It constructs a MAP from runtime integer and STRING columns and retains
upstream's output assertions. Flink 1.18's legacy output context retains the existing
`collection value representation requires a final projection or fused consumer` fallback,
with an exact fallback contract and runtime-route scope. That boundary remains with
[#234](https://github.com/datafusion-contrib/StreamFusion/issues/234). Standard runtime-source
collection SQL tests on both lines additionally require empty JVM-evaluator descriptors for
the new STRING constructor kernels; a NativeCalc label alone does not establish Rust scalar execution.

The agent self-tests select their contract resource once per JVM. With JDK 17, run separate
Maven invocations from the repository root for each supported resource, including native
state on 1.18. Mock fixtures must supply the selectors used by both releases; the COLLECT
witness includes the 1.18 environment selector as well as 2.2 async-state selection.

```sh
mvn -f dev/flink-suite/agent/pom.xml test -Dstreamfusion.flink-suite.flink-line=2.2
mvn -f dev/flink-suite/agent/pom.xml test -Dstreamfusion.flink-suite.flink-line=1.18
mvn -f dev/flink-suite/agent/pom.xml test -Dstreamfusion.flink-suite.flink-line=1.18 \
  -Dstreamfusion.flink-suite.native-rocksdb=true
```

Focused SQL additionally covers container fanout, ownership, failures and repeated recovery;
the single upstream method does not establish every nested signature by itself.

## Runner and suite selection

StreamFusion can run Flink's own table planner runtime integration tests with native acceleration
installed in every streaming planner. This follows the purpose of DataFusion Comet's upstream Spark
SQL jobs while keeping Flink stricter: it uses an unmodified release tag and injects StreamFusion
only into the forked test JVM rather than copying or patching upstream tests.

Run the planner runtime suite from the repository root:

```bash
bin/flink-suite.sh
```

The same harness also runs Flink's unchanged format integration tests, the Kafka connector's
unchanged table SQL integration tests, Paimon's unchanged table SQL integration tests, and
Delta's unchanged portable SQL sink tests:

```bash
bin/flink-suite.sh formats
bin/flink-suite.sh parquet
bin/flink-suite.sh orc
bin/flink-suite.sh kafka
bin/flink-suite.sh paimon
bin/flink-suite.sh delta
bin/flink-suite.sh all
```

`formats` covers Flink's JSON (including Debezium and Ogg CDC), CSV, Avro, and Protobuf integration
tests and compiles the Confluent Avro module (the pinned release contains only unit tests in
that module). The Kafka repository's separate `SQLClientSchemaRegistryITCase` exercises Confluent
reads, writes and schema evolution using real Schema Registry, Kafka and Flink containers; it is
not yet selected by this runner. Protobuf's SQL integration fixture uses batch mode and remains
stock Flink. These suites do not yet require per-format native codec execution evidence.
`parquet` runs Flink's unchanged `ParquetFsStreamingSinkITCase` and
`ParquetTimestampITCase`, and fails unless the suite proves that a native Parquet writer was
created. `orc` runs `OrcFsStreamingSinkITCase` and `OrcFileSystemITCase` and requires a successful
columnar ORC writer marker (the writer now uses the host's Java ORC vectors). The harness runs timestamp tests with a UTC JVM. `kafka` covers `DynamicKafkaTableITCase`, `KafkaChangelogTableITCase`,
`KafkaTableITCase`, and `UpsertKafkaTableITCase` from the pinned Kafka connector release. The Kafka
suite starts broker containers and therefore requires a working Docker daemon. Its changelog
tests replay Debezium, Canal and Maxwell events through SQL; the format suite also replays Ogg
events. These test change-event handling, not capture from a live database. On Flink 2.2, `paimon` runs the
Paimon Flink connector's `AppendOnlyTableITCase`, `AppendTableITCase`, `BatchFileStoreITCase`,
`ComputedColumnAndWatermarkTableITCase`, `ContinuousFileStoreITCase`, `ReadWriteTableITCase`,
`PrimaryKeyFileStoreTableITCase`, `CompositePkAndMultiPartitionedTableITCase`,
`FullCompactionFileStoreITCase`, `FlinkJobRecoveryITCase`, `RescaleBucketITCase`,
`ScanBucketITCase`, `KeyOnlyDeletesITCase`, `FirstRowITCase`, and `CoordinatorCommitITCase` from the
pinned Paimon release,
built against the suite's Flink version, and fails unless it proves both that a streaming insert
wrote an append-table data file from a native Arrow bundle and that one wrote a primary-key
level-0 file natively, and that a native snapshot merger emitted an Arrow batch. The markers are
emitted only after the corresponding write or read returns successfully.
The agent selects the same complete-plan streaming hook as the deployed planner factory.
`ContinuousFileStoreITCase.testSourceReuseWithScanPushDown` passes unchanged: compatible projected
scans share one native source, while filtered and limited scans stay separate.
Streaming inserts covered by the [Paimon connector whitelist](connectors/paimon.md)
can take the native sink, including coordinated writers, coordinator commits, dynamic
partition routing, and automatic append-buffer spilling. The regular StreamFusion SQL parity
suite forces Arrow spilling and checkpoint failure/recovery; the unchanged upstream streaming
tests exercise the surrounding writer lifecycle. Batch inserts,
unsupported primary-key options, and compaction rewrites use stock Paimon. `CoordinatorCommitITCase`
checks removal of the global committer, coordinator metrics, committed rows, and snapshot watermark
parity for active and idle inputs. Because
Surefire appends StreamFusion's classpath in no fixed order, the agent also resolves Paimon's
`parquet` and `orc` format identifiers to the StreamFusion factories whenever the module is present, standing in
for the `01-streamfusion-paimon.jar` ordering a deployment relies on. Paimon's module declares the
planner test-jar before the planner itself, which would place stock Calcite ahead of Flink's patched
validator classes (breaking `CALL` procedures and time travel in stock tests), so the runner drops
the resolved calcite-core from that module's test classpath and appends it after the planner
instead. `all` runs formats, Parquet, ORC,
the planner runtime suite, Paimon, Delta, and Kafka in that order.

`delta` compiles the unchanged `FlinkSqlTest` and its `TestHelper` from Delta `v4.4.0`
against published `delta-flink_2.2:4.4.0` and Delta Kernel artifacts. It does not build or
publish Delta or Unity Catalog production code from source. The four SQL cases cover batch
grouped aggregation, streaming path-table writes, partitioned streaming writes, and a
many-types streaming write. The original committed-row and file-statistics assertions remain
intact. The two supported streaming loads additionally require positive native Parquet encoding
counts for each test invocation. The many-types case includes `TIME(0)` and requires full
fallback with that unsupported-type reason. Batch aggregation uses stock Flink. All four cases
must execute in a full run; a missing or skipped case fails the summary. This portable suite
does not include the separate `FlinkSqlIntTest`, which requires remote Databricks and Unity
Catalog credentials.

The runner clones Flink `release-2.2.1`, Kafka connector `v5.0.0`, and Paimon `2.1.0` (its
`release-2.1.0` tag, commit `3a5ffe7870697e2c2605e63413fb5ac5800aae7e`), plus Delta `v4.4.0` for its SQL tests, under `.flink-suite/2.2`, verifies that each checkout is clean, builds and
installs StreamFusion and its supported format/connector modules, and builds the required upstream
reactors with tests skipped. The suite runs on JDK 17 and selects Flink 2.2's released
`java11-target` build profile so its isolated dependencies meet StreamFusion's Java 11
bytecode gate; Flink 1.18 retains its upstream compiler target. This changes compiler
configuration, without rewriting upstream source or bypassing dependency validation. A test-only
Java agent then installs StreamFusion whenever an upstream test creates a streaming planner, and
loads the native library at that moment the way a TaskManager loads it once at startup, so no
upstream job pays the first-load latency inside its first native task; batch planners remain stock
Flink. The default run executes the planner module's unchanged `*ITCase`
runtime integration suite with one active test JVM, then summarizes Surefire failures. Flink 1.18
starts a fresh fork per class; the 2.2 planner reuses its fork, as configured upstream. Serial execution
keeps concurrently created MiniClusters from exhausting a developer machine or CI runner.

The experimental 1.18 runner selects Flink `release-1.18.1`, Kafka connector `v3.2.0-rc1`, and
Paimon `release-2.0.0` with its `flink1` profile. Paimon 2.1's coordinator operator is binary
incompatible with Flink 1.18, so this host keeps the working released 2.0 connector. The 1.18 Paimon suite runs its complete version-specific module plus the explicitly listed shared compatibility regressions described below. Shared fixtures compile against their declared 1.20.1 API; both test sets execute with released 1.18.1 dependencies and the matching StreamFusion payload. Kafka 3.2 has no Maven wrapper, so both its compile and test invocations use the pinned Flink wrapper (Maven 3.8.6) with the Kafka POM passed explicitly. This keeps released sources untouched and avoids the host's Maven version: Maven 3.10 rejects duplicate dependency-plugin declarations in the released Python module while constructing the reactor, before the selected Java tests can run. Kafka releases that provide their own wrapper continue to use it. Kafka's final candidate tag (`d12f73c8`) matches the
[official 3.2.0 source archive](https://archive.apache.org/dist/flink/flink-connector-kafka-3.2.0/);
that release has no `v3.2.0` tag. The Kafka suite compiles and runs directly from the
Java connector module POM, selecting its artifact ID for test execution. This keeps
the unchanged Java SQL tests and inherited parent configuration while avoiding
reactor validation of unrelated Python packaging. Kafka 3.2.0's Python POM declares
its dependency plugin twice; current CI Maven rejects that unrelated module even
when the root reactor selects only the Java connector. No upstream POM or test is
rewritten. Run `FLINK_VERSION=1.18.1 bin/flink-suite.sh config`
to inspect the selection, then replace `config` with the desired suite. Each line has separate
checkouts, Maven repository, StreamFusion source/build outputs, injection-agent JAR, classpath,
native-execution reports and diagnostics under `.flink-suite/<line>/`. `FLINK_SUITE_ROOT`
changes that parent directory without removing the per-line separation. Build reuse only reads
the selected line's artifacts. Before any suite starts, every StreamFusion classpath JAR must
identify its module and requested Flink line in its manifest; renaming or copying a payload from
the other line is rejected. Duplicate payloads and a missing core also fail this check.
Delta has no admitted 1.18 payload and is rejected before cloning.

Kafka 3.2 normally isolates the planner behind `flink-table-planner-loader`. Its 1.18 test
invocation instead puts the same unshaded planner used by the Flink suites in the test JVM,
beside the injected native planner. The runner excludes the isolated loader and orders stock
Calcite after Flink's patched classes. This changes only the harness classpath; the pinned
connector sources, SQL and result assertions remain unchanged.

On Flink 2.2, the suite agent extends Kafka's topic-creation fixture with a bounded
administrative readiness check. Partition offsets can be readable before the metadata
and producer-state requests used by the host exactly-once sink succeed. The check uses
the released connector's own administrative utility, retrying only unknown-topic and
leader-not-available responses with a 30-second retry budget and five-second API
timeouts. Authorization and other failures remain fatal. This changes fixture
synchronization only; sink execution, SQL, and result assertions stay upstream's,
and failed tests are not rerun.

The Flink 2.2 `CommonExecSinkITCase` bounded source fixture waits for its already scheduled
`NoMoreSplits` notification before emitting data or ending input. Its released split holder
otherwise emits immediately: a NOT NULL task failure can race that notification, and the
result reports a secondary lost-operator-event failure instead of the real sink error.
The test agent gates this exact fixture reader's availability and polling until its original
notification handler runs; closing a reader releases any pending availability. The original
reader still supplies every row, conversion, end signal and exception. Rows, SQL, production
sink enforcement and upstream assertions remain unchanged, and failed tests are not retried.
Focused bytecode tests cover split/empty readers, independent readers, closure, and propagation
of the original sink exception after readiness.

The 1.18 execution contract resource names methods verified in that release's unchanged source.
It retains scalar, aggregate, rank, distinct-window and lookup witnesses; it excludes the
retracting window TVF method absent from that release and the unavailable Delta suite. Agent and
report summarizer select the same resource. The upstream CI matrix includes both lines and its
required aggregate check requires every leg to succeed on source-changing pushes, manual runs
and schedules. [Documentation-only pushes](ci.md#documentation-only-pushes) can reuse exact-base
coverage only after both required push workflows pass with unchanged tested source; unproven
baselines still run every leg. Each executed leg archives its own result totals,
execution-contract counts and diagnostics; the two lines have different upstream corpora and
host-capability skips. Java, module, image and qualified-artifact jobs also exercise both lines as
blocking checks. Production support additionally requires the real-cluster upgrade and publication
work tracked in [#188](https://github.com/datafusion-contrib/StreamFusion/issues/188) and
[#189](https://github.com/datafusion-contrib/StreamFusion/issues/189).

Selected upstream SQL tests also have **per-invocation native execution contracts**, declared in
`dev/flink-suite/agent/src/main/resources/native-execution.tsv`. The unchanged `CalcITCase.testNotIn`
must execute a native filter or Calc, and `testLongProjectionList` must execute a native Calc;
`AggregateITCase.testGroupByAgg` must execute a native grouped aggregate;
and `WindowDistinctAggregateITCase.testTumbleWindow`, `testHopWindow`, and `testCumulateWindow` must
execute either a single-phase native window aggregate or **both** native local and global halves
when the fixture's `splitDistinct` parameter is false.
Each parameter variant must satisfy its own contract, including backend, mini-batch, async-state,
and distinct-splitting variants selected by the pinned tests. The `splitDistinct=true` variants
require nonempty work from **both** native local and global window aggregates, since exact SUM0
supports the split-count reductions. A native Calc or a single window stage is insufficient.
Flink 1.18's changelog-state variants retain the explicit backend fallback contract, independently
of distinct splitting. The separate SQL parity suite retains forced single-phase attached-window
fallback checks; these are not the plans used by the upstream split-distinct witnesses.
`CalcITCase.testIfFunction` requires a native Calc. A fixture parameter change that prevents selecting exactly
one contract fails the test. Native and expected-fallback counts are reported separately.
`WindowAggregateITCase.testRetractPreviousSlicingStateWithSlicingWindow` also requires fallback
with the restricted retracting-aggregate diagnostic (the query also uses COUNT DISTINCT) for every phase, backend, timestamp and
async-state variant. Its unchanged CDC input includes a delete whose final window has no prior
insert; the upstream negative-count expectation remains intact.
`RankITCase.testTopNWithGroupByAndRetract` requires nonempty native updates from both the
grouped aggregate and Top-N. Its variable-size counterpart, `testTopNWithVariableTopSize`,
requires the explicit nullable-bound fallback; its aggregated bound also lacks a partition-invariance proof. Top-N input is credited only
after its native push returns, including when an input coalescer delays that call.
`LookupJoinITCase.testJoinTemporalTable` requires a completed native synchronous lookup batch.
`AsyncLookupJoinITCase.testAsyncJoinTemporalTable` and `testAsyncJoinTemporalTableWithRetry`
require completed native async lookup batches across every executed backend, object-reuse,
output-order and cache variant. These counters are recorded after the host-delegating columnar
operator completes its batch; merely opening the operator earns no credit.
The 1.18 contracts also cover legacy upsert sinks after joins and Top-N: native heap/native
RocksDB variants, including automatically adapted stock RocksDB selections, must perform join/rank
work. Changelog-state variants must report their explicit backend fallback. The host still validates and consumes the original
proven sink keys.
Calc contracts also cover numeric-to-boolean predicates, IN and SEARCH predicates, quoted LIKE
patterns, and reuse of one RAND value across expressions. Each requires nonempty native Calc or
filter work while retaining the unchanged upstream result assertions.
Other upstream cases still check
result parity without a per-test acceleration contract; planner installation alone does not prove
that any particular query ran natively.

The agent binds each runtime operator to the test invocation in which it opens and counts nonempty
input rows only after a method that performs native evaluation or aggregation returns successfully.
Opening an operator, accepting an empty batch, or buffering input before a native update earns no
credit. Task retries stay within the invocation; late work from an operator belonging to a finished
invocation cannot satisfy a later one. Tests within a fork must remain serial. A missing required
operator fails the JUnit test while retaining the upstream result assertions. The summarizer also
matches invocation counts in JUnit XML to the evidence files, so a missing agent, a missing variant's
proof, stale evidence, and execution failures hidden behind an expected-failure annotation all fail
the suite. The runner clears the selected suite's evidence before every run. Evidence lives in
`.flink-suite/<line>/native-execution/<suite>/` and is uploaded with the upstream CI log.
Each full suite also requires every method contracted for that suite to execute, so removing or renaming
an upstream test cannot silently shrink this coverage. Focused selections require evidence only
for their selected methods.
The full `state` run requires at least one executed, non-skipped test in every selected stateful class and every contracted method in those classes. Native witnesses remain explicitly bounded to the methods in the contract resource; classes without contracts still retain their unchanged result assertions,
and `all` includes that native RocksDB run as well as the ordinary runtime suite.

The summary also writes `.flink-suite/<line>/diagnostics/<suite>/execution-audit.json`, uploaded with
those CI diagnostics. Schema version 1 retains every parsed Surefire case (including duplicates,
skips and failures), its report-relative location, and whether that method is contracted. The
summary reports the complete executed denominator, the contracted subset, and the executed
cases outside that scope. For example, 32 passing Calc cases with eight execution witnesses mean
eight contracted executions and 24 unclassified executions, not 32 accelerated tests.

The unchanged Table API UDF fixtures also have explicit contracts. Job-parameter lifecycle,
multiple rich functions, and the code-generation split case require native Calc/filter work.
Constructor-state, inline and non-static object projection fixtures currently require full host
fallback with `Calc: unsupported function/operator: AS`, because their retained alias wrappers
are not admitted. Both heap and RocksDB fixture variants are checked; passing host results are
not reported as native execution. These contracts measure native operator work, not JNI-call
frequency or throughput.
The complete Table API Calc class passes 56 of 57 reported cases, with one upstream skip;
12 executed cases have contracts (6 native, 6 explicit fallback), leaving 44 executed cases
unclassified. This is the audit denominator, not an overall acceleration percentage.

Validated evidence retains the fixture selector, per-operator native input counts, expected
contract and recorded fallback reasons. Routes distinguish native work, mixed native work plus
recorded fallback, full fallback, and unclassified evidence. Counts are explicitly evidence-record
counts when SQL inventory is disabled: parameterized XML cases and witness files then share
only method-level totals, so the artifact does not invent one-to-one matches. A satisfied
individual record cannot override stale/duplicate evidence or a failed overall summary.

When SQL inventory collection is enabled, raw invocation JSON now also contains `native_work`:
nonempty row counts from the existing instrumented operator callbacks, including invocations
without an explicit execution contract. Opening an operator or observing an empty batch earns
no work. Bindings are removed when the invocation finishes, so later callbacks from an already
bound operator cannot credit a subsequent test. This retains the suite's serial-invocation
requirement and existing instrumented-operator coverage; an empty map is not proof of fallback.

The inventory's `execution_contracts` entries link each written witness filename (`record_id`)
and its test/fixture selector to the inventory's exact invocation UUID. The legacy four-field
TSV format remains unchanged. When SQL inventory is enabled, the runner passes it and the
released Flink line to the summary, which joins witnesses to `execution-audit.json` by the
invocation marker and report/case index. Parameterized method suffixes are normalized for
contract lookup while each invocation retains its full identity. Every executed contracted
case needs exactly one matching test/fixture witness; duplicate, missing, stale or unmatched
links fail the summary. Witness row counts cannot exceed the enclosing invocation's observed
work. Skipped cases need no fabricated identity. The join publishes exact associations only
when all links validate, and retains uncontracted row observations without yet assigning them
a route.

The observer also records streaming-environment `executeAsync` jobs under the invocation
that entered submission. Repeated overload callbacks deduplicate by job ID, and a submission
returning after its originating invocation ended cannot attach to the next case. At invocation
completion the observer samples the job-result futures without blocking: `SUCCEEDED` means a
completed result, `SUBMITTED` means the future is still pending, `RESULT_FAILED` preserves an
exceptional/cancelled result request, and `UNAVAILABLE` records a client that cannot expose it.
A failed result request alone is not labelled a failed host job. After a result future completes,
the observer requests the client's job status asynchronously and samples that future at invocation
completion as `job_status`. Pending status requests do not delay the test; failed requests remain
`job_status_error` observations. This distinguishes an explicit `FAILED` job status from cancelled
jobs and result/status retrieval failures. The raw inventory and joined
audit retain these observations separately from JUnit outcomes and native work. Submission
snapshots attach the stream graph's job type, node names, parallelism, operator factories and
declared operator classes and input edges to that job ID. Reflection failures remain explicit observations.
These snapshots describe the submitted graph, not proof that each node processed records.
For Flink's generated operator factories, the observer reads the generated class name directly;
it does not request class loading, which would trigger source compilation during observation.

`translation_details` retains each translation's planner class, outcome, physical-plan indices
and returned root count alongside the original planner-name list. Pipeline creation matches the
actual transformation objects against those returned roots. The submitted graph records
`sql_translation_ids` and `sql_translation_complete`; completeness requires every pipeline input
to have one observed origin and every root of each selected translation to be included. Partial,
missing and ambiguous matches stay explicitly incomplete. These links are scoped to the current
invocation and are not inferred from which plan was recorded most recently. Full-fallback and
deliberately unmodified routes use these links.

Direct DataStream submission also records links at stream-graph generation. The observer follows
transformation inputs from terminal roots and requires every input path to reach an observed SQL
translation root. Sources outside those roots, ambiguous origins, partial multi-root translations,
cycles and observation errors keep the link incomplete. Successful links record
`sql_translation_link_kind=transformation_inputs`; this permits downstream DataStream wrappers
without assuming that unrelated branches came from SQL.

Native callbacks with a Flink job metric ID accumulate under that job, including callbacks
that arrive before submission returns its client. At invocation completion, matching jobs
receive their `native_work`; unknown job IDs remain in `unmatched_native_jobs`. Callbacks
without a job metric ID remain in `unattributed_native_work`. The invocation-level aggregate
retains all these observations for existing contracts, but only associated counters establish
work for a particular job. Job IDs from finished invocations cannot receive credit in later
invocations, including newly opened operators. The joined audit validates positive integer work
counts and requires the associated, unmatched and unattributed counters to sum exactly to the
invocation total. An unmatched job ID cannot also appear among submitted jobs. Invalid partitions
fail the join before it publishes any exact case associations.

The separate `runtime_route` field recognizes `skipped`, `native`, `mixed`, `full_fallback`,
`unmodified_plan`, `scan_only`,
`batch_host_only` and `host_failure`; unresolved evidence remains `unclassified`.
Batch host-only requires a nonempty set of successful job results, a submitted batch graph for
every job, resolved Flink operator classes or Flink's `CodeGenOperatorFactory` for every graph node,
and a complete partition with
no native work. Pending results, graph observation errors, unknown operator classes and older
inventories without counter partitions remain `unclassified`. A host-failure route requires
at least one exceptional execution result with an explicit `FAILED` job status or a matching
Flink `JobResult` application status of `FAILED`, resolved Flink operator classes in every submitted batch/streaming graph,
no native work, and all other jobs either successful or similarly confirmed failed. Cancelled,
suspended or pending jobs cannot establish this route. The agent observes `JobResult` before
its conversion to an execution result, preserving the authoritative outcome even after an archived
job's status lookup becomes unavailable. Results join by job ID, including those observed before
submission returns. Repeated outcomes are deduplicated; conflicting outcomes, a successful result
paired with a failed application status, or contradictory terminal job statuses stay unclassified.
Unmatched results remain in `unmatched_job_results` through inventory loading and the exact
JUnit join, and prevent invocation classification. The structured partition is kept in JSON;
CSV summaries omit it alongside other nested job evidence. Malformed
status lists and overlapping submitted/unmatched identities fail validation. Finished-invocation
job IDs cannot contribute results to a later invocation.

For streaming native routes, every job must succeed and every native operator type in its graph
must have positive, job-associated work. The initial explicit type list covers Calc, Filter,
synchronous/asynchronous lookup join, columnar local/global group aggregate, updating join, Top-N and global
window aggregate. Unsupported native types and counters for types absent from the graph remain
unclassified. Sources, sinks, row/Arrow transposes and columnar key-group routing are permitted
boundaries. The columnar mini-batch assigner is also a boundary: it forwards Arrow batches
unchanged and emits scheduling markers without per-row computation. Other resolved Flink operators
alongside native work produce `mixed`. These counts
establish work per operator type and job, not per individual graph node or subtask.

Generated `SourceConversion` operators are also boundaries when their input paths lead only to
known sources. Generated `SinkConversion`, Flink output conversion and sink constraint enforcers
qualify when their output paths lead only through those adapters to known terminal sinks. Generated
classes require Flink's code-generation factory identity. Classification checks each graph node's
position, so a converter with the same class name elsewhere remains host computation. Missing or
invalid edges and cycles cannot establish these additional boundaries.

`scan_only` requires a successful streaming graph containing both source and sink operators,
with every node in the explicit StreamSource/SourceOperator/StreamSink/CollectSinkOperator/
SinkWriterOperator/table SinkOperator list or a verified conversion boundary, and no native work. Generic maps,
misplaced converters and unknown classes do not establish scan-only execution. Multiple native/scan-only/mixed jobs roll up to
`mixed` when their routes differ. Pending or unclassified jobs and any unattributed/unmatched
native work keep the entire invocation unclassified. Batch/streaming mixtures remain unclassified.
These are execution observations, independent of JUnit success and existing contract verdicts;
they do not replace the published planning/admission labels.

`full_fallback` requires a successful host-only streaming job with a complete translation link.
Every linked translation must have succeeded, and every referenced physical-plan observation
must come from translation, contain only host roots and record nonempty fallback reasons.
Missing, duplicate or invalid translation identities, absent reasons, EXPLAIN-only plans and
native plan roots cannot establish this route. `runtime_fallback_reasons` retains the validated
reasons by job ID. Native and full-fallback jobs in the same invocation produce `mixed`; a
full-fallback job alongside a scan-only job remains full fallback.

`unmodified_plan` requires a successful host-only streaming job whose linked translations all
come from planners explicitly created with preservation enabled. The observer binds that choice
to each actual planner instance and copies it into `translation_details.planner_configuration`,
including planners created before an invocation starts. A preserved planner elsewhere in the
same test cannot classify an ordinary planner's job. Batch jobs retain their batch route; an
explicitly preserved streaming plan takes precedence over scan-only classification.

For direct summary calls, use `--sql-inventory <directory> --flink-line 2.2` (or `1.18`).
Add repeatable `--require-runtime-route-prefix <class#method-prefix>` options to declare the
scope that must have classified runtime evidence. Every matching parameterized case must be
classified; an empty prefix, a prefix matching no cases, missing inventory or an unclassified
case fails the summary and retains the failed artifact. The artifact records these prefixes
and `testcases_by_runtime_route` counts over the complete JUnit denominator, including skips
and cases outside the selected scope. Contract-scope counts remain separate.

Use `--runtime-route-scope <json>` to require exact fixture variants instead of entire method
prefixes. The versioned scope declares a Flink line and cases with canonical `test` method,
exact JUnit `display_name`, positive `count` and expected `route`. Missing or duplicate variants,
wrong routes, unclassified evidence and invalid/wrong-line scopes fail the summary. The audit
embeds every requirement and the matched invocation identities, while retaining all other cases
in its complete denominator.

Schema version 1 supports a fixed `route`. Version 2 also supports
`route_by_contract_variant`, an exact mapping from the invocation-linked execution witness's
`variant` to its required route. Missing or unmapped variants fail; this is not a set of interchangeable
acceptable routes. Flink 1.18's test environment randomly enables changelog state, so its keyed
fixtures require `native` for `changelog=false` and `full_fallback` for `changelog=true`. The same
exact witness is independently checked against the existing execution contract. No upstream test
configuration is changed to force a preferred route.

The bundled `runtime-route-scope-2.2.json` and `runtime-route-scope-1.18.json` require 199 and 172
verified runtime variants respectively. Run them with:

```sh
FLINK_VERSION=2.2.1 FLINK_SUITE_RUNTIME_AUDIT=true bin/flink-suite.sh runtime
FLINK_VERSION=1.18.1 FLINK_SUITE_RUNTIME_AUDIT=true bin/flink-suite.sh runtime
```

The option selects the scope's upstream methods (26 on 2.2 and 25 on 1.18), enables fresh SQL inventory collection/reporting,
and applies the appropriate scope automatically. Add `FLINK_SUITE_REUSE_BUILD=true` after a
compatible suite build. A custom `FLINK_SUITE_TEST` selection must still satisfy every required
variant; omitted variants fail. The audit option requires the runtime suite and cannot be combined
with runtime sharding. The commands retain complete reports, inventory and the execution audit
under the suite workspace. CI runs the audit before the first runtime shard on each released
line, reusing that worker's restored build. It uploads audit evidence before the full shard can
replace reports. Audit failure fails the worker and the final upstream gate; no extra runners or
shared-build downloads are added.
Each audit must match every listed requirement; missing, duplicate or unclassified required
variants fail. The execution audit retains the exact matched count and any unrequired early returns.
The batch `TableSinkITCase#testCollectSinkConfiguration` fixture establishes the host-failure
route while its expected exception remains a passing upstream test.

The keyed `AggregateITCase#testGroupByAgg` fixture covers HEAP/ROCKSDB, immediate/mini-batch
and local/global modes. Local and global native operators must each have positive associated
work; observing only the global half cannot establish a fully native split job. The seven Flink
2.2 variants run natively, including both async-state settings. The six Flink 1.18 variants follow
their recorded changelog-state setting. Split jobs record twelve local input rows and six global
partial rows; immediate jobs record twelve global input rows.

`DataStreamJavaITCase#testFromAndToChangelogStreamEventTime` verifies `mixed` on both releases
with object reuse enabled and disabled: four native Calc input rows coexist with Flink map,
watermark and window operators in the same successful graph. Native operator presence alone
therefore cannot label the whole job native.

Auditing remaining upstream methods and operator types remains part of #168; the published
inventory remains a planning/admission report and no full-suite native percentage is claimed. Agent and Python tests verify the
collection/linkage paths with synthetic observations, not additional upstream SQL coverage.

An unchanged Flink 2.2.1 `batch.sql.CalcITCase#testSelectStar` run verifies the first runtime
scope: one passed invocation, one successful batch job with `FINISHED` status, one complete
translation-to-pipeline link, zero native work and a `batch_host_only` route. The explicit
`--require-runtime-route-prefix org.apache.flink.table.planner.runtime.batch.sql.CalcITCase#testSelectStar`
summary check passes. Its source-conversion and Calc nodes use generated names without a package
prefix; their Flink factory establishes host origin. This is one batch execution check, not
native SQL coverage or a claim that the entire batch suite has been audited.
The same unchanged test and required scope also pass on Flink 1.18.1: one successful batch
invocation and complete translation link. Its separate job-status request was unavailable;
that error remains visible and does not replace the completed execution result.

The unchanged Flink 2.2.1 `stream.sql.CalcITCase#testLongProjectionList` also passes its explicit
runtime audit scope: one invocation, one successful job and three native Calc input rows matched
to both its job ID and existing exact contract witness, with no unassociated work. The submitted
graph retains both Arrow transposes. The runtime classifier reports `native` after verifying
the source-conversion and sink-adaptation boundaries by their graph edges; the existing native
contract remains satisfied. This fixture submits its converted
DataStream directly, bypassing SQL executor pipeline creation. A rerun verifies a complete link
to translation 0 through `transformation_inputs`, while preserving the same three job-associated
native rows.
The same streaming contract and required scope pass on Flink 1.18.1 with one invocation, three
job-associated native rows, verified conversion boundaries and a complete direct-submission translation link.

The unchanged Flink 2.2.1 `stream.table.CalcITCase#testInlineScalarFunction` verifies full
fallback for both HEAP and ROCKSDB variants: two passed invocations and two completed host jobs,
each linked to `Calc: unsupported function/operator: AS`, with zero native work. Both the existing
fallback contracts and the required runtime route prefix pass.
The same two variants and required route prefix pass on Flink 1.18.1 with the same reason.
Those two invocations have no legacy method contract on 1.18; their runtime classification comes
from completed job and linked translation evidence. Across these three selected methods the
runtime denominator is four passed invocations per released line, not the full upstream suite.

The unchanged Flink 2.2.1 `TableEnvironmentITCase#testFromToDataStreamAndExecuteSql` passes all
three variants. `StreamTableEnvironment:isStream=true` runs three successful host jobs with
complete links to preserved planners and classifies as `unmodified_plan`. The two
`TableEnvironment` variants return before job execution and remain unclassified. The complete
JUnit denominator is retained; passing those early-return variants does not establish SQL
execution, and requiring the entire method prefix would correctly fail on their missing routes.
Flink 1.18.1 has the same three passed variants and classifications; its streaming variant runs
two successful jobs with preserved-planner links.

The unchanged `stream.sql.CalcITCase#testSelectStarFromNestedTable` passes on both released
lines with one successful source/conversion/table-sink job, no native work and a `scan_only`
route. It verifies that nested rows at host boundaries do not earn native execution credit.
Across all five selected methods, each line has eight passed JUnit invocations: six classified
runtime routes and two explicitly unclassified early returns. No full-suite percentage is inferred
from this selection.

Outside the declared contracts, cases without the runtime evidence above remain
unclassified. The artifact does not infer routes from test class names or JUnit outcomes.
Broader verified invocation scopes and boundary classification remain
[#168](https://github.com/datafusion-contrib/StreamFusion/issues/168). Agent unit-test output is
outside the suite's report/evidence directories and contributes no SQL cases. The summary writes
failed artifacts for missing or malformed reports/evidence and retains process failures; an
earlier build or installation failure can stop the runner before the summary is reached.

These checks prove native data-path execution, not a speedup. Release benchmarks measure performance
separately. The ordinary Java job also tests the evidence collector and summarizer, including
missing/empty work, wrong operators, incomplete two-phase routes, and cross-invocation isolation.

Flink's published planner artifact relocates its internal Calcite classes, while its source tests use
the unshaded classes. The runner therefore keeps an isolated Maven repository and compiles
an isolated copy of the StreamFusion source tree against the checkout's untouched parser, Calcite
bridge, and planner output. Those unshaded artifacts are installed with the dependencies their
shaded form bundles declared as ordinary dependencies, the same way Flink's IntelliJ profile exposes
them, so every upstream test module resolves Flink's patched Calcite classes through its own planner
dependency, ahead of stock Calcite. Surefire does not preserve the order of the StreamFusion classpath
it appends, so nothing may depend on that order for class resolution. Suite-only artifacts remain
under `.flink-suite`; production build outputs and the developer's normal Maven repository are not
replaced. Test JVMs load the engine and optional native libraries from the isolated source build's
`native/target/debug` directory through `java.library.path`, as required by development mode.

Flink's plan unit tests assert stock physical operator names, so an accelerator necessarily changes
their golden output. Run `bin/flink-suite.sh diagnostic` to include those tests when inspecting plan
coverage; their `Calc` versus `NativeCalc`-style diffs are diagnostic output, not result-parity bugs.

The checkout is cached between runs. Set `FLINK_SUITE_ROOT` to put it elsewhere, or tune local test
parallelism with `FLINK_SUITE_UNIT_FORKS` and `FLINK_SUITE_IT_FORKS`. The runner uses only public
artifact repositories, independent of developer-specific Maven mirrors. `FLINK_VERSION` is pinned by
the harness and should only be changed after validating the injection point against that release.

After a successful build, skip the StreamFusion and Flink rebuild while iterating on test selection:

```bash
FLINK_SUITE_REUSE_BUILD=true FLINK_SUITE_TEST='org.apache.flink.table.planner.runtime.stream.sql.CalcITCase' bin/flink-suite.sh runtime
```

JSON compiled-plan tests and the one Table API test that asserts Flink's exact operator names still
run, but their planners intentionally remain stock Flink: an accelerator's additional exec-node types
and operator names are outside those tests' contract. All other streaming planners receive
StreamFusion. The runner also reports Flink's independently reproducible batch `CURRENT_DATE`
timezone failure as an expected upstream failure instead of attributing it to StreamFusion.

Every mode propagates a failed Maven process even when the available XML reports pass. Runtime
and diagnostic modes permit one narrow exception: a completed Maven session must report only
Surefire 3.2.2 assertion failures, and every failed XML case must be explicitly allowed (currently
only batch `CalcITCase#testCurrentDate`). Errors in that method are not allowed failures. The
Maven event listener records the completed session result in `diagnostics/<mode>/maven-result.tsv`;
it does not change Maven or JUnit outcomes. Missing or inconsistent session evidence, fork crashes,
timeouts, incomplete or malformed XML, and unexpected test failures all fail the runner. Passing
partial reports cannot establish process success. Native execution contracts remain independently
required, including when an expected assertion failed.

The harness checks include real Maven subprocesses with an allowed assertion followed by a fork
crash or timeout. Run them with `mvn -f dev/flink-suite/agent/pom.xml package` followed by
`python3 -m unittest discover -s dev/flink-suite -p 'test_*.py'`. The workflow command checks
also require Bash and `jq`, both provided on the CI runners.

During development, select one or more Surefire test classes without changing the upstream checkout:

```bash
FLINK_SUITE_TEST='org.apache.flink.table.planner.runtime.stream.sql.CalcITCase,org.apache.flink.table.planner.runtime.stream.table.CalcITCase' bin/flink-suite.sh
```

The same `FLINK_SUITE_TEST` and `FLINK_SUITE_REUSE_BUILD=true` controls apply to `formats`,
`parquet`, `orc`, `kafka`, `paimon`, and `delta`. Reuse mode requires that the selected mode has been built once normally.

The focused Paimon coordinator run includes its four paged writer-restoration cases, three
commit-coordinator cases, a deterministic primary-key write to verify native file creation,
and continuous-read cases to exercise native snapshot merging:

```bash
FLINK_SUITE_TEST='org.apache.paimon.flink.CoordinatorCommitITCase,org.apache.paimon.flink.BatchFileStoreITCase#testWriteRestoreCoordinator*,org.apache.paimon.flink.ReadWriteTableITCase#testStreamingReadWriteWithPartitionedRecordsWithPk,org.apache.paimon.flink.ContinuousFileStoreITCase' \
  bin/flink-suite.sh paimon
```

The focused streaming dynamic-partition run uses Paimon's unchanged skewed-input SQL test,
alongside a primary-key write and continuous-read cases to satisfy all three native write/read checks:

```bash
FLINK_SUITE_TEST='org.apache.paimon.flink.AppendTableITCase#testPartitionDynamicStreaming,org.apache.paimon.flink.ReadWriteTableITCase#testStreamingReadWriteWithPartitionedRecordsWithPk,org.apache.paimon.flink.ContinuousFileStoreITCase' \
  bin/flink-suite.sh paimon
```

The Flink checkout remains byte-for-byte unchanged. Every canonical push to `main` and every branch push in a personal fork
runs all eight upstream suites in GitHub Actions: planner runtime, formats, Parquet, ORC, Kafka,
Paimon, Delta and state/recovery. The weekly schedule and ordinary manual dispatch run the same complete matrix.
PRs use the exact-head fork push result, mirrored upstream by the lightweight reporter;
see [fork setup and migration](ci.md#contributing-from-a-personal-fork).
Each run rebuilds StreamFusion once per Flink line from that revision in the isolated suite directory
and shares the prepared build with its test jobs. Each job uploads its complete test log with the
commit SHA. These checks complement the released-artifact
SQL parity tests in ordinary CI; a passing local Maven suite alone does not establish upstream
integration compatibility.

Merges to `main` require **All CI tests** and **All upstream integration tests**, enforced by
the repository's **Require all test suites** ruleset with the configured maintainer bypass. The fork reporter supplies these same required checks
without rerunning the test matrix upstream.
The first check waits for Rust, Java/SQL parity, every format/connector module, both Paimon formats,
Delta and both deployed Flink image integration jobs. The Flink 1.18 image job also packages the
qualified release artifacts and runs the loader/mixed-line identity tests, reusing its optimized
build rather than compiling the same payload in a separate job. The second waits for both shared builds,
every selected upstream suite, the complete runtime-shard coverage checks, and the legacy Delta host audit.
Each check runs even when a dependency fails and succeeds only when every dependency succeeds;
failed, cancelled or unexpectedly skipped jobs cannot produce a green aggregate check. Matrix
additions are included automatically; new independent test jobs must be added to the corresponding
aggregate's `needs` list. These two check names are part of the merge contract and must stay aligned
with the GitHub ruleset. The existing PR and other branch-protection rules remain in place.

The Java job owns the complete runtime suite, including its separate ORC classpath. Delta and
Paimon jobs compile the runtime and its shared test fixtures but pass `-Dsf.runtime.tests.skip=true`
to avoid repeating that suite in each lake connector job. Each lake job still runs its complete
connector suite; **All CI tests** requires both the Java job and every lake job. Ordinary `mvn test`
continues to run the runtime tests, and the standard `-DskipTests` still skips all test execution.
Pushing a newer commit to a fork branch or canonical `main` cancels its superseded run so the current revision can start. Merge requirements always apply to the
pull request's current revision.

Before committing operator changes, run the relevant unchanged upstream integration classes
alongside the local SQL parity and recovery tests. Record the class selection and actual result
in the commit. A selected run is not the full upstream suite, and pending CI is not a passing result.
`FLINK_SUITE_REUSE_BUILD=true` reuses the existing StreamFusion binaries as well as Flink's;
omit it after source changes so the upstream tests execute the current implementation.

The validated Flink 2.2.1 baseline is 8,619 tests: 8,570 passed, 48 skipped by Flink, zero unexpected
failures or errors, and the one independently reproduced `CURRENT_DATE` xfail described above.
The September 19, 2026 Flink 1.18.1 runtime baseline is 5,686 cases: 5,661 passed, 25 upstream
skips and no failures or errors. Its 65 execution contracts passed, with 31 native and 34
expected-fallback invocations. The newer [invocation inventory](sql-inventory/index.md) classifies
the full runtime corpus using observed planner admission; only those execution contracts assert
native work or expected fallback explicitly. The full 1.18 state run
has 1,120 passed and 16 upstream skips, with 18 native and 12 expected-fallback witnesses.
The format baseline is 185 tests: 175 passed and 10 skipped by Flink. The Kafka SQL baseline is 86
tests, all passed. The Parquet sink baseline is 8 tests, all passed, including the suite's explicit
proof that Flink instantiated the native Parquet writer. The complete Paimon baseline is 265
tests, all passed with native source sharing, including native append-write, primary-key-write,
and snapshot-merge markers. The complete-plan hook also passed 816 targeted Flink join, Calc and
JSON function cases.
The Kafka 3.2 / Flink 1.18 baseline is 72 cases, all passed with no skips. That release contains
the changelog, table and upsert classes; `DynamicKafkaTableITCase` belongs to the newer Kafka
suite. Kafka invocations do not yet have individual native-route contracts, so their passing
total is not a native-coverage percentage.
The portable Delta SQL baseline is four tests, all passed: native write evidence covers 5,000
unpartitioned rows and 1,000 partitioned rows, with one explicit `TIME(0)` fallback contract.
The ORC Java-writer validation on September 14, 2026 passed all 46 unchanged Flink ORC SQL tests.
A targeted upstream Paimon run passed 22 continuous-read, partition-write and schema-change cases;
the [ORC page](connectors/orc.md#build-and-verification) distinguishes that run from local tests
that explicitly exercise ORC streaming.

The agent logs each unchanged Paimon SQL `ITCase` test invocation and its completion, including
the full exception on failure. It includes randomized table defaults when the fixture supplies
them, including inherited defaults; other fixtures report `none`. Fatal MiniCluster errors
are printed immediately, even when upstream logging is disabled. If an invocation runs for two
minutes, it emits all JVM thread stacks to the suite log before CI's job timeout can discard the
active test's unwritten JUnit report.
Paimon also writes rolling cluster logs under `.flink-suite/<line>/diagnostics/paimon`; CI retains these
and Surefire reports alongside the console log. Tests without an upstream timeout have a ten-minute
JUnit timeout, and Surefire fails any Paimon class whose JVM exceeds thirty minutes. Existing upstream
timeouts and result assertions remain in force. These limits report failure; they do not retry or
turn a failed invocation into a skip.
For local diagnosis, `-Dstreamfusion.flink-suite.diagnostic-delay-seconds=<seconds>` changes only
when the one-time stack dump is emitted; the default is 120 seconds.

The full Paimon run executes `testStandAloneLookupJobRandom` and
`testStandAloneFullCompactJobRandom` in separate JVMs after the other tests.
Paimon 2.0.0's stock `StoreCompactOperator.close()` dereferences its writer even when cancellation
interrupted initialization before the writer existed. This was reproduced without StreamFusion
by closing an uninitialized stock compactor in Flink's operator harness. These randomized SQL tests
can pass their row assertions and hit that cleanup race while cancelling their conflicting compaction
jobs, killing the class's shared TaskManager and stranding subsequent tests. A separate JVM contains
that upstream fixture failure without changing the test, its random options, or its assertions.
The three streaming failure-injection cases, `testNoChangelogProducerStreamingRandom`,
`testFullCompactionChangelogProducerStreamingRandom` and
`testLookupChangelogProducerStreamingRandom`, also run separately. In
[the #273 Paimon run](https://github.com/datafusion-contrib/StreamFusion/actions/runs/36411039663/job/108907928301),
an injected `FailingFileIO.ArtificialException` escaped stock `MergeTreeWriter.close()`
through `TableWriteOperator.close()`. Flink treated the cleanup failure as fatal and stopped
TaskManager #0. Later jobs had no resources; the class JVM timed out roughly 28 minutes after
the initial failure. The final report listed 237 passed tests but had no completed report for
the timed-out class. Isolation preserves the original failed test and its diagnostics while
preventing that lost TaskManager from blocking unrelated tests. It does not fix or suppress
the upstream cleanup failure, disable failure injection, or retry random outcomes.
The same stock changelog-compaction exception boundary reappeared on Flink 2.2/Paimon 2.1
in PR #624; [#629](https://github.com/datafusion-contrib/StreamFusion/issues/629) tracks the
remaining investigation. A passing randomized rerun does not establish that this race is fixed.
All invocations' reports contribute to the result and native execution checks; any Maven
failure remains blocking, including a process timeout without a finished JUnit report.
Explicit `FLINK_SUITE_TEST` selectors keep their requested grouping for diagnosis.

## Paimon compaction fixtures

The modern maintenance corpus includes **152 unchanged compaction invocations** from the canonical released
Paimon **2.1.0** test JARs on Flink **2.2.1**. This is a separate maintenance corpus, covering core
selection/rewriter contracts, writer state recovery, dedicated workers, actions, refresh and SQL
procedures. The [case manifest](https://github.com/datafusion-contrib/StreamFusion/blob/main/dev/flink-suite/paimon-compaction-cases.csv)
pins every method and parameter invocation and states its expected native, stock-policy or
selection/upgrade behavior. No upstream test body, assertion, option or scheduling decision is changed.

```bash
python3 bin/paimon-compaction-suite.py
# With matching libraries already built:
python3 bin/paimon-compaction-suite.py --skip-native-build
# Use release libraries for a local diagnostic:
python3 bin/paimon-compaction-suite.py --release --skip-native-build
```

The runner verifies these SHA-256 hashes before executing the published fixtures. They correspond
to canonical `release-2.1.0`, commit `3a5ffe7870697e2c2605e63413fb5ac5800aae7e`:

| Test JAR | SHA-256 |
| --- | --- |
| `paimon-core-2.1.0-tests.jar` | `3f9ef9505f84c837bd015fb6a494d0309bca1f35716d66e48f360b8e82b1e076` |
| `paimon-flink-common-2.1.0-tests.jar` | `b7e83358fa1df5ce7c7a215f979fa91ecf8b904fd12a70689bb5d503babfe3b1` |

Both stock and native runs must report the exact 152 invocations with zero failures, errors or
skips. Reports under `streamfusion-paimon/target/upstream-compaction/{stock,native}` retain JUnit
XML, per-invocation JSON and a result CSV. Native records include selected input files/bytes,
output rows, output level, delete retention and explicit fallback reasons. Required payload cases
must complete real native groups with positive input files and bytes; selection-only and metadata
upgrade cases do not count as native coverage. The unchanged randomized restore fixture may
interrupt an attempt; successful selected rewrites must remain native, and other failures fail
validation. Interrupted primary-key rewrite/upgrade entry reports `InterruptedIOException`
before preflight, including when cancellation has already closed its owner; ordinary
closed-owner misuse still fails. Late callbacks retain their originating invocation.

The expanded 2.1 local qualification passes all 152 assertions in both modes with zero skips
and 242 native rewrite groups. Append action invocations now require native work rather than
retaining their former PK-only selection expectation. The append schema-change invocation
requires both native current-schema rewriting and the explicit `schema-evolution` fallback;
either missing path or a different fallback reason fails its contract. The 1.0 corpus passes
all 72 applicable invocations in each mode with zero skips and 384 native groups. The combined
[448-invocation result inventory](benchmarks/paimon-merge-compaction-20261105/upstream-results.csv)
retains each case, expected route, native/fallback counts and input formats. The shared focused
regression inventory has 178 parameter invocations across the three released hosts, including
positive first-row provider installation before unstarted lookup work.

The test agent installs the production decorator through the public core writer factory and
invokes the production graph adapter before a dedicated graph executes. Test-only reflection
constructs a resolved table view from the pinned core fixture's existing FileIO, schema and
catalog environment. Fixtures that install their own factory retain it, preserving their exact
stock-delegate and ownership assertions. Unadapted `CompactProcedureITCase` calls remain stock
and bypass both adapters. The stock mode resolves canonical format factories and retains Java
planning; benchmark `previous` mode retains the prior StreamFusion format shim and stock compaction.
These modes exist only in the test agent, with no production performance switch.

The append additions cover the released append coordinator/task, table compaction, fixed-bucket
manager/write and dedicated single-table worker fixtures. The upstream-disabled
`BucketedAppendCompactManagerTest.testPick` is recorded explicitly in the release metadata;
its enabled sibling methods remain in the corpus. Release metadata pins fixture hashes and host
profiles. The additional engine invocations retain upstream forced partial-update and aggregate compactions,
unsupported numeric-type delegation and first-row history/upgrade behavior. Paimon 1.0/Flink 1.18 runs **72 applicable unchanged invocations** with
`--paimon-version 1.0.0`, including its renamed aggregate fixture and the matching released
core/worker/action tests. A broader unchanged stock probe attempted 79 maintenance invocations:
69 passed and 10 could not run on this host. Seven common SQL procedure cases require named
arguments unsupported by Flink 1.18, and three common append-worker cases call a newer host
constructor absent on 1.18. Each is recorded individually, with its stock failure reason, in the
release metadata; these are not counted as passing or skipped acceptance cases. The three
engine integration cases bring the applicable inventory to 72. Shared host-specific worker
harnesses still require native append work on 1.0, including shutdown/refresh coverage. The shared `PaimonCompactionMergeEngineTest` additionally verifies first-row, basic
partial-update and bounded aggregation physical output against the stock rewriter on both
modern and legacy releases; it supplements the unchanged upstream fixtures.

The Flink 2.2/JDK 17 Paimon Parquet CI job runs both modes and retains the reports. This validation
is independent of the general upstream SQL corpus and of release performance measurements. CI
resolves the maintenance profile and agent dependencies with bounded download retries before
running either mode; test failures are never retried by that preparation step.

## Shared builds and runtime shards

CI prepares one common build for each pinned Flink line. It builds the untouched Flink planner,
StreamFusion's native workspace and Java payloads, and the upstream format test classes. The 2.2
build also includes the Delta payload. Kafka and Paimon compile their own test fixtures in their
consumer jobs, reusing the common build. The isolated Maven repository at `.flink-suite/<line>/m2`
is cached separately by platform, JDK, Flink version and build inputs; `setup-java`'s ordinary
Maven cache serves the injection-agent build. Rust dependencies are cached in the isolated source
build's actual native target directory. Every run still rebuilds current StreamFusion code.
The Rust cache keeps the existing `upstream-suite` key. Each Flink line runs its own preparation
and consumer jobs in a reusable workflow: 1.18 consumers need only the 1.18 preparation, and 2.2
consumers need only the 2.2 preparation. Suite selection happens inside preparation, removing a
separate runner allocation. The parent gate still requires both lines to succeed.

The Flink wrapper installs its pinned Maven distribution under
`.flink-suite/<line>/maven-wrapper`, which travels in the shared build with executable bits preserved.
Packing and restoring reject a missing or nonexecutable distribution. Consumer jobs use this
prepared wrapper home instead of downloading Maven independently on every runner. Preparation
caches that home alongside the isolated Maven repository so warm builds also reuse the distribution.
PR #310's
post-merge Flink 2.2 runtime shard failed during that redundant download, before Maven or any
tests started; its PR checks had succeeded. Test failures remain blocking and are never retried
or replaced by cached results.

Preparation verifies the compressed shared archive with `zstd --test` and publishes a SHA-256
sidecar. Consumers use `gh run download` with the current workflow run ID and exact artifact
name, checking that digest before accepting the transfer. A failed, missing or truncated
download gets at most three attempts, with 5- and 15-second waits and a thirty-minute limit per
attempt. The archive can exceed 2.5 GiB; an earlier ten-minute transfer budget repeatedly
expired on a hosted runner before tests started. Every attempt uses a fresh temporary directory, including `gh`'s temporary ZIP
storage, so a killed download cannot leak large partial archives between attempts. A partial
transfer never becomes an accepted build. Restore checks the digest again, extracts into a temporary directory,
validates build identity and relocated classpath inputs, then publishes the suite directory.
Extraction or identity errors fail the job without leaving a partially restored suite.
Only downloads retry; compilation, restore validation and tests retain their failure behavior.

The preparation job also caches the compiled, clean Flink checkout using an exact key containing
the released Flink version, actual JDK version, runner platform/architecture, and build script/settings/workflow
hash. Maven still runs the same build commands to validate and install its outputs; this is incremental
compilation reuse, not a cached test result or a skipped StreamFusion build. A cache miss takes the
normal cold-build path. Only main-branch preparation jobs save the compiled Flink and isolated Maven
caches, before any upstream tests run, and shared artifacts continue to exclude test reports.
Rust caches in ordinary CI, upstream preparation, and Paimon 1.0 compatibility are saved only on each
repository's `main`. Fork feature branches restore available caches but do not save their own copies
of these large caches; sibling branches cannot reuse those copies. Syncing and pushing `main` in a
fork seeds that fork's default-branch caches. Forks do not share the canonical repository's caches. The ordinary Maven dependency cache
managed by `setup-java` retains its existing policy.

The optimized image jobs cache `native/target/release-staging`, the actual Cargo target directory
used by the release builder. Debug dependencies retain their separate caches. The first run after
a cache configuration change may be cold; this policy does not increase the repository cache quota
or guarantee retention when main-branch caches alone exceed that quota.

`bin/ci-free-disk.py` checks free space on the workspace filesystem and removes unused preinstalled
toolchains one at a time, stopping when enough space is available. Build jobs target 30 GiB free;
upstream consumers target 20 GiB before downloading the shared build. The script fails early if
those targets cannot be met and refuses to delete anything outside GitHub-hosted Linux runners.
These are initial capacity targets to validate on hosted runs, not measured peak usage guarantees.

Before compiling Flink, preparation resolves the selected reactor's parent POMs and build
extensions with `bin/ci-maven-dependencies.py --model-only`. The fixed Help `evaluate` goal
loads models without executing a lifecycle or requiring unbuilt reactor artifacts. It uses
the line's pinned wrapper, profile and isolated repository. Downloads have bounded setup
retries and a 60-second HTTP read inactivity timeout for both Resolver and Wagon; the original
build and unchanged upstream test commands still run once. See [dependency setup](ci.md#maven-dependencies).
The shared repository also contains the selected release's `paimon-vfs-hadoop` transitive
provider graph, so Paimon connector compilation does not first fetch its Hadoop client graph
after the dependency-preparation stage. Provider downloads use the same bounded setup retries.

This follows [Flink's own compile-and-fan-out CI pattern](https://github.com/apache/flink/blob/master/.github/workflows/template.flink-ci.yml):
compile once, transfer build outputs, then run module groups on independent runners. Flink also
caches Maven dependencies, compiles with Maven reactor parallelism, and trims its transferred build
artifact. CI likewise uses `FLINK_SUITE_BUILD_THREADS=1C` for the two upstream reactor compilation
commands, allowing one Maven build thread per CPU. Local commands default to one thread. Test forks
and native-execution checks follow the settings below; Flink's workflow is a design reference, not an additional
dependency. The source-suite payload build skips duplicate Javadoc generation; the ordinary CI
Java/module jobs still run the bound Javadoc check.

CI debug builds use Cargo's `line-tables-only` debug information for both development and test
profiles. This retains native filename/line-number backtraces without generating type and local
variable debug information for every dependency. Debug assertions, overflow checks, optimization
levels, package boundaries, and test selectors remain unchanged. Local development and release
build profiles retain their defaults. The environment settings participate in the Rust cache key;
the first run after this change rebuilds dependencies, and main-branch runs seed the smaller caches.
Java and optional lake jobs share one `debug-jvm` dependency cache instead of storing nearly
identical multi-gigabyte caches. The main-branch Flink 2.2 Paimon ORC job seeds it after its runtime,
core and lake native builds; other Java/lake jobs only restore it. Cargo still validates each
package's features and fingerprints and rebuilds any missing variants.

A local cold `streamfusion-bridge` test build with four Cargo jobs, incremental compilation disabled,
and the same 24 passing tests took 37.0 seconds with full debug information and 28.9 seconds with
line tables (22% less elapsed time). Its target directory fell from 746 MB to 473 MB (37% smaller).
These are one-pass build/test measurements on the same machine with warm dependency downloads,
not an engine throughput benchmark or a hosted full-workspace speedup estimate.

Ordinary module CI groups the same tags into Kafka, row formats (JSON, CSV, RAW, Avro,
Avro-Confluent, Protobuf), and columnar formats (ORC, Parquet), each on both Flink versions.
Every distinct native package still receives an independent `cargo check -p ... --all-targets`;
checks continue after a failure and fail the job if any package failed. Each group then runs Maven
once with the union of its tags. A test carrying multiple tags within a group executes once rather
than being repeated in separate jobs. The CI grouping test compares selected tags against the Java
sources so an added module tag cannot silently lose coverage.

The Flink 2.2 Paimon CI consumer uses two Surefire JVM forks for its main class suite instead of
one. Paimon's released build already exposes `flink.forkCount` and configures 2 GiB heaps per fork.
Tests within each class remain serial, while independent classes can execute in separate JVMs.
The five isolated failure/cancellation methods still run in their separate sequential Maven
invocations. Flink 1.18 Paimon, runtime shards, native-execution contracts, and local default fork
counts retain their existing settings. Hosted validation must confirm the two-fork setting does
not introduce resource failures before treating its expected elapsed-time reduction as measured.

A compressed artifact transfers the clean Flink checkout, compiled classes, Maven artifacts,
injection agent and native libraries. It excludes Cargo intermediates and previous test reports
or execution evidence. Consumers require the same revision, Flink version and platform, relocate
the generated classpath to their checkout, and validate the payload manifests before testing.
The artifact is shared within that workflow run; successful test results are never reused.

Preparation also resolves Surefire's dynamically loaded JUnit Platform provider and the
launcher matching the pinned host: Surefire 3.2.2 / Platform 1.11.4 on Flink 2.2,
and Surefire 3.0.0-M5 / Platform 1.9.1 on Flink 1.18. A skipped-test build resolves the plugin without loading
these runtime dependencies. The agent's own tests use a separate Maven repository and plugin
version, so their success cannot prove that the transferred suite repository is complete.
The artifact mode of `bin/ci-maven-dependencies.py` resolves the two runtime artifacts
with their complete transitive dependencies, using bounded download retries to populate
the isolated `m2` directory before packing. It avoids resolving unrelated build plugins.
Each artifact resolves separately in `dev/flink-suite/runtime-dependencies/pom.xml`, which
has no parent, dependencies or dependency management. This keeps provider resolution
independent of the host model and its JUnit BOM, including the original
`opentest4j:1.2.0` dependency needed during Surefire 3.2.2 startup. Both Maven settings
files use the suite's public-repository configuration. Those retries never execute or retry
a test lifecycle. Preparation then runs the
unchanged stock `CalcITCase#testSimpleProject` once with Maven's offline flag, requiring a
real test invocation and a successful result before publishing the shared build. This smoke
check uses no injection agent, contributes no native-coverage evidence, and its reports are
excluded from the shared artifact. Update these pins when upgrading the corresponding
upstream Surefire or JUnit version.

Preparation also resolves Kafka 5.0's separate Surefire 3.2.5 provider and Platform
1.13.3 launcher on the Flink 2.2 line. Compiling its tests does not load that launcher,
and Flink's Platform 1.11.4 runtime does not cover it. A connector job failed before
any Kafka tests ran when Maven Central returned HTTP 502 for the 1.13.3 launcher
descriptor. Preparing both original dependency graphs with the same bounded setup
retries keeps this download out of test startup without changing Kafka's dependency
versions or retrying its tests. The isolated loader regression also covers this
Surefire/JUnit combination with Jupiter and Vintage tests running offline.

The same setup stage resolves the original `httpclient:4.4.1` dependency graph required by
Paimon 2.0's released Hive dependencies. Its remote-resource license plugin reads those
original descriptors outside the host's dependency management: a CI consumer failed before
tests when Maven Central returned HTTP 502 for `httpcomponents-client:4.4.1`. Preparing this
graph with bounded retries before packing keeps that download out of connector compilation.
This populates the isolated repository without changing Paimon's resolved runtime versions
or retrying its build or tests. Update this setup pin with the released Hive dependency graph.

The preparation regression test uses a fresh repository for each pinned Surefire/JUnit
combination, with a host JUnit BOM and both Jupiter and Vintage tests. Its negative controls
reproduce a wrong 1.18 provider pin and deliberately remove the original 2.2 OpenTest4J
artifact to model the incomplete CI cache. After dependency preparation, both engines must
execute successfully offline. This is a runtime-loader regression, not a replacement for
the unchanged upstream SQL smoke test or the complete integration suites.

Short connector suites run sequentially in one job per Flink line: formats first, then Parquet,
ORC, Kafka, and (on 2.2 only) Delta. Formats runs first because its report cleanup spans the format
modules. Each command retains its original selector, native-execution audit, diagnostics and log;
a failed command does not prevent the remaining suites from running, and any failure fails the
job. Paimon and native state suites remain independent, as do the four runtime shards per line.
Inventory dispatches retain their runtime/connectors/all selections. New commits cancel obsolete
runs for the same fork feature branch or canonical main branch, following Flink's own per-ref cancellation policy. Manual SQL
inventories have separate concurrency groups by scope so routine pushes do not cancel them. The
latest main revision and each current PR head still require the complete suite.

The required **All upstream integration tests** job checks all dependency results, then downloads
both lines' runtime reports and runs the existing complete-shard verifier itself. Failed, cancelled
or unexpectedly skipped dependencies fail the gate. The legacy Delta audit is intentionally skipped
only for inventory dispatches; connector-only inventories do not request runtime artifacts.

A normal upstream workflow uses 18 runner jobs (two preparations, fourteen consumers, the legacy
Delta audit, and the final gate), down from 19 with a separate matrix selector and 28 before grouping.
Shared-build downloads remain fourteen. Ordinary CI uses 17 jobs: one Rust job, two Java jobs, five lake-module jobs, two image jobs,
six grouped module jobs, and the final gate. Folding the Flink 1.18 artifact checks into its image
job removed one allocation; grouping short module checks removed another twelve. All test
selections and required gates are retained.
For context, September 27, 2026 [PR #269's upstream run](https://github.com/datafusion-contrib/StreamFusion/actions/runs/36298667226)
spent 42.7 aggregate runner-minutes deleting preinstalled tools and 24.0 downloading/restoring shared
builds. Its coverage jobs waited 33.9 and 36.1 minutes after the final suite completed, then took six
and eight seconds. These are baseline CI measurements, not SQL benchmarks or promised wall-clock
savings: queue conditions vary, cold caches still require preparation, and grouped jobs rerun their
whole connector group when retried. Compare the first cold and subsequent warm CI runs before
claiming an end-to-end speedup.

September 28 baseline measurements further motivate cache retention and release-build reuse:
[PR #263's Flink 1.18 image job](https://github.com/datafusion-contrib/StreamFusion/actions/runs/36492833948/job/109165232775)
took 31.6 minutes, including 28.9 minutes building the image after a Rust cache miss and 49 seconds
running its smoke test. [PR #272's upstream preparation](https://github.com/datafusion-contrib/StreamFusion/actions/runs/36492695468/job/109164790675)
took 26.8 minutes, including 20.1 minutes building and 4.2 minutes reclaiming disk space. These are
pre-change job execution times, excluding queueing; hosted before/after speedups are not yet established.

A local Flink 2.2.1/JDK 17 validation with warm Maven/Cargo caches and four JVM-visible CPUs took
21.4 seconds for the cached planner reactor and 237.5 seconds for complete preparation, including
fresh StreamFusion Java packaging. This excludes archive transfer and test execution and is not a
hosted-runner before/after comparison.

The runtime suite runs in four jobs per version. `runtime_shards.py` discovers the compiled
top-level `*ITCase` classes selected by the planner's upstream Surefire configuration, and assigns
each complete class to one shard. It balances historical durations from
`runtime-durations-<line>.tsv`; newly discovered classes receive an estimated duration and are
included automatically. The timings and reported-case counts come from the September 25 1.18
inventory run and September 26 2.2 CI run, excluding injection-agent unit tests.

Every shard retains serial execution inside its test JVMs and the upstream fork-reuse policy.
It requires all execution contracts belonging to its classes, including every recorded native
or fallback invocation. Coverage verification requires the baseline number of reported cases per
class, including skips: 5,686 for 1.18.1 and 8,619 for 2.2.1 in total. A missing class, lost parameter
variant, overlapping shard, changed plan or missing shard fails the required aggregate check.
When intentionally upgrading the pinned corpus, regenerate timings and counts from a complete
unsharded run; do not lower counts to accept an incomplete run.

```bash
python3 dev/flink-suite/runtime_shards.py record \
  --reports .flink-suite/1.18/flink-1.18.1/flink-table/flink-table-planner/target/surefire-reports \
  --output dev/flink-suite/runtime-durations-1.18.tsv
```

Local full and focused commands retain their existing behavior. To prepare once and run one of
the four CI shards locally:

```bash
FLINK_VERSION=1.18.1 bin/flink-suite.sh prepare
FLINK_VERSION=1.18.1 FLINK_SUITE_REUSE_COMMON_BUILD=true FLINK_SUITE_SHARD=1 bin/flink-suite.sh runtime
```

`FLINK_SUITE_SHARD` accepts 1–4 and cannot be combined with `FLINK_SUITE_TEST`. Use independent
suite directories when running shards concurrently. `FLINK_SUITE_REUSE_COMMON_BUILD` skips the
common build while still compiling connector-specific fixtures; `FLINK_SUITE_REUSE_BUILD` also
requires and reuses those fixtures. Omit both after source changes. SQL-inventory dispatches use
the same shards and retain separate per-shard inventories and evidence in their artifacts.

## Expected host failures in SQL parity audits

Released Flink 2.2.1/JDK 17 fails these expressions even without StreamFusion or an audit source
adapter. Both a one-row DataStream table containing `doc = '{"v":1}'` and a SQL VALUES table
reproduce them:

| Expression | Resolved SQL result type | Host conversion failure |
|---|---|---|
| `JSON_VALUE(doc, '$.v' RETURNING BOOLEAN NULL ON ERROR)` | BOOLEAN | `Integer` to `Boolean` |
| `JSON_VALUE(doc, '$.v' RETURNING DOUBLE NULL ON ERROR)` | DOUBLE | `Integer` to `BigDecimal` |

The failure is a `ClassCastException` during scalar result conversion, after JSON parsing and
path evaluation succeed. Flink's generated BOOLEAN conversion casts the selected object directly
to `Boolean`; DOUBLE casts it to `BigDecimal` before extracting a double. That conversion happens
outside JSON_VALUE's ON ERROR policy. The source column is correctly typed STRING; matching the
declared SQL result schema does not coerce the JSON token's Java object type.

`FlinkJsonReturningHostContractTest` is the independent reproducer and checks the inferred types
and exception causes. Separate controls with JSON `true`, `1.0`, and integer `1` for RETURNING
BOOLEAN, DOUBLE, and INTEGER respectively succeed and match native execution. Run it with
`mvn -pl streamfusion-runtime -am test -Dtest=FlinkJsonReturningHostContractTest`.

Upstream tracking is [FLINK-40463](https://issues.apache.org/jira/browse/FLINK-40463) and
[Flink PR #29063](https://github.com/apache/flink/pull/29063). The proposed conversion layer
replaces exact Java object casts and brings conversion errors under ON ERROR handling;
the PR explicitly reproduces DOUBLE conversion failing on integer JSON tokens. As of
September 16, 2026 it is open. StreamFusion keeps released Flink 2.2.1 and its explicit
exception expectations; a future released dependency upgrade must revalidate that contract.

The audit must retain the original JSON tokens and classify these cases as expected host
failures, rather than missing fixtures, native fallback, or successful result parity. Do not
rewrite integer `1` to decimal `1.0` just to obtain a successful baseline. Native scalar-conversion failures preserve the host ClassCastException and its source/target
Java types through a typed error channel, including in filters and across multiple batches.

## Comparing failed SQL executions

`NativeFailureParity` runs stock Flink and the native-enabled query independently from fresh
fixture factories. The host outcome is captured before the native attempt; no assertion or
expected host error can skip that second attempt. Each outcome retains the exception chain,
collected rows with RowKind, planner substitution/fallback status and fallback reasons.
For a submitted job, a failed collection also reads the terminal job result. This preserves the
operator exception when the collect transport instead reports that its task has already failed.
Local collection errors remain authoritative if the job succeeded, was cancelled by iterator
cleanup, or does not return a terminal result within 30 seconds. Interrupts remain set.

The helper records setup, planning, submission and collection boundaries separately. During
submission or collection, the originating exception stack can identify operator initialization
(`open`/`initializeState`) or row evaluation (`eval`, accumulation, processing, or end-of-input).
Without that evidence it preserves the observed boundary, rather than claiming to know where a
remote failure originated. A source failure delivered by the collect iterator is one such case.
The route describes the plan: a native operator whose `open` fails has not evaluated any rows.

Failure assertions require both executions to fail, matching root-cause classes and a meaningful
message fragment, the expected phase, and an explicit native or fallback route. Success controls
require both to succeed and compare collected results. Wrapper exception text and stack traces
need not match. Partial output is retained for inspection, but asynchronous failed jobs do not
promise identical delivered prefixes; tests assert prefixes only where the fixture defines them.
The single malformed-decimal input, for example, yields no collected rows on either engine.

`FlinkFailureParitySqlHarnessTest` covers:

- SINGLE_VALUE cardinality errors through the native grouped aggregate with the same
  TableRuntimeException, and malformed runtime DECIMAL casts with actual native substitution and
  identical NumberFormatException messages.
- CASE short-circuiting, JSON NULL/DEFAULT ON ERROR, and native TRY_CAST-to-DECIMAL conversion failures.
- Planning rejection, UDF initialization failure and a source failure observed during collection.
- Deliberate success/failure mismatches in either direction, which must fail the parity assertion.
- JSON RETURNING scalar-conversion errors: both engines throw ClassCastException with identical
  source/target type diagnostics on the tested JDK 17 baseline. Cases cover Integer, Long,
  BigInteger, BigDecimal, Boolean and String input objects, native projections and filters,
  and a failure after multiple batches. The native kernel carries a structured error through
  DataFusion; the guarded JNI boundary raises the Java exception after Arrow buffers unwind.

This does not establish identical diagnostics for every native error. JSON ERROR policies and
native integer parsing still use their existing generic native exception wrapper.
The JSON path grammar suite also uses `NativeFailureParity` to check scalar-conversion failures
against Flink's root exception, independently of its TableRuntimeException wrappers.
Malformed STRING-to-BOOLEAN tests preserve literal NUL characters in diagnostic text;
the JNI exception message no longer escapes them as a backslash and zero.

Run the failure suite and independent host reproducer together:

```bash
mvn -pl streamfusion-runtime -am test -Dtest=FlinkFailureParitySqlHarnessTest,FlinkJsonReturningHostContractTest
```

The [portable SQL audit](sql-parity-audit.md) adds typed UDF/UDTF/UDAF and CDC fixtures,
checkpoint failure/recovery, expanded parameter variants and explicit execution-mode accounting.
Its public issue-derived matrix is independent of the unavailable private September audit corpus.

### Flink 1.18 state fixtures

Shared Top-N fixtures retain a key-selector copy method on both lines without requiring the
newer interface declaration, so changing-bound checkpoint and rescaling comparisons compile
against the released 1.18 API too.

The 1.18 runtime suite leaves the upstream fixture's heap or stock RocksDB selection unchanged.
Production planning transparently adapts stock RocksDB to StreamFusion's native backend. Its execution
contracts require native work for admitted queries on both backends. Selectors can combine inherited fixture parameters, such as
`state=HEAP&splitDistinct=false&changelog=false`; a missing field or ambiguous match fails the
invocation. `changelog` reads the fixture's actual randomized execution-environment setting.
Enabled changelog state requires its explicit planning fallback; the suite does not turn off
upstream checkpoint randomization. Both legacy and modern lookup-source variants require native
lookup work. Legacy scalar-registration lifecycle, combined-function and code-generation-split
fixtures also require native Calc/filter work. The summarizer
resolves old Surefire simple class names against the enclosing fully qualified suite name and
still requires one evidence record per executed invocation.
The separate state suite replaces legacy programmatic RocksDB selection with StreamFusion's
backend for configurations without changelog state, preserving the fixture's checkpoint storage
and incremental-checkpoint setting. Changelog-enabled fixtures retain the stock backend.
Its own contract manifest requires native work for admitted replaced cases as well.

The 1.18 Paimon suite runs every integration-test class in Paimon's
[`paimon-flink-1.18` module](https://github.com/apache/paimon/tree/release-2.0.0-rc10/paimon-flink/paimon-flink-1.18/src/test/java):
append compaction, managed memory, orphan removal, positional SQL procedures and Iceberg
interoperability. This is the module selected by
[Paimon's own 1.x CI](https://github.com/apache/paimon/blob/release-2.0.0-rc10/.github/workflows/utitcase-flink-1.x-others.yml).
It also runs the eleven shared methods listed in `dev/flink-suite/paimon-flink118-shared-tests.txt`
(22 parameterized invocations) for catalog construction, native writes and snapshot reads,
branch isolation, schema history, savepoint recovery and bucket rescaling. Every listed method
and every version-specific class must execute a non-skipped case; missing coverage fails the job.
The version-specific module runs in separate JVMs and a separate report directory so its two
class names shared with common fixtures cannot shadow each other's tests or overwrite results.
All failures in either test set block the suite, as do missing native bundle, level-zero writer
or snapshot-reader witnesses. These suite-wide witnesses do not classify every case as native.
A fresh complete build and combined run has 75 cases: 71 pass and four are upstream skips
for named-argument variants that the 1.18 host does not support. The version-specific module
accounts for 53 cases and the shared regressions for 22.

Shared fixtures compile against their declared 1.20 API; both sets run on Flink 1.18.1 with the
released `paimon-flink-1.18:2.0.0` production JAR in place of locally compiled production classes. A generated test POM under diagnostics
puts that released Maven dependency first and retains the original dependencies, compiled test
directory, resources and working directory. The common main output is an empty directory, so
its 1.20 helpers cannot shadow the released runtime; the published JAR is loaded directly.
The versioned JAR supplies both missing compatibility types (`CatalogMaterializedTable`,
`OpenContext`) and replacements for helpers whose managed-memory signatures differ by line.
Appending that JAR after the 1.20 common classes leaves those incompatible helpers in control.
The runner does not add Flink 1.20 runtime JARs or modify upstream sources or assertions.

Paimon's shared programmatic catalog fixture also calls `CatalogTable.newBuilder()`, an API
absent from Flink 1.18. On that line only, the agent constructs the same resolved catalog table
through `CatalogTable.of`: identical columns, primary-key name/columns, partition keys, options
and comment. This adapts fixture construction, not its SQL or result assertions; the eight
sink-parallelism variants remain in the blocking suite. Flink 2.2 executes the original helper.
The recovery fixture also maps its three moved checkpoint-setting field references to the
released 1.18 option and enum locations. Both recovery and bucket-rescaling fixtures map their
restore-path key to `execution.savepoint.path`.
The agent also applies those restore settings to the generated stream graph only while a
recovery or bucket-rescaling fixture is executing: the 1.18 executor does not propagate restore
options from mutable table configuration into its execution environment. Without both adaptations, the source starts
again without restoring its checkpoint. Retention values and the ignore-unclaimed-state behavior
are preserved; setup SQL, savepoint operations and recovery assertions are retained. Neither fixture adapter is applied
on 2.2.

The broader cross-version probe remains a failed diagnostic, not a passing 1.18 suite.
Running the full selected 1.20 common corpus on 1.18 initially produced 262 invocations:
238 passed, 22 failed and two were upstream skips. The fixture adapters and statement-hint
fix resolve the catalog, recovery and branch failures. Stock-only controls, with every
StreamFusion JAR and the agent removed, reproduce the remaining named-compaction, randomized
named-rescale and overwrite-file-layout failures. Paimon documents that
[Flink 1.18 procedures accept positional arguments only](https://github.com/apache/paimon/blob/release-2.0.0-rc10/docs/docs/flink/procedures.md);
its own 1.18 procedure fixtures cover that released API. The common 1.20 corpus is not the
1.18 acceptance suite, and none of its failed SQL or assertions has been rewritten to pass.
The historical-schema timeout in that probe passes focused reruns and remains selected in the
shared compatibility regressions. The two release lines therefore have different Paimon test
corpora; a green 1.18 run does not claim that the full 1.20 common corpus passes on 1.18.

## Complete SQL invocation inventory

The [searchable SQL inventory](sql-inventory/index.md) labels every invocation from the full
Flink 1.18.1 and 2.2.1 planner runtime corpora and the selected upstream format, Parquet, ORC,
Kafka, Paimon and Flink 2.2 Delta suites, with per-line CSV and compressed JSON downloads.
Use the suite filter to recover the original 5,661 and 8,571 passing runtime cases. Connector
corpora add 353 and 584 passing cases respectively; the selected Paimon classes differ by release
as documented above. This snapshot excludes the separate state-backend suite and legacy Delta
1.18 host audit.

Set `FLINK_SUITE_SQL_INVENTORY=true` for a selected suite to record each JUnit invocation's SQL,
planner mode, complete-plan admission counts, fallback reasons and translation failures.
The optional observer preserves upstream inputs, assertions and existing native execution contracts.
It records batch and intentionally unmodified planners as well as streaming planners.

Each executed JUnit case carries an invocation identifier that joins its XML outcome to exactly
one JSON observation under `diagnostics/<suite>/sql-inventory`. Parameterized cases retain their
JUnit unique ID and display name. The inventory generator rejects missing, duplicate, stale or
wrong-line observations and inconsistent XML counts. Skipped cases remain visible without claiming
execution. Expected translation failures and parameter variants that return before executing a
query receive no native coverage credit.

```sh
FLINK_VERSION=1.18.1 FLINK_SUITE_SQL_INVENTORY=true bin/flink-suite.sh runtime
python3 dev/flink-suite/sql_inventory.py \
  --reports .flink-suite/1.18/flink-1.18.1/flink-table/flink-table-planner/target/surefire-reports \
  --evidence .flink-suite/1.18/diagnostics/runtime/sql-inventory \
  --line 1.18 --revision "$(git rev-parse HEAD)" --output .flink-suite/1.18/sql-inventory/runtime
```

The **Upstream Flink suite** workflow's manual `sql_inventory` option defaults to the two planner
runtime legs. Set `inventory_scope=connectors` to select formats, Parquet, ORC, Kafka, Paimon and
the supported Flink 2.2 Delta leg, or `all` to include every normal suite. The runner joins the
selected suite's reports automatically and writes `sql-inventory/<suite>` alongside its original
reports and observations. Inventory completeness failures fail the runner even when upstream
assertions pass. Its labels describe:

- **accelerated**: the query passed whole-query admission during execution translation and the
  test passed. Every interior operator is native; rowwise sources, sinks and perimeter transposes
  are allowed by the [all-or-nothing policy](operators/index.md#the-all-or-nothing-island).
  The classifier checks final root operators, not just positive substitution counts. One unsupported
  interior operator falls back the entire query. This is planner evidence, not a throughput
  measurement or a new per-query native-work contract. EXPLAIN-only plans earn no execution credit.
- **not accelerated**: batch, deliberately preserved stock plans, source/constant-only plans,
  validation/API fixtures without an execution plan, upstream skips/failures, or documented non-goals.
- **should be accelerated**: an in-scope streaming coverage gap, categorized by its recorded
  reason. A test with several queries can contain admitted queries and wholly fallen-back queries;
  the inventory retains both verdicts instead of treating one admitted query as proof for all queries.

The primary label always describes whole-query SQL admission, including in connector suites.
Each test row has `query_verdicts` in JSON and expandable **Query plans** in the report; separate
per-line query-plan CSVs contain one verdict per observed root. The three test labels remain a
rollup for the original test denominator. Source/constant-only roots have no interior SQL computation.
Plan/root indices identify observations within one invocation. Repeated optimization attempts are
retained, so these are not distinct-query or completed-execution counts. The existing observer does
not match individual SQL strings or translation failures to roots; fallback reasons belong to an
optimizer call and may span several roots. A passing fixture whose translations all fail receives
no admission credit.

Categories and notes are generated from the observed admission decisions. The complete raw
plans, SQL and invocation identities remain in JSON for review. This inventory extends the
coverage accounting tracked in [#168](https://github.com/datafusion-contrib/StreamFusion/issues/168);
the stricter, bounded execution contracts continue to run independently.

To publish validated inventories as a standalone searchable report, repeat `--inventory` for
each line and suite:

```sh
python3 dev/flink-suite/render_sql_inventory.py \
  --inventory .flink-suite/1.18/sql-inventory/runtime/inventory.json \
  --inventory .flink-suite/2.2/sql-inventory/runtime/inventory.json \
  --inventory .flink-suite/2.2/sql-inventory/kafka/inventory.json \
  --output docs/sql-inventory \
  --flink-repository /path/to/flink
```

The optional Flink clone supplies verified source links at the release tags; no checkout or source
change is required. CSVs escape NUL and unpaired Unicode surrogates from negative string fixtures while
JSON preserves the exact values. The report distinguishes observed planner gaps from batch,
metadata, schema, API-validation and early-return cases. It retains mixed native/fallback invocations
as coverage targets whenever an in-scope query remains on the host.

Connector invocations also retain the specific native physical components admitted in their
execution plans. A fully admitted SQL query may use rowwise source/sink boundaries; native connector
decode or encode is a separate optimization. Observations include the implementation
class and connector/format identifiers of retained host boundaries, without copying arbitrary
connector options. Existing per-invocation Delta write
contracts and suite-level Parquet/ORC/Paimon markers remain independent checks. The inventory
does not turn a suite-level marker into evidence for every invocation in that suite.

The report renderer accepts several inventories for each Flink line when their suites are distinct.
It adds a suite filter and retains each row's engine revision. Optional `--kafka-repository`,
`--paimon-repository` and `--delta-repository` arguments verify source links against the same pinned
release tags as the runner. Repeated input for the same line and suite is rejected.

Retained streaming filesystem, Kafka, Paimon or Delta boundaries remain targets in the separate
`connector_label`, `connector_category` and `connector_note` columns. They never override the primary
query-admission label. `sql_label` remains an alias of that primary label for existing consumers.
Connector notes name the host implementation and format; `native_components` is diagnostic evidence,
not a criterion for partial query credit. Internal DataStream, collect, values and test-format
boundaries are excluded from connector targets. Expected translation errors, batch and non-executing
fixtures keep their existing exclusions.
Direct Java serializer and DataStream/legacy DataSet format tests receive `format-api` and
`non-sql-program` exclusions: these upstream fixtures bypass SQL admission entirely.

### Paimon host API precedence

The released Paimon 2.0.0 Flink 1.18 connector embeds a `RowData` interface that
includes the newer `getVariant` method. The optional StreamFusion Paimon module
places the host `flink-table-common` dependency before that connector on its compile
classpath. This keeps shared projection wrappers on the actual Flink 1.18 contract
when the source-suite clean build causes javac to resolve their source implicitly.
It does not patch Paimon, add VARIANT support to Flink 1.18, or change test selectors.
The backend comparison and Fluss source/sink implementation are unaffected.

### Paimon random full-compaction completion

Released Paimon 1.0 and 2.0 `PrimaryKeyFileStoreTableITCase.checkChangelogTestResult`
counts every changelog event whose value is at least 10,000 before stopping. Legitimate
unchanged terminal UPDATE_BEFORE/UPDATE_AFTER pairs can exhaust that counter while
other keys still contain earlier values. Replaying the captured PR300 warehouse with stock
Paimon reproduced the original early assertion; continuing to complete current terminal-key
coverage passed every original row assertion and final result assertion.

The suite agent narrowly adapts that private method's counter to current distinct positive
terminal-key coverage: INSERT/UPDATE_AFTER adds the `(partition,key)` when its value is
at least 10,000; UPDATE_BEFORE/DELETE retires it. The original producer-count comparison,
SQL, iterator, row checker, final assertions and timeout remain unchanged. The adapter does
not filter legitimate no-op changelog pairs or modify released source files. Its bytecode
visitor rejects unknown shapes instead of silently applying a different transformation,
and a thread-local scope clears coverage on normal and exceptional exits.

The counter visitor reads the original local-variable slots before lifecycle advice remaps them.
A composed transformation regression loads and executes the rewritten method, checks repeated
terminal pairs, and verifies cleanup after normal and exceptional exits.

## Iceberg streaming SQL

`bin/flink-suite.sh iceberg` runs the published Iceberg 1.12.0 Flink 2.2 test
artifacts against released Flink 2.2, then repeats the same streaming invocations
with StreamFusion's experimental Iceberg planner path enabled. It also requires
the connector's Arrow hook, native Parquet readback, scalar precision, keyed
checkpoint/restart stress, and SQL native-file witnesses.
The CI connector matrix includes this suite on the 2.2 line. It requires Java 17.

The initial upstream selection is `TestFlinkTableSinkExtended`: all four methods
and all three streaming sink-API parameters (legacy, V2, and default), preserving
the SQL, data, assertions, and upstream timeouts. Its eight batch invocations are
explicitly excluded by an execution condition. Stock/native Surefire inventories
must match: 12 executed streaming sink invocations each. Unsupported sink APIs and
distributions continue to exercise stock fallback. Published test JARs retain the
upstream parameterization; no Iceberg production source is built or modified.

The source selection adds the unchanged `testWatermarkValidConfig` and
`testWatermarkInvalidConfig` methods from published `TestIcebergSourceSql`.
Their table environments execute in streaming mode; its five batch cases are
explicitly excluded. The extension installs StreamFusion in the fixture's existing
streaming table environment after its original setup. These source SQL cases
currently verify stock-source fallback and downstream parity; they do not establish
native source admission. Published `iceberg-data` and `iceberg-core` test JARs provide
the original helpers. The verifier requires the same 14 executed upstream SQL cases
and 13 batch exclusions across both engines, plus all required native witnesses.

Reports are retained under `.flink-suite/2.2/iceberg/{stock,native}/target/surefire-reports`.
`TestFlinkTableSink` and additional source/recovery suites remain to be integrated;
its base fixture starts Hive and REST services even for Hadoop parameters, so that
fixture dependency footprint must be provisioned before admitting its cases. The
coverage inventory will expand alongside the capabilities in
[issue #373](https://github.com/datafusion-contrib/StreamFusion/issues/373).

The per-invocation contracts for `CalcITCase.testInlineScalarFunction`,
`testScalarFunctionConstructorWithParams` and `testNonStaticObjectScalarFunction` require
`NativeCalcOperator`, matching the default-enabled scalar UDF path. Local Table API controls
check constructor-bound and instance function values with positive native input metrics.
The exact heap/RocksDB runtime scope records its mixed native/host route on both release lines.
An obsolete `AS` fallback expectation must not mask that native execution.


`AggregateITCase#testGroupBySingleValue` now has an explicit native grouped-aggregate
contract and exact released heap/RocksDB/mini-batch fixture scope. That unchanged method
covers scalar-subquery aggregation and CHAR single-value results; it does not by itself
prove every retracting scalar partial. The focused signed SINGLE_VALUE suite supplements
it with raw changelogs at controlled bundle boundaries, keyless empty results, materialized
interleaved relations and two restores on each backend and released Flink line.


The unchanged `AggregateITCase.testListAggWithRetraction` also has per-phase consumed-work
contracts. Its one-hour-retention single-phase variants require native grouped aggregation;
the two-phase variants retain the explicit append-only LISTAGG partial-layout fallback until
the retracting merge is implemented. Focused operator tests add independent list-entry and
pending-retraction deadlines, survivor refresh and repeated recovery on both state backends.


Runtime route scopes use each released fixture's exact display name; Vintage method prefixes
must match the selected method. The SINGLE_VALUE upstream fixture also runs a retracting
keyless-join probe that retains its existing host fallback before its independent native
scalar query. Its whole-fixture route is therefore mixed, while the grouped-aggregate
contract still requires consumed native input. Flink 1.18 changelog-state variants retain
explicit full fallback. DataStream `InputConversionOperator` is counted as source conversion
only when its graph inputs lead directly to a verified source boundary. The same class
inside computation remains host work; no native-work or local/global requirement is relaxed.

The primitive-array and filtered object-array stream UNNEST fixtures now require
positive input consumption by `NativeColumnarUnnestOperator` on both released
lines, including the 1.18 state contract inventory. These unchanged upstream
fixtures complement runtime-source regressions for preserved string carriers
and collection aggregate recovery.

The unchanged SQL `AggregateITCase.testOverloadedAccumulator` is contracted across its
complete released parameter matrix. Its function has no merge method, so synchronous
variants retain a physical single-phase group with or without mini-batching, including
requests for local/global planning. They require consumed JVM group input on both supported
lines and heap/RocksDB backends. Flink 1.18 changelog-state and Flink 2.2 asynchronous-state
variants retain explicit full fallback. Route scopes preserve each release's exact display
names and contract variants. A data-validation test requires one matching contract and route
declaration for every fixture configuration. Collection-result mini-batch exclusions are unchanged.
The 1.18 state-contract inventory includes this fixture too, so its separate run with changelog
state disabled must prove native group input rather than validating only the default exclusion.


Iceberg's native evidence inventory also requires nested source/sink and maintenance
round-trips, nested schema evolution, nested equality-key/null-parent handling,
v2 position deletes/v3 vectors and row lineage, plus nested savepoint/rescaling
through normal, range and dynamic sinks. SQL witnesses require native source/sink
nodes, no payload transpose nodes, native task counters and Rust data-file footers.
Captured-channel recovery witnesses also require zero Arrow allocations and zero
task off-heap reservations after each scenario, alongside zero native handles. Failed
handover batches may await their cleaner backstop, so the bounded cleanup check drains
them before another MiniCluster selects a different task off-heap budget. The normal
runtime guard still rejects changing that budget while reservations remain live.
Unsupported nested name mapping, pushed predicates and partition sources have explicit
admission checks. These are StreamFusion witness tests alongside the unchanged
published Iceberg streaming test inventory.


The Iceberg profile also ports the released 1.12.0 rewrite tests. Streaming maintenance
cases are inherited directly from the published `TestRewriteDataFiles` test class
and run with stock and native workers. The SQL-backed action test keeps the upstream
queries, options and assertions; its adapter substitutes only file processing under
the original action planner and optimistic commit logic. The port uses Hadoop with
both original Hadoop base namespaces, all three file formats, and the released v2-and-above parameters;
Hive/REST fixture services are excluded. Parquet admits the native worker, while
Avro/ORC remain stock. The older action API's V3/V4 rejection assertions remain intact;
streaming V3 rewrite support is checked by the separate maintenance cases.

These SQL setup/readback queries retain the upstream batch TableEnvironment. Actual
native file processing explicitly uses Flink STREAMING execution; the inherited
maintenance tests use the upstream live trigger source. This does not enable bounded
SQL sources or introduce a new SQL CALL procedure. The test-only boundary retains
Iceberg's bin packing, residual handling, stale-snapshot conflict checks, starting
sequence numbers and commit cleanup. Native rewritten Parquet footers are required;
empty/constructor-rejection/control-only cases do not invent file-work witnesses.
The upstream test profile pins JUnit 5.14.4 to match Iceberg's release and execute its
`FieldSource` cases. [Provenance](https://github.com/datafusion-contrib/StreamFusion/blob/main/dev/flink-suite/iceberg/rewrite-tests.json)
and the adjacent compressed port diff record the fixture and entry-point adaptations.


## Native witnesses for canonical restore programs

With `streamfusion.flink-suite.canonical=true`, the suite agent binds the inherited released
`RestoreTestBase.testRestore` invocation to its concrete operator family and program ID.
The declarations live in `dev/flink-suite/agent/src/main/resources/canonical-native-execution.tsv`.
Continuation programs require positive consumed input in the declared native family; an
unrelated Calc, stock fallback, or imported state cannot satisfy that requirement. Program
selectors normalize hyphens to underscores. Exact family fallbacks require their recorded
planner reason and no input or imported state in that family.

The released literal-aggregate join program supplies no post-restore input. Its explicit
`@restore:NativeColumnarUpdatingJoinOperator` contract instead requires successful nonempty
JNI state import and a successful native operator open, with no resumed native input or
fallback. The agent records batches after the JNI import returns; failure or opening alone
cannot prove restoration. Reports retain consumed inputs in the original field and add an
optional fifth field for imported rows in opened native operators. The verifier exposes
`native_canonical_import_rows` separately and classifies that scoped proof as `native_restore`.
These counts establish restored native work, not throughput or future-input continuation.

The released temporal-sort restore fixtures distinguish a secondary-key rowtime sort from a
single processing-time order. The unchanged rowtime fixture orders by both rowtime and a DOUBLE
key, so it retains the stock sort under the existing secondary-key gap. Its mixed-route contract
requires nonempty native Calc input, the explicit secondary-order fallback reason and nonempty
input in a real stock rowtime sort whose managed state and secondary comparator initialized.
A Calc alone, opening alone, native-sort input/imports or a successful restore cannot satisfy it.
This is a stock-state compatibility control, not proof of native temporal-sort state transfer;
the single-key native sort handoffs retain their separate positive execution requirements. Flink
elides the processing-time sort into a forward edge. Its exact fixture contract is
`@elided:TemporalSort`, restricted to `program=temporal_sort_proctime`: at least one job must have
been submitted successfully, every recorded source-to-sink graph must be complete and contain
no stock or native sort, and no native sort/state import or fallback may occur. The original
upstream output and restore assertions remain unchanged. Unknown programs cannot use this
contract. This route is reported as `ordering_elided` and receives neither native execution nor
fallback credit. A seventh TSV column carries submitted graphs and raw open/import observations;
the audit independently validates that evidence and rejects missing graphs or hidden sort work.

Run the contract self-tests with:

```sh
mvn -f dev/flink-suite/agent/pom.xml -Dstreamfusion.flink-suite.canonical=true \
  -Dtest=CanonicalRestoreExecutionTest test
python3 -m unittest discover -s dev/flink-suite -p test_summarize.py
```

The state adapters and complete compatibility matrix remain tracked by
[P0 #569](https://github.com/datafusion-contrib/StreamFusion/issues/569). Benchmarking and
tuning are deferred until functionality is complete.

Window Top-N uses a separate native operator from ordinary Top-N. The execution agent instruments
its successful `open` and consumed Arrow batches explicitly, so a window-rank restore contract
requires work from window Top-N itself even when native aggregates or Calc also run in the plan.

Window-deduplication restore programs require the shared native window-selection operator to
consume input. Opening it without rows, or running an unrelated native Calc, cannot satisfy
that gate. Dedicated witness controls cover these cases, retaining the original released
fixture assertions alongside the native-family requirement.

## Window join canonical restore qualification

The unmerged #569 stock compiled-plan integration passes all eight released Flink 2.2.1
`WindowJoinEventTimeRestoreTest` programs in the stock control and native lanes, with no failures,
errors or skips. The six admitted INNER/LEFT/RIGHT/FULL TUMBLE and INNER HOP/CUMULATE programs
require successful native window-join input. The agent also records successful direct canonical
imports; each of these six programs imports pending rows into the native state owner.

Released SEMI/ANTI window joins retain their stock implementation. Each has an explicit contract
requiring the exact planner fallback reason, absence of native window-join input/import, and
successful input in the initialized stock list/timer owner. An unrelated native Calc, an opened
but unused stock operator, missing state, or an incorrect fallback cannot satisfy these controls.
The summary keeps stock state-owner work separate from native input. The original released test
classes, frozen plans/savepoints and expected outputs are unchanged.

The window-join stock compiled-plan workflow also passes these fixtures in a clean component
with automatic state and identity binding. Its separate 46-case job matrix covers both engine
orders and repeated switches. Ordinary SQL identities and the rest of the full #569 matrix
remain separate requirements. Benchmarking of canonical handoff remains deferred.

## Shared asynchronous capture restore controls

The asynchronous capture implementation is checked with the released First-N, append-rank,
update-rank, retract-rank and eager-deduplication programs on Flink 2.2.1. The existing wrappers
select released programs and retain their inherited restore assertions. Stock and native runs each
pass six invocations. The native audit additionally requires positive canonical import and
subsequent input in the expected owner: all six routes are native, with no expected fallback.
These controls qualify those selected programs, not every variant of the five operator families
or a cross-version upgrade. Broader operator checks retain their separate coverage boundaries.

```sh
CANONICAL_UPSTREAM_TESTS='tech.streamfusion.canonical.FirstNRestoreTest#testRestore,tech.streamfusion.canonical.AppendRankRestoreTest#testRestore,tech.streamfusion.canonical.UpdateRankRestoreTest#testRestore,tech.streamfusion.canonical.RetractRankRestoreTest#testRestore,tech.streamfusion.canonical.EagerDedupRestoreTest#testRestore' \
  bin/canonical-savepoints.sh sql -Dnative.build.skip=true
```

The additional `TypedGroupRestoreTest` selector inherits the released group-aggregate restore
assertions and chooses the scalar and shared DISTINCT programs. Both stock and native runs pass
two invocations. The execution audit requires canonical import and subsequent input in the native
group owner; both routes are native with no expected fallback. The released fixtures and assertions
are unchanged, and the selector does not expand operator admission.

```sh
CANONICAL_UPSTREAM_TESTS='tech.streamfusion.canonical.TypedGroupRestoreTest#testRestore' \
  bin/canonical-savepoints.sh sql -Dnative.build.skip=true
```

## Released Flink 1.18 canonical protocol controls

The canonical fixture module also accepts `-Pflink-1.18,canonical-stock`. Flink 1.18 predates
the newer released SQL `RestoreTest` framework, so the module excludes those wrapper sources and
selects the unchanged released `HeapSavepointStateBackendSwitchTest` and
`RocksSavepointStateBackendSwitchTest` controls. They come from the canonical Maven release test
artifact; no sibling Flink build or copied implementation is needed.

Run the stock protocol reference with:

```sh
mvn -Pflink-1.18,canonical-savepoints,canonical-stock -pl dev/canonical-savepoints -am test \
  -Dsf.runtime.tests.skip=true
```

These are upstream backend-format controls. Native execution and actual stock/native continuation
are established separately by StreamFusion's shared operator and isolated complete-job tests.
The `bin/canonical-savepoints.sh` SQL/native evidence workflow retains its separately qualified
2.2 restore programs; this profile does not claim that absent framework exists on 1.18.

## Rank streaming and shared transfer audit

The rank/list streaming cohort reuses the unchanged released append, update and retract rank
programs, plus interval/temporal/window joins and window rank/dedup restore programs. Stock and
native each pass 32 invocations with no skips. The native audit confirms 29 native routes, two
expected stock window-join fallbacks and one interval elision. Thin contract import entry points
retain the owned row list for instrumentation while the shared sender owns batching and C Data;
retracting metadata headers and validation passes cannot count as imported rows. All four temporal
programs now require canonical import and later input in the actual native temporal owner; new
negative controls reject validation-only and input-only evidence. All 202 canonical execution-audit
controls pass. These are released fixture
proofs, not exhaustive operator/layout or cross-release upgrade qualification.

## Released canonical savepoint runner

Run the opt-in Flink 2.2.1 canonical lane with JDK 17:

```sh
bin/canonical-savepoints.sh sql
bin/canonical-savepoints.sh runtime
```

The runner packages and tests the execution agent, then runs each selected released test artifact
on stock and native classpaths. Stock excludes the StreamFusion runtime; a JUnit guard verifies
that exclusion and initializes the JNI library on the native leg. Restore fixtures are unpacked
into a disposable working directory, without modifying Flink tests or their original assertions.
The module rejects other Flink release lines.

`CANONICAL_UPSTREAM_TESTS` selects an explicit comma-separated set of released classes/methods.
The SQL inventory includes operator restore programs; the runtime inventory includes canonical
formats, backend switching, timers and savepoint lifecycle. Required execution contracts and
missing-test checks make partial or stock-only runs fail the native qualification. Broad SQL
family completion remains work in [#569](https://github.com/datafusion-contrib/StreamFusion/issues/569);
the presence of a runner or a passing stock lifecycle test does not prove native state interchange.
The selected window-join restore lane passes eight stock and eight native invocations against
the compiled-plan integration, including its six actual native state owners and two verified
stock SEMI/ANTI fallbacks. Benchmarking remains deferred.

### Released runtime control qualification

The full seven-class Flink 2.2.1 runtime selection passes 51 active invocations on each configured
engine classpath, with zero failures or errors. Each lane reports the same one upstream-disabled
`SavepointITCase.testTriggerSavepointAndResumeWithNoClaim` invocation (FLINK-25427). The active
controls comprise 16 savepoint-format cases, eight heap/Rocks backend-switch cases, one timer
case, 18 savepoint lifecycle cases, four stop-with-savepoint cases and four JobMaster triggering
cases. The original classes and assertions are unchanged.

These controls have no native state-owner execution contracts. They exercise the released Flink
runtime/backend lifecycle with stock and StreamFusion dependency visibility; they do not establish
conversion of StreamFusion operator state. Native-family SQL restore witnesses and isolated
cross-engine job continuations remain separate qualifications.

### Interval restore state-owner proof

The execution agent tracks successful native interval input and direct canonical row import,
excluding timer-transfer batches from row counts. Seventy-seven canonical witness controls pass,
including rejection of opened-only, import-only, Calc-only or window-only work for an interval
restore contract.

The clean compiled-plan integration passes all three unchanged released interval programs and
all eight window programs on each engine: eleven stock and eleven native-side passes. Each of
the event-time and processing-time interval programs imports ten pending rows and consumes five
new native rows. The window selection includes six actual native state owners and two verified
stock SEMI/ANTI paths.

The pinned impossible-interval LEFT join is already stateless in Flink. Its exact
`@elided:IntervalJoin` contract requires a successfully submitted complete acyclic graph with no
join or CoProcess state owner, no native state import, no stateful native operator or fallback,
and actual native Calc work. Original upstream assertions still check the null-padded rows. The
audit reports `interval_elided`, separately from native operator and fallback credit. Calc work
without that submitted-graph evidence cannot pass. Unknown programs cannot reuse the contract.
Seventy-seven Java witness controls and 48 summary-parser controls include missing/forged graph,
wrong operator, hidden state/import and failed-submission rejections. Full-job/type/version and
connector qualification remains tracked by #569; these upstream fixtures do not replace it.

The interval integration additionally passes 84 isolated complete-job handoffs (457 combined
checks), retaining custom sources, transactional sink effects, both transposes and stock worker
classpaths without StreamFusion. This includes both clock modes, residual predicates, no-input
re-export, real checkpoint recovery and 1 → 3 → 2 rescaling. These are StreamFusion integration
fixtures, separate from the unchanged released upstream programs and their assertions. The
remaining #569 matrix stays open; benchmarking is deferred.


### Temporal canonical restore witnesses

All four unchanged released 2.2.1 `TemporalJoinRestoreTest` programs pass on stock and native.
The explicit program contracts require actual native temporal input; unrelated Calc work,
import-only/open-only paths, fallback and unknown programs cannot qualify them. Each native
program imports four retained temporal rows and consumes four post-restore rows. Instrumentation
counts row transfer independently of cursor/callback metadata. The row/map-key programs derive
scalar keys before the temporal boundary; the legacy function fixture uses rowtime. Eighty-eight
canonical witness controls and 48 summary controls pass. This is upstream compiled-plan restore
coverage, not the full isolated engine-switch/connector/failure matrix of
[#569](https://github.com/datafusion-contrib/StreamFusion/issues/569). Benchmarking is deferred.

The separate temporal integration matrix passes 26 isolated complete-job scenarios and 240
combined checks: both engine orders and native stores, INNER/LEFT, residual predicates, no-input
re-export, native checkpoint recovery and 64-key rescaling through 1 → 3 → 2 tasks. It retains
two custom sources, a transactional sink, both transposes and stock classpath exclusion. Pending
probes/version history survive every cut; final committed output also matches an independent
version-history oracle. These StreamFusion-owned fixtures preserve all probes until final drain
and do not qualify intermediate temporal-output commit failures. They complement the unchanged
released upstream programs; they do not replace their assertions or complete the broader #569
connector/failure/version/ordinary-SQL matrix. Benchmarking remains deferred.


After changing state registration to use actual runtime primary IDs, all 15 unchanged released
2.2.1 temporal/interval/window restore programs pass on each engine. The native audit verifies
12 actual join state imports with continued native input, two stock window SEMI/ANTI owners and
one join elided by Flink. These retain the released programs' original UID settings. Separate
StreamFusion integration jobs qualify 34 generated-ID round trips and ten explicit-UID controls;
124 planner/identity and 249 job/runtime/metadata checks pass. The generated-ID job scope is
listed in [canonical state coverage](backends/canonical-state.md#generated-operator-ids-in-qualified-compiled-plans).
Ordinary-SQL optimizer and broader connector/type/version/failure qualification remain open;
benchmarking is deferred.


### Selecting the released first-N restore program

`tech.streamfusion.canonical.FirstNRestoreTest` is a thin subclass of the released
`RankRestoreTest`; its only override selects `RankTestPrograms.RANK_N_TEST`. It keeps the
released compiled plan/savepoint fixture, inputs, inherited restore implementation and original
assertions. The full upstream rank class and all its execution contracts remain intact for
qualifying other rank adapters.

Select it through the canonical lane with
`CANONICAL_UPSTREAM_TESTS=tech.streamfusion.canonical.FirstNRestoreTest#testRestore bin/canonical-savepoints.sh sql`.
The stock leg excludes StreamFusion runtime classes. The native leg requires positive first-N
input and records actual imported counter batches, excluding the validation-only pass. The
qualified fixture imports two counters and processes four new rows. Together with the four
existing join restore classes, the selection passes 31 original programs on each engine. These
reports overlap previous cohorts and are not cumulative coverage counts. This proves that
first-N fixture, not the other rank algorithms or all schema/backend/version variants.


The normalizer qualification retains all five released `ChangelogNormalizeRestoreTest` programs
and their original fixtures/assertions. Three native paths each import two canonical rows and
consume new input; validation-only visits are excluded from import counts. The two pushed-filter
paths retain the stock normalizer and exact fallback reason. Together with first-N and joins,
all 36 selected programs pass per engine under their existing strict route contracts. This lane
does not qualify unrelated aggregate/dedup/rank adapters or replace complete-job connector tests.


### Selecting the released eager-dedup restore programs

`tech.streamfusion.canonical.EagerDedupRestoreTest` subclasses the released `DeduplicationRestoreTest`
and selects its `deduplicate-asc-proctime` and `deduplicate-desc` programs. Inputs, compiled plans,
savepoint fixtures, inherited restoration and original assertions remain unchanged. The original
full deduplication class remains unchanged. Its historical ascending-rowtime fixture was
compiled on Flink 1.19 and omits `outputInsertOnly`; released Flink 2.2.1 therefore selects the
eager `RowTimeDeduplicateFunction`. It does not exercise the newer watermark-buffered function.

Run it with
`CANONICAL_UPSTREAM_TESTS=tech.streamfusion.canonical.EagerDedupRestoreTest#testRestore bin/canonical-savepoints.sh sql`.
Both programs pass on each engine. Each native leg records two actual canonical imports and four
new input rows in the eager deduplicator; validation-only visits are excluded. The stock leg
excludes StreamFusion runtime classes. Six new audit controls reject missing/unrelated input and
ensure validation does not inflate imported-row counts. Together with the selected first-N and
five normalizer programs, this cohort passes eight programs per engine. These are overlapping
cohorts, not cumulative coverage counts, and do not qualify the full deduplication family.


The full released deduplication class now passes all three programs on stock and native
classpaths. The ascending-rowtime program also imports two rows and consumes four new rows in
the eager native owner. Alongside first-N and five normalizer programs, all nine programs pass per
engine: seven actual native owners and two expected normalizer filter fallbacks. Original inputs,
historical compiled plans and assertions remain unchanged. Each dedup program explicitly requires
the eager family in the execution audit. The buffered import hook counts only mutating passes;
99 audit controls include cases proving that buffered import/input cannot satisfy the historical
eager contract. These upstream results do not qualify current buffered-rowtime SQL jobs; those
have separate direct, planner and process tests. Reports overlap earlier selections.


### Selecting the released append-rank restore program

`tech.streamfusion.canonical.AppendRankRestoreTest` selects only
`RankTestPrograms.RANK_TEST_APPEND_FAST_STRATEGY` from the released `RankRestoreTest`. The compiled
plan, savepoint, source data, inherited restore method and expected results remain unchanged.
Run it with
`CANONICAL_UPSTREAM_TESTS=tech.streamfusion.canonical.AppendRankRestoreTest#testRestore bin/canonical-savepoints.sh sql`.
Its native witness imports three retained rows and processes two new Top-N rows; validation-only
visits do not count as imports. Stock runs exclude the StreamFusion runtime. The append, first-N,
normalizer and full dedup selection passes ten original programs per engine; eight require their
native owners and two keep the expected normalizer-filter fallback. The canonical agent passes
104 audit controls, including checks that unrelated work or imports alone cannot satisfy append
continuation. This qualifies the released append fixture and the
[documented append scope](operators/top-n.md#value-ordered-append-canonical-state-partial), not
updating/retracting ranks or the full operator/connector/version matrix. Benchmarking is deferred.


### Selecting the released updating-rank restore program

`tech.streamfusion.canonical.UpdateRankRestoreTest` selects the released
`RankTestPrograms.RANK_TEST_UPDATE_FAST_STRATEGY` through a thin subclass. It inherits the original
restore implementation, compiled plan/savepoint, input, two-field ordering, settings and assertions.
Select it with `CANONICAL_UPSTREAM_TESTS=tech.streamfusion.canonical.UpdateRankRestoreTest#testRestore bin/canonical-savepoints.sh sql`.
The fixture imports six retained native rows and processes two new Top-N rows. Shared rank import
advice records only successful applied transfers, excluding the validation-only pass; unrelated
operators or import-only execution cannot satisfy continuation.

The updating/append/first-N/normalizer/full-dedup selection passes eleven original programs in both
engines, with nine required native paths and two expected normalizer-filter fallbacks. All 109
canonical audit controls pass. Stock controls exclude StreamFusion runtime classes; the bundled
native library is verified against the qualified debug artifact. This qualifies the selected
released fixture and the [documented updating subset](operators/top-n.md#updating-canonical-state-partial),
while the wider operator/connector/version matrix remains open. Benchmarking is deferred.


### Selecting the released retracting-rank restore program

`tech.streamfusion.canonical.RetractRankRestoreTest` selects the released
`RankTestPrograms.RANK_TEST_RETRACT_STRATEGY` through a thin subclass, retaining the original
compiled plan/savepoint, source schema and changelog settings, input, inherited restore method
and expected outputs. Select it with
`CANONICAL_UPSTREAM_TESTS=tech.streamfusion.canonical.RetractRankRestoreTest#testRestore bin/canonical-savepoints.sh sql`.
It imports six retained native rows and consumes two new Top-N rows. A dedicated witness counts
only committed data row kinds, excluding count-map/list/bound headers and validation-only visits.
The `@resume:NativeColumnarTopNOperator` contract requires both a nonempty canonical import into
an opened Top-N operator and new input through that family; stock-owner work or fallback reasons
cannot satisfy it. Import counts and new input remain separate evidence fields.

The retract/update/append/first-N/normalizer/full-dedup selection passes twelve original programs
per engine: ten required native paths and two expected normalizer-filter fallbacks. All 118
canonical witness controls pass, including metadata-only, validation-only, import-only, open-only,
wrong-family and fallback rejection. All 49 Python summary controls pass too. Stock test processes
exclude StreamFusion classes; the embedded native library matches the qualified debug artifact.
This qualifies the released restore fixture. Complete-process handoffs and the broader coverage
matrix remain separate checks; see [retracting scope](operators/top-n.md#retracting-canonical-state-partial).
Benchmarking remains deferred.


The complete unchanged released `RankRestoreTest#testRestore` also passes all four original
programs in each engine, with all four required native paths. Its retracting program separately
proves six imported rows and two new Top-N rows through the stronger continuation contract.
These are the same four rank programs selected by the thin classes above, so the qualification
counts are overlapping, not additive. No program is excluded from this rank-class run.


### Released grouped-state canonical resume proofs

The unchanged released Flink 2.2.1 `GroupAggregateRestoreTest#testRestore` selection passes all nine
stock invocations and five native canonical resumes. Four UDF programs still fail strict native
execution checks, so the complete native selection remains red. Original queries, fixtures and
sink assertions remain unchanged.

| Released program | Applied stock groups | New global native rows | New local native rows |
| --- | ---: | ---: | ---: |
| `group-aggregate-simple` | 2 | 3 | — |
| `agg-with-state-ttl-hint` | 2 | 3 | — |
| `group-aggregate-simple-mini-batch` | 2 | 2 | 3 |
| `group-aggregate-distinct` | 3 | 7 | — |
| `group-aggregate-distinct-mini-batch` | 3 | 5 | 7 |

Every program has an explicit selector. Qualified grouped contracts require applied nonempty
canonical state import and new input in the same opened native grouped owner. Mini-batches
additionally require new input in the native local stage. The agent records header batches only
after successful JNI-backed import, excludes validation-only passes, and associates the native
handle with the operator that subsequently opens. Unknown programs, unrelated Calc work,
wrong/unopened handles, empty imports, missing stages and import-only or input-only execution
cannot satisfy the proof. Canonical controls pass 162 Java cases and 51 independent Python
report/parser cases. These protocol checks do not qualify other operator or connector variants.

Use the normal released runner without editing upstream tests or fixtures:

```sh
CANONICAL_UPSTREAM_TESTS=org.apache.flink.table.planner.plan.nodes.exec.stream.GroupAggregateRestoreTest#testRestore \
  bin/canonical-savepoints.sh sql
```

The [grouped-state coverage page](operators/group-by.md#canonical-localglobal-distinct-mini-batches-partial)
records the exact scalar/view, type, backend and complete-job qualification. All broader #569
operator/connector/version requirements remain open; benchmarking remains deferred.

Scalar JVM local/global aggregation additionally contracts the unchanged SQL
`AggregateITCase.testVarArgsNoGroupBy` and `testVarArgsWithGroupBy` fixtures. Ordinary and single-phase mini-batch variants
require consumed JVM grouped input. Actual local/global variants require positive input
consumption by **both** `NativeColumnarJvmLocalGroupOperator` and
`NativeColumnarJvmGlobalGroupOperator`. Their inherited Arrow-boundary implementation is
instrumented at the shared operator, while observations retain the concrete phase class.
Route scopes cover each released heap/RocksDB parameter and preserve the existing async
and Flink 1.18 changelog-state fallback variants. These fixtures supplement the typed,
controlled-clock bundle and checkpoint/savepoint-rescaling checks; unrelated native work
cannot satisfy either phase contract.

Ordinary DataStream-backed table programs retain their original execution route when Flink's
released execution-node metadata declares the graph nonserializable. Canonical stock-template
binding must not force these programs through JSON serialization. Unchanged upstream aggregate,
nested-table Calc and OVER assertions still run with their original native-execution contracts;
this boundary does not add DataStream-bridge canonical interchange coverage.


Bounded join coverage adds the unchanged
`OuterJoinITCase#testLeftUpperAndLowerWithAgg` fixture on both lines. Its four
original `expectedJoinType` variants (broadcast hash, hash, sort merge and nested
loop) require positive `NativeBatchJoinOperator` work. Both route scopes enumerate
these exact display names. The observer instruments both batch join input methods;
result parity or unrelated native Calc work cannot satisfy the join contract.

The unchanged batch date fixtures `CalcITCase#testDateCompareWithDateString` and
`#testDateEqualsWithDateString` retain explicit standalone-Calc performance
fallback contracts on both supported lines. Their exact runtime-route cases are
`batch_host_only`; these passing oracle fixtures receive no native-work credit.
Separate admitted-pipeline DATE controls verify native work and absence of
registered Java predicate evaluators.

The inherited batch aggregate identity check applies to contracts that include those fixtures; the Flink 1.18 RocksDB-only contract has no batch aggregate fixtures.

The unchanged `JoinITCase#testLongHashJoinGenerator` checks Flink-generated operator source code, so that invocation retains the stock planner. The adjacent SQL join fixtures retain their native execution contracts. The batch code splitting fixtures explicitly check deep-expression fallback and positive native work for wide grouped reductions.
