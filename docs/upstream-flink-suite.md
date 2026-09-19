# Upstream Flink suite

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
events. These test change-event handling, not capture from a live database. `paimon` runs the
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

The runner clones Flink `release-2.2.1`, Kafka connector `v5.0.0`, and Paimon `2.0.0` (its
`release-2.0.0-rc10` tag), plus Delta `v4.4.0` for its SQL tests, under `.flink-suite`, verifies that each checkout is clean, builds and
installs StreamFusion and its supported format/connector modules, and builds the required upstream
reactors with tests skipped. A test-only
Java agent then installs StreamFusion whenever an upstream test creates a streaming planner, and
loads the native library at that moment the way a TaskManager loads it once at startup, so no
upstream job pays the first-load latency inside its first native task; batch planners remain stock
Flink. The default run executes the planner module's unchanged `*ITCase`
runtime integration suite serially in one fork, then summarizes Surefire failures. Serial execution
keeps concurrently created MiniClusters from exhausting a developer machine or CI runner.

Selected upstream SQL tests also have **per-invocation native execution contracts**, declared in
`dev/flink-suite/agent/src/main/resources/native-execution.tsv`. The unchanged `CalcITCase.testNotIn`
must execute a native filter or Calc, and `testLongProjectionList` must execute a native Calc;
`AggregateITCase.testGroupByAgg` must execute a native grouped aggregate;
and `WindowDistinctAggregateITCase.testTumbleWindow`, `testHopWindow`, and `testCumulateWindow` must
execute either a single-phase native window aggregate or **both** native local and global halves
when the fixture's `splitDistinct` parameter is false.
Each parameter variant must satisfy its own contract, including backend, mini-batch, async-state,
and distinct-splitting variants selected by the pinned tests. The `splitDistinct=true` variants
explicitly require full fallback because attached-window aggregation needs two-phase execution.
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
`.flink-suite/native-execution/<suite>/` and is uploaded with the upstream CI log.
Each full suite also requires every method contracted for that suite to execute, so removing or renaming
an upstream test cannot silently shrink this coverage. Focused selections require evidence only
for their selected methods.

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
`python3 -m unittest discover -s dev/flink-suite -p 'test_*.py'`.

During development, select one or more Surefire test classes without changing the upstream checkout:

```bash
FLINK_SUITE_TEST='org.apache.flink.table.planner.runtime.stream.sql.CalcITCase,org.apache.flink.table.planner.runtime.stream.table.CalcITCase' bin/flink-suite.sh
```

The same `FLINK_SUITE_TEST` and `FLINK_SUITE_REUSE_BUILD=true` controls apply to `formats`,
`parquet`, `orc`, `kafka`, `paimon`, and `delta`. Reuse mode requires that the selected mode has been built once normally.

The focused Paimon coordinator run includes its four paged writer-restoration cases, three
commit-coordinator cases, and a deterministic primary-key write to verify native file creation:

```bash
FLINK_SUITE_TEST='org.apache.paimon.flink.CoordinatorCommitITCase,org.apache.paimon.flink.BatchFileStoreITCase#testWriteRestoreCoordinator*,org.apache.paimon.flink.ReadWriteTableITCase#testStreamingReadWriteWithPartitionedRecordsWithPk' \
  bin/flink-suite.sh paimon
```

The focused streaming dynamic-partition run uses Paimon's unchanged skewed-input SQL test,
alongside a primary-key write to satisfy the suite's two native-write checks:

```bash
FLINK_SUITE_TEST='org.apache.paimon.flink.AppendTableITCase#testPartitionDynamicStreaming,org.apache.paimon.flink.ReadWriteTableITCase#testStreamingReadWriteWithPartitionedRecordsWithPk' \
  bin/flink-suite.sh paimon
```

The Flink checkout remains byte-for-byte unchanged. Every push to `main` and every pull request
runs all eight upstream suites in GitHub Actions: planner runtime, formats, Parquet, ORC, Kafka,
Paimon, Delta and state/recovery. The weekly schedule and manual dispatch run the same complete matrix.
Each run rebuilds StreamFusion from that revision in the isolated suite directory and uploads
its complete build/test log with the commit SHA. These checks complement the released-artifact
SQL parity tests in ordinary CI; a passing local Maven suite alone does not establish upstream
integration compatibility.

Merges to `main` require **All CI tests** and **All upstream integration tests**, enforced by
the repository's **Require all test suites** ruleset with no bypass actors, including administrators.
The first check waits for Rust, Java/SQL parity, every format/connector module, both Paimon formats,
Delta and the deployed Flink image integration job. The second waits for all eight upstream suites.
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
Updating a pull request cancels its superseded upstream run so the current revision can start;
pushes to `main` retain their running upstream checks. Merge requirements always apply to the
pull request's current revision.

Before committing operator changes, run the relevant unchanged upstream integration classes
alongside the local SQL parity and recovery tests. Record the class selection and actual result
in the commit. A selected run is not the full upstream suite, and pending CI is not a passing result.
`FLINK_SUITE_REUSE_BUILD=true` reuses the existing StreamFusion binaries as well as Flink's;
omit it after source changes so the upstream tests execute the current implementation.

The validated Flink 2.2.1 baseline is 8,619 tests: 8,570 passed, 48 skipped by Flink, zero unexpected
failures or errors, and the one independently reproduced `CURRENT_DATE` xfail described above.
The format baseline is 185 tests: 175 passed and 10 skipped by Flink. The Kafka SQL baseline is 86
tests, all passed. The Parquet sink baseline is 8 tests, all passed, including the suite's explicit
proof that Flink instantiated the native Parquet writer. The complete Paimon baseline is 265
tests, all passed with native source sharing, including native append-write, primary-key-write,
and snapshot-merge markers. The complete-plan hook also passed 816 targeted Flink join, Calc and
JSON function cases.
The portable Delta SQL baseline is four tests, all passed: native write evidence covers 5,000
unpartitioned rows and 1,000 partitioned rows, with one explicit `TIME(0)` fallback contract.
The ORC Java-writer validation on September 14, 2026 passed all 46 unchanged Flink ORC SQL tests.
A targeted upstream Paimon run passed 22 continuous-read, partition-write and schema-change cases;
the [ORC page](connectors/orc.md#build-and-verification) distinguishes that run from local tests
that explicitly exercise ORC streaming.

The agent logs each unchanged `PrimaryKeyFileStoreTableITCase` invocation, its randomized table
defaults, and its completion, including the full exception on failure. Fatal MiniCluster errors
are printed immediately, even when upstream logging is disabled. If an invocation runs for two
minutes, it emits all JVM thread stacks to the suite log before CI's job timeout can discard the
active test's unwritten JUnit report.
Paimon also writes rolling cluster logs under `.flink-suite/diagnostics/paimon`; CI retains these
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
All invocations' reports contribute to the result and native execution checks; any Maven
failure remains blocking, including a process timeout without a finished JUnit report.
Explicit `FLINK_SUITE_TEST` selectors keep their requested grouping for diagnosis.

The upstream workflow caches Rust dependencies in the isolated source build's native target
directory, where this runner actually compiles them. It still rebuilds changed workspace crates.
The runner builds the complete native workspace once before packaging Java modules; Maven then
reuses those freshly built libraries instead of rebuilding Rust for each module's feature set.

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
