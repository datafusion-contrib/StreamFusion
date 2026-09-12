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
unchanged table SQL integration tests, and Paimon's unchanged append-table SQL integration tests:

```bash
bin/flink-suite.sh formats
bin/flink-suite.sh parquet
bin/flink-suite.sh kafka
bin/flink-suite.sh paimon
bin/flink-suite.sh all
```

`formats` covers Flink's JSON (including Debezium and Ogg CDC), CSV, Avro, and Protobuf integration
tests and compiles the Confluent Avro module (the pinned release contains no integration test in
that module). `parquet` runs Flink's unchanged `ParquetFsStreamingSinkITCase` and
`ParquetTimestampITCase`, and fails unless the suite proves that a native Parquet writer was
created. `kafka` covers `DynamicKafkaTableITCase`, `KafkaChangelogTableITCase`,
`KafkaTableITCase`, and `UpsertKafkaTableITCase` from the pinned Kafka connector release. The Kafka
suite starts broker containers and therefore requires a working Docker daemon. `paimon` runs the
Paimon Flink connector's `AppendOnlyTableITCase`, `AppendTableITCase`, `BatchFileStoreITCase`,
`ComputedColumnAndWatermarkTableITCase`, `ContinuousFileStoreITCase`, `ReadWriteTableITCase`,
`PrimaryKeyFileStoreTableITCase`, `CompositePkAndMultiPartitionedTableITCase`,
`FullCompactionFileStoreITCase`, `FlinkJobRecoveryITCase`, `RescaleBucketITCase`,
`ScanBucketITCase`, `KeyOnlyDeletesITCase`, `FirstRowITCase`, and `CoordinatorCommitITCase` from the
pinned Paimon release,
built against the suite's Flink version, and fails unless it proves both that a streaming insert
wrote an append-table data file from a native Arrow bundle and that one wrote a primary-key
level-0 file natively. The native-write markers are emitted only after the write returns successfully.
Streaming inserts covered by the [Paimon connector whitelist](connectors/paimon.md)
can take the native sink, including coordinated writers, coordinator commits, and dynamic
partition routing. Batch inserts,
unsupported primary-key options, and compaction rewrites use stock Paimon. `CoordinatorCommitITCase`
checks removal of the global committer, coordinator metrics, committed rows, and snapshot watermark
parity for active and idle inputs. Because
Surefire appends StreamFusion's classpath in no fixed order, the agent also resolves Paimon's
`parquet` format identifier to the StreamFusion factory whenever the module is present, standing in
for the `01-streamfusion-paimon.jar` ordering a deployment relies on. Paimon's module declares the
planner test-jar before the planner itself, which would place stock Calcite ahead of Flink's patched
validator classes (breaking `CALL` procedures and time travel in stock tests), so the runner drops
the resolved calcite-core from that module's test classpath and appends it after the planner
instead. `all` runs formats, Parquet,
the planner runtime suite, Paimon, and Kafka in that order.

The runner clones Flink `release-2.2.1`, Kafka connector `v5.0.0`, and Paimon `2.0.0` (its
`release-2.0.0-rc10` tag) under `.flink-suite`, verifies that each checkout is clean, builds and
installs StreamFusion and its supported format/connector modules, and builds the required upstream
reactors with tests skipped. A test-only
Java agent then installs StreamFusion whenever an upstream test creates a streaming planner, and
loads the native library at that moment the way a TaskManager loads it once at startup, so no
upstream job pays the first-load latency inside its first native task; batch planners remain stock
Flink. The default run executes the planner module's unchanged `*ITCase`
runtime integration suite serially in one fork, then summarizes Surefire failures. Serial execution
keeps concurrently created MiniClusters from exhausting a developer machine or CI runner.

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

During development, select one or more Surefire test classes without changing the upstream checkout:

```bash
FLINK_SUITE_TEST='org.apache.flink.table.planner.runtime.stream.sql.CalcITCase,org.apache.flink.table.planner.runtime.stream.table.CalcITCase' bin/flink-suite.sh
```

The same `FLINK_SUITE_TEST` and `FLINK_SUITE_REUSE_BUILD=true` controls apply to `formats`,
`parquet`, `kafka`, and `paimon`. Reuse mode requires that the selected mode has been built once normally.

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

The Flink checkout remains byte-for-byte unchanged. A scheduled and manually dispatchable GitHub
Actions workflow runs the same command, keeping the full compatibility suite out of the pull-request
critical path while still detecting upstream-contract regressions.

The validated Flink 2.2.1 baseline is 8,619 tests: 8,570 passed, 48 skipped by Flink, zero unexpected
failures or errors, and the one independently reproduced `CURRENT_DATE` xfail described above.
The format baseline is 185 tests: 175 passed and 10 skipped by Flink. The Kafka SQL baseline is 86
tests, all passed. The Parquet sink baseline is 8 tests, all passed, including the suite's explicit
proof that Flink instantiated the native Parquet writer. The Paimon append-table baseline is 200
tests, all passed, including the proof that a streaming insert wrote a native Paimon bundle.
