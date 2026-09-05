# Upstream Flink suite

StreamFusion can run Flink's own table planner runtime integration tests with native acceleration
installed in every streaming planner. This follows the purpose of DataFusion Comet's upstream Spark
SQL jobs while keeping Flink stricter: it uses an unmodified release tag and injects StreamFusion
only into the forked test JVM rather than copying or patching upstream tests.

Run the planner runtime suite from the repository root:

```bash
bin/flink-suite.sh
```

The same harness also runs Flink's unchanged format integration tests and the Kafka connector's
unchanged table SQL integration tests:

```bash
bin/flink-suite.sh formats
bin/flink-suite.sh parquet
bin/flink-suite.sh kafka
bin/flink-suite.sh all
```

`formats` covers Flink's JSON (including Debezium and Ogg CDC), CSV, Avro, and Protobuf integration
tests and compiles the Confluent Avro module (the pinned release contains no integration test in
that module). `parquet` runs Flink's unchanged `ParquetFsStreamingSinkITCase` and
`ParquetTimestampITCase`, and fails unless the suite proves that a native Parquet writer was
created. `kafka` covers `DynamicKafkaTableITCase`, `KafkaChangelogTableITCase`,
`KafkaTableITCase`, and `UpsertKafkaTableITCase` from the pinned Kafka connector release. The Kafka
suite starts broker containers and therefore requires a working Docker daemon. `all` runs formats,
Parquet, the planner runtime suite, and Kafka in that order.

The runner clones Flink `release-2.2.1` and Kafka connector `v5.0.0` under `.flink-suite`, verifies
that each checkout is clean, builds and installs StreamFusion and its supported format/connector
modules, and builds the required upstream reactors with tests skipped. A test-only
Java agent then installs StreamFusion whenever an upstream test creates a streaming planner; batch
planners remain stock Flink. The default run executes the planner module's unchanged `*ITCase`
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
replaced.

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
`parquet`, and `kafka`. Reuse mode requires that the selected mode has been built once normally.

The Flink checkout remains byte-for-byte unchanged. A scheduled and manually dispatchable GitHub
Actions workflow runs the same command, keeping the full compatibility suite out of the pull-request
critical path while still detecting upstream-contract regressions.

The validated Flink 2.2.1 baseline is 8,619 tests: 8,570 passed, 48 skipped by Flink, zero unexpected
failures or errors, and the one independently reproduced `CURRENT_DATE` xfail described above.
The format baseline is 185 tests: 175 passed and 10 skipped by Flink. The Kafka SQL baseline is 86
tests, all passed. The Parquet sink baseline is 8 tests, all passed, including the suite's explicit
proof that Flink instantiated the native Parquet writer.
