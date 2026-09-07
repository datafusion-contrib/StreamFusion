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

## Installation audit and exit status

The suite agent targets the pinned release's `DefaultPlannerFactory` and `DelegatePlannerFactory`
and checks that they declare the expected `create(Context)` method. Transformation records are kept per
classloader. A successful return from `NativePlanner.install` is recorded for that table
configuration; entering factory advice alone is not sufficient. An independent constructor check
requires each eligible streaming table environment to have completed installation. Batch/automatic
mode and the existing unmodified JSON-plan/test exclusions remain exempt. Merely loading a valid
factory without creating an environment is allowed. Unknown factory implementations are not
automatically supported: an eligible environment created without installation fails the audit.

Instrumentation failures, caught installation errors, and eligible environments without installation
remain fatal even if another configuration or classloader succeeds later. This proves installation,
not that every query used a native operator; ordinary planner fallback still applies. The audit
relies on the supported factory methods and table-environment constructor contract remaining visible.
Changes to both observation points require new compatibility validation.

Each runner invocation uses a fresh audit directory. Each agent JVM writes a `.pending` receipt
at startup and replaces it with `.ok` or `.failed` at shutdown; failed audits exit with code 70.
A late audit failure invalidates a prior success receipt and terminates the JVM. The runner
rejects missing, failed, or incomplete receipts and Surefire dump files. It never turns
a nonzero Maven exit into success because the test XML looks clean. Surefire's test-failure-ignore
option lets the XML summarizer apply the named expected-failure allowlist without suppressing
unexpected assertions/errors; fork failures, dumps, and incomplete audit receipts remain failures.
Receipts are retained beneath `.flink-suite/audit-<mode>.*` for diagnosis.

PR CI packages the agent and runs three real Surefire 3.2.2 process regressions through the
launcher: an abnormal exit after passing XML and a successful audit, an abort before audit
completion, and an allowlisted assertion failure. No Rust build or MiniCluster is required.

```sh
mvn -f dev/flink-suite/agent/pom.xml clean package
python3 -m unittest discover -s dev/flink-suite -p 'test_fork_exit.py'
```

These regressions test exit propagation, not the full Flink installation path. Use the upstream
integration suite to validate the supported planner hooks. Temporary projects and reports are
isolated; released Maven dependencies share the normal local cache.

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
