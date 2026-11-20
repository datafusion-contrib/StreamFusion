# Developer tools

`bin/` contains stable command entry points. Python implementations live in the importable
`dev.tools` package and their tests in `dev/tools/tests/`. No Python installation or third-party
Python dependency is required. Run commands from a source checkout; command entry points resolve
their own repository even when invoked from another working directory. Individual experiment
commands can still require a clean checkout, a particular working directory or a controlled host
as documented in their methodology.

These are development, validation and release tools. They are not part of a deployed Flink
distribution's `bin` directory. Optional profiling, historical comparisons and report validators
remain maintained inputs even when they are not regular CI steps.

## Ownership

| Location | Responsibility |
| --- | --- |
| `bin/*.py` | Compatibility commands forwarding arguments and exit status to one implementation |
| `bin/*.sh` | Stable build, deployment, suite and benchmark commands |
| `dev/tools/ci/` | Qualification policy, dependency preparation, transferable builds, coverage and scheduling |
| `dev/tools/checks/` | Deployment and upstream-classpath checks; expression-protocol generation |
| `dev/tools/suites/` | Released Hudi and Paimon compaction validation |
| `dev/tools/benchmarks/` | Local experiments, immutable historical-fixture support and result reporting |
| `dev/tools/profiling/` | CI scheduling analysis and explicit JVM/startup investigations |
| `dev/tools/tests/` | Tool unit and bounded subprocess regressions; no engine performance workloads |
| `dev/profiling/` | Java JFR/agent support, including `CiJfrEvents.java` |

Keep experiments with different input, measurement or validation contracts separate. Share only
mechanical process execution, paired order and timing; retain each experiment's test identities,
native witnesses, report schema and failure assertions. Report modules expose `report(root)` and
an explicit `main()`; importing them does not consume command-line arguments or write artifacts.

## Build, release and installation

| Command | Purpose and effects |
| --- | --- |
| `build-release.sh` | Build and stage selected Flink-line/native deployment payloads; needs Maven, Rust and the selected target toolchains; can download released dependencies |
| `check-artifacts.sh` | Check built JAR/native payloads and deployment boundaries, including glibc/TLS; needs archive/ELF tools for the selected platform |
| `package-release.sh` | Package existing release JARs and emit the archive path plus a SHA-256 file; invokes Maven for version metadata |
| `build-flink-image.sh` | Build the selected Flink image with Docker/buildx; can build missing release payloads and fetch image inputs; `--load` controls local image loading |
| `install-flink.sh` | Validate the selected official Flink distribution and copy loader/core JARs into its `lib`; modifies the destination installation |

See [releasing](releasing.md) and [deployment](deployment.md) for the supported invocation and
payload contracts. Packaging is distinct from building and validating; these remain separate
commands so existing payloads can be checked and packaged without rebuilding them.

## Build and compatibility checks

| Command | Purpose and inputs |
| --- | --- |
| `check-java-bytecode.py` | Inspect compiled class-file versions for Java compatibility |
| `check-native-glibc.py` | Check the glibc symbols of packaged Linux libraries; needs `readelf` |
| `check-native-tls.py` | Reject Linux JNI payloads requiring static TLS; needs `readelf` |
| `check-native-workspace.py` | Check Rust workspace/deployment boundaries and optional built-library exports; Cargo metadata may resolve dependencies |
| `check-flink-suite-classpath.py` | Validate identified source-suite JARs against the requested Flink line |
| `check-flink-suite-unshaded.py` | Validate prepared unshaded planner, bridge and parser JAR/POM inputs |
| `generate-expression-protocol.py` | Generate maintained Java/Rust protocol tags; `--check` verifies without writing source files |

The glibc and TLS commands retain their positional archive arguments. They are not general
`argparse` commands; passing no archives performs no archive inspection.

## CI and correctness suites

| Commands | Purpose and effects |
| --- | --- |
| `ci-campaign.py`, `ci-plan.py` | Verify/reuse exact-head qualifications and prove documentation-only reuse; use Git/GitHub CLI and repository history; campaign `start` dispatches workflows and `retry` retries failed jobs |
| `ci-maven-dependencies.py`, `ci_maven_cache.py` | Prepare released dependencies and verify/transfer a complete Maven cache; may download dependencies; preserve dependency-only retry rules |
| `ci_java_build.py`, `ci_native_source.py`, `ci_native_test_artifact.py` | Verify and transfer source-matched Java/native outputs; source verification can query/download GitHub artifacts |
| `ci_runtime_test_shards.py`, `ci_canonical_coverage.py`, `ci_canonical_jobs.py` | Partition unchanged selections, validate canonical inventories and run every selected worker; emit includes, manifests, logs and outcome audits |
| `ci_lake_tests.py` | Execute the selected lake module and retain separate Paimon format outcomes |
| `ci-free-disk.py` | Reclaim known unused toolchains only on GitHub-hosted Linux runners; explicitly restricted from local/self-hosted cleanup |
| `flink-suite.sh` | Run unchanged upstream Flink/connector suites; preparation clones pinned public source and builds artifacts; `config` only prints configuration |
| `canonical-savepoints.sh` | Run selected released canonical savepoint compatibility checks |
| `delta-legacy-audit.sh` | Audit the legacy Delta connector using pinned public source and released dependencies |
| `doris-suite.sh`, `hudi-suite.py`, `iceberg-suite.sh`, `paimon1-suite.sh`, `paimon-compaction-suite.py` | Connector-specific released correctness suites; require their documented JVM/native/service inputs and can prepare dependencies |

Read the [CI guide](ci.md) before qualification and the [upstream-suite guide](upstream-flink-suite.md)
before preparing source suites. Suite execution retains native/recovery/SQL evidence and
failure propagation. A test failure must still reach its outcome audit.

Minimal workflow checkouts must include the implementation behind a Python command. Cache keys
must hash the implementation. Hashing only a thin `bin/` entry point no longer identifies the executable input.

## Local benchmarks and evidence

| Command | Experiment or output |
| --- | --- |
| `bench-native.py` | Discover/run focused native Criterion or selected local benchmark harnesses; writes source/environment metadata and logs |
| `bench-jvm-aggregates.py` | Matched stock/native JVM aggregate measurements and validation |
| `bench-jvm-previous.py` | Compare immutable historical execution using unchanged fixtures in an explicitly selected disposable worktree |
| `bench-nested-scalars.py` | Compare stock, historical and candidate nested-scalar execution with matched fixtures |
| `bench-hudi-java.py` | Focused historical Java-only Hudi comparison with identical native inputs; temporarily replaces whitelisted production files, restores them on exit, and records hashes |
| `bench-hudi-matrix.py` | Validate complete Hudi query/engine/trial matrices or explicitly separate focused diagnostics |
| `bench-iceberg.sh`, `bench-paimon.sh` | Complete local matched connector jobs, including parity and native execution witnesses |
| `bench-iceberg-io.py` | Focused released Iceberg file-task comparison; separate from complete streaming-job results |
| `report-iceberg.py`, `report-paimon.py` | Validate paired observations and regenerate Markdown/CSV and the existing Paimon JSON audit; do not launch jobs |
| `collect-paimon-pruning-results.py` | Collect local pruning logs into structured observations |
| `export-criterion-comparison.py` | Export selected Criterion comparisons |
| `checkpoint-iceberg.py` | Resume complete-cell local Iceberg experiments and archive evidence; optional `--push REMOTE` commits and publishes completed cells |

Use an isolated worktree for any comparison that replaces source files, especially when multiple
agents share a repository. `bench-hudi-java.py` restores its selected files but still mutates that
checkout while measuring. Separate source, data and result directories for concurrent experiments.
Historical fixture copying lives in `dev/tools/benchmarks/test_sources.py`; missing or ambiguous
fixtures still fail before measurement.

Performance jobs remain local and require controlled resources, release builds, warmup, repeated
measurements, stock/reference parity and native witnesses. Keep all observations, including slow
trials. The [benchmark methodology](benchmarks.md) defines measurement boundaries; the
[evidence guide](benchmarks/evidence.md) defines versioned public archives and checksums.
Moving a report generator does not authorize deleting historical evidence.

## Profiling

| Command | Question answered |
| --- | --- |
| `profile-ci.py` | Actions dependency readiness, queue delay, overlap and total duration; reads GitHub metadata |
| `profile-runtime-ci.py` | Runtime test JVM/startup, fork, plan-cache and optional JFR comparisons with unchanged selected invocations |
| `profile-canonical-ci.py` | Canonical worker compilation, CDS, GC and fork-count experiments with unchanged recovery assertions |
| `profile-canonical-reference-ci.py` | Repeated stock-reference execution versus a shared reference with fixed handoff outcomes |
| `profile-ci-details.py` | Focused phase/JFR/recovery diagnostics for explicitly selected workloads |

JVM experiments invoke Maven/Java and write logs plus `summary.json`; JFR collection needs the
selected JDK and its Java helper. These are explicit diagnostics, distinct from end-to-end engine
performance benchmarks and ordinary correctness qualification. Scheduling analysis is not a
replacement for a JVM profile.

## Tool tests

From the repository root:

```sh
python3 -m dev.tools.tests --list
python3 -m dev.tools.tests
python3 -m unittest dev.tools.tests.test_report_tools
python3 -m unittest discover -s dev/flink-suite -p 'test_*.py'
```

The runner discovers every `test_*.py` module and fails on unknown exclusions, import errors,
empty modules or an empty selection. CI excludes only explicitly named tests already owned by
other build/compatibility phases. New tool test modules join the ordinary group automatically,
including benchmark result validators; they do not run the performance workloads.
Use JDK 17 for the complete tool test corpus and the upstream instrumentation agent; isolated
Paimon fork probes compile Java 17 fixtures. This does not change the Java 11 deployment target
of compatible engine modules. Some bounded tests resolve released Maven dependencies into fresh
repositories, and ELF-specific tests require Linux. Deployment checks that need built payloads
retain their existing release-stage placement.
