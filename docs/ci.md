# CI setup and failure handling

Ordinary CI runs Rust tests, Java tests on Flink 2.2 and
1.18, optional lake and format/connector suites, and optimized image integration tests.
The upstream Flink workflow runs separately; both aggregate gates must pass before merging.
Confirm the actual post-merge workflows as well as the PR checks when verifying a repair.

CI, upstream Flink, and Paimon compatibility workflows cancel older test runs when a newer
push or PR update targets the same ref. The latest main commit includes the earlier changes
and receives the complete checks, without waiting behind tests for superseded main commits.
Release publishing retains its non-canceling concurrency policy.

The ordinary CI and upstream Flink aggregate gates run after successful or failed jobs,
but skip canceled workflows (`always() && !cancelled()`). An unconditional `always()`
gate can wait for another runner after the obsolete test jobs have been canceled,
keeping the workflow's concurrency slot occupied and the replacement run pending.
Failures and unexpected skipped dependencies still fail the aggregate's result checks.

## Native execution checks instead of exhaustive benchmark smoke

Ordinary CI does not run the exhaustive native Criterion fixture suite or require new
Criterion coverage for every native change. The suite did not enforce timing regressions;
the successful main run on 2026-10-20 spent about 135 minutes on its release fixture job.
Removing that job also removes its release-cache restore/save, artifact upload and CI-only
fixture sharding. Existing Criterion suites remain available for manual performance investigations;
there is no replacement scheduled benchmark job.

The [upstream Flink workflow](upstream-flink-suite.md) remains required on both Flink lines.
It runs unchanged upstream assertions and validates declared execution contracts against
positive native work. Missing evidence, zero native work and unexpected fallback fail the
applicable checks. The runtime audit additionally enforces declared routes for specific test
variants, and the aggregate gate requires all planned runtime shards and invocations.
When native support changes, update those expectations alongside the upstream coverage.
These checks establish correct results and the expected execution path; throughput claims
still require explicit release-mode comparisons against stock Flink and previous behavior.

## Shared debug native libraries

Ordinary CI builds all workspace debug native libraries once in a dedicated Ubuntu 24.04
job. That job verifies the package dependencies and JNI export boundaries, then uploads only
the shared libraries as a commit-specific artifact. The four Java and eleven lake-module
test jobs download those libraries into `native/target/debug` and pass
`-Dnative.build.skip=true` to Maven. Both JDKs and Flink lines exercise the same native build;
each consumer still compiles its selected Java reactor and runs its existing tests. A failed
producer blocks the consumers and fails the aggregate CI gate. Consumers have no fallback to
an older cache or a local native rebuild.

The producer is the only writer of the `debug-jvm` dependency cache. Consumers transfer final
libraries rather than restoring Cargo intermediates or installing a Rust toolchain. The Rust
test job separately compiles its test harnesses and checks every tagged native package in
isolation once, preserving the checks for dependencies accidentally supplied by the workspace.
The native build retains Cargo's `--timings` report as a separate diagnostic artifact.

Optimized image builds and the separately prepared upstream
Flink suite keep their own build configurations. The debug artifact is scoped to its current
workflow run and commit; it is not a release payload or a cache of test outcomes. This replaces
the independent debug library builds with one without changing test selection or test concurrency.
Actual elapsed-time savings must be measured in GitHub Actions after this change runs.

All Rust workflows, including the consolidated JVM aggregate and nested-scalar diagnostics,
use the local `setup-rust` action to install Rust 1.94.0 and remove the
runner image's unused `stable` toolchain before restoring caches. The Rust cache action hashes
every installed compiler, not just the selected compiler. Previous logs showed the unused
hosted toolchain changing from 1.99.0 to 1.99.1 while the project stayed on 1.94.0, splitting
the same debug dependency cache into two approximately 2.43 GiB entries. Removing the unused
toolchain prevents those image updates from invalidating the pinned build's cache; dependency,
compiler-option, target and actual project-toolchain changes still affect the cache keys.

## Connector and format test coverage

The full Java suite includes every connector and format tag on both Flink lines and both
JDKs. Its default Surefire execution runs untagged tests and every tag except ORC; a second
execution runs ORC with its isolated protobuf classpath. These same tests do not need six
additional tagged-module jobs. Standalone native package checks remain in the Rust job.
`bin/test_ci_module_groups.py` exercises the actual runtime Surefire configuration with one
fixture for every declared module tag and one untagged fixture, requiring each to run once.

## Consolidated feature validation

Keep related coverage and default-enable changes on one PR while repairing their combined CI.
A newer PR head cancels obsolete runs, including JVM aggregate release diagnostics; correctness
checks and diagnostic fixtures still run on the current head. Superseded PRs should be closed
and their active runs canceled rather than leaving multiple copies queued.

JVM aggregate and nested-scalar artifacts use fresh runner temporary directories,
outside the cached Cargo target. Restored dependency builds must not restore candidate-library
snapshots or old trials into a new run. Artifact uploads retain these temporary outputs even
when a diagnostic fails.

Paimon 1.0 compatibility builds release workspace libraries once with `mimalloc`, matching
the reactor's `bench` profile, and verifies JNI/package boundaries before Maven runs. Both
the source/sink parity phase and the upstream SQL phase pass `-Dnative.build.skip=true`
and reuse those libraries. Previously each Maven module invoked Cargo with a different
package selection: the successful main run on 2026-10-20 performed seven release builds,
recompiling DataFusion three times. The test selections, JVM limits and single-fork SQL
execution stay the same. Cargo's compilation timings are retained with the test evidence.

Default-enabled paths need positive native-route assertions. Fixed-size-binary compatibility
tests construct that legacy physical schema explicitly rather than changing global feature
flags. Component-timestamp extrema exercise checkpoint/retraction values on RocksDB, including
cases that previously asserted fallback. Hidden-offset recovery expectations distinguish heap
row mutation from serialized state copies; the released-Flink SQL controls remain authoritative.
Nested connector schemas must exclude internal ROW transport fields while retaining user fields,
nulls, logical ordering and independently owned Arrow buffer references.

## Reuse SQL results across parity assertions

`NativeParity.assertOrderedKindedParity` checks the ordered changelog, including row kinds,
and folds those same collected rows to compare the final materialized multisets. It executes
one stock job and one native job and retains the native-substitution assertion. Six SQL
suites reuse these results instead of launching an identical pair solely for the materialized
comparison. The variable-bound update-fast Top-N suite alone avoids 100 duplicate job
submissions across 50 cases.

Separate mini-batch configurations, recovery runs and unordered-only comparisons keep their
own executions. In particular, the updating-limit ranked mini-batch case still runs its
materialized comparison when ordered parity does not apply. Standalone materialized checks
continue to fold the result iterator as a stream, without buffering an entire changelog.
The existing job matrix and Surefire fork counts stay the same.

## MiniCluster reuse within a test JVM

A JUnit launcher-session listener owns the automatic StreamFusion test MiniCluster.
Every test class in the same Surefire fork reuses that cluster, including when Surefire
launches a separate JUnit root for each class. Each test still gets a fresh
execution-environment binding, which is cleared afterwards; Flink 1.18 also binds and clears
its batch test context. The launcher closes the cluster and its temporary directories when
its session finishes, including after test failures. Surefire forks retain their independent clusters;
JUnit tests remain serial within each fork, and the existing fork counts do not change.

After each test, the extension still checks live JNI handles, Arrow allocator bytes and
TaskManager pool reservations. At each class boundary it cancels unfinished jobs and waits
for terminal results before another class uses the cluster. Tests that need a private cluster
for memory budgets, metrics, checkpoint recovery or remote deployment keep their existing
private fixtures. Each class restores the shared default memory budget before running, so
a private budget test cannot change later operator-harness defaults. Existing test selections
and production execution behavior remain the same.

A lifecycle regression executes a failing class that leaves a host job running, then proves
that a second class sees the same cluster, the abandoned job is canceled and a new bounded
job returns the expected rows. It also verifies that closing the launcher session stops
the cluster.
The regression uses separate JUnit roots in one launcher session, matching reused Surefire
forks; root ownership alone would restart the cluster between those classes. The fixture
continues to use Flink's released MiniCluster resource and test-environment APIs
on both supported lines.

A local paired check of 66 tests in 19 lightweight operator classes reduced the Surefire
phase from 5.514 s to 4.344 s with one fork and identical release libraries. A two-fork
check confirms that ordinary classes start only one cluster per fork, rather than one per
class; the lifecycle regression deliberately opens one additional independent launcher
session. This removes repeated fixture startup, but full-workflow savings still need CI
measurement.

Two-fork compatibility checks pass 304 cases on each Flink line, retaining the existing
rescaling skips (two on each line) and ten scalar-key capability skips on Flink 1.18.
These cover typed SQL keys, checkpoint/rescale recovery, native initialization failures,
custom memory budgets, metrics and separate task-JVM deployment. The launcher-session
regression also restores the default memory budget after a failing class changes it,
preserving later operator-harness defaults.

## Fluss SQL fixture lifetime

Each Fluss SQL test invocation keeps one stock and one native table environment for its
DDL, reads and inserts. `@AfterEach` attempts to close both catalogs and clears the cache,
including when the test fails. Parameterized invocations receive fresh environments.
Job completion, result iterator closure, discovery-job cancellation and native-memory checks
retain their existing boundaries. This uses the existing Java and image jobs and fork counts.

Previously every SQL helper created and closed a catalog. Phase timings and thread dumps
identified the Fluss client's two-second Netty shutdown quiet period on each close, including
catalog and source-enumerator cleanup. In a paired local JDK 17 comparison using the same
release libraries, the unchanged plain bucket/statistics case fell from 44.149 s to 21.642 s
(51% less time); ten read/insert catalog closes alone accounted for 20.104 s before reuse.
This measures test setup and cleanup overhead, not connector throughput. Production Fluss
connection lifetimes and the Nexmark measurement harness are unchanged.

The complete 29-case suite passes without skips on Flink 2.2 and 1.18, including schema
evolution, expected stock failures, live partition discovery and native-memory cleanup.
Local JDK 17 checks using release libraries took 311.374 s and 298.023 s respectively;
the two compatibility checks ran concurrently in isolated worktrees, so these are validation
timings rather than a paired full-suite speedup measurement. The existing CI matrix measures
the effect on both JDKs and the optimized image job.

## Maven dependencies

The upstream execution-evidence agent is a separate JDK 17 development tool. Its standalone
POM pins the compiler plugin to 3.13.0 so Maven 3.8 also honors `maven.compiler.release=17`
instead of inheriting a compiler that defaults to unsupported Java 5 source and target levels.

Java CI jobs resolve their selected reactor's dependencies before building or testing:

```sh
python3 bin/ci-maven-dependencies.py --projects streamfusion-runtime
python3 bin/ci-maven-dependencies.py --profile flink-1.18 --projects streamfusion-runtime
```

The helper invokes the released Maven dependency plugin's `go-offline` goal, which resolves
project, plugin and report dependencies without invoking a build or test lifecycle. It keeps
the job's profiles, project selection and reactor dependencies. The settings file uses the
project's public repositories without adding mirrors or changing dependency versions.

Resolution has three attempts, with waits of 5 and 15 seconds and a ten-minute limit per
attempt. Each attempt uses Maven's `-U` option to recheck missing releases, including a lookup
that failed during a previous attempt. Persistent download failures, invalid dependencies and
timeouts remain blocking. Once resolution succeeds, the existing build and test commands run
once with their original assertions and failure handling. Tests are never retried by this helper;
their commands still use normal Maven resolution for any artifacts requested dynamically by plugins.

[PR #311's post-merge row-format job](https://github.com/datafusion-contrib/StreamFusion/actions/runs/37938400080/job/113846210991)
failed before compilation because one runner reported published Flink 2.2.1 and Kafka dependencies
as missing. Other jobs resolved those releases successfully. Separating downloads from tests
allows transient resolution failures to recover without rerunning test code; forcing missing-release
checks prevents Maven's failed-lookup cache from defeating the next setup attempt.

The upstream suite transfers its prepared pinned Maven distribution to its consumer runners
and verifies that it starts with downloads blocked. See the
[shared-build guide](upstream-flink-suite.md#shared-builds-and-runtime-shards).
