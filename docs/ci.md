# CI setup and failure handling

Contributor builds and tests run in **personal forks**, using each fork owner's GitHub Actions
quota instead of the shared `datafusion-contrib` quota. Opening or updating a PR does not launch
the test matrix in the canonical repository. Source-changing canonical `main` pushes still run
the complete post-merge checks, the upstream Flink weekly run remains available, and publishing stays upstream.

## Contributing from a personal fork

1. Fork `datafusion-contrib/StreamFusion` into your personal account. If that name is already
   occupied by an unrelated repository, choose another fork name.
2. Enable workflows in the fork's **Actions** tab before pushing your branch. GitHub disables
   Actions in new forks until the owner enables them.
3. Keep `upstream` pointed at the canonical repository and add your fork as `fork` (or use it
   as `origin`). Fetch upstream and rebase your feature branch on current `upstream/main` so it
   contains these workflow changes. Push the feature branch to your fork, then open a PR into
   `datafusion-contrib/StreamFusion:main`.
4. Wait for **CI** and **Upstream Flink suite** on the exact branch head to pass, plus any
   path-selected compatibility, diagnostic or documentation workflows. Include fork run links
   in the PR so reviewers can inspect jobs and artifacts directly. Rebase and rerun after
   incorporating new upstream changes before merging.

For example, after adding the fork remote:

```sh
git fetch upstream
git rebase upstream/main
git push -u fork HEAD
gh pr create --repo datafusion-contrib/StreamFusion --base main --head YOUR_LOGIN:YOUR_BRANCH
```

Pushes to any branch in a fork run CI and the upstream Flink suite. Paimon and release diagnostics
retain their path filters; docs changes build the strict MkDocs site in the fork. Branch pushes
in the canonical repository skip those jobs unless the branch is `main`; development branches
belong in personal forks. Tags do not trigger these branch test workflows. Manual CI dispatches
are available for retries. For the upstream Flink merge gate, use or rerun a **push** run: a manual
SQL-inventory dispatch may execute only part of the matrix and is never accepted as full-suite evidence.

The lightweight **Fork CI status** workflow reads public fork runs on PR updates and watches
unfinished runs every two minutes. Scheduled and manual refreshes remain available in the
canonical repository. It reports the existing **All CI tests** and
**All upstream integration tests** checks for the exact PR head SHA and branch. It also mirrors the
CI result under the legacy **Rust tests** and **Java tests (debug native build)** names required by
the older ruleset. Each mirrored CI check requires the entire CI workflow to succeed. Failed,
canceled, skipped or neutral workflows cannot satisfy the gates; missing or disabled workflows
report action required. Existing GitHub rulesets stay in place. The reporter checks out only
trusted default-branch code and never executes PR code with its write-capable token.

Each mirrored check includes individual job links, start delays and execution times from the
fork. Start delay includes prerequisite builds as well as runner queueing; it is not labelled
as pure runner wait. Queued jobs do not acquire a fictitious execution time from GitHub's
preallocated start timestamp. The ordinary Java jobs are named **Runtime, connectors and
formats** because their selection includes all three; Doris's real-server suite remains a
separate job. Full workflow results continue to control the required gates.

GitHub cron is best effort: a requested fifteen-minute schedule can be delayed or dropped for
hours. Fork workflow completions do not trigger `workflow_run` in the canonical repository,
so a single PR-event refresh followed only by cron can leave successful tests displayed as
pending. The schedule requests minutes 7, 22, 37 and 52 to avoid the busiest quarter-hour
boundaries; active-run tracking does not depend on that schedule.

The reporter uses a partial clone and sparse checkout of its script and test file. These
two files total about 42 KB, compared with 323 MB of tracked blobs in the November 7 tree.
It still executes only trusted default-branch code. This avoids downloading benchmark archives
and SQL inventory reports for every watcher; it also reduces disk work on the slim runner.

Each watcher uses a short `ubuntu-slim` job and dispatches a follow-up on the trusted default
branch before its six-minute polling window ends. Continuations retain the original six-hour
deadline. They fetch the current PR head on every poll and stop when both suites are terminal
or the PR closes. Unchanged results do not generate repeated check writes. If a previously
completed check needs to show an active fork run, the reporter creates a new check attempt;
requeuing the completed check can retain its old conclusion. An active attempt can pass only
after the exact-head workflow succeeds. This also prevents an earlier successful attempt from
satisfying the gate during a rerun. A brief startup grace
allows fork runs to appear after a PR update; absent/disabled workflows still report action
required. Temporary network/server failures retry within the same deadline; permission failures
remain visible. Deadline exhaustion fails the watcher without passing unfinished checks.

The reporter's `actions: write` permission is used to dispatch only this trusted reporter workflow
with a PR number and deadline; no contributor workflow is dispatched and no personal access token
or fork secret is required. Per-PR concurrency queues continuations instead of canceling their
parent. Lightweight canonical runner time is consumed while watching; expensive builds and tests
remain in the personal fork. Runner queue delays can still postpone a refresh, so the two-minute
poll interval is not a guaranteed delivery time.

To refresh and watch a particular PR, including a manually rerun fork suite:

```sh
gh workflow run fork-ci-status.yml --repo datafusion-contrib/StreamFusion -f pr_number=123
```

Omit `pr_number` to refresh every open PR and start watchers for unfinished suites. The
`watch_until` input is for internal continuation; leaving it empty starts a new watch deadline.
The reporter does not rerun tests, weaken required checks or accept partial upstream inventory
runs as complete coverage. Its polling and continuation tests run in ordinary fork CI without
a write-capable token.

Reporter-created checks carry a `streamfusion:fork-ci:v1:` external ID scoped to the PR,
workflow and check name. The reporter searches every check-run page for that marker and the
GitHub Actions application before updating a check. Ordinary workflow jobs and older unmarked
mirrors are never adopted, even when their names match; a new marked mirror is created instead.
If one PR cannot be refreshed, the reporter continues visiting the others, then fails the workflow
with the affected PRs and API endpoints in its job summary. This does not change the exact-head
test requirements or turn missing, failed or unmigrated fork results into success.

Existing PRs need the new workflow configuration on their feature branch. For a PR whose head is
already in a personal fork, rebase on current upstream `main`, enable Actions and push again. For
a PR whose head is a canonical-repository branch, push the rebased branch to your fork and open a
replacement PR referencing the original, then close the original. GitHub cannot change an existing
PR's head repository. The reporter marks same-repository PRs as requiring migration rather than
running their expensive jobs upstream. Runs queued before this change are not automatically moved
or canceled; cancel obsolete PR runs in the canonical Actions tab when migrating them.

CI, upstream Flink, and Paimon compatibility cancel older push runs on the same branch. The latest
main commit receives complete checks, without waiting behind superseded main commits. Release
publishing retains its non-canceling concurrency policy.

Docs builds have read-only repository permissions. Only canonical `main` uploads a Pages artifact
and deploys, with Pages/OIDC permissions confined to deployment. Forks build without publishing.
Fork docs pushes cancel obsolete builds; canonical main deployments remain serialized. Release
tag validation and all dependent release publishing jobs are also restricted to the canonical repo.

The ordinary CI and upstream Flink aggregate gates run after successful or failed jobs,
but skip canceled workflows (`always() && !cancelled()`). An unconditional `always()`
gate can wait for another runner after the obsolete test jobs have been canceled,
keeping the workflow's concurrency slot occupied and the replacement run pending.
Failures and unexpected skipped dependencies still fail the aggregate's result checks.

## Documentation-only pushes

Both required workflows first compare committed paths with a candidate baseline. Only
`readme.md`, Markdown under `docs/`, and `docs/benchmarks/ci-profile-YYYYMMDD.json` timing
evidence qualify. Source, tests, workflows, dependency/build configuration, instruction files,
other data files and unknown paths require the full matrix. Rename detection is disabled so
moving a source file into `docs/` still counts its deletion from source.

For an existing branch, the candidate is the exact pre-push commit on that branch. For a new
branch, it is current canonical `main`. Both **CI** and **Upstream Flink suite** must have a
completed successful **push** run for that exact baseline SHA and branch. A newer failed,
canceled or running push prevents reuse; manual inventory runs cannot establish coverage.
Missing history, unavailable API access, missing workflows or incomplete/failed evidence all
select the full matrix. Manual dispatches and weekly schedules always run the full selection.

When those proofs pass, the documentation push has unchanged tested source. Its native and
integration jobs are deliberately skipped, and both aggregate checks succeed on the new head
only when the plan succeeds and every test dependency is skipped. Strict docs builds still
run. The normal path still requires every test dependency to succeed. This avoids hours of
retesting unchanged source without letting a docs follow-up hide failures on its base commit.
The plan uses a read-only token and sparse checkout of its script; its job summary records the
decision and baseline SHA. Real Git rename/deletion cases and the actual aggregate shell
scripts are covered by regressions, including unexpected skips, failures and unproven plans.

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

The nested-scalar release comparison pins a historical runtime whose Calc metrics use
`NativeCalcExecNode[...]`. Its Rust arithmetic control must show positive consumed
rows under that historical name. Candidate checks retain the detailed `NativeCalc(...)`
name, and fallback controls require zero native operators. This compatibility changes
only metric-name recognition; workloads, timing and the pinned baseline stay unchanged.

## Shared debug native libraries

Host and Darwin release builds compile the selected native packages together in one Cargo
invocation per target, then stage their individual libraries. Previously the host-only image
path invoked Cargo separately for every package on each Flink line, allowing different feature
graphs to rebuild shared dependencies. Package selection, mimalloc, platform staging and JNI
export checks are unchanged. The shell regression verifies both Flink lines, both Darwin
architectures, every staged library and propagation of native build failures. This removes
repeated compiler invocations; hosted elapsed-time improvement still needs measurement.

Ordinary CI builds all workspace debug native libraries once in a dedicated Ubuntu 24.04
job. That job verifies the package dependencies and JNI export boundaries, then uploads only
the shared libraries as a commit-specific artifact. The eight Java and eleven lake-module
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
The Rust test job reads that same dependency cache instead of maintaining another copy;
it still compiles and runs its own harnesses and standalone package checks.

The Rust test job runs the same 30 GiB free-space preflight as the shared native build,
before restoring Cargo caches. The post-merge run for #595 exhausted its runner's disk
while compiling tests; the runner itself crashed before it could upload a test result.
The preflight removes only unused hosted-runner toolchains and fails early if it cannot
provide enough initial capacity.

Optimized image builds and the separately prepared upstream
Flink suite keep their own build configurations. The debug artifact is scoped to its current
workflow run and commit; it is not a release payload or a cache of test outcomes. This replaces
the independent debug library builds with one without changing test selection or test concurrency.
Actual elapsed-time savings must be measured in GitHub Actions after this change runs.

Personal-fork branch pushes also save their Cargo dependency caches and prepared pinned Flink
checkout/Maven caches. Restricting saves to `main` left normal fork development branches unable
to warm their own caches for subsequent pushes. GitHub keeps those caches scoped to the branch;
canonical development branches remain read-only and canonical `main` retains its normal writes.
Native libraries and test results still come from the exact current commit, not the dependency
cache. The debug-library producer remains the only writer of its shared cache.

Iceberg Rust 0.11 requires Rust 1.95, so the shared toolchain is 1.95.0.
All Rust workflows, including the consolidated JVM aggregate and nested-scalar diagnostics,
use the local `setup-rust` action to install Rust 1.95.0 and remove the
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

## Java test memory budget

Each ordinary Java matrix job runs at most two Surefire forks at a time with a 4 GiB heap each,
including the second JVM-boundary/cache verification phase. CI disables fork reuse, so each
test class gets a fresh JVM. Local Maven defaults keep fork reuse enabled; the CI invocation
sets `-Dsf.testForks=2 -Dsf.testReuseForks=false` explicitly. The default 8 GiB per-fork heap
combined with CPU-based fork counts leaves insufficient predictable headroom for Arrow buffers, native
operators, embedded clusters and the Maven process on hosted runners.

Released Flink MiniClusters keep completed execution graphs and serialized results in an
unbounded in-memory archive. Its JVM shutdown hook can retain this archive even after the
cluster stops, so closing private clusters or rotating the shared cluster does not reliably
release their results inside a reused JVM. A fresh process per class bounds this accumulation
without reflecting into Flink internals or evicting results during a scenario. Classes retain
their shared launcher-session cluster and all within-class lifecycle, failure and recovery checks.
The tradeoff is additional JVM and native-library startup per class;
complete CI elapsed time, rather than local function throughput, measures that overhead.
The 4 GiB per-fork cap also leaves room for the temporary copies made while checkpointing and serializing
large binary collection results. The two configured heaps total at most 8 GiB; Arrow/native
allocations, Maven, containers and the operating system still need separate headroom.
A local JDK 11/Flink 1.18 diagnostic ran the same 447 cases without skips or failures in
354.31 seconds with one fork and 239.08 seconds with two. Both runs use fresh processes per
class and the same debug native library, matching CI's execution mode. This single pair is
not a full hosted-CI speedup or memory qualification; the retained
[diagnostic and scheduling model](benchmarks/ci-latency-20261106.json) states its limits.

The complete test selection, both Flink lines and both JDKs remain required. Tests stay
serial within the fork, and result, native-route, leak and recovery assertions are unchanged.
Runtime and lake matrices stop their remaining jobs after a job fails. A failed head cannot
merge; a subsequent fixed head still requires every matrix member to succeed. This releases
runners promptly instead of completing an already-failed matrix, and adds no test retries.
The Java suite logs host memory/swap/CPU counters with `vmstat` every 30 seconds, so a runner
shutdown retains resource evidence in the job log even when artifact upload cannot run.
A shutdown signal alone does not prove a JVM or host out-of-memory condition; inspect these
counters and any JVM diagnostics before assigning a cause. The job timeout remains 150 minutes.

## Ordinary runtime test shards

Each Flink/JDK pair runs three Java jobs. The complete reactor is compiled first with tests
deferred; `bin/ci_runtime_test_shards.py` then partitions the compiled runtime classes matching
Surefire's default `Test*`, `*Test`, `*Tests` and `*TestCase` names. Recorded class durations
from `dev/ci/runtime-durations.json` place the longest classes first into the least-loaded
partition. New classes receive a one-second estimate; timing estimates affect placement only,
never selection. Nested classes stay with their outer class,
and all parameterized invocations stay together. Empty inventories, empty shards and invalid
indices fail before test execution rather than restoring an unfiltered Surefire run.

The `sf.runtime.testIncludesFile` property activates a runtime-only Maven profile with that
shard's includes file. Both existing Surefire executions retain their ordinary/ORC tag filters and classpaths.
The three main-suite selections therefore cover the same runtime tests once per Flink/JDK pair;
the additional JVM-boundary/cache phase still runs once on JDK 17 for each Flink line, in
shard 1. Deployment-module tests and bytecode/package checks run in shard 1 of every Flink/JDK
pair. Shards 2 and 3 still build the complete reactor, but use `skipTests=true` with
`sf.runtime.tests.skip=false`: the runtime's assigned classes run and downstream module tests
are deferred to shard 1. An actual multi-module Maven regression checks all three selections.
This preserves the reactor's assembly order and module-owned test selections. Local `mvn test`
does not activate this profile.

The three classes producing the required portable, recovery and rescale SQL audit JSON files
stay in shard 1, whose upload still fails if those files are absent. A regression checks that
the artifact producers match this pinned set. The recursive scalar, mixed scalar/container,
recursive table-function and nested table-function bridge matrices also stay in shard 1.
On the first recursive-scalar CI run, these four suites took 17.4 minutes together on
Flink 2.2/JDK 11 in shard 2. That shard completed its 10,341 runtime cases without failures,
then reached the 150-minute job limit during the later connector tests, while shard 1
finished in about 115 minutes. Moving those four complete classes initially balanced the
two partitions without reducing their test selection or raising the timeout.
As the suite grew, run `42014164780` at `01c3c1418b55` reported 8,704 ordinary runtime
cases (36 existing skips) and 24 ORC cases in Flink 2.2/JDK 11 shard 1 without failures or errors, but reached
the same limit during the subsequent Fluss tests. The runtime phase alone took about
141 minutes; the other seven Java jobs passed. Three partitions now distribute the complete
runtime inventory across more jobs, leaving room for the remaining reactor modules and
JDK 17 boundary checks under the unchanged timeout.
The [retained scheduling model](benchmarks/ci-latency-20261106.json) uses the original
773-class Flink 2.2/JDK 11 inventory and observed maximum report times from all host variants.
Its largest estimated partition falls from 92.19 to 81.26 minutes with the same classes.
This is a scheduling estimate, not a measured CI speedup: the source run also contains failed
and interrupted jobs, and real elapsed time includes setup, queueing and test variability.
Each job also uploads its selected classes and
complete-inventory digest as a runtime-partition artifact; failure diagnostics have unique
shard names and are also requested on cancellation, including job timeouts, when the runner
can still upload artifacts. The aggregate gate requires all twelve Java jobs across both Flink lines and JDKs.

Actual Maven regression fixtures compare the unpartitioned test cases with the combined
two- and three-shard plans, including ORC tags, nested classes, parameterized invocations and existing skips.
Duplicate executions fail through exclusive result creation, and an intentionally failing
selected test must still fail Maven. Sharding retains the fresh-process memory boundary
and distributes its startup cost across jobs under the unchanged 150-minute budget; it does not
reduce data, assertions, native-work checks or recovery scenarios.

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
Result reuse does not change the job matrix or test selections.

## MiniCluster reuse within a test JVM

A JUnit launcher-session listener owns the automatic StreamFusion test MiniCluster.
Every test class in the same Surefire fork reuses that cluster, including when Surefire
launches a separate JUnit root for each class. Each test still gets a fresh
execution-environment binding, which is cleared afterwards; Flink 1.18 also binds and clears
its batch test context. The launcher closes the cluster and its temporary directories when
its session finishes, including after test failures. Surefire forks retain their independent clusters;
JUnit tests remain serial within each fork.

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

The collection parity suites are an exception: their private metrics cluster belongs to one
test invocation. This includes primitive, binary, string, structured and nested aggregates,
string and array keys, legacy consumers, standalone expressions and native collection kernels.
Retained Flink gauges can reference finished tasks, execution graphs and collection state,
and the embedded job-result store also retains
serialized collection results until cluster shutdown. Keeping that cluster for an entire class
accumulates results across jobs. Each suite explicitly uses `PER_METHOD`; `@AfterEach` closes
its cluster and reporter after the invocation's value, type, native-work and recovery assertions,
including on failure or skip.
The SQL, input rows, changelog comparisons, recovery prefixes and test parallelism stay the same.
The automatic shared MiniCluster still belongs to the launcher session.

## Fluss SQL fixture lifetime

The released Fluss/ZooKeeper containers allow at most three startup attempts before tests
begin. Docker can race other services for an ephemeral host port; each Testcontainers attempt
creates a new container and port mapping. Persistent startup failures remain blocking, and
test methods are never retried. A failed cluster startup closes every container and its network;
cleanup failures are suppressed on the original startup error. Normal close also attempts every
resource even if an earlier close fails.

Each Fluss SQL test invocation keeps one stock and one native table environment for its
DDL, reads and inserts. `@AfterEach` attempts to close both catalogs and clears the cache,
including when the test fails. Parameterized invocations receive fresh environments.
Job completion, result iterator closure, discovery-job cancellation and native-memory checks
retain their existing boundaries. This uses the existing Java and image jobs.

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
their commands still use normal Maven resolution for any other artifacts requested dynamically by plugins.

The download stage sets a 60-second HTTP read inactivity timeout and five transport retries
for [Resolver 1.9](https://maven.apache.org/resolver-archives/resolver-1.9.24/configuration.html),
[Resolver 2](https://maven.apache.org/resolver/configuration.html) and the older
[Wagon transport](https://maven.apache.org/wagon/wagon-providers/wagon-http/).
Both Resolver property families are supplied: older Maven ignores the newer names, while
the newer hosted Maven ignores the older names. Each version uses its supported settings.
This bounds stalled requests within the existing ten-minute setup attempt, while allowing
connection resets to recover before restarting the dependency goal. These options apply only
to dependency setup, not to test execution.
Resolver also retries HTTP 429/500/502/503/504 responses at that request boundary. A local
HTTP fixture verifies five 502 responses with Resolver and five dropped connections with
Wagon, requiring recovery in a single setup invocation rather than using the outer retries.

Before compiling the pinned upstream Flink reactor, the suite invokes the helper with
`--model-only`. It runs the fixed Maven Help `evaluate` goal for `project.id`, preserving the
wrapper, profile, selected projects and isolated repository. This resolves parent POMs and
build extensions with the same bounded setup retries without trying to download unbuilt
reactor artifacts or invoking a build/test lifecycle. The post-merge Flink 2.2 preparation
for #595 failed loading a build extension's JUnit POM, before its build could reach the
existing runtime dependency preparation.

Upstream preparation also fetches the released `paimon-vfs-hadoop` provider's original
transitive graph at the selected Paimon version. This covers Hadoop client artifacts needed
when compiling Paimon's unchanged connector tests after restoring the shared build. A
validation run for #596 failed in that skipped-test compilation on a Central HTTP 502 for
`hadoop-mapreduce-client-jobclient:3.3.4`; the provider graph includes that artifact. Resolving
the published provider independently avoids running `go-offline` across upstream report and
legacy test-plugin graphs that request obsolete JDK `tools.jar` artifacts on JDK 17.

The Java matrix also prefetches the original transitive graphs of the JUnit Platform providers
used by runtime Surefire 3.2.5 and the separate evidence agent's Surefire 3.5.3. These downloads
use the same bounded setup retries in the independent runtime-dependencies POM, outside the
application's dependency management. The providers can request older Platform artifacts even
when the project has already resolved a newer JUnit version: the Flink 1.18/JDK 11 job for
PR #549 failed downloading `junit-platform-engine:1.9.3` at Surefire startup after `go-offline`
had succeeded. Resolving each provider's own graph before the lifecycle covers that gap without
changing test dependency versions, skipping assertions or retrying tests.

Runtime and canonical jobs also prefetch the ORC execution's separately pinned
`protobuf-java:2.5.0` through that same retry stage. Surefire resolves this additional classpath
even when the selected canonical classes contain no ORC tests; reactor `go-offline` does not
cover it. A JDK 17 canonical shard completed all 190 tests successfully, then failed this later
download with Maven Central HTTP 429. Prefetching the existing dependency moves that network
failure into bounded setup retries without changing the dependency version, test selection or
assertions. Persistent setup failure still fails the job.

[PR #311's post-merge row-format job](https://github.com/datafusion-contrib/StreamFusion/actions/runs/37938400080/job/113846210991)
failed before compilation because one runner reported published Flink 2.2.1 and Kafka dependencies
as missing. Other jobs resolved those releases successfully. Separating downloads from tests
allows transient resolution failures to recover without rerunning test code; forcing missing-release
checks prevents Maven's failed-lookup cache from defeating the next setup attempt.

The upstream suite transfers its prepared pinned Maven distribution to its consumer runners
and verifies that it starts with downloads blocked. See the
[shared-build guide](upstream-flink-suite.md#shared-builds-and-runtime-shards).
That preparation includes Kafka 5.0's distinct Surefire 3.2.5 provider and Platform
1.13.3 launcher; resolving Flink's provider alone does not cover Kafka test startup.

## Doris connector

The required `doris-tests` job runs `bin/doris-suite.sh`: the Arrow adapter tests and the
unchanged source, sink, catalog and lookup SQL tests from released Doris connector 26.3.0
against a real released Doris 4.1.4.1 FE/BE fixture. StreamFusion activation, per-test SQL
inventory and nonempty Arrow source-read witnesses are audited. One stale upstream batch
timestamp assertion is accepted only after reproducing and matching its exact failure with
StreamFusion disabled; all original test SQL and assertions still execute. The incremental reader has unit coverage; a compatible Doris 5.0 server is not
yet available as a released CI fixture.

## Complete canonical job matrix

Flink 2.2 canonical handoff jobs run in 16 additional required shards on each of JDK 11 and 17.
Canonical jobs use the same duration-based balancing as runtime shards, with complete-job
timings retained in `dev/ci/runtime-durations.json`. All 116 runnable canonical classes have
recorded timings; the two abstract bases have zero cost. New classes use the same one-second
default as ordinary runtime classes until measured.
The existing three runtime shards retain all other default Surefire classes on both Flink lines.
Both selections are computed from the compiled test inventory: their union is the original
selection and their intersection is empty. Balancing changes only class placement; test
methods, inherited parameterized cases, nested classes and ORC tag handling are unchanged.
Every canonical shard is required by `All CI tests`.
Canonical jobs set `JAVA_TOOL_OPTIONS=-XX:TieredStopAtLevel=1`, inherited by Maven, Surefire
and the isolated stock/native workers. These short-lived workers repeatedly pay for HotSpot
optimizing compilation and exit before amortizing it. First-tier compilation reduces that
startup CPU cost while preserving fresh processes, both backends, both directions, every
parameterized invocation, native execution evidence, checkpoints and rescaling assertions.
Ordinary runtime, upstream and release/benchmark jobs retain their usual JVM compilation.
This is a CI scheduling optimization, not a SQL throughput result. See the
[CI profile](benchmarks/ci-profile-20261107.json) for measurements and their limits.

Canonical workers also use class-data sharing through `SF_CANONICAL_CDS_DIR`, set only for
their test step. The first successful worker on each classpath records loaded classes, then
HotSpot builds a metadata archive. Later stages use that archive in fresh JVMs with
`-Xshare:on`. A process lock serializes initial archive creation across the two Surefire forks.
Every scenario still creates its own clusters, native state, user-function instances,
checkpoints and results; the archive stores class metadata rather than a running application.

[AppCDS requires JAR classpaths](https://docs.oracle.com/en/java/javase/11/tools/java.html),
so the harness snapshots compiled class directories into deterministic JARs while preserving
classpath order and resource paths. Compiled directories remain immutable for each Surefire
fork. Snapshot names derive from their contents; an existing snapshot is checked for corruption
and retained without changing its timestamp. Changed compiled bytes produce a new snapshot.
Archive keys include the JDK, VM arguments, classpath, worker class and engine role. Caches live
under the current runner's temporary directory and are removed after outcomes and failed
archive diagnostics are uploaded. Default local Maven runs do not enable this cache.
Each runner uses two fresh 2 GiB Surefire JVMs, with process reuse disabled. This preserves all
118 canonical classes on each JDK while reducing canonical runner jobs from 64 to 32. The stock
and native worker processes retain their separate 1 GiB heaps; native memory and containers
also need headroom. Full hosted CI remains the qualification for this scheduling change.

The first combined run exhausted the three-hour JDK 11 limit after seven classes passed in
one canonical shard. Its alphabetical round-robin selection combined several of the longest
job suites. Recorded maximum XML class times from that run now spread this work across the
same 16 shards per JDK. These times are scheduling hints, not performance measurements or
passing-test evidence; all assertions and the complete canonical matrix remain required.

These jobs share the exact-commit debug native artifact, retain their selection manifests and
Surefire reports, and enable the existing Docker-backed Kafka canonical cases with
`SF_KAFKA_INTEGRATION=true`. The canonical Kafka profile supplies the JSON module used by those
jobs. Native execution and clean stock-worker classpath assertions remain active. Benchmarking
for the canonical delivery is deferred by the maintainer.

The separate schedule is necessary for the existing complete-job tests: retained local XML for
62 of the 118 classes alone totals median shard work of roughly 260, 382 and 467 minutes under
the previous three-way hash partition. Those observations are scheduling evidence, not a
throughput benchmark. The ordinary 150-minute job limit cannot accommodate that selection.
Canonical shards have a 180-minute limit; no test is removed to make a shard pass.

## Profiling CI latency and cost

Use `python3 bin/profile-ci.py --repo OWNER/REPO --run RUN_ID` with an authenticated `gh`
CLI. Add `--json` for reusable evidence. The profiler reads the latest run attempt, paginates
all jobs, totals actual completed execution intervals and identifies failed steps. It separates
workflow elapsed time from summed runner time; job start delays include prerequisites,
workflow concurrency and runner queueing. Queued jobs do not receive fictitious execution time.
Canceled jobs without a started step also receive no execution interval. These observations
are not billed minutes and do not isolate pure runner queue time.

The green canonical main run [42357977614](https://github.com/datafusion-contrib/StreamFusion/actions/runs/42357977614)
took 321.6 minutes elapsed and 72.33 runner-hours. Canonical handoff jobs consumed 54.23
runner-hours, ordinary Java jobs 15.06, and the remaining jobs 3.04. The required upstream
workflow took 123.3 minutes elapsed but only 4.23 runner-hours: start delays dominate its
elapsed time while the larger CI matrix competes for runners. Increasing shard counts alone
would not eliminate that shared capacity cost.

The **CI startup profile** workflow runs on manual dispatch or changes to its workflow/profiler.
It builds the exact commit's debug core library once, then compares default and first-tier
JVM compilation, plus first-tier compilation with class metadata archives, on each of JDK 11
and 17. Archive preparation is recorded once, followed by three repetitions alternating settings
on the same runner. All settings run the identical five cases: stock/native join continuations on both
backends, native checkpoint recovery on both backends, and a managed-UDF decimal-key handoff.
The profiler rejects missing, changed, failed or skipped invocations and retains elapsed/CPU
times and class outcomes. These are deliberately small timing probes; the complete CI and
upstream aggregate checks remain the merge requirements.

The initial hosted comparison of first-tier compilation alone passed all 60 invocations.
Median elapsed time fell from 117.93 to 87.55 seconds on JDK 11 (25.8%) and from 148.37 to
111.39 seconds on JDK 17 (24.9%); CPU time fell 58.4% and 56.1%, respectively. These compare
settings within each runner, not JDK performance across different runners.
An isolated JDK 17 group-aggregation worker probe then reduced startup from about 2.9 to
1.6 seconds for stock Flink and from 3.2 to 1.6 seconds for native execution with prepared
metadata. That component probe excludes archive construction; the paired harness includes
archive preparation as a separate measured phase and reuses it for subsequent timings, matching
the longer canonical job. The short cold-cache probe, including construction for five cases,
showed no elapsed-time benefit over first-tier compilation; those measurements remain in the
evidence file. Complete CI establishes the amortized benefit across its full test selection.

Locally, first compile the default runtime with `mvn -pl streamfusion-runtime -am test-compile`
and a matching native build. Then run
`python3 bin/profile-canonical-ci.py --output /path/to/new-evidence-directory --repetitions 3 --cds`.
The `--cds` option requires a JVM supporting class-data sharing. The profiler creates and removes
a small archive before starting, supporting JDK installations without a prebuilt default archive.
Omitting it retains the default/first-tier comparison on unsupported VMs, such as
the local ARM JDK 11 build used during this investigation.
The script preserves concise JSON and failed logs, removes its successful logs/reports and
temporary fixtures, and leaves normal JVM options unchanged outside each measured subprocess.

Recent failure diagnoses:

- [CI 42308241944](https://github.com/datafusion-contrib/StreamFusion/actions/runs/42308241944):
  bounded processing-time RANGE finished before its next-millisecond timer emitted output.
  Commit `18df0c55e` keeps the finite fixture open until all expected rows are evaluated.
- [CI 42293838298](https://github.com/datafusion-contrib/StreamFusion/actions/runs/42293838298):
  SQL/JSON parse-error parity compared Jackson's transient source label after Flink transport.
  The same exception, diagnostic and coordinates differed only in `UNKNOWN` versus `REDACTED`.
  The comparison now checks stable diagnostic fields and all coordinates, with a serialization
  regression and negative controls for genuine mismatches.
- [Reporter 42357436842](https://github.com/datafusion-contrib/StreamFusion/actions/runs/42357436842):
  the runner returned `fsync ... Input/output error` during all three checkout attempts.
  Sparse checkout reduces its repository disk work; it cannot guarantee against runner I/O faults.
- Reporters [42346046364](https://github.com/datafusion-contrib/StreamFusion/actions/runs/42346046364)
  and [42309074911](https://github.com/datafusion-contrib/StreamFusion/actions/runs/42309074911)
  exhausted their six-hour watch deadlines while PR #607's fork CI was still pending.
  They left required checks pending rather than accepting unfinished tests.
- [Docs 42308241816](https://github.com/datafusion-contrib/StreamFusion/actions/runs/42308241816):
  the strict documentation build passed, but GitHub Pages remained in `syncing_files` until
  deployment timed out. The subsequent main deployment passed.
- [Paimon compatibility 41951091666](https://github.com/jordepic/StreamFusion-contrib/actions/runs/41951091666):
  generated legacy sources referenced coordinator APIs absent from released Paimon 1.0.
  Subsequent compatibility runs, including the latest completed branch run, passed.
