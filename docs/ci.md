# CI setup and failure handling

Contributor builds and tests run in **personal forks**, using each fork owner's GitHub Actions
quota instead of the shared `datafusion-contrib` quota. Opening or updating a PR does not launch
the test matrix in the canonical repository. Source-changing canonical `main` pushes still run
the complete post-merge checks, the upstream Flink weekly run remains available, and publishing stays upstream.

Runtime shards finish independently after a sibling failure, preserving complete diagnostic reports
from the cohort. Every required shard must still pass the aggregate gate. Superseded branch pushes
retain workflow cancellation.

Image smoke failures include current TaskManager/JobManager container state, logs and any JVM fatal-error report before the
containers close. Failed smoke reports are retained for three days, so heartbeat timeouts can be
distinguished from JVM/native crashes or container OOM without weakening the submission assertion.

Canonical-job dependency preparation includes the selected runtime module's reactor dependencies.
The Kafka canonical profile references the matching JSON module; a cold runner must resolve that
module inside the reactor rather than depend on an installed copy of an unpublished project JAR.

Prefer focused local tests and reused build caches during development. Combine related work into
a coherent, locally validated change before pushing for the full CI matrix; intermediate commits
do not each need a CI push. Cancel superseded or redundant runs you started, including unnecessary
duplicate post-merge runs, and leave other contributors' runs alone. The final change still needs
the required successful checks on its exact PR head before merging.

## Contributing from a personal fork

1. Fork `datafusion-contrib/StreamFusion` into your personal account. If that name is already
   occupied by an unrelated repository, choose another fork name.
2. Enable workflows in the fork's **Actions** tab before pushing your branch. GitHub disables
   Actions in new forks until the owner enables them.
3. Keep `upstream` pointed at the canonical repository and add your fork as `fork` (or use it
   as `origin`). Fetch upstream and rebase your feature branch on current `upstream/main` so it
   contains these workflow changes. Push the feature branch to your fork, then open a PR into
   `datafusion-contrib/StreamFusion:main`.
4. After focused local validation, explicitly dispatch **CI** and **Upstream Flink suite**
   on the fork branch, plus applicable compatibility, diagnostic or documentation workflows.
   Wait for the exact branch head to pass. Include fork run links
   in the PR so reviewers can inspect jobs and artifacts directly. Rebase and rerun after
   incorporating new upstream changes before merging.

For example, after adding the fork remote:

```sh
git fetch upstream
git rebase upstream/main
git push -u fork HEAD
gh pr create --repo datafusion-contrib/StreamFusion --base main --head YOUR_LOGIN:YOUR_BRANCH
gh workflow run ci.yml --repo YOUR_LOGIN/YOUR_FORK --ref YOUR_BRANCH
gh workflow run flink-suite.yml --repo YOUR_LOGIN/YOUR_FORK --ref YOUR_BRANCH
```

For docs changes, also dispatch `docs.yml`. Dispatch `paimon1.yml`,
`nested-scalars.yml` or `jvm-aggregates.yml` when the change touches their listed
compatibility/diagnostic scope. Run these once the combined change is locally ready;
commits and intermediate pushes do not each need a matrix. Until older feature
branches include this configuration, `[skip ci]` can suppress their push-triggered
workflows; it does not suppress the lightweight `pull_request_target` reporter.

After syncing the fork's default branch with already-merged upstream code, the native
dependency cache can be primed there once:

```sh
gh workflow run ci-profile.yml --repo YOUR_LOGIN/YOUR_FORK --ref main -f suite=native-only
```

GitHub lets feature branches restore the default branch's cache, while sibling branches
cannot restore each other's caches ([cache access rules](https://docs.github.com/en/actions/reference/workflows-and-actions/dependency-caching#restrictions-for-accessing-a-cache)).
The native-only producer builds the complete workspace with the normal debug dependency
configuration. A verified sibling-branch run restored that default-branch cache in about
33 seconds, then built in 24.06 seconds with 42 positive-duration compilation units and
no RocksDB rebuild. Cold builds of the same native tree recorded 639 such units and
598.57–632 seconds. The consumer created no duplicate branch cache. This is native setup
evidence, not whole-CI latency or a same-run CPU comparison; normal tests and source
validation still run. Cargo rebuilds changed project crates and dependencies as needed.

Feature branches use explicit workflow dispatches. Test and docs push triggers are
limited to `main` and explicit `ci/**` submission branches. Paimon compatibility
and the two release diagnostics run automatically only on `main`; their existing
path filters still apply there. On other branches, dispatch these optional
workflows only when their functionality changed. Development branches belong in
personal forks, and tags do not trigger these branch test workflows.

The upstream full-suite dispatch is identified as **Full upstream Flink suite
(merge gate)**. Inventory dispatches have a separate title and cannot satisfy that
gate; older unmarked manual runs are also excluded. Existing exact-head push
evidence remains valid.

A `ci/**` branch explicitly requests the two merge-gate matrices and applicable
docs validation on push. This supports adoption while the canonical reporter still
requires upstream push evidence. Keep ordinary work on `feature/**` (or another
name outside `ci/**`) and update a submission branch only when ready for another
full run. Optional compatibility/release diagnostics stay explicit on submission
branches too: changing their trigger policy alone does not need another release
benchmark. The PR must point at the exact tested submission head; results from
another branch or SHA are never inherited.

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
fork when job details are available. The reporter reads those details in small pages so large
matrices do not time out on a single request. If that endpoint is temporarily unavailable, the
check still reports the exact-head workflow result and links to the full run. Start delay
includes prerequisite builds as well as runner queueing; it is not labelled
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

CI, upstream Flink, and Paimon compatibility cancel older runs on the same branch,
including repeated manual dispatches. Upstream inventory runs use a separate group. The latest
main commit receives complete checks, without waiting behind superseded main commits. Release
publishing retains its non-canceling concurrency policy.

Docs builds have read-only repository permissions. Only canonical `main` uploads a Pages artifact
and deploys, with Pages/OIDC permissions confined to deployment. Forks build without publishing.
Fork docs dispatches cancel obsolete builds; canonical main deployments remain serialized. Release
tag validation and all dependent release publishing jobs are also restricted to the canonical repo.

Documentation uses MkDocs 1.6's generated-file hook to render SQL inventory downloads
from the checked-in snapshots and retrieve hash-pinned historical benchmark artifacts.
Both are staged under `target/`, keeping generated exports and historical run outputs
out of the maintained source tree. Existing published paths remain unchanged; an
unavailable or corrupt artifact fails the docs build. See the
[evidence policy](benchmarks/evidence.md#source-repository-and-archived-run-outputs)
for retrieval and retention. The docs workflow tests snapshot rendering and evidence
retrieval before building; ordinary source/test jobs do not fetch these archives.

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

Previous-runtime diagnostics copy the current benchmark fixtures together with their test-only
timing and metrics helpers and the selected Flink line's local-context adapter. All copied files
are hashed, including dependencies absent from the historical checkout, so the immutable baseline
can compile the current instrumentation without copying any production implementation from the
candidate. The fixture-copy regression checks both supported Flink lines.

## Shared debug native libraries

Host and Darwin release builds compile the selected native packages together in one Cargo
invocation per target, then stage their individual libraries. Previously the host-only image
path invoked Cargo separately for every package on each Flink line, allowing different feature
graphs to rebuild shared dependencies. Package selection, mimalloc, platform staging and JNI
export checks are unchanged. The shell regression verifies both Flink lines, both Darwin
architectures, every staged library and propagation of native build failures. This removes
repeated compiler invocations; hosted elapsed-time improvement still needs measurement.

Ordinary CI builds all workspace debug native libraries once in a dedicated Ubuntu 24.04
job. That job verifies the package dependencies and JNI export boundaries. The original
`debug-native-SHA` artifact retains full symbols for diagnostics. Test consumers use
`test-native-SHA`, containing the same libraries after `strip --strip-debug`; canonical
consumers use `canonical-native-SHA`, containing core, JSON and Kafka libraries. Staging
preserves the Cargo outputs and cache, verifies ELF identity and every allocated section's
type, flags, virtual address, size and bytes, and retains static function names. The producer
removes its staged copies after upload. Consumers download their payload into `native/target/debug` and pass
`-Dnative.build.skip=true` to Maven. Both JDKs and Flink lines exercise the same native build;
runtime and lake consumers compile their selected Java reactors, while canonical consumers
restore their compiled Java inputs. Every consumer runs its existing tests. A failed
producer blocks the consumers and fails the aggregate CI gate. Consumers have no fallback to
an older cache or a local native rebuild.

The producer is the only writer of the `debug-jvm` dependency cache. Consumers transfer final
libraries rather than restoring Cargo intermediates or installing a Rust toolchain. The Rust
test job separately compiles its test harnesses and checks every tagged native package in
isolation once, preserving the checks for dependencies accidentally supplied by the workspace.
The native build retains Cargo's `--timings` report as a separate diagnostic artifact.
That artifact also records original and staged library sizes. A real Linux amd64 staging
probe retained all allocated sections across all 12 libraries while reducing uncompressed
bytes from 1,814,625,232 to 755,022,680. Independent DEFLATE level-1 payload totals fall from
405,636,999 to 173,982,675 bytes; the canonical three-library subset totals 73,386,844 bytes.
These are file-size measurements rather than whole-workflow timing. Full-symbol diagnostics
and release libraries retain their existing behavior. See the
[structured probe](benchmarks/ci-fast-loops-20261112.json) for each library and limits.
The Rust test job reads that same dependency cache instead of maintaining another copy;
it still compiles and runs its own harnesses and standalone package checks.

The Rust test job runs the same 30 GiB free-space preflight as the shared native build,
before restoring Cargo caches. The post-merge run for #595 exhausted its runner's disk
while compiling tests; the runner itself crashed before it could upload a test result.
The preflight removes only unused hosted-runner toolchains and fails early if it cannot
provide enough initial capacity.

Upstream Flink preparation restores the same `debug-jvm` dependency cache, at the same
`native/target` path and with matching Rust environment settings, on both Flink lines.
Cargo still builds all current workspace libraries from each suite's isolated source checkout.
Only the dynamic libraries reported by that successful Cargo invocation are staged into
the suite build; cached extra libraries cannot enter its artifact. Upstream preparation
does not write another cache. This removes two redundant dependency-cache copies without
sharing test outcomes or changing native package/feature selection. The ordinary native
producer remains the cache writer. Optimized image builds keep their release configuration.
The November 12 cache snapshot recorded two 1.26 GB upstream entries with the same key but
different versions, in addition to the ordinary debug entry. Each line cached a different
target path, and the upstream environment also omitted the producer's `RUST_BACKTRACE`.
Normalizing those paths and settings removes about 2.53 GB of redundant cache candidates.
This is projected storage avoidance, not measured reclaimed space or an elapsed-time gain.
GitHub includes cached paths in [cache version matching](https://docs.github.com/en/actions/reference/workflows-and-actions/dependency-caching)
and evicts older entries when the repository's cache budget fills. Default-branch retention
still depends on that budget; matching caches never replace current-source compilation.
The debug artifact is scoped to its current
workflow run and commit; it is not a release payload or a cache of test outcomes. This replaces
the independent debug library builds with one without changing test selection or test concurrency.
Actual elapsed-time savings must be measured in GitHub Actions after this change runs.

Personal-fork workflow runs also save their Cargo dependency caches and prepared pinned Flink
checkout/Maven caches. Restricting saves to `main` left normal fork development branches unable
to warm their own caches for subsequent runs. GitHub keeps those caches scoped to the branch;
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

Lake coverage uses nine jobs for the same thirteen module/format configurations. Each
Paimon Flink/JDK pair resolves dependencies and compiles once, then runs the complete
Parquet and ORC suites in separate Maven/Surefire processes with distinct report suffixes.
The precompiled profile skips repeat Java compilation and javadoc while preserving resources,
test discovery and every assertion. Both formats execute even if one fails; either failure
fails the job. A failed preparation stops before either format. Outcome validation rejects
empty, wholly skipped or malformed reports and clears only prior reports with its own suffix.
The Flink 2.2/JDK 17 upstream compaction checks remain required after the paired formats.
Other lake configurations retain their existing commands. Real Maven regressions exercise
both formats and prove a failed first format does not suppress the second; a matrix contract
compares all thirteen expanded configurations with the original coverage.

The full Java suite includes every connector and format tag on both Flink lines and both
JDKs. Its default Surefire execution runs untagged tests and every tag except ORC; a second
execution runs ORC with its isolated protobuf classpath. These same tests do not need six
additional tagged-module jobs. Standalone native package checks remain in the Rust job.
`bin/test_ci_module_groups.py` exercises the actual runtime Surefire configuration with one
fixture for every declared module tag and one untagged fixture, requiring each to run once.

## Java test memory budget

The ordinary Java test step uses first-tier compilation and the worker settings measured by
the paired runtime diagnostic:

| Flink / JDK | Fresh Surefire forks | Heap per fork |
| --- | ---: | ---: |
| 1.18 / 11 or 17 | 4 | 2 GiB |
| 2.2 / 11 or 17 | 4 | 2 GiB |

Every combination caps the configured Java heaps at 8 GiB in total. Native allocations,
metaspace and VM overhead still need headroom; full CI validates the complete test selection.
The test step prints available processors and memory and samples `vmstat` throughout execution.
The separate JVM-boundary/cache verification phase retains two 4 GiB forks and usual compilation.
CI disables fork reuse, so each test class gets a fresh JVM. Local Maven defaults retain fork
reuse and usual compiler settings. The default 8 GiB per-fork heap
combined with CPU-based fork counts leaves insufficient predictable headroom for Arrow buffers, native
operators, embedded clusters and the Maven process on hosted runners.

Savepoint CI uses four fresh 1 GiB parent forks per job, each retaining its existing fresh
1 GiB engine worker for a handoff stage. The configured parent heaps total 4 GiB, with
8 GiB of combined parent/worker heaps when all four workers run. The required matrix
contains 64 jobs across both Flink lines and JDKs; first-tier compilation, immutable metadata sharing and fresh engine
process boundaries remain in place. The test step records processors, memory and `vmstat`.

Released Flink MiniClusters keep completed execution graphs and serialized results in an
unbounded in-memory archive. Its JVM shutdown hook can retain this archive even after the
cluster stops, so closing private clusters or rotating the shared cluster does not reliably
release their results inside a reused JVM. A fresh process per class bounds this accumulation
without reflecting into Flink internals or evicting results during a scenario. Classes retain
their shared launcher-session cluster and all within-class lifecycle, failure and recovery checks.
The tradeoff is additional JVM and native-library startup per class;
complete CI elapsed time, rather than local function throughput, measures that overhead.
The aggregate heap cap also leaves room for the temporary copies made while checkpointing and serializing
large binary collection results. The configured heaps total at most 8 GiB; Arrow/native
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
The current weights use the subsequent complete passing matrix at `b802a9bb7`. Runtime
costs have Flink-line overrides so, for example, cases skipped on Flink 1.18 do not receive
their Flink 2.2 execution cost. The same inventories remain complete and disjoint; only class
placement changes. The [updated scheduling model](benchmarks/ci-profile-20261108.json) lowers
the largest estimated runtime shard from 9,905.76 to 9,039.83 class-seconds on Flink 2.2 and
from 6,054.61 to 5,904.29 on Flink 1.18. These sums exclude parallel overlap and runner/setup
delays and establish no elapsed-time claim. Unknown classes retain a conservative older
measurement when available, otherwise the one-second default.
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
The shared cluster starts when a test first requests an execution environment. Pure unit,
operator-harness and isolated-worker driver tests keep the normal memory budget and leak
checks without starting an unused parent cluster. The deferred factory starts the released
MiniCluster resource, installs Flink's ordinary factories, then repeats the environment
lookup with the original configuration. Flink 1.18 defers both batch and streaming contexts.
`-Dsf.test.lazyCluster=false` retains eager startup for controlled comparisons.
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
An additional regression proves a pure test keeps its memory budget without a running
cluster. The Flink 1.18 adapter also executes batch jobs after batch-first and stream-first
environment lookups.

Three alternating local comparisons retain all 20 selected unit/operator cases, four fresh
2 GiB forks and first-tier compilation. Flink 2.2/JDK 17 elapsed medians fall from 4.016 to
3.435 seconds (14.5%), with 39.5% less child CPU. Flink 1.18/JDK 11 falls from 4.530 to
3.794 seconds (16.2%), with 38.5% less child CPU. Each eager trial starts three parent
clusters; each lazy trial starts zero. A separate two-case canonical continuation comparison
with prepared metadata archives falls from 24.758 to 24.112 seconds (2.6%), with 4.2% less
CPU. These are selected local comparisons, not whole-CI gains. The
[structured trials](benchmarks/ci-fast-loops-20261112.json) retain every measurement and limit.

The focused Linux run 43225947903 passes all four Flink/JDK cohorts: 480 timed invocations
and 320 lifecycle, native-memory and SQL controls, with no failures or skips. Across those
cohorts, eager-to-lazy unit/operator medians improve 24.2–29.0% elapsed and 35.5–41.2% CPU.
These hosted selected-workload results still do not establish a whole-CI speedup.

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

Canonical Java compilation runs once for each Flink/JDK pair, in four required preparation
jobs. These jobs depend on the change plan and start alongside native work; compilation
does not download or wait for a native library. They retain Maven dependency resolution,
Java and protobuf compilation and javadoc checks, with test execution deferred.
The 64 consumer shards require both Java preparation and the native producer. The first
shard in each Flink/JDK cohort executes the reactor dependencies' test lifecycle; the
remaining shards skip dependency tests. Every shard runs its complete selected runtime
inventory. This keeps once-per-cohort dependency-test coverage without delaying pure
compilation behind the runtime queue.

Each preparation uploads a one-day, exact-commit archive of the runtime, core and JSON
compiled classes, non-native resources and test classes. Native libraries are supplied by
the separate exact-commit native artifact and copied by the normal resource lifecycle;
packaged reactor JARs are regenerated by that lifecycle rather than transferred twice.
The archive excludes Surefire reports,
checkpoints, diagnostics and other execution state. Consumers verify its checksum, source
commit, Flink line, complete Java version, operating system and architecture before restoring
into fresh output directories. They download the same commit's canonical native payload,
resolve dependencies with bounded retries, discover their complete compiled canonical
inventory, and create independent workers, archives and application state.
Both preparation and consumer jobs pin the same complete Temurin release (11.0.32+9 or
17.0.21+9). Floating major-version selection previously chose 11.0.33+10 for a producer
and 11.0.32+1 for consumers on different runner images; the identity guard correctly
rejected the transfer. Version mismatches report both identities and remain errors.
The initial 11.0.32+1 pin existed on cached runner images but was absent from the vendor's
remote release list. The current pin uses the [published Temurin 11.0.32+9 release](https://github.com/adoptium/temurin11-binaries/releases/tag/jdk-11.0.32%2B9).
The focused preparation diagnostic resolves the pinned version remotely even when a runner
already has a cached JDK, preventing a cache-only build from passing that setup check.
The [short-loop measurement](benchmarks/ci-fast-loops-20261112.json) inspected the
Flink 2.2/JDK 11 artifact from run 43203430658: 811,024,598 compressed bytes contained
1,318,153,104 uncompressed bytes of native libraries plus 460,739,751 bytes of packaged
JARs, compared with 28,625,775 bytes of classes and other resources. Repacking only the
shared Java inputs at gzip level 1 produced 10,387,432 bytes, a 98.7% transfer reduction
for that artifact. This measures archive size; it does not establish whole-CI savings.

Consumers keep Maven's `test` lifecycle so uninstalled reactor dependencies resolve correctly.
The explicit `sf.ci.precompiledJava=true` profile skips only Java compilation and the already
completed javadoc check. Surefire remains active through `sf.runtime.tests.skip=false`;
reactor dependency tests run in shard 1 for the same Flink/JDK pair. Protobuf/resource
preparation and both Surefire executions retain their usual lifecycle behavior. Ordinary
local builds and the other CI jobs do not activate this profile. A real three-module Maven
regression transfers compiled outputs to a fresh consumer, verifies reactor dependency
resolution and parameterized cases, and requires an intentional selected failure to fail Maven.
The regression compiles without any native library, checks the first consumer executes
dependency and runtime tests, then verifies a later consumer skips dependency tests while
still running every parameterized runtime case.

The [retained baseline](benchmarks/ci-profile-20261112.json) records every canonical class's
outcome counts, each cohort's complete inventory and invocation digest, and Actions phase costs.
The preceding [passing CI run](https://github.com/jordepic/StreamFusion-contrib/actions/runs/43116124202)
spent 128.6 runner-minutes on the initial compile/partition step across 64 canonical shards.
Sharing compilation replaces 64 preparations with four; archive preparation, transfer and
remaining lifecycle costs still require hosted measurement before claiming elapsed-time savings.

The [complete shared-compilation candidate](benchmarks/ci-profile-20261112.json) passed all
100 CI jobs and all 19 upstream checks. Every canonical cohort retained its 120 selected
classes, 118 reported suites, exact invocation identities and outcomes: 3,592 cases on
Flink 1.18 and 3,676 on 2.2 per JDK, with no skips or failures. Initial compilation and
packing used 8.6 runner-minutes instead of the reference's 128.6-minute compile/partition
phase, with another 13.7 minutes for consumer extraction. Whole CI used 57.64 runner-hours
versus 56.92 (1.3% more) and took 236.9 minutes versus 217.2. Higher JDK 11 test execution
time offset setup savings. These cross-run observations include cold native setup, runner
variation and competing diagnostics; they establish no whole-CI speedup from compilation
reuse alone. The combined candidate must qualify the collector and cache changes below.

JDK 11 canonical jobs use Serial GC for Maven, fresh Surefire parents and their fresh engine
workers. The separate CDS generator uses G1: the first combined candidate crashed inside
Temurin 11.0.32+9's Serial full GC while relocating metadata (`AdjustPointerClosure::do_oop`).
The collector override applies only to archive generation. A Linux child-JVM control records
the actual collectors and checks that a G1-generated archive is reused by fresh Serial workers.
JDK 17 keeps its usual worker collector. The shared compiler preparations retain their
existing settings. Artifact identity reads the complete JDK version without inherited runtime
option variables, so changing the test collector cannot turn an option-pickup banner into
a false compiler-version mismatch. This affects only CI test JVMs and preserves every
provider, checkpoint, rescale and native-work assertion.

Combined candidate 43284069894 finished in 184.7 minutes and used 49.18 runner-hours, but
one canonical invocation failed during that metadata-generator crash; its aggregate gate failed.
Its full upstream suite 43284074933 passed all 19 jobs. These observations preserve the failed
qualification and do not establish a successful whole-CI speedup. The repaired candidate still
requires both complete merge gates and full canonical invocation/outcome comparison.

The isolated canonical job worker exits explicitly after its job, native-work
checks and try-with-resources cluster cleanup complete. A Flink 2.2/JDK 11
checkpoint worker finished that work in about seven seconds but retained an idle
non-daemon Pekko metrics dispatcher, keeping the JVM alive until the four-minute
process timeout. Success exits with status zero; any job, assertion or cleanup
failure still prints its original stack and exits nonzero. The process regression
injects a lingering non-daemon thread and checks both outcomes and completed cleanup.
The timeout and all result, state-transfer and native-execution checks are unchanged.

Kafka checkpoint handoffs wait for the fixture sink's external commit before cancelling the
completed checkpoint leg. The coordinator's checkpoint future can complete before the committer
receives its completion notification. The boundary still requires the exact row count and the
subsequent engine switches check for loss, duplicates and replay; a missing commit times out.

Floating DISTINCT local/global fixtures with processing-time mini-batch markers compare the
materialized changelog, result types and consumed work in both native stages. Independent runs
can flush at different boundaries and emit different valid intermediate updates. Tests with
deterministic bundle boundaries retain their stricter raw-changelog assertions. The global
fixture also checks every retraction against the current result and rejects a second live result.

Both Flink lines run complete canonical handoff jobs in 16 required shards on each of JDK 11
and 17. Every shard uses the same commit's canonical native payload, startup compiler settings and
scoped class-data cache. Flink 1.18 combines its release profile with `canonical-kafka`; dependency
resolution, compilation and execution use the same profiles. Artifacts include the Flink line,
JDK and shard, and selection manifests record the line. The former five-family smoke step in
1.18's runtime shard is replaced by this exhaustive compiled-inventory selection.

Each line selects from its own compiled test inventory and duration estimates, preserving every
method and inherited parameterized invocation. Canonical job fixtures and their typed-state
bases now live in shared test sources for joins, sort, normalization, deduplication, rank,
window and grouped aggregates and managed UDFs. Kafka source, partition-discovery and all twelve
sink/pipeline job classes are shared as well. Both release lines now compile the same 120-class
canonical job inventory, including two abstract bases without Surefire reports. Narrow fixture adapters inspect each released connector's recovery
format: 1.18's legacy writer prefix and committer collector, or 2.2's explicit writer ownership
and precommitted transactions. The 1.18 connector uses its released incrementing transaction
scheme; 2.2 retains both incrementing and pooling providers. Local fixture selection can sample
scenarios across all twelve classes while hosted CI retains every release-supported provider.
Release adapters retain version-specific runtime behavior. Compilation and selection parity do not establish
that all handoffs pass: the complete required matrix remains the qualification gate. Adding a
shared fixture automatically includes it in both lines' required canonical matrix. The existing
three runtime shards retain the other default Surefire classes on both lines: the two selections
have a complete union and empty intersection within each compiled inventory. Nested classes and ORC tag handling are unchanged. Every line/JDK/shard is required
by `All CI tests`; no local smoke flag is set in hosted canonical CI.

Local qualification includes all four temporal native-checkpoint-to-canonical continuations on
1.18 (compiled plans and ordinary SQL, both native stores). Fifteen focused cache, managed-UDF
failure and mini-batch ordering controls also pass on 1.18. A clean 2.2 build retains all 118
canonical job classes and passes the same controls plus its transient-factory restoration test
(16 checks). These selections do not qualify the
remaining inherited job providers or the newly shared Kafka source cases; hosted CI runs their
full providers. The Kafka parity cohort additionally passes 24 selected real-broker scenarios
on 1.18 across all twelve sink/pipeline classes and source checkpoint recovery, plus 22 admission
controls. Four full continuation combinations subsequently cover SQL/compiled submissions and
both initial engine orders with the corrected metadata cache. Six shared cache controls pass,
including actual archive reuse across stage directories. Twelve real continuation workers reuse
two metadata archives; their application state and runtime directories remain independent.
A clean 2.2 build compiles the same 118-class inventory and passes the equivalent 24 selected
broker scenarios, 22 admission checks and six cache controls without skips. Across the two release
classpaths, these runs retain six worker metadata archives instead of generating one per stage.
Canonical scheduling now uses that complete matrix's class times with startup compilation
and metadata sharing enabled, replacing estimates from the earlier JVM settings. For the
same 118 classes, including two abstract bases, the largest modeled shard falls from 7,417.10
to 6,110.50 class-seconds while average work stays 5,995.60. This rebalances existing work;
the complete hosted matrix establishes its actual effect on job and workflow time.
Canonical jobs set `JAVA_TOOL_OPTIONS="-XX:TieredStopAtLevel=1 -XX:ReservedCodeCacheSize=512m"`,
inherited by Maven, Surefire and the isolated stock/native workers. These short-lived workers repeatedly pay for HotSpot
optimizing compilation and exit before amortizing it. First-tier compilation reduces that
startup CPU cost while preserving fresh processes, both backends, both directions, every
parameterized invocation, native execution evidence, checkpoints and rescaling assertions.
The ordinary runtime test step also uses first-tier compilation after its separate paired
qualification. First-tier test runs reserve a 512 MiB compiler cache: the local canonical
operator matrix exhausted the default 48 MiB cache and HotSpot stopped compilation. The larger
reservation prevents that limit from forcing the remaining test code into the interpreter; it
is a capacity correction, with no new throughput or hosted CI timing claim. Upstream, JVM-boundary and release/benchmark jobs retain usual compilation.
This is a CI scheduling optimization, not a SQL throughput result. See the
[CI profile](benchmarks/ci-profile-20261107.json) for measurements and their limits.

For local canonical iteration, `-Dstreamfusion.test.canonicalSmoke=true` selects deterministic
representatives from the largest canonical matrices, including typed grouped-state
lifecycles, numeric/string/temporal extrema, extrema orphan/cache and late-corruption checks,
temporal-sort precision, interval typed-key and
processing-clock matrices, window-deduplication, shared-pane offsets/DISTINCT, processing-clock
windows, typed append/update/retract rank matrices, and unbounded event-time OVER state/planner
matrices. Every individual parameter value and
every pair of values present in the original provider remains represented. The interval
operator selections reduce 171 event-time invocations to 63 and 208 processing-time invocations
to 69 locally; default/CI retain the original providers and every invocation. Existing release
assumptions still apply; a skipped case does not qualify a native path. Named corruption and
regression providers remain exhaustive except the extrema corruption combinations named above.
This is an iteration check, not full matrix qualification;
omitting the flag retains every invocation in its original order. CI does not set this flag and
continues to run the complete matrices. Batch operators and release lines in these short checks,
then use the complete matrix for the final qualification. The 2,500-key window-deduplication
continuation sends three native Arrow batches while retaining every stock input and result
assertion, avoiding 2,500 individual input conversions per invocation.

The local fixed-window job selector `streamfusion.test.groupArithmeticJobSmoke=true` keeps
continued-input jobs and one no-input RocksDB recovery control, so both parameterized methods
receive executable cases. Default and hosted providers remain exhaustive.

The canonical unbounded event-time OVER state, planner and full-job fixtures are shared between
released Flink 1.18 and 2.2. The release adapters choose the actual V1 polling or V2 timestamp
owner; V1-specific callback cases run on 1.18 and are explicitly skipped on 2.2. Existing full-job
providers remain exhaustive by default; `sf.canonical.eventOver.job.smoke=true` keeps the established
local ROWS-cleanup/RANGE selection for fast complete-job iteration. This selection is not the
full 1.18 job-matrix qualification.

Unopened fixture definitions use the same scoped cache on both releases. Flink 1.18 StreamNode
graphs are not serializable, so managed-UDF and mini-batch graph fixtures construct independent graphs there;
2.2 retains serialized graph definitions and restores transient operator factories. Neither path
shares running operators, user-function instances or state between cases.

Canonical workers also use class-data sharing through `SF_CANONICAL_CDS_DIR`, set only for
their test step. The first successful worker on each classpath records loaded classes, then
HotSpot builds a metadata archive. Later stages use that archive in fresh JVMs with
`-Xshare:on`. A process lock serializes initial archive creation across the two Surefire forks.
Every scenario still creates its own clusters, native state, user-function instances,
checkpoints and results; the archive stores class metadata rather than a running application.

[AppCDS requires JAR classpaths](https://docs.oracle.com/en/java/javase/11/tools/java.html),
so the harness snapshots compiled class directories into deterministic JARs while preserving
classpath order and non-native resource paths. These test-only metadata JARs omit bundled
`.so`, `.dylib` and `.dll` copies under `tech/streamfusion/native/`: native workers already
receive the current libraries through their explicit `java.library.path`. Original compiled
resources and deployable JARs remain intact. `-Dsf.canonical.packNativeResources=true` restores
the previous snapshot contents for a controlled comparison. A hosted four-parent JDK 11 JFR
profile found packing in 1,543 of 1,771 Java execution samples, predominantly SHA-256; these
samples attribute work and are not an end-to-end speedup measurement.
A local synthetic 323 MB packing loop with four fresh cache instances, repeated in three
alternating pairs, reduced elapsed medians from 13.57 to 0.042 seconds on JDK 11 and from
13.91 to 0.027 seconds on JDK 17. This isolates redundant packing and hashing; it does not
measure engine throughput or full CI. The structured short-loop evidence records every trial.
The subsequent hosted setup probe 43282442243 passed all eight recovery/Kafka cases with
positive native-work and stock-isolation assertions. Four parent recordings contained 37
classpath-packing samples out of 269 Java samples, versus 1,543 out of 1,771 before the change;
SHA-256 leaf samples fell from 1,442 to 25. These separate instrumented runs confirm the
targeted work was removed, without establishing an exact CPU or elapsed speedup.
Compiled directories remain immutable for each Surefire
fork. Snapshot names derive from their contents; an existing snapshot is checked for corruption
and retained without changing its timestamp. Changed compiled bytes produce a new snapshot.
Archive keys include the JDK, VM arguments, classpath, worker class and engine role. A stage's
`java.io.tmpdir` is excluded from the metadata key: it remains a fresh runtime argument, not a
reason to rebuild an identical archive. A real child-JVM control reuses one archive across two
stage directories and checks actual temporary-file placement; changed roles and other VM
settings still select different archives. Caches live
under the current runner's temporary directory and are removed after outcomes and failed
archive diagnostics are uploaded. Default local Maven runs do not enable this cache.
Each runner uses four fresh 1 GiB Surefire JVMs, with process reuse disabled. The earlier 2.2-only
change preserved its 118 classes on each JDK while reducing that line's runner jobs from 64 to 32.
The required two-line matrix now has 64 jobs, preserving each line's compiled inventory. The stock
and native worker processes retain their separate 1 GiB heaps; native memory and containers
also need headroom. The complete 32-shard hosted canonical matrix passed on both JDKs;
the full CI and upstream aggregates remain required for source changes.

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

### Runner queue discipline

Use local selected tests and short repeated profiling loops while iterating. The optional
`cluster-startup` hotspot mode runs the unchanged 20-case comparison on selected Flink lines
and both pinned JDK releases. Locally, after compiling the requested line, use
`python3 bin/profile-runtime-ci.py --cluster-startup --line 2.2 --repetitions 3 --output NEW_DIRECTORY`.
It checks every invocation/outcome and the number of parent clusters actually started.
The full CI and upstream matrices remain final qualification gates.

Before a dispatch, inspect active workflows across the fork and reuse their handles. Keep
optional diagnostics separate from the candidate's full qualification campaign. Stop this
task's remaining campaign after a shared setup failure makes success impossible, retain the
failure, and repair it with a short regression or focused hosted probe. Other sessions' runs
remain untouched. The project instructions in `CLAUDE.md` make this the default agent workflow.

The passing compilation-only run 43149560105 reached 20 simultaneous CI jobs. Its 64 canonical
jobs waited a median 4,331.5 seconds (72.2 minutes), with a maximum of 9,000 seconds
(150 minutes), **after all four preparation jobs had finished**. This eligibility-to-start
interval includes scheduler and runner waiting rather than Java preparation. It differs from
the profiler's broader start-delay field, which also includes prerequisites. Reducing repeated
setup, avoiding overlapping campaigns, balancing work and removing dependency barriers should
precede increasing job counts. Preserve all configurations and test invocations when grouping
compatible checks. Structured timing observations live alongside the short-loop evidence.

The **CI startup profile** workflow's `canonical-setup` suite delegates to the
**Canonical setup probe**, checking one chosen Flink/JDK pair with two fresh runners.
Its producer compiles and packs Java before any native library is available.
The consumer verifies the pinned JDK and transfer, reuses a source-verified existing native
producer, stages only core/JSON/Kafka libraries without debug information, and runs eight
selected recovery and real-broker source/sink cases. Positive native work and existing
stock isolation assertions remain active. Smoke flags are confined to this diagnostic;
the ordinary CI matrix retains every provider. This probe checks setup quickly and cannot
satisfy the full CI merge gate.
The first successful run, 43240390726, prepares Java in 170 runner-seconds and completes
the fresh consumer in 333 runner-seconds, including dependency resolution and all eight
checks. Its Java transfer is 10,391,513 ZIP bytes. This validates the setup and smaller
native subset; it does not qualify the remaining canonical inventory or establish net CI
timing. The structured evidence retains every selected class outcome and phase boundary.

Set the setup suite's `parent_jfr` input to record the four fresh parent test JVMs separately
from their isolated engine workers. The diagnostic summarizes execution/native samples,
compiler activity, allocations and waits, then removes the recordings. Instrumented elapsed
times remain attribution evidence. The `lake-setup` suite separately runs both complete Paimon
format configurations for one chosen line/JDK and retains compilation, per-format elapsed/CPU
times and JUnit outcomes. These small workflows remain separate from merge-gate qualification.
The setup probe's `metadata_regression` input instead runs all eight worker-cache controls plus
the complete 28-case append-arithmetic class that exposed the JDK 11 crash. It requires all 36
checks to pass, including actual collector isolation and unchanged native/stock continuation,
checkpoint and rescale checks. This mode records two parent JVMs when `parent_jfr` is enabled.

Native-source verification reads producer jobs in 20-row pages and retries transient
HTTP 429/5xx responses with bounded delays. The first cluster-startup diagnostic encountered
three HTTP 502 failures before test execution; retrying only those jobs passed. Source
differences, failed producers and permission failures remain errors.

For a short local attribution loop, set `SF_CANONICAL_PROFILE_DIR` to a task-specific
directory and run `CanonicalGroupArithmeticJobTest` with
`-Dstreamfusion.test.groupArithmeticJobSmoke=true`. This retains continued-input handoffs
through both stores and records each fresh stock/native worker separately with JFR's
profile settings. The optional instrumentation is added after archive identity and dump
commands are constructed, preserving ordinary metadata reuse. Remove the variable for
uninstrumented A/B timing. Summarize the recordings and delete them after inspection;
instrumented times are attribution evidence, not speedup measurements.

The handoff-readiness polling experiment retained the existing one-second fallback:
a 100 ms probe showed no consistent elapsed benefit and slightly higher CPU on both
JDKs. The [decision record](https://github.com/datafusion-contrib/StreamFusion/blob/main/.claude/wontdos/ci-handoff-polling.md)
and [structured trials](benchmarks/ci-profile-20261112.json) preserve the result.
Hotspot jobs use a shallow partial checkout and fetch only the native-producing
commit needed for their unchanged source comparison, avoiding a complete history clone.

To compare a canonical matrix with a previous complete run, download that candidate's
`canonical-job-results-*` artifacts into one directory. Pass it to `bin/profile-ci.py`
with `--canonical-reports DIRECTORY --canonical-baseline BASELINE.json` alongside the
usual `--repo`, `--run` and optional `--json` arguments. The helper requires every cohort
and declared shard, matches each selection manifest to its reports, rejects duplicate
classes and malformed outcome totals, and compares the full parameterized invocation
multiset with the baseline digest. Missing, duplicated or newly skipped invocations fail
the comparison. Abstract inventory entries need no report, while every class with a baseline
report, including zero-case suites, must still appear. The tool recomputes the complete
selected-inventory digest, including abstract entries, from the manifests.
This verifies preserved test coverage; GitHub's required workflow results
and the assertions inside those tests remain the correctness gates. Comparing timings
alone cannot establish equivalent coverage.

The [initial collector probe](benchmarks/ci-profile-20261112.json) passed all 80
invocations on Flink 2.2, including separate archive preparation. Serial GC reduced
the five-case JDK 11 elapsed median from 112.40 to 102.25 seconds (9.0%) and CPU median
from 228.31 to 179.45 seconds (21.4%). On JDK 17, elapsed medians were 58.61 and 59.09
seconds, respectively: no elapsed improvement. These are within-run probe results, not
whole-matrix savings. JDK 17 retains its default collector; the broader comparison qualifies
the JDK 11 choice.

The optional `canonical-gc-matrix` hotspot dispatch extends the small collector probe to
both Flink lines on JDK 11. It runs the eight complete canonical classes already used for
the worker-count comparison: 64 invocations per phase, separate preparation for each
collector, then two alternating G1/Serial pairs with four fresh 1 GiB parent forks.
Each line must retain all class outcomes and invocation identities. The completed broader
probe passed all 768 invocations, including both collectors' separate archive preparation.
Each measurement runs the same 64 cases with four busy forks. Medians from two alternating pairs:

| Flink / JDK | G1 elapsed | Serial elapsed | Elapsed reduction | CPU reduction |
| --- | ---: | ---: | ---: | ---: |
| 1.18 / 11 | 734.42 s | 595.56 s | 18.9% | 21.2% |
| 2.2 / 11 | 786.30 s | 627.81 s | 20.2% | 22.7% |

The [retained trials](benchmarks/ci-profile-20261112.json) include preparation separately,
both successful runs and their source commits. These results select Serial GC for JDK 11
canonical jobs. They are measured worker-throughput gains, not whole-workflow savings;
the final combined CI matrix remains required.
The complete worker mixes use Maven's normal test lifecycle with precompiled Java and
runtime tests explicitly enabled, matching canonical CI. This resolves the JSON module
through the current reactor rather than depending on an installed project artifact.
Set the dispatch's `flink_line` to either release to qualify only that cohort; `all`
retains both. This allows repairing a failed setup without repeating a completed probe.
Workflow concurrency distinguishes the requested line, while job concurrency still prevents
duplicate probes for the same branch, mode, actual Flink line and JDK.

The `runtime-hotspots` dispatch also accepts `hotspot_mode=canonical-gc`. It compares
G1 and Serial GC on the same runner using the existing five canonical handoff cases,
on both JDK 11 and 17. Both collectors use first-tier compilation, the current 512 MiB
code-cache reservation and four fresh 1 GiB Surefire forks. Each collector prepares
its own job-local metadata archives before three alternating measurement pairs; archive
construction is reported separately. Exact invocation identities, successful outcomes,
and at least two worker archives remain mandatory. The probe retains clean stock/native
processes and their checkpoint assertions. It does not change the merge-gate JVM settings
or establish complete-matrix savings. Use an existing successful native-producing CI run
with identical native sources through `native_run`, as for the other hotspot diagnostics.

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

The [updated complete canonical matrix](https://github.com/jordepic/StreamFusion-contrib/actions/runs/42415314977)
passed all 32 shards with the same test selection. Summed runner time fell from 54.23 to
27.19 hours (49.9%), and the longest canonical job fell from 141.4 to 70.5 minutes.
These full-job times include archive preparation, compilation and artifact handling.
The [updated upstream workflow](https://github.com/jordepic/StreamFusion-contrib/actions/runs/42415315130)
passed in 110.2 minutes elapsed and 4.01 runner-hours. Its unchanged JVM settings and
different start delays mean that observed difference does not establish an upstream optimization.
Comparisons between hosted runs also include runner and cache variability; within-run paired
startup probes below help isolate the JVM settings.

Both complete source-head workflows passed. The full CI comparison, including every required
runtime, connector, image, Rust and savepoint job, was:

| Measurement | Baseline | Updated | Reduction |
| --- | ---: | ---: | ---: |
| Workflow elapsed | 321.6 min | 194.8 min | 39.4% |
| Summed runner execution | 72.33 h | 44.74 h | 38.1% |
| Canonical runner execution | 54.23 h | 27.19 h | 49.9% |
| Longest canonical job | 141.4 min | 70.5 min | 50.2% |

That source run's ordinary Java jobs consumed 14.55 runner-hours and the remaining jobs 3.00 hours.
The longest ordinary Java job took 103.6 minutes under the previous runtime JVM settings.
Elapsed time also includes shared runner capacity and
dependency delays. These results measure CI turnaround and execution cost, not SQL throughput.

The subsequent documentation-only evidence update exercised coverage reuse against that fully
passing source baseline. [CI](https://github.com/jordepic/StreamFusion-contrib/actions/runs/42430504179)
finished in 16 seconds and [upstream](https://github.com/jordepic/StreamFusion-contrib/actions/runs/42430504172)
in 15 seconds, each using 11 seconds of runner execution. Both plans verified the exact baseline's
two successful push workflows, every build/test dependency skipped, and both aggregates passed.
The strict documentation build also passed. Source/configuration changes still require full tests.

The **CI startup profile** workflow is an explicitly dispatched diagnostic, keeping profiling
cost outside ordinary pushes and merges. Select `runtime` (the default) or `canonical`.
Both build the exact commit's debug core library once.
The native producer builds the same complete workspace as ordinary native CI before sharing
its dependency cache, even when the selected Java probe only loads core. A core-only writer
previously seeded a 959 MB entry under the full workspace's key. The full build restored that
exact key but still compiled 405 units in 7m05s; Cargo attributed 315.19 seconds to RocksDB's
build step. The cache action then reported the entry up to date instead of enriching it.
[The action's save logic](https://github.com/Swatinem/rust-cache/blob/d1527c8f67ef9af357290475d4af4b0fab8f12e0/src/save.ts)
returns early on an exact hit. Keeping the producer scope consistent avoids that incomplete
entry. `native-only` runs just this build/cache diagnostic without starting Java comparisons.
The [warm-cache verification](https://github.com/jordepic/StreamFusion-contrib/actions/runs/42506483948)
on the corrected source restored the same key and completed Cargo in 29.50 seconds,
rebuilding 42 of 639 units with no RocksDB units rebuilt. This measures dependency reuse;
the Cargo duration excludes cache download and is not a repeated workflow comparison.

The runtime comparison covers Flink 1.18/2.2 and JDK 11/17. It runs the unchanged OVER,
decimal negate/absolute-value, string collection and stateful SQL rescaling classes with the
two fresh 4 GiB Surefire forks used by the runtime baseline. Three pairs alternate default and first-tier
compilation on each runner. Every trial must retain all 305 discovered cases, including exactly
two existing skips on Flink 2.2 or 58 on Flink 1.18. The profiler compares individual case names
and outcomes as a multiset, rejects missing classes, new skips, failures or inconsistent reports,
and records elapsed/CPU and per-class times. Run it locally after compiling the matching line:
`python3 bin/profile-runtime-ci.py --line 2.2 --repetitions 3 --output /path/to/new-evidence-directory`.
This diagnostic establishes whether compiler settings help the ordinary runtime mix; the complete
required workflows remain the qualification for any CI setting change.

The [completed runtime compiler comparison](https://github.com/jordepic/StreamFusion-contrib/actions/runs/42489473910)
retained all 7,320 discovered invocations: 6,600 passed and 720 had their existing skips,
with identical individual outcomes between settings. Uninstrumented medians across three pairs:

| Flink / JDK | Default elapsed | First-tier elapsed | Elapsed reduction | CPU reduction |
| --- | ---: | ---: | ---: | ---: |
| 1.18 / 11 | 203.95 s | 173.22 s | 15.1% | 53.1% |
| 1.18 / 17 | 244.90 s | 221.26 s | 9.7% | 47.6% |
| 2.2 / 11 | 325.16 s | 285.18 s | 12.3% | 37.2% |
| 2.2 / 17 | 275.08 s | 255.63 s | 7.1% | 28.7% |

The decimal negate/absolute-value class showed little elapsed change, while other classes
benefited more. These measurements establish improvements for the selected test mix and do
not establish a full-workflow improvement; the complete CI run remains necessary.

The subsequent [concurrency comparison](https://github.com/jordepic/StreamFusion-contrib/actions/runs/42492916674)
held first-tier compilation fixed and retained the same exact invocations/outcomes in all four
uninstrumented timing jobs. Medians across three alternating pairs:

| Flink / JDK | Two 4 GiB forks | Four 2 GiB forks | Elapsed reduction | CPU change |
| --- | ---: | ---: | ---: | ---: |
| 1.18 / 11 | 162.83 s | 108.67 s | 33.3% | +19.1% |
| 1.18 / 17 | 207.10 s | 138.82 s | 33.0% | +9.9% |
| 2.2 / 11 | 291.44 s | 293.22 s | -0.6% | +8.2% |
| 2.2 / 17 | 251.09 s | 213.19 s | 15.1% | +8.3% |

The initial smaller probe selected two 4 GiB forks for Flink 2.2/JDK 11. Its string collection class alone took
278.29 seconds with two forks and 287.18 with four; the other classes also slowed under four.
The other combinations selected four 2 GiB forks. These observations measure the selected test mix,
not the complete matrix. The timing workflow's separate initial JFR export failed after tests
passed; [the corrected JFR-only run](https://github.com/jordepic/StreamFusion-contrib/actions/runs/42495045635)
passed both instrumented modes. Its default run recorded 439.12 seconds of summed C2 compiler
activity intervals across four JVMs; the first-tier run recorded no C2 events. This is activity
attribution, not compiler CPU seconds or an instrumented speedup claim. Detailed counts,
sample summaries and all timing trials are retained in the November 8 evidence file.

All twelve runtime jobs in the [complete-matrix source run](https://github.com/jordepic/StreamFusion-contrib/actions/runs/42499556204)
passed. Summed runtime runner execution fell from 14.55 to 8.94 hours (38.5%); the longest
runtime job fell from 103.6 to 78.2 minutes. The required runtime matrix remains twelve jobs:
fork counts change Java processes within each runner. Grouped across all three shards per
Flink/JDK combination, the class report multiset retains identical test, failure, error and
skip counts, including the additional JVM-boundary verification on JDK 17.

| Flink / JDK | Previous runner minutes | Updated runner minutes | Reduction |
| --- | ---: | ---: | ---: |
| 1.18 / 11 | 173.82 | 88.68 | 49.0% |
| 1.18 / 17 | 192.40 | 110.18 | 42.7% |
| 2.2 / 11 | 231.08 | 194.37 | 15.9% |
| 2.2 / 17 | 275.50 | 143.38 | 48.0% |

All twelve runners reported four CPUs, with zero swap usage in the recorded `vmstat`
samples. These before/after hosted measurements include runner and cache variability;
the alternating paired probes above separately test the selected settings. The complete
source workflow subsequently passed in 161.6 minutes elapsed and 38.57 runner-hours,
compared with 194.8 minutes and 44.74 runner-hours previously (17.0% and 13.8% reductions).
All 32 savepoint artifacts retained the same partition inventories and class-level outcomes:
116 runnable classes, 3,620 cases and no skips per JDK. Savepoint execution changed little,
from 27.19 to 26.48 runner-hours, and its longest job increased from 70.5 to 73.0 minutes.
Both exact-head aggregates and strict docs passed;
[PR #614](https://github.com/datafusion-contrib/StreamFusion/pull/614) merged with its tested
head unchanged. These final measurements are recorded on the follow-up branch to preserve
that qualification.

Two further observations identify follow-up measurements without claiming an optimization.
One completed runtime job compiled its 1,029 test sources three times, taking approximately
10.01, 9.52 and 11.07 seconds; eliminating the later passes could address about 21 seconds
in that observed job. Reanalysis of the earlier default-JVM JFR recordings found 64.98
seconds of main-thread retry sleeps inside result collection and 80.72 seconds waiting on
futures in the string-collection class. Recorded waits exclude short unrecorded waits,
overlap asynchronous work and are not CPU time. They do not establish removable delay,
safe fixture reuse, or a measured benefit from changing polling.

Repeated lifecycle invocations now set `sf.protobuf.checkStaleness=true` after the job's
initial complete compilation. The protobuf generator otherwise rewrites unchanged test
sources, causing another compilation of the runtime's entire test inventory. A real Java 11
Maven fixture using the released protobuf/compiler plugins reproduced that rewrite and
dependent recompilation on an unchanged second pass. With staleness checking, the generated
source and dependent class timestamps stayed unchanged and javac compiled no sources.
A subsequent normal compile regenerated and recompiled an edited proto successfully.
Skipping the protobuf mojo entirely was rejected: it drops the generated source root and
the same fixture failed to compile its dependent source.

The initial compilation and ordinary Maven defaults keep staleness checking disabled, so
changes to proto inputs or generator versions still regenerate code. The optimization is
limited to subsequent invocations within the same immutable CI checkout, including package
verification. The observed 21-second repeated compilation remains a baseline observation;
the full updated workflow must establish the actual saving.

The next runtime investigation selects `runtime-hotspots` in the manually dispatched
**CI startup profile** workflow. It selects the five slow aggregate/state and recursive-conversion classes from
the completed Flink 2.2 runtime reports, retaining all 2,759 cases without skips. Both JDKs
use their current first-tier compiler and worker settings. This is one instrumented
attribution run, not a paired speedup measurement. Optional aggregate timers cover stock
and native fixture planning and private metrics-cluster startup/close; JFR records execution
samples, native stacks, compiler activity and sampled allocations. Ordinary tests leave
these timers disabled. Successful profiles remove recordings and temporary fixture data.

Supply a native-producing workflow run as `native_run`. The diagnostic verifies a successful
producer in the same repository and unchanged native sources/toolchain before downloading
its existing artifact, avoiding a duplicate native build. It has no push trigger and adds
no required jobs. Runtime evidence for the preceding PR is retained separately while that
PR's source head and required CI remain unchanged.

Set `hotspot_mode=parallel` to run two alternating pairs on JDK 11 using these five
classes, with first-tier compilation fixed and two 4 GiB versus four 2 GiB workers.
These comparisons have no JFR or fixture-timing instrumentation and must retain identical
individual outcomes across all four trials. The broader mix addresses a limitation of the
earlier probe: its JDK 11 elapsed time was dominated by the string-collection class.
The completed full Flink 2.2/JDK 11 jobs recorded 46.5–52.9% mean idle CPU in 30-second
`vmstat` samples. That observation motivates this comparison but does not prove that
additional processes improve its elapsed time or CPU cost.

The [broader worker comparison](https://github.com/jordepic/StreamFusion-contrib/actions/runs/42513596995)
completed all four trials with identical individual case outcomes: 11,036 invocations passed,
with no failures or skips. Two alternating pairs on the same JDK 11 runner measured median
elapsed time of 1,396.32 seconds with two fresh 4 GiB forks and 1,070.20 seconds with four
fresh 2 GiB forks, a 23.4% reduction. Child CPU time increased from 2,210.25 to 2,920.23
seconds (32.1%). Each individual class ran slower with four workers; additional overlap
shortened overall completion. The required runtime matrix now selects four 2 GiB workers
for Flink 2.2/JDK 11 as well, retaining the same twelve jobs and total Java heap budget.
The complete required matrix must qualify this setting. These gains are not multiplied
with the separate fixture-cache measurements.

JDK 11 compilation recordings identify the compiler by `compileLevel`; newer JDKs expose
the compiler name directly. The reader supports both schemas. Verified test outcomes,
elapsed/CPU measurements and fixture timings are saved before optional JFR decoding so
an attribution failure retains those measurements for offline analysis without repeating
the test execution.
The larger JDK 11 recordings also exceeded the consumer's 512 MiB constant-pool budget;
postprocessing now allows a 2 GiB reader heap after the test JVMs have exited. Both real
JDK schemas were decoded successfully, and the raw failed recordings were removed after
recovering their attribution locally.

The [completed attribution](https://github.com/jordepic/StreamFusion-contrib/actions/runs/42511164171)
retained all 2,759 cases without skips on both JDKs. On JDK 17 the fixture timers recorded
1,245.10 summed seconds in repeated aggregate/state plan construction, versus 148.70
seconds in private metrics-cluster startup and close. These intervals overlap across forks.
Both JDKs repeatedly sampled Flink's generated-code splitter and parser; inclusive frame
counts overlap and are not exact CPU attribution. The released 2.2.1 runtime bytecode
confirms that generated classes enter the splitter with the configured threshold.
The [follow-up evidence](benchmarks/ci-runtime-followup-20261108.json) retains class outcomes,
timers, sample summaries and the Java 11 postprocessing limitation.

The `hotspot_mode=plan-cache` experiment compares two alternating pairs on each JDK.
Only the two aggregate/state fixture builders participate: a cache retains
serialized unopened operator definitions, keyed by all planning inputs. Every fixture
deserializes independent operators, functions and serializers, then creates its existing
fresh harness/backend and runs every original assertion, snapshot and recovery stage.
The cache retains at most 128 definitions and 32 MiB per fixture builder; it never retains
open operators, backend state or a shared running job. These fixture builders enable it
by default; `-Dsf.fixturePlanCache=false` retains uncached construction for comparisons.
Both settings use released Flink's normal generated-code splitting configuration. Timed
comparisons have no JFR or fixture timers, hold the current worker/heap settings fixed,
and compare individual case outcomes across all 2,759 invocations in each trial.

The [completed fixture comparison](https://github.com/jordepic/StreamFusion-contrib/actions/runs/42517795512)
passed all 22,072 invocations across both JDKs without failures, errors or skips, retaining
identical individual case outcomes between settings. Medians across two alternating pairs:

| JDK | Reference elapsed | Cached definitions | Elapsed reduction | CPU reduction |
| --- | ---: | ---: | ---: | ---: |
| 11 | 1,042.88 s | 675.31 s | 35.2% | 52.5% |
| 17 | 1,068.60 s | 568.39 s | 46.8% | 58.8% |

Those recorded comparisons used the preceding PR's worker budgets: two 4 GiB forks on
JDK 11 and four 2 GiB forks on JDK 17. They establish fixture-construction savings within
those selected workloads, not a complete-matrix improvement or a product of separate
worker and cache gains. Future hotspot/cache comparisons follow the current four-fork
2 GiB budget. Worker-only and compiler comparisons explicitly disable fixture caching;
the cache comparison explicitly toggles it, and attribution runs use the current default.
Complete required CI must qualify the combined configuration and every remaining fixture.

`hotspot_mode=canonical-parallel` investigates the remaining savepoint execution cost
without adding required runners. It selects eight complete classes with comparable measured
durations (108–181 seconds on JDK 17 in the latest complete source run), retaining all
64 cases. One separate preparation phase runs those same cases and prepares worker
metadata. Two alternating pairs then compare two fresh 2 GiB parent forks with four fresh
1 GiB parent forks, holding the total parent heap cap at 4 GiB. Each running parent still
starts its existing fresh 1 GiB engine worker; the maximum configured parent-plus-worker
heaps are 6 GiB and 8 GiB respectively. Both variants use the existing first-tier compiler,
released Flink configuration and metadata archives. Preparation cost, complete case outcomes,
elapsed/CPU time and `vmstat` samples are retained. This is a controlled diagnostic;
the complete matrix remains necessary before changing the required savepoint worker count.

The [completed savepoint worker comparison](https://github.com/jordepic/StreamFusion-contrib/actions/runs/42518477689)
passed all 640 invocations across both JDKs including preparation, with no skips. Timed
measurement medians across two alternating pairs:

| JDK | Two parent forks | Four parent forks | Elapsed reduction | CPU increase |
| --- | ---: | ---: | ---: | ---: |
| 11 | 665.69 s | 540.48 s | 18.8% | 19.6% |
| 17 | 563.81 s | 414.06 s | 26.6% | 13.5% |

Separate preparation took 739.42 seconds on JDK 11 and 601.76 seconds on JDK 17;
each phase retained the same two metadata archives. Recorded samples showed zero swap
use on both runners. These comparisons select four fresh 1 GiB parents for the existing
savepoint jobs. Full source CI must still qualify all recovery assertions, cold setup,
complete class inventories and the combined configuration. This is test execution
throughput within each runner, not fewer cases or additional required runners.

The full required savepoint matrix subsequently passed all 32 jobs at source
`2f82b3401d5b1441d2302b37522af01847967054`. Artifact comparisons retained the exact
118-class partition inventory, 116 runnable classes, 3,620 cases and zero skips per JDK.
Including cold setup, combined runner time fell from 95,325 to 80,332 seconds (15.7%)
versus the preceding completed source run, saving 4.16 runner hours without adding jobs.
JDK 11 runner time fell from 54,117 to 44,631 seconds (17.5%), with the longest job
falling from 4,379 to 3,530 seconds (73.0 to 58.8 minutes). JDK 17 runner time fell from
41,208 to 35,701 seconds (13.4%), with the longest job falling from 3,242 to 2,928 seconds
(54.0 to 48.8 minutes). These are complete-matrix observations with runner/cache variability,
not a whole-workflow speedup. The same source failed a Flink 1.18/JDK 17 binary recovery
case because its result accumulator was missing, and fail-fast cancelled sibling runtime
jobs. Full runtime qualification and the cause of that failure remain unresolved; the
passing upstream suite and this savepoint group do not substitute for them.

Further attribution uses `hotspot_mode=deeper-runtime` on a separate source branch. It
retains all 1,817 recursive conversion cases and all 29 Fluss SQL cases on JDK 17/Flink 2.2.
Optional timers separate environment construction, SQL conversion, explain, submission,
collection, and Fluss broker/catalog lifecycle. JFR retains sampled execution, recorded
main-thread waits, GC activity and heap summaries; container samples account separately
for Fluss processes outside the test JVM. Instrumented durations are attribution, not
speedup measurements. Wait intervals exclude unrecorded short waits and can overlap across
threads; GC activity is not exact stop-the-world pause time and heap summaries are not RSS.

The [completed recursive attribution](benchmarks/ci-deeper-profile-20261108.json) passed all
1,817 cases without skips. Across its two JVMs, collection took 516.31 summed seconds;
JFR recorded 510.72 seconds of main-thread sleep in released Flink's collect-result retry.
Explain calls took 37.34 summed seconds and private-cluster start/close took 95.16 seconds.
The released 2.2.1 collector hard-codes a 100 ms retry rather than exposing a polling option.
`hotspot_mode=recursive-completion` therefore compares two alternating pairs with an optional
100 ms event-driven wait for job completion before collecting. A timeout falls through to
normal live collection, avoiding a deadlock when a result exceeds the collector buffer.
Both modes retain every row, original assertion and native execution check, four fresh
2 GiB forks and first-tier compilation. The comparison disables JFR and phase timers.
The [complete comparison](benchmarks/ci-deeper-profile-20261108.json) passed all 7,268
invocations without skips and with identical individual outcomes. Median elapsed fell
from 461.44 to 342.33 seconds (25.8%); child CPU increased from 605.48 to 628.22 seconds
(3.76%). Four forks were configured, but the two selected classes create two fresh JVMs
per trial; this is not a measurement of the complete runtime group under four busy forks.
The two recursive classes now opt into this brief completion wait through a dedicated
small-bounded-result helper. Other parity calls retain their existing collection behavior.
`sf.parity.awaitSmallResults=false` retains the control behavior for comparisons.
An added regression sends 8 MiB through the ordinary 4 MiB collect buffer, requiring live
draining after the brief timeout, complete stock/native row parity and positive native work.
`hotspot_mode=runtime-regressions` qualifies this case and the three encoding cases before
the complete required matrix. The focused JDK 17/Flink 2.2 run passed all four cases,
including complete stock/native rows and positive native work for the buffer fallback.
Complete source qualification subsequently passed; its final matrix evidence is in the
documentation-only `ci-profile-20261108.json` record. The other benchmark JSON files retain
their historical experiment and pre-qualification candidate snapshots.

The initial Fluss diagnostic could not load its new timing helper through a stale standalone
test dependency; it did not execute the 29 SQL cases. `hotspot_mode=fluss-details` attributes
that suite independently using the current Maven reactor and its test artifacts.
The [completed Fluss attribution](benchmarks/ci-deeper-profile-20261108.json) then passed
all 29 cases without skips: catalog closure took 116.20 seconds, with JFR recording 117.88
seconds of main-thread wait in the released client's Netty shutdown. Stock collection took
103.16 seconds, native collection 13.14 seconds and explain calls 3.92 seconds. Elapsed
time includes the current Maven reactor; recorded child CPU excludes broker containers.
Released Fluss calls Netty's default graceful shutdown without exposing a client quiet-period
option. `hotspot_mode=fluss-close` compares two alternating pairs of the existing serial
catalog closures versus closing the two independent catalogs concurrently on this same runner.
Both closures complete before the test continues; their errors remain failures and the
temporary executor is shut down. Each case retains fresh environments, catalogs and all
existing assertions. `sf.fluss.parallelCatalogClose=false` retains serial closure for comparisons.
The comparison disables JFR and phase timers and separately records Docker image preparation.
The [complete closure comparison](benchmarks/ci-deeper-profile-20261108.json) passed all
116 invocations without skips and retained identical individual outcomes. Median elapsed,
including the Maven reactor, fell from 324.26 to 267.13 seconds (17.6%). Child CPU changed
from 48.33 to 48.86 seconds (+1.09%), excluding the external broker containers. Docker
image preparation took 18 seconds separately; each trial retained fresh containers, JVMs,
environments and catalogs. These measurements select parallel catalog closure by default
for the qualification candidate, which subsequently passed the complete required matrix.

`hotspot_mode=recovery-diagnostics` repeats the complete 54-case binary collection class
on Flink 1.18/JDK 17 with alternating 2/4 GiB heaps, retaining its 28 existing skips.
All four trials retain that Flink line when selecting profiles and validating outcomes.
It logs recovery result-accumulator presence, encoded sizes and heap usage alongside JFR
and GC logs. The optional `ci-diagnostics` Maven profile supplies an error-level SLF4J
provider with explicit accumulator-registry warnings so task-side serialization errors are
visible. Released Flink 1.18 bytecode catches accumulator serialization failures, logs a
warning and returns no snapshot; the diagnostic retains those warnings even if a later
snapshot succeeds. Ordinary test logging is unchanged.
The [completed four-trial diagnostic](benchmarks/ci-deeper-profile-20261108.json) passed all
216 invocations with the same 112 existing skips and no serialization warning. One complete
831-row result encoded to 165,299,820 bytes. The 2 GiB trials recorded before-GC heap maxima
of 2,098,188,448 and 2,102,691,184 bytes, with full compactions. This confirms memory pressure;
the original missing-accumulator failure's cause remains unconfirmed.
`hotspot_mode=recovery-transport` compares two alternating pairs at the unchanged 2 GiB
heap with optional result-accumulator compression (`sf.recoveryResults.compress`). Checkpoint contents, captured rows and parity assertions are
unchanged. Only serialized accumulator rows use fast JDK deflate; decoding streams each
row through the released Flink serializer. The comparison uses ordinary test logging,
no JFR and no phase timers, while retaining encoded sizes and individual outcomes.
Separate round trips on released Flink 1.18.1 and 2.2.1 under Java 11 passed all three
codec cases: nested wide binary values, nulls, multiset counts, every changelog kind and
corrupt-input rejection.
The [complete transport comparison](benchmarks/ci-deeper-profile-20261108.json) passed all
four full-class trials with identical individual outcomes (216 invocations, 112 existing
skips). The wide 831-row result fell from 165,299,820 bytes to approximately 1.6 MB while
retaining every row. Median elapsed changed from 84.75 to 83.75 seconds (1.18%) and child
CPU from 107.95 to 104.17 seconds (3.50%). Elapsed ranges overlap; this establishes a
large transport-size reduction, not a material execution speedup. Point-in-time heap
messages are not peak memory measurements. The qualification candidate enables compact
transport by default, retaining `sf.recoveryResults.compress=false` for the original
transport comparison. That candidate subsequently passed full required CI.
Any failed trial remains a failed diagnostic; its verified counts and condensed failure
evidence are retained. Raw recordings and bulky successful reports are removed after analysis.

### Complete follow-up qualification

Source `eb87b1c588c83dd65b6c3ba708f36e6e53a87e5c` passed all
[62 CI jobs](https://github.com/jordepic/StreamFusion-contrib/actions/runs/42538650664),
[19 upstream checks](https://github.com/jordepic/StreamFusion-contrib/actions/runs/42538650683),
strict docs and the path-selected compatibility workflow. All 12 runtime jobs and 32
savepoint jobs remain. The [final source evidence](benchmarks/ci-profile-20261108.json)
compares every existing runtime class outcome counter and skip count with the preceding
complete passing source, accounting only for 24 added runnable regression cases.
The runtime matrix executed 102,464 cases with the same 3,212 existing skips; the savepoint
matrix retained 118 selected/116 runnable classes and 3,620 cases without skips per JDK.
The previously failing binary recovery case passed in its complete runtime job; its original
missing-accumulator cause remains unconfirmed.

| Completed execution | Previous runner hours | Combined runner hours | Reduction |
| --- | ---: | ---: | ---: |
| Runtime, 12 jobs | 8.94 | 8.05 | 10.0% |
| Savepoints, 32 jobs | 26.48 | 21.53 | 18.7% |
| Other CI, 18 jobs | 3.14 | 3.50 | -11.4% |
| Complete CI, 62 jobs | 38.57 | 33.08 | 14.2% |

The longest runtime job fell from 78.2 to 58.3 minutes; the longest savepoint job fell from
73.0 to 62.4 minutes. The Flink 1.18/JDK 11 cohort used 5.8% more runner time, while the
other cohorts improved; all individual cohort observations are retained. These are
cross-run observations including cold setup and runner/cache variability, not products of
the selected paired gains.

Whole-workflow elapsed time increased from 161.6 to 230.8 minutes. Median delay between
the completed native dependency and Java/savepoint job starts increased from 36.4 to
90.7 minutes, including runner waiting and orchestration. No end-to-end wall-clock speedup
is established by this run. The default-branch cache experiment above separately removes
the measured cold native dependency rebuild from subsequent matching branch builds.

The `runtime-parallel` diagnostic holds first-tier compilation fixed and compares two fresh
4 GiB forks with four fresh 2 GiB forks in three alternating pairs on every Flink/JDK combination.
It retains the exact cases, outcomes and total 8 GiB heap cap; additional VM overhead is still
part of the measured workload and full CI qualification. It also runs a separate JDK 17/Flink 2.2
JFR attribution pair, selectable alone as `runtime-jfr`. That pair records Java execution/native
method samples, every compiler activity event and sampled allocations. A small Java 11-compatible
reader consumes `RecordingFile` directly and streams compact events to the summarizer; it avoids
the JDK XML printer's large buffered rendering, which exhausted its heap on the initial real trace.
Summaries retain sample
counts and top stack frames, compiler activity durations and allocation weights; successful raw
recordings are removed. Java execution and native-method samples remain separate: native samples
include blocking I/O and are not CPU attribution. Inclusive stack counts overlap, native samples do not resolve Rust
internals, compiler activity intervals are not compiler CPU seconds, and allocation weights are
sampled estimates. Instrumented elapsed times are excluded from speedup comparisons.

The canonical diagnostic compares default and first-tier
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

The subsequent [hosted archive comparison](https://github.com/jordepic/StreamFusion-contrib/actions/runs/42415314960)
passed all 100 invocations, including preparation. Median five-case elapsed times were:

| JDK | Default | First-tier | First-tier + prepared archive | Reduction vs default |
| --- | ---: | ---: | ---: | ---: |
| 11 | 182.86 s | 134.36 s | 109.76 s | 40.0% |
| 17 | 141.17 s | 104.96 s | 77.33 s | 45.2% |

Prepared-archive ranges were 109.10–110.72 seconds on JDK 11 and 76.96–78.40 seconds on
JDK 17 across three repetitions. CPU medians fell from 553.82 to 225.61 seconds (59.3%)
and from 415.20 to 145.84 seconds (64.9%), respectively. The separate preparation phase,
including five passing cases and archive construction, took 189.88 seconds on JDK 11 and
105.39 seconds on JDK 17. These are comparisons within each runner; the different default
times between runs and JDKs also show why probe results cannot establish whole-workflow savings.

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

- [CI 42616136585](https://github.com/liuyongvs/StreamFusion/actions/runs/42616136585):
  the JSON UNION fallback Top-N fixture sorted only by a nullable value and compared raw
  changelog histories from independent executions. UNION input interleaving changes transient
  updates, and tied sort values leave the selected row identities unspecified. The fixture now
  adds the unique row ID to the sort key and checks the materialized final changelog against
  all 17 expected IDs, NULL values and multiplicities, with and without chaining. Complete
  fallback and absence of native execution remain required; other cases retain their existing
  changelog comparisons, including explicit coverage of every row kind across native UNION.
- [Upstream suite 42598514603](https://github.com/liuyongvs/StreamFusion/actions/runs/42598514603):
  Paimon's append-only external-path refresh test returned all five expected rows but found no
  file in the original directory. It submits INSERT asynchronously and immediately changes the
  path, allowing the first checkpoint to refresh the writer before its first file. The suite
  adapter waits for an actual file in the original directory before that test's ALTER statement,
  retaining its SQL, all rows and both directory assertions. Missing files, I/O errors and
  interruption remain failures; the wait is limited to 60 seconds and this single test invocation.
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

The runtime Java CI command bounds each Surefire fork at 30 minutes. A fork that hangs after
reporting its test results fails the gate instead of occupying a runner until the full job timeout;
the test selection and assertions remain the same. Canonical child-worker timeouts embed their
logs and thread dumps in the failure report for diagnosis.
