# Nested Java scalar comparisons

The nested scalar implementation remains default off. On October 16 the maintainer
accepted shipping its verified semantics despite the measured performance regressions,
with further optimization deferred. Correctness and allocation profiles do not establish acceleration. The release diagnostics compare complete jobs
against stock Flink and immutable StreamFusion revision
`b170a474d5658330ed7e92e9b8bcc54117c8d287`, before nested scalar admission.

`bin/bench-nested-scalars.py` creates a separate checkout of that revision, copies only
three current test fixtures and checks that previous production sources remain unchanged.
It builds the actual previous native library and Java runtime. A shared Cargo target reuses
dependency compilation; separate pinned libraries preserve each runtime's code. Classpath
and loader library hashes must agree. All builds finish before any measurements.

Each runtime executes identity and reusable-transform callbacks with nominal string widths
16 and 256, plus a primitive Rust calculation control. Input includes NULL rows, nullable
fields/arrays/elements and all four nested RowKinds. The source appends a varying suffix
to the configured string prefix. Default workloads use 2 million rows, parallelism one,
2 GiB heap, two warmup pairs and five alternating measured pairs. Production IPC and both
row/Arrow transposes remain in the execution. EXPLAIN and each job's cluster setup occur
before the clock; the complete `executeSql().await()` is timed. Native plans must consume
rows, stock plans must execute no native operators, and previous nested callbacks must
fall back. The previous primitive calculation must still execute natively.

Every runtime has its own stock controls. Keep all trials and ranges: cross-JVM stock
drift prevents assigning the complete observed difference solely to the new implementation.
Raw commands, fixture/library hashes, build/query logs, GC and safepoint logs, and trial
arrays are retained in `results.json` and adjacent files. No admission is automatic.

After an ordinary candidate release build, run with JDK 17:

```bash
python3 bin/bench-nested-scalars.py --flink-line 2.2 \
  --previous-worktree /tmp/streamfusion-previous-nested \
  --native-directory native/target/release \
  --output native/target/nested-scalar-diagnostics
```

The previous worktree path must be new. Use `--flink-line 1.18` after building the matching
candidate compatibility line. `--smoke` runs the same ten runtime/shape/width combinations
with 2,000 rows and one warmup/measured pair; its durations are excluded from performance
evidence. `--cargo-target-directory` selects a dependency build cache without changing
the previous source revision or pinned runtime library.

[Local smoke evidence](results/nested-scalar-runtime-smoke-2026-10-16.json) retains all
20 successful workload checks across both released Flink lines, clean previous production
trees, distinct candidate/previous library hashes and the exact executed smoke runner.
The local 1.18 candidate uses copied sources; every production Java source matches the
candidate revision and its loaded library matches the pinned candidate release. All smoke
durations are excluded.

With `--profiler-agent /path/to/libasyncProfiler.so` from released async-profiler 4.0,
separate identity, transform and Rust control jobs record Java/native CPU-clock samples
at 2 ms with DWARF stacks and Java allocation samples at 1 MiB, for both runtimes.
These jobs use at most one million rows and three measured pairs. Their durations are
excluded. Allocation samples estimate allocation volume, not copied bytes or retained
heap; inclusive stacks overlap. Rust control costs cannot be attributed to a callback
shape that does not execute the same Rust work.

The `Nested scalar release diagnostics` workflow runs these comparisons on separate
Flink 2.2.1 and 1.18.1 hosts, verifies callback parity first and uploads full mixed JFR
recordings and logs even on failure. It fingerprints native inputs, compiler and build
flags before reusing a release payload. A superseding run cancels older diagnostics.
The [completed isolated matrix](results/nested-scalar-isolated-2026-10-16.json) retains
all 20 unprofiled runtime/workload comparisons, 12 mixed Java/Rust CPU/allocation recordings
as collapsed stacks, full trial arrays and GC/query logs. Both jobs succeeded. Candidate
callback medians regress by about 30–53%; this is coverage groundwork, not acceleration.

| Callback / nominal width | Flink 2.2 stock/native median, s | Flink 1.18 stock/native median, s |
| --- | ---: | ---: |
| identity / 16 | 2.346 / 3.576 | 1.231 / 1.815 |
| reusable transform / 16 | 2.559 / 3.610 | 1.273 / 1.947 |
| identity / 256 | 3.525 / 4.883 | 1.920 / 2.717 |
| reusable transform / 256 | 3.940 / 5.194 | 2.305 / 3.007 |

The measured merge is `83970cab7d69bac237ec41b0db518c001af777cc`; production/native/build
inputs and the three timing fixtures match the recovery/deployment candidate `eab6d2ec`.
Later integration with main is not measured by this matrix. Actual previous nested
callbacks retain fallback and their independent stock controls. Raw JFRs remain in the
[original CI artifacts](https://github.com/datafusion-contrib/StreamFusion/actions/runs/38889442933);
checked-in compressed payloads retain complete collapsed CPU/allocation stacks and logs.
[Coverage and remaining contracts](../operators/calc-filter.md#experimental-nested-java-scalar-results)
continue to govern the opt-in whitelist.

Recovery is verified separately from timing by `FlinkNestedScalarRecoveryTest`: four
memory/RocksDB and parallelism-one/three cases per released line. Both completed-checkpoint
failures must restore their exact source offsets and yield the same 48 results as uninterrupted
stock Flink. Identity callbacks retain nested kinds; reusable transform callbacks exercise
owned results across batch flushes and task recreation. See the
[recovery evidence](results/nested-scalar-recovery-2026-10-16.json). Run with the same pinned
native resource/loader paths and `streamfusion.experimental.nestedScalarUdfs=true` as the
benchmark, selecting `-Dtest=FlinkNestedScalarRecoveryTest`. These correctness checks do not
establish throughput.

`FlinkNestedScalarRemoteDeploymentTest` separately submits from the planner JVM to a child
TaskManager JVM over REST. Both JVMs enable the experimental flag. Four checks across the
two released lines verify 5,003 stock/native results at parallelism one and three, record
worker PIDs and native consumed-row counts, and require clean native resources before child
exit. The [cross-JVM evidence](results/nested-scalar-remote-2026-10-16.json) retains these
observations. It covers one-host remote submission; process-failure recovery and inconsistent
planner/task flags are outside this fixture.
