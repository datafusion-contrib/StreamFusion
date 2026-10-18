# Iceberg boundary optimization and fast iteration

Supported Iceberg streaming contracts accelerate by default. These changes reduce
source/sink setup, Arrow handoff and Java routing costs. Java still owns discovery,
partition transforms, rolling checks, deletes, checkpoints and commits; Rust reads
Parquet into Arrow and encodes data files from Arrow.

Use the [fast file-task loop](../iceberg-io.md) for development:

```sh
bin/bench-iceberg-io.py --output target/io-before
# Edit the connector, then compare against the saved native observations.
bin/bench-iceberg-io.py --baseline target/io-before/observations.jsonl
bin/bench-iceberg-io.py --cases route
```

On this host the default process takes about 8–9 seconds including validation,
and routing alone takes about two seconds. An unchanged build check takes about
0.04 seconds; incremental connector Java compilation takes roughly 1.5–3.5 seconds.
Initial release compilation and Maven preparation are separate costs. This keeps
cluster startup, query state and commit waits out of the inner development loop.

## Matched component comparison

Release + mimalloc, JDK 17, Flink 2.2.1, Java Iceberg 1.12.0, iceberg-rust 0.10.1,
Arrow/Parquet 58.3, 4 GiB heap, local warm filesystem. Host: Intel i7-12650H,
16 logical CPUs, WSL2 Linux. Each engine consumes 262,144 deterministic six-column
rows, with nullable scalar values and Unicode strings, four bucket partitions,
16 input files and 8,192-row fetch batches. Zstd 3, 128 MiB row groups, 1 MiB
pages, 2 MiB dictionaries and a 512 MiB rolling target match. Each cohort uses two
warmups and five measurements, alternating implementation order.

| Boundary | Previous native median [range], ms | Current native median [range], ms | Current cohort stock median [range], ms | Stock/current | Previous/current |
|---|---:|---:|---:|---:|---:|
| Read | 52.72 [48.15–57.38] | 47.25 [44.03–53.46] | 122.78 [107.64–208.05] | 2.60× | 1.12× |
| Write | 156.65 [155.03–161.85] | 141.36 [136.35–146.95] | 197.54 [190.58–221.95] | 1.40× | 1.11× |
| Route destinations | 26.69 [26.12–27.59] | 12.58 [11.72–18.81] | 27.40 [26.29–83.73] | 2.18× | 2.12× |

Raw final cohorts: [previous read/write](quick-release-before/observations.jsonl),
[current read/write](quick-release/observations.jsonl),
[previous routing](quick-route-before/observations.jsonl),
[current routing](quick-route-final/observations.jsonl). Stock read/write medians in
the previous cohort were 107.09 [99.05–120.16] / 197.17 [187.65–217.66] ms;
previous-cohort routing was 28.96 [25.31–67.69] ms. Cohort drift and overlapping
read ranges limit attribution; these are repeated local observations.

The reader measurement includes Java-planned file-task setup, real FileIO,
metadata translation, JNI, native decode, Arrow C Data import and close/recycle.
Its timed consumer counts records; separate scans verify every field and ID.
The writer starts with equivalent prebuilt RowData/Arrow inputs and times the
production task writer, partitioning, rolling, JNI, encoding, close and metrics.
Every output is decoded by stock Java Iceberg and checked for values, IDs and the
expected encoder footer; all trials require released native handles/reservations.
Routing verifies every destination against the original Java selector; it excludes
Rust batch splitting, transport and key-group-to-writer mapping. File-task timings
exclude Flink scheduling, discovery, shuffle, state, checkpoints and table commits.

## Focused streaming confirmation

The unchanged Iceberg Nexmark harness ran q0/q1 with two million events,
parallelism four, four-bucket input/output tables, memory state and mini-batch off.
Timing includes streaming completion, checkpoints and final drain/commit. Previous
code has one warmup/five measured pairs; current code has one warmup/three pairs.

| Query | Previous native median [range], s | Current native median [range], s | Current cohort stock median [range], s | Stock/current |
|---|---:|---:|---:|---:|
| q0 | 0.666 [0.582–0.756] | 0.616 [0.597–0.648] | 1.175 [1.170–1.415] | 1.91× |
| q1 | 0.671 [0.650–0.707] | 0.601 [0.590–0.692] | 1.208 [1.175–1.214] | 2.01× |

All eight final pairs including warmups pass committed-output parity. Native
witnesses require Arrow sources/writers, native Parquet tasks, no stock task
fallbacks, no row transposes and no query operator fallbacks. Plans, graphs,
metadata and checkpoint file sizes are in the
[final evidence archive](nexmark-routing-after/evidence.tar.gz), with
[observations](nexmark-routing-after/observations.jsonl) and
[audit](nexmark-routing-after/audit.json). The
[previous cohort](before/results.md) used stock medians 1.032 / 1.138 seconds.
Native medians fell about 7% / 10%, but ranges overlap and stock timing drifted.
This focused confirmation does not replace or claim to improve every cell in the
[complete earlier matrix](../iceberg-final-20261017/README.md).

The separate dynamic writer diagnostic retains two million generated Arrow rows,
one warmup/three pairs, four-way parallelism, four buckets per target, v2 tables,
one-second checkpoints, a non-draining savepoint and terminal draining savepoint.
It validates all committed values with Java Iceberg; keyed mode repeats 100,000
keys and retains Java delete encoding. Both cohorts pass all 16 trials.

| Dynamic mode | Previous native median [range], s | Current native median [range], s | Current stock median [range], s | Stock/current |
|---|---:|---:|---:|---:|
| Forward/hash append | 2.301 [2.220–2.480] | 2.571 [2.341–2.684] | 2.500 [2.308–3.150] | 0.97× |
| Keyed | 3.646 [3.551–3.725] | 3.546 [3.407–3.998] | 3.122 [3.031–3.532] | 0.88× |

[Before](dynamic-before/results.md) and [after](dynamic-after/results.md) retain
raw observations, graphs, final metadata, checkpoint sizes and logs. These results
remain slower than stock and do not establish an overall dynamic writer win.
[Issue #387](https://github.com/datafusion-contrib/StreamFusion/issues/387) tracks
remaining dynamic routing costs. Acceleration remains on by default.

## Where time and allocations went

The original q0 CPU diagnostic aggregates three measured pairs after warmup, using
async-profiler `itimer` for both engines. [Inclusive counts](inclusive-cpu.csv)
show source fetch at 102 stock / 53 native samples and sink processing at 154 / 124.
The native sink has 98 drain, 90 length-check and 83 native-write samples; the
hash router has 95. Java Parquet add has 123 stock samples and zero native samples.
Total JVM samples are 1,477 stock / 833 native. Counts overlap, include JVM work
and represent sampled CPU rather than wall-time attribution.

The separate original allocation diagnostic has 17,720 native sample weight,
including 5,980 under routing, 4,238 under partition-path creation, 1,269 under
key-group range calculation and 465 under sink C Data export. These are profiler
sample weights, not allocated object counts or bytes. [Counts](allocation-samples.json)
and the original [CPU](before-cpu/evidence.tar.gz) /
[allocation](before-alloc/evidence.tar.gz) stacks are retained.

The implemented response is bounded and task-local: cache exact Java-selected
key groups for immutable integer partition tuples, precompute writer ranges,
retain one imported Arrow batch across its file drains, reuse the fixed source
schema and retain JSON serializer caches across file tasks. The
[optimization ledger](../../optimizations/iceberg-boundary-overhead.md) explains
ownership and source inspiration. No Java partition transform is replaced.

A short allocation-profile smoke run of the new `--profile` command is retained in
[quick-route-alloc](quick-route-alloc/observations.jsonl); its timings are diagnostic.
The earlier [whole-job CPU/wall/kernel investigation](../iceberg-profile-20261017/README.md)
identified substantial state snapshots and filesystem waits in slower state-heavy
queries. Those costs remain tracked in
[issue #391](https://github.com/datafusion-contrib/StreamFusion/issues/391).

## Validation and retained experiments

The serial release suite passes **302 tests**, with **13 batch-only exclusions**,
across 36 classes: SQL, append/keyed/dynamic/range, types, predicates, evolution,
source resume/cancellation, captured-channel recovery/rescaling and upstream
streaming cases. The new tests cover shared leases across inputs/files/drains and
nullable partition groups across cache eviction. Six release Rust unit tests pass.
Criterion smoke passes 72 C Data fixtures (54 new fanout cases) and all 126 Iceberg
fixtures. The handoff allocation CSV also contains 24 existing transformation
probes. [Summary and library hashes](validation/summary.json),
[JUnit reports](validation/reports.tar.gz), compressed logs and
[allocation/buffer-sharing probes](validation/handoffs/allocations.csv) retain the
checks. Those probes measure requests and sharing, not copied bytes or job speedups.

The original implementation is commit `1efcbff4`; saved baseline class hashes are
in [before/class-files.json](before/class-files.json). The shared current benchmark
harness is used with those classes and the original release libraries. Older
baseline environment records inherited preparation fingerprints; copied records
explicitly remove misleading production-source hashes. Method records match for
the final before/after component pairs. Current production
[source hashes](production-files.json), [class hashes](production-classes.json)
and per-cohort environment/library hashes identify the measured uncommitted
implementation that ships with this report.

Intermediate cohorts remain visible: `shared-json`, `shared-batch` and
`nexmark-after` record partial changes including unfavorable end-to-end waits.
`before-20m` is a 20-million-event baseline-only probe; its roughly seven-minute
process motivated the smaller development loop. `quick-current`, `quick-before`,
`quick-current-2`, `quick-final` and `quick-before-final` predate matched source
fetch sizes (stock 2,048/native 8,192); they are exploratory results rather than
the final stock comparison. `quick-matched-*` retain matched-fetch repetitions;
`quick-route` retains the initial routing probe. No unfavorable cohort was dropped.
