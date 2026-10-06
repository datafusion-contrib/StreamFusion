# Reproduce the performance audit

The five audit candidates are pending rowtime deduplication, sparse interval probing, window
output handoff, Delta checkpoint buffering, and Paimon partition pruning. The benchmark-only
commit adds their fixtures while preserving production behavior from `96f98a32`; subsequent
optimization commits change production. Run the identical fixture source and configuration on
that benchmark-only commit and each relevant optimization commit. Record both Git revisions and
fixture hashes. Do not compare an old fixture with a new one, or call a fixture-only commit an
optimized implementation.

Use Java 17 consistently (`JAVA_HOME` and `PATH` must select an installed JDK 17), released
connector artifacts, and release native libraries. Maven's `bench` profile builds release native
code with mimalloc and disables local zero-copy exchange for these comparisons. Criterion runs
below use the default allocator unless `--features mimalloc` is explicitly selected; label that
choice and never mix allocator configurations in a before/after ratio. If using
`-Dnative.build.skip=true`, first build and verify every loaded native library for that exact
checkout, release profile, and allocator. Otherwise leave native building enabled as below.

Run suites serially on an idle shared host. Before each run record CPU/JDK versions, native
library hashes, resource limits, allocator/build flags, and competing activity. Recheck activity
between trials: Maven's single benchmark fork does not exclude external jobs. If another
benchmark, build, or sustained workload starts, retain the contaminated logs, mark them, and rerun
the affected complete before/after pair when quiet. Do not mix quiet baseline data with a
contended candidate. Separate JVMs can drift through JIT, GC, temperature, and shape order;
retain every observation, median, range, and IQR instead of selecting favorable trials.

## Native arrival costs

The reported short diagnostic runs used the following direct commands on the benchmark-only
checkout, followed by the same commands on the candidate with `--baseline audit-before` in place
of `--save-baseline audit-before`. Preserve the Criterion baseline directory while rebuilding;
copy immutable result artifacts before subsequent runs overwrite estimates.

```bash
cargo bench --manifest-path native/Cargo.toml --locked -p streamfusion --bench dedup_pending -- \
  --sample-size 10 --warm-up-time 0.3 --measurement-time 0.5 --save-baseline audit-before
cargo bench --manifest-path native/Cargo.toml --locked -p streamfusion --bench interval_probe -- \
  --sample-size 10 --warm-up-time 0.3 --measurement-time 0.5 --save-baseline audit-before
```

On the candidate checkout:

```bash
cargo bench --manifest-path native/Cargo.toml --locked -p streamfusion --bench dedup_pending -- \
  --sample-size 10 --warm-up-time 0.3 --measurement-time 0.5 --baseline audit-before
cargo bench --manifest-path native/Cargo.toml --locked -p streamfusion --bench interval_probe -- \
  --sample-size 10 --warm-up-time 0.3 --measurement-time 0.5 --baseline audit-before
```

These release diagnostics use the default allocator and the operators' unaccounted test-budget
constructors. They do not measure task memory-budget reservation, rejection, or pressure-driven
compaction costs. The short warmup, measurement target, and ten samples make them diagnostics;
use the longer default runner commands below as confirmation before broad performance claims.
Rust labels `bytes=8/264` describe repeated `x` suffix widths, not total UTF-8 payload length:
dedup strings also contain initial/arrival and key/ordinal prefixes, and interval strings contain
an ordinal plus separator. Keep these exact payload constructions identical before/after.

Discover suites and validate fixtures before timing:

```bash
python3 bin/bench-native.py --list
python3 bin/bench-native.py --package streamfusion --bench dedup_pending --bench interval_probe --smoke
python3 bin/bench-native.py --package streamfusion --bench dedup_pending --bench interval_probe --save-baseline audit-before
```

Run the same suites on the candidate, with `--save-baseline audit-after`, and retain runner logs,
metadata, allocation diagnostics, and Criterion estimates. Baseline names alone do not isolate
checkouts: use distinct artifact directories/targets or preserve immutable outputs before the
next build. `--smoke` executes oracles once and is not a timing result. These two suites use Criterion's defaults: three-second warmup, five-second measurement target,
and 100 samples per case. Record the configuration with the estimates; do not substitute smoke
elapsed time for repeated measurements.

`dedup_pending` times production arrivals over retained pending winners. `interval_probe` times
production arrivals against existing interval state, including output creation and eager cleanup.
Input construction and retained-state setup are outside timing. These boundaries omit full job
startup, ingress/egress transposes, watermark drains, and checkpoint work.

Complete stock/native SQL comparisons preserve those production edges:

```bash
SF_BENCHMARK=true mvn -Pbench -pl streamfusion-runtime -am test \
  -Dtest=PendingDedupBenchmark -Dsurefire.failIfNoSpecifiedTests=false \
  -Ddedup.rows=262144 -Ddedup.keys=16384 -Ddedup.width=8 -Ddedup.warmup=2 -Ddedup.runs=5
SF_BENCHMARK=true mvn -Pbench -pl streamfusion-runtime -am test \
  -Dtest=SparseIntervalJoinBenchmark -Dsurefire.failIfNoSpecifiedTests=false \
  -Dinterval.rows=65536 -Dinterval.keys=16384 -Dinterval.probes=4096 \
  -Dinterval.match-keys=256 -Dinterval.width=264 -Dinterval.warmup=2 -Dinterval.runs=5
```

SQL `dedup.width` and `interval.width` likewise specify repeated suffix lengths; dedup payloads
also include an ordinal and separator, while interval payloads include side/ordinal prefixes.
They are not total string-byte widths.

Retain the wider dedup and other cardinality/width cases in the ledger when testing their claims;
one favorable shape cannot establish every admitted workload is faster.

## Window shaping, flush, and complete jobs

```bash
SF_BENCHMARK=true mvn -Pbench -pl streamfusion-runtime -am test \
  -Dtest=WindowOutputShapingBenchmark -Dsurefire.failIfNoSpecifiedTests=false \
  -Dshaping.warmup=20 -Dshaping.runs=31
SF_BENCHMARK=true mvn -Pbench -pl streamfusion-runtime -am test \
  -Dtest=WindowOutputHandoffBenchmark -Dsurefire.failIfNoSpecifiedTests=false \
  -Dhandoff.warmup=3 -Dhandoff.runs=9
SF_BENCHMARK=true mvn -Pbench -pl streamfusion-runtime -am test \
  -Dtest=WindowOutputHandoffJobBenchmark -Dsurefire.failIfNoSpecifiedTests=false \
  -Dhandoff.job.rows=262144 -Dhandoff.job.groups=65536 \
  -Dhandoff.job.warmup=2 -Dhandoff.job.runs=5
```

Repeat the complete-job command with `-Dhandoff.job.properties=0` and
`-Dhandoff.job.properties=2` to retain both output-property shapes.

Shaping isolates production final-window output handling using prepared Arrow results. Full flush
also includes native aggregate flush and C Data import. Complete jobs retain both transposes,
computation, collecting output Row copies, and closing the result iterator. Validate exact keys, properties,
payloads and ownership outside the timed boundaries where the fixtures specify. Keep unfavorable
shape/property results and stock drift; a faster isolated shaping path is not a complete-job
speedup. Matching Flink 1.18 checks use `-Pflink-1.18,bench`; the recorded Flink 2.2 comparisons
must remain separately labeled.

## Delta live buffers

```bash
SF_DELTA_BUFFER_BENCHMARK=true SF_DELTA_BUFFER_REPEATS=7 \
  mvn -Pbench,delta -pl streamfusion-delta -am test \
  -Dtest=DeltaBufferingBenchmark -Dsurefire.failIfNoSpecifiedTests=false
```

The fixture warms each shape once, then measures append and 16-key repeated upserts over
1,024-row batches, 4,096/32,768 rows, and 16/256-byte payloads. `SF_DELTA_BUFFER_ROWS` can narrow
sizes; retain the same value before/after. Only production writer arrivals are timed. Prebuilt
Arrow inputs, checkpoint commit, released-Kernel exact final-state readback, and bounded cleanup
retries are outside timing. Retained Arrow ownership and thread heap allocations are different
metrics. Input/write allocation requests are not bytes copied, and zero write-phase Arrow
allocation requests do not prove no copying. This fixture does not replace the connector's
complete stock/native sink pipeline comparison.

## Paimon full drain and SQL prefixes

```bash
SF_PAIMON_PARTITION_PRUNING_BENCHMARK=true SF_PAIMON_PARTITION_ROWS=131072 \
  SF_PAIMON_PARTITION_REPEATS=3 mvn -Pbench,paimon -pl streamfusion-paimon -am test \
  -Dtest=PaimonPartitionPruningBenchmark -Dsurefire.failIfNoSpecifiedTests=false
SF_PAIMON_PARTITION_SQL_BENCHMARK=true SF_PAIMON_PARTITION_ROWS=262144 \
  SF_PAIMON_PARTITION_REPEATS=5 mvn -Pbench,paimon -pl streamfusion-paimon -am test \
  -Dtest=PaimonPartitionSqlBenchmark -Dsurefire.failIfNoSpecifiedTests=false
```

Both fixtures warm each shape once. The full-drain fixture actively reads the shown row/repetition
environment variables, with defaults 131,072 rows and three observations. It compares unfiltered native,
directly-pruned native, and released stock pruning at two payload widths. It consumes every
planned split and checks decoded/selected rows, checksums, and actual native file-open witnesses.
Planned file bytes remain metadata totals, not measured I/O. Direct pruning uses the released
read-builder API and production native reader but bypasses SQL planning.

SQL compares stock/native queries for p00, p31, and all partitions on one immutable table. Add
`SF_PAIMON_PARTITION_REQUIRE_PRUNING=true` only to candidate runs to require the selective native
plan witness; baseline must run the identical fixture without requiring a newly introduced hint.
Keep source-transformation witnesses in both. Selective live queries cancel after their expected
result prefix, possibly before unrelated partitions decode. Names p00/p31 do not establish runtime
scheduling endpoints; the fixture logs released planning order. Only the unfiltered case collects
every initial snapshot row. Prefix latency, isolated full-drain scan reduction, and sustained SQL
throughput must be reported separately.

## Retained results

Publish per-technique conclusions and raw observations through the
[optimization ledger](../optimizations/index.md). Keep every trial, build configuration, stock
comparison, benchmark-only revision and production revision with the artifacts.
`bin/export-criterion-comparison.py` preserves native estimates and raw iteration/time arrays;
`bin/collect-paimon-pruning-results.py` preserves full-drain counters and optional paired SQL
source names, planning witnesses and samples.
