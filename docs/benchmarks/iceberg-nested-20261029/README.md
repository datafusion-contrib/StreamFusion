# Nested Iceberg and sorted/equality maintenance

Release + mimalloc measurements on 2026-10-29. [All repeated results and ranges](results.md)
retain the slower standalone 2M spill cohort alongside isolated repeats. Running
streaming jobs show 3.24×/5.90× in-memory sort, 3.01×/4.35× forced-spill sort, and
1.85×/2.83× composite-equality commit-latency ratios at 400k/2M rows. Nested file
workers show 2.30×/2.62× unsorted and 6.17×/3.59× sorted ratios versus the stock
controls. These are separate timing boundaries. The two timing boundaries below
are separate: a file worker and commit latency in already-running Flink streaming
maintenance graphs. Neither is a Flink batch-execution benchmark.

## Workload and validation

Four prebuilt Parquet files contain 400,000 or 2,000,000 rows in total, with unique
permuted integer IDs, nullable Unicode strings (4,096 distinct values), and nullable
long shard keys (64 values). Nested cases add a nullable struct, nullable lists with
null elements, and nullable map values. Both engines use the same logical values,
Zstd level 3, writer properties, task resources, Java FileIO and Java partition/rolling
policy. Fixtures, plans and correctness readback are outside the file-worker timer.

Sort cases order by string ascending/nulls first, then ID descending/nulls last.
Normal sorting has 512 MiB; forced-spill sorting has 8 MiB per worker. Stock uses
Flink's production BinaryExternalSorter and generated comparator, with default spill
compression enabled. Native uses DataFusion SortExec. Positive spill counts and bytes
are required for both engines at both measured sizes; the memory cohort must not spill.
Stock data files use Iceberg's normal Flink Parquet writer, including the declared
sort-order metadata. Native uses the production Rust reader, deletes, sorter and writer.

Equality cases use sparse integer keys, nullable strings, or nullable (long, string)
tuples, with v2/v3 sequence applicability decided by Iceberg's planner. Composite sets
contain 260 key tuples, including null combinations. Fixture/delete-file encoding is
untimed. Each measured output is checked with stock Iceberg readback using a full-row
count/sum/xor fingerprint; sorted outputs also require correct order and sort-order ID.
Every footer must identify the intended encoder. Handles, Arrow allocation and task
reservations must return to zero after processing or job cancellation.

One warmup and three alternating stock/native measured pairs are retained in each
cohort. Tables use one rewrite worker, 8 GiB JVM heaps, and a 1 GiB task off-heap budget.
This is a short finite-cohort measurement inside streaming maintenance, not evidence
of long-duration steady-state throughput or behavior under concurrent writers.

## Continuous maintenance boundary

The actual unbounded TableMaintenance graph keeps Iceberg's discovery, trigger,
planner, locks and commit stages. Trigger/lock polling and checkpoints use 100 ms.
The stock equality worker is unchanged. Released Iceberg's maintenance worker does
not sort: sorted controls replace only that worker with stock Iceberg IO plus the
production Flink sorter. This is explicitly a benchmark control, not a claim that the
released worker implements sorted rewriting.

Both warmed stock/native jobs remain running; one receives a new cohort at a time.
The other polls its unchanged table. The timer starts before the eligible input
commit and ends after observing the replacement commit (5 ms client polling).
For equality cases the data append is committed first, outside timing; a later
equality-delete commit makes the cohort eligible. This preserves delete sequence
semantics. Input-file creation, cleanup of the previous output and validation are
untimed. Startup plus first rewrite is separately retained as trial -1; its spill
fields are placeholders, and spill assertions use measured cohorts. Every native
measured cohort requires exactly one native group, zero stock fallback and zero
rewrite errors. Job cancellation happens after the measurements.

## File-worker boundary

The timer retains production Parquet reading, deletes, Arrow filtering/sorting,
JNI, borrowed Java views, host rolling, Parquet writing, close and footer metrics.
It excludes job startup, discovery/planning and metadata commit. Payloads remain
columnar; filtering, sorting, casting and encoding may allocate column buffers.
The stock path consumes its usual RowData; no perimeter cost intrinsic to file
processing is removed. These numbers must not be presented as full-job speedups.

## Reproduce

Use JDK 17 and a normal release+mimalloc build:

```sh
cargo build --locked --release --manifest-path native/Cargo.toml -p streamfusion-iceberg --features mimalloc
SF_ICEBERG_MAINTENANCE_BENCHMARK=true \
SF_ICEBERG_MAINTENANCE_ROWS=100000 \
SF_ICEBERG_MAINTENANCE_RUNS=3 \
mvn -Piceberg,bench -pl :streamfusion-iceberg -am \
  -Dnative.build.skip=true -Dsf.runtime.tests.skip=true \
  -Dtest=IcebergMaintenanceBenchmark,IcebergStreamingMaintenanceBenchmark \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

`SF_ICEBERG_MAINTENANCE_ROWS` is rows **per file**; use 500000 for 2M total.
`SF_ICEBERG_MAINTENANCE_CASES` selects file scenarios;
`SF_ICEBERG_MAINTENANCE_STREAM_CASES` selects continuous scenarios.
Raw JSONL retains warmups and every measured trial. Environment/build hashes and
previous-version provenance are recorded alongside the observations.


## Previous version and evidence

The previous production Java classes and release+mimalloc DSO come from
`984f0451`; their Iceberg production sources are identical to the upstream base
`2441db79` (verified with Git diff). The file harness exposes a `main` entry point
for matching-version comparisons. With the previous production jar first on the
test classpath and its matching DSO first on `java.library.path`, set
`SF_ICEBERG_MAINTENANCE_PREVIOUS=true`. Use the same release JVM arguments, resources,
fixtures and scenarios. The previous-compatible seven-argument rewrite entry point
is used; native spill counters are unavailable there and recorded as -1. Current
stock/native cohorts independently require observed spill activity. Nested scenarios
are rejected in previous mode instead of implying prior native support.

The initial full cohorts are retained, including warmups. A separate 400k previous
repeat supplies the unprofiled baseline table; the first previous 400k cohort received
a short perf probe during equality work and remains diagnostic. Fresh-process spill
repeats retain their GC/safepoint logs and are reported separately. A repository search overlapped the full 2M file cohort, so it has competing-host-IO
confounding. The streaming cohorts preceded it. There is no established causal
explanation for the full-cohort spill outliers; do not silently replace
them with isolated results or claim a standalone spill speedup from the full 2M cohort.
`environment.json` records library hashes, hardware, versions and source hashes.

Correctness coverage includes v1/v2/v3 nested round-trips, struct/list/map evolution,
pruned projections, nullable parents and elements, NaN footer metrics, nested TIME
and timestamps, normal/hash/range partition routing, upserts, nested single/composite
equality keys, projected-away delete keys, v2 position deletes/v3 vectors, sorting,
V3 row lineage, continuous SQL initial/later snapshots and 2→3 recovery through
normal/range/dynamic sinks. SQL native plans require no row/Arrow transpose nodes.
