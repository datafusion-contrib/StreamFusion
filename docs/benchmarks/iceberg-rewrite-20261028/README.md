# Iceberg rewrite file-processing comparison — 2026-10-28

This comparison times one group selected by Iceberg's existing bin-pack planner.
Stock uses Iceberg's released Flink RowData task reader/writer; native uses the new
Arrow file-processing worker. Previous StreamFusion maintenance used the stock path.
Both engines read the same four prebuilt Parquet files and write one output file
with Zstd level 3. The timer includes file IO, delete application, JNI, Java borrowed
row-view traversal, rolling checks, Parquet encoding, file close and metrics. Table
creation, planning, job startup, commits, readback and cleanup are outside timing.
These are file-task results, not complete Flink maintenance-job speedups.

Each size uses one warmup and three alternating measured pairs, with the same
8 GiB maximum Java heap and 256 MiB task off-heap budget. The input contains unique
integer IDs and nullable Unicode strings (4,096 distinct non-null payloads, null
one row in seven). Delete cases remove every eighth physical position and add a
single integer-key equality-delete file selecting IDs 17, 118, 219, and so on.
V2 uses Parquet position deletes; V3 uses four vectors sharing a Puffin file.
Sorting is off. No transformed or composite equality-key performance is claimed.

The worker keeps Iceberg's reader for decode, positional deletes, deletion vectors
and row lineage. Single integer/long/string equality keys use DataFusion's existing
static membership kernel; composite and other keys retain Iceberg's predicate path.
An initial direct-RC3 run regressed sharply with large equality-delete files. CPU
sampling located its hot addresses in Arrow's comparison kernel, matching RC3's
per-delete predicate tree. The static membership filter removes that repeated
comparison while preserving columnar payloads and null-safe delete semantics.

Validation reads every output through stock Iceberg, checks row count and two mixed
row-value fingerprints, checks each data-file footer for the expected Java/Rust
encoder, and requires zero remaining native handles, Arrow allocation and task
reservations. Native code is an ordinary release build with mimalloc; no profile
changes or development encoder substitutes are used. Build and hardware details
are recorded in [environment.json](environment.json).

Reproduce from the repository root with JDK 17 selected:

```sh
cargo build --release --manifest-path native/Cargo.toml -p streamfusion-iceberg --features mimalloc
SF_ICEBERG_REWRITE_BENCHMARK=true SF_ICEBERG_REWRITE_ROWS=500000 \
  mvn -Piceberg,bench -pl :streamfusion-iceberg -am \
  -Dnative.build.skip=true -Dtest=IcebergRewriteBenchmark \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

`SF_ICEBERG_REWRITE_ROWS` is the row count **per input file**. Use 100000 for 400k
input rows or 500000 for 2M. All samples, including warmups, are retained below.
Results use warm-cache local disk on one host; short 400k trials retain visible JVM
warmup/noise and do not establish distributed or cold-storage throughput.

## Results

Elapsed seconds, median [minimum–maximum] of the three measured samples:

| Input / case | Stock (previous behavior), s | Native, s | Stock/native |
|---|---:|---:|---:|
| 400k / v2-append | 0.156 [0.148–0.165] | 0.079 [0.077–0.088] | 1.98× |
| 400k / v2-deletes | 0.252 [0.248–0.259] | 0.077 [0.075–0.087] | 3.27× |
| 400k / v3-deletes | 0.273 [0.273–0.276] | 0.106 [0.101–0.111] | 2.58× |
| 2m / v2-append | 0.495 [0.487–0.555] | 0.196 [0.193–0.200] | 2.53× |
| 2m / v2-deletes | 0.906 [0.854–0.944] | 0.250 [0.247–0.259] | 3.63× |
| 2m / v3-deletes | 1.042 [0.991–1.121] | 0.326 [0.315–0.331] | 3.20× |

All 48 trials, including warmups, pass readback, footer and resource checks. Samples:
[400k](400k.jsonl), [2M](2m.jsonl). Outputs contain 400,000 / 2,000,000 rows without
deletes and 346,534 / 1,732,673 rows with deletes. Every trial writes one output file.

Correctness validation separately passes the 356-case optional Iceberg/upstream
module selection (13 existing batch exclusions), plus focused wholly-deleted-file,
V3 mid-file savepoint/rescaling and sort-failure cleanup checks. Seven native unit tests pass, including
forced external-sort spill and byte/statistics parity with the pinned Iceberg writer.
Sorting is verified for order, metadata and V3 lineage but is outside this timing.

The recorded cohorts use the main implementation subsequently committed in
`5f595536adef0d4015f23b3aa9b1b57bdb029609`. A follow-up preserves microsecond TIME
payloads through borrowed Flink views and excludes TIME partition routing. The
integer/string benchmark corpus and native DSO are unchanged; TIME correctness and
ordering are tested separately, with no TIME throughput claim.
