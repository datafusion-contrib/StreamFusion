# Optional Linux RocksDB io_uring batch reads

RocksDB's SST MultiRead path can submit independent block reads through
io_uring instead of issuing them serially. Native keyed-state probes already
use the released binding's pinned batched MultiGet API. This change makes
RocksDB's Linux read integration available through an optional build feature
and a process-level runtime switch, without changing the pinned values,
operators, write batches, checkpoint protocol, or dependency source.

The released `librocksdb-sys` defines a strong, always-true enable hook.
Defining that hook again would create a duplicate symbol; the Linux linker
wraps its calls instead. The wrapper caches the exact `true` value of
`SF_ROCKSDB_IO_URING`. This keeps the same binary usable for OFF and ON tests,
and leaves the path disabled until explicitly selected. Unsupported or
sandbox-denied ring creation follows RocksDB's ordinary-read fallback.
[Build and deployment settings](../backends/rocksdb.md#linux-io_uring-reads)
describe the optional liburing dependency.

## Measurements and limits

On October 1, 2026 (EDT), an isolated experimental snapshot ran the Nexmark
query suite with a generated row source and blackhole sink: 2,000,000 events,
parallelism 4, native RocksDB, mini batching off, one-second checkpoints,
release + mimalloc, Java 17, ordinary IPC shuffle, and Linux
6.18.40.1-microsoft-standard-WSL2 with 16 logical CPUs and about 10 GiB RAM.
Both row/Arrow transpose boundaries remained in each query plan. The generated
source, event schemas, watermarks, SQL and blackhole sink were unchanged;
only runner selection, backend options, generator parallelism and recording
were adjusted. There were no Kafka jobs or stock Flink references, as requested
for this isolation experiment. This is not the Kafka README headline workload.

OFF ran before ON in fresh JVMs, with zero warmups and three measured trials
per query per mode. All 23 queries (q6 excluded) passed: 138 measured trials
plus one q4 backend-engagement preflight per mode. The same frozen binary and
source were used in both modes. The complete pair took 876.8 seconds.

| Statistic | io_uring ON throughput change |
|---|---:|
| Geometric mean, best of three | +3.93% |
| Median query, best of three | +0.80% |
| Geometric mean, median of three | +1.08% |
| Median query, median of three | +0.59% |
| Queries faster / slower by best time | 16 / 7 |

q3 improved by 57.8% using best times; q17 regressed by 9.7%, q19 by 4.5%,
and q20 by 7.2%. Only q3 had all ON trials faster than all OFF trials;
q17 and q19 had all ON trials slower than all OFF trials. These results are
not a claim of a broad, repeatable speedup, and the path is not enabled by
default. Maintainer approval to submit the integration followed review of
these measurements, including their modest aggregate gain and regressions.

Five-second host sampling observed at least 4.63 GiB available RAM, zero
swap-out and no memory-pressure stalls. System-wide I/O full-stall time was
14.43% of the sampled interval. The blackhole sink removes output connector
I/O, but RocksDB writes, checkpoints and database cleanup still affect timing;
these samples do not attribute stalls to individual processes. Several short
queries also include a substantial startup cost.

The measured baseline was a frozen worktree snapshot at commit
`0d2ef488949ae80cee141d679e17f10fea08e8dd`, including pre-existing worktree
changes and a shared Kafka prerequisite fix. It was not unmodified repository
HEAD. That shared fix is irrelevant to the blackhole sink and is not included
in this integration PR. The final PR is based on canonical main; the numbers
above describe the experimental snapshot, not a fresh benchmark of that PR.
The benchmark library SHA-256 was
`63403851fddcb69b43bbaf25c7ba2ae60cd394020669b98d961781bf8c74c68c`.

[Per-query best times](../benchmarks/io-uring-2026-10-01-row-blackhole-best3-2m/comparison.csv),
[all three trial times and ranges](../benchmarks/io-uring-2026-10-01-row-blackhole-best3-2m/all-trials.csv),
[configuration](../benchmarks/io-uring-2026-10-01-row-blackhole-best3-2m/configuration.json),
[summary](../benchmarks/io-uring-2026-10-01-row-blackhole-best3-2m/summary.json),
and [completion audit](../benchmarks/io-uring-2026-10-01-row-blackhole-best3-2m/completion-audit.json)
retain the full evidence. The
[isolated runner patch](../benchmarks/io-uring-2026-10-01-row-blackhole-best3-2m/row-blackhole-harness.patch)
records how this variant was selected; it is not applied to the production
Nexmark harness in this PR. It is against the frozen experimental source,
whose prerequisites differ from canonical main.

## Reproduction

Use a release library built with the feature above. In the isolated snapshot
with the runner patch, run:

```sh
SF_BENCHMARK=true SF_ROW_ROCKS_BENCH=true SF_ROWS=2000000 \
SF_PARALLELISM=4 SF_WARMUP=0 SF_RUNS=3 SF_ROCKSDB_IO_URING=false \
mvn -pl :streamfusion-runtime -Pbench \
  -Dnative.build.skip=true -Dnative.test.resource.skip=true \
  -Dtest=NexmarkMatrixBenchmark#rowDataRocksComparison test
```

Repeat with `SF_ROCKSDB_IO_URING=true` in a fresh JVM. Use Java 17 and UTC for
both. The native backend, mini-batch and checkpoint settings are pinned by the
runner method. Verification additionally uses a cold-SST pinned-read test with
block caching disabled and the pinned read executed before the legacy read,
so a warmed cache cannot hide whether the ring is used. Test-only cache
settings do not affect production options or benchmark jobs.
