# Kafka to Iceberg: fresh RocksDB Nexmark comparison

The fresh streaming comparison completes all **23 queries, 30 query/variant cells
and 120 paired validations**. With RocksDB and mini-batching off, geometric means
of stock/native median elapsed-time ratios are **1.44× for exact SQL** and **1.47×
for the README query selection**. Each query contributes once; the latter selects
its existing labelled expression variant where present. Exact SQL has 16 median
wins and seven losses, including near-parity cases. All results remain in the suite.

| Selection | Queries | Repeated-median geomean | Secondary best-of-first-two geomean |
|---|---:|---:|---:|
| Exact SQL | 23 | **1.44×** | See per-cell report |
| README variants | 23 | **1.47×** | 1.24× |

[Every cell, median, range and sampling convention](results.md),
[raw observations](observations.jsonl), [timing/memory metrics](metrics.csv),
[configuration](method.json), and [audit](audit.json) retain the complete evidence.
Run `python3 docs/benchmarks/kafka-iceberg-final-20261017/verify.py` from a checkout
to recompute the audit. The comparison starts with a fresh JVM and broker, with
no resumed cells or measurements carried over from interrupted/preflight runs.
The full Maven execution takes **33 minutes 11 seconds**, including warmups and
untimed corpus production/readback. No profiler runs during these primary timings.

## Matched settings and validation

Both engines consume the unchanged two-million-event Nexmark Kafka JSON corpus
from four topic partitions, at job parallelism four. Every output is a fresh
four-bucket Iceberg v2 table, using hash distribution and identifier-field upserts
for the seven updating queries. Both engines retain the normal Iceberg sink and
Java delete-file encoding. The data writer has parallelism four except q5: its
global aggregate leads Flink to select one writer on both engines. Its table still
has the same four-bucket partition spec. Small result sets may occupy fewer buckets.

Both engines receive a 4 GiB JVM heap and 512 MiB managed memory across four slots
(128 MiB per slot), one-second checkpoints, snapshot compression off, object reuse,
an IANA `UTC` session zone, and `max.poll.records=8192`. Output Parquet settings are
Zstd level 3, 128 MiB row groups, 1 MiB pages, 2 MiB dictionaries and a 512 MiB target
file size. Local Arrow object handoffs are disabled; native shuffles use Arrow IPC.
Each cell has one warmup and three measured pairs in alternating engine order.

Execution remains streaming with finite earliest-to-latest Kafka offsets. Timings
include planning/submission, source consumption, SQL/state/shuffle work, checkpoints
and the final Iceberg commit. Committed-table readback is untimed. Deterministic
outputs match by row count and full-value multiset fingerprints. Processing-time
q12 checks emitted per-bidder counts against input bounds; unfinished windows remain
open, and clock-dependent output is not required to match. q21's fixture emits no
rows on either engine. Neither empty-output query supplies a data-file throughput
measurement.

The audit verifies 240 trials (180 measured), 120 passing pairs (90 measured),
120 native graphs without row/Arrow transposes or operator fallback, and all
240 output table partition/configuration metadata files. q4 observes both real
RocksDB backends. Runtime checks require native Kafka sources and native Iceberg
writer factories, parquet-rs data-file footers, Java delete-file footers and zero
stock source fallback. All 108 nonempty native measurements have data files;
empty outputs retain the graph/writer assertions. Payloads stay Arrow from native
Kafka decode through query/shuffle execution to Rust Parquet data-file encoding.

The host is an Intel i7-12650H under WSL2/ext4, JDK 17, Flink 2.2.1 and Iceberg
Java 1.12.0. Release/mimalloc libraries and their hashes are recorded in
[the environment](cohorts/cohort-001/environment.json); the measured code commit
is `16e25a53685f7c3f836e05968251262351ce1d11`. Production engine/connector code is
unchanged by the benchmark setup correction to use the headline harness's UTC ID.
Per-cell archives retain plans, graphs and final table metadata; payload/state
files remain local. Sanitized compressed Maven/Cargo logs accompany the cohort.

## Regressions and comparison limits

| Query | Stock median [range], seconds | Native median [range], seconds | Stock/native |
|---|---:|---:|---:|
| q0 | 1.914 [1.898–2.092] | 4.709 [1.696–6.955] | 0.41× |
| q5 | 3.482 [3.417–7.135] | 4.997 [4.766–9.721] | 0.70× |
| q19 | 14.719 [12.357–23.550] | 26.405 [24.030–31.231] | 0.56× |

The complete report also retains the smaller losses. Large timing ranges occur on
both engines in several state-heavy queries, including q4/q9/q18/q20/q23. These
end-to-end totals do not isolate the Parquet encoder. q19's committed summaries
show similar physical data/delete volumes on both engines; elapsed time alone
does not identify the source, writer or state cost.

The separate [q0/q19 CPU diagnostics](profiles/README.md) pass all four profiled
pairs with identical release libraries. Neither primary regression reproduces;
q19's native Rust data-write/finish entry is 8.0% of execution CPU samples, while
retained Java position-delete finalization is 21.0%. Inclusive CPU samples overlap
and do not explain the primary elapsed-time reversals. These diagnostic timings
remain excluded from the suite metrics.

The later [matched Kafka→Paimon run](../kafka-paimon-final-20261017/README.md)
measures **1.43× exact SQL**, almost identical to this run's 1.44× overall. Its
append / primary-key geomeans are 1.24× / 1.97×, versus Iceberg's 1.45× / 1.40×.
All per-query comparisons and Paimon regressions remain in that report. This does
not show a large whole-suite Paimon advantage; update-file semantics and the wide
stateful timing ranges still matter to individual queries.

The historical Paimon disk suite uses Flink 1.18 and best-of-two samples. The
Iceberg-to-Iceberg suite uses a different source/completion protocol and the
MiniCluster's default managed-memory budget. Their geometric means provide
historical context rather than a controlled connector-only comparison with this
explicitly sized Kafka-to-Iceberg run. The headline Kafka benchmark is unchanged.

```sh
SF_ICEBERG_SOURCE=kafka SF_ICEBERG_MODES=disk-off \
  SF_BENCH_MANAGED_MEMORY='512 mb' SF_ROWS=2000000 SF_PARALLELISM=4 \
  SF_WARMUP=1 SF_RUNS=3 bin/bench-iceberg.sh
```

Use JDK 17. Supported acceleration remains enabled by default when its modules are
installed; the stock leg executes Flink's normal rowwise source/operators/writer.
