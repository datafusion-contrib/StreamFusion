# Kafka to Paimon: fresh RocksDB Nexmark comparison

The fresh streaming run completes all **23 queries, 30 exact/variant cells and
120 paired validations**. The geometric mean of stock/native median elapsed-time
ratios is **1.43× for exact SQL** and **1.43× for the README variant selection**.
Exact SQL has 20 median wins and three losses. Every measured sample is retained.

The whole-suite point estimate is almost identical to the fresh Kafka→Iceberg
run. Paimon has a stronger updating-query result, offset by a weaker append result:

| Exact SQL selection | Queries | Paimon / stock Flink | Iceberg / stock Flink |
|---|---:|---:|---:|
| All | 23 | **1.43×** | **1.44×** |
| Append | 16 | **1.24×** | **1.45×** |
| Primary-key / identifier-field updates | 7 | **1.97×** | **1.40×** |

Each entry is the geometric mean of per-query stock/native median ratios;
values above one favor StreamFusion. The README selection uses each existing
labelled expression variant where present, giving 1.43× Paimon versus 1.47×
Iceberg. These measurements do not show a large whole-suite Paimon advantage.

[All medians and ranges](results.md), [per-query Iceberg comparisons](iceberg-comparison.md),
[raw observations](observations.jsonl), [every timing](metrics.csv),
[configuration](method.json), [environment and library hashes](environment.json),
[timing audit](audit.json), and [plan/table/file audit](evidence-audit.json)
retain the complete result. Run
`python3 docs/benchmarks/kafka-paimon-final-20261017/verify.py` from a checkout to
regenerate the reports, validate the archived witnesses, and recompute the
Iceberg comparison and write-volume witnesses from both archived suites. The full Maven run
takes **32 minutes 35 seconds**, including warmups and untimed setup/readback.

## Matched settings and validation

Both engines consume the unchanged two-million-event Nexmark Kafka JSON corpus
from four topic partitions. Job parallelism is four. Each output is a fresh
Paimon table with four fixed buckets: append tables use the first output column
as their bucket key, and updating tables declare the existing query's real primary
key. Paimon's deduplicate merge engine and normal in-job compaction remain enabled.
All source and writer plans have parallelism four except q5's writer: Flink selects
one writer for its global aggregate on both engines, exactly as in the Iceberg run.
Its output table still has four buckets. Small outputs can occupy fewer buckets.

Both engines use a 4 GiB JVM heap, 512 MiB managed memory across four slots
(128 MiB per slot), RocksDB, mini-batching off, one-second checkpoints, snapshot
compression off, object reuse, an IANA UTC session zone and `max.poll.records=8192`.
Native shuffles use Arrow IPC; local object handoffs are disabled. Test logging
uses WARN with the Log4j2 binding. Each cell has one warmup and three measured
pairs, alternating engine order. No old or resumed measurements enter the result.

The sink retains Paimon 2.0.0's defaults on both engines: Zstd level 1, 256 MiB
append / 128 MiB primary-key file targets, and a 256 MiB writer buffer. The latter
uses Paimon's default independent allocator; the configured managed-memory pool
sizes state. These defaults differ from Iceberg's explicit Zstd level 3 and
512 MiB target. The host is an Intel i7-12650H under WSL2/ext4, JDK 17 and Flink
2.2.1. Release/mimalloc libraries are staged by `bin/bench-paimon.sh`. Measured code
is clean commit `413bd056cce485c8438d73fe892fbc37d674e464`; production engine and
connector code is unchanged from the Iceberg comparison. Optional library sets
and therefore their build hashes differ.

Execution stays streaming with finite earliest-to-latest Kafka offsets. Timing
covers `executeSql` planning/submission through end-of-input and the final commit.
Corpus production, explain-plan inspection and stock committed-table readback
are untimed. Every deterministic pair matches its final full-value multiset.
Clock-dependent q12 instead validates each bidder's emitted count against its
input bound, leaving unfinished processing-time windows open. One native q12
measurement crosses a window boundary and emits 46 rows; the other q12 trials
emit none. q21's fixture emits no rows on either engine. These are not comparable
data-file throughput cases.

The audit verifies 240 trials (180 measured), 120 passing pairs, 120 native plans
without row/Arrow transposes or operator fallback, 240 table schemas, eight real
q4 RocksDB witnesses, and 3,114 Parquet footers. All 109 nonempty native trials
contain Rust Parquet files. Stock trials contain no Rust-written files. Paimon's
normal Java compaction rewrites remain permitted. Native Kafka decode, SQL and
routing keep payloads in Arrow until native Parquet encoding. The
[evidence archive](evidence.tar.gz) retains every plan, DDL, check summary, footer
inventory, table schema and snapshot; large payloads remain local. Sanitized
[run](run.log.gz) and [Cargo](cargo.log.gz) logs accompany it. No measured trial is
profiled; one thread dump inspected q0's warmup, outside the reported samples.

## Regressions and the Iceberg comparison

| Query | Stock median [range], s | Native median [range], s | Stock/native |
|---|---:|---:|---:|
| q5 | 3.583 [3.334–3.972] | 4.618 [4.555–4.674] | 0.78× |
| q20 | 4.359 [4.137–14.932] | 9.493 [4.236–20.648] | 0.46× |
| q23 | 10.881 [10.321–22.122] | 17.014 [11.059–19.158] | 0.64× |

q5 also loses in Iceberg. The large append q23 has a 3.869-second native warmup
but all three measured trials are slower; the warmup does not replace them.
Large ranges also occur in q4/q7/q9/q18/q19. Their point estimates can move
substantially with only three samples. For example, native q18 is slower in this
Paimon run than in Iceberg, while q19 is faster. Several simpler queries finish
in one to two seconds and include material startup/lifecycle costs.

Updating tables perform different physical work. The retained
[write-volume witnesses](write-volumes.json) show q15's Paimon native tables with
a median **seven physical rows in present data files**, versus Iceberg's
**1.84M added data records plus roughly 1.84M position deletes**. Both end with one
logical row and pass their own stock/native parity check. For q19, the analogous
Paimon present-file median is **1.24M rows**, while Iceberg adds **7.76M data
records, 6.38M position deletes and 1.35M equality deletes**. Paimon's buffered
merge and Iceberg's retained Java delete path give different costs even when the
logical output agrees. Paimon counts include obsolete/compaction files still
present; Iceberg counts sum snapshot additions. They show physical representation,
not a matched measurement of total bytes ever written.

The earlier [Iceberg q19 CPU diagnostic](../kafka-iceberg-final-20261017/profiles/README.md)
observes retained Java position-delete finalization at 21% of execution CPU samples
and Rust data-write/finish at 8%. That separate run does not reproduce the primary
Iceberg elapsed-time loss. Together with the mixed results and wide ranges here,
it supports investigating update bookkeeping and checkpoint/file work before
attributing the whole gap to Parquet encoding; it does not establish a causal
explanation of these elapsed times. The primary matrices remain separate and
unmodified. Paimon's historical 2.46× disk headline used Flink 1.18 and best-of-two
sampling, so it is not the result of this fresh Flink 2.2 repeated-median run.

```sh
# JDK 17; defaults already select RocksDB/off, four inputs and four output buckets.
SF_ROWS=2000000 SF_WARMUP=1 SF_RUNS=3 bin/bench-paimon.sh
```

Supported acceleration remains enabled by default.
