# StarRocks Stream Load rows, 2026-10-26

This report measures the native StarRocks row encoder described on the
[connector page](../../connectors/starrocks.md) and in the
[optimization ledger](../../optimizations/batch-native-starrocks-rows.md). It has two parts:

- A small-functionality gate comparing the connector's own serializer in Java with the native
  encoder timed inside Rust.
- Complete jobs from SQL submission until the rows are committed and queryable in a real StarRocks
  server.

[`results.json`](results.json) holds the summaries, configuration and control definitions. It
records the SHA-256 of every measured production source, benchmark harness and native library.
The sources were measured on base `dc198ece` before they were committed. Every complete-job run,
including warmups, every measured encoding trial and every superseded cohort is retained.
[`profiles.json`](profiles.json) summarizes the JFR profiles cited below.

## Environment

| Item | Value |
| --- | --- |
| Host | Apple M1 Max, 10 cores, 32 GiB, macOS 26.5.2 (arm64) |
| Docker VM | 10 CPUs, 15.6 GiB; StarRocks container limited to 8 GiB |
| StarRocks | `starrocks/allin1-ubuntu:4.1.6`, pinned by digest, native arm64 |
| Connector | `com.starrocks:flink-connector-starrocks:1.2.16_flink-1.18` |
| Flink | 1.18.1 MiniCluster, OpenJDK 17.0.20, 4 GiB heap, UTC |
| Native code | Release build with mimalloc (`bench` Maven profile), rustc 1.94.0 |

## Encoding

The stock side runs the released connector's row serializer over prebuilt binary rows and takes
the UTF-8 bytes its stream-load SDK buffers, all in Java. The native side is the
`starrocks_encode` Criterion suite. It times the same rows, as prebuilt Arrow batches, inside Rust,
in production-sized slabs that reuse the task's buffers, with the connector's JSON key order. Both
produce the same bytes.

The rows hold a non-null BIGINT, nullable INT and STRING, DECIMAL(18,2), DOUBLE, DATE,
TIMESTAMP(3) and BOOLEAN, plus a nullable 70-character comment with a non-ASCII letter.
Changelog cases use primary-key upsert options, with row kinds cycling `+I/-U/+U/-D`; the default
`sink.ignore.update-before` drops the `-U` rows. The Java figures are medians of seven trials after
three warmups, with engines rotated.

| Format | Row kinds | Rows/batch | Stock Java ns/row | Native Rust ns/row | Speedup |
| --- | --- | ---: | ---: | ---: | ---: |
| CSV | insert | 1,024 | 889 [888–917] | 285 | 3.12× |
| CSV | insert | 8,192 | 906 [900–943] | 291 | 3.12× |
| CSV | changelog | 1,024 | 704 [703–705] | 216 | 3.26× |
| CSV | changelog | 8,192 | 719 [718–770] | 226 | 3.19× |
| JSON | insert | 1,024 | 1,187 [1,184–1,215] | 357 | 3.33× |
| JSON | insert | 8,192 | 1,195 [1,190–1,222] | 372 | 3.21× |
| JSON | changelog | 1,024 | 934 [932–958] | 280 | 3.33× |
| JSON | changelog | 8,192 | 940 [939–1,058] | 289 | 3.25× |

Native figures are Criterion's point estimates; [`criterion/`](criterion/starrocks_encode.txt) has
their confidence intervals, outlier counts, metadata and allocation profile. With its buffers
reused, a slab takes three allocations totalling 896 bytes, whatever its row count.

The Java benchmark also times two conversion-inclusive diagnostics, which are outside the gate:

| Format | Row kinds | Rows/batch | Production Java entry ns/row | From RowData ns/row |
| --- | --- | ---: | ---: | ---: |
| CSV | insert | 8,192 | 602 (1.50× stock) | 721 (1.26×) |
| CSV | changelog | 8,192 | 459 (1.56× stock) | 574 (1.25×) |
| JSON | insert | 8,192 | 842 (1.42× stock) | 961 (1.24×) |
| JSON | changelog | 8,192 | 659 (1.43× stock) | 774 (1.21×) |

The production entry exports a shared Arrow batch through the C Data Interface, encodes it over
JNI, creates one `String` per row and takes the SDK's UTF-8 bytes. From RowData additionally runs
the row-to-Arrow transpose first. The 1,024-row cells are in [`encode-java.csv`](encode-java.csv).

## Complete jobs

Each job reads a rowwise `datagen` source (sequence `id`, random `DATE`, `TIMESTAMP(3)` and
48-character `STRING`), projects it, and writes it to a fresh four-bucket DUPLICATE KEY table with
replication one:

```sql
INSERT INTO dst SELECT id, CAST(MOD(id, 1000) AS INT), CONCAT('customer-', CAST(id AS STRING)),
  CAST(MOD(id, 100000) AS DECIMAL(18, 2)), CAST(id AS DOUBLE) / 3, event_day, event_time,
  MOD(id, 2) = 0, note FROM src
```

The sink uses the connector defaults (transactional sink function, at-least-once) at parallelism
two. The timer runs from SQL submission until `await()` returns, so it includes planning, the
connector's JDBC table check, startup, transposes, JNI, encoding, HTTP Stream Load and the final
commit. Table creation and validation are outside the timer.

The three modes are:

- **stock**: Flink's planner and the released sink.
- **previous**: StreamFusion with `-Dstreamfusion.starrocks.enabled=false`, so native Calc runs
  between the row transposes and the released writer serializes rows.
- **native**: native Calc after the entry transpose, with the native encoder in the released
  writer and no exit transpose.

Every run asserts the expected [plans](plans/native-csv.txt) and validates the committed table's
`COUNT(*)`, `SUM(id)`, `SUM(quantity)` and `SUM(active)`. Each cell has two warmups and six measured
trials, with the mode order rotated every trial, on a freshly started container. Medians and full
ranges, in seconds:

| Rows | Format | Stock | Previous | Native | Stock / native | Previous / native |
| ---: | --- | ---: | ---: | ---: | ---: | ---: |
| 1,000,000 | CSV | 3.616 [3.495–3.908] | 3.773 [3.564–4.287] | 3.207 [2.859–3.663] | 1.13× | 1.18× |
| 1,000,000 | JSON | 4.474 [3.997–4.692] | 4.450 [4.053–5.081] | 3.577 [3.452–6.023] | 1.25× | 1.24× |
| 5,000,000 | CSV | 12.786 [11.279–14.098] | 11.701 [11.132–13.329] | 10.392 [9.576–12.505] | 1.23× | 1.13× |
| 5,000,000 | JSON | 15.198 [14.393–20.429] | 17.093 [14.433–26.984] | 14.427 [13.238–23.746] | 1.05× | 1.18× |

The native median is lower in every cell, but every range overlaps. Native was the slowest mode
in one round each of the 1M-row JSON, 5M-row CSV and 5M-row JSON cells. In two 5M-row JSON
rounds, two of the three modes took 20–27 s, which inflates the previous mode's median there.

The difference between the 5M and 1M medians approximates the sustained rate over the extra 4M
rows. It inherits the outlier rounds above, so the previous mode's JSON rate is understated:

| Format | Stock rows/s | Previous rows/s | Native rows/s | Native / stock | Native / previous |
| --- | ---: | ---: | ---: | ---: | ---: |
| CSV | 436,000 | 505,000 | 557,000 | 1.28× | 1.10× |
| JSON | 373,000 | 316,000 | 369,000 | 0.99× | 1.17× |

Sustained JSON writing stays within 0.99–1.07× of stock Flink in this and every earlier per-row
cohort; only the rejected batch-string design reached 1.21×. These are bounded jobs on one host,
with the StarRocks server sharing it. They do not establish unbounded-stream throughput or a
Nexmark result.

[`connector.csv`](connector.csv) holds the measured trials, and [`runs.txt`](runs.txt) every run in
execution order, including warmups. Compare within a cohort. The superseded cohorts are:

- [`before-rebase/`](before-rebase/README.md): the same design before rebasing and before the
  per-task spelling check, on a fresh container: 1.05–1.16× over stock, 1M-row ranges separate.
- [`long-lived-fixture/`](long-lived-fixture/README.md): that build on the container used all day,
  1.07–1.28×.
- [`per-row-recomputed-key/`](per-row-recomputed-key/README.md): before rows carried the SDK's
  table key, 1.08–1.13×.
- [`rejected-batch-rows/`](rejected-batch-rows/README.md): the rejected one-row-per-batch design,
  1.18–1.29× with every range separate. It was faster, and was rejected for the SDK's row
  accounting and size limits.

## Where the remaining time goes

Encoding is no longer the writer's main cost. In JFR profiles of 5M-row CSV jobs on this code,
the stock sink thread's samples include 22.8% in the connector's row transformer and 13.3% in its
serializer. The native sink thread spends 25.5% in Rust encoding and its JNI copy and 12.4% in
the SDK's write path, including 2.1% in its table lookup. Each native writer subtask parked about
2.0 s per CSV job, waiting for the SDK to drain buffered rows to StarRocks, against about 0.5 s
for stock. In a 5M-row JSON job, native writers parked 4.7 s of 13.3 s, against 3.0 s of 16.5 s
for stock. With encoding cheap, the writers wait on StarRocks ingestion on the same host, which
caps the complete-job gain well below the encoding gate, most of all for JSON.

[`before-rebase/profiles.json`](before-rebase/profiles.json) attributes the per-row table key:
without it, the SDK's region lookup took 24.2% of the native sink thread's samples.

## Reproduce

Run each part separately to reproduce these settings. Start the pinned server with
`SF_STARROCKS_MEMORY=8g bin/starrocks-suite.sh fixture`, then:

```sh
bin/bench-native.py --package streamfusion-starrocks --bench starrocks_encode
SF_STARROCKS_BENCH=true mvn -B -ntp -Pbench,flink-1.18,starrocks -pl streamfusion-starrocks -am test \
  -Dsurefire.failIfNoSpecifiedTests=false -Dtest=StarRocksEncodeBenchmark \
  '-Dsf.extraJvmArgs=-Xmx4g -Duser.timezone=UTC -Dsf.starrocks.benchmark.profile=release'
SF_STARROCKS_BENCH=true mvn -B -ntp -Pbench,flink-1.18,starrocks -pl streamfusion-starrocks test \
  -Dsurefire.failIfNoSpecifiedTests=false -Dtest=StarRocksConnectorBenchmark \
  '-Dsf.extraJvmArgs=-Xmx4g -Duser.timezone=UTC -Dsf.starrocks.benchmark.profile=release -Dsf.starrocks.benchmark.warmups=2 -Dsf.starrocks.benchmark.trials=6'
```

`bin/starrocks-suite.sh benchmark` runs both Java benchmarks with their defaults instead: one
complete-job warmup and three trials, against a fixture limited to 6 GiB unless
`SF_STARROCKS_MEMORY` says otherwise. The Criterion metadata path has the checkout directory
redacted to `<repository>`.
