# Kafka connector metadata: Kafka source routes

A Kafka table that declared any connector metadata column used to run its whole source on Flink, so
a native pipeline over it paid Flink's JSON decode plus a RowData→Arrow transpose. These runs
compare three routes for such a table on Flink 2.2.1, from planning through the last collected row:

- **stock**: Flink's Kafka source and JSON deserializer, Flink operators;
- **previous**: Flink's Kafka source and deserializer, a RowData→Arrow transpose, then
  StreamFusion's native operators (what a metadata-reading table ran before this change);
- **native**: Flink's Kafka source handing each split's records to the native decode, which fills
  the value and metadata columns, then the same native operators.

Each run's output digest must match across the three routes, and each saved plan must show its
route (only the native plan decodes natively, with the metadata columns on the decode; the previous
plan still runs native operators).

## Results

Median [min–max] seconds over six measured trials after two warmups; ratios divide medians.

| Workload | Records | Stock | Previous | Native | Stock/native | Previous/native |
|---|---:|---:|---:|---:|---:|---:|
| JSON + metadata | 1,000,000 | 1.619 [1.522–1.890] | 1.332 [1.224–1.390] | 1.016 [0.988–1.149] | 1.59× | 1.31× |
| JSON + metadata | 10,000,000 | 6.553 [6.307–7.056] | 5.703 [5.331–5.800] | 3.390 [3.275–3.610] | 1.93× | 1.68× |

The one-million-record runs are dominated by job startup; the ten-million-record runs measure
sustained decoding. [Every sample](observations.jsonl), [the audited table](results.md), the
[method](method.json), the [environment](environment.json) and each route's plan are kept here.

## Workload

Each record is a four-field JSON value (`id`, `tenant` over 64 tenants, `name`, `amount`) with three
headers (`trace`, `source`, `tenant`) and its own record timestamp, over four partitions. The table
reads `partition`, `offset`, `timestamp` and `headers`, and the query is `SELECT tenant, COUNT(*) AS
records, SUM(amount) AS amount, MAX(part) AS part, MAX(off) AS last_offset, MAX(ts) AS latest,
COUNT(hdrs['trace']) AS traced FROM cdc GROUP BY tenant` with mini-batch aggregation
(`allow-latency` 1 s, `size` 50,000) on every route, parallelism four, a release native build and a
single-node Kafka 4.2.2 broker running on the host. The record timestamps are fixed values from
2020, so the driver creates its topics without time retention.

## Earlier runs

The first attempt read `CARDINALITY(hdrs)`, which the native planner keeps on Flink, so the native
route never ran natively; the query now reads a header by key. A later run failed one ten-million
comparison: the broker's seven-day retention deleted the topic's segments, whose record timestamps
are from 2020, while a route was still reading. Six repeated trials then matched on every route,
and the driver has since created its topics without time retention. Neither run is kept: neither
completed.

## Reproduce

With a Kafka broker at `localhost:19092`:

```bash
SF_BENCHMARK=true mvn -Pbench -pl streamfusion-runtime test -Dtest=KafkaSourceBenchmark \
  -Dkafka.bench.brokers=localhost:19092 -Dkafka.bench.workloads=json-metadata \
  -Dkafka.bench.records=1000000,10000000 -Dkafka.bench.output=<archive>
python3 bin/report-kafka-source-bench.py <archive>
```

Without `kafka.bench.brokers` the driver starts a Testcontainers broker instead; on macOS, Docker
Desktop's port forwarding then bounds every route's throughput.

## Environment

Apple M1 Max (10 cores, 32 GB), macOS, OpenJDK 17, Rust 1.94. The machine was shared: the VS Code
renderer and Java language server each used about one core, and the load average was about 10.8
when the run started, just after a build. Routes rotate their order every run, so this load widens the ranges rather
than favoring one route. `environment.json` records the git objects of the measured build; this
commit carries the same production code.
