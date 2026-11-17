# Nested Maxwell and Canal: Kafka source routes

Maxwell and Canal tables with nested columns used to stay on Flink's decoder, so a native pipeline
over them paid Flink's JSON decode plus a RowData→Arrow transpose. These runs compare three routes
for such a table on Flink 2.2.1, from planning through the last collected row:

- **stock**: Flink's Kafka source and Maxwell/Canal deserializer, Flink operators;
- **previous**: Flink's Kafka source and deserializer, a RowData→Arrow transpose, then
  StreamFusion's native operators (what a nested Maxwell/Canal table ran before this change);
- **native**: Flink's Kafka source handing bytes to the native decode, then the same native
  operators.

Each run's output digest must match across the three routes, and each saved plan must show its
route (only the native plan decodes natively; the previous plan still runs native operators).

## Results

Median [min–max] seconds over six measured trials after two warmups; ratios divide medians.

| Workload | Records | Stock | Previous | Native | Stock/native | Previous/native |
|---|---:|---:|---:|---:|---:|---:|
| nested Maxwell | 1,000,000 | 2.345 [2.277–2.638] | 1.548 [1.489–1.645] | 1.295 [1.274–1.309] | 1.81× | 1.20× |
| nested Maxwell | 10,000,000 | 16.091 [15.817–16.742] | 8.416 [8.191–9.002] | 6.454 [6.404–6.637] | 2.49× | 1.30× |
| nested Canal | 1,000,000 | 3.868 [3.806–4.235] | 2.222 [2.201–2.570] | 1.799 [1.780–1.810] | 2.15× | 1.24× |
| nested Canal | 10,000,000 | 32.052 [31.794–32.442] | 15.952 [15.799–17.097] | 11.763 [11.458–12.099] | 2.72× | 1.36× |

At one million messages a fixed per-job cost of about 0.7–0.8 s is a fifth to a half of each
median; at ten million it is 2–11%, so those runs measure sustained decoding. [Every sample](observations.jsonl), [the audited table](results.md), the
[method](method.json), the [environment](environment.json) and each route's plan are kept here.

## Workloads

The table has scalar columns plus a nested ROW (with an ARRAY inside), a MAP<STRING, BIGINT> and an
ARRAY<ROW>. Every key is inserted, updated twice through `old` images that carry nested pre-images
(so the presence scan meets column names inside them), then updated again or deleted. A Canal
message carries two keys of the same partition, so it fans out to two rows per image. A key's
changes stay in one partition, in order, over four partitions.

The query is `SELECT tenant, COUNT(*) AS changes, SUM(amount) AS amount, SUM(info.score) AS score,
COUNT(name) AS named FROM cdc GROUP BY tenant` with mini-batch aggregation (`allow-latency` 1 s,
`size` 50,000) on every route, parallelism four, a release native build and a single-node Kafka
4.2.2 broker running on the host. Without mini-batching, the aggregate emits an update per input
row, and fetching those results bounds every route alike instead of the source being measured.

## Reproduce

With a Kafka broker at `localhost:19092`:

```bash
SF_BENCHMARK=true mvn -Pbench -pl streamfusion-runtime test -Dtest=KafkaSourceBenchmark \
  -Dkafka.bench.brokers=localhost:19092 -Dkafka.bench.workloads=nested-maxwell,nested-canal \
  -Dkafka.bench.records=1000000,10000000 -Dkafka.bench.output=<archive>
python3 bin/report-kafka-source-bench.py <archive>
```

Without `kafka.bench.brokers` the driver starts a Testcontainers broker instead; on macOS, Docker
Desktop's port forwarding then bounds every route's throughput.

## Environment

Apple M1 Max (10 cores, 32 GB), macOS, OpenJDK 17, Rust 1.94. The machine was shared: the VS Code
renderer and Java language server each used about one core, and the load average was about 12
when the run started, just after a test build. Routes rotate their order every run, so load widens
the ranges rather than favoring one route. `environment.json` records the git objects of the
measured build; this commit carries the same production code.
