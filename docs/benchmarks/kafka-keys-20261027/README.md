# Keyed Kafka tables: Kafka source routes

A Kafka table with a `key.format` used to decode natively only with a single raw key over a JSON,
CSV or raw value, decoding each record's value alone. A JSON, CSV, Avro or Avro Confluent key kept
the table on Flink's decoder, so a native pipeline over it paid Flink's key and value decode plus a
RowData→Arrow transpose. These runs compare three routes for keyed tables, from planning through
the last collected row:

- **stock**: Flink's Kafka source and key/value deserializers, Flink operators;
- **previous**: Flink's Kafka source and deserializers, a RowData→Arrow transpose, then
  StreamFusion's native operators (what these keyed tables ran before this change);
- **native**: Flink's Kafka source handing each split's keys and values to the native decode, which
  decodes both in batches and pairs them per record, then the same native operators.

Each run's output digest must match across the three routes, and each saved plan must show its
route (only the native plan decodes natively, with the key format on the decode; the previous plan
still runs native operators).

## Results on Flink 2.2.1

Median [min–max] seconds over six measured trials after two warmups; ratios divide medians.

| Workload | Records | Stock | Previous | Native | Stock/native | Previous/native |
|---|---:|---:|---:|---:|---:|---:|
| JSON key and value | 1,000,000 | 1.245 [1.021–1.588] | 1.000 [0.874–1.147] | 0.887 [0.772–0.915] | 1.40× | 1.13× |
| JSON key and value | 10,000,000 | 3.407 [3.098–4.176] | 2.362 [2.256–2.907] | 1.876 [1.809–1.937] | 1.82× | 1.26× |
| raw key, JSON value | 1,000,000 | 1.045 [0.985–2.299] | 0.870 [0.862–0.880] | 0.761 [0.755–0.893] | 1.37× | 1.14× |
| raw key, JSON value | 10,000,000 | 3.396 [3.214–3.706] | 2.748 [2.368–3.903] | 1.877 [1.811–2.681] | 1.81× | 1.46× |
| Avro Confluent key and value | 1,000,000 | 1.320 [1.309–1.687] | 1.084 [1.062–1.199] | 0.763 [0.755–0.880] | 1.73× | 1.42× |
| Avro Confluent key and value | 10,000,000 | 6.076 [5.740–9.624] | 5.791 [5.262–6.471] | 1.717 [1.704–1.833] | 3.54× | 3.37× |

The one-million-record runs are dominated by job startup. The ten-million-record runs still carry
about 0.65–0.8 s of it, a third or more of each native median, so their ratios understate the
sustained gain: over the nine million records between the two sizes, native decoded 2.19×
(JSON), 2.11× (raw) and 4.99× (Avro Confluent) faster than stock Flink, and 1.38×, 1.68× and
4.93× faster than the previous route. [Every sample](observations.jsonl), [the audited table](results.md), the
[method](method.json), the [environment](environment.json) and each route's plan are kept here.

The raw-key table already decoded natively before this change, so its previous route above is
Flink's decode, not the earlier native composition. That composition is compared under
[Raw keys before batching](#raw-keys-before-batching).

## Results on Flink 1.18.1

The same workloads with the `flink-1.18` build (Kafka connector 3.2.0-1.18, client 3.4.0), kept in
[`flink-1.18/`](flink-1.18/):

| Workload | Records | Stock | Previous | Native | Stock/native | Previous/native |
|---|---:|---:|---:|---:|---:|---:|
| JSON key and value | 1,000,000 | 0.774 [0.603–0.919] | 0.474 [0.462–0.575] | 0.351 [0.347–0.354] | 2.21× | 1.35× |
| JSON key and value | 10,000,000 | 3.208 [2.927–3.577] | 2.265 [1.921–3.452] | 1.509 [1.297–1.526] | 2.13× | 1.50× |
| raw key, JSON value | 1,000,000 | 0.741 [0.575–1.128] | 0.356 [0.334–0.456] | 0.337 [0.236–1.395] | 2.20× | 1.06× |
| raw key, JSON value | 10,000,000 | 3.001 [2.773–3.204] | 1.655 [1.505–1.712] | 1.185 [1.172–1.292] | 2.53× | 1.40× |
| Avro Confluent key and value | 1,000,000 | 0.880 [0.774–1.120] | 0.650 [0.548–0.752] | 0.234 [0.231–0.339] | 3.76× | 2.78× |
| Avro Confluent key and value | 10,000,000 | 5.214 [5.073–5.789] | 4.207 [3.879–4.459] | 1.125 [0.963–1.620] | 4.63× | 3.74× |

## Raw keys before batching

Before this change a raw-keyed table already decoded natively, but each record's value decoded in
its own call so that the record's key could repeat per row. [`raw-key-before/`](raw-key-before/)
runs the raw-key workload on the production code just before keys decoded in batches, with the
same driver, on Flink 2.2.1:

| Records | Stock | Previous | Native | Stock/native | Previous/native |
|---:|---:|---:|---:|---:|---:|
| 1,000,000 | 1.296 [1.141–1.573] | 0.909 [0.882–1.162] | 1.106 [0.983–1.232] | 1.17× | 0.82× |
| 10,000,000 | 3.854 [3.122–4.540] | 2.652 [2.413–3.252] | 3.409 [3.291–4.101] | 1.13× | 0.78× |

That composition trailed the previous route and was barely faster than stock Flink. This
change's keyed decode, keys decoded in batches beside the values, brought the ten-million-record
native median from 3.409 s to 1.877 s (1.82×), or 1.60× to 1.88× once each run is set against
its own stock and previous routes. Both cohorts ran on the same host and build profile. The earlier composition's run
started at a load average of about 11 and the batched run at about 4, but the previous route took
2.65 s and 2.75 s in them, so the load does not account for the gap.

## Workloads

Every record carries a key and a value over four partitions, with 64 tenants:

- **JSON key and value**: the key `{"tenant":"t<i mod 64>","id":<i>}` read as `k_tenant` and
  `k_id` through `key.fields = 'k_tenant;k_id'` and `key.fields-prefix = 'k_'`, and the value
  `{"name":"n<i>","amount":<i mod 1000>}`;
- **raw key, JSON value**: an eight-byte big-endian BIGINT key, and the value
  `{"tenant":"t<i mod 64>","name":"n<i>","amount":<i mod 1000>}`;
- **Avro Confluent key and value**: the JSON workload's fields as Confluent-framed Avro under
  producer record names (`com.example.OrderKey`, `com.example.Order`), resolved through a local
  registry that serves the one read endpoint both engines use (`GET /schemas/ids/<id>`).

Every table uses `value.fields-include = 'EXCEPT_KEY'`. The query aggregates the key fields with
the value's: `SELECT k_tenant, COUNT(*) AS records, SUM(amount) AS amount, MAX(k_id) AS last_id,
COUNT(name) AS named FROM cdc GROUP BY k_tenant` for the JSON and Avro tables, and `SELECT tenant,
COUNT(*) AS records, SUM(amount) AS amount, MAX(k) AS last_key, COUNT(name) AS named FROM cdc GROUP
BY tenant` for the raw-key table. Every route runs mini-batch aggregation (`allow-latency` 1 s,
`size` 50,000), parallelism four, a release native build and a single-node Kafka 4.2.2 broker running
on the host.

## Reproduce

With a Kafka broker at `localhost:19092`:

```bash
SF_BENCHMARK=true mvn -Pbench -pl streamfusion-runtime test -Dtest=KafkaSourceBenchmark \
  -Dkafka.bench.brokers=localhost:19092 \
  -Dkafka.bench.workloads=json-keyed,raw-keyed,avro-confluent-keyed \
  -Dkafka.bench.records=1000000,10000000 -Dkafka.bench.output=<archive>
python3 bin/report-kafka-source-bench.py <archive>
```

Add `-Pflink-1.18` (after a `clean`) for the Flink 1.18 runs. Without `kafka.bench.brokers` the
driver starts a Testcontainers broker instead; on macOS, Docker Desktop's port forwarding then
bounds every route's throughput.

## Environment

Apple M1 Max (10 cores, 32 GB), macOS, OpenJDK 17, Rust 1.94. The machine was shared: the VS Code
renderer and its Java and C++ language servers each used about one core. Each run started just
after its build, at a load average of about 4 for Flink 2.2, 6 for Flink 1.18 and 11 for the earlier
raw-key composition. Routes rotate their order every run, so load widens the ranges rather than
favoring one route. Each `environment.json` records the git objects of its measured build.
