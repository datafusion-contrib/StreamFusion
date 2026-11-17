# Canal include filters: Kafka source routes

A Canal topic usually multiplexes a database's tables, and a Flink table reads its share through
`canal-json.database.include` / `canal-json.table.include`. These runs compare three routes for
such a table on Flink 2.2.1, from planning through the last collected row:

- **stock**: Flink's Kafka source and Canal deserializer, Flink operators;
- **previous**: Flink's Kafka source and Canal deserializer, a RowData→Arrow transpose, then
  StreamFusion's native operators (what a table with include filters ran before this change);
- **native**: Flink's Kafka source handing bytes to the native Canal decode with the filters, then
  the same native operators.

Each run's output digest must match across the three routes, and each saved plan must show its
route (only the native plan decodes natively; the previous plan still runs native operators).

## Results

Median [min–max] seconds over six measured trials after two warmups; ratios divide medians.

| Workload | Records | Stock | Previous | Native | Stock/native | Previous/native |
|---|---:|---:|---:|---:|---:|---:|
| one of ten tables | 1,000,000 | 1.214 [1.198–1.433] | 1.124 [1.065–1.191] | 0.973 [0.965–0.983] | 1.25× | 1.16× |
| one of ten tables | 10,000,000 | 5.164 [4.992–6.820] | 4.886 [4.583–4.910] | 3.329 [3.264–4.783] | 1.55× | 1.47× |
| all ten tables | 1,000,000 | 1.640 [1.395–2.139] | 1.293 [1.188–1.395] | 1.168 [1.067–1.185] | 1.40× | 1.11× |
| all ten tables | 10,000,000 | 6.831 [6.574–7.635] | 5.787 [5.518–6.054] | 5.051 [4.950–5.205] | 1.35× | 1.15× |
| sharded tables | 1,000,000 | 1.342 [1.230–1.558] | 1.124 [1.099–1.449] | 1.006 [0.980–1.098] | 1.33× | 1.12× |
| sharded tables | 10,000,000 | 5.274 [4.871–5.451] | 5.053 [4.779–5.391] | 4.018 [3.929–4.144] | 1.31× | 1.26× |

The one-million-record runs are dominated by job startup; the ten-million-record runs measure
sustained decoding. [Every sample](observations.jsonl), [the audited table](results.md), the
[method](method.json), the [environment](environment.json) and each route's plan are kept here.

## Workloads

Every message is a Canal INSERT as Canal writes it: string values plus the `database`, `table`,
`es`, `ts`, `id`, `isDdl`, `mysqlType`, `sqlType`, `pkNames`, `old` and `sql` envelope fields.
Messages rotate through the tables; the measured table admits a share of them, spread over all
four partitions, with `database.include = 'shop'` and:

- **one of ten tables**: ten tables, `table.include = 't0'` (10% admitted);
- **all ten tables**: ten tables, `table.include = 't\d'` (every message admitted, so the filter
  is pure overhead on top of the decode);
- **sharded tables**: a million tables, `table.include = 't\d*0'` (10% admitted). Nearly every
  table name is new to the native verdict cache, so this is the filter's worst case.

The query is `SELECT tenant, COUNT(*) AS changes, SUM(amount) AS amount FROM cdc GROUP BY tenant`
with mini-batch aggregation (`allow-latency` 1 s, `size` 50,000) on every route, four partitions,
parallelism four, a release native build and a single-node Kafka 4.2.2 broker running on the host.

## Earlier runs that shaped this one

Both are kept, raw, under [`superseded/`](superseded/):

- [`collect-bound`](superseded/collect-bound/) ran without mini-batching and was stopped partway
  (no sharded runs, and the all-ten-tables runs incomplete). The aggregate emitted an update per
  input row, and fetching those results bound every route alike: the measured all-ten-tables trials
  at a million records spanned 8.47–9.31 s whichever route decoded them. Mini-batching keeps the
  result small so the source is what gets measured.
- [`per-value-upcalls`](superseded/per-value-upcalls/) is a complete run of an earlier revision.
  Its native filter decided each uncached value with its own JVM call, and Canal's old-image
  presence scan parsed every message a second time. At ten million records it measured the sharded
  case at 7.900 s against 5.571 s previous (0.71×), and the all-ten-tables case at 6.395 s against
  6.378 s (1.00×). Batching a decode batch's undecided values into one JVM call, and scanning only
  updates for presence, produced the results above.

## Reproduce

With a Kafka broker at `localhost:19092` and Docker not required:

```bash
SF_BENCHMARK=true mvn -Pbench -pl streamfusion-runtime test -Dtest=KafkaSourceBenchmark \
  -Dkafka.bench.brokers=localhost:19092 \
  -Dkafka.bench.workloads=canal-one-of-ten-tables,canal-all-ten-tables,canal-sharded-tables \
  -Dkafka.bench.records=1000000,10000000 -Dkafka.bench.output=<archive>
python3 bin/report-kafka-source-bench.py <archive>
```

Without `kafka.bench.brokers` the driver starts a Testcontainers broker instead; on macOS, Docker
Desktop's port forwarding then bounds every route's throughput.

## Environment

Apple M1 Max (10 cores, 32 GB), macOS, OpenJDK 17, Rust 1.94. The machine was shared: the VS Code
renderer and Java language server each used about one core, and the load average was about 7.3
when the run started. Routes rotate their order every run, so this load widens the ranges rather
than favoring one route. `environment.json` records the git objects of the measured build; this
commit carries the same production code.
