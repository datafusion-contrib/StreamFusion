# Run the Nexmark matrix with Apache Fluss as the source

**Status:** TODO (not started). Requested 2026-06-30.

Add **Apache Fluss** as a fourth source in `NexmarkMatrixBenchmark`, alongside the generator and the
three Kafka formats (json/avro/protobuf). Fluss is a streaming-native storage layer (Apache incubating,
ex-Alibaba/Ververica) with a first-class Flink connector and a **columnar** log/at-rest format.

**Why it's the interesting source for this engine.** Every source we benchmark today hands Flink
rowwise `RowData`, so the native island pays a `RowData → Arrow` transpose at ingest (and `Arrow →
RowData` at the sink). The differential profiles of the stateful queries show that perimeter transpose
is now a real share of the remaining cost once the operator bodies are columnar (e.g. q20 ~34%). Because
Fluss is columnar on the wire / at rest, it's the source most likely to let the native island ingest
**Arrow batches with little or no row transpose** — potentially StreamFusion's largest end-to-end margin
vs stock Flink. Worth measuring whether the Fluss connector can be read straight into Arrow (a native
decode/scan, like the Parquet/ORC file sources) rather than through `RowData`.

**Shape of the work.**
- A `fluss`-connector source table in the matrix harness (a Fluss test cluster/container, mirroring the
  Kafka Testcontainers setup), producing the same wide Nexmark event row.
- Compare native vs **stock Flink-on-Fluss** as the baseline, same steelman perimeter (rowwise sink,
  object reuse on both engines) so the comparison is honest.
- Investigate a native columnar read path for Fluss (skip the `RowData` transpose at ingest) — that is
  the point of the exercise; if it still has to go through `RowData`, note that and measure anyway.
- Report alongside the generator + Kafka rungs in the same table.

Relates to: ticket 30 (Nexmark benchmark), ticket 32 (native decode to Arrow at ingest), ticket 24
(columnar endpoints beyond local Parquet), ticket 34 (columnar sinks / nested boundary types).
