# Connectors

**[Kafka](kafka.md)** is the only connector StreamFusion considers production-grade today, across
every [format](#formats) Flink itself supports for it. It's covered in depth in this section.

[Parquet](parquet.md) (a local file source, and a sink to any filesystem Flink supports) and
[Delta Lake](delta.md) (partitioned append and merge-on-read sinks) also have native paths, but they
have not seen the same production hardening as Kafka — treat them as experimental.

## Formats

A format decodes or encodes the bytes on the wire; it's independent of which connector carries
them (though not every connector supports every format — see each connector's page). StreamFusion
ships a native implementation for:

- [JSON](formats/json.md)
- [CSV](formats/csv.md)
- [Avro](formats/avro.md)
- [Avro Confluent](formats/avro-confluent.md) (schema-registry-backed Avro)
- [Protobuf](formats/protobuf.md)
- [Raw](formats/raw.md) (the single-column passthrough format)
- [CDC JSON](formats/cdc-json.md) (Debezium, OGG, Maxwell, and Canal's changelog envelopes)

Each optional format is its own `streamfusion-*` Maven artifact and native library, mirroring
Flink's own connector/format module split — installing a format you don't use never pulls its
native code into a connector you do. A missing format module is always a normal planner fallback
to Flink's own decode/encode path, never a linkage failure. See [Deployment](../deployment.md) for
which JARs a given connector+format combination needs.
