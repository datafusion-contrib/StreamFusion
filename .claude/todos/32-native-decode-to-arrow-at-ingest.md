# Native decode-to-Arrow at the ingestion edge (skip RowData on source formats)

**Status:** in progress — JSON decode kernel landed (Phase 0); ORC + Parquet file sources landed via
DataFusion's file scan + the framework's split handoff (Phase 1). Streaming decode operator (Phase 2)
next.
**Source:** every record we ingest from Kafka (or any non-Parquet source) is decoded on the
JVM `bytes → GenericRecord/JsonNode/… → RowData` and only *then* transposed to Arrow by our
source-edge `StreamPhysicalRowDataToArrow`. That row materialization is the single highest-traffic
transpose in any pipeline (it runs on *every* record, before any filtering). This ticket moves the
decode itself into native Rust, straight to Arrow, so the row representation never exists.

Flink formats overview (the input set this ticket covers):
https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/connectors/table/formats/overview/

## Why
- The expensive part of ingest is **not** the byte copy — it is the per-record `RowData`
  materialization (decode to an intermediate object, then field-by-field converters, then a
  full-width `RowData` allocation). For Avro that is `GenericDatumReader` → `GenericRecord` →
  `AvroToRowDataConverters` (see `~/data/flink/flink-formats/flink-avro/src/main/java/org/apache/flink/formats/avro/AvroToRowDataConverters.java`).
- This is the one transpose on the hottest edge, and it is the piece that makes StreamFusion
  compelling on **streaming ingest** specifically (today our strongest numbers are Parquet-source).
- Mature Rust `bytes → Arrow RecordBatch` decoders already exist for nearly every format (below),
  most sharing an identical push API, so the native side is largely a dependency-add + thin wrapper.

## Two source kinds, handled differently
The right design depends on where the bytes live:

- **File sources (filesystem connector: JSON/CSV/Avro/ORC/Parquet files).** The bytes are in a file
  Rust can open itself — so we **hand Rust the path and read+decode directly**, never routing the
  data through the JVM. This is exactly the existing native Parquet source (path + projection in,
  `ArrowBatch` out; no row world, no copy). There is no useful intermediate format — the file *is*
  the format. So a file source per format is a native source in the ParquetSource mold, and it is the
  **easiest** work (a proven pattern) — do these first. (Remote/Iceberg endpoints for these are
  ticket 24.)
- **Streaming sources (Kafka, etc.).** The bytes arrive over a socket inside Flink's connector
  (offsets, consumer groups, checkpoint/restore) and land on the JVM heap before we see them. Here we
  keep Flink's connector and pay one off-heap copy into a native **decode operator** (below). The
  zero-copy end state — owning the consumer in Rust — is large enough to be its own effort
  (**ticket 33**), deferred until the decode-operator path proves the copy is the bottleneck.

The native **decoders** (arrow-json/csv/avro, prost-reflect, …) are shared across both kinds; what
differs is the *entry point*: a file reader over a path vs. a push `Decoder` over a bytes column.

## Streaming seam: keep the Flink source, pay one cheap copy
The remainder of this section is the **streaming/Kafka** path. No Flink table format can emit Arrow:
the produced runtime type is always `RowData`. There are exactly three shapes, none carrying a
columnar type for a streaming source:

1. **Record-at-a-time** `DeserializationSchema<RowData>.deserialize(byte[]) → RowData` —
   JSON, CSV, Avro, Confluent-Avro, Protobuf, Raw.
2. **CDC envelope fan-out** `deserialize(byte[], Collector<RowData>)` emitting 0–2 rows per
   message with `RowKind` set — Debezium, Canal, Maxwell, OGG.
3. **File bulk** `BulkFormat<RowData, FileSourceSplit>` (file-only, never Kafka) — Parquet, ORC.

So the interception recipe is the same for every streaming format: take the **`raw` passthrough**
(each message → a one-column `BYTES` `RowData`;
`~/data/flink/flink-table/flink-table-runtime/src/main/java/org/apache/flink/formats/raw/RawFormatDeserializationSchema.java`),
keep Flink's source/connector for everything else, and move the real decode into a native, batched
operator at the source edge — replacing the `RowDataToArrow` transpose with a format decode kernel.
Downstream native operators consume `ArrowBatch` unchanged (existing `ColumnarInput`/`ColumnarOutput`
+ transition-inserter machinery).

### The one unavoidable copy (and why owning the consumer is ticket 33)
We do **not** reimplement Kafka consumption (partition discovery, offset commit, checkpoint/restore,
poll loop) here — Flink owns *getting the bytes*; Rust owns *bytes → Arrow*. Removing the copy
entirely means consuming Kafka in Rust so bytes land in Rust-readable memory from the socket on; that
is **ticket 33** (large, deferred until this path proves the copy is the bottleneck).

- The lowest seam that hands us the raw value is
  `~/data/flink-connector-kafka/flink-connector-kafka/src/main/java/org/apache/flink/connector/kafka/source/reader/deserializer/KafkaRecordDeserializationSchema.java:60`
  — `deserialize(ConsumerRecord<byte[], byte[]> record, Collector<T> out)`. The value-only path we
  bypass is `KafkaValueOnlyDeserializationSchemaWrapper.java:51`
  (`deserializationSchema.deserialize(message.value(), out)`), where `message.value()` is a JVM heap
  `byte[]`.
- **One copy (JVM-resident bytes → off-heap) is fundamental** and cannot be avoided without owning
  Kafka's fetch path: (a) the Kafka client's `ByteArrayDeserializer` already landed the value on the
  JVM heap, and (b) a moving GC means Rust cannot stably read JVM heap memory (`GetByteArrayRegion`
  copies; `GetPrimitiveArrayCritical` pins/stalls GC and is unsafe to hold across a batch decode).
  The newer Kafka `Deserializer(..., ByteBuffer)` overload does not help — that buffer is a view into
  Kafka's fetch buffer, recycled on the next `poll()`.
- **But collapse it to that one cheap copy:** in a custom `KafkaRecordDeserializationSchema`, append
  each record's bytes into a reused, off-heap `DirectByteBuffer` (heap→off-heap memcpy, the
  unavoidable copy), tracking per-record offsets. At the batch boundary hand Rust the pointer via
  JNI `GetDirectBufferAddress` (zero-copy crossing) + the offsets array; Rust feeds the slices into
  the format `Decoder` and flushes one Arrow batch. Result: one memcpy + one JNI crossing **per
  batch**, no per-record JNI array copy, **no `RowData` materialization**.

## Per-format findings (Flink decode path → Rust Arrow decoder)

Legend: ✅ ready (decoder exists, version-compatible) · ⚙️ ready decoder + native envelope step ·
🔧 decoder exists, we write the schema→Arrow mapping · 📁 file/bulk seam (not the Kafka edge).

### JSON — ✅
- Flink: `~/data/flink/flink-formats/flink-json/src/main/java/org/apache/flink/formats/json/JsonRowDataDeserializationSchema.java`
- Rust: `~/data/arrow-rs/arrow-json/src/reader/mod.rs` — `Decoder` (`:446`), `decode(&[u8]) -> usize`
  (`:472`), `flush() -> Option<RecordBatch>` (`:677`). arrow v58 (matches native crate).

### CSV — ✅
- Flink: `~/data/flink/flink-formats/flink-csv/src/main/java/org/apache/flink/formats/csv/CsvRowDataDeserializationSchema.java`
- Rust: `~/data/arrow-rs/arrow-csv/src/reader/mod.rs` — `Decoder` (`:600`), `decode` (`:636`),
  `flush` (`:657`). Same push API as JSON.

### Avro — ✅
- Flink: `~/data/flink/flink-formats/flink-avro/src/main/java/org/apache/flink/formats/avro/AvroRowDataDeserializationSchema.java`
  (+ `AvroToRowDataConverters.java`, the field-by-field cost we remove).
- Rust: `~/data/arrow-rs/arrow-avro/src/reader/mod.rs` — `Decoder` (`:642`), `decode` (`:706`),
  `flush` (`:862`); `ReaderBuilder::build_decoder()` (`:1312`). Full logical-type mapping in
  `codec.rs`; `examples/decode_kafka_stream.rs` is our exact use case.

### Confluent-Avro — ✅
- Flink: `~/data/flink/flink-formats/flink-avro/src/main/java/org/apache/flink/formats/avro/RegistryAvroDeserializationSchema.java`.
- Rust: same `arrow-avro` `Decoder`; Confluent wire format (`0x00` + 4-byte BE schema id) is built in
  via `Fingerprint::Id` + `SchemaStore` (`~/data/arrow-rs/arrow-avro/src/schema.rs:687`), with
  mid-stream schema switching (`reader/mod.rs:742-770`). Populate `SchemaStore` from the registry
  at plan/open time (registry URLs are in repo `CLAUDE.md`).

### Debezium / Canal / Maxwell / OGG (CDC) — ⚙️ (the standout fit)
- Flink (all emit a 4-way `RowKind` changelog via the `Collector` variant):
  - `~/data/flink/flink-formats/flink-json/src/main/java/org/apache/flink/formats/json/debezium/DebeziumJsonDeserializationSchema.java`
    (`deserialize(byte[], Collector)` ~`:130`, sets UPDATE_BEFORE/AFTER/INSERT/DELETE ~`:148-164`)
  - `.../canal/CanalJsonDeserializationSchema.java` (~`:213`)
  - `.../maxwell/MaxwellJsonDeserializationSchema.java` (~`:124`)
  - `.../ogg/OggJsonDeserializationSchema.java` (~`:158`)
- Rust: the body is JSON → `arrow-json` `Decoder` (Debezium also has a Confluent-Avro variant →
  `arrow-avro`). The before/after/op envelope → a **native interpreter that emits our `$row_kind$`
  column** (divergences/13) and feeds the existing native changelog operators (GROUP BY / updating
  join / Top-N). A CDC → native-changelog pipeline then materializes **zero rows** end to end. This
  lands directly on the changelog machinery we already built — highest-value target.

### Protobuf — 🔧 (decoder exists; we write the descriptor→Arrow mapping)
- Flink: `~/data/flink/flink-formats/flink-protobuf/src/main/java/org/apache/flink/formats/protobuf/deserialize/PbRowDataDeserializationSchema.java`
  — runtime-dynamic (loads the descriptor via `PbFormatUtils.getDescriptor()`).
- Rust: `~/data/prost` (v0.14.4) decodes the wire format, but is **compile-time codegen only**
  ("Does not include support for runtime reflection or message descriptors", `README.md:26`) — it
  does ship `prost-types` (`FileDescriptorSet`). To match Flink's dynamic model use **`prost-reflect`**
  (`DynamicMessage`, runtime descriptor-driven; mature ecosystem crate, not vendored). No turnkey
  proto→Arrow batch builder exists anywhere — the **descriptor → Arrow array** construction is the
  one custom piece (same category as arrow-avro's internals). Bounded build, not a from-scratch decoder.

### Parquet — ✅ already done (📁 file seam)
- Flink: `~/data/flink/flink-formats/flink-parquet/src/main/java/org/apache/flink/formats/parquet/ParquetFileFormatFactory.java`
  (`BulkFormat`, file-only).
- Rust: `~/data/arrow-rs/parquet` (`ParquetPushDecoder`). We already have a native Parquet source —
  different (file/row-group) seam, not the Kafka edge.

### ORC — ✅ ready (📁 file seam)
- Flink: `~/data/flink/flink-formats/flink-orc/src/main/java/org/apache/flink/orc/OrcFileFormatFactory.java`
  (`BulkFormat`, file-only).
- Rust: `~/data/orc-rust` (v0.8.0, arrow **58.0** — exact match) — `ArrowReaderBuilder`/`ArrowReader`
  implementing `RecordBatchReader` (`src/arrow_reader.rs:39,233,324`; re-export `lib.rs:74`). All
  ORC types/encodings/compression. File reader (correct seam for ORC; parallels our Parquet source).

### Raw — n/a
- `~/data/flink/flink-table/flink-table-runtime/src/main/java/org/apache/flink/formats/raw/RawFormatDeserializationSchema.java`.
  This is the *seam* (single `BYTES` column) we exploit for the streaming formats, not a decode target.

## Design
The native **decoders** are shared; the two source kinds wrap them differently:

- **File sources (easiest, do first).** A native source per file format in the ParquetSource mold:
  path + projection in, `ArrowBatch` out, reading directly in Rust (no JVM). JSON/CSV via
  arrow-json/csv file readers, Avro via arrow-avro, ORC via orc-rust. Matcher keys on
  `connector=filesystem` + `format=<x>` (like `ParquetSourceMatcher`). Remote/Iceberg endpoints for
  these are ticket 24.
- **Streaming/Kafka decode operator (harder).** A native columnar operator parameterized by format:
  `ArrowBatch[bytes] → ArrowBatch[decoded]`, fed by stock `raw` bytes + the off-heap handoff above.
  - JSON/CSV/Avro/Confluent-Avro share the identical `decode`/`flush` contract → one operator covers all.
  - CDC (Debezium/Canal/Maxwell/OGG): same body decoder + a native envelope→`$row_kind$` step, feeding
    the existing native changelog operators. Zero row materialization on a CDC pipeline.
  - Protobuf: `prost-reflect` + a descriptor→Arrow builder (the one real build).

## Open questions (streaming path)
- **Event time / watermarks.** The timestamp is a decoded field, opaque until native decode. Decide
  once: decode natively then feed our columnar watermark assigner, or pre-extract the ts field. (CDC
  carries source ts in the envelope.)
- **Own format id vs. reuse `raw`.** A dedicated `streamfusion`/`avro-arrow` format factory validates
  schema at DDL time and reads registry config, but is more surface than stock `raw` + a planner rule.
- **Where the substitution is decided.** A *source*-level rewrite (the scan's row type changes from
  the typed schema to `raw` bytes, with the decode operator restoring it) — the largest unknown.
- **Key/headers.** `raw` covers the value; a key column would be a second `BYTES` column off the
  `ConsumerRecord` (the `KafkaRecordDeserializationSchema` seam exposes key/headers/timestamp too).

## Scope / phasing (easiest → hardest)
- **Phase 0 — native decode kernels (shared primitives).** Pure-Rust `bytes → Arrow` decoders with
  unit tests + micro-benches, no JVM wiring. **JSON kernel done** (`JsonDecoder`, ~5.3 Melem/s on a
  three-field object, `json_decode/three_field_object`). The streaming path (Phase 2) will reuse it.
- **Phase 1 — file sources. DONE (ORC + Parquet).** Both read natively through DataFusion's file scan
  (Parquet via DataFusion's `ParquetSource`, ORC via datafusion-orc's `OrcSource`), with the
  framework's file source owning enumeration/split-assignment/checkpointing and the native side
  reading one file's byte range — splittable at row-group/stripe granularity, projection pushed into
  the decode. The old self-enumerating Parquet source is gone. Parity vs Flink's own readers.
  - **Avro OCF dropped:** Flink's Avro sink writes the row as a top-level nullable union
    (`["null", record]`), and **both** Arrow-bridge Avro readers reject it — arrow-avro at decoder
    build (`codec.rs`) and DataFusion's at record read (`arrow_array_reader.rs`: "expected avro schema
    to be a record"). DataFusion's *schema* converter tolerates it (wraps it as a `""` struct field)
    but its reader does not. Reading Flink-written Avro therefore needs OCF header surgery (rewrite the
    embedded writer schema to a wrapper record, then unwrap the struct) or an upstream reader that
    accepts a top-level union — neither worth it now. Registry/Kafka Avro is a top-level record and is
    fine — that's the streaming path below.
  - **CSV/JSON file sources** remain (text formats: schema must be supplied, decode semantics vs
    Flink's options need parity-gating) — lower priority than the streaming path.
  - **Test-baseline note:** flink-orc pins orc-core 1.5.6 (protobuf 2.5) which collides with Arrow's
    protobuf 4.x; the build pins protobuf 2.5 in test scope so the ORC baseline runs. The native read
    path never touches orc-core, so this is test-only.
- **Phase 2 — streaming decode operator.** The `ArrowBatch[bytes] → ArrowBatch[decoded]` operator +
  off-heap Kafka handoff + source-edge planner rule, for JSON/CSV/Avro/Confluent-Avro.
- **Phase 3 — CDC family.** Body decode + native envelope → `$row_kind$`, reusing changelog operators.
- **Phase 4 — Protobuf** (`prost-reflect` + descriptor→Arrow).
- **Later — fully native Kafka source: ticket 33** (removes the off-heap copy entirely).

## Parity / acceptance
- File sources: byte/row-identical to Flink's filesystem connector + stock format over the same files
  (read via both, compare), like the existing Parquet source's parity test.
- Each format: native decode produces a `RowData`-materialization-identical result to Flink's
  stock format, verified by the harness (decode via both paths from the same bytes, compare).
- CDC: the collapsed changelog (`NativeParity.assertChangelogParity`) matches Flink's `RowKind`
  stream.
- A vs-Flink ingest throughput number per format (the readme carries one per operator).
- The off-heap handoff pays exactly one memcpy + one JNI crossing per batch (assert no per-record
  JNI array copy in the hot path).
