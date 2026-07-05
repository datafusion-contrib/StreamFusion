# Native decode-to-Arrow at the ingestion edge (skip RowData on source formats)

**Status:** core DONE — trimmed to the tail. File sources (Parquet + ORC) land via DataFusion's file
scan + the framework's split handoff; the streaming decode operator (`NativeBytesDecodeOperator`) is
built, wired (`NativeKafkaDecodeExecNode`, routed by `KafkaTables`), and parity-tested for JSON,
Confluent/bare Avro, CSV, protobuf, and Debezium/OGG CDC. `avro-confluent` routing shipped 2026-07-03:
the operator fetches each frame's writer schema from the registry by id on first sight (plain-HTTP
`GET /schemas/ids/<id>`, no Confluent client dependency), patches the reader's record names onto it as
aliases (arrow-avro enforces the spec's name check; Avro Java skips it), and registers it into the
native store — following mid-stream schema evolution like Flink's own deserializer; auth/SSL/explicit-
schema registry options fall back (coverage doc §5). **Remaining tail only:** (a) **Maxwell/Canal**
exact-parity auto-routing (decoded, but parity-gated to fallback today); (b) **CSV/JSON *file*** sources
(the lower-priority file formats — Avro OCF was dropped, arrow-avro can't read Flink's top-level-union);
(c) the **JSON half of the format-option parity audit** (found 2026-07-03 while wiring
`ignore-parse-errors`): the JSON decode ignores `json.fail-on-missing-field=true` and
`json.timestamp-format.standard` (the native timestamp parser is lenient — it accepts ISO-8601
`T`-separated strings where Flink's default `SQL` standard throws), and its scalar coercions don't
match Flink's converters (string-encoded numbers with trim, `Infinity`/`NaN`/`1.5d`, number→STRING
echo, strict `ISO_LOCAL_DATE`) — match natively per `flink_text.rs` (the shared Flink-exact parsers
the CSV rewrite introduced) or gate the option. **The CSV half SHIPPED (2026-07-05):** the decode
was found silently divergent on valid data even with default options (empty string → NULL,
truncating decimals, no trimming, unread delimiter/quote/null-literal options, ARRAY columns routed
then crashed) — arrow-csv's envelope isn't configurable into Flink's, so the decode now splits
records with csv-core and converts with Flink-exact parsers, options honored natively
(escape-character and non-scalar columns gated), `ignore-parse-errors` per-field granularity
included, all parity-pinned by `CsvDecodeParityTest` (divergences/21).
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

The native **decoders** (simd-json/arrow-csv/arrow-avro, prost-reflect, …) are shared across both kinds; what
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

**Verified factory inventory (Flink 2.2, `~/data/flink`, 2026-06-26).** A Kafka `value.format`/
`key.format` accepts *any* `DeserializationFormatFactory` on the classpath; the Kafka connector defines
none of its own. The complete built-in set, by `factoryIdentifier()`:
- Plain (append): `json` (flink-json), `csv` (flink-csv), `avro` (flink-avro, **bare** Avro — no
  framing, schema from the RowType), `avro-confluent` (flink-avro-confluent-registry, **Confluent
  framing + registry**), `protobuf` (flink-protobuf), `raw` (flink-table-runtime, single column).
- CDC (changelog): `debezium-json`, `canal-json`, `maxwell-json`, `ogg-json` (flink-json),
  `debezium-avro-confluent` (flink-avro-confluent-registry).
- **File-only, NOT Kafka** (these are `*FileFormatFactory`/`BulkReaderFormatFactory`, not
  `DeserializationFormatFactory`): `parquet`, `orc`, and the file variants of avro/csv/protobuf.

The 11 Kafka formats collapse to **5 underlying wire codecs**: JSON text (`json` + the 4 CDC-JSON, the
CDC ones add only an envelope), Avro binary (`avro` bare / `avro-confluent` framed / `debezium-avro-
confluent`), Protobuf, CSV text, Raw. Direct-to-Arrow Rust crates exist for the first four codecs
(`arrow-json`/`arrow-csv`/`arrow-avro`, all already on the classpath; raw is trivial); **only Protobuf
and the CDC envelopes have no off-the-shelf Arrow decoder** — those are the real build (see below).

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

**Decoder status (2026-06-26): JSON, CSV, raw, bare-Avro, Confluent-Avro, and Protobuf decoders are all
BUILT and unit-tested** in `MessageDecoder` (native/src/formats.rs). `createDecoder(format, …)` codes:
**0 = JSON, 1 = Confluent-Avro, 2 = CSV, 3 = raw, 4 = bare Avro**; Protobuf via `createProtobufDecoder`.
The CDC family (Debezium/OGG/Maxwell/Canal) is also built, and the planner rule that auto-routes a SQL
`format=…` table to the native decode operator is wired (`KafkaTables` → `NativeKafkaDecodeExecNode`)
for JSON, Confluent/bare Avro, CSV, protobuf, and Debezium/OGG. See the Status line for the residual
tail (Maxwell/Canal auto-routing; CSV/JSON file sources; format-option parity audit).

### JSON — ✅ built (format 0)
- Flink: `~/data/flink/flink-formats/flink-json/src/main/java/org/apache/flink/formats/json/JsonRowDataDeserializationSchema.java`
- Rust: a simd-json tape walk into typed Arrow builders, schema-driven (arrow-json's raw-literal
  path retained for decimal-bearing schemas — divergences/18). `JsonDecoder` in native/src/json.rs.

### CSV — ✅ built (format 2)
- Flink: `~/data/flink/flink-formats/flink-csv/src/main/java/org/apache/flink/formats/csv/CsvRowDataDeserializationSchema.java`
- Rust: `arrow::csv` `Decoder` (same push API as JSON), `with_header(false)`; each message is one record,
  fed newline-terminated. `CsvDecoder` in native/src/formats.rs. (v1 uses default CSV options — delimiter/quote/escape/
  null-literal parity with Flink's options is a follow-up.)

### Avro (bare) — ✅ built (format 4)
- Flink: `~/data/flink/flink-formats/flink-avro/src/main/java/org/apache/flink/formats/avro/AvroRowDataDeserializationSchema.java`
  (+ `AvroToRowDataConverters.java`, the field-by-field cost we remove). Bare datum, **no framing**;
  reader schema from the table RowType (`AvroSchemaConverter`).
- Rust: `arrow-avro` has no prefix-free push decoder (`build_decoder` requires a writer-schema store),
  so we register the reader schema at **synthetic id 0** and **prepend the 5-byte id-0 Confluent header**
  to each datum — reusing the framed decoder. `decode_avro_body(.., bare=true)`.

### Confluent-Avro — ✅ built and routed (format 1)
- Flink: `~/data/flink/flink-formats/flink-avro/src/main/java/org/apache/flink/formats/avro/RegistryAvroDeserializationSchema.java`.
- Rust: same `arrow-avro` `Decoder`; Confluent wire format (`0x00` + 4-byte BE schema id) is built in
  via `Fingerprint::Id` + `SchemaStore` (`~/data/arrow-rs/arrow-avro/src/schema.rs:687`), with
  mid-stream schema switching (`reader/mod.rs:742-770`).
- **Routed (2026-07-03):** the store starts empty; the decode operator scans each batch's frame ids
  and, on first sight of an id, fetches the writer schema from the registry (plain HTTP, mirroring
  the client's one read endpoint), alias-patches the reader's record names onto it (Avro Java skips
  the resolution name check; arrow-avro enforces it but honors aliases), and registers it natively —
  so evolution mid-stream works exactly like Flink's lazy per-id lookup. Registry auth/SSL/explicit
  `schema` options fall back. Parity: `NativeConfluentAvroDecodeSqlHarnessTest` (mock registry +
  Kafka container, two writer versions, projection).

### Debezium / OGG / Maxwell / Canal (CDC JSON) — ✅ all four built (formats 6, 7, 8, 9)
- **Built (2026-06-26):** `CdcJsonDecoder` (native/src/formats.rs) decodes a CDC envelope straight to a
  **columnar changelog batch** — physical columns + trailing `$row_kind$` Int8. One JSON-decode pass decodes
  every body's envelope (the pre/post images as nested structs of the physical columns, made nullable; the op
  field per dialect; unknown envelope fields ignored), then each physical column is gathered with a single
  `arrow::compute::interleave` choosing the right pre/post-image struct child **per output row** (built per
  field so the partial-`old` coalesce can pick per field). One unified pre-index/post-index gather model
  handles all four dialects (and Canal's array fan-out — the indices just point into the flattened list
  values). RowKind bytes 0/1/2/3 match `RowKind.toByteValue()`. A null body (tombstone) and unknown/skip ops
  contribute no rows.
- **`CdcDialect` (4 dialects), via a `CdcSpec` {before_field, after_field, op_field, shape, arrays} + op→action map:**
  - **Debezium** (code 6): `{before, after, op}`, op `c`/`r`→INSERT, `u`→UB+UA, `d`→DELETE. Shape
    `BeforeAfter` (full pre/post images; null `before` on u/d skips the record, matching Flink throw +
    ignore-parse-errors).
  - **OGG** (7): `{before, after, op_type}`, `I`/`U`/`D`, `T` truncate skipped. Shape `BeforeAfter`.
  - **Maxwell** (8): `{data, old, type}`, `insert`/`update`/`delete`. Shape `DataOld`: `data` = full
    post-image, `old` = *partial* pre-image — UPDATE_BEFORE is `coalesce(old, data)` per field; DELETE reads
    `data` (it holds the deleted row).
  - **Canal** (9): `{data, old, type}` as **arrays** (`arrays=true`), `INSERT`/`UPDATE`/`DELETE`, `CREATE`
    (DDL) skipped. Same `DataOld` merge, applied per array element (`data[i]` paired with `old[i]`); a message
    fans out per element. Envelope images are `List<Struct>`; the gather indexes into the flattened list values.
  - **Divergence (DataOld):** a field deliberately changed *to* null is indistinguishable from an absent
    (unchanged) field once the JSON decode parses `old`, so it falls back to `data`; Flink keeps the null
    (`oldField.findValue` sees JSON key presence). Rare; documented in `CdcShape::DataOld`.
- All decoded against the exported physical schema via `createDecoder` (no new JNI). Rust tests green:
  `cdc_debezium_decode_emits_changelog`, `cdc_debezium_skips_tombstone_and_unknown_op`,
  `cdc_ogg_dialect_uses_op_type`, `cdc_maxwell_merges_partial_old_image`,
  `cdc_canal_fans_out_arrays_and_merges_old` (49 Rust tests pass).
- **Strictness = Flink parity (2026-06-26; skip mode added 2026-07-03):** in default mode the decoder
  does **not** silently drop bad rows; with `ignore-parse-errors` it skips per message, like Flink. An unknown op
  or a null pre-image on an update/delete **fails** (`panic`, the codebase's bad-data convention), matching
  Flink's default *throw* — so the result is identical (both produce no wrong output), never a silent
  divergence. Only a tombstone/empty message is skipped (Flink skips those too). Canal `CREATE` (DDL) is the
  one deliberate skip Flink also makes. Tests: `cdc_unknown_op_fails`, `cdc_debezium_null_before_update_fails`
  (both `#[should_panic]`).
- **Planner wiring — identical-or-fall-back (2026-06-26):** the contract is identical results to Flink, so the
  planner routes a CDC table natively **only when we reproduce Flink exactly**, else it falls back (leaves
  Flink's own decoder in place). `KafkaTables.isCdcDecode` routes a scan only when **all** hold, else fall back:
  - format ∈ {debezium-json (6), ogg-json (7)} — the full pre/post-image dialects. **Maxwell (8)/Canal (9)
    fall back**: their partial-`old` pre-image can't be reproduced bit-identically from the decoded image
    alone (a field changed *to* null is indistinguishable from an unchanged/absent one once the JSON decode
    decodes `old`; Flink uses raw JSON key-presence). Decoders + unit tests stay (correct for the
    no-null-change case); just not planner-routed.
  - `<format>.schema-include` ≠ true (the `{schema, payload}` wrapper isn't handled).
  - ~~`<format>.ignore-parse-errors` ≠ true~~ — **lifted 2026-07-03**: skip mode is native. Flink
    implements it as a catch-everything around each message's decode; the native decode mirrors that
    with per-message isolation (decode the batch optimistically; if anything in it fails, redo it
    message by message, dropping the failures — fresh state per try). Wired for the JSON-decoded
    formats (plain `json` and the CDC envelopes); CSV/protobuf with the flag fall back, and the
    native *source* declines flagged tables so the decode path (which honors the skip) takes them.
  - **all columns physical** (`FilesystemTables.allPhysicalColumns` via the resolved schema) — metadata/
    computed columns aren't produced by the value decode.
  A CDC source emits a changelog, so — like the native GROUP BY/join/Top-N — `isCdcDecode` is checked
  **before** `PhysicalPlanScan`'s insert-only guard; the append decode path (`isNativeKafkaDecode`, codes
  0/2/3 only) stays below it. Shared `kafkaDecode(...)` helper builds `StreamPhysicalNativeKafkaDecode`. The
  existing Arrow→RowData transition reads `$row_kind$` back onto each row, so the decode node feeds a correct
  `RowKind` changelog downstream with **no `RowData` envelope decode**.
- **End-to-end test:** `NativeCdcDecodeSqlHarnessTest` (SF_BENCHMARK-gated, testcontainers, one Kafka
  container, a topic per dialect): debezium/ogg use `NativeParity.assertChangelogParity` (route + collapsed
  changelog matches stock Flink's `*JsonDeserializationSchema`); maxwell/canal use `NativeParity.assertFallback`
  (asserts the query stays on Flink and still matches Flink's result).
- **Remaining CDC follow-ups (each would lift one fallback restriction):** (a) **Maxwell/Canal exact parity** —
  needs JSON key-presence in `old` (a light per-message scan, or carry presence out of the decode) to match
  Flink's "absent ⇒ copy data, present-null ⇒ keep null"; then route them. (b) **`schema-include`**
  (`{schema, payload}` wrapper) + CDC **metadata columns** (ts_ms, source.*) — decode the wrapper / project
  metadata, then drop those fallback gates. (c) **debezium-avro-confluent** — same envelope shape, Avro
  decoder inside (arrow-avro against a `ROW<before,after,op>` reader schema). (d) **primary-key/upsert**
  CDC tables get a `ChangelogNormalize` above the scan — verify/handle the wrapper. (e) Canal uneven
  `data`/`old` arrays (length mismatch) — current decoder falls back to post-image; reconcile with Flink's
  `getRow(i)` throw. (`ignore-parse-errors` shipped 2026-07-03 — see the planner-wiring bullet.)

#### Original design notes (retained)
- Flink (all emit a 4-way `RowKind` changelog via the `Collector` variant):
- Flink (all emit a 4-way `RowKind` changelog via the `Collector` variant):
  - `~/data/flink/flink-formats/flink-json/src/main/java/org/apache/flink/formats/json/debezium/DebeziumJsonDeserializationSchema.java`
    (`deserialize(byte[], Collector)` ~`:130`, sets UPDATE_BEFORE/AFTER/INSERT/DELETE ~`:148-164`)
  - `.../canal/CanalJsonDeserializationSchema.java` (~`:213`)
  - `.../maxwell/MaxwellJsonDeserializationSchema.java` (~`:124`)
  - `.../ogg/OggJsonDeserializationSchema.java` (~`:158`)
- Rust: the body is JSON → the shared JSON decode (Debezium also has a Confluent-Avro variant →
  `arrow-avro`). The before/after/op envelope → a **native interpreter that emits our `$row_kind$`
  column** (divergences/13) and feeds the existing native changelog operators (GROUP BY / updating
  join / Top-N). A CDC → native-changelog pipeline then materializes **zero rows** end to end. This
  lands directly on the changelog machinery we already built — highest-value target.
- **How Flink does it internally (confirmed in source, 2026-06-26):** the CDC format is *not* a new
  wire decoder — it is a thin layer over the plain decoder. `DebeziumJsonDeserializationSchema`'s ctor
  builds `createJsonRowType(...)` = `ROW<before: <physical>, after: <physical>, op: STRING, …>` and
  hands it to a stock `JsonRowDataDeserializationSchema`. `deserialize(byte[], Collector)` then (1)
  decodes the **whole envelope to a `GenericRowData`** via that inner decoder, (2) reads `op`, pulls the
  nested `before`/`after` rows, sets `RowKind`, and emits the **physical** row(s): `c`/`r`→after INSERT;
  `u`→before UPDATE_BEFORE + after UPDATE_AFTER (**one message → two rows**); `d`→before DELETE. So the
  order is **decode-envelope-to-RowData first, then a cheap envelope→physical transform** — it reuses
  the JSON/Avro RowData decoder verbatim. (`debezium-avro-confluent` = same shape, Avro decoder inside.)
- **Rust reference (no standalone crate, but a proven pattern): RisingWave.** `~/data/risingwave`
  `src/connector/src/parser/{unified/debezium.rs, debezium/*}`: a `ChangeEvent` trait (`op()` +
  `access_field(before/after)`) and `DebeziumChangeEvent<A>` over a **lazy `Access` accessor** on the
  parsed JSON/Avro — it does *not* materialize a full envelope row; it reads `op`, then lazily accesses
  only the needed before/after sub-object, writing columns straight into a chunk builder. Output is a
  **`StreamChunk` = columnar data chunk + an `Ops` array** (`Op::{Insert,Delete,UpdateInsert,
  UpdateDelete}`), update→two ops — i.e. the *columnar* form of Flink's RowData+RowKind, exactly our
  `RecordBatch` + `$row_kind$` column. (Arroyo does the same, serde-struct based, less vectorized.)
  Both live inside their projects — no `arrow-cdc` crate to drop in; we port the pattern.
- **Columnar wrinkles a row-at-a-time engine avoids but we must handle:** (a) **update row-doubling** —
  output row count ≠ input row count, computed per batch then gathered; (b) **nested before/after
  extraction** — decoding the envelope yields `before`/`after` as nested *struct*
  columns, so building the final top-level columns is a per-row `take`/gather of the right struct's
  fields by `op`, plus the `$row_kind$` Int8 column. RisingWave sidesteps this by being accessor/row-
  oriented at parse; our batch decode does struct-column surgery + gather.

### Protobuf — ✅ decoder built (via `ptars`) and JVM-wired + routed
- **Flink's wire format = BARE protobuf bytes, NOT Confluent (confirmed in source, 2026-06-26).**
  `PbRowDataDeserializationSchema.deserialize(byte[])` → `ProtoToRowConverter` → `parseFrom(byte[])` on
  the **whole** message: no magic byte, no schema id, no message-index varints. The descriptor comes
  from a **generated Java class** named by the `message-class-name` option (`PbFormatUtils.getDescriptor`
  → `Class.forName(name).getMethod("getDescriptor")`). **There is no Confluent-protobuf format in OSS
  Flink** (only `avro-confluent`/`debezium-avro-confluent` are registry-framed). So the earlier
  "Confluent-registry-driven" plan was wrong for matching Flink — corrected to bare-bytes + descriptor.
- **A proto→Arrow library DOES exist — `ptars` (`~/data/ptars`, crates.io).** (Earlier note "no turnkey
  proto→Arrow builder exists anywhere" was wrong.) It converts the protobuf **wire format straight into
  Arrow arrays** — no per-row `DynamicMessage`, no intermediate value tree — driven by a runtime
  `prost_reflect::MessageDescriptor`, deriving the batch schema from the descriptor. Entry:
  `binary_array_to_record_batch_direct(&BinaryArray, &MessageDescriptor, &PtarsConfig) -> RecordBatch`
  (also `MessageDecoder::new/finish`, `messages_to_record_batch`). Handles nested/repeated/maps/enums/
  well-known types. `PtarsConfig` knobs: `confluent_wire_policy` (Raw = Flink's bare bytes; Standard/
  Protobuf strip Confluent framing for the future registry case), `EnumRepr` (Int32/String/Binary),
  timestamp units.
- **Rejected `prost-arrow` (`~/data/prost-arrow`).** It's a **compile-time `derive(ToArrow)`** on
  prost-generated structs — needs the `.proto` at *build* time, so it can't decode a runtime
  user-supplied schema in a SQL connector. (Also stale, 2024-03.) `ptars`'s runtime/descriptor model is
  the right fit; `prost` alone is wire-codec/codegen only (no Arrow); Arroyo does proto→`serde_json`→
  arrow-json (an extra hop); RisingWave hand-rolls DynamicMessage→its columns. ptars is the most direct.
- **Built (2026-06-26):** `ProtobufDecoder` (native/src/formats.rs) = `prost_reflect::DescriptorPool::decode`
  of a `FileDescriptorSet` → `MessageDescriptor` → `ptars::binary_array_to_record_batch_direct`; wired as
  `MessageDecoder::Protobuf`; JNI `createProtobufDecoder(byte[] descriptor, String messageName)`; Java
  `Native.createProtobufDecoder` declared; Rust unit test green (`protobuf_decode_emits_one_row_per_
  message`); full `cargo test` (41) + `mvn test` (242) green.
- **Caveats / remaining:**
  - **ptars is a crates.io dep** (`ptars = "0.0.18"`), Apache-2.0, `0x26res/ptars`. The arrow-58 bump
    we'd carried as an in-tree vendor patch was upstreamed (#243) and released as v0.0.18, so the fork is
    gone — the plain crate now matches our arrow. prost-reflect pinned to ptars' **0.16**.
  - **Schema parity vs Flink:** ptars *derives* the Arrow schema from the descriptor; Flink derives the
    RowType via `PbToRowTypeUtil`. Reconcile edge cases (enum repr — set `EnumRepr` to match Flink;
    64-bit ints; timestamps/well-known; `oneof`) and either align via `PtarsConfig` or cast/rename to the
    declared schema. Add a parity test (decode via both, compare) before shipping.
  - **JVM wiring DONE:** `ProtobufDescriptors` serializes the descriptor off the generated message
    class to a `FileDescriptorSet`, `KafkaTables` gates+routes `protobuf` via `NativeKafkaDecodeExecNode`,
    and `NativeProtobufDecodeSqlHarnessTest` covers it end to end. Remaining representation
    reconciliations (enum/unsigned/bytes/defaults) are tracked in ticket 34.
  - ptars also goes the **other way** (`record_batch_to_array`/`MessageEncoder`) — free protobuf *sink*
    encoding when we need it.

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

### Raw — ✅ built (format 3) — and still the seam for the others
- `~/data/flink/flink-table/flink-table-runtime/src/main/java/org/apache/flink/formats/raw/RawFormatDeserializationSchema.java`.
- Dual role: (1) the *seam* (single `BYTES` column) we exploit to get every other format's bytes to the
  native decoder; (2) a decode target in its own right — `RawDecoder` casts the body column to the
  single declared column type (Binary passthrough, or Binary→Utf8 for a STRING column), 1:1 with rows.

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
- **Phase 2 — streaming decode operator + planner routing. DONE for JSON/CSV/raw (2026-06-26).** The
  source-level rewrite is built: a Kafka `format=…` scan is substituted (`PhysicalPlanScan`) with
  `StreamPhysicalNativeKafkaDecode` → `NativeKafkaDecodeExecNode`, which chains **Flink's own
  `KafkaSource` producing raw value `byte[]`s** (value-only `ByteArrayDeserializer`; Flink owns
  consume/offsets/checkpoint/auth — `KafkaTables.buildBytesSource`) → **`NativeBytesDecodeOperator`**
  (batches the bytes, one native decode per batch via the shared `MessageDecoder`) → Arrow. No Flink
  `RowData` decode. The downstream Arrow→RowData transpose at the collect/sink edge is inserted by the
  existing transition pass. End-to-end SQL test green (`NativeKafkaDecodeSqlHarnessTest`, a bounded CSV
  table). Gated by `operator.kafkaDecode.enabled`; ordered after the native-rdkafka-source branch, so a
  JSON table still uses that (ticket 33) and CSV/raw take the decode path. flink-csv added (provided) so
  Flink can validate+build the table before substitution (it validates with the format factory even
  though we replace the decode). **Remaining:** wire Avro (the `AvroSchemaConverter` top-level-union
  schema handling) and protobuf (reflective descriptor extraction off `message-class-name`) into
  `decodeFormatCode`/the exec node — the decoders already exist (formats 1/4/Protobuf); only the
  schema/descriptor extraction at plan time is left. The operator flushes on batch size, end of
  input, a processing-time interval (the latency bound for a stream below the batch size — shipped
  2026-07-03), and before each checkpoint barrier (buffered bytes are records the source's checkpoint
  already covers; the pre-barrier flush keeps them from being lost on restore).
  (`NativeJsonBytesDecodeOperator` is now superseded by the general `NativeBytesDecodeOperator`
  — only a benchmark still references the old one.)
- **Phase 3 — CDC family. DONE for Debezium + OGG JSON (2026-06-26).** `CdcJsonDecoder` decodes the
  `{before, after, op}` envelope in one JSON-decode pass and `interleave`s the right before/after struct
  child per output row to a columnar changelog (physical columns + `$row_kind$`), updates fanning out to
  UB+UA — the batch form of RisingWave's row-at-a-time `DebeziumChangeEvent`. Formats 6 (debezium-json) /
  7 (ogg-json); routed before `PhysicalPlanScan`'s insert-only guard since a CDC source is itself a
  changelog; Arrow→RowData transition restores each row's `RowKind`. Parity vs Flink's own
  `DebeziumJsonDeserializationSchema` (`NativeCdcDecodeSqlHarnessTest`). Remaining: Canal/Maxwell
  (different envelope shapes), debezium-avro-confluent (Avro body), schema-include + metadata, upsert/PK
  ChangelogNormalize — see the CDC per-format finding above.
- **Phase 4 — Protobuf. Decoder DONE (2026-06-26)** via `ptars` (wire→Arrow, descriptor-driven) +
  `prost-reflect` for the runtime descriptor; `MessageDecoder::Protobuf` + `createProtobufDecoder` JNI +
  Rust test green. Remaining: JVM descriptor-extraction off the message class + planner/operator gating +
  end-to-end SQL test; Flink schema-mapping parity. (ptars vendored in-tree with the arrow-58 patch —
  OSS-portable, done.) See the Protobuf finding above.
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
