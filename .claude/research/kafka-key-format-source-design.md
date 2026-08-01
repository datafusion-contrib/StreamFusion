# Native `key.format` on the Kafka source — design (2026-07-31)

Investigation for accelerating tables with `key.format`/`key.fields`/`key.fields-prefix`/
`value.fields-include`, which today always fall back (`KafkaTables.decodeCommon`). Verified against
flink-connector-kafka @ 2960af0e (5.0). File:line cites were checked against StreamFusion main @
4814891.

## Flink's merge semantics (DynamicKafkaDeserializationSchema)

- Projections: `createKeyFormatProjection` maps `key.fields` **in declared order** (not schema
  order; duplicates legal) to physical indices; prefix must match and is stripped only from the
  KEY format's row type. `EXCEPT_KEY` = physical minus key positions, schema order; `ALL` +
  non-empty prefix is rejected. On plain `kafka`, `key.format` always implies non-empty
  `key.fields` (validated at DDL).
- Per record: key deserializer's rows are buffered, then the output is the **cartesian**
  `|keyRows| × |valueRows|`. Zero key rows ⇒ the record produces NOTHING (value ignored).
- **Null-key rule** (the big parity trap): a null Kafka key produces 0 key rows for json/csv/avro
  (record silently dropped) but ONE row with a null field for `raw` (record kept). RowKind comes
  from the value row.
- 1:N decodes: only the JSON family fans out (top-level JSON array → N rows). CDC formats are 1:N
  but can never be key formats (must be INSERT-only).
- upsert-kafka source: key.fields forced to the PK, value format wrapped only to advertise
  changelog {UPDATE_AFTER, DELETE} + upsertMode=true + forced EARLIEST. Tombstone (null value) ⇒
  DELETE row with key columns populated and every value column NULL; null-key tombstone silently
  dropped.
- Metadata: value-format metadata rides at the tail of the value row (`adjustedValueProjection`);
  moot for us while the physical-columns-only gate stands.

## What the native paths carry

- **Fused rdkafka source** (native/src/kafka.rs): buckets copy only `message.payload`;
  `message.key`/`key_len` are available in rdkafka-sys. Adding a second BinaryBuilder + a
  selectable `[key, body]` schema is small; everything downstream (NativeKafkaSplitReader,
  NativeSourceRecord, BoundedSplitTracker) is schema-agnostic. Constraint: the fused decode attach
  takes ONE decoder (FormatDriver v1 has a single `decode_body_batch`) — keyed decode needs an
  atomic dual attach and (beyond raw) a v2 vtable.
- **Shallow decode path**: KafkaSource is built with `valueOnly(ByteArrayDeserializer)`; element
  type `byte[]` (nullable). Key+value needs a custom `KafkaRecordDeserializationSchema<KeyedBytes>`
  (2-field record + a KeyedBytesTypeInformation mirroring NullableBytesTypeInformation).

## Alignment contract

Decoders drop rows invisibly today (per-row `continue`). Two contracts:
- **(A) presence mask** per decode (BooleanArray over input rows) — covers everything except JSON
  top-level arrays; merge = mask AND + filter, then positional scatter.
- **(B) source-index column** (Int32, monotone) — fully general (JSON arrays), merge via `take`.
Key and value formats can live in different DSOs, so the CONNECTOR orchestrates two decode calls
and does a pure-Arrow interleave; a FormatDriver v2 (`decode_body_batch_aligned`) carries the
mask/index. A v1-only DSO degrades to JVM-mediated keyed decode (attach refusal already modeled).

## Increments

1. **`key.format = 'raw'` — SHIPPED 2026-08-01** (decode-operator path; the fused source declines
   keyed tables). Implementation deviates from the sketch in one good way: no new SPI/JNI surface
   at all — the keyed composition (`KeyedDecodeSpec` markers → `keyed.*` option lines → a rust
   `KeyedDecoder` wrapping the value decoder + the parity-pinned `RawDecoder`, per-record value
   decode for exact source alignment incl. JSON array fan-out) rides the existing
   createDecoder/decodeInto entry points, and the keyed edge frames key+value bytes into one
   byte[] element so the plain nullable-bytes serializer carries it. Value formats admitted:
   JSON/CSV/raw. e2e parity pinned vs stock Flink (both fields-include modes, null keys).
2. **json/csv keys** — contract (A) + driver ABI v2 + atomic dual attach; re-enable projection
   pushdown with a key/value split in CalcMatcher; parity cases: null-key drop, whitespace-only
   key, bad key + ignore-parse-errors drops the whole record.
3. **upsert-kafka source** — separate project: row-kind-carrying batches, per-row DELETE synthesis
   (key columns only), null-key-tombstone drop, `.changelogSafe()` substitution; smallest win
   (ChangelogNormalize already native) with the hardest semantics.

## Blockers / traps found

- **StreamPhysicalNativeKafkaDecode digest omits key.format** (`explainTerms` = topic + value
  format) — two tables differing only in key.format would digest identically. Must fix with the
  feature (reuse-barrier hazard). The fused source's sharingKey (whole options map) is safe.
- Option plumbing is the biggest mechanical chunk: `NativeFormatProviders.formatIdentifier` and
  `NativeFormatOptions.option/encode` are hard-wired to the value prefix; key options live ONLY
  under `key.<fmt>.*` with factory defaults otherwise (the sink already models this).
- `streamfusion-kafka` shades an explicit class allowlist from streamfusion-runtime — every new
  planner/operator class must be added or the connector artifact NoClassDefFoundErrors at runtime
  while passing all in-repo tests.
- `NativeBodyBatchDecoder.beforeDecode` is hard-coded to column 0 — a keyed avro-confluent key
  would need per-column hooks (deferred; registry keys are last/never anyway).
- A keyed shallow operator must flush both vectors atomically pre-barrier.

## Live divergence found (independent of key.format) — FIXED 2026-07-31

**Top-level JSON arrays**: fixed — both native JSON subpaths now fan a top-level array out into
one row per element with Flink parity (strict: any bad element fails the message; skip mode:
per-element drop, granularity notes in divergences/21), and the CDC envelope decode explicitly
rejects array roots on both subpaths (the arrow-json subpath previously fanned a CDC array out —
a silent divergence, also fixed). Consequence for the alignment contract above: the plain `json`
VALUE format is genuinely 1:N with no per-row source-message metadata surviving the decode, so a
keyed JSON-value table needs contract **(B)** (source-index column) — contract (A)'s presence
mask cannot represent fan-out. raw/csv/avro/protobuf values stay ≤1 row per message (mask-able).
