# Columnar sinks + nested types across the native↔host boundary

**Status:** open — **nested-type transpose DONE** (direction 2 below); columnar sinks (direction 1) and
the protobuf representation reconciliations remain.

## Done (2026-06-26)
- The row↔Arrow transpose now carries every logical type, including `ROW`/`ARRAY`/`MAP`/`VARBINARY`. We
  replaced the hand-rolled converter with Flink's own Arrow↔RowData machinery, vendored in-tree
  (`io.github.jordepic.streamfusion.arrow`, repackaged from `flink-python`, Apache-2.0) — the per-type
  column readers/field writers plus a trimmed schema/reader/writer factory (`ArrowConversion`), timestamps
  pinned to nanos to match the native side. `RowDataArrowConverter` is now a thin wrapper (write/read +
  `$row_kind$`); `read()` is column-backed then deep-copied off the buffers (the boundary operator releases
  the batch immediately — true zero-copy still wants direction 1).
- The protobuf gate is loosened accordingly: nested-message (→ROW), repeated (→ARRAY), and map (→MAP)
  fields of supported scalars now route, parity-tested against Flink (`NativeProtobufDecodeSqlHarnessTest`).
- **All Kafka format sources now carry complex types**, each parity-tested against Flink for nested
  ROW/ARRAY/MAP: protobuf, bare-Avro (`NativeAvroDecodeSqlHarnessTest`), JSON
  (`NativeJsonDecodeSqlHarnessTest`), and CDC/Debezium nested rows (`NativeCdcDecodeSqlHarnessTest`).
  CSV/raw are flat by nature. To get JSON onto the (nested-capable) shallow decode path, the fully-native
  rdkafka source is now opt-in/off by default (`operator.kafkaSource.enabled`, also behind the `kafka`
  cargo feature) — JSON falls through to the decode operator like the other formats. One fix the JSON map
  case surfaced: `ArrowConversion.toArrowSchema` must force the map key non-null (Flink's data model) or
  the decoded map is rejected when read back.
**Source:** the row↔Arrow transpose (`RowDataArrowConverter`) carries only scalar/string/temporal/
decimal columns; it has no case for `ROW`/`ARRAY`/`MAP`/`VARBINARY`. So any native operator that
*produces* a nested column cannot hand it back to a rowwise host operator (a sink, `collect()`, or a
not-yet-native downstream op) — the Arrow→RowData edge throws `unsupported column type`.

## Why this matters now
The native decoders already produce nested Arrow for several formats — protobuf messages with nested
`message`/`repeated`/`map` fields, Avro records with nested records/arrays, JSON with nested objects.
But because the boundary transpose can't carry those columns, a `SELECT * FROM t` (which lands in a
rowwise `collect`/sink) would fail at the transpose. So the planner currently **gates those tables to
fall back** (e.g. `ProtobufDescriptors.isFlatScalarMessage` routes only flat scalar protobuf; nested/
repeated/map/bytes/enum fall back). The decoders are capable; the boundary is the limit.

## Two directions (the end state)
1. **Custom columnar sinks** — the preferred end state for columnar workflows. A native columnar sink
   (and columnar `collect`) consuming `ArrowBatch` directly means a native source→…→sink pipeline pays
   **no transpose at all**, nested columns included. This is the real fix for throughput: the transpose
   exists only to bridge to rowwise host operators, so removing the rowwise edge removes the problem.
   Relates to ticket 24 (columnar endpoints) — the Parquet sink is already columnar; generalize the
   pattern and add a columnar collect for SQL result fetching.
2. **Nested types in the transpose** — the fallback bridge for when a rowwise host operator is genuinely
   downstream. Extend `RowDataArrowConverter` (both `read` and `write`) to map `StructVector`↔`RowData`,
   `ListVector`↔`ArrayData`, `MapVector`↔`MapData`, and `VarBinary`↔`byte[]`. This lets complex messages
   route even into a rowwise edge, and benefits **every** format (not just protobuf).

## Remaining
- **Direction 1 (columnar sinks)** — the throughput end state, still open. A native columnar sink + a
  columnar `collect` consuming `ArrowBatch` directly removes the transpose for columnar workflows and
  lets `read()` be a true zero-copy view (the batch outlives the rows). Relates to ticket 24.
- **Protobuf representation reconciliations** (no longer boundary gaps — they're decode-semantics):
  `enum` (int-vs-name matched to the declared column type), unsigned/`fixed` ints (unsigned here vs signed
  in Flink — a value divergence), `bytes` (decode works and the boundary carries `VARBINARY`; only the
  `byte[]` parity *test* comparison is missing), proto3 missing-field defaults, and well-known types
  (`google.protobuf.*`, dedicated Arrow type vs Flink's nested row). Each gated off until reconciled + parity-tested.
- **Avro/JSON nested objects** can now route through the boundary too (same fix); wire + parity-test when picked up.
