# Protobuf

**Status:** Native encode and decode, with the fallbacks below.

Row fields map to protobuf message fields by name in both directions. See [Kafka](../kafka.md) for
how the `protobuf` value format combines with the rest of the connector's key/value pipeline.

## Encode

On write, the provider reruns the same row↔descriptor mapping that Flink's own submission
validation performs — a mismatched column type or name is caught at plan time, so the table falls
back and Flink raises its own error rather than StreamFusion diverging on a bad mapping.

- A `NULL` column leaves the corresponding proto field unset.
- A `NULL` nested inside a container (message/repeated/map field) is written as that field's type
  default; `protobuf.write-null-string-literal` controls what a null string becomes.
- The wire bytes match protobuf-java's exact serialization shape — including the map-entry fields
  protobuf-java always writes even at their default values, which prost would omit.
- `protobuf.read-default-values` is decode-only in Flink and is ignored on write, matching Flink.

## Decode

The native protobuf decoder falls back to Flink's own deserializer, each caught at plan time, when:

- a field's wire representation needs reconciliation with Flink's mapping: **enums**,
  **unsigned integers**, **bytes**, or the **well-known types** (`Timestamp`, `Duration`, wrapper
  types, and the like);
- the message's presence shape makes an unset field decode differently from Flink's: a
  **non-proto3** `.proto` file, a proto3 **`optional` scalar**, or a **scalar `oneof` arm**;
- **`protobuf.read-default-values = 'true'`** is set — natively, matching Flink's *default* mode,
  an absent or empty-on-the-wire message/repeated/map field decodes to `NULL`; the `true` setting
  asks Flink to materialize default instances instead, which the native decoder doesn't produce;
- **`ignore-parse-errors`** is set on a protobuf table — Flink skips a malformed message whole, but
  the native decoder fails on it instead, so the table falls back to Flink's own decode path (the
  JSON-decoded formats honor the per-message skip natively; CSV reproduces Flink's finer per-field
  granularity — see each format's own page).
