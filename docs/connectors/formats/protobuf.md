# Protobuf

**Status:** Native encode and decode, with the fallbacks below.

Row fields map to protobuf message fields by name in both directions. See [Kafka](../kafka.md) for
how the `protobuf` value format combines with the rest of the connector's key/value pipeline.

## Encode

Native encode admits protobuf `bool`, `float`, `double`, `string`, the signed 32-bit integer
families mapped to Flink INT, and the signed 64-bit integer families mapped to BIGINT. It supports
those recursively through messages/ROW, repeated fields/ARRAY, and maps. Every table field must
name a descriptor field of the matching shape; extra descriptor fields remain unset.

- A `NULL` column leaves the corresponding proto field unset.
- A `NULL` nested inside a container (message/repeated/map field) is written as that field's type
  default; `protobuf.write-null-string-literal` controls what a null string becomes.
- The wire bytes match protobuf-java's exact serialization shape — including the map-entry fields
  protobuf-java always writes even at their default values, which prost would omit.
- `protobuf.read-default-values` is decode-only in Flink and is ignored on write, matching Flink.

Encode stays on Flink for protobuf bytes, enums, unsigned integers (`uint32`/`uint64`), unsigned
fixed integers (`fixed32`/`fixed64`), any other row↔descriptor mapping outside the admitted set, a
recursive message or deprecated proto2 group, or a `protobuf.write-null-string-literal` containing
a line break that the native plan cannot represent. Flink then performs its normal validation and
serialization.

## Decode

The native reader handles the protobuf shapes Flink exposes to SQL: every scalar wire type,
`bytes`, enums, nested and well-known messages, repeated fields, maps, proto2 presence, proto3
`optional`, and oneofs. It decodes directly into Arrow arrays and reconciles protobuf-specific
representations with Flink's table types:

- protobuf's unsigned 32- and 64-bit values retain their bits in Flink's signed `INT` and `BIGINT`;
- enum names become `STRING`, while numeric enum columns use Flink's requested integer width;
- `bytes` become `VARBINARY`; well-known messages remain ordinary nested `ROW` values, as in Flink;
- with the default `protobuf.read-default-values = 'false'`, absent proto2 values and empty
  containers preserve Flink's null behavior; proto3 scalar getters still produce protobuf defaults;
- `protobuf.read-default-values = 'true'` is native for proto3, including default nested messages
  and empty containers.

The generated message class is needed only while Flink plans the job. StreamFusion reads it through
Flink's user-code classloader, resolves the descriptor immediately, and serializes only portable
decoder-plan data into the Kafka source. Each task recursively compiles that descriptor into a
reusable native field/tag plan once when its decoder opens; batch decoding performs no descriptor
traversal. TaskManagers therefore do not need a separately generated Rust library or a second job
artifact, and every admitted message uses the same production decoder.

The decoder falls back to Flink's own deserializer, with each case caught at plan time, when:

- **`protobuf.read-default-values = 'true'`** is used with proto2, whose schema may declare an
  arbitrary default value for each field;
- **`ignore-parse-errors`** is set on a protobuf table — Flink skips a malformed message whole, but
  the native decoder fails on it instead, so the table falls back to Flink's own decode path (the
  JSON-decoded formats honor the per-message skip natively; CSV reproduces Flink's finer per-field
  granularity — see each format's own page);
- the schema contains a deprecated proto2 **group**, or a **recursive message** that cannot be
  represented by a finite Arrow schema without choosing an arbitrary maximum depth.
