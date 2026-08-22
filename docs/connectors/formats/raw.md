# Raw

**Status:** Native encode and decode, single physical column only, with the fallbacks below.

`raw` treats one physical column as the entire message body — no framing, no field names on the
wire. It's the simplest format [Kafka](../kafka.md) supports, and the most tightly parity-pinned:
both directions are checked byte-for-byte against Flink's own (de)serializer
(`RawDecodeParityTest`).

## Encode

The declared column is written verbatim as the record's value:

- `CHAR`/`VARCHAR` writes UTF-8 bytes; `BINARY`/`VARBINARY`, including fixed-length `BINARY`,
  writes the bytes as-is.
- `BOOLEAN` writes a single byte.
- `TINYINT`/`SMALLINT`/`INT`/`BIGINT`/`FLOAT`/`DOUBLE` write a fixed-width buffer in the table's
  `raw.endianness`.

The column **must be `NOT NULL`**: Flink serializes a null field as a null `byte[]`, which becomes
a Kafka tombstone — a shape the native sink's value path never produces, so a nullable raw column
isn't accelerated.

A **non-UTF-8 `raw.charset`** stays on Flink on write, the same as on read (below).

## Decode

Each whole Kafka message becomes the single physical column's value:

- `CHAR`/`VARCHAR` and `VARBINARY` pass through unchanged.
- `BOOLEAN` reads one byte, `!= 0`.
- `TINYINT`/`SMALLINT`/`INT`/`BIGINT`/`FLOAT`/`DOUBLE` read an exact-length buffer honoring
  `raw.endianness` (`'little-endian'`/`'big-endian'`, case-insensitive; big-endian is the
  default).
- A wrong-length fixed-width message fails the job with Flink's own error text — `raw` defines no
  `ignore-parse-errors`.
- A null message stays a null field.

One deliberate divergence from Flink: a string column fed invalid UTF-8 bytes fails the native
decode outright, because Arrow strings must be valid UTF-8, where Flink smuggles the bytes through
unvalidated as `StringData`.

### Fallbacks

Each of the following is caught at plan time, before the job starts:

- a schema with **more than one physical column** (`raw` is single-column only; Flink raises its
  own `ValidationException`);
- an **invalid `raw.charset`/`raw.endianness`** value (again, Flink's own `ValidationException`);
- a **non-UTF-8 `raw.charset`** — any charset name that resolves to UTF-8 is native; the decode
  path has no Java charset machinery to fall back on for anything else;
- a **`RAW<T>` column** — its bytes belong to a Java `TypeSerializer`, not a wire encoding (see the
  type-level `RAW` exclusion);
- a **fixed-length `BINARY` column on decode** — Flink passes any message length through verbatim,
  while Arrow's fixed-size binary type enforces the declared length exactly. Fixed-length BINARY
  remains native on encode.
