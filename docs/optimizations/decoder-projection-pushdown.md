# Projection pushdown into every decoder

**Applies to:** the JSON, Avro, and protobuf decoders

## What it is

The query's projection narrows what each decoder builds, rather than decoding every wire field and
discarding the unread ones afterward (`64ddc2a`, `83b3d69`, `86908f1`, `4af9d63`).

- **JSON** decodes straight to the narrowed schema.
- **Avro** keeps the full writer schema (required for correct decoding) but materializes only the
  reader-schema columns.
- **Protobuf** prunes its descriptor so unread fields are skipped on the wire.

## Measured, and why the split

Profiling drove the split: build/copy-bound formats gain a lot from pruning, tokenize-bound JSON
gains little, because JSON's cost is the parse itself rather than building/copying values.

- Avro: Kafka/Avro q0–q2 1.06–1.18x → 1.64–1.83x.
- Protobuf: 0.88–0.94x → 1.26–1.36x.
