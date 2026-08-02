# SIMD JSON parsing

**Applies to:** the native JSON and CDC decoders

## What it is

The JSON and CDC decoders parse with simd-json's two-stage approach instead of arrow-json's scalar
byte-at-a-time tokenizer (`bee7b44`): SIMD structural indexing finds every brace/quote/colon in wide
vector operations, producing a flat *tape* (no DOM), which a schema-driven walk then appends
straight into typed Arrow builders (`decode_json_bodies_simd`). Semantics are pinned to the old
path's coercions.

## How the walk works

- **One appender per output column** — `JsonAppend` implementations per Arrow type. The walk
  dispatches on the *schema*, not the JSON, so unknown keys are skipped without materializing
  anything, and each column's builder does its own coercion inline.
- **Row-aligned object walk** — an object's fields are collected into a slot-per-schema-field
  scratch (stack-allocated up to 32 fields, duplicate keys last-wins like Jackson/arrow-json), then
  appended one value per child so every column stays aligned even with missing or reordered keys.
- **Buffer reuse** — simd-json parses in place (it mutates the input), so each body is copied into
  one reused scratch `Vec`, and the parser's internal `Buffers` are reused across documents. That
  copy is included in the measured win below.

## Pinned semantics, one carve-out

The walker replicates arrow-json's per-type coercions exactly, because those are what the Kafka
parity tests hold against Flink. The exception is DECIMAL: simd-json parses numbers eagerly to
i64/f64 and drops the raw literal, so a decimal wider than f64 precision would round. Schemas
containing a decimal anywhere — recursively through ROW/ARRAY/MAP — keep the arrow-json
raw-literal path instead, which parses the exact digit string like Flink's `BigDecimal`.

## Measured

A realistic ~210 B Nexmark-bid document into a 3-column projected schema dropped 1.36 ms → 985 µs
per 4096-row batch (+37%). The CDC decoders share this walk, and their Debezium envelopes are
several times the payload size. This is what flipped JSON from tokenize-bound parity to the Rust
decode being JSON's best rung across the Nexmark matrix (`fc78b3d`).
