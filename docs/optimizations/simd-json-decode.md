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
- **Schema-order dispatch** — Kafka producers normally emit object fields in their declared schema
  order. The walker first compares each key with the next schema field and advances by ordinal;
  reordered, duplicate, or unknown keys retain the compiled name lookup as a semantic fallback.
  The cursor-drift detector uses the same dispatch rule.
- **Parser-state reuse** — simd-json parses in place (it mutates the input), so each body is copied
  into one reused scratch `Vec`. Its internal `Buffers` and parsed `Tape` storage are both retained
  across documents; `fill_tape` clears and refills the existing nodes instead of allocating a tape
  vector for every Kafka record. The input copy is included in the measured win below.

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

On the Q3 native Kafka source's steady-state critical path, async-profiler attributed 2,846 of
30,607 decode CPU samples (9.30%) directly to the schema-field hash lookup, plus 1,288 comparison
samples specifically beneath appender/drift field dispatch. Schema-order dispatch reduced both
groups to zero in the matched post-change profile. The 8192-document Nexmark JSON-to-Arrow
Criterion median improved from 5.104 ms to 4.605 ms (**9.8% less time**). A 2M-event, mini-batching
off, best-of-three end-to-end run then measured Q3 at 1.139 s for StreamFusion versus 1.146 s for
Flink (**1.01x**).

The next Q3 profile then exposed simd-json's per-document tape allocation. Reusing the tape reduced
the same 8192-document Criterion median from 4.614 ms to 4.413 ms (**4.35% less time**). The matched
2M-event Q3 run improved to 1.084 s for StreamFusion versus 1.157 s for Flink (**1.07x**). A proposed
uninitialized slot array plus presence bitmap was rejected after its isolated overhead erased the
entire tape-reuse gain; the existing stack `Option<Value>` slots remain.
