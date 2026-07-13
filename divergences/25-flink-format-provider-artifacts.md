# 25 — Flink-style format artifacts over a private cross-DSO ABI

## Reference pattern

Flink distributes connector and format implementations as separate JARs. The table runtime discovers
them through `META-INF/services/org.apache.flink.table.factories.Factory`, so a job installs only the
connector and serialization formats it uses.

## StreamFusion decision

StreamFusion follows that deployment shape for native value decoding. `streamfusion-kafka` owns Kafka
consumption and emits Arrow batches containing raw Kafka value bodies. `streamfusion-json`,
`streamfusion-csv`, `streamfusion-raw`, `streamfusion-avro`,
`streamfusion-avro-confluent-registry`, and `streamfusion-protobuf` register
`NativeFormatProvider` implementations through Java `ServiceLoader`. The planner selects a provider
only when its artifact and supported options are present; otherwise it leaves the table on stock Flink.

## Why not let Kafka call every format directly?

An earlier native Kafka source owned the message decoder. That made its DSO link Kafka plus every
format dependency and made the base deployment unable to follow Flink's optional-format convention.
Passing a Rust decoder handle from the Kafka DSO to a format DSO would exchange Rust-owned objects and
allocator state across dynamic-library boundaries, which is not a stable ABI.

Arrow's C Data Interface is already the ownership-safe JNI boundary in this project. The Kafka DSO
exports a body batch to Java and the format DSO imports that batch through the same interface; each
handle remains private to its creator. This adds a DSO boundary at source ingest, but keeps the format
installable, testable, and fallback-safe. A future fused ABI is acceptable only after benchmarks show
the boundary is material and it can preserve these ownership rules.

## 2026-07-12: the decode moved back into the poll, through an ADBC-style driver ABI

A benchmarking round answered the "future fused ABI" clause. Profiles first showed the
decode-as-operator arrangement putting the format work (65% of a JSON job's CPU) on the task thread
behind the whole island while the fetch thread idled, so the decode moved to the fetch thread via
the provider SPI; an A/B against the pre-split source then showed the remaining gap was not
threading, the extra copy, or the C Data crossings (~3%), and a decoder-only Criterion A/B showed
the decode itself unchanged. What the arrangement had actually lost was decoding **inside the poll
call** — and separately, the published pre-split Kafka numbers came from the ladder harness, whose
tables declare no watermark; the matrix's Kafka table does, and its pushed `WATERMARK` kept the
whole scan on Flink (the gate below), so the matrix "native source" cells of that period never
measured the native source at all — only the downstream island over Flink's own consume+decode.

The decode now runs inside `pollKafkaBatch` again, dispatched through the pattern ADBC's driver
manager uses (`AdbcDriverInit`): each format DSO exports one init function; the connector obtains
its address through the format's Java facade (never symbol linkage), calls it with the ABI version
it was compiled to speak, and the format fills a `#[repr(C)]` vtable or refuses. Everything
crossing the boundary is C — the vtable, function pointers, opaque handles, and Arrow C Data whose
release callbacks carry buffer ownership back into the producing library — so the original
ownership rules stand. A refusal (a format artifact from another release, or a format needing
per-batch JVM work like the registry variant) falls back to the split reader's JVM-mediated decode:
mixed-version deployments degrade in speed, never in correctness. On the like-for-like ladder
corpus this measured **faster than the pre-split fused source** (JSON q0 2.35× vs 1.94× stock
Flink, same machine and day), so the boundary now costs less than the coupling it replaced.

Watermark regeneration, gated off while decoding happened after the source (computing a rowtime
maximum inside the connector would have re-coupled it to formats), came back once the in-poll decode
gave the split reader typed batches: it reads the max rowtime off the decoded Arrow batch on the JVM
side — no format coupling, no ABI addition — and stamps it as the batch's record timestamp, driving
the same per-split strategy the Fluss source uses (Flink's own one-generator-per-split machinery,
min combination, idleness). Supported shapes and the remaining fallbacks are listed in
`docs/coverage-and-fallbacks.md`.
