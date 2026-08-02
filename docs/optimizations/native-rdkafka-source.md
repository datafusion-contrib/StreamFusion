# Native rdkafka source

**Applies to:** the native Kafka source's consume path

## What it is

The consume loop itself moved into Rust: payloads are polled straight into an Arrow binary builder,
with no JVM heap `byte[]` and no per-record JNI call (`7b16368`, `0f229d9`).

## Measured

~5x the JVM-client path on raw consume+decode.

## Production-shaped refinements

Raw consume speed alone wasn't production-shaped throughput; three further changes closed that gap:

- **Pipelining the reader** — draining the batch queue to amortize per-message poll cost, with
  decode overlapped on a background thread — took it from parity to ~1.15x over the shallow path on
  JSON (`b5fa0c2`).
- **Socket buffer auto-tuning** — letting librdkafka auto-tune socket buffers instead of pinning the
  Java client's small defaults removed a measurable throttle (`f6e658b`).
- **Poll timeout** — cutting the poll timeout 1000 → 100 ms removed dead seconds at a bounded read's
  tail (`81a9f54`).

The background decode thread introduced here was later removed once consume itself got fast enough
to make inline decode win — see [Kafka consume fast path](kafka-consume-fast-path.md).
