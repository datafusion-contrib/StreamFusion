# Kafka consume fast path

**Applies to:** the native Kafka source's consume path — see
[divergences/19](../../divergences/19-kafka-consume-fast-path.md)

## The finding

Per-thread profiling showed librdkafka's delivery thread, not the app thread, capped native consume
~30% below the Java client. Its top non-I/O costs were per-message bookkeeping that the JVM
sidesteps via TLAB + bulk GC and CRC intrinsics.

## Three compounding levers

Together these took 10M-msg raw consume from 3.33M/s (0.73x the Java client) to 5.34M/s (1.21x):

- **mimalloc symbol-aliasing** — the opt-in `mimalloc` cargo feature link-aliases the libc
  allocation symbols to mimalloc inside the library only, covering librdkafka's per-message op
  calloc (broker thread) + free (app thread), which no Rust `#[global_allocator]` can reach, and the
  Rust side's own allocator churn — with no process-wide override
  (divergences/19 records the zone-swap and mimalloc-v3 dead ends).
- **`check.crcs=false`** — now follows librdkafka's own default; its software CRC32C on ARM (no
  hardware path outside x86 SSE4.2) taxed the delivery thread ~13.5% (+22%).
- **Bulk-drain via `rd_kafka_consume_callback_queue`** — bulk-moves the backlog under one queue lock
  instead of locking per message against the enqueuing broker thread (+11%), with `max_records`
  enforced via `rd_kafka_yield`.

## Downstream effects

The split reader's background decode thread is gone: with consume this fast, inline decode won on
every format (Flink already pipelines fetcher vs. task thread). The reader also now primes broker
metadata before `assign()` — a cold assign otherwise parks partitions in leader-query for ~0.5s
until the periodic refresh.

## Measured

Net: the end-to-end Nexmark Kafka ladder's source rung runs at 2.2–3.4x stock Flink with the
`mimalloc` build (JSON 2.20–2.26x, Avro 2.99–3.38x, protobuf 2.29–2.36x; ~2–2.6x on the default
build).
