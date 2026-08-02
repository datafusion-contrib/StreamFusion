# Native exactly-once Kafka production

**Applies to:** the exactly-once Kafka sink

The exactly-once sink's record path moves entirely into Rust: librdkafka serializes and produces
each Arrow batch inside the checkpoint epoch's transaction, eliminating the per-record heap `byte[]`
materialization and the Java producer from the hot path. Flink's stock committer still finishes the
transaction from the (transactional.id, producer id, epoch) identity — see
[divergences/26](https://github.com/datafusion-contrib/StreamFusion/blob/main/divergences/26-kafka-eo-java-commit-plane.md)
— so recovery semantics are unchanged.

Producers are one-shot per epoch and pre-warmed on a dedicated thread: creation, `init_transactions`,
and the coordinator identity read all overlap the previous epoch, keeping the barrier path to
flush-only.

With the produce-path parity fixes below, the exactly-once Nexmark matrix went from 19-of-23 wins
(mini-batching off) to 22-of-23 at 1.07–2.84x, with record-heavy queries gaining the most (q1
1.65x→2.22x, q14 1.45x→2.36x, q15 1.21x→2.02x; full tables in `docs/benchmarks.md`).

## Produce-path parity: behavior, not knob values

The first native-producer matrix regressed every record-heavy cell; a differential profile (the
producing thread ~80% asleep on queue-full backpressure) plus a Flink-free producer probe (native
transactional produce at 0.44x of kafka-clients on identical records) attributed it to translating
Java's *default values* into librdkafka keys with different semantics. Three fixes, each
probe-measured:

- `batch.size` is not defaulted across — Java's 16,384 is a per-partition accumulator target while
  librdkafka's caps the whole MessageSet; pinning it cost ~4x, and unpinning took the probe to
  2.3–3x the Java client.
- librdkafka's 100K message-count queue cap is disabled so the translated `buffer.memory` byte
  budget is the only bound, as in Java.
- Nagle is disabled to match kafka-clients' hardcoded `TCP_NODELAY`.

The probe stays in-tree (`SF_PRODUCER_PROBE`) as the fast attribution loop for future producer parity
questions.

Residual known gap: librdkafka computes record-batch CRC32C in software on ARM (an upstream FIXME),
worth tens of milliseconds per 500K records — a future upstream contribution, not a current
bottleneck.
