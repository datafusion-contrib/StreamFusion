# Batch-native Kafka JSON serialization

**Applies to:** the Kafka JSON sink encode path

## Whole-batch encode instead of per-record Jackson

A whole Arrow batch crosses JNI once and arrow-json encodes its rows in one writer pass, rather than
transposing the batch to `RowData` and invoking Flink/Jackson once per record. The JNI call
materializes the final heap `byte[]` values directly because KafkaProducer's Java API requires them
— there is no intermediate native pointer registry or second copy/drain call. Flink's stock Kafka
sink still consumes those bytes, so the optimization changes only serialization while retaining its
transaction and recovery path.

A 4096-row scalar JSON Criterion comparison measured 592 µs / 6.92 M rows/s for one batch writer
versus 3.55 ms / 1.15 M rows/s when invoking the production writer once per row: **6.0x** from
batching alone. Broker tests additionally prove committed exactly-once output before and after a
post-checkpoint failover.

## Records produce straight from the encode buffer

The JSON encoder used to split its one line-delimited batch buffer into a fresh `Vec<u8>` per record
before materializing the final JVM records — a per-record native allocation and copy that was pure
waste because the encode-to-Java path must materialize a heap `byte[]` regardless. The encoder now
returns one buffer plus per-row line ranges, and JNI reads those slices in place.

Together with the escape fast path below, the 4096-row Criterion encode dropped 592 µs → 497 µs per
batch (**6.9 → 8.2 M rows/s**); the q9 differential profile had shown the copy as part of the sink's
memmove + allocator-churn tail.

## Escaping is a bulk scan, not a per-byte table walk

arrow-json hands every string value to serde_json's serializer, which scans one byte at a time
against an escape table — and almost no real value needs escaping at all. The sink's encoder factory
now supplies a string encoder that answers "anything to escape?" with a word-at-a-time scan (the
standard SWAR zero-byte/less-than masks over eight bytes per step — the same idea simdjson's
serializer applies with SIMD) and bulk-copies the clean value; values that do escape take a loop
replicating serde_json's exact table, so output stays byte-identical (pinned by a parity test against
the stock arrow-json writer across every escape class).

Strings are the wide columns of the changelog-heavy sinks — q9's upsert rows carry `itemName`,
`description`, and two Nexmark `extra` paddings per record.
