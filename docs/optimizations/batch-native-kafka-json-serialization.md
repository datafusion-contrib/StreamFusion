# Batch-native Kafka JSON serialization

**Applies to:** the Kafka JSON sink encode path

## Whole-batch encode instead of per-record Jackson

A whole Arrow batch crosses JNI once and arrow-json encodes its rows in one writer pass, rather than
transposing the batch to `RowData` and invoking Flink/Jackson once per record. Flink's stock Kafka
sink still consumes the encoded bytes, so the optimization changes only serialization while
retaining its transaction and recovery path.

The sink also creates a task-local native encoder handle at operator `open`. Format options are
parsed once, while logical-type descriptors, field names, and key/value projections are copied and
projected once. The first batch builds the annotated Arrow schema and parses the nested
`TIMESTAMP_LTZ` marking tree; later batches reuse that plan. The steady-state batch call carries
only the handle and Arrow C Data addresses. This mirrors
Flink's task-local `SerializationSchema` lifecycle and removes configuration reconstruction from
every query's producer path.

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

## Encode rows directly into the final slab

Arrow's line-delimited writer creates a 16 KiB staging buffer, copies full chunks into the caller's
writer, appends a newline after every row, and leaves the caller to scan the completed output to
recover record boundaries. Kafka already needs one contiguous slab plus offsets, so the sink now
uses Arrow's lower-level row encoder directly: each row appends to the final slab and its start/end
offsets are recorded immediately. The scalar encoders and StreamFusion's Flink-parity overrides are
unchanged.

On the 4096-row production Criterion case this reduced the median from 519.3 µs to 394.5 µs
(**24.0% less time**, 7.9 → 10.4 M rows/s). In matched Q0 exactly-once profiles, JSON encoding fell
from 40.0% to 30.2% of critical sink-task CPU samples; Kafka's record accumulator remained about
half of that path.

## Return contiguous slabs instead of one JNI array per record

The first batch implementation returned `byte[][][]`. Although encoding was batched, JNI still
allocated and populated one JVM `byte[]` for every key and value. The production handoff now follows
Comet's bulk row-transfer shape: one key slab, one value slab, and offset/length arrays. A negative
length distinguishes a null key or Kafka tombstone from a present empty value. Each emitted record
holds a view of those JVM-owned slabs; the stock Kafka serializer performs the unavoidable final
`byte[]` copy when Kafka accepts that record.

This keeps ownership simple—Kafka may retain a record after the native call returns—while reducing
the return boundary from O(rows) JNI allocations and object-array stores to six JNI arrays per
batch. In the 2M-row q19 exactly-once profile, per-record JNI materialization fell from **29.2% to
1.0%** of critical-task CPU samples, and serialization plus JVM handoff fell from **53.6% to 34.1%**.
The post-change headline run completed q19 in 7.820 s versus Flink's 8.461 s (1.08x); repeated-loop
wall times remained noisy, so the removed profile frames are the stronger attribution.

## Pre-size the JSON output buffer

The writer now reserves one Arrow batch footprint before encoding. That lower bound avoids most
grow-and-copy work without paying to zero an aggressively oversized allocation. The 4096-row
production encoder improved from 493.9 µs to 490.4 µs (**1.8%**); reserving twice the Arrow
footprint regressed by 13.9% and was rejected.

## Give Kafka's accumulator room to batch

Kafka 4.2 defaults each destination partition to a 16 KiB producer batch with a 5 ms linger. q19's
task threads consequently spent 32.3% of their producer-path samples parked on accumulator and
sender-wakeup monitors: the sender drained tiny batches while the task appended the next record.
The headline harness now gives both Flink and StreamFusion the same 512 KiB batch and 20 ms linger.
That is large enough for roughly one partition's share of the 8192-row source batch while remaining
well below the job's one-second checkpoint interval.

In adjacent 2M-row q19 best-of-two runs, Flink improved 8.461 s → 6.992 s and StreamFusion improved
7.820 s → 6.321 s. A follow-up profile reduced monitor-wait share within the producer path from
32.3% to 24.9%; the remaining framing share rose because the task spent more time making forward
progress. The tuned result was 1.11x StreamFusion/Flink.

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
