# Decode inside the poll, dispatched through a driver ABI

**Applies to:** the native Kafka source's format decode

## History

The format-artifact split had moved decode out of the source's poll call into a planner-inserted
operator. A profiling round showed that arrangement
serializing the format work — 65% of a JSON job's CPU — behind the island on the task thread, and an
A/B against the pre-split source pinned the rest of its cost to leaving the poll call itself.

## What it is

Decode now runs inside `pollKafkaBatch` again, on the just-written, cache-hot payload bytes,
invoked through an ADBC-style versioned C-ABI vtable that the format DSO's exported init fills in
for the connector. The address is handed over via Java but never linked; a driver's refusal or
absence falls back to the split reader's JVM-mediated decode.

## Measured

On the like-for-like ladder corpus this runs **faster than the pre-split fused source ever did**:
JSON q0 2.35× vs 1.94× stock Flink, 1.80M vs 1.53M ev/s, same machine/day.

On the timestamp-heavy matrix corpus the operator-bearing queries gained from the restored
consume∥island overlap: q11 protobuf 2.11× → 3.13×, q11 avro 1.68× → 2.21×.

The pass-through matrix cells remain bound by timestamp-string parsing both engines pay — the
corpus difference that had masqueraded as a regression, since the old published Kafka table used
the ladder's BIGINT-timestamp schema rather than the matrix corpus's string timestamps.
