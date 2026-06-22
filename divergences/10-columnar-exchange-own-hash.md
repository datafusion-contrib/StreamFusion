# The columnar exchange uses its own hash, not Flink's key-group hash

**Kind:** structural — how the keyed shuffle partitions rows.
**Diverges from:** Flink's keyed exchange, which partitions by key group (a
`MathUtils.murmurHash` of the serialized key, mapped to one of
`maxParallelism` key groups).
**Follows:** Comet's principle (match the host hash only when the host reads the
partitions) — applied in reverse: here nothing host-side reads them, so we don't.

## The decision
The columnar exchange splits a batch by key and routes each key's rows to a
channel using an internal hash (Rust `DefaultHasher`), **not** Flink's key-group
assignment. It guarantees only that *all rows of a given key land on the same
channel* — it makes no promise about *which* channel, and the channel does not
match what Flink's keyed exchange would pick.

## Why this is parity-safe
The downstream native window operator is **not a Flink keyed operator**. It holds
its windows in *operator* state and re-groups rows by key itself (the native
aggregator keys internally), so it never consults Flink's key-group assignment or
keyed-state backend. All it needs from the shuffle is co-location: every row of a
key together on one subtask. Any deterministic key→channel function gives that.

This is the same property the **row-fed** native window already relies on today:
it sits behind Flink's hash exchange as a plain (non-keyed) operator and re-groups
internally; Flink's hash co-locates keys, but the operator would be equally
correct behind any co-locating partitioner. The columnar exchange just supplies
its own co-locating partitioner while keeping the data in Arrow.

Comet, by contrast, must reproduce Spark's Murmur3(seed=42)+pmod exactly, because
Spark's shuffle reader and other operators consume the partitions by id. We have
no such consumer — the partitions are read only by our own native operator — so
matching Flink's hash would be cost with no benefit.

## Scope / consequences
- Substitution is gated: the columnar exchange is used **only** when its
  downstream is a native columnar operator that re-groups by key (today, the
  columnar window). A host operator behind the exchange would depend on Flink's
  key groups, so the exchange stays on the host there (the planner only rewrites
  the exchange as part of substituting the columnar window above it).
- The shuffle still ships Arrow batches over the network edge via Arrow IPC
  (`ArrowBatchSerializer`); only the channel-selection function differs from Flink.
- Watermarks propagate through the partition transformation unchanged (Flink
  broadcasts them to all channels and the downstream takes the min), so event-time
  semantics are unaffected.
