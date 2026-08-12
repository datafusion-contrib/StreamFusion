# The columnar exchange follows Flink's key-group assignment

**Kind:** parity guarantee — how the keyed shuffle partitions rows.
**Matches:** Flink's `KeyGroupRangeAssignment`: `BinaryRowData.hashCode()`, then
`MathUtils.murmurHash`, with routing by `keyGroup * parallelism / maxParallelism`.

## The decision

The columnar exchange projects the logical key columns into Flink's `BinaryRowData` layout in Rust.
For every Arrow row it calculates the same BinaryRow hash Flink would calculate for the projected
`RowData` key, mixes that hash with `MathUtils.murmurHash`, and gathers rows into one Arrow record
per non-empty destination channel. The Flink partitioner maps a representative group tag from that
channel's range back to the same destination. `TIMESTAMP` precision is supplied by the planner,
because Arrow's timestamp type does not retain it.

This replaces the former `DefaultHasher` over Arrow row-encoding bytes. That internal hash was only
safe while every native consumer stored one opaque operator-state snapshot. Native `GROUP BY` now
writes one raw keyed-state payload per Flink key group, so the exchange and checkpoint layout must
agree exactly for rescaling to be correct.

## Scope / consequences

- Equal join keys on both inputs receive the same BinaryRow/key-group assignment, including NULL
  keys and the supported scalar key types.
- Aligned-only jobs follow Arroyo's destination-server batching shape while retaining Flink's key
  hash and channel mapping. Rows keep their original order within each destination.
- When unaligned checkpoints are enabled, StreamFusion deliberately uses a finer recovery wire
  shape: one gathered Arrow fragment per key group, tagged with parent sequence, row ordinals, and a
  compact non-empty-key-group manifest. Flink can keep/drop those records with its existing `RANGE`
  recovery filter; a checkpointed receiver reassembles destination-local parent order during normal
  execution by merging the fragments' already-sorted ordinal streams. Old attempt fragments remain
  independent on restore because siblings already processed before the checkpoint live in downstream
  operator state; the new attempt resumes reassembly under a fresh epoch. This avoids both
  topology-specific channel records and contiguous-run fragmentation without changing Flink's
  recovery API.
- Flink's configured `pipeline.max-parallelism` is the authoritative key-group count for every
  native keyed transformation and exchange. Deriving it again from restored parallelism would make
  checkpoints incompatible when a rescale crosses Flink's default-max-parallelism thresholds.
- The `GROUP BY` raw keyed-state path restores every key-group payload Flink assigns to a rescaled
  subtask and merges them back into one Rust hot-state map. Its 1→2 harness test exercises this
  redistribution.
- The recursive BinaryRow writer covers ARRAY, MAP, MULTISET, and ROW keys (including nested nulls,
  variable-width values, decimals, and timestamps). `RAW<T>` remains a deliberate host fallback:
  its serializer defines its bytes and hash, but native sources do not carry its serializer contract
  ([decision](../.claude/wontdos/22-generic-raw-type-acceleration.md)).
