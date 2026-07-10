# The columnar exchange follows Flink's key-group assignment

**Kind:** parity guarantee — how the keyed shuffle partitions rows.
**Matches:** Flink's `KeyGroupRangeAssignment`: `BinaryRowData.hashCode()`, then
`MathUtils.murmurHash`, then `keyGroup * parallelism / maxParallelism`.

## The decision

The columnar exchange projects the logical key columns into Flink's `BinaryRowData` layout in Rust.
For every Arrow row it calculates the same BinaryRow hash Flink would calculate for the projected
`RowData` key, mixes that hash with `MathUtils.murmurHash`, assigns the result to a key group, and
maps the key group to the downstream channel. `TIMESTAMP` precision is supplied by the planner,
because Arrow's timestamp type does not retain it.

This replaces the former `DefaultHasher` over Arrow row-encoding bytes. That internal hash was only
safe while every native consumer stored one opaque operator-state snapshot. Native `GROUP BY` now
writes one raw keyed-state payload per Flink key group, so the exchange and checkpoint layout must
agree exactly for rescaling to be correct.

## Scope / consequences

- Equal join keys on both inputs receive the same BinaryRow/key-group assignment, including NULL
  keys and the supported scalar key types.
- The exchange still keeps the data plane columnar: it gathers homogeneous Arrow sub-batches and
  ships them through `ArrowBatchSerializer`; only the destination calculation changed.
- The `GROUP BY` raw keyed-state path restores every key-group payload Flink assigns to a rescaled
  subtask and merges them back into one Rust hot-state map. Its 1→2 harness test exercises this
  redistribution.
- The recursive BinaryRow writer covers ARRAY, MAP, MULTISET, and ROW keys (including nested nulls,
  variable-width values, decimals, and timestamps). `RAW<T>` remains a deliberate host fallback:
  its serializer defines its bytes and hash, but native sources do not carry its serializer contract
  ([decision](../.claude/wontdos/22-generic-raw-type-acceleration.md)).
