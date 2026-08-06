# Zero-copy local shuffle: ownership transfer instead of IPC bytes

**Applies to:** the columnar exchange at parallelism > 1

This remains an optional single-TaskManager deployment optimization. The `bench` Maven profile
disables it so headline Nexmark results measure Arrow IPC on every shuffle edge, matching the
cross-TaskManager serialization path.

Flink serializes every record crossing a network edge, even when producer and consumer subtasks
share a JVM — so at parallelism > 1 the columnar exchange used to pay a full Arrow IPC round trip
per batch (schema re-encoded every batch, buffers copied out and rebuilt on the far side).

## How it works

Because the split operator emits destination-homogeneous sub-batches and the partitioner routes
each to exactly one channel, a batch has exactly one consumer, and a same-process edge can move it
by ownership transfer: the serializer parks the batch in a process-global handle table and writes a
28-byte token-guarded handle; the deserializer claims it back, buffers untouched.

## When it's planned

Planned per edge only when it is provably sound:

- Local/MiniCluster execution, or `streamfusion.exchange.zeroCopyLocal` vouching for a single-TaskManager
  deployment.
- Aligned checkpoints — unaligned checkpoints persist in-flight records whose handles would be dead
  on restore, so zero-copy is not planned across an edge that could carry unaligned checkpoint
  state.

Any handle that still escapes its process fails loudly on a JVM token check instead of dereferencing
foreign memory. Cross-process edges keep the IPC format.

## Measurement

Measured on the exactly-once Kafka pipeline at parallelism 4 (2M events, interleaved on/off legs in
one session): q4, whose per-auction aggregate shuffles the full bid stream, runs **~11% faster**
with mini-batch off (0.82 → 0.91 M events/s mean, the legs' ranges disjoint); with mini-batch on the
effect is within noise, exactly as expected — two-phase aggregation pre-shrinks the shuffle to near
nothing. q0 (no exchange) is unchanged.

### The first A/B was flawed

The first A/B of this change measured +23% — on two identical bytes-path runs: the planner's wrapper
environment defeated the original local-execution check, so zero-copy never engaged, and
cross-session noise supplied the "win". The corrected result above (~11% on q4) is what actually
holds once the gate genuinely engages.

The parallelism parity test now asserts engagement (the handle table must fill and drain) so the
gate cannot silently regress again.
