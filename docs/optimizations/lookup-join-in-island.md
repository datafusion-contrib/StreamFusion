# Lookup joins kept in the island

**Applies to:** sync and async lookup joins

A lookup join left on the host drags the probe-side Calc and source back to rowwise with it, since
the plan splits at the join rather than around it — so both lookup variants are implemented to stay
native.

The sync operator keeps probe batches Arrow and calls the connector's real `LookupFunction` once per
row. The point lookup is row-oriented no matter what — the operator around it doesn't need to be.

The async operator fires the connector's `asyncLookup` for each *distinct* key in a batch
concurrently and joins on the task thread: a batch's lookups overlap, duplicate keys are deduped
(safe because the dimension state is fixed within a batch), and all I/O begins and ends inside
`processElement`, so nothing is in flight across a checkpoint. This is the within-batch model Arroyo
and RisingWave use, and it avoids Flink's own `AsyncWaitOperator` mailbox/replay machinery entirely
— there is no cross-checkpoint replay queue to manage because no lookup outlives the batch that
issued it.
