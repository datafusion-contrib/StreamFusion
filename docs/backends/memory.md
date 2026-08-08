# Memory backend

**Status:** Native, default

Every native stateful operator — aggregates, joins, dedup, Top-N, window operators, and the rest —
holds its state as in-process, JVM-heap-resident structures unless `state.backend.type` selects
something else. There is no separate configuration to opt into this backend; it is what StreamFusion
runs on out of the box, and what every operator falls back to when a query condition or build
configuration keeps it off the [RocksDB backend](rocksdb.md).

## Checkpointing model

State is checkpointed as the backend-independent [canonical state format](canonical-state.md): on
each barrier, an operator serializes its live state by key group into versioned, bounded managed-state
chunks. There is no incremental upload and no manifest diffing — every checkpoint is a complete
snapshot of the operator's current state, the same shape regardless of how much changed since the
last barrier. Legacy raw keyed-state snapshots remain readable.

This is the simplest possible durability story, and it is fast for the common case: no on-disk
table, no compaction, no point-read join on the hot path. The tradeoff is checkpoint size and
duration scale with total state size rather than with the delta, which matters once state grows past
what comfortably re-serializes and uploads every barrier.

## Restore

A memory-backend checkpoint or canonical savepoint can restore on either memory or the
[RocksDB backend](rocksdb.md). A RocksDB restore imports the logical partitions into its snapshot
store on the first run; subsequent checkpoints use the native RocksDB lifecycle.

## When to reach for the alternative

The memory backend is the right default for most jobs. Consider the
[RocksDB backend](rocksdb.md) when checkpoint size or duration dominated by large keyed state becomes
a problem — see [Benchmarks](../benchmarks.md) for measured memory-vs-RocksDB throughput across the
Nexmark queries, and [Configuration](../configuration.md) for how backend selection and other runtime
flags are set.
