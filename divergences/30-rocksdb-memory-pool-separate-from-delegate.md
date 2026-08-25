# Native RocksDB memory pool leased beside the delegate's, not shared with it

## Flink's decision

Flink's RocksDB backend bounds all RocksDB memory in a slot with one shared
`LRUCache` + `WriteBufferManager` pair, allocated through the slot's
`MemoryManager` shared-resource lease and sized from managed memory (or the
`state.backend.rocksdb.memory.fixed-per-slot` / `fixed-per-tm` overrides) with
the `write-buffer-ratio` split formulas.

## What we did instead

The native stores follow the same options, the same precedence, the same split
formulas, and the same slot-scoped lease machinery — but their cache and
write-buffer manager are created inside StreamFusion's own RocksDB library and
leased under a separate resource id. The delegate `EmbeddedRocksDBStateBackend`
(JVM keyed state, timers, host fallback operators) keeps its own frocksdbjni
pool.

## Why

A C++ `Cache`/`WriteBufferManager` cannot be handed across two independently
linked RocksDB libraries; sharing the delegate's Java-owned objects with the
Rust-owned instance is not possible. Sizing our pool from the same budget and
leasing it externally (rather than reserving managed memory a second time)
avoids double-reserving the slot's managed memory, which would fail the task.
The cost is that a slot running both native stores and substantial JVM-delegate
RocksDB state holds two pools of the configured size; in accelerated jobs the
delegate's databases are near-empty, so the second pool stays at its floor.

Two smaller gaps ride along: the rust-rocksdb binding exposes no
high-priority cache pool (`memory.high-priority-pool-ratio` is read by the
delegate only), and `memory.partitioned-index-filters` is not applied to the
native instance.
