# Backends

Stateful native operators (aggregates, joins, dedup, Top-N, and the rest) hold their state behind a
`state.backend.type`-selected store:

- **[Memory](memory.md)** — the default. Full raw keyed-state blobs, checkpointed whole.
- **[RocksDB](rocksdb.md)** — Flink-compatible persistent state with native operators talking
  directly to a Rust-owned RocksDB instance.

Both backends run the same native operators and produce the same results — the backend only
changes how state durably survives a checkpoint/restore, never query semantics. See each page for
exactly which operators and shapes are covered.
