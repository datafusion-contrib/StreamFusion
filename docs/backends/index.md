# Backends

Stateful native operators (aggregates, joins, dedup, Top-N, and the rest) hold their state behind a
`state.backend.type`-selected store:

- **[Memory](memory.md)** — the default. Full raw keyed-state blobs, checkpointed whole.
- **[Paimon](paimon.md)** *(experimental)* — a persistent backend built on local Apache Paimon
  primary-key tables, with incremental checkpoints instead of whole-state snapshots.

Both backends run the same native operators and produce the same results — the backend only
changes how state durably survives a checkpoint/restore, never query semantics. See each page for
exactly which operators and shapes are covered.
