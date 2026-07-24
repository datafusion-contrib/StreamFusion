# Persistent state: local Paimon tables, not RocksDB and not Arroyo's model

**Kind:** architectural — where durable operator state lives and how it checkpoints.
**Diverges from:** Arroyo (and the obvious RocksDB-via-Rust alternative).
**Forced by parity:** partly — Flink's incremental-checkpoint contract shapes the design; the
storage engine choice is ours.

## Their decision

Arroyo keeps operator state in memory and checkpoints it to object storage through its own
controller; there is no per-operator embedded KV store and no Flink-style shared-state registry.
DataFusion Comet is stateless (batch), so it has no position here. Flink's own persistent backend
is RocksDB behind JNI: rowwise, byte-serialized keys *and* values, per-entry
serialize/deserialize on every access, incremental checkpoints via immutable SST files registered
with the `SharedStateRegistry`.

## What we do instead

Native operator state moves into a **local Apache Paimon primary-key table** (via paimon-rust,
Vortex file format) behind a storage seam in the Rust operators — selected with Flink's normal
`state.backend.type` toggle, memory remaining the default. Reads are **read-through, mini-batched
sort-merge probes** (no resident authoritative map, no cache in front); writes buffer as dirty
working-set entries and commit as one typed Arrow batch per checkpoint barrier. Durability lands
exactly at checkpoints — between barriers the write buffer is RAM, playing the role RocksDB's
memtable+WAL play, except the "WAL" is the checkpoint itself.

Why Paimon over rust-rocksdb:

- **No per-entry serialization tax.** State rows are typed Arrow columns end to end — the write
  path is `write_arrow_batch`, the read path streams Arrow — where any KV engine forces
  encode/decode per entry per access.
- **Incremental checkpoints are structural.** A Paimon snapshot is a manifest-pinned set of
  immutable, uniquely named files; "new since the last checkpoint" is a manifest diff. The Java
  side mirrors `RocksIncrementalSnapshotStrategy`'s bookkeeping (confirmed-base placeholders,
  notification-delay pruning, sharing-strategy switch) over Paimon files and emits ordinary
  `IncrementalRemoteKeyedStateHandle`s, so the JM-side registry contract is Flink's own.
- **Bucket = Flink key group, spec-compliant.** The table carries a computed key-group INT column
  as leading primary-key column and bucket key under Paimon's `mod` bucket function with
  `bucket = maxParallelism` — floor-mod of an in-range int is the identity. Rescale is therefore
  file reassignment: restore adopts bucket directories from any number of checkpoint file sets by
  hard-linking data files and committing their existing metadata (public `CommitMessage`), no row
  rewrites.
- **The same tables on object-store FileIO later** are the disaggregated backend with no redesign.

## Costs and edges we accept

- paimon-rust has **no LSM compaction or snapshot expiry** yet. The store bounds read
  amplification itself: a bucket over a live-file trigger is rewritten into one merged file as a
  copy-on-write commit before the checkpoint's data commit, and local files unreachable from the
  latest snapshot are unlinked after each checkpoint (uploads read from per-checkpoint hard-link
  directories, so GC and uploads never race). Real leveled compaction is upstream work.
- Vortex state files are **not readable by Java Paimon** (no Vortex format there), and values are
  Rust-defined — Java moves opaque files, as it did raw keyed blobs.
- Canonical savepoints cannot be expressed; native-format savepoints work.
- Multiset-state aggregates (retracting MIN/MAX, DISTINCT) and non-scalar state shapes stay on
  memory state until the row codec grows side tables (see `docs/coverage-and-fallbacks.md` §c).

The full design record, including the verified paimon-rust API survey and the rejected
alternatives (rust-rocksdb baseline, Tonbo, fjall, SlateDB, ForSt), is in
`.claude/research/paimon-vortex-state-backend-plan.md`.
