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

- paimon-rust has **no LSM compaction or snapshot expiry** yet, so table maintenance is ours to
  arrange. Preferred: the optional `streamfusion-paimon-compactor` module hands the whole
  operation to **stock Java Paimon** at each barrier (its own picks, its sequence-preserving
  rewriter, its exact deletion handling) — cross-implementation round trips (Rust writes → Java
  reads and compacts → Rust restores and continues) are pinned by the module's tests against
  released Paimon 1.4.1. Fallback (no Paimon on the classpath, or a state file format the
  deployed Paimon can't read): a faithful port of Java's `UniversalCompaction` pick strategy
  executed natively as copy-on-write commits, with two constraints Java doesn't need because its
  rewriter preserves per-record sequence numbers and ours assigns fresh ones — picks are taken
  only as newest-prefixes of the run list (sound exactly because every excluded run is older),
  and a partial pick that would carry deletion rows escalates to a full merge. Local files
  unreachable from the latest snapshot are unlinked after each checkpoint (uploads read from
  per-checkpoint hard-link directories, so GC and uploads never race). Upstreaming real
  compaction (and a sequence-preserving rewrite) to paimon-rust retires both halves.
- Vortex state files are **not readable by released Java Paimon** — the Java Vortex format
  (reader and writer over the native vortex library) exists on Paimon master, targeted at 2.0.
  Until then the Java compactor declines vortex tables; deployments that want Java-owned
  maintenance (or Java-readable state tables for inspection) run `parquet` state files. Values
  stay Rust-defined either way.
- Canonical savepoints cannot be expressed; native-format savepoints work.
- Multiset-state aggregates (retracting MIN/MAX, DISTINCT) and non-scalar state shapes stay on
  memory state until the row codec grows side tables (see `docs/coverage-and-fallbacks.md` §c).

The full design record, including the verified paimon-rust API survey and the rejected
alternatives (rust-rocksdb baseline, Tonbo, fjall, SlateDB, ForSt), is in
`.claude/research/paimon-vortex-state-backend-plan.md`.
