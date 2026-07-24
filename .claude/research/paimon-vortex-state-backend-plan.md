# Plan: Paimon/Vortex native state backend (local-first)

*2026-07-23. Design agreed in session; supersedes the rust-rocksdb toggle plan as the primary track.*

**Status (2026-07-23, end of day): SHIPPED for the pilot.** Jordan skipped the Phase 0 go/no-go
bench and green-lit implementation directly. Phases 1–3 landed: the `KeyedStateStore` seam
(`native/src/state/`), `PaimonGroupStore` (read-through, vortex, bucket = key group via the `mod`
bucket function + a computed key-group PK column — **no upstream `BucketFunction` contribution
needed**, better than §Phase 1 predicted), the JNI checkpoint surface, the Java
`PaimonStateBackend`/`PaimonKeyedStateBackend`/`PaimonSnapshotStrategy` emitting
`IncrementalRemoteKeyedStateHandle`s, and the group-aggregate pilot behind `state.backend.type`.
Coverage/limits: `docs/coverage-and-fallbacks.md` §(c); architecture record: `divergences/27`.
Remaining from this doc: multiset side tables (MIN/MAX retract, DISTINCT), other operators,
time-range shape, TTL, object-store FileIO, upstreaming compaction/expiry to paimon-rust, and the
performance measurements (the Phase 0 rocksdb comparison and a Nexmark-style A/B of memory vs
paimon backend) which were deferred, not run.

## Thesis

StreamFusion's native state is memory-only. We add a second state-table implementation backed by
**local Apache Paimon primary-key tables** (via `paimon-rust`) using the **Vortex** file format with
its default BtrBlocks-style compression:

- State access is mini-batched, so lookups are **sort-merge probes** against key-sorted immutable
  runs with manifest/zone-map pruning — **no index by default**. Index-nested-loop only beats merge
  at tiny probe-selectivity, and Paimon's global B-tree index (already in `paimon-rust`, parallel
  shard queries via `global-index.thread-num`) remains an opt-in escape hatch if run-count or K/N
  ever justifies it.
- Writes append **typed Arrow columns** (`TableWrite::write_arrow_batch`), eliminating the per-entry
  serialize/deserialize tax every KV engine (RocksDB included) imposes. Compaction folds state
  semantically via Paimon merge engines (`deduplicate`, `partial-update`, aggregation).
- Checkpoints are incremental by construction: a Paimon snapshot is a manifest-pinned immutable file
  set; "new files since last checkpoint" is a manifest diff.
- Rescale: **bucket = Flink key group**, so rescale is file/directory reassignment — no clipping, no
  row rewrites.
- Local disk now; the same tables on object-store `FileIO` later = the disaggregated backend with no
  redesign. This resolves the recorded tension between resident-RocksDB and the disaggregated
  direction (`.claude/research/flink-arroyo-accelerator-findings.md`, divergence 16): one backend,
  two storage tiers.

Values (and everything past the key-group routing) stay Rust-defined; Java never deserializes state
— it moves opaque files, same as today's raw-keyed Arrow blobs.

## Verified building blocks (all checked in source, 2026-07-23)

`~/data/paimon-rust` (`crates/paimon`, HEAD 2026-07-23):
- PK/MOR machinery: `table/kv_file_writer.rs` (per-bucket writers, Java-matching sequence numbers),
  `table/merge_tree_split_generator.rs`, `table/sort_merge.rs` (Deduplicate / PartialUpdate merge
  functions; aggregation configs in `spec/aggregation.rs`).
- Write path: `WriteBuilder` → `TableWrite::write_arrow_batch(&RecordBatch)` /
  `write_arrow(&[RecordBatch])` → `prepare_commit() -> Vec<CommitMessage>` → `TableCommit` →
  snapshot commit (`table/table_commit.rs`, `table/snapshot_commit.rs`).
- Read path: `ReadBuilder` with `with_filter(Predicate)` + `is_exact_filter_pushdown`,
  `new_scan()`/`new_read()`, plus `new_incremental_scan` (changelog replay).
- **Vortex format reader AND writer**: `src/arrow/format/vortex.rs` (~1.4k lines, exact predicate
  application), behind the `vortex` feature (`vortex = "0.75"`); `vortex-btrblocks` compressor comes
  with it. Note: Java Paimon has **no** Vortex format — state tables won't be Java-readable.
- **DataFusion integration**: `crates/integrations/datafusion` (DataFusion 54) —
  `PaimonTableProvider`, `PaimonCatalogProvider`, filter pushdown, physical plans, SQL context.
- Global B-tree index: `src/btree/` + `table/global_index_scanner.rs` (bounded-parallel shard
  queries) — the opt-in index.
- **Missing** (irrelevant to this plan unless the index path is adopted): Java's
  `LookupLevels`/lookup-SST subsystem has no Rust port.

StreamFusion side (from the state/checkpoint architecture map, this session):
- Operators hold bespoke in-memory maps keyed by `ByteKey`; no backend seam exists yet.
- Checkpoints stream full Arrow-IPC blobs into Flink **raw keyed state**
  (`RawKeyedState.java`, `isUsingCustomRawKeyedState()==true`) — raw state cannot participate in
  `SharedStateRegistry`, so **incremental requires becoming a real keyed state backend** emitting
  `IncrementalRemoteKeyedStateHandle` (reusable class in flink-runtime; contract mapped from
  `RocksIncrementalSnapshotStrategy` in `~/data/flink`).
- `flink_key.rs` already ports Flink's key-group math (`flink_key_group`); `ManagedMemoryBudget.java`
  already reserves managed memory RocksDB-style (sizes the write buffer here).

## Phase 0 — Go/no-go benchmark (scratch crate, no repo churn)

Standalone criterion bench (scratchpad crate; release profile) comparing, over a grid of
probe-batch size K ∈ {1k, 10k}, state size N ∈ {1e5…1e8}, value width (realistic accumulator
schemas), L0 run count (compaction cadence), and miss rate:

1. **Paimon/Vortex local PK table**: `write_arrow_batch` per mini-batch, commit per simulated
   checkpoint; probe = sorted key set as an exact-pushdown predicate through `ReadBuilder` (and once
   through `PaimonTableProvider` to validate the DataFusion route), MOR merge included.
2. **rust-rocksdb baseline**: `WriteBatch` writes; `batched_multi_get_cf(sorted_input=true)` reads;
   per-entry bincode-style value encode/decode included (that tax is real and belongs in the
   baseline's cost).

Measure read path, write path, and end-to-end simulated operator loop (probe → update in Arrow →
append). Also measure run-count sensitivity explicitly — it, not N, is the predicted failure mode of
index-free probing.

**Decision rule:** Paimon within ~1.5× of rocksdb on adversarial probe reads AND ahead on the
write/end-to-end loop → proceed. Otherwise: record numbers, move this doc to `.claude/wontdos/`,
execute the rocksdb plan (whose full design — delegating backend, key-group prefix layout, batched
state-access layer — is in the session record and memory).

## Phase 1 — State-table seam in `native/`

- New module (e.g. `native/src/state/`): a `StateTable` seam with two access shapes:
  - **KV probe**: `probe(sorted keys: RecordBatch) -> RecordBatch` (key cols + typed value cols);
    `append(upserts/retracts: RecordBatch)`.
  - **Time-range**: `append(RecordBatch)`, `scan(range)`, `evict(range)` — the interval/temporal
    join & sorter shape; maps to append-only Paimon tables with time predicates (Arroyo's
    `ExpiringTimeKeyTable` is the reference for this shape).
  Memory implementation = today's structs behind the seam, monomorphized (no `dyn` in the per-row
  loop); existing benchmarks must not regress before any Paimon code lands.
- `PaimonStateTable`: local `FileIO` path under the TM working directory; an Arrow **write-buffer
  memtable** overlaying reads (read-your-writes within compaction lag; evicted at flush — bounded,
  not the authoritative working set); flush at `prepareSnapshotPreBarrier` via
  `write_arrow` + `prepare_commit`.
- Schema: state key fields as typed PK columns; accumulators as typed value columns; merge engine
  per table (`deduplicate` default; aggregation/partial-update where operator semantics allow).
- **Bucket = key group**: Paimon's default bucket is `Math.abs(row.hashCode() % numBuckets)`
  (`DefaultBucketFunction`); Flink's key group is `MathUtils.murmurHash(row.hashCode()) %
  maxParallelism` — the *inner* row hash is byte-identical shared lineage
  (`MemorySegmentUtils.hash` ↔ `BinarySegmentUtils.hash`, both → `MurmurHashUtils.hashBytes`), but
  Flink applies a second murmur remix Paimon skips (verified 2026-07-23). `BucketFunction` is
  pluggable (`bucket-function.type`: DEFAULT/MOD/HIVE), so the plan is a small upstream
  contribution: a `FLINK_KEY_GROUP` bucket-function type (Java enum + class, mirrored in
  paimon-rust's bucket assignment). With `numBuckets = maxParallelism` it reproduces Flink's
  key-group assignment exactly — spec-compliant, no divergence hack.

## Phase 2 — Flink incremental-checkpoint integration (Java)

Same architecture as the rocksdb design, with Paimon files in place of SSTs:
- A delegating `StateBackend` (config-selected; wraps HashMap/RocksDB for JVM fallback operators);
  native operators register their Rust table handles in `open()`.
- Snapshot sync phase (JNI): flush memtable, `prepare_commit` + local snapshot commit; return
  `{snapshot id, new data files, manifest/snapshot files}` as paths.
- Async phase (Java): upload **data files as SHARED** state via `CheckpointStreamFactory`
  (dedup/diffing keyed on file name — Paimon data files are immutable and uniquely named),
  manifest/snapshot files as PRIVATE; emit `IncrementalRemoteKeyedStateHandle`. Uphold the six
  registry invariants mapped from `RocksIncrementalSnapshotStrategy` (placeholders for reused files,
  diff only against confirmed checkpoints, prune bookkeeping on complete/abort, discard shared only
  if never registered).
- Restore: download handle files into a local table dir, open at the pinned snapshot. Local Paimon
  snapshot expiry aligned to checkpoint subsumption notifications.
- Rescale: assign bucket directories by `KeyGroupRange` intersection; no clipping. Multi-handle
  merge = union of bucket dirs (disjoint by construction).

## Phase 3 — Pilot operator: GROUP BY aggregate

Contains both the easy case (scalar SUM/COUNT accumulators) and the hard cases (MIN/MAX retract
multisets, DISTINCT sets → child tables keyed `(group key, value)` relying on PK-sorted order).
Mini-batch emission needs previous values → one probe per mini-batch (already the design's unit).
Gate: parity matrix (memory vs Paimon vs Flink, identical output incl. order), checkpoint/restore/
rescale ITs, a shared-state GC test (subsume → no leaked/prematurely-deleted files), and the Nexmark
row-fed A/B per `nexmark-perf-ab-methodology`.

## Phase 4+ — Rollout

- Migrate operators by state shape: time-shaped buffers (interval/temporal/window joins, sorter) are
  arguably a *more* natural fit than KV (they already store `Vec<RecordBatch>`).
- Idle-state TTL support unlock (today TTL≠0 falls back; Paimon retention/compaction can implement
  it) → update `docs/coverage-and-fallbacks.md` when it ships.
- Object-store `FileIO` = disaggregated tier (config, not redesign). Revisit lookup-SST/global-index
  acceleration only if run-count benchmarks demand it.
- Upstream anything general to `paimon-rust` (probe-path gaps, bucket-function pluggability,
  DataFusion provider fixes) rather than forking.

## Risks

1. **Run-count sensitivity without an index** — compaction cadence is the tuning knob; Phase 0
   measures it explicitly; global index is the escape hatch.
2. **paimon-rust maturity** on the exact path we'd hammer (PK MOR + Vortex at mini-batch cadence).
3. **Custom bucket function** — pluggability already exists; risk reduced to landing a small
   `FLINK_KEY_GROUP` `BucketFunction` upstream (Java + paimon-rust) and keeping the two in sync.
4. **Checkpoint-cadence file churn** (small files per bucket per checkpoint) — local disk absorbs;
   compaction + snapshot expiry bound it; measure in Phase 3.
5. **SharedStateRegistry correctness** — copy the RocksDB strategy's invariants; dedicated GC test.
6. Vortex 0.75 pin / format evolution; Java-unreadable state tables (accepted, documented).
