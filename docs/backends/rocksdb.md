# RocksDB state backend

StreamFusion's persistent backend follows Flink's RocksDB lifecycle while keeping native operator
state in a RocksDB instance owned and accessed directly by Rust. Java coordinates Flink checkpoint
handles and file upload, but native state reads, writes, flushes, and compactions do not cross JNI.
Host-side fallback state and timers use Flink's `EmbeddedRocksDBStateBackend` delegate.

Select the backend with the normal Flink setting:

```properties
state.backend.type=tech.streamfusion.state.RocksDBNativeStateBackendFactory
```

The backend translates Flink's `state.backend.rocksdb.*` options into the Rust RocksDB instance,
including local directories, incremental checkpoints, compaction style, level and target-file
sizes, write-buffer settings, compression, log level, and TTL compaction query cadence. Incremental
checkpoints reuse immutable SST handles; full checkpoints upload the complete live file set.

## Write path and memory

The write path is Flink's: state writes go through to the RocksDB memtable (WAL off), amortized to
one write per touched key per batch, and RocksDB's background threads own all memtable flushing and
compaction. There is no StreamFusion-side write buffer above RocksDB and no forced flush — the
checkpoint barrier commits the current batch's residue and takes RocksDB's native hard-link
checkpoint, which flushes live memtables itself. Typed-store values are compact arrow-row bytes; a
state-TTL value carries its last-write timestamp as a fixed 8-byte prefix, so the TTL compaction
filter reads one integer per entry.

Native store memory follows Flink's RocksDB memory control (`state.backend.rocksdb.memory.*`): one
shared block cache and write-buffer manager per slot — sized by `memory.fixed-per-slot` if set,
else the slot's managed-memory share (`memory.managed`, on by default), else `memory.fixed-per-tm`
at TaskManager scope — with memtables charged against the cache under Flink's
`memory.write-buffer-ratio` split. With none of these configured, each store falls back to its own
per-instance write buffers and block cache from the translated options.

Aligned same-range recovery opens a local copy of the checkpoint database. Rescaling reads only the
key groups assigned to the recovering subtask and writes a new local database. This preserves
Flink's max-parallelism and key-group redistribution semantics.

Stateful native operators use this backend in tests and production. Operators with a typed direct
state codec access RocksDB per key; other native operators persist their existing key-group snapshot
payloads in the same Rust-owned RocksDB lifecycle.

## Compatibility boundary

The compatibility target is Flink's SQL state and recovery semantics, not a byte-for-byte clone of
`EmbeddedRocksDBStateBackend` internals. Both backends disable the RocksDB WAL, drain the configured
write batch at the checkpoint barrier, create the immutable local snapshot with RocksDB's native
`Checkpoint` API, upload its files asynchronously, and reuse completed-checkpoint SST handles when
incremental checkpointing is enabled.

There are also deliberate implementation differences:

- the group aggregate, changelog normalize, keep-last deduplicate, updating join, and the three
  Top-N rankers perform per-key RocksDB reads and writes (the join's two sides share one database,
  keyed by a table prefix, under one checkpoint); the remaining native operators — the
  watermark-bounded windowed set — replace one key-group snapshot payload in RocksDB at each
  checkpoint;
- the native stores' shared cache and write-buffer manager live in StreamFusion's own RocksDB
  library, sized by the same Flink options and formulas but leased separately from the delegate
  backend's pool (C++ objects cannot cross the two RocksDB libraries), and the binding exposes no
  high-priority cache pool (`memory.high-priority-pool-ratio` is not applied);
- RocksDB options factories are unsupported; and
- Flink's local-recovery snapshot and restore-tuning options are not implemented for native state.

The normal RocksDB compaction, compression, write-buffer, block-cache, logging, TTL-filter cadence,
local-directory, and incremental-checkpoint settings listed above are translated. The direct SQL
and operator recovery suites cover result, checkpoint, incremental reuse, restore, and TTL
semantics; they do not imply identical local I/O behavior for the differences above.

## Savepoints and backend changes

Native-format savepoints retain the incremental RocksDB/SST representation. Canonical savepoints
materialize the backend-independent [StreamFusion canonical state format](canonical-state.md) and
use Flink's full-snapshot writer, producing `KeyGroupsSavepointStateHandle` state. They can restore
onto either this backend or the memory backend. The direct group-aggregate store scans and decodes
its logical rows for this operation, so canonical savepoints are intentionally full and more
expensive than ordinary incremental checkpoints.
