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

- only the group-aggregate state codec currently performs per-key RocksDB reads and writes; the
  remaining native operators replace one key-group snapshot payload in RocksDB at a flush or
  checkpoint;
- StreamFusion accounts native state against Flink task off-heap memory rather than Flink's managed
  RocksDB memory pool;
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
