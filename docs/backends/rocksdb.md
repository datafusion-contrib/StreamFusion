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
