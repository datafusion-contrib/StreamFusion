# Canonical conversion staging

Flink's RocksDB canonical savepoints pin its existing database and merge serialized state iterators
directly into the backend-independent canonical writer. StreamFusion's live state has different
operator layouts and serializers, so its database records need conversion before that writer can
consume them.

Native RocksDB First-N, arithmetic GROUP BY, typed scalar GROUP BY without DISTINCT views,
eager deduplication, normalization and processing-time unbounded OVER use a pinned MVCC cursor. The cursor owns a database
reference and decoder configuration; asynchronous conversion reads bounded frames without borrowing
the running operator. Flink still owns canonical framing, metadata, key-group offsets and handles.
Java cancellation and native RAII release the cursor, and a backend lease prevents directory or
memory-pool disposal while asynchronous readers remain. This follows Flink's snapshot architecture.

Memory state lacks that native read snapshot. Its value-state adapters convert synchronously at
the barrier into one temporary stock-serialized file indexed by Flink key group. The immutable file
implements the same Flink snapshot-resource interface and is removed on completion, cancellation
or failed capture.

Contracts that require reads while constructing their stock state still use a temporary stock
RocksDB view. Removing that view needs streaming conversion for their map/list/timer layouts.
Import and per-key working sets remain an explicit boundary. These remaining staging paths add
I/O and retain synchronous conversion work; they do not have the MVCC path's barrier behavior.

Benchmarking is deferred by the maintainer for this implementation goal. No conversion throughput
or barrier-latency improvement has been measured.
