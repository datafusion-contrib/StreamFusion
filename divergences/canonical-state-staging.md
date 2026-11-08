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

The other RocksDB contracts also pin native database views, but reuse their existing stock-state
adapters on the asynchronous writer thread. Their savepoint owns an independent temporary stock
RocksDB view for map/list/timer construction. The barrier captures database pins and immutable
reader arguments; opening native decoders, hydrating metadata and converting stock state happen on
the asynchronous writer thread. The reader owns a separate shared-cache lease. The captured native
decoder rejects writes, shares the original database's lifetime, and uses the clock frozen at capture. Native TTL
compaction retains pinned entries until all readers release them.

Memory capture copies mutable native state while sharing immutable buffers and payloads. Window
and session accumulators share versions with copy-on-write detachment. Capture visits the resident
state graph; stock conversion and serialization execute asynchronously. This costs additional
retained memory and does not promise constant-time capture. Unbound private recovery and captured
JVM operator state keep their existing backend-specific snapshot behavior.

Reusing the stock adapters keeps one definition of each released state layout. It retains temporary
disk I/O and operator-specific per-key/inventory working sets. Eliminating that staging would require
additional streaming codecs for map/list/timer layouts; it is separate from moving conversion off
the task thread. The expanded capture mechanisms still require the broader qualification matrix.

Benchmarking is deferred by the maintainer for this implementation goal. No conversion throughput
or barrier-latency improvement has been measured.
