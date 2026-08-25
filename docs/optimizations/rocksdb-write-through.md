# RocksDB write-through on Flink's write path

**Applies to:** every native operator running on the RocksDB state backend's typed store (the
group aggregate, changelog normalize, keep-last deduplicate, updating join, the three Top-N
rankers, the event-time window join, and the aligned and session event-time window aggregates
today; each migrated operator inherits it)

Found by issue [#26](https://github.com/datafusion-contrib/StreamFusion/issues/26): at 10M events
the backend spent 80% of CPU in its own memory-pressure flush, and a StreamFusion-only tuning knob
(`write-buffer-mb`) swung the result from 3x slower than Flink to 2x faster.

The original store retained dirty entries in a Java-governed map above RocksDB — a second memtable.
When it hit its threshold it drained the whole map on the task thread, encoded every value as a
standalone Arrow IPC stream, forced a memtable flush into small L0 files, and cleared the read
cache. All of that was overhead management for a buffer RocksDB already has.

The store now follows Flink's write path with the batching advantage kept:

- **Write-through per bundle.** Dirty entries are written to the RocksDB memtable (WAL off) at
  every bundle boundary — one coalesced write per touched key per bundle, where Flink pays one per
  record. RocksDB's background threads own all flushing and compaction; the barrier only commits
  the current bundle's residue, so checkpoint sync time no longer scales with the interval's
  write volume. The working map is a per-bundle read/dedup cache, nothing more.
- **One columnar conversion per bundle.** Values are compact arrow-row bytes: the whole dirty set
  encodes in a single `RowConverter` pass, and `begin_batch` hydrates misses with one `multi_get`
  plus a single batch decode — replacing a per-value Arrow IPC stream (schema framing per value,
  parsed even inside the TTL compaction filter). The TTL timestamp is now a fixed 8-byte value
  prefix, making the compaction filter one integer read per entry.
- **Flink's memory governance.** One shared block cache and write-buffer manager per slot, sized by
  `state.backend.rocksdb.memory.*` with Flink's exact split formulas, replaces per-store 256 MB
  caches. Total native state memory stops scaling with operator count.

The `streamfusion.state.rocksdb.write-buffer-mb` knob and the memory-pressure flush are deleted;
there is no StreamFusion-specific state memory tuning.

Measured (Nexmark state-backend A/B, 200K events, parallelism 2, best of 1, vs Flink RocksDB):

- q4: 1.41s → 1.30s (4.20x → 5.05x); with the old knob at 1 MiB (the pathology at small scale) the
  old code degraded to 1.91s — that failure mode no longer exists.
- q7: 1.33s → 1.21s (2.31x → 2.70x).
