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

- the group aggregate, changelog normalize, keep-last deduplicate, updating join, the three Top-N
  rankers, the event-time window join, and the aligned event-time window aggregates
  (tumble/hop/cumulate, including the global two-phase half) perform per-key RocksDB reads and
  writes (a two-sided operator's tables share one database under table-prefixed keys; window
  buffers append sequence-keyed entries and windowed aggregate state keys by window end, so
  watermarks fire with per-key-group range scans); session aggregates join them with key-major
  session lists that hydrate by prefix scan for merging, and over aggregates across every admitted
  shape — the unbounded folds and ranking window functions keep one fold row per key; bounded
  ROWS/RANGE frames keep their per-key sliding buffers as `[key][rowtime][arrival]`-ordered frame
  rows (a firing prefix-scans exactly the fired keys, recomputes on the resident buffer, then
  writes the appended rows and deletes the evicted ones) with per-key deadline stamp rows;
  DISTINCT aggregates keep insert-only seen-sets as per-element companion rows, point-probed per
  batch; and the proctime shapes run the same layouts eagerly, ordered by a persisted arrival
  counter — event-time window rank, the event-time interval join
  (append-mostly sequence-keyed buffers carrying each row's matched flag), the temporal join
  (versioned build rows in byte-comparable version order), keep-first deduplicate, and the
  temporal sort buffer. Group aggregates with MIN/MAX retraction or DISTINCT keep their per-key
  multisets as companion element tables: a bundle point-reads only the elements its batches name,
  the running count/sum/extreme rides the main row, changes write through as element-level deltas,
  and a retraction that removes the current extreme reseeks the ordered element table (a numeric
  MIN/MAX over an insert-only input needs no multiset at all and runs as a plain running value). Typed
  stores persist an operator's processing-time timer deadline under a reserved key, so proctime
  windows, sessions, rank, and the window and interval joins run direct too. Every native operator
  now reads and writes RocksDB per key; the generic snapshot path remains only as the fallback for
  a shape whose state has no fixed-type native codec (a DISTINCT over a value type with no faithful
  type code), which keeps its memory-resident state and replaces key-group snapshot payloads in
  RocksDB at each checkpoint;
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
onto either this backend or the memory backend. A direct store scans and decodes its logical rows
for this operation, so canonical savepoints are intentionally full and more expensive than ordinary
incremental checkpoints.

Restoring a canonical savepoint (or legacy raw keyed state) onto this backend decodes the blob key
groups once at open and bulk-writes them — rows, TTL stamps, watermarks, sequence high-water marks,
and timer deadlines — through the operator's typed store, so a memory-to-RocksDB transition
continues on the direct per-key path and its next checkpoint is an ordinary incremental RocksDB
handle. A multiset group aggregate's import also spreads each blob's side batches into its
companion element tables, and an over aggregate's import fans its buffer rows into the frames
table and its seen-sets into the distinct element tables. Only the snapshot-path fallback shapes
above restore such state into the generic snapshot store instead.
