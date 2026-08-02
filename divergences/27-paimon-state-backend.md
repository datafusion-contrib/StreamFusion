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
`state.backend.type` toggle, memory remaining the default. The store holds exactly **two
components: a write buffer and the disk table**. Reads resolve per input batch with one
point-read join: the batch's keys not already in the write buffer are pushed into the table
reader as an exact `IN` predicate (file/page stats prune, then a single hash-set pass filters
rows at parquet decode — a fork patch, upstreamable, replaced the reader's per-literal `IN` loop
that would have made this quadratic), and the matched rows live only until the end of the
batch's bundle. There is **no retained cache of clean rows between bundles**: re-reads are
served by the OS page cache plus decode, never by a second in-memory copy of committed state.
The split list is planned once per pinned snapshot (the table is immutable between barriers), so
per-batch probes pay no manifest walk. Two earlier read designs were tried and rejected: the
original key-probe-per-batch on the bucket-per-key-group layout profiled as a file-open storm
(evidence that turned out to be about the layout, not the probe granularity), and the
interval-resident working set that replaced it duplicated committed state in memory and tied the
memory bound to touched-state size rather than written-state size. Writes buffer as dirty
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
- **A small fixed bucket count, clipped at recovery — the RocksDB shape.** The table carries a
  computed key-group INT column as leading primary-key column and bucket key under Paimon's `mod`
  bucket function, but the bucket count is deliberately small and decoupled from max parallelism
  (`streamfusion.state.paimon.buckets`, default 1: one LSM per subtask). The original design set
  `bucket = maxParallelism` so bucket id equaled key group and rescale was free file reassignment
  — but that wrote one file per touched key group per commit, fragmentation proportional to max
  parallelism, judged too much steady-state overhead for a property rescale rarely uses (Flink
  itself never physically partitions RocksDB by key group; the group is a key prefix in one CF,
  and rescale clips). Key-group locality survives de-bucketing because `kg` leads the primary
  key: files' row groups are kg-clustered, so the per-batch key probe pushes the keys' groups as
  a stats-prunable companion predicate and reads stay proportional to touched groups. Restore has two paths: a single source covering exactly
  this subtask's range (and the same bucket count) adopts every bucket wholesale — data files
  hard-linked, committed by existing metadata (public `CommitMessage`), no row read — while
  rescale (or a bucket-count change) pays a one-time clip at recovery: each source is scanned
  under a key-group-range predicate and the surviving rows are rewritten into the fresh table in
  one commit, RocksDB's restore-time clip in Paimon terms.
- **The same tables on object-store FileIO later** are the disaggregated backend with no redesign.

## Costs and edges we accept

- paimon-rust has **no LSM compaction or snapshot expiry** yet, and we deliberately carry **no
  native compaction of our own**: table maintenance belongs exclusively to the optional
  `streamfusion-paimon-compactor` module, which hands the whole operation to **stock Java
  Paimon** (its own picks, its sequence-preserving rewriter, its exact deletion handling).
  Maintenance splits in two, both serialized on one per-backend mutex (Paimon supports exactly
  one compactor per table at a time): the **minimal round runs synchronously inside the
  barrier** — deletion-vector reads skip level 0, so up-leveling the barrier's runs (with the
  vectors maintained through Paimon's lookup index) is correctness-critical, Paimon's own
  `lookup-wait` model — with the universal triggers disabled so it never grows beyond the
  delta; the **discretionary shaping merges run on a background thread** kicked after each
  barrier, the RocksDB model, safe to lag arbitrarily because every level-1+ file with its
  vectors reads correct standalone. The local GC only deletes files it previously listed as
  live, so an in-flight shaping round can lose an input to GC and retry, never corrupt.
  Deletion vectors themselves are capability-gated: legal binary primary keys crash the sorted
  lookup store's comparator on current Paimon releases (fix contributed upstream), so the
  compactor probes the deployed Paimon's comparator with the state tables' exact key shape and
  the backend falls back to merge-read tables when it fails.
  Cross-implementation round trips (Rust writes → Java reads and compacts → Rust
  restores and continues) are pinned by the module's tests against released Paimon. Without the
  module, tables stay correct but accumulate one sorted run per touched bucket per checkpoint
  (warned, not failed) — one maintenance implementation, zero drift, was judged worth that
  degradation. (A native port of Java's `UniversalCompaction` picks was built and then removed
  by that decision — commit b555abf holds it if the trade ever reverses; upstreaming real
  compaction to paimon-rust is the durable fix.) Local files unreachable from the latest
  snapshot are unlinked after each checkpoint (uploads read from per-checkpoint hard-link
  directories, so GC and uploads never race).
  *(2026-07-29: the merge-read fallback deployment mode — both the capability-probe fallback
  and the run-without-the-module degradation above — was removed once a Paimon bundle carrying
  the comparator fix, apache/paimon#8873, became publishable as the default. Deletion vectors
  are now unconditional; a missing or incapable compactor fails the backend closed at creation
  instead of degrading, and restoring a table without the option is refused outright — no
  merge-read state table was ever produced in production. The capability probe survives as a
  validation. Merge reads remain only inside the Rust unit suite, which cannot run a Java
  compactor against its own commits.)*
- Vortex state files are **not readable by released Java Paimon** — the Java Vortex format
  (reader and writer over the native vortex library) exists on Paimon master, targeted at 2.0,
  and is absent from every 1.4.x release. State files therefore default to `parquet` (Java can
  maintain and inspect them today); `vortex` is opt-in and currently unmaintained. Values stay
  Rust-defined either way. *(2026-07-29: with the 2.0-SNAPSHOT bundle as the default, vortex
  state tables are maintainable; on a bundle without the format the backend fails closed at
  creation rather than running unmaintained.)*
- Canonical savepoints cannot be expressed; native-format savepoints work.
- Multiset-state aggregates (retracting MIN/MAX, DISTINCT) stay on memory state until the row
  codec grows side tables (see the [Paimon backend page](https://datafusion-contrib.github.io/StreamFusion/backends/paimon/)).

## State shapes mirror Flink's state primitives

The store grew four shapes, each the analog of a Flink state primitive as RocksDB lays it out:
a **single-value** store (ValueState; PK `[kg, k]`, one typed row per key), a **list** store
(ListState; PK `[kg, k, ord]`, one row per element, a dirty key rewriting its whole list — exactly
RocksDB ListState's whole-value rewrite — with positions preserving order-sensitive semantics like
Top-N tie order), and a **map** store (MapState; PK `[kg, k, r]`, one row per entry, `r` the row's
Flink BinaryRow bytes — a stable wire format, unlike arrow-row). The updating join runs two map
tables (one per side) under one operator backend — the analog of Flink's two named join states as
two column families in one RocksDB — carried by one incremental handle whose meta document stores
an opaque snapshot token the native store packs both snapshot ids into. The map store's flush is
per-entry, like RocksDB MapState's per-entry puts and deletes, but derived rather than tracked:
the operator mutates a key's whole entry map in place, and at the barrier the store diffs it
against the image read from the table when the key was first fetched — only entries that differ
are upserted, only vanished rows are tombstoned, so a hot join key's untouched rows cost nothing
per checkpoint.

The fourth shape serves the watermark-driven operators (first consumer: rowtime keep-first
dedup): a **time-buffered** store whose write buffer is not a decoded map but arrival-ordered
Arrow batches with a per-batch liveness bitmap, a key index, and per-batch min/max on the time
column — a queryable set, because watermark firing must answer "every pending row with
`rowtime ≤ watermark`" *including* uncommitted adds and deletes, which per-key slots cannot
express. The firing read is an overlay: the committed table scanned under the time predicate
(stats-pruned, exact at decode), minus rows shadowed by an uncommitted version of the same key (a
DataFusion right-anti hash join against the buffer's touched keys), plus the buffer's own live
rows in range. RocksDB cross-check: Flink serves the same firing from ordered iteration (timers
in a dedicated CF iterated by time); the pruned range scan plays that role — no total order is
needed because a firing collects *all* rows ≤ watermark. Payload moves as Arrow columns end to
end in this shape (input batch → buffer → barrier flush; committed scan → emission), never
through per-cell scalars, and a fired key keeps a marker row on disk so emitted-ness survives
checkpoints where the memory path grows an in-RAM emitted-key set forever.

The second range-read consumer is **event-time window rank** (window Top-N / window dedup), on
the same shape with a composite key: one table row per buffered rank position under
`[kg, key, window_end, window_start, ord]`. Its open windows' buffers stay decoded in memory for
the checkpoint interval — every touch re-ranks them, so they are the write buffer, not a cache —
and stage into the dirty region at the barrier as whole-buffer rewrites (upserts `0..len`,
tombstones for vacated committed positions), the RocksDB `ListState` rewrite shape. A window
first touched in an interval seeds from the committed table *before* the batch's own rows rank
in, preserving the ROW_NUMBER arrival-order tie-break. Firing merges the in-memory buffers with
a committed scan under `window_end ≤ watermark` (positions already fired this interval are
shadowed by the region's staged deletions), then stages `-D` rows for every fired position. Two
deliberate scope edges: the watermark rides the opaque snapshot token (the memory path persists
it in its raw snapshot; without it a restored subtask re-buffers replayed rows of already-fired
windows), and the **proctime** window rank keeps memory state — it closes windows on
processing-time timers whose deadline travels in raw state, not on watermarks.

The third range-read consumer is the **event-time OVER aggregate**, which splits into two tables
under one operator directory because its two states have different shapes: the pending input
rows (time-buffered — one row per buffered input row, keyed by an **arrival sequence** whose
big-endian bytes make byte order arrival order; a firing is the `rowtime ≤ watermark` overlay
merged back into sequence order, and fired rows leave state as `-D` rows) and the per-key
running fold (point-access — one typed row per key holding exactly the running scalars the raw
snapshot round-trips, hydrated by the key probe for just the fired keys and written back as
dirty slots). The arrival sequence rides the opaque snapshot token next to both tables' snapshot
ids, mirroring how the join packs two ids and window rank packs its watermark: without it a
restored subtask's new rows would emit ahead of older pending rows. RocksDB cross-check: Flink's
rowtime OVER keeps the same two states per key — `MapState<Long, List<row>>` of pending rows and
a `ValueState` of accumulators — and its timer sweep is the ordered-CF iteration our pruned range
scan replaces. Two OVER shapes stay on memory state: proctime (eager emission, no watermark) and
bounded ROWS/RANGE frames (per-key row buffers with trailing-edge eviction, a list shape, not a
fixed-width fold).

The OVER aggregate's pending side generalized into a reusable **row-buffer table** (whole input
rows keyed by an arrival sequence, a time column as the fire predicate, fired rows leaving state
as `-D`), and the fourth range-read consumer — the **event-time window join** — is simply two of
them, one per side, under one operator directory. Its fire column is the row's window end; both
sides' fired rows come back in arrival order, so the memory path's own join code runs over them
unchanged, and outer-join match state stays transient within one firing (both sides of a window
close together, so the inner join over the closed rows sees every potential match — nothing else
persists). RocksDB cross-check: Flink's window join keeps per-window row lists in keyed MapState
and iterates windows from a timer sweep; the row-buffer table's stats-pruned `window_end ≤
watermark` scan replaces that iteration, with no per-window key needed because a firing drains
every closed window at once.

The fifth range-read consumer is the family of **aligned window aggregates** (tumbling /
hopping / cumulative; single-phase and the global two-phase half), on the window-rank
discipline: one table row per open (key, window) under `[kg, key, window_end, window_start]`,
carrying the typed key columns (emission needs them decoded) and the accumulators' state fields
— the same scalars the raw snapshot round-trips through `state()`/`merge_batch`. The interval's
touched windows stay decoded in operator memory as the write buffer — every row folds into them
— seeded from the committed table on a key's first touch (one probe per key per interval; the
table is immutable between barriers), staged wholesale at the barrier, and then dropped from
memory. A firing hydrates the committed windows it closes (minus region-deleted rows) into the
same decoded map and lets the memory path's own drain emit, so window order and per-window key
sort cannot diverge. The late-data watermark rides the snapshot token. RocksDB cross-check:
Flink's slicing window operator keys accumulators by (key, slice) in RocksDB and iterates
window-end timers from the ordered timer CF; the stats-pruned `window_end ≤ watermark` scan is
that iteration, and the barrier's whole-row rewrite is the memtable flush. Deliberately memory:
proctime windows (timer deadline in raw state) and the local two-phase half (slice-bounded
state that drains at every barrier).

The **session aggregate** (sixth range-read consumer) reuses the window-aggregate discipline
under a session-shaped key: PK `[kg, key, window_start]` with the end a *value* column, because
a session's start is stable under extension (the same row rewrites) while a merge removes
starts. The one genuinely new obligation is merge bookkeeping: the seed scan records which
committed starts it loaded per key, the barrier tombstones the loaded starts no live session
carries (a merge consumed them), and a key whose sessions all fire before the barrier
tombstones its loaded starts at the firing — otherwise a consumed start's stale row would
outlive the key's presence in the decoded map and resurrect on a later probe. RocksDB
cross-check: Flink's merging window assigner keeps a per-key window-mapping state plus
per-window accumulators in RocksDB and rewrites both on merge; the tombstone-on-merge is that
rewrite, expressed as LSM deletes.

The **interval join** (seventh range-read consumer) is the first store whose reads happen on
*push* rather than at a watermark: each side is a keyed row buffer (PK `[kg, equi-key, seq]`,
time and matched-flag columns, typed payload), and an incoming batch probes the opposite side by
its equi keys with overlay semantics — committed rows for the keys minus rows the region
superseded, plus the region's live rows, merged back into arrival order — then runs the memory
path's own tagged join. Eviction is the familiar time range read, staging deletions. The memory
path's transient matched-id sets become a persistent matched *column*, maintained by
read-modify-write through the region (the keep-first fired-marker pattern): a committed row's
first match re-stages its full probe row with the flag set, so an evicted-never-matched outer
row null-pads exactly once across barriers and restores. RocksDB cross-check: Flink's interval
join keeps per-key row lists in MapState iterated per probe and tracks outer matches the same
row-attached way; the equi-key probe is that iteration through the `IN` pushdown.

The **temporal join** (eighth and final consumer) splits naturally: the probe side reuses the
interval join's keyed row buffer (the changelog kind packed as a trailing payload column), and
the versioned build side is the one state shape Paimon models *better* than it models itself in
RAM — `rightState.put(rowTime, row)` last-write-wins per timestamp IS the deduplicate merge
engine, so build-side writes are plain upserts with no read, no merge bookkeeping, and every
`RowKind` kept as a column. One deliberate deviation: version pruning is lazy (a probed key
prunes its stale versions at the firing that probed it; unprobed keys keep old versions on
disk), where the memory path prunes every key at every watermark — cheap over RAM maps,
a full scan over a table. Correctness never depends on pruning, only state size does; the
Java compactor's maintenance keeps the sorted runs bounded either way.

The full design record, including the verified paimon-rust API survey and the rejected
alternatives (rust-rocksdb baseline, Tonbo, fjall, SlateDB, ForSt), is in
`.claude/research/paimon-vortex-state-backend-plan.md`.
