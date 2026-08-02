# Paimon backend

**Status:** Experimental / opt-in

!!! warning "Not the production default"
    The Paimon backend is experimental and opt-in. The [memory backend](memory.md) remains the
    default and the production-recommended choice. Select Paimon only when you have read this page
    in full, understand its restore-compatibility rules below, and have deployed the required
    compactor module.

## What it is

Selecting `state.backend.type: io.github.jordepic.streamfusion.state.PaimonStateBackendFactory`
moves a supported native operator's state into a **local Apache Paimon primary-key table** instead
of a raw keyed-state blob. Checkpoints become **incremental**: snapshots travel through the keyed
state backend as `IncrementalRemoteKeyedStateHandle`s, so a data file already uploaded by a
completed checkpoint is referenced, not re-uploaded. An aligned restore adopts a table's files
wholesale; a rescale clips each source by key-group range at recovery. JVM-side keyed state
elsewhere in the same job (fallback operators, timers) is unaffected and keeps running on the
wrapped hashmap backend.

State data files default to **uncompressed parquet** — the format is Java-maintainable and
inspectable today. A `vortex` option exists but is opt-in and currently **unmaintained**: it awaits
Paimon 2.0's Java Vortex writer/reader (tracked upstream as
[apache/paimon#7543](https://github.com/apache/paimon/issues/7543)), which is absent from every
released 1.4.x line. Until that ships, do not describe this backend as "using Vortex" — parquet is
the default and only actively maintained format.

## Why Paimon, not RocksDB

Flink's own persistent backend is RocksDB behind JNI: rowwise, byte-serialized keys and values with
a per-entry encode/decode on every access. StreamFusion's native operators already hold state as
typed Arrow columns, so a KV engine would impose a serialization tax that doesn't otherwise exist in
the native path. Paimon avoids it: the write path is `write_arrow_batch`, the read path streams
Arrow, and a Paimon snapshot is a manifest-pinned set of immutable files — "new since the last
checkpoint" is a manifest diff, which is what makes incremental checkpoints structural rather than
bolted on. The Java side mirrors `RocksIncrementalSnapshotStrategy`'s own bookkeeping over Paimon
files, so the checkpoint-coordinator contract is Flink's own.

The state tables use a small, fixed bucket count (`-Dstreamfusion.state.paimon.buckets`, default
`1`) decoupled from max parallelism — one LSM per subtask, the same shape RocksDB itself uses (Flink
never physically partitions RocksDB by key group either; the group is a key prefix in one column
family). An aligned restore adopts every bucket's files wholesale; a rescale or bucket-count change
pays a one-time clip at recovery, scanning each source under a key-group-range predicate and
rewriting survivors into the fresh table.

### Store shape and access pattern

The store holds exactly **two components**: a write buffer and the disk table. Writes land as dirty
working-set entries in the buffer and commit as one typed Arrow batch per checkpoint barrier —
durability lands exactly at checkpoints, with the write buffer playing the role RocksDB's
memtable+WAL play (except the "WAL" is the checkpoint itself).

Reads resolve per input batch with one **point-read join**: the batch's keys not already covered by
the write buffer are pushed into the table reader as an exact `IN` predicate (file/page stats prune,
then a hash-set pass filters rows at parquet decode), and the matched rows live only until the end
of the batch's bundle. There is deliberately **no retained cache of clean rows between bundles** —
re-reads are served by the OS page cache plus decode, never a second in-memory copy of committed
state. Watermark-driven and range-scanning operators (dedup, window rank, `OVER`, window/interval/
temporal joins) instead query the table with a time-bounded range read merged against the write
buffer, rather than a per-key point probe; see
[`divergences/27-paimon-state-backend.md`](https://github.com/datafusion-contrib/StreamFusion/blob/main/divergences/27-paimon-state-backend.md)
for the full per-operator shape of each store variant.

## Operator coverage

Every stateful native operator's **event-time mode** runs on the Paimon backend. What keeps memory
state regardless of backend selection is exactly the proctime modes, bounded `OVER` frames, the
local half of two-phase window aggregates, and the multiset/type gates below — none of these are
query fallbacks; the query still accelerates, the operator just checkpoints the old way, in full.

| Operator / shape | Runs on Paimon backend |
|---|---|
| Non-windowed `GROUP BY` aggregate (single- and two-phase global) | Yes |
| Eager dedup (rowtime/proctime keep-last, proctime keep-first) | Yes |
| Watermark-driven rowtime keep-first dedup | Yes |
| Changelog normalizer | Yes |
| Streaming Top-N — append-only, retracting (one list store), update-fast (row-keyed map) | Yes, all three |
| Updating join (INNER/LEFT/RIGHT/FULL/SEMI/ANTI) | Yes |
| Event-time window rank / window dedup | Yes |
| Event-time `OVER` (unbounded RANGE, `ROW_NUMBER`/`RANK`/`DENSE_RANK`) | Yes |
| Event-time window join (all kinds) | Yes |
| Event-time aligned window aggregate (tumbling/hopping/cumulative — single-phase and the global two-phase half) | Yes |
| Event-time session aggregate | Yes |
| Event-time interval join (all kinds) | Yes |
| Temporal join | Yes |
| Proctime `OVER`, proctime window rank, proctime window join, proctime aligned/session window aggregate, proctime interval join | No — memory state (processing-time timer deadlines travel in raw state) |
| Bounded ROWS/RANGE `OVER` frames | No — memory state (row-buffer with trailing-edge eviction, not a fixed-width fold) |
| Local half of two-phase window aggregates | No — memory state (slice-bounded, drains every barrier) |
| Retracting `MIN`/`MAX`, `COUNT`/`SUM(DISTINCT)` (multiset-state aggregates) | No — memory state (persisted row codec doesn't carry multisets yet) |

An aggregate list containing even one multiset-state aggregate keeps the **whole operator** on
memory state, not just that column. Likewise, any persisted scalar (or, for row-payload operators —
dedup, changelog normalizer, Top-N, join sides — any column of the row type) outside
`boolean`/`tinyint`/`smallint`/`int`/`bigint`/`float`/`double`/`varchar`/`varbinary`/`decimal`/
`date`/`timestamp` (zoneless milli/micro/nanosecond) keeps the operator on memory state.

Two other conditions fall an operator back to memory state even though its shape is otherwise
supported: restoring from a memory-backend checkpoint (there is no silent migration between
backends), and a native build without the `paimon-state` feature (the backend probe reports
unavailable — this is a graceful fallback, never a linkage failure).

Canonical savepoints are rejected outright for Paimon-backed operators
(`UnsupportedOperationException`); native-format savepoints work as usual (uploaded whole, no file
sharing, restorable in either `CLAIM` or `NO_CLAIM` mode).

## Table maintenance (compaction)

**Compaction belongs exclusively to stock Java Paimon** — the native Rust store never compacts
itself. The backend **requires** a maintainer: `streamfusion-paimon-compactor.jar`, plus a Paimon
bundle carrying the binary-key lookup comparator fix
([apache/paimon#8873](https://github.com/apache/paimon/issues/8873)), must sit in Flink's `lib/`.
Without both, backend creation fails closed with a message naming the missing requirement — there
is no maintainer-less deployment mode.

State tables always carry deletion vectors and compact **synchronously at every barrier** (Paimon's
own `lookup-wait` model): between the barrier's data commit and the checkpoint's file listing, Java
Paimon's lookup compaction up-levels the barrier's level-0 run and marks overwritten rows in
deletion-vector index files, so every committed snapshot holds only standalone-correct files and
reads never merge sorted runs. A rescale restore's clip rewrite is compacted the same way before the
first record. A failed maintenance round fails the snapshot outright — reads over an uncompacted run
would silently miss the barrier's rows, since Paimon skips level 0 under deletion vectors.

A useful side effect: parquet state tables are ordinary Paimon tables, readable by any Paimon
tooling for state inspection.

## Restore compatibility

Paimon-backend state follows the same restore rules as every StreamFusion checkpoint, with one
additional constraint specific to backend selection. **A restore works** only when all of the
following hold:

- **Same StreamFusion release** on both sides — same JAR version, same native libraries, same raw
  keyed-state format version.
- **Same plan shape** — same query, same acceleration setting, and the same set of StreamFusion JARs
  in `lib/`, so every operator that was native at snapshot time is native again.
- **Same state backend selection** — a memory-backend snapshot restores on memory state; there is
  **no silent migration** to or from the Paimon backend in either direction.
- Parallelism may change freely within max parallelism — native state rescales through Flink's own
  key-group redistribution (Paimon tables clip by key-group range at recovery).

`--allowNonRestoredState` is **not a safe escape hatch** for any mismatch above. It silently drops
every state handle that finds no matching operator; with a whole native island unmatched — which,
if source operators differ, includes source offsets — the job "restores" and then re-reads from the
connector's default start position or committed group offsets. That is data loss or duplication, not
an upgrade.

### Safe procedures

- **Same-version restore** (infrastructure moves, parallelism changes, config changes that do not
  alter the plan): stop with a savepoint, restore with the identical JARs, native libraries,
  acceleration settings, and backend selection. This is the only path that carries Paimon-backed
  operator state across.
- **Anything that changes the plan shape, the backend selection, or the state format** (toggling
  acceleration, adding/removing StreamFusion modules, switching backends, a coverage-changing or
  state-format-changing upgrade): **drain and start fresh** — `stop --drain` so windows and timers
  fire and downstream results complete, then submit the new configuration as a new job from clean
  source positions. Operator state is deliberately left behind; correctness comes from the drain,
  not from carrying state across incompatible plans.

## Performance

See [Benchmarks](../benchmarks.md) for measured Nexmark throughput comparing the Paimon backend
against stock Flink on RocksDB, and against StreamFusion's own memory backend. See
[Configuration](../configuration.md) for the `-Dstreamfusion.state.paimon.*` flags that control
bucket count and state file format.
