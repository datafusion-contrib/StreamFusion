# The Paimon sink enters through Paimon's format SPI and bundle write, not a native table writer

## Arroyo's and paimon-rust's decision

Arroyo's lake sinks own the whole write natively: partitioning, file layout, multipart upload, and
their own two-phase commit. paimon-rust (`paimon` crate 0.3) likewise offers a Rust `TableWrite`
with `write_arrow_batch`, bucket assigners, and `prepare_commit`, i.e. a full native writer that
would let StreamFusion own Paimon data files end to end.

## What we did instead

The Java Paimon connector keeps the table: bucket assignment rules, sequence numbers, rolling,
statistics, manifests, snapshots, commits, compaction, and recovery are all Paimon's released code.
StreamFusion contributes only the two pieces that touch every row:

- **The shuffle.** Rust splits each Arrow batch into one sub-batch per `(partition, bucket)`,
  computing Paimon's partition `BinaryRow` and default bucket hash column-wise with the native
  Flink key encoder (Paimon's `BinaryRow` is Flink's `BinaryRowData` layout and Murmur hash), and a
  `StreamPartitioner` applies Paimon's `ChannelComputer.select` formula per batch.
- **The encoding.** A `FileFormatFactory` registered under Paimon's own `parquet` identifier hands
  each bundle to the existing native Parquet encoder; the writer factory decides per file and lets
  Paimon's stock writer handle any row-fed file (stock compaction rewrites, batch inserts, spill mode).

The entry point is Paimon's public bundle write (`TableWriteImpl.writeBundle` →
`BundleFormatWriter`), reached from a `TableWriteOperator` subclass fed with routed Arrow batches
instead of rows. Released Paimon 2.0.0 walks a bundle row by row before the format writer; the
bundle's rows are zero-copy Arrow views behind Paimon's own Flink row adapter, so Paimon counts
sequence numbers (and, in its spill path, serializes rows) exactly as it would stock rows, while the
format writer recognises the bundle behind the first row and encodes the whole batch once.

## Why

- **Identical results.** Every Paimon-level artefact — manifests, statistics, snapshot commit
  kinds, sequence numbering, compaction decisions, recovery — comes from the released connector, so
  parity with a stock Paimon job holds by construction rather than by re-implementation.
  paimon-rust's `CommitMessage` is not Java-serializer compatible and its compaction is incomplete,
  so a native table writer could not have shared a table with Flink-side compaction jobs.
- **Colocated compaction.** Users run compaction inside the write job; Paimon's in-job
  coordinator/worker topology is inherited unchanged, and its rewrites simply take the row-fed path
  of the same format factory.
- **Smallest surface.** The whole module is Java plus the Parquet encoder that already ships for
  the Parquet and Delta sinks; there is no new native library and no Paimon fork or snapshot.

## Consequences

- **Classpath order matters.** Paimon picks the first factory claiming an identifier, so the module
  ships as `01-streamfusion-paimon.jar` to sort before `paimon-flink-*` in `lib/`, and the planner
  fails closed: it declines the sink with a logged reason when `parquet` does not resolve to our
  factory. An upstream priority on `FileFormatFactory` would remove the ordering rule.
- **Row-fed paths are stock speed.** Compaction rewrites and Paimon's buffer-spill mode (more than
  `write-max-writers-to-spill` writers in one task) encode through Paimon's writer; correctness is
  unaffected, and in spill mode absolute sequence numbers differ from a stock run because Paimon
  reassigns them after re-buffering whole routed batches instead of single rows.
- **Primary-key tables need one more native piece.** Their merge-tree path sorts rows in a buffer
  before writing and has no bundle entry, so the sink writes their level-0 files itself and hands
  them to Paimon's writer for compaction; see [32](32-paimon-pk-l0-through-the-compactor-hook.md).
