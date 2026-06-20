# Columnar endpoints beyond local Parquet (Iceberg, remote filesystems)

**Status:** open
**Source:** the native Parquet source/sink (ticket 21) are local-only; the matchers fall back on
any non-`file:` scheme.

## Why
The columnar source→sink win (3.19× on a local Parquet copy) is the project's strongest result,
but real pipelines read/write Iceberg tables and remote filesystems (HDFS, S3/object store).
Extending the columnar endpoints there is what makes the win apply to actual workloads.

## Scope
- **Remote filesystems** for the existing Parquet source/sink: `hdfs:`/`s3:` paths. The native
  reader/writer use local `std::fs`; remote needs an object-store/HDFS reader (DataFusion's
  `object_store`, or reuse the cluster's HDFS access). The matchers currently reject non-`file:`
  schemes — that gate is where support plugs in.
- **Iceberg** source/sink: catalog resolution, split planning, schema evolution, and Iceberg's
  metadata/commit protocol. Much larger than a filesystem path. The native side can read/write
  Parquet data files as Arrow; the Iceberg table semantics (snapshots, manifests, commits) are the
  work. Consider `iceberg-rust` on the native side.

## Parity / correctness
- Each endpoint must produce/consume byte-identical data to Flink's equivalent connector, verified
  by the harness (write via both, read both back; or read via both, compare).
- Exactly-once commit semantics must match the connector being replaced (filesystem rolling,
  Iceberg snapshot commit).

## Sequencing
Remote-filesystem Parquet is the smaller step and extends the proven local path; Iceberg is the
bigger, higher-value target. Both come after the core columnar mechanism (shuffle, windows) so the
endpoints feed a complete columnar pipeline.
