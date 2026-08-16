# 22 — The Parquet sink is encoding-only native; Flink owns the IO and the commit

## What Arroyo does

Arroyo's filesystem sink owns the entire pipeline: its Rust operator partitions batches, encodes
Parquet, uploads multipart parts through `object_store`, checkpoints the upload IDs and part ETags,
and completes the multipart upload in its two-phase-commit hook — the recovery protocol, credential
resolution, and per-scheme quirks (R2 equal part sizes, local temp-file rename) all live natively.

## What we do instead

Only the byte encoding is native. The Rust side uses parquet-rs' lower-level column writers and
appends one completed column chunk at a time through a bounded one-MiB JNI bridge, plus the batch
partition-splitter (Arroyo's `partitioning.rs` shape). It does not stage a complete compressed row
group again in the output bridge before Flink can consume the bytes. Everything else is Flink's own
machinery, reused verbatim:
`StreamingFileWriter`/`Buckets` for rolling and the pending-file exactly-once commit,
`RecoverableWriter` streams over Flink's FileSystem plugins for the actual IO, and
`PartitionCommitter` for partition commit and `_SUCCESS` files. The bridge crosses JNI with one
copy into a reusable array; partition paths are named by Flink's own
`RowDataPartitionComputer` reading each single-key group's first row.

## Why deviate

This project's mandate is 1:1 fidelity with **Flink**, not with Arroyo, and the sink is where that
distinction bites:

- **Every Flink filesystem works day one** — `s3:`, `gs:`, `abfs:`, `hdfs:`, `oss:`, `file:` —
  with every flink-conf credential key untouched, because Flink itself consumes them. The Arroyo
  shape would mean reimplementing credential translation per scheme and would still cover fewer
  filesystems (no HDFS in `object_store`).
- **Exactly-once comes from Flink's own battle-tested protocol** (rename on local/HDFS,
  complete-multipart-upload on S3) instead of a reimplemented upload-ID state machine checkpointed
  across the JNI boundary.
- **Full partition support is inherited**, including partition-commit triggers, watermark-driven
  partition-time commit, `_SUCCESS` files, and custom commit policies — a subsystem Arroyo simply
  does not have and we would otherwise have to rebuild and keep in lockstep with Flink.

The cost is one memcpy of already-compressed bytes per bridge chunk (the floor for Flink's
`byte[]`-only stream API; the host's own writer pays equivalent internal copies), and local disk
staging on S3 (Flink's recoverable writer stages parts on disk — Arroyo holds full parts in RAM
instead). A from-first-principles native IO writer (object_store multipart, no host stream in the
path) remains the theoretical optimum and is tracked as a follow-up issue.

## Output parity, not byte identity

The written files are row-identical and schema-identical to the host's (forced Flink-shaped
descriptor: minimal fixed-width decimals at every precision, INT_8/INT_16 annotations, TIME_MILLIS,
INT64 timestamps flagged unadjusted; parquet-mr-matched effective settings: snappy default,
untruncated chunk statistics, byte-bounded row groups, zstd level 3). Byte-level identity is not
defined for Parquet across writers and is deliberately not chased: `created_by` names the writer,
dictionary pages are labeled `RLE_DICTIONARY`+`PLAIN` where parquet-mr v1 labels the same layout
`PLAIN_DICTIONARY`, arrow-rs writes no optional per-page CRC32 where parquet-mr does, and page/row-
group split points follow different size-estimation heuristics. Readers observe identical data.
