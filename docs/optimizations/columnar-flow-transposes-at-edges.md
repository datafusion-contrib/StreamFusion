# Columnar flow with transposes only at boundaries

**Applies to:** the plan-transition pass between rowwise and columnar operators

Rather than fusing operator subtrees, each operator is tagged rowwise or columnar; columnar
operators flow Arrow batches into one another and a row↔Arrow transpose is inserted only where a
columnar operator meets a rowwise one. The conversion is paid once at the region's edge, never
inside a chain.

The [Doris adapter](../connectors/doris.md) borrows the released client's shaded Arrow buffers
through retained foreign allocations and emits Arrow IPC into its stock writer/committer.
Its verified snapshot-to-insert pipeline removes both row transposes. The 2026-10-22 release
fixture retains planning, startup, JNI, codecs, network I/O, commit and visibility wait.
At 1M scalar rows the Flight and Thrift median speedups are 1.27× and 1.12× over stock;
Flight with arrays, rows and timestamps reaches 1.48×. The 10M-row Flight case is at parity
(0.99×), while Thrift reaches 1.09×. Full ranges and both stock/previous controls remain on
the connector page. Sink IPC copies remain, and this is not a whole-network zero-copy claim.

This was the change the first end-to-end benchmarks demanded: a lone native operator paid two
conversions per batch and ran below Flink (filter 0.58x, window 0.81x); a fully-columnar Parquet
copy runs 3–5x.

Paimon's primary-key sink retains that columnar boundary through first-row, partial-update and
aggregation merges. Pick-style field reducers keep indices into the input Arrow columns; arithmetic,
boolean operations and string concatenation create new scalar values. A gather per column constructs the merged
batch, including nested values, for the native Parquet writer. Released Java Paimon parses options
and owns compaction and commits without receiving input rows on this path.

The [release writer diagnostic](../connectors/paimon.md#merge-engine-writer-diagnostic) measured
1.20–1.27× throughput against stock ingestion for the four new modes, including routing and the
ingress RowData-to-Arrow conversion. Unsupported merge combinations retain the stock writer.

Local Paimon merging before shuffle reuses that native buffer for deduplicate and first-row tables
with `changelog-producer=none`. It groups by the full table key, gathers surviving Arrow values,
and transfers their vectors to routing without converting through Java rows. Field merges and
changelog-producing tables retain Java's buffer grouping because their intermediate merges are
observable. The [local merge diagnostic](../connectors/paimon.md#local-merge-diagnostic) measured
4.78× deduplication and 2.93× first-row throughput between Arrow boundaries, versus Java local merge
with its input/output conversions. Whole SQL jobs measured 1.20× with Parquet and 1.17× with ORC,
including ingress, shuffle, writing and commit.

Paimon's streaming source applies the same boundary in reverse: Java plans committed files and
owns storage access, while native Parquet decoding emits Arrow directly for append files and
primary-key changelog files. Admitted initial deduplication snapshots also merge in native code,
using paimon-rust's loser tree and Arrow cursors. Other snapshot merges stay in Java and pay one
row-to-Arrow conversion after the merge. The
[source file-reader diagnostic](../connectors/paimon.md#source-file-reader-diagnostic) measured
1.30× append and 2.18× changelog throughput against the stock reader, including the Java byte-range
bridge and Arrow import. These numbers measure local file reads, not whole Flink jobs.

The [snapshot catch-up diagnostic](../connectors/paimon.md#snapshot-catch-up-diagnostic) measured
1.61–2.58× throughput over the Java merge-to-Arrow path with integer keys on one, four and eight
commits, and 1.68–2.08× with decimal, timestamp, binary and date keys on four commits. Both paths
include storage reads, emit Arrow and checksum the same integer payload. Keeping one running winner per key and flushing retained
output references by bytes avoids pinning all input versions during snapshot catch-up.
