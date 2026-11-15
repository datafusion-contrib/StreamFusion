# Columnar flow with transposes only at boundaries

**Applies to:** the plan-transition pass between rowwise and columnar operators

Rather than fusing operator subtrees, each operator is tagged rowwise or columnar; columnar
operators flow Arrow batches into one another and a row↔Arrow transpose is inserted only where a
columnar operator meets a rowwise one. The conversion is paid once at the region's edge, never
inside a chain.

Hudi's MOR sink retains each incoming Arrow batch in Java and records per-bucket
row positions in bitsets. Hudi's original bucket function performs key routing
and buffer admission in arrival order, including mid-batch flushes. Shared batch
references keep the original vectors alive across buckets. The log writer passes
selected vectors to Rust's Parquet encoder. This removes a payload gather for
bucket routing, a second admission pass and a separate native append buffer.
The Java page pool still controls flush boundaries, with a serializer hook that
reserves the stock row geometry while skipping payload writes. The Hudi connector
page records complete-job measurements and the current memory constraints. The
million-row read/Calc/upsert pipeline measures 1.58× over stock and 1.38× over the
previous SF pipeline with its stock sink.

The MOR source reuses Hudi's Java file-group merger over Arrow row references.
Terminal projection runs once per output batch, and spilled controls serialize
batch/row locations. A Rust Arrow interleave gathers selected payloads into output batches.
Only generated key-delete metadata needs new Arrow values. This boundary permits
the existing merger to choose winners and operation kinds while retaining
columnar payloads through the source and downstream native operators.
The original Hudi fetcher performs decoding and merging asynchronously while the
mailbox emits bounded batches and records logical-row checkpoint progress. Base
row cursors are reused until their batch positions are recorded, following the
SDK's transient base-row contract. Log records retain stable Arrow row views.
The million-row MOR-to-COW append pipeline measures 1.68× over stock and 1.06× over
previous SF. Small jobs and continuous row-collector pipelines have overlapping
ranges and retain their measured regressions on the connector page.

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
owns storage access, while released paimon-rust 0.4.0 decodes append and primary-key changelog
files into Arrow. Both Paimon adapters use the same FileIO/Arrow bridge. Admitted initial
snapshots use upstream deduplication, first-row or basic partial updates; a columnar selection
removes winning retracts while preserving the stored add kind. The copied loser tree and reducers
are removed. Potential sequence ties and other unverified semantics retain Java and pay one
row-to-Arrow conversion after merging.

The [released-reader comparison](../connectors/paimon.md#released-reader-migration-diagnostic)
retains production file access, JNI, projection, decode and Arrow import. It compares canonical
released Paimon and the previous StreamFusion implementation, including unfavorable cases.
Public reader reuse reduces owned algorithms; it does not make every merge faster. In particular,
upstream partial-update allocation is more expensive than the previous custom gathering loop.
