# Java Paimon source lifecycle with native file decode

The first streaming source retains Java Paimon's ContinuousFileStoreSource enumerator, discovery,
assignment and checkpoint serializers. It replaces the task-side reader and emission type with
Arrow batches, following StreamFusion's Kafka source boundary. Released Paimon 2.0.0's
FileStoreSourceSplitReader, FileStoreSourceReader, MergeFileSplitRead and KeyValueTableRead were
consulted before implementation. Streaming primary-key splits use Java's no-merge file path;
initial snapshot splits may need sorted-run merging.

Arroyo's filesystem source already reads ParquetRecordBatch streams into its collector and keeps
file progress separately. We retain that columnar structure, but leave file enumeration and
logical-row checkpoint positions with Paimon's Flink source contract. Restores can change batch
size, so positions count emitted rows rather than decoded batches. Independent bucket splits may
reorder after restoration; the order of changes within each key is preserved.

Comet's NativeUtil C Data imports informed ownership and failure cleanup. The native synchronous
Parquet reader requests byte ranges through Paimon's seekable FileIO. A reusable Java 64 KiB array
feeds a borrowed direct buffer during the JNI call. No second storage client or credential stack
is introduced. Exported Arrow buffers transfer to the source batch; its existing abandoned-batch
backstop covers records discarded during task failure.

Both Paimon adapters now import canonical crates.io paimon-rust 0.4.0 rather than maintaining
copies of its reader algorithms. Its public `Table::from_resolved_schema`, `DataSplit`,
`ReadBuilder` and `AuditLogRead` APIs accept Java-selected work. A public `FileIOProvider` adapts
Java seekable inputs to OpenDAL range reads; the actual URI remains bound to Java and is never
reparsed into a separate storage client. This retains Java discovery, credentials and checkpoint
ownership. The metadata-only descriptor bridges incompatible Java split serialization versions.
Append bucket -1 is represented as an unused bucket 0 in the Rust descriptor because the public
builder rejects -1; original bucket paths and explicit file paths remain authoritative.

The released crate unconditionally includes its Parquet, ORC, Mosaic and index dependencies even
with default features disabled. Consequently the optional Paimon reader DSO now embeds upstream
formats. This is an explicit exception to independent connector/format native code packaging,
chosen to reuse the supported public reader without a fork or copied internals. Generic format
artifacts remain independent, source admission still requires the selected installed format,
and the writer path is unchanged. No engine/DataFusion or other StreamFusion cdylib is linked
into this DSO. A future upstream format feature split can remove the duplicate decoder footprint.

The source's native file path is restricted to the current schema without deletion-vector
selections. Historical schemas, deletion vectors, historical non-Parquet files and specialized
split representations retain Java's table reader and conversion. This is an explicit hybrid read
contract, not a late failure on an unsupported file. Query filters stay as Flink residuals; no
filter is silently consumed. Advanced source options and abilities decline at planning time.

The connector coverage page describes the exact gates, memory boundary, validation and release
comparisons against stock Paimon and the previous StreamFusion implementation.

Released Rust owns run planning and merging. Java interval partitioning and format footer reads
remain preflight fan-in/memory checks. The public audit reader retains winning snapshot row kinds;
an Arrow selection removes winning retracts and converts only the row-kind sidecar. Ordinary
snapshot reads normalize kinds to INSERT and therefore do not preserve Java's source contract.
Streaming changelog reads use the ordinary public row-kind reader without reconciliation.

Sequence ties retain Java: overlapping file sequence intervals or more rows than distinct
sequence positions cannot enter native merging. ORC CHAR schemas also retain Java because the
released reader pads values differently. Partial updates with `ignore-delete=true` also retain
Java for stored/unknown retracts because initial retract cell handling differs. These gates trade some native coverage for deleting the
custom loser tree and reducers, following the maintainer's simplicity-first scope. Unsupported
partial-update sequence groups and field aggregates retain Java. ResourceContext charges projected
Parquet row groups, not all merge/output memory; the existing encoded-row-group preflight remains.
Recovery replays emitted logical rows and supports Java/native restoration in either direction.
Current exact coverage, limits and measurements are in the connector page. Native compaction can
reuse this boundary later under [#592](https://github.com/datafusion-contrib/StreamFusion/issues/592).

Finite-file completion flushes the split's final bounded watermark before releasing its Flink
source output. Flink 2.2.1 removes a split's periodic generator in `releaseOutputForSplit`; an
Arrow reader can prefetch EOF and release a small file before that generator's next tick. The
three-row snapshot/one-row follow-up regression reproduced a stalled second window while stock
Paimon advanced. The completion flush uses only the maximum timestamp already emitted by that
split, minus the admitted watermark delay. It passes through Flink's existing split multiplexer,
retaining its handling of other active splits and idleness. Active files still emit periodically;
no watermark is inserted inside an Arrow batch. This is an explicit final-progress flush at file
completion, rather than relying on the stock reader's slower iteration and single-batch pool to
leave time for a periodic callback. Shared native window branches and stock Flink produce the same
results across both commits for Parquet and ORC.
