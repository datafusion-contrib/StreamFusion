# Delta Lake

The optional `streamfusion-delta` module accelerates data-file writes for the published Delta 4.4
Flink connector. Delta Kernel and the connector still own table creation and discovery, logical-to-
physical schema transforms, optimistic commits, transaction-log actions, primary-key lookup, and
merge-on-read deletion vectors. StreamFusion replaces only the path table's Parquet data-file
writer and the batch-preserving handoff into it.

Both unpartitioned and partitioned path tables are supported in `append` and `upsert` write modes.
This includes direct paths on every Hadoop filesystem supported by the Delta connector (local,
HDFS, S3A, ABFS, and GCS). Catalog-managed tables currently stay on the stock connector path because
the published connector API does not expose a supported engine replacement for them. The released
Delta connector retains its normal table and commit-coordination behavior; StreamFusion decorates
the published path-table API rather than carrying a fork or vendored Delta implementation.
SQL path tables accept connector-specific `fs.*` options such as S3A endpoints, credentials, and
path-style access, or can use the normal ambient/core-site configuration.

For example, an S3 path table can be declared directly in SQL:

```sql
CREATE TABLE delta_sink (
  id BIGINT,
  payload ROW<name STRING, scores ARRAY<INT>>,
  dt STRING
)
WITH (
  'connector' = 'delta',
  'table_path' = 's3a://my-bucket/events',
  'partitions' = 'dt',
  'file_rolling.strategy' = 'count',
  'file_rolling.count' = '-1',
  'fs.s3a.endpoint' = 'https://s3.us-east-1.amazonaws.com',
  'fs.s3a.path.style.access' = 'false'
);
```

AWS workload identity, instance roles, or Hadoop `core-site.xml` remain the preferred credential
sources. For S3-compatible stores, the usual `fs.s3a.access.key`, `fs.s3a.secret.key`, endpoint,
SSL, credentials-provider, and path-style settings can instead be supplied as table options.

In the partitioned path, Arrow batches are split by partition and exchanged between tasks while
they are still Arrow. After the exchange, one ownership-carrying batch record enters StreamFusion's
Delta writer instead of one Flink `StreamRecord` and Java wrapper per row. Java uses lightweight row
views only for Delta's RowKind, partition-value, and primary-key bookkeeping. It records the row
positions that survive the merge and hands the original Arrow buffers plus those positions to Rust;
the data-file payload is never transposed row-by-row. Dense selections pass through unchanged, while
sparse selections gather each Arrow column once immediately before the standard parquet-rs
`ArrowWriter` encodes it. Ignored update-before and key-only delete records never reach a data file.

The Arrow schema crosses the C Data Interface once when each data-file encoder opens; subsequent
batches export only their arrays. Java opens and owns the Hadoop output stream, while encoded bytes
return through the same bounded one-MiB bridge used by the plain Parquet sink. After Rust finalizes
the standard Arrow writer and its footer, Java flushes and closes the stream. Delta Kernel's footer
reader then derives the typed row count, minimum, maximum, and null-count statistics, and Kernel
creates and commits the resulting data-file actions. StreamFusion does not implement Delta log,
statistics, or commit semantics independently.

The native path is whitelist-first. It accepts Boolean, integer, floating-point, decimal, string,
binary, date, timestamp, `ROW`, `ARRAY`, and `MAP` columns recursively. Schema evolution,
`INSERT OVERWRITE`, intervals, and types the Delta connector itself cannot write stay on the stock
connector path. Delta Lake data files remain Parquet: the transaction log and deletion-vector
sidecars are protocol files, not alternative table data formats.

Count- and size-based file rolling remain on the native path. The connector default is size rolling
at 100 MiB. Count rolling uses exact row boundaries. Size rolling uses parquet-rs' encoded-byte
estimate and checks it every 1,024 rows, so a file can exceed the configured size by one check
interval. Negative limits disable the selected strategy. Rolling opens a new Java-owned Hadoop
stream and a new standard Arrow writer; it does not move table or commit ownership into Rust.

The native data-file writer honors Delta's `delta.parquet.compression.codec` **table property** and
the standard Hadoop `parquet.compression`, block/page/dictionary sizes, dictionary setting, and
writer version. Delta table properties must be established through Delta's table API or existing
table metadata; the published SQL connector does not accept arbitrary `delta.*` table properties as
connector options. Unsupported codecs or writer behavior (validation, custom padding,
multithreaded Zstandard, disabled Zstandard buffer pooling, or a custom Delta Kernel target file
size) delegates that data-file write to Delta Kernel's stock Parquet handler.

Build with the `delta` Maven profile and deploy `streamfusion-delta`, `streamfusion-parquet`, and
published `io.delta:delta-flink_2.2:4.4.0` together. The module has no snapshot, local-Maven, path, or
forked Delta dependency.

On the 2M-event, four-partition Kafka JSON Nexmark sink diagnostic (memory state, mini-batching off,
one warmup, best of three), all 23 queries supported by Flink completed and StreamFusion's suite
geomean was **1.522×** the stock published-Delta path. Updating queries used Delta 4.4 merge-on-read
upserts; naturally append-only queries used append mode. See
[Benchmarks](../benchmarks.md#parquet-and-delta-sink-diagnostics) for the exact method and
reproduction commands.
