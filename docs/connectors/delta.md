# Delta Lake

The optional `streamfusion-delta` module accelerates data-file writes for Flink's Delta connector.
Delta Kernel and the connector still own table metadata, optimistic commits, primary-key matching,
and merge-on-read deletion vectors. StreamFusion replaces only the Parquet data-file encoder.

Both unpartitioned and partitioned path tables are supported in `append` and `upsert` write modes.
This includes direct paths on every Hadoop filesystem supported by the Delta connector (local,
HDFS, S3A, ABFS, and GCS). Catalog-managed tables currently stay on the stock connector path because
the published connector API does not expose a supported engine replacement for them. The released
Delta connector retains its normal table and commit-coordination behavior; StreamFusion supplies
the batch writer and replaces only the path table's Kernel data-file writer.
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

In the partitioned path, Arrow batches are split by partition and exchanged between
tasks while they are still Arrow. After the exchange, one ownership-carrying batch record enters
StreamFusion's Delta writer instead of one Flink `StreamRecord` and Java wrapper per row. The writer
inspects changelog keys through one reusable cursor and records row positions for data-file output;
ignored update-before and key-only delete records are never materialized. The data-file writer
consumes those Arrow buffers directly. When merge-on-read selects a non-contiguous set of rows, the
Java side retains
views of the original vectors and passes only the selected row numbers to native code. The native
writer gathers each column once, immediately before encoding, instead of copying every selected
value through Java vectors. Each retained row now implements the Flink getters directly over the
shared column batch rather than allocating a second `ColumnarRowData` delegate.

The Arrow schema crosses the C Data Interface once when each data-file encoder opens; subsequent
batches export only their arrays. After the native encoder closes the Parquet footer, Delta Kernel's
standard footer reader derives the typed row count, minimum, maximum, and null-count statistics that
are published in the transaction log. This keeps Delta's nested-type and statistics semantics while
avoiding a second row representation or a separate statistics implementation.

The native path is whitelist-first. It accepts Boolean, integer, floating-point, decimal, string,
binary, date, timestamp, `ROW`, `ARRAY`, and `MAP` columns recursively. Schema evolution,
`INSERT OVERWRITE`, intervals, and types the Delta connector itself cannot write stay on the stock
connector path. Delta Lake data files remain Parquet: the transaction log and deletion-vector
sidecars are protocol files, not alternative table data formats.

Count- and size-based file rolling remain on the native path. Count rolling uses exact row
boundaries. Size rolling uses parquet-rs' encoded-byte estimate and checks it every 1,024 rows,
matching delta-rs' approximate batching model; a file can therefore exceed the configured size by
one check interval. Negative limits disable the selected strategy. Java still creates and closes
each Hadoop stream, derives file statistics, and hands the resulting files to Delta Kernel for
commit. The native data-file writer honors Delta's `delta.parquet.compression.codec` table
property and the standard Hadoop `parquet.compression`, block/page sizes, dictionary setting, and
writer version. Unsupported codecs or writer behavior (validation, custom padding, multithreaded
Zstandard, disabled Zstandard buffer pooling, or a custom Delta Kernel target file size) delegates
the data-file write to Delta Kernel's stock Parquet handler.

Build with the `delta` Maven profile and deploy `streamfusion-delta`, `streamfusion-parquet`, and
published `io.delta:delta-flink_2.2:4.4.0` together. The module has no snapshot, local-Maven, path, or
forked Delta dependency. Performance numbers measured with the former local connector build were
removed and must be regenerated against this released-only path before they are reported again.
