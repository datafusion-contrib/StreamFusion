# Delta Lake

The optional `streamfusion-delta` module accelerates data-file writes for Flink's Delta connector.
Delta Kernel and the connector still own table metadata, optimistic commits, primary-key matching,
and merge-on-read deletion vectors. StreamFusion replaces only the Parquet data-file encoder.

Both unpartitioned and partitioned tables are supported in `append` and `upsert` write modes. This
includes direct paths on every Hadoop filesystem supported by the Delta connector (local, HDFS,
S3A, ABFS, and GCS), Unity Catalog path access, and catalog-managed Unity Catalog tables. The
connector builds the table and retains its normal discovery, temporary-credential refresh, and
commit-coordination behavior; StreamFusion decorates only the Kernel engine's data-file writer.
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
  'fs.s3a.endpoint' = 'https://s3.us-east-1.amazonaws.com',
  'fs.s3a.path.style.access' = 'false'
);
```

AWS workload identity, instance roles, or Hadoop `core-site.xml` remain the preferred credential
sources. For S3-compatible stores, the usual `fs.s3a.access.key`, `fs.s3a.secret.key`, endpoint,
SSL, credentials-provider, and path-style settings can instead be supplied as table options.

In the partitioned path, Arrow batches are split by partition and exchanged between
tasks while they are still Arrow. After the exchange, the connector receives lightweight `RowData`
views over the same Arrow buffers for changelog and primary-key bookkeeping; the rows are not
materialized or transposed back into independent objects. The data-file writer consumes those Arrow
buffers directly. When merge-on-read selects a non-contiguous set of rows, the Java side retains
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

Build with the `delta` Maven profile and deploy `streamfusion-delta`, `streamfusion-parquet`, and the
matching Delta Flink connector together. The connector integration used by this module requires the
matching Delta build because it provides the Arrow-batch write hook and decorated-engine support.

For a differential performance check, q0 is the fair baseline: both engines execute the same query
and connector configuration, object reuse is disabled for both, and retained Delta tables are
compared with `EXCEPT ALL` in both directions. On the 2M-event Kafka JSON run with memory state and
mini-batching disabled, the native path took 2.962 seconds versus 4.233 seconds for Flink
(**1.43x**, one warmup and best of two). Caching successful data-directory initialization and
removing the extra per-row delegate reduced the native result to 2.710 seconds versus 4.195 seconds
for Flink (**1.55x**). Both tables contained 1,840,000 rows and both differences
were empty. A smaller matched
CPU profile showed the remaining native-side Java cost concentrated in Delta's shared per-row
primary-key and merge bookkeeping, not schema export, Arrow row views, statistics extraction, or the
native Parquet encoder.
