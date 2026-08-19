# Parquet

**Status:** experimental. Unlike [Kafka](kafka.md), which is production-grade and documented in
depth, the Parquet native sink has not seen the same hardening — expect rough edges and treat
fallback to stock Flink as the normal, safe outcome.

## Source

Parquet reads always use Flink's stock source. The incomplete native Parquet reader was removed;
the optional StreamFusion Parquet module now accelerates only sink encoding.

## Sink

The sink accepts **any filesystem Flink has a plugin for** (`file:`/`s3:`/`gs:`/`abfs:`/`hdfs:`/…).
The native side only encodes Parquet bytes; Flink's own recoverable output streams do the I/O, so
filesystem plugins, credentials, exactly-once commit, and partition commit all remain Flink's own
code. Completed row groups are appended one column chunk at a time through a bounded one-MiB bridge,
letting Flink form object-store upload parts without retaining a second complete compressed row
group in the native output bridge. The parquet-rs column writers still hold the current row group's
encoding working set, as parquet-mr's writers do.

Writer admission is whitelist-first. Supported tables translate the effective DDL-over-Hadoop
configuration for compression, row-group/page/dictionary sizes, dictionary encoding,
writer version, and timestamp unit. Known Flink no-op keys are ignored explicitly; an unknown
`parquet.*` writer key falls back instead of being silently accepted. Flink still owns rolling,
partition commit, and filesystem-specific options without translation. `ROW`, `ARRAY`, `MAP`, and
`MULTISET` are encoded recursively with Flink's exact three-level Parquet list/map layout; nested
dates, decimals, times, and timestamps use the same host-compatible leaf encoding as top-level
columns.

Falls back to Flink on:

- Timestamp columns without `'parquet.write.int64.timestamp' = 'true'` or
  `'parquet.utc-timezone' = 'true'` set.
- Unsupported leaf types such as `RAW` and intervals, including when nested.
- Tables where every column is a partition key, leaving a zero-column file schema.
- `'auto-compaction' = 'true'`.
- Unsupported compression codecs, or multithreaded zstd.
- `INSERT OVERWRITE`.
- A changelog (retracting) input through the standard filesystem connector. The benchmark-only
  `changelog-parquet` connector can persist the raw physical change stream with a native
  `_row_kind` column; it is not a materialized-table sink.

See [Deployment](../deployment.md) for the JARs a Parquet sink needs.
