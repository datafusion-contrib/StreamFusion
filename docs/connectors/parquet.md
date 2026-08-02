# Parquet

**Status:** experimental. Unlike [Kafka](kafka.md), which is production-grade and documented in
depth, the Parquet native paths have not seen the same hardening — expect rough edges and treat
fallback to stock Flink as the normal, safe outcome.

## Source

The native source reads only local `file:` paths. Any other scheme (`hdfs:`, `s3:`, …) or any
non-Parquet source format falls back to Flink's own reader. An ORC source existed and was removed
— its scan engine lagged DataFusion releases and required carrying a fork pin through every
DataFusion bump. Restoring it is tracked as
[issue #19](https://github.com/datafusion-contrib/StreamFusion/issues/19); it is not planned work
described here.

## Sink

The sink accepts **any filesystem Flink has a plugin for** (`file:`/`s3:`/`gs:`/`abfs:`/`hdfs:`/…).
The native side only encodes Parquet bytes; Flink's own recoverable output streams do the I/O, so
filesystem plugins, credentials, exactly-once commit, and partition commit all remain Flink's own
code.

Falls back to Flink on:

- Timestamp columns without `'parquet.write.int64.timestamp' = 'true'` or
  `'parquet.utc-timezone' = 'true'` set.
- Nested written columns (`ARRAY`/`MAP`/`MULTISET`/`ROW`/`RAW`) — scalar columns are fully covered.
- `'auto-compaction' = 'true'`.
- Unsupported compression codecs, or multithreaded zstd.
- `INSERT OVERWRITE`.
- A changelog (retracting) input.

See [Deployment](../deployment.md) for the JARs a Parquet source or sink needs.
