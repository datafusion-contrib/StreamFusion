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
filesystem plugins, credentials, exactly-once commit, bucket assignment, file naming, and partition
commit all remain Flink's own code. Each open Flink part file owns one standard parquet-rs
`ArrowWriter`. It consumes Arrow batches directly and sends encoded bytes through a reusable,
bounded one-MiB JNI bridge. StreamFusion does not maintain a separate low-level Parquet writer and
does not transpose the batches through Java rows before writing them.

Flink also owns file rolling. Part files roll on checkpoints, configured size, rollover time, or
inactivity using the normal `sink.rolling-policy.*` options. Size checks observe the bytes already
drained to Flink's stream, so—as with the stock bulk writer—the visible size advances at completed
row-group granularity. `BulkWriter.flush()` is intentionally a no-op: the standard Arrow writer
closes its active row group and writes the footer when Flink finishes the part file. There is no
custom column-chunk flush path, so the active row group may remain in the writer until its normal
row-group threshold or file finalization.

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

On the 2M-event, four-partition Kafka JSON Nexmark sink diagnostic (memory state, mini-batching off,
one warmup, best of three), all 23 queries supported by Flink completed and StreamFusion's suite
geomean was **1.535×** the stock parquet-mr path. See [Benchmarks](../benchmarks.md#parquet-and-delta-sink-diagnostics)
for the exact method and reproduction commands.

See [Deployment](../deployment.md) for the JARs a Parquet sink needs.
