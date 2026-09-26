# Parquet

**Status:** experimental. Unlike [Kafka](kafka.md), which is production-grade and documented in
depth, the Parquet native sink has not seen the same hardening — expect rough edges and treat
fallback to stock Flink as the normal, safe outcome.

## Source

Filesystem Parquet scans use Flink's stock source. [Paimon streaming sources](paimon.md#streaming-source)
use this module's native decoder over Java-owned FileIO, including admitted snapshot merges and
changelog tailing. ORC uses the same file lifecycle through its separate [ORC module](orc.md).

## Sink

The sink accepts **any filesystem Flink has a plugin for** (`file:`/`s3:`/`gs:`/`abfs:`/`hdfs:`/…).
The native side only encodes Parquet bytes; Flink's own recoverable output streams do the I/O, so
filesystem plugins, credentials, exactly-once commit, bucket assignment, file naming, and partition
commit all remain Flink's own code. Each open Flink part file owns a parquet-rs writer.
INT64 files use the standard `ArrowWriter`. INT96 files combine its Arrow column encoders with
parquet-rs' typed INT96 encoder, preserving nested definition/repetition levels. Both consume
Arrow batches directly and send encoded bytes through a reusable, bounded one-MiB JNI bridge,
without transposing the batches through Java rows.

Flink also owns file rolling. Part files roll on checkpoints, configured size, rollover time, or
inactivity using the normal `sink.rolling-policy.*` options. Size checks observe the bytes already
drained to Flink's stream, so—as with the stock bulk writer—the visible size advances at completed
row-group granularity. `BulkWriter.flush()` is intentionally a no-op: the standard Arrow writer
closes its active row group and writes the footer when Flink finishes the part file. The INT96
adapter also closes row groups at the configured byte threshold, checking every 1,024 input rows.
Its pending timestamp values and levels count toward that threshold.

Writer admission is whitelist-first. Supported tables translate the effective DDL-over-Hadoop
configuration for compression, row-group/page/dictionary sizes, dictionary encoding,
writer version, and timestamp unit. Known Flink no-op keys are ignored explicitly; an unknown
`parquet.*` writer key falls back instead of being silently accepted. Flink still owns rolling,
partition commit, and filesystem-specific options without translation. `ROW`, `ARRAY`, `MAP`, and
`MULTISET` are encoded recursively with Flink's exact three-level Parquet list/map layout,
including the legacy `MAP_KEY_VALUE` annotation on each repeated `key_value` group. Nested dates,
decimals, times, and timestamps use the same host-compatible leaf encoding as top-level columns.
`BINARY(n)` Arrow buffers are converted to Parquet `BYTE_ARRAY`, matching Flink and Paimon
rather than Parquet fixed-length byte arrays; nulls, nested fields and sliced batches are preserved.
The partitioned SQL parity test compares stock and native footer schemas and rows with MAP columns.

Falls back to Flink on:

- INT64 timestamp columns without `'parquet.utc-timezone' = 'true'` set.
- Unsupported leaf types such as `RAW` and intervals, including when nested.
- Tables where every column is a partition key, leaving a zero-column file schema.
- `'auto-compaction' = 'true'`.
- Unsupported compression codecs, or multithreaded zstd.
- `INSERT OVERWRITE`.
- A nullable query field assigned to a `NOT NULL` target, or a bounded `CHAR`/`VARCHAR` or
  `BINARY`/`VARBINARY` target while `table.exec.sink.type-length-enforcer` is enabled. The stock
  sink path preserves Flink's configured fail/drop and trim/pad/error behavior.
- A changelog (retracting) input through the standard filesystem connector. The benchmark-only
  `changelog-parquet` connector can persist the raw physical change stream with a native
  `_row_kind` column; it is not a materialized-table sink.

On the 2M-event, four-partition Kafka JSON Nexmark sink diagnostic (memory state, mini-batching off,
one warmup, best of three), all 23 queries supported by Flink completed and StreamFusion's suite
geomean was **1.535×** the stock parquet-mr path. See [Benchmarks](../benchmarks.md#parquet-delta-and-paimon-sink-diagnostics)
for the exact method and reproduction commands.

See [Deployment](../deployment.md) for the JARs a Parquet sink needs.

## Timestamp values

The native reader converts physical timestamp units into the engine's millisecond/fraction pair.
INT96 requires two aligned column reads with the released Arrow API: milliseconds retain the date,
and wrapping subtraction of the nanosecond read recovers the fraction. This preserves wide dates,
nested values and nulls without row materialization. INT64 timestamps need one read. The sink floors
to its configured physical unit and matches Flink's Java `long` overflow at that file boundary.
An explicitly selected INT64 nanosecond unit therefore still cannot represent dates outside roughly
1677–2262; use microseconds for wide SQL dates when six fractional digits suffice. This physical
format limit does not affect the lossless representation inside operators and checkpoints.
With `parquet.write.int64.timestamp=false` (the host default), the sink writes INT96 instead.
It preserves the complete millisecond/fraction value as a Julian day and nanoseconds within the
day, matching Flink's truncating division and remainder even before 1970. INT96 does not narrow
the value through an i64 epoch-nanosecond intermediate. Nested timestamps, null/empty collections,
partition projection and sliced batches use the same encoding. For INT96 local-timezone output,
one JVM callback per timestamp column converts the millisecond/fraction pairs using Flink's
`TimestampData.toTimestamp()` and the host writer's Julian-day arithmetic. This preserves default
JVM timezone, daylight-saving gaps/overlaps and the legacy calendar before 1582. The result is a
column of twelve-byte values, encoded by the native writer without a rowwise table transpose.
INT64 local-timezone output remains outside admission.

## Flink 1.18 file schema

The development profile preserves the older host writer's declared map-key nullability. The native
file encoder applies that property to the Arrow write schema and the Parquet descriptor together,
so definition levels agree and keys survive readback. Operator batches retain their existing Arrow
representation. The partitioned SQL sink parity check compares data, directories, success markers
and complete footer schemas. See [Flink line compatibility](../flink-compatibility.md).
