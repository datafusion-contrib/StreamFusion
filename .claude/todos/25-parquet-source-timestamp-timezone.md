# Native Parquet source: timestamp time-zone parity

**Status:** DONE. The fully-columnar windowed query over a Parquet source
(`FlinkWatermarkAssignerSqlHarnessTest.windowedAggregateOverParquetSourceMatchesHost`)
is enabled and green.

## What was wrong
A windowed aggregate over a Parquet source read the rowtime column with a
time-zone offset versus the host, shifting the window labels (e.g. `06:00` vs
`00:00`). `TIMESTAMP_WITHOUT_TIME_ZONE` round-trips differently in the two
engines: the host applies the format's `utc-timezone` option (default `false`,
the legacy Hive convention) — `TimestampColumnReader` does `new
java.sql.Timestamp(instantMillis)` then `TimestampData.fromTimestamp`, converting
the UTC instant through the JVM-local zone — while `parquet-rs` hands back the raw
UTC instant.

## Fix (landed)
`ParquetSourceTimestamps.normalize` replays the host's exact conversion in the
same JVM for each timestamp column, then re-encodes it the way the row→Arrow
transpose does (`TimestampData.getMillisecond() * 1e6 + nanoOfMillisecond`). A
source-fed batch is then byte-identical to a transpose-fed one, so the whole
downstream pipeline stays correct unchanged. The `utc-timezone` setting is read
off the table options and threaded to the source; with it `true`, the raw instant
already equals that encoding and the batch passes through untouched.

## Related divergence (not a blocker)
Per-batch watermark assignment ([divergences/09](../../divergences/09-per-batch-watermark-assignment.md)):
the columnar assigner advances the watermark once per batch, so an out-of-order
straggler packed in the same batch that the host's per-row assigner would drop is
kept. The parity test uses a watermark delay that keeps windows open until
end-of-input MAX, avoiding that corner.

## Possible follow-ups (lower priority)
- INT64-encoded parquet timestamps (`write.int64.timestamp = true`, "time-zone
  agnostic") and explicit `TIMESTAMP_LTZ` columns: the normalize path covers the
  default INT96 + `utc-timezone=false` case verified here; add parity coverage for
  the INT64/LTZ permutations.
