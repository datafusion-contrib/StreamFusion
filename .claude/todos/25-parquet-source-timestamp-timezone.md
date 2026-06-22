# Native Parquet source: timestamp time-zone parity

**Status:** OPEN. Blocks fully-columnar windowed queries over a Parquet source
(`FlinkWatermarkAssignerSqlHarnessTest.windowedAggregateOverParquetSourceMatchesHost`
is `@Disabled` pending this).

## Symptom
A windowed aggregate over a Parquet source with a `WATERMARK` clause routes
correctly (native source → native watermark assigner → native window) and the
aggregates are right, but the window-boundary timestamps differ from the host by
the local time-zone offset (e.g. `06:00` vs `00:00` on a UTC-6 box).

## Root cause
`TIMESTAMP_WITHOUT_TIME_ZONE` round-trips through Parquet differently in the two
engines:

- The host wrote the column as Parquet **INT96** (legacy Impala timestamps),
  encoded using the JVM-default local zone.
- `parquet-rs` reads INT96 as a raw-UTC `Timestamp(Nanosecond)` and applies no
  zone adjustment, so the epoch millis the native side derives are off by the
  writer's local-zone offset.
- The mismatch is symmetric and direction-dependent: each engine round-trips its
  **own** written data correctly, but cross-engine reads disagree by the offset.
  Writing the input via the native sink (INT64 `TIMESTAMP(ns)`) instead flips the
  shift onto the host's read — confirming it is a convention mismatch, not a bug
  in one reader.

So the aggregation is internally consistent (both rt and window bounds share the
offset, so grouping is unaffected — only the absolute labels move).

## What this is NOT
Not the projection bug (fixed: the source now honors projection pushdown) and not
the watermark assigner (it forwards batches and emits watermarks correctly; the
unit-aware rowtime read already handles nano/micro/milli). This is purely the
source's timestamp→epoch interpretation.

## Fix direction
Make the native source derive the same epoch millis Flink does for a timestamp
column:

- Match Flink's Parquet timestamp convention on read — apply the session zone
  (`table.local-time-zone`) for `TIMESTAMP_WITHOUT_TIME_ZONE`, leave
  `TIMESTAMP_LTZ` as UTC. Needs the column's logical type and the session zone
  threaded to the reader.
- INT96 specifically is writer-zone-ambiguous; decide whether to (a) replicate
  Flink's local-zone INT96 convention, or (b) support only INT64 logical
  timestamps and record INT96 as a divergence.
- Add the parity test back once the native source's epoch millis match the host
  for both INT96- and INT64-encoded inputs.

## Verified adjacent work (already landed)
- Projection pushdown honored by the native source (`FlinkParquetProjectionSqlHarnessTest`).
- Columnar watermark assigner operator + matcher + planner wiring, gated to the
  bounded-out-of-orderness shape and the columnar-neighbour guardrail.
