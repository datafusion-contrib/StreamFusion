package io.github.jordepic.streamfusion.operator;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.TimeStampNanoVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.util.TransferPair;
import org.apache.flink.table.data.TimestampData;

/**
 * Reconciles the timestamp columns a native Parquet read produces with how the host's Parquet reader
 * interprets them, so a columnar source matches the host byte for byte.
 *
 * <p>The native reader ({@code parquet-rs}) decodes a timestamp to the raw UTC instant. The host
 * reader applies the {@code utc-timezone} option (default {@code false}): with it off it converts
 * that instant through the JVM-local zone (Flink's {@code TimestampColumnReader} does {@code new
 * java.sql.Timestamp(instantMillis)} then {@code TimestampData.fromTimestamp}), the legacy Hive
 * convention. We replay that exact conversion in the same JVM, then re-encode each value the way the
 * row→Arrow transpose does — {@code TimestampData.getMillisecond() * 1e6 + nanoOfMillisecond} — so a
 * source-fed batch is indistinguishable from a transpose-fed one and the whole downstream pipeline
 * (watermark assigner, window, Arrow→row) stays correct unchanged. With {@code utc-timezone = true}
 * the raw instant already equals that encoding, so the batch passes through untouched.
 */
final class ParquetSourceTimestamps {

  private ParquetSourceTimestamps() {}

  /**
   * Returns a batch whose timestamp columns carry the host's wall-clock encoding. When no conversion
   * is needed (UTC mode, or no timestamp columns) the input is returned as-is; otherwise a new root
   * is built and the input is consumed.
   */
  static VectorSchemaRoot normalize(
      VectorSchemaRoot in, BufferAllocator allocator, boolean utcTimestamp) {
    if (utcTimestamp || !hasTimestamp(in)) {
      return in;
    }
    List<FieldVector> fields = new ArrayList<>(in.getFieldVectors().size());
    for (FieldVector vector : in.getFieldVectors()) {
      if (vector instanceof TimeStampVector) {
        fields.add(toWallClockNanos((TimeStampVector) vector, allocator));
      } else {
        // Non-timestamp columns move over unchanged (zero-copy buffer transfer).
        TransferPair transfer = vector.getTransferPair(allocator);
        transfer.transfer();
        fields.add((FieldVector) transfer.getTo());
      }
    }
    VectorSchemaRoot out = new VectorSchemaRoot(fields);
    out.setRowCount(in.getRowCount());
    in.close(); // releases the original timestamp vectors (the rebuilt ones are independent)
    return out;
  }

  private static boolean hasTimestamp(VectorSchemaRoot root) {
    for (FieldVector vector : root.getFieldVectors()) {
      if (vector instanceof TimeStampVector) {
        return true;
      }
    }
    return false;
  }

  /**
   * Converts a UTC-instant timestamp column to the host's local-zone wall-clock encoding, replaying
   * {@code TimestampColumnReader.int96ToTimestamp(false, ...)} per value.
   */
  private static TimeStampNanoVector toWallClockNanos(TimeStampVector src, BufferAllocator allocator) {
    ArrowType.Timestamp type = (ArrowType.Timestamp) src.getField().getType();
    int rows = src.getValueCount();
    TimeStampNanoVector out = new TimeStampNanoVector(src.getName(), allocator);
    out.allocateNew(rows);
    for (int i = 0; i < rows; i++) {
      if (src.isNull(i)) {
        out.setNull(i);
        continue;
      }
      long instantNanos = toNanos(src.get(i), type.getUnit());
      long seconds = Math.floorDiv(instantNanos, 1_000_000_000L);
      int nanosOfSecond = (int) Math.floorMod(instantNanos, 1_000_000_000L);
      Timestamp ts = new Timestamp(seconds * 1_000L);
      ts.setNanos(nanosOfSecond);
      TimestampData wall = TimestampData.fromTimestamp(ts);
      out.set(i, wall.getMillisecond() * 1_000_000L + wall.getNanoOfMillisecond());
    }
    out.setValueCount(rows);
    return out;
  }

  /** Scales a raw timestamp in its column unit up to nanoseconds. */
  private static long toNanos(long raw, org.apache.arrow.vector.types.TimeUnit unit) {
    switch (unit) {
      case SECOND:
        return raw * 1_000_000_000L;
      case MILLISECOND:
        return raw * 1_000_000L;
      case MICROSECOND:
        return raw * 1_000L;
      case NANOSECOND:
      default:
        return raw;
    }
  }
}
