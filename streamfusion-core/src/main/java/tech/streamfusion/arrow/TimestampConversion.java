package tech.streamfusion.arrow;

import org.apache.flink.table.data.TimestampData;

/** Checked conversion from timestamp components to external signed 64-bit timestamp units. */
public final class TimestampConversion {
  private TimestampConversion() {}

  public static long toNanos(TimestampData timestamp) {
    return toUnits(timestamp.getMillisecond(), timestamp.getNanoOfMillisecond(), 1_000_000L);
  }

  public static long toMicros(long millis, int nanoOfMillisecond) {
    return toUnits(millis, nanoOfMillisecond / 1000, 1000L);
  }

  private static long toUnits(long millis, long fraction, long scale) {
    // The floor millisecond below Long.MIN_VALUE / scale can still have a representable fraction.
    return millis < 0
        ? Math.addExact(Math.multiplyExact(millis + 1, scale), fraction - scale)
        : Math.addExact(Math.multiplyExact(millis, scale), fraction);
  }
}
