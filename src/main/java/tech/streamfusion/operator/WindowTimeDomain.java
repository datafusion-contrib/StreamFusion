package tech.streamfusion.operator;

import java.time.Instant;
import java.time.ZoneId;

/** Fixed-offset local window bounds share one clock translation with their consumers. */
final class WindowTimeDomain {
  private WindowTimeDomain() {}

  static long offsetMillis(String zoneId) {
    var rules = ZoneId.of(zoneId).getRules();
    if (!rules.isFixedOffset()) {
      throw new IllegalArgumentException("Native windows require a fixed time-zone offset");
    }
    return rules.getOffset(Instant.EPOCH).getTotalSeconds() * 1000L;
  }

  static long gridStart(long epochMillis, long gridMillis, long offsetMillis) {
    long shift = Math.floorMod(offsetMillis, gridMillis);
    return Math.floorDiv(shift == 0 ? epochMillis : Math.addExact(epochMillis, shift), gridMillis)
            * gridMillis
        - shift;
  }

  static long closeThreshold(long epochMillis, long offsetMillis) {
    if (epochMillis == Long.MIN_VALUE || epochMillis == Long.MAX_VALUE) {
      return epochMillis;
    }
    // Flink fires at end - 1; the native stores index the exclusive window end.
    long shift = offsetMillis + 1;
    try {
      return Math.addExact(epochMillis, shift);
    } catch (ArithmeticException overflow) {
      return shift > 0 ? Long.MAX_VALUE : Long.MIN_VALUE;
    }
  }
}
