package tech.streamfusion.operator;

import java.time.Instant;
import java.time.ZoneId;
import java.time.zone.ZoneOffsetTransition;
import java.time.zone.ZoneRules;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;

/** JDK offsets for native UNNEST casts, cached over each transition interval. */
public final class UnnestTimeZoneOffsetFunction extends ScalarFunction {
  private static final long serialVersionUID = 1L;
  private final String zoneId;
  private transient ZoneRules rules;
  private transient long startSecond;
  private transient long endSecond;
  private transient Integer offsetSeconds;

  public UnnestTimeZoneOffsetFunction(String zoneId) {
    this.zoneId = zoneId;
  }

  @Override
  public void open(FunctionContext context) {
    rules = ZoneId.of(zoneId).getRules();
    offsetSeconds = null;
  }

  public Integer eval(Long epochMillis) {
    if (epochMillis == null) return null;
    long second = Math.floorDiv(epochMillis, 1000);
    if (offsetSeconds == null || second < startSecond || second >= endSecond) {
      Instant instant = Instant.ofEpochMilli(epochMillis);
      offsetSeconds = rules.getOffset(instant).getTotalSeconds();
      ZoneOffsetTransition previous = rules.previousTransition(instant.plusNanos(1));
      ZoneOffsetTransition next = rules.nextTransition(instant);
      startSecond = previous == null ? Long.MIN_VALUE : previous.getInstant().getEpochSecond();
      endSecond = next == null ? Long.MAX_VALUE : next.getInstant().getEpochSecond();
    }
    return offsetSeconds;
  }
}
