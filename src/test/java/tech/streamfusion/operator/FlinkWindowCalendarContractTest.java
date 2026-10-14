package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import org.apache.flink.table.runtime.util.TimeWindowUtil;
import org.junit.jupiter.api.Test;

class FlinkWindowCalendarContractTest {
  private static final ZoneId LOS_ANGELES = ZoneId.of("America/Los_Angeles");

  @Test
  void overlapAssignmentSharesTheLocalGridButTimersUseTheLaterOccurrence() {
    long first = Instant.parse("2024-11-03T08:30:00Z").toEpochMilli();
    long second = Instant.parse("2024-11-03T09:30:00Z").toEpochMilli();
    long local = localMillis("2024-11-03T01:30:00");
    assertEquals(local, TimeWindowUtil.toUtcTimestampMills(first, LOS_ANGELES));
    assertEquals(local, TimeWindowUtil.toUtcTimestampMills(second, LOS_ANGELES));
    assertEquals(first, TimeWindowUtil.toEpochMills(local, LOS_ANGELES));
    assertEquals(second, TimeWindowUtil.toEpochMillsForTimer(local, LOS_ANGELES));
  }

  @Test
  void gapTimersAndEmittedWindowTimeUseDifferentReleasedHostConversions() {
    long local = localMillis("2024-03-10T02:30:00");
    assertEquals(
        Instant.parse("2024-03-10T10:30:00Z").toEpochMilli(),
        TimeWindowUtil.toEpochMills(local, LOS_ANGELES));
    assertEquals(
        Instant.parse("2024-03-10T10:00:00Z").toEpochMilli(),
        TimeWindowUtil.toEpochMillsForTimer(local, LOS_ANGELES));
  }

  @Test
  void historicalTimersCannotBeRepresentedByOneMonotonicLocalWatermark() {
    long earlierLocal = localMillis("1883-11-18T12:07:01.999");
    long laterLocal = localMillis("1883-11-18T12:07:02");
    long earlierDeadline = TimeWindowUtil.toEpochMillsForTimer(earlierLocal, LOS_ANGELES);
    long laterDeadline = TimeWindowUtil.toEpochMillsForTimer(laterLocal, LOS_ANGELES);
    assertEquals(Instant.parse("1883-11-18T20:59:59.999Z").toEpochMilli(), earlierDeadline);
    assertEquals(Instant.parse("1883-11-18T20:07:02Z").toEpochMilli(), laterDeadline);
    assertTrue(earlierDeadline > laterDeadline);
  }

  private static long localMillis(String value) {
    return LocalDateTime.parse(value).toInstant(ZoneOffset.UTC).toEpochMilli();
  }

  @Test
  void repeatedWindowTimesPreserveEachReleasedConversion() {
    long[] values = {
      localMillis("2024-03-10T02:30:00"),
      localMillis("2024-03-10T02:30:00"),
      localMillis("2024-11-03T01:30:00"),
      localMillis("2024-11-03T01:30:00"),
      localMillis("1883-11-18T12:07:02"),
      localMillis("1883-11-18T12:07:02"),
      localMillis("1969-12-31T23:59:59.999"),
      localMillis("0001-01-01T00:00:00"),
      localMillis("9999-12-31T23:59:59.999"),
      localMillis("2024-03-10T03:30:00"),
      localMillis("2024-11-03T02:30:00"),
      0,
      -1
    };
    long[] local = new long[512];
    for (int i = 0; i < local.length; i++) local[i] = values[(i / 2 + i % 2 * 3) % values.length];
    for (String zoneId : new String[] {"America/Los_Angeles", "Asia/Shanghai", "GMT+05:45"}) {
      ZoneId zone = ZoneId.of(zoneId);
      long[] expected =
          java.util.Arrays.stream(local)
              .map(value -> TimeWindowUtil.toEpochMills(value, zone))
              .toArray();
      org.junit.jupiter.api.Assertions.assertArrayEquals(
          expected, new WindowCalendar(zoneId).epochWindowMillis(local));
    }
  }
}
