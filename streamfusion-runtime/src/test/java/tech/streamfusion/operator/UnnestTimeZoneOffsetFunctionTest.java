package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.time.Instant;
import java.time.ZoneId;
import java.util.Random;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class UnnestTimeZoneOffsetFunctionTest {
  @ParameterizedTest
  @ValueSource(
      strings = {
        "America/New_York",
        "Europe/Berlin",
        "Pacific/Apia",
        "Asia/Kathmandu",
        "UTC",
        "+05:30"
      })
  void cacheMatchesJdkRulesAcrossTransitionsAndUnorderedFullRangeInput(String zone)
      throws Exception {
    var function = new UnnestTimeZoneOffsetFunction(zone);
    function.open(null);
    assertNull(function.eval(null));
    var rules = ZoneId.of(zone).getRules();
    long[] probes = {
      Long.MIN_VALUE,
      Long.MAX_VALUE,
      -1,
      0,
      Instant.parse("2026-03-08T07:00:00Z").toEpochMilli(),
      Instant.parse("2026-11-01T06:00:00Z").toEpochMilli(),
      Instant.parse("2011-12-30T10:00:00Z").toEpochMilli(),
      Instant.parse("1985-12-31T18:30:00Z").toEpochMilli()
    };
    for (long probe : probes) {
      for (int delta : new int[] {0, -1, 1, 0}) {
        long value = probe + delta;
        assertEquals(
            rules.getOffset(Instant.ofEpochMilli(value)).getTotalSeconds(), function.eval(value));
      }
    }
    var random = new Random(84921);
    for (int i = 0; i < 1024; i++) {
      long value = random.nextLong();
      assertEquals(
          rules.getOffset(Instant.ofEpochMilli(value)).getTotalSeconds(), function.eval(value));
    }
    assertSame(function.eval(1000L), function.eval(2000L));
  }
}
