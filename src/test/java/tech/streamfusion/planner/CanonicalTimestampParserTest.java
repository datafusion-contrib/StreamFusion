package tech.streamfusion.planner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Random;
import java.util.TimeZone;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.utils.DateTimeUtils;
import org.junit.jupiter.api.Test;

class CanonicalTimestampParserTest {
  private static final DateTimeFormatter FORMAT =
      DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss", java.util.Locale.ROOT);

  private static void assertReleasedParity(String text) {
    var input = StringData.fromString(text);
    for (int precision = 0; precision <= 9; precision++) {
      var local = CanonicalTimestampParser.parse(input, precision, null);
      assertNotNull(local, text);
      assertEquals(DateTimeUtils.parseTimestampData(text, precision), local, text);
      for (String zone : List.of("UTC", "Asia/Shanghai", "America/Los_Angeles", "Pacific/Apia")) {
        var timezone = TimeZone.getTimeZone(zone);
        assertEquals(DateTimeUtils.parseTimestampData(text, precision, timezone),
            CanonicalTimestampParser.parse(input, precision, timezone), text + " " + zone);
      }
    }
  }

  @Test
  void canonicalValuesMatchReleasedParserAcrossPrecisionRangeAndZones() {
    for (String text : List.of("0001-01-01 00:00:00", "9999-12-31 23:59:59.999999999",
        "1969-12-31 23:59:59.999999999", "2024-03-10 02:30:00.123456789",
        "2024-11-03 01:30:00", "2011-12-30 12:00:00", "2000-02-29 23:59:59")) {
      assertReleasedParity(text);
    }
    var random = new Random(278);
    for (int i = 0; i < 250; i++) {
      var value = LocalDateTime.of(1 + random.nextInt(9999), 1 + random.nextInt(12),
          1 + random.nextInt(28), random.nextInt(24), random.nextInt(60), random.nextInt(60));
      int fractionDigits = i % 10;
      String text = value.format(FORMAT);
      if (fractionDigits != 0) {
        text += "." + String.format(java.util.Locale.ROOT, "%09d", random.nextInt(1_000_000_000))
            .substring(0, fractionDigits);
      }
      assertReleasedParity(text);
    }
  }

  @Test
  void noncanonicalAndMalformedValuesRequestOriginalGeneratedFallback() {
    for (String text : List.of("", "bad", "2024-02-29", "2024-2-29 1:2:3",
        "2023-02-29 00:00:00", "2024-02-30 00:00:00", "2024-02-29 24:00:00",
        "0000-01-01 00:00:00", "+10000-01-01 00:00:00", "-0001-01-01 00:00:00",
        "2024-02-29 12:34:56.", "2024-02-29 12:34:56.1234567890",
        "2024-02-29 12:34:56.a", "2024-02-29 12:34:56.１２３",
        "2024-02-29T12:34:56", "2024-02-29 12:34:56Z", " 2024-02-29 12:34:56",
        "2024-02-29 12:34:56 ", "2024-02-29 12:34:60", "2024-02-29 12:6x:00")) {
      assertNull(CanonicalTimestampParser.parse(StringData.fromString(text), 9, null), text);
    }
  }
}
