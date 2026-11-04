package tech.streamfusion.planner;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;

class IntegerPrintfFormatTest {
  @Test
  void exactIntegerFormatsRespectIndexingAndQualificationBounds() {
    for (String format : List.of("", "%%", "%d/%s/%1$x", "%+020d:%2$-12.3S:%1$08X",
        "%1$d/%<x/%s", "%b/%1$B/%.0b/%2$8.2B", "%-8.3b", "%256d", "%.256s",
        "a".repeat(512))) {
      assertTrue(IntegerPrintfFormat.supports(format, 2), format);
    }
    assertTrue(IntegerPrintfFormat.supports("%%", 0));
  }

  @Test
  void invalidFlagsTypesAndUnboundedFormatsRetainTheirPreviousRoute() {
    for (String format : List.of("%n", "%f", "%c", "%+b", "%0b", "%#B", "%#x", "%257d", "%.257s",
        "%+s", "%--12d", "%0d", "%+ d", "%1$<s", "%<s", "%0$d", "%3$d",
        "%2147483648$d", "%2147483648d", "%", "a".repeat(513), "\u00e9%s")) {
      assertFalse(IntegerPrintfFormat.supports(format, 2), format);
    }
    assertFalse(IntegerPrintfFormat.supports("%d", 0));
  }
}
