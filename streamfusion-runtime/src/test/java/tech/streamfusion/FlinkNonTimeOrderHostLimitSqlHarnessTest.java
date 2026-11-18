package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class FlinkNonTimeOrderHostLimitSqlHarnessTest {
  @ParameterizedTest
  @ValueSource(strings = {
    "CAST(n AS TINYINT)", "CAST(n AS SMALLINT)", "CAST(n AS INT)", "n",
    "CAST(n AS DECIMAL(20,2))", "s", "CAST(s AS CHAR(8))",
    "day_value", "time_value", "flag", "t", "instant_value"
  })
  void descendingNonTimeOrderRetainsTheReleasedPlanningFailure(String order) {
    assertHostFailure(order + " DESC NULLS FIRST", "ASCENDING mode");
  }

  @ParameterizedTest
  @ValueSource(strings = {
    "n ASC, id ASC", "s ASC NULLS FIRST, day_value ASC NULLS LAST",
    "t ASC, instant_value ASC", "n ASC, CAST(n AS DOUBLE) ASC"
  })
  void multipleOrdinarySortKeysRetainTheReleasedPlanningFailure(String order) {
    assertHostFailure(order, "single time column");
  }

  private static void assertHostFailure(String order, String message) {
    var comparison = NativeFailureParity.run(
        () -> FlinkOverBasicCountsSqlHarnessTest.environment(32),
        "SELECT id, k, ROW_NUMBER() OVER (PARTITION BY k ORDER BY " + order + ") FROM src");
    assertNotNull(comparison.host().failure());
    assertNotNull(comparison.nativeRun().failure());
    assertEquals(comparison.host().rootCause().getClass(), comparison.nativeRun().rootCause().getClass());
    assertEquals(comparison.host().phase(), comparison.nativeRun().phase());
    assertEquals(comparison.host().rootCause().getMessage(), comparison.nativeRun().rootCause().getMessage());
    assertTrue(comparison.host().rootCause().getMessage().contains(message), comparison::toString);
    assertFalse(comparison.nativeRun().plan().contains("NativeColumnarTopN"));
  }
}
