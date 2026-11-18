package tech.streamfusion;

import org.junit.jupiter.api.Test;

class FlinkDayOfWeekSqlHarnessTest {
  @Test
  void calendarFieldMatchesAcrossYearBoundariesLeapDaysAndEpoch() throws Exception {
    NativeParity.assertParity(
        TextTimeFunctionTestInputs::calendar, "SELECT id, DAYOFWEEK(d), DAYOFWEEK(ts) FROM inputs");
  }

  @Test
  void millisecondPrecisionSourceColumnsStayNative() throws Exception {
    NativeParity.assertParity(
        () -> TextTimeFunctionTestInputs.calendar(3), "SELECT id, DAYOFWEEK(ts) FROM inputs");
  }
}
