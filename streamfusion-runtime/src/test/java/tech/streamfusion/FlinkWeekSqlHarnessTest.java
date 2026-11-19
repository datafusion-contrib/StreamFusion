package tech.streamfusion;

import org.junit.jupiter.api.Test;

class FlinkWeekSqlHarnessTest {
  @Test
  void calendarFieldMatchesAcrossYearBoundariesLeapDaysAndEpoch() throws Exception {
    NativeParity.assertParity(
        TextTimeFunctionTestInputs::calendar, "SELECT id, WEEK(d), WEEK(ts) FROM inputs");
  }

  @Test
  void millisecondPrecisionSourceColumnsStayNative() throws Exception {
    NativeParity.assertParity(
        () -> TextTimeFunctionTestInputs.calendar(3), "SELECT id, WEEK(ts) FROM inputs");
  }
}
