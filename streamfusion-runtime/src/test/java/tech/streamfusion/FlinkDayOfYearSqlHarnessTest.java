package tech.streamfusion;

import org.junit.jupiter.api.Test;

class FlinkDayOfYearSqlHarnessTest {
  @Test
  void calendarFieldMatchesAcrossYearBoundariesLeapDaysAndEpoch() throws Exception {
    NativeParity.assertParity(
        TextTimeFunctionTestInputs::calendar, "SELECT id, DAYOFYEAR(d), DAYOFYEAR(ts) FROM inputs");
  }

  @Test
  void millisecondPrecisionSourceColumnsStayNative() throws Exception {
    NativeParity.assertParity(
        () -> TextTimeFunctionTestInputs.calendar(3), "SELECT id, DAYOFYEAR(ts) FROM inputs");
  }
}
