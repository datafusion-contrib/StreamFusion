package tech.streamfusion;

import org.junit.jupiter.api.Test;
import tech.streamfusion.runtime.common.NativeParity;

class FlinkQuarterSqlHarnessTest {
  @Test
  void calendarFieldMatchesAcrossYearBoundariesLeapDaysAndEpoch() throws Exception {
    NativeParity.assertParity(
        TextTimeFunctionTestInputs::calendar, "SELECT id, QUARTER(d), QUARTER(ts) FROM inputs");
  }

  @Test
  void millisecondPrecisionSourceColumnsStayNative() throws Exception {
    NativeParity.assertParity(
        () -> TextTimeFunctionTestInputs.calendar(3), "SELECT id, QUARTER(ts) FROM inputs");
  }
}
