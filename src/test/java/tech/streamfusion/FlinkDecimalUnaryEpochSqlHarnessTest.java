package tech.streamfusion;

import java.util.List;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.api.Test;

class FlinkDecimalUnaryEpochSqlHarnessTest {
  @ParameterizedTest
  @CsvSource({"1,0", "4,2", "18,0", "18,3", "18,18", "19,0", "19,19", "38,0", "38,18", "38,38"})
  void totalUnaryDecimalEpochProducersPreserveReleasedSemantics(int precision, int scale)
      throws Exception {
    var coefficients = tech.streamfusion.planner.IntegralExpressionBenchmarkFunctions
        .decimalEpochCoefficients(precision, scale).toArray(new java.math.BigInteger[0]);
    for (boolean nullable : new boolean[] {false, true}) {
      String sql = "SELECT id,TO_TIMESTAMP_LTZ(-d,0),TO_TIMESTAMP_LTZ(ABS(d),3),"
          + "TO_TIMESTAMP_LTZ(-(-d),3),TO_TIMESTAMP_LTZ(ABS(ABS(e)),0),"
          + "TO_TIMESTAMP_LTZ(-ABS(d),3),TO_TIMESTAMP_LTZ(ABS(-ABS(e)),0),"
          + "TO_TIMESTAMP_LTZ(CAST(-d AS DECIMAL(38," + scale + ")),3),"
          + "CASE WHEN MOD(id,3)=0 THEN TO_TIMESTAMP_LTZ(ABS(d),0) "
          + "ELSE TO_TIMESTAMP_LTZ(-e,3) END FROM src";
      for (boolean legacy : new boolean[] {false, true}) {
        FlinkDecimalBooleanTryCastSqlHarnessTest.verify(precision, scale, nullable, legacy,
            sql, true, coefficients, "America/New_York");
        FlinkDecimalBooleanTryCastSqlHarnessTest.verify(precision, scale, nullable, legacy,
            sql + " WHERE id<0", true, coefficients, "UTC");
      }
    }
  }

  @Test
  void overflowProducingArithmeticAndFallibleOrRuntimePrecisionKeepTheirRoutes() throws Exception {
    for (String sql : List.of("SELECT id,TO_TIMESTAMP_LTZ(d+d,3) FROM src",
        "SELECT id,TO_TIMESTAMP_LTZ(CAST(d AS DECIMAL(18,0)),3) FROM src",
        "SELECT id,TO_TIMESTAMP_LTZ(d/(id-id),0) FROM src WHERE id<0",
        "SELECT id,CASE WHEN id>=0 THEN TO_TIMESTAMP_LTZ(-d,3) "
            + "ELSE TO_TIMESTAMP_LTZ(d/(id-id),0) END FROM src",
        "SELECT id,TO_TIMESTAMP_LTZ(-d,CASE WHEN id<1000 THEN 0 ELSE 3 END) FROM src")) {
      FlinkDecimalBooleanTryCastSqlHarnessTest.verify(18, 3, true, false, sql, false);
    }
  }
}
