package tech.streamfusion;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class FlinkDecimalUnaryArithmeticTryCastSqlHarnessTest {
  @ParameterizedTest
  @CsvSource({"1,0", "4,2", "18,0", "18,3", "18,18", "19,0", "19,19", "38,0", "38,18", "38,38"})
  void verifiedUnaryArithmeticFormatsWithoutAnEvaluator(int precision, int scale) throws Exception {
    BigInteger[] values = tech.streamfusion.planner.IntegralExpressionBenchmarkFunctions
        .decimalEpochCoefficients(precision, scale).toArray(new BigInteger[0]);
    String sql = "SELECT id,TRY_CAST(ABS(d*d) AS STRING),"
        + "TRY_CAST(-ABS(d*d) AS CHAR(48)),TRY_CAST(-(d+e) AS VARCHAR(8)),"
        + "TRY_CAST(ABS(-ABS(-d+e)) AS STRING),"
        + "TRY_CAST(d+ABS(d*d) AS STRING),TRY_CAST(ABS(d*d) AS STRING) IS NULL,"
        + "CHAR_LENGTH(TRY_CAST(-ABS(d*d) AS STRING)),"
        + "CASE WHEN MOD(id,3)=0 THEN TRY_CAST(ABS(d*d) AS STRING) "
        + "ELSE TRY_CAST(-(d+e) AS STRING) END FROM src";
    for (boolean nullable : new boolean[] {false, true}) {
      for (boolean legacy : new boolean[] {false, true}) {
        FlinkDecimalBooleanTryCastSqlHarnessTest.verify(precision, scale, nullable, legacy,
            sql, true, values, "UTC");
        FlinkDecimalBooleanTryCastSqlHarnessTest.verify(precision, scale, nullable, legacy,
            sql + " WHERE id<0", true, values, "UTC");
        FlinkDecimalBooleanTryCastSqlHarnessTest.verify(precision, scale, nullable, legacy,
            "SELECT id,CASE WHEN TRY_CAST(ABS(d*d) AS STRING) IS NULL OR MOD(id,3)=0 "
                + "THEN TRY_CAST(-ABS(d*d) AS STRING) ELSE 'other' END FROM src",
            true, values, "UTC");
      }
    }
  }

  @Test
  void ordinaryCastKeepsItsProjectionDefaults() throws Exception {
    for (boolean nullable : new boolean[] {false, true}) {
      BuiltinFunctionParity.assertParity(() -> environment(nullable),
          "SELECT CAST(-ABS(d*d) AS STRING),TRY_CAST(-ABS(d*d) AS STRING),"
              + "CAST(-ABS(d*d) AS STRING) IS NULL,TRY_CAST(-ABS(d*d) AS STRING) IS NULL "
              + "FROM src");
      BuiltinFunctionParity.assertParity(() -> environment(nullable),
          "SELECT TRY_CAST(-ABS(d*d) AS STRING) FROM src "
              + "WHERE TRY_CAST(ABS(d*d) AS STRING) IS NULL OR d=0");
    }
  }

  @Test
  void unverifiedAndFallibleProducersKeepFallbackAndFailureSemantics() throws Exception {
    for (String sql : List.of("SELECT TRY_CAST(-ABS(CAST(d AS DECIMAL(18,0))) AS STRING) FROM src",
        "SELECT TRY_CAST(ABS(d/(d-d)) AS STRING) FROM src WHERE d=123",
        "SELECT CASE WHEN d IS NOT NULL THEN TRY_CAST(ABS(d*d) AS STRING) "
            + "ELSE TRY_CAST(ABS(1/(d-d)) AS STRING) END FROM src")) {
      NativeParity.assertFallbackReasonContains(() -> environment(true), sql,
          "unsupported TRY_CAST DECIMAL");
      NativeFailureParity.run(() -> environment(true), sql)
          .assertSuccess(NativeFailureParity.Route.FALLBACK);
    }
    NativeFailureParity.run(() -> environment(true),
        "SELECT TRY_CAST(-ABS(d/(d-d)) AS STRING) FROM src")
        .assertFailure(ArithmeticException.class, NativeFailureParity.Phase.ROW_EVALUATION,
            NativeFailureParity.Route.FALLBACK);
  }

  private static org.apache.flink.table.api.TableEnvironment environment(boolean nullable) {
    var decimal = DataTypes.DECIMAL(38,18);
    return BuiltinFunctionParity.environment(
        DataTypes.ROW(DataTypes.FIELD("d", nullable ? decimal : decimal.notNull())),
        List.of(Row.of(new BigDecimal("99999999999999999999.999999999999999999")),
            Row.of(new BigDecimal("-2.000000000000000001")), Row.of(BigDecimal.ZERO)));
  }
}
