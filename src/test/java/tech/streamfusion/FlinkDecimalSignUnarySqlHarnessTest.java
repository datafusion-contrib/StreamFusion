package tech.streamfusion;

import java.math.BigInteger;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class FlinkDecimalSignUnarySqlHarnessTest {
  @ParameterizedTest
  @CsvSource({"1,0", "4,2", "18,0", "18,3", "19,0", "19,19", "38,0", "38,18", "38,38"})
  void verifiedUnaryProducersPreserveSignsNullsAndIntermediateTypes(int precision, int scale)
      throws Exception {
    BigInteger[] values = tech.streamfusion.planner.IntegralExpressionBenchmarkFunctions
        .decimalEpochCoefficients(precision, scale).toArray(new BigInteger[0]);
    for (boolean nullable : new boolean[] {false, true}) {
      var expressions = new java.util.ArrayList<String>();
      for (String operand : List.of("ABS(d)", "-d", "-ABS(d)", "ABS(-ABS(d))",
          "CAST(-d AS DECIMAL(38," + scale + "))", "ABS(d*d)", "-(d+e)",
          "d+ABS(d*d)")) {
        String sign = "SIGN(" + operand + ")";
        expressions.add(nullable ? sign : "(" + sign + ">0 OR CAST(NULL AS BOOLEAN))");
      }
      String sql = "SELECT id," + String.join(",", expressions) + " FROM src";
      FlinkDecimalBooleanTryCastSqlHarnessTest.verify(precision, scale, nullable, false,
          sql, true, values, "UTC");
      FlinkDecimalBooleanTryCastSqlHarnessTest.verify(precision, scale, nullable, true,
          sql + " WHERE id<0", true, values, "UTC");
    }
  }

  @ParameterizedTest
  @CsvSource({"1,1", "18,18"})
  void compactSignAndItsWidenedRepresentationKeepGeneratedEvaluation(int precision, int scale)
      throws Exception {
    FlinkDecimalBooleanTryCastSqlHarnessTest.verify(precision, scale, true, false,
        "SELECT id,SIGN(ABS(d)),SIGN(-d),CAST(SIGN(ABS(d)) AS STRING),"
            + "SIGN(ABS(d)) IS NULL,"
            + "SIGN(CAST(SIGN(ABS(d)) AS DECIMAL(38," + scale + "))) FROM src", false);
  }

  @Test
  void unverifiedUnaryProducersKeepTheirPreviousEvaluation() throws Exception {
    for (String operand : List.of("ABS(d/(id-id))", "ABS(CAST(d AS DECIMAL(17,3)))",
        "ABS(CAST(d AS DECIMAL(18,2)))", "ABS(SIGN(d))")) {
      FlinkDecimalBooleanTryCastSqlHarnessTest.verify(18, 3, true, false,
          "SELECT id,SIGN(" + operand + ") FROM src WHERE id<0", false);
    }
    FlinkDecimalBooleanTryCastSqlHarnessTest.verify(18, 3, true, false,
        "SELECT id,CASE WHEN id>=0 THEN SIGN(ABS(d)) "
            + "ELSE SIGN(ABS(d/(id-id))) END FROM src", false);
  }

  @Test
  void basicConsumersRetainReleasedProjectionDefaultsAndNullFlags() throws Exception {
    for (int scale : new int[] {18, 38}) {
      for (boolean nullable : new boolean[] {false, true}) {
        for (boolean legacy : new boolean[] {false, true}) {
          BuiltinFunctionParity.assertParity(() -> {
            var table = environment(scale, nullable);
            table.getConfig().set("table.exec.legacy-cast-behaviour", legacy ? "ENABLED" : "DISABLED");
            return table;
          }, "SELECT id,SIGN(ABS(d)) IS NULL,SIGN(-d)>0,CAST(SIGN(-d) AS STRING),"
              + "CHAR_LENGTH(CAST(SIGN(ABS(d)) AS STRING)),"
              + "CASE WHEN SIGN(-ABS(d))<0 THEN 'negative' ELSE 'other' END FROM src");
        }
      }
    }
  }

  @Test
  void nonNullableNullResultsRetainErrorAndDropEnforcement() throws Exception {
    for (int scale : new int[] {18, 38}) {
      for (String policy : List.of("ERROR", "DROP")) {
        java.util.function.Supplier<org.apache.flink.table.api.TableEnvironment> factory = () -> {
          var table = environment(scale, false);
          table.getConfig().set("table.exec.sink.not-null-enforcer", policy);
          return table;
        };
        String sql = "SELECT id,SIGN(ABS(" + (scale == 38 ? "d" : "d*d") + ")) FROM src";
        if (policy.equals("DROP")) BuiltinFunctionParity.assertParity(factory, sql);
        else {
          var runs = NativeFailureParity.run(factory, sql);
          org.junit.jupiter.api.Assertions.assertNotNull(runs.host().rootCause(), runs.toString());
          runs.assertFailure(runs.host().rootCause().getClass(), "is NOT NULL",
              NativeFailureParity.Phase.ROW_EVALUATION, NativeFailureParity.Route.NATIVE);
        }
      }
    }
  }

  @Test
  void characterTryCastAndSelectedFailuresKeepTheirPreviousContracts() throws Exception {
    NativeParity.assertFallbackReasonContains(() -> environment(18, true),
        "SELECT TRY_CAST(SIGN(ABS(d)) AS STRING) FROM src", "unsupported TRY_CAST DECIMAL");
    NativeFailureParity.run(() -> environment(18, true),
        "SELECT SIGN(ABS(d/(id-id))) FROM src")
        .assertFailure(ArithmeticException.class, NativeFailureParity.Phase.ROW_EVALUATION,
            NativeFailureParity.Route.NATIVE);
  }

  private static org.apache.flink.table.api.TableEnvironment environment(int scale, boolean nullable) {
    var decimal = org.apache.flink.table.api.DataTypes.DECIMAL(38, scale);
    var fields = org.apache.flink.table.api.DataTypes.ROW(
        org.apache.flink.table.api.DataTypes.FIELD("id", org.apache.flink.table.api.DataTypes.INT().notNull()),
        org.apache.flink.table.api.DataTypes.FIELD("d", nullable ? decimal : decimal.notNull()));
    var values = new java.util.ArrayList<org.apache.flink.types.Row>();
    BigInteger max = BigInteger.TEN.pow(38).subtract(BigInteger.ONE);
    for (BigInteger coefficient : List.of(max.negate(), BigInteger.ONE.negate(), BigInteger.ZERO,
        BigInteger.ONE, max)) {
      values.add(org.apache.flink.types.Row.of(values.size(), new java.math.BigDecimal(coefficient, scale)));
    }
    if (nullable) values.add(org.apache.flink.types.Row.of(values.size(), null));
    return BuiltinFunctionParity.environment(fields, values);
  }
}
