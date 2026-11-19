package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class FlinkDecimalAbsNegateSqlHarnessTest {
  @ParameterizedTest
  @CsvSource({"1,0", "4,2", "18,0", "18,3", "18,18", "19,0", "19,19", "38,0", "38,18", "38,38"})
  void absoluteValueConsumesVerifiedNegativeProducers(int precision, int scale) throws Exception {
    BigInteger[] values = tech.streamfusion.planner.IntegralExpressionBenchmarkFunctions
        .decimalEpochCoefficients(precision, scale).toArray(new BigInteger[0]);
    String sql = "SELECT id,ABS(-d),ABS(-(-e)),ABS(ABS(-d)),"
        + "ABS(CAST(-d AS DECIMAL(38," + scale + "))),"
        + "CAST(ABS(-d) AS STRING),ABS(-d)=ABS(e),ABS(-d) IS NULL,"
        + "CASE WHEN MOD(id,3)=0 THEN ABS(-d) ELSE ABS(-e) END FROM src";
    String arithmetic = "SELECT id,ABS(-d+e),ABS((-d)*e),ABS(-(d+e)),ABS(-d-e) FROM src";
    String epoch = "SELECT id,TO_TIMESTAMP_LTZ(ABS(-d),3),"
        + "TO_TIMESTAMP_LTZ(ABS(CAST(-e AS DECIMAL(38," + scale + "))),0) FROM src";
    for (boolean nullable : new boolean[] {false, true}) {
      for (boolean legacy : new boolean[] {false, true}) {
        for (String query : List.of(sql, epoch)) {
          FlinkDecimalBooleanTryCastSqlHarnessTest.verify(precision, scale, nullable, legacy,
              query, true, values, "UTC");
          FlinkDecimalBooleanTryCastSqlHarnessTest.verify(precision, scale, nullable, legacy,
              query + " WHERE id<0", true, values, "UTC");
        }
        BigInteger[] arithmeticValues = nullable ? values
            : new BigInteger[] {BigInteger.ZERO, BigInteger.ONE, BigInteger.ONE.negate()};
        FlinkDecimalBooleanTryCastSqlHarnessTest.verify(precision, scale, nullable, legacy,
            arithmetic, true, arithmeticValues, "UTC");
        FlinkDecimalBooleanTryCastSqlHarnessTest.verify(precision, scale, nullable, legacy,
            arithmetic + " WHERE id<0", true, values, "UTC");
      }
    }
  }

  @Test
  void nonNullableArithmeticOverflowKeepsConstraintEnforcement() throws Exception {
    for (boolean legacy : new boolean[] {false, true}) {
      for (String policy : List.of("ERROR", "DROP")) {
        java.util.function.Supplier<org.apache.flink.table.api.TableEnvironment> factory = () -> {
          var table = BuiltinFunctionParity.environment(
              DataTypes.ROW(DataTypes.FIELD("d", DataTypes.DECIMAL(38,18).notNull())),
              List.of(Row.of(new BigDecimal("99999999999999999999.999999999999999999")),
                  Row.of(BigDecimal.ZERO)));
          table.getConfig().set("table.exec.legacy-cast-behaviour",
              legacy ? "ENABLED" : "DISABLED");
          table.getConfig().set("table.exec.sink.not-null-enforcer", policy);
          return table;
        };
        String sql = "SELECT ABS((-d)*d) FROM src";
        if (policy.equals("DROP")) BuiltinFunctionParity.assertParity(factory, sql);
        else {
          var runs = NativeFailureParity.run(factory, sql);
          assertNotNull(runs.host().rootCause(), runs.toString());
          runs.assertFailure(runs.host().rootCause().getClass(), "is NOT NULL",
              NativeFailureParity.Phase.ROW_EVALUATION, NativeFailureParity.Route.NATIVE);
        }
      }
    }
  }

  @Test
  void unverifiedCastsAndFallibleInputsKeepGeneratedEvaluation() throws Exception {
    for (String sql : List.of("SELECT id,ABS(-CAST(d AS DECIMAL(18,0))) FROM src",
        "SELECT id,TO_TIMESTAMP_LTZ(ABS(-d+e),3) FROM src",
        "SELECT id,ABS(-(d/(id-id))) FROM src WHERE id<0",
        "SELECT id,CASE WHEN id>=0 THEN ABS(-d) ELSE ABS(-(d/(id-id))) END FROM src")) {
      FlinkDecimalBooleanTryCastSqlHarnessTest.verify(38, 18, true, false, sql, false);
    }
  }

  @Test
  void catalogAbsoluteValueKeepsItsImplementation() throws Exception {
    BuiltinFunctionParity.assertParity(() -> {
      var table = BuiltinFunctionParity.environment(
          DataTypes.ROW(DataTypes.FIELD("d", DataTypes.DECIMAL(18,3))),
          List.of(Row.of(new BigDecimal("-2.001")), Row.of((Object) null)));
      table.createTemporaryFunction("ABS", new CustomAbs());
      return table;
    }, "SELECT `default_catalog`.`default_database`.`ABS`(-d) FROM src");
  }

  public static class CustomAbs extends org.apache.flink.table.functions.ScalarFunction {
    public String eval(
        @org.apache.flink.table.annotation.DataTypeHint("DECIMAL(18,3)") BigDecimal value) {
      return value == null ? "custom:null" : "custom:" + value.toPlainString();
    }
  }
}
