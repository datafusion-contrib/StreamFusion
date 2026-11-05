package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import tech.streamfusion.planner.NativePlanner;

class FlinkDefaultPrecisionEpochSqlHarnessTest {
  @TestFactory
  Stream<DynamicTest> releasedOneArgumentNumericEpochsUseMilliseconds() {
    if (org.apache.flink.runtime.util.EnvironmentInformation.getVersion().startsWith("1.18.")) {
      return Stream.of(DynamicTest.dynamicTest("released single-argument signature is absent", () -> {
        for (boolean nativeRun : new boolean[] {false, true}) {
          var table = BuiltinFunctionParity.environment(
              DataTypes.ROW(DataTypes.FIELD("n", DataTypes.BIGINT())), List.of(Row.of(1L)));
          if (nativeRun) NativePlanner.install(table);
          for (String input : List.of("n", "CAST(n AS DECIMAL(38,18))")) {
            var failure = assertThrows(ValidationException.class,
                () -> table.sqlQuery("SELECT TO_TIMESTAMP_LTZ(" + input + ") FROM src"));
            assertTrue(failure.getMessage().contains("Was expecting 2 arguments"), failure.toString());
          }
        }
      }));
    }
    var cases = new ArrayList<DynamicTest>();
    for (String type : List.of("TINYINT", "SMALLINT", "INT", "BIGINT")) {
      cases.add(DynamicTest.dynamicTest(type, () -> {
        for (boolean nullable : new boolean[] {false, true}) {
          String sql = "SELECT id,TO_TIMESTAMP_LTZ(n),TO_TIMESTAMP_LTZ(n*n),"
              + "TO_TIMESTAMP_LTZ(ROUND(n,-2)),TO_TIMESTAMP_LTZ(CAST(n AS BIGINT)),"
              + "TO_TIMESTAMP_LTZ(n) IS NULL,"
              + "CASE WHEN MOD(id,3)=0 THEN TO_TIMESTAMP_LTZ(n) "
              + "ELSE CAST(NULL AS TIMESTAMP_LTZ(3)) END FROM src";
          FlinkIntegralEpochSqlHarnessTest.verify(type, nullable, "America/New_York", sql, true);
          FlinkIntegralEpochSqlHarnessTest.verify(type, nullable, "UTC", sql + " WHERE id<0", true);
          FlinkIntegralEpochSqlHarnessTest.verify(type, nullable, "UTC",
              "SELECT id,FLOOR(TO_TIMESTAMP_LTZ(n) TO HOUR),"
                  + "CEIL(TO_TIMESTAMP_LTZ(n) TO DAY) FROM src", true);
        }
      }));
    }
    for (int[] decimal : new int[][] {{1,0}, {18,3}, {18,18}, {19,0}, {38,0}, {38,18}, {38,38}}) {
      int precision = decimal[0], scale = decimal[1];
      cases.add(DynamicTest.dynamicTest("DECIMAL(" + precision + "," + scale + ")", () -> {
        var coefficients = tech.streamfusion.planner.IntegralExpressionBenchmarkFunctions
            .decimalEpochCoefficients(precision, scale).toArray(new java.math.BigInteger[0]);
        String sql = "SELECT id,TO_TIMESTAMP_LTZ(d),TO_TIMESTAMP_LTZ(-d),"
            + "TO_TIMESTAMP_LTZ(ABS(e)),TO_TIMESTAMP_LTZ(CAST(d AS DECIMAL(38," + scale
            + "))),TO_TIMESTAMP_LTZ(d) IS NULL,"
            + "CASE WHEN MOD(id,3)=0 THEN TO_TIMESTAMP_LTZ(e) "
            + "ELSE CAST(NULL AS TIMESTAMP_LTZ(3)) END FROM src";
        for (boolean nullable : new boolean[] {false, true}) {
          FlinkDecimalBooleanTryCastSqlHarnessTest.verify(precision, scale, nullable, false,
              sql, true, coefficients, "America/New_York");
          FlinkDecimalBooleanTryCastSqlHarnessTest.verify(precision, scale, nullable, true,
              sql + " WHERE id<0", true, coefficients, "UTC");
        }
      }));
    }
    cases.add(DynamicTest.dynamicTest("unverified producers and text retain generated evaluation", () -> {
      FlinkIntegralEpochSqlHarnessTest.verify("BIGINT", true, "UTC",
          "SELECT id,TO_TIMESTAMP_LTZ(n/(id-id)) FROM src WHERE id<0", false);
      FlinkDecimalBooleanTryCastSqlHarnessTest.verify(18, 3, true, false,
          "SELECT id,TO_TIMESTAMP_LTZ(d+d) FROM src", false);
      FlinkDecimalBooleanTryCastSqlHarnessTest.verify(18, 3, true, false,
          "SELECT id,TO_TIMESTAMP_LTZ(-ABS(d)) FROM src", false);
      BuiltinFunctionParity.assertParity(() -> BuiltinFunctionParity.environment(
          DataTypes.ROW(DataTypes.FIELD("s", DataTypes.STRING())),
          List.of(Row.of("2024-01-02 03:04:05"), Row.of((Object) null))),
          "SELECT TO_TIMESTAMP_LTZ(s) FROM src");
    }));
    cases.add(DynamicTest.dynamicTest("catalog functions keep their implementation", () -> {
      BuiltinFunctionParity.assertParity(() -> {
        var table = BuiltinFunctionParity.environment(
            DataTypes.ROW(DataTypes.FIELD("n", DataTypes.BIGINT())),
            List.of(Row.of(1L), Row.of((Object) null)));
        table.createTemporaryFunction("TO_TIMESTAMP_LTZ", new CustomEpoch());
        return table;
      }, "SELECT `default_catalog`.`default_database`.`TO_TIMESTAMP_LTZ`(n) FROM src");
    }));
    return cases.stream();
  }

  public static class CustomEpoch extends org.apache.flink.table.functions.ScalarFunction {
    public String eval(Long value) {
      return value == null ? "custom:null" : "custom:" + value;
    }
  }
}
