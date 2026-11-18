package tech.streamfusion;

import static org.apache.flink.table.api.DataTypes.*;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class FlinkDecimalTruthStringSqlHarnessTest {
  @ParameterizedTest
  @CsvSource({"1,0", "1,1", "4,2", "18,0", "18,3", "18,18", "19,0", "19,19",
      "38,0", "38,18", "38,38"})
  void characterConsumersRetainCasePaddingNullsAndDecimalTruth(int precision, int scale)
      throws Exception {
    for (boolean nullable : new boolean[] {false, true}) {
      for (boolean legacy : new boolean[] {false, true}) {
        for (String operator : List.of("CAST", "TRY_CAST")) {
          var outputs = new ArrayList<String>();
          for (String target : List.of("STRING", "VARCHAR(1)", "VARCHAR(3)", "CHAR(4)", "CHAR(8)")) {
            for (String operand : List.of("TRY_CAST(d AS BOOLEAN)", "NOT TRY_CAST(d AS BOOLEAN)",
                "TRY_CAST(d AS BOOLEAN) OR TRY_CAST(d AS BOOLEAN) IS NULL")) {
              outputs.add(operator + "(" + operand + " AS " + target + ")");
            }
          }
          String sql = "SELECT id," + String.join(",", outputs) + " FROM src";
          FlinkDecimalBooleanTryCastSqlHarnessTest.verify(
              precision, scale, nullable, legacy, sql, true);
          FlinkDecimalBooleanTryCastSqlHarnessTest.verify(
              precision, scale, nullable, legacy, sql + " WHERE id<0", true);
        }
      }
    }
  }

  @ParameterizedTest
  @CsvSource({"CAST,false", "CAST,true", "TRY_CAST,false", "TRY_CAST,true"})
  void unverifiedProducersAndIntegerConsumersKeepTheirContracts(String operator, boolean legacy)
      throws Exception {
    java.util.function.Supplier<TableEnvironment> factory = () -> environment(legacy);
    NativeParity.assertFallbackReasonContains(factory,
        "SELECT TRY_CAST(TRY_CAST(d AS BOOLEAN) AS INT) FROM src", "unsupported TRY_CAST BOOLEAN");
    String text = operator + "(TRY_CAST(d AS BOOLEAN) AS CHAR(8))";
    BuiltinFunctionParity.assertParity(factory, "SELECT id," + text + ",CHAR_LENGTH(" + text
        + "),CONCAT(" + text + ",'!') FROM src WHERE " + text + " IS NOT NULL");
    String failing = operator + "(TRY_CAST(d/(id-id) AS BOOLEAN) AS STRING)";
    for (String sql : List.of("SELECT " + failing + " FROM src WHERE id<0",
        "SELECT CASE WHEN id>=0 THEN " + text + " ELSE " + failing + " END FROM src")) {
      if (operator.equals("CAST"))
        NativeParity.assertFallbackReasonContains(factory, sql, "unsupported TRY_CAST DECIMAL");
      else BuiltinFunctionParity.assertParity(factory, sql);
    }
    NativeFailureParity.run(factory, "SELECT " + failing + " FROM src")
        .assertFailure(ArithmeticException.class, NativeFailureParity.Phase.ROW_EVALUATION,
            operator.equals("CAST") ? NativeFailureParity.Route.FALLBACK : NativeFailureParity.Route.NATIVE);
  }

  private static TableEnvironment environment(boolean legacy) {
    var table = BuiltinFunctionParity.environment(ROW(FIELD("id", INT().notNull()),
        FIELD("d", DECIMAL(18, 3))),
        List.of(Row.of(0, new BigDecimal("2.001")), Row.of(1, BigDecimal.ZERO), Row.of(2, null)));
    table.getConfig().set("table.exec.legacy-cast-behaviour", legacy ? "ENABLED" : "DISABLED");
    return table;
  }
}
