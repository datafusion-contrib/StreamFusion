package tech.streamfusion;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class FlinkIntegralTruthStringSqlHarnessTest {
  @ParameterizedTest
  @CsvSource({"TINYINT,true,false", "TINYINT,false,false", "SMALLINT,true,false",
      "SMALLINT,false,false", "INT,true,false", "INT,false,false", "BIGINT,true,false",
      "BIGINT,false,false", "TINYINT,true,true", "TINYINT,false,true", "SMALLINT,true,true",
      "SMALLINT,false,true", "INT,true,true", "INT,false,true", "BIGINT,true,true",
      "BIGINT,false,true"})
  void characterConsumersRetainWidthsCasePaddingAndNulls(
      String type, boolean nullable, boolean legacy) throws Exception {
    for (String operator : List.of("CAST", "TRY_CAST")) {
      var outputs = new ArrayList<String>();
      for (String target : List.of("STRING", "VARCHAR(1)", "VARCHAR(3)", "CHAR(4)", "CHAR(8)")) {
        for (String operand : List.of("TRY_CAST(n AS BOOLEAN)",
            "NOT TRY_CAST(n+CAST(1 AS " + type + ") AS BOOLEAN)",
            "TRY_CAST(n*n AS BOOLEAN) OR n IS NULL")) {
          outputs.add(operator + "(" + operand + " AS " + target + ")");
        }
      }
      String query = "SELECT id," + String.join(",", outputs) + " FROM src";
      FlinkIntegralEpochSqlHarnessTest.verify(type, nullable, "UTC", query, true, legacy);
      FlinkIntegralEpochSqlHarnessTest.verify(type, nullable, "UTC", query + " WHERE id<0", true, legacy);
    }
  }

  @ParameterizedTest
  @CsvSource({"CAST,false", "CAST,true", "TRY_CAST,false", "TRY_CAST,true"})
  void stringConsumersAndUnverifiedProducersKeepTheirContracts(String operator, boolean legacy)
      throws Exception {
    java.util.function.Supplier<TableEnvironment> factory = () -> environment(legacy);
    NativeParity.assertFallbackReasonContains(factory,
        "SELECT TRY_CAST(TRY_CAST(n AS BOOLEAN) AS INT) FROM src", "unsupported TRY_CAST BOOLEAN");
    String text = operator + "(TRY_CAST(n AS BOOLEAN) AS CHAR(8))";
    BuiltinFunctionParity.assertParity(factory,
        "SELECT id," + text + ",CHAR_LENGTH(" + text + "),CONCAT(" + text
            + ",'!') FROM src WHERE " + text + " IS NOT NULL");
    String failing = operator + "(TRY_CAST(n/(id-id) AS BOOLEAN) AS STRING)";
    for (String sql : List.of("SELECT " + failing + " FROM src WHERE id<0",
        "SELECT CASE WHEN id>=0 THEN " + text + " ELSE " + failing + " END FROM src")) {
      if (operator.equals("CAST"))
        NativeParity.assertFallbackReasonContains(factory, sql, "unsupported TRY_CAST");
      else BuiltinFunctionParity.assertParity(factory, sql);
    }
    NativeFailureParity.run(factory, "SELECT " + failing + " FROM src")
        .assertFailure(ArithmeticException.class, NativeFailureParity.Phase.ROW_EVALUATION,
            operator.equals("CAST") ? NativeFailureParity.Route.FALLBACK : NativeFailureParity.Route.NATIVE);
  }

  private static TableEnvironment environment(boolean legacy) {
    var table = BuiltinFunctionParity.environment(DataTypes.ROW(
        DataTypes.FIELD("id", DataTypes.INT().notNull()), DataTypes.FIELD("n", DataTypes.INT())),
        List.of(Row.of(0, 2), Row.of(1, 0), Row.of(2, null)));
    table.getConfig().set("table.exec.legacy-cast-behaviour", legacy ? "ENABLED" : "DISABLED");
    return table;
  }
}
