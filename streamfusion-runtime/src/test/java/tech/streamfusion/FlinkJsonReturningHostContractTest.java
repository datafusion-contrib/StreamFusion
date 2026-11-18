package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static tech.streamfusion.compat.FlinkTestSources.fromData;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class FlinkJsonReturningHostContractTest {
  @ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(strings = {"BOOLEAN", "DOUBLE"})
  void integerJsonTokenFailsHostReturningConversion(String type) {
    String targetClass =
        type.equals("DOUBLE")
            ? tech.streamfusion.compat.FlinkTestCapabilities.JSON_FRACTION_CLASS
            : "java.lang.Boolean";
    TableEnvironment table = runtimeInput("{\"v\":1}");
    String expression = "JSON_VALUE(doc, '$.v' RETURNING " + type + " NULL ON ERROR)";
    String runtimeSql = "SELECT " + expression + " FROM inputs";
    assertEquals(type, table.sqlQuery(runtimeSql).getResolvedSchema()
        .getColumnDataTypes().get(0).getLogicalType().getTypeRoot().name());
    assertClassCast(table, runtimeSql, targetClass);

    TableEnvironment values = TableEnvironment.create(
        org.apache.flink.table.api.EnvironmentSettings.inStreamingMode());
    assertClassCast(values, "SELECT " + expression
        + " FROM (VALUES ('{\"v\":1}')) AS input_values(doc)", targetClass);
  }

  @ParameterizedTest
  @CsvSource({"BOOLEAN,true", "DOUBLE,1.0", "INTEGER,1"})
  void correctlyTypedJsonTokensRestoreSuccessfulNativeParity(String type, String token)
      throws Exception {
    NativeParity.assertParity(
        () -> runtimeInput("{\"v\":" + token + "}"),
        "SELECT JSON_VALUE(doc, '$.v' RETURNING " + type + " NULL ON ERROR) FROM inputs");
  }

  private static void assertClassCast(TableEnvironment table, String sql, String targetClass) {
    Exception failure = assertThrows(Exception.class, () -> {
      try (var rows = table.executeSql(sql).collect()) {
        while (rows.hasNext()) rows.next();
      }
    });
    for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
      if (cause instanceof ClassCastException) {
        assertTrue(cause.getMessage().contains("java.lang.Integer"), cause.toString());
        assertTrue(cause.getMessage().contains(targetClass), cause.toString());
        return;
      }
    }
    throw new AssertionError("Expected released Flink's scalar conversion failure", failure);
  }

  private static TableEnvironment runtimeInput(String document) {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    var table = StreamTableEnvironment.create(env);
    table.createTemporaryView(
        "inputs",
        fromData(env, Types.ROW_NAMED(new String[] {"doc"}, Types.STRING), Row.of(document)));
    return table;
  }
}
