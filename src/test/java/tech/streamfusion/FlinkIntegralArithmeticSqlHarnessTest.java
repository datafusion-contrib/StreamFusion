package tech.streamfusion;

import static org.apache.flink.table.api.DataTypes.*;
import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.planner.NativePlanner;

class FlinkIntegralArithmeticSqlHarnessTest {
  static java.util.stream.Stream<Arguments> functionsAndTypes() {
    return java.util.stream.Stream.of("ABS").flatMap(function ->
        java.util.stream.Stream.of("TINYINT", "SMALLINT", "INT", "BIGINT")
            .map(type -> Arguments.of(function, type)));
  }

  @ParameterizedTest
  @MethodSource("functionsAndTypes")
  void consumesWrappingArithmeticAcrossBatchesWithNoJvmBinding(String function, String type) throws Exception {
    for (boolean nullable : new boolean[] {false, true}) {
      DataType valueType = type.equals("TINYINT") ? TINYINT()
          : type.equals("SMALLINT") ? SMALLINT() : type.equals("INT") ? INT() : BIGINT();
      if (!nullable) valueType = valueType.notNull();
      DataType inputType = ROW(FIELD("id", INT().notNull()),
          FIELD("v", valueType), FIELD("w", valueType));
      List<Row> input = new ArrayList<>();
      long[] edges = {Long.MIN_VALUE, Long.MIN_VALUE + 1, -9007199254740993L,
          -129, -128, -1, 0, 1, 127, 128, 32767, 32768,
          Integer.MIN_VALUE, Integer.MAX_VALUE, 9007199254740993L, Long.MAX_VALUE};
      for (int id = 0; id < 5003; id++) {
        Object v = narrow(type, id < edges.length ? edges[id] : id * 7919L);
        Object w = narrow(type, edges[(id * 7 + 3) % edges.length]);
        input.add(Row.of(id, nullable && id % 7 == 0 ? null : v,
            nullable && id % 11 == 0 ? null : w));
      }
      String one = "CAST(1 AS " + type + ")";
      String sql = "SELECT id, ABS(v+w), ABS(v-w), ABS(v*w), ABS((v+w)*(w-" + one + ")), "
          + "ABS(ABS(v+" + one + ")-w), ABS(CAST(v+w AS BIGINT)), "
          + "CAST(ABS(v*w) AS STRING), CASE WHEN id<0 THEN ABS(v*w) ELSE ABS(v-w) END "
          + "FROM src WHERE id>=0";
      sql = sql.replace("ABS(", function + "(");
      List<Row> expected = null;
      org.apache.flink.table.catalog.ResolvedSchema expectedSchema = null;
      for (boolean nativeEnabled : new boolean[] {false, true}) {
        try (var metrics = new RecoveryExecutionMetrics()) {
          var env = metrics.environment(new org.apache.flink.configuration.Configuration(), 1, 0);
          var table = StreamTableEnvironment.create(env);
          table.createTemporaryView("src", tech.streamfusion.compat.FlinkTestSources.fromData(
              env, input, org.apache.flink.table.runtime.typeutils.ExternalTypeInfo.<Row>of(inputType)),
              Schema.newBuilder().fromRowDataType(inputType).build());
          var scan = nativeEnabled ? NativePlanner.install(table) : null;
          if (nativeEnabled) assertEquals(expectedSchema, table.sqlQuery(sql).getResolvedSchema());
          else expectedSchema = table.sqlQuery(sql).getResolvedSchema();
          if (nativeEnabled) {
            assertTrue(table.explainSql(sql).contains("NativeCalc"));
            assertFalse(scan.functionExecutions().isEmpty());
            assertTrue(scan.functionExecutions().stream().allMatch(route -> route.contains("jvm=[]")),
                scan.functionExecutions().toString());
          }
          List<Row> actual = new ArrayList<>();
          boolean[] seen = new boolean[input.size()];
          try (var iterator = table.executeSql(sql).collect()) {
            while (iterator.hasNext()) {
              Row row = iterator.next();
              int id = (int) row.getField(0);
              assertTrue(id >= 0 && id < seen.length);
              assertFalse(seen[id], "Duplicate input ID " + id);
              seen[id] = true;
              actual.add(row);
            }
          }
          assertEquals(input.size(), actual.size());
          actual.sort(java.util.Comparator.comparingInt(row -> (int) row.getField(0)));
          if (nativeEnabled) {
            assertEquals(expected, actual, type + " nullable=" + nullable);
            assertTrue(scan.fallbackReasons().isEmpty(), scan.fallbackReasons().toString());
            RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeCalc");
          } else expected = actual;
        }
      }
    }
  }

  private static Object narrow(String type, long value) {
    switch (type) {
      case "TINYINT": return (byte) value;
      case "SMALLINT": return (short) value;
      case "INT": return (int) value;
      case "BIGINT": return value;
      default: throw new IllegalArgumentException(type);
    }
  }
}
