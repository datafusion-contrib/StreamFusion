package tech.streamfusion;

import static org.apache.flink.table.api.DataTypes.*;
import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.runtime.typeutils.ExternalTypeInfo;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.planner.NativePlanner;

class FlinkFixedBinaryBase64SqlHarnessTest {
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void castsPreserveActualLengthsAndLegacyBehavior(boolean legacy) throws Exception {
    String sql =
        "SELECT id, TO_BASE64(CAST(b AS BINARY(1))), TO_BASE64(CAST(b AS BINARY(4))),"
            + " TO_BASE64(TRY_CAST(b AS BINARY(4))), TO_BASE64(CAST(s AS BINARY(4))),"
            + " TO_BASE64(TRY_CAST(s AS BINARY(4))), TO_BASE64(CAST(NULL AS BINARY(2))),"
            + " TO_BASE64(X'00FF80'), CHAR_LENGTH(TO_BASE64(CAST(b AS BINARY(4)))) FROM src";
    var table = source(legacy);
    var scan = NativePlanner.install(table);
    table.explainSql(sql);
    assertTrue(
        scan.functionExecutions().stream()
            .anyMatch(route -> route.contains("TO_BASE64") && route.contains("jvm=[]")),
        scan.functionExecutions().toString());
    BuiltinFunctionParity.assertParity(() -> source(legacy), sql);
    BuiltinFunctionParity.assertParity(
        () -> source(legacy),
        "SELECT TO_BASE64(CASE WHEN id<2 THEN CAST(b AS BINARY(4))"
            + " ELSE CAST(s AS BINARY(4)) END),"
            + " TO_BASE64(COALESCE(CAST(b AS BINARY(4)),CAST(s AS BINARY(4))))"
            + " FROM src WHERE TO_BASE64(CAST(b AS BINARY(4))) <> ''");
    BuiltinFunctionParity.assertParity(
        () -> source(legacy),
        "SELECT TO_BASE64(CAST(FROM_BASE64(TO_BASE64(b)) AS BINARY(4))),"
            + " TO_BASE64(TRY_CAST(FROM_BASE64(TO_BASE64(b)) AS BINARY(4))) FROM src");
  }

  @Test
  void fixedBinarySelectionPreservesReleasedBytes() throws Exception {
    tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction("ELT");
    BuiltinFunctionParity.assertParity(
        () -> source(false),
        "SELECT TO_BASE64(ELT(MOD(id,4),CAST(b AS BINARY(4)),CAST(s AS BINARY(4)))) FROM src");
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 2, 3, 16, 256})
  void consumedFixedBinaryInputNeedsNoJvmEvaluator(int width) throws Exception {
    List<Row> rows = new ArrayList<>();
    for (int i = 0; i < 2053; i++) {
      byte[] bytes = new byte[width];
      for (int j = 0; j < width; j++) bytes[j] = (byte) (i * 131 + j * 127);
      rows.add(Row.of(i % 7 == 0 ? null : bytes));
    }
    try (var metrics = new RecoveryExecutionMetrics()) {
      var env = metrics.environment(new org.apache.flink.configuration.Configuration(), 1, 0);
      var table = StreamTableEnvironment.create(env);
      var type = ROW(FIELD("b", BINARY(width)));
      table.createTemporaryView(
          "src",
          tech.streamfusion.compat.FlinkTestSources.fromData(
              env, rows, ExternalTypeInfo.<Row>of(type)),
          Schema.newBuilder().fromRowDataType(type).build());
      var scan = NativePlanner.install(table);
      String sql =
          "SELECT TO_BASE64(b), TO_BASE64(CAST(b AS BINARY(" + (width + 1) + "))) FROM src";
      assertTrue(table.explainSql(sql).contains("NativeCalc"));
      assertTrue(
          scan.functionExecutions().stream()
              .anyMatch(route -> route.contains("TO_BASE64") && route.contains("jvm=[]")),
          scan.functionExecutions().toString());
      int count = 0;
      try (var results = table.executeSql(sql).collect()) {
        while (results.hasNext()) {
          byte[] value = (byte[]) rows.get(count).getField(0);
          String expected = value == null ? null : Base64.getEncoder().encodeToString(value);
          String widened =
              value == null
                  ? null
                  : Base64.getEncoder().encodeToString(java.util.Arrays.copyOf(value, width + 1));
          assertEquals(Row.of(expected, widened), results.next());
          count++;
        }
      }
      assertEquals(rows.size(), count);
      assertTrue(scan.fallbackReasons().isEmpty(), scan.fallbackReasons().toString());
      RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeCalc");
    }
    BuiltinFunctionParity.assertParity(
        () ->
            BuiltinFunctionParity.environment(
                ROW(FIELD("b", BINARY(width).notNull())), List.of(Row.of(new byte[width]))),
        "SELECT TO_BASE64(b) FROM src");
  }

  @Test
  void emptyBatchesAndUnselectedFailuresKeepShortCircuiting() throws Exception {
    String failing = "TO_BASE64(CAST(CAST(1/(id-id) AS STRING) AS BINARY(4)))";
    BuiltinFunctionParity.assertParity(
        () -> source(false), "SELECT " + failing + " FROM src WHERE id<0");
    BuiltinFunctionParity.assertParity(
        () -> source(false),
        "SELECT CASE WHEN id<0 THEN "
            + failing
            + " ELSE TO_BASE64(CAST(b AS BINARY(4))) END FROM src");
    NativeFailureParity.run(() -> source(false), "SELECT " + failing + " FROM src")
        .assertFailure(
            ArithmeticException.class,
            "zero",
            NativeFailureParity.Phase.ROW_EVALUATION,
            NativeFailureParity.Route.NATIVE);
  }

  private static TableEnvironment source(boolean legacy) {
    var table =
        BuiltinFunctionParity.environment(
            ROW(FIELD("id", INT()), FIELD("b", BYTES()), FIELD("s", STRING())),
            List.of(
                Row.of(0, new byte[0], ""),
                Row.of(1, new byte[] {0, (byte) 255, (byte) 128}, "a\u0000b"),
                Row.of(2, new byte[] {1}, "\u00e9\u4e2d\ud83d\ude00"),
                Row.of(3, new byte[] {1, 2, 3, 4, 5, 6, 7}, "1234567"),
                Row.of(4, null, null)));
    table.getConfig().set("table.exec.legacy-cast-behaviour", legacy ? "ENABLED" : "DISABLED");
    return table;
  }
}
