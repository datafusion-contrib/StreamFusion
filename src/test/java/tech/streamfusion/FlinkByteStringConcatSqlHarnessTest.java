package tech.streamfusion;

import static org.apache.flink.table.api.DataTypes.*;
import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.planner.NativePlanner;

class FlinkByteStringConcatSqlHarnessTest {
  private static final byte[][] VALUES = {
    {(byte) 0xff},
    {0x41, (byte) 0xe4},
    {(byte) 0xc3},
    {(byte) 0xa9},
    {(byte) 0xed, (byte) 0xa0, (byte) 0x80},
    {0},
    {},
    "abc".getBytes(java.nio.charset.StandardCharsets.UTF_8),
    "\u4e2d\u6587\ud83d\ude00".getBytes(java.nio.charset.StandardCharsets.UTF_8),
    null
  };

  @ParameterizedTest
  @ValueSource(
      strings = {
        "SELECT CONCAT(s, '!') AS v FROM src",
        "SELECT CONCAT(CONCAT(s, t), '!') AS v FROM src WHERE id >= 2",
        "SELECT CONCAT(s, t) AS v FROM src WHERE id = -1"
      })
  void runtimeBytesMatchReleasedFlinkThroughNativeExecution(String sql) throws Exception {
    List<String> expected = run(sql, false);
    List<String> actual = run(sql, true);
    assertEquals(expected, actual);
    if (!sql.contains("WHERE")) {
      assertEquals("/yE=", actual.get(0));
      assertEquals("QeQh", actual.get(1));
    }
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "SELECT CONCAT_WS(',', s, t) AS v FROM src",
        "SELECT CONCAT_WS(s, t, '', CAST(NULL AS STRING), '!') AS v FROM src",
        "SELECT CONCAT_WS(s) AS v FROM src",
        "SELECT CONCAT_WS(s, CAST(NULL AS STRING), CAST(NULL AS STRING)) AS v FROM src",
        "SELECT CONCAT(CONCAT_WS(s, t, ''), '!') AS v FROM src WHERE id >= 2",
        "SELECT CONCAT_WS(CONCAT(s, '?'), CONCAT_WS('!', t, s), '') AS v FROM src",
        "SELECT CONCAT_WS(s, t) AS v FROM src WHERE id = -1"
      })
  void concatWsPreservesSeparatorsAndSkipsOnlyNullValues(String sql) throws Exception {
    assertEquals(run(sql, false), run(sql, true));
  }

  @Test
  void passthroughAndConcatPreserveTheirRawBytes() throws Exception {
    String sql = "SELECT s AS original, CONCAT(s, '!') AS v FROM src WHERE id < 2051";
    List<String> expected = run(sql, Route.HOST, Result.PASSTHROUGH);
    assertEquals("/w==:/yE=", expected.get(0));
    assertEquals("QeQ=:QeQh", expected.get(1));
    assertEquals(expected, run(sql, Route.NATIVE, Result.PASSTHROUGH));
  }

  @ParameterizedTest
  @ValueSource(strings = {"id < 2049", "id < 2049 AND s IS NOT NULL"})
  void watermarkAndNumericFilterForwardBytes(String predicate) throws Exception {
    String sql = "SELECT s AS v FROM timed_src WHERE " + predicate;
    List<String> expected = run(sql, false);
    assertEquals("/w==", expected.get(0));
    assertEquals("QeQ=", expected.get(1));
    assertEquals(expected, run(sql, Route.NATIVE, Result.STRING, "NativeFilter"));
  }

  @Test
  void byteOperationsRunAfterWatermarkAssignment() throws Exception {
    String sql = "SELECT CONCAT(s, '!') AS v FROM timed_src WHERE id >= 2";
    assertEquals(run(sql, false), run(sql, true));
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "SELECT CONCAT(s, '!') FROM src WHERE s = 'a'",
        "SELECT LOWER(CONCAT(s, '!')) FROM src",
        "SELECT CONCAT(s, '!'), LOWER(t) FROM src",
        "SELECT LOWER(s) FROM timed_src",
        "SELECT CONCAT(s, '!') FROM src WHERE id = 0 "
            + "UNION ALL SELECT CAST('ok' AS STRING) FROM (VALUES (0))",
        "SELECT CONCAT(s, '!'), COUNT(*) FROM src GROUP BY CONCAT(s, '!')"
      })
  void unqualifiedConsumersKeepTheStockPlan(String sql) throws Exception {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var table = source(metrics, sql.contains("timed_src"));
      NativePlanner.install(table);
      String plan = table.explainSql(sql);
      assertFalse(plan.contains("RowDataToArrow"), plan);
    }
  }

  @Test
  void unsupportedCompositionUsesStockBeforeStringBytesEnterArrow() throws Exception {
    String sql = "SELECT LOWER(CONCAT(s, '!')) AS v FROM src WHERE id <> -1";
    assertEquals(run(sql, false), run(sql, Route.FALLBACK, Result.STRING));
  }

  @Test
  void unionAllPreservesBytesWhenBothBranchesUseBinary() throws Exception {
    String sql =
        "SELECT CONCAT(s, '!') AS v FROM src WHERE id < 100 "
            + "UNION ALL SELECT CONCAT(t, '!') AS v FROM src WHERE id < 100";
    var expected = run(sql, false);
    var actual = run(sql, Route.NATIVE, Result.STRING);
    expected.sort(String::compareTo);
    actual.sort(String::compareTo);
    assertEquals(expected, actual);
  }

  @Test
  void callbackReturningInternalStringUsesStock() throws Exception {
    String sql = "SELECT TO_BASE64(emit_bytes(id)) AS v FROM src WHERE id < 2";
    List<String> expected = run(sql, false);
    assertEquals(List.of("L3dBPQ==", "L3dFPQ=="), expected);
    assertEquals(expected, run(sql, Route.FALLBACK, Result.STRING));
  }

  @FunctionHint(
      output = @DataTypeHint(value = "STRING", bridgedTo = StringData.class))
  public static final class EmitBytes extends ScalarFunction {
    public StringData eval(Integer id) {
      return BinaryStringData.fromBytes(new byte[] {(byte) 0xff, id.byteValue()});
    }
  }

  private enum Route {
    HOST,
    NATIVE,
    FALLBACK
  }

  private enum Result {
    STRING,
    INTEGER,
    PASSTHROUGH
  }

  static List<String> run(String sql, boolean nativeRun) throws Exception {
    return run(sql, nativeRun ? Route.NATIVE : Route.HOST, Result.STRING);
  }

  static List<String> runIntegers(String sql, boolean nativeRun) throws Exception {
    return run(sql, nativeRun ? Route.NATIVE : Route.HOST, Result.INTEGER);
  }

  private static List<String> run(String sql, Route route, Result result) throws Exception {
    return run(sql, route, result, "NativeCalc");
  }

  private static List<String> run(String sql, Route route, Result result, String nativeOperator)
      throws Exception {
    try (var metrics = new RecoveryExecutionMetrics()) {
      boolean timed = sql.contains("timed_src");
      var table = source(metrics, timed);
      table.createTemporarySystemFunction("emit_bytes", EmitBytes.class);
      var scan = route == Route.HOST ? null : NativePlanner.install(table);
      if (route == Route.NATIVE) {
        String plan = table.explainSql(sql);
        assertTrue(plan.contains("byteStrings="), () -> plan + scan.fallbackReasons());
        assertTrue(plan.contains(nativeOperator), plan);
        assertTrue(plan.contains("RowDataToArrow"), plan);
        if (timed) assertTrue(plan.contains("NativeWatermarkAssigner"), plan);
        if (sql.contains("UNION ALL")) assertTrue(plan.contains("NativeUnion"), plan);
      } else if (route == Route.FALLBACK) {
        String plan = table.explainSql(sql);
        assertFalse(plan.contains("RowDataToArrow"), plan);
        assertFalse(scan.fallbackReasons().isEmpty(), plan);
      }
      var output =
          table.<RowData>toDataStream(
              table.sqlQuery(sql),
              (result == Result.PASSTHROUGH
                      ? ROW(FIELD("original", STRING()), FIELD("v", STRING()))
                      : ROW(FIELD("v", result == Result.INTEGER ? INT() : STRING())))
                  .bridgedTo(RowData.class));
      var actual = new ArrayList<String>();
      try (var results =
          output
              .map(
                  row -> {
                    if (result == Result.PASSTHROUGH)
                      return rawString(row, 0) + ":" + rawString(row, 1);
                    if (result == Result.INTEGER && !row.isNullAt(0))
                      return Integer.toString(row.getInt(0));
                    return rawString(row, 0);
                  })
              .returns(org.apache.flink.api.common.typeinfo.Types.STRING)
              .executeAndCollect()) {
        results.forEachRemaining(actual::add);
      }
      if (route == Route.NATIVE) {
        assertTrue(scan.fallbackReasons().isEmpty(), scan.fallbackReasons().toString());
        assertFalse(scan.functionExecutions().isEmpty());
        assertTrue(
            scan.functionExecutions().stream().allMatch(execution -> execution.contains("jvm=[]")),
            scan.functionExecutions().toString());
        RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), nativeOperator);
        if (timed) {
          RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeWatermarkAssigner");
        }
      }
      return actual;
    }
  }

  private static String rawString(RowData row, int field) {
    return row.isNullAt(field)
        ? "NULL"
        : Base64.getEncoder().encodeToString(row.getString(field).toBytes());
  }

  private static StreamTableEnvironment source(RecoveryExecutionMetrics metrics, boolean timed) {
    var env = metrics.environment(new org.apache.flink.configuration.Configuration(), 1, 0);
    var table = StreamTableEnvironment.create(env);
    var fields = new ArrayList<>(
        List.of(FIELD("id", INT()), FIELD("s", STRING()), FIELD("t", STRING()),
            FIELD("unused", STRING())));
    if (timed) fields.add(FIELD("rt", TIMESTAMP(3)));
    var type = ROW(fields.toArray(new org.apache.flink.table.api.DataTypes.Field[0]));
    List<RowData> rows = new ArrayList<>();
    for (int i = 0; i < 2053; i++) {
      byte[] value = VALUES[i % VALUES.length];
      var row = new GenericRowData(fields.size());
      row.setField(0, i);
      row.setField(1, value == null ? null : BinaryStringData.fromBytes(value));
      row.setField(2, i % 7 == 0 ? null : BinaryStringData.fromBytes(new byte[] {(byte) 0xa9}));
      row.setField(3, BinaryStringData.fromBytes(new byte[] {(byte) 0xff}));
      if (timed) row.setField(4, TimestampData.fromEpochMillis(1000L + i));
      rows.add(row);
    }
    var schema = Schema.newBuilder().fromRowDataType(type);
    if (timed) schema.watermark("rt", "rt - INTERVAL '1' SECOND");
    var stream = tech.streamfusion.compat.FlinkTestSources.fromData(
        env, rows, InternalTypeInfo.of((RowType) type.getLogicalType()));
    table.createTemporaryView(timed ? "timed_src" : "src", stream, schema.build());
    return table;
  }
}
