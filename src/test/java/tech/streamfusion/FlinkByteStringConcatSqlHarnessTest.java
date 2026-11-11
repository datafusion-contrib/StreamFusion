package tech.streamfusion;

import static org.apache.flink.table.api.DataTypes.*;
import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;
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

  @ParameterizedTest
  @ValueSource(
      strings = {
        "SELECT CONCAT(s, '!') FROM src WHERE s = 'a'",
        "SELECT s, CONCAT(s, '!') FROM src",
        "SELECT LOWER(CONCAT(s, '!')) FROM src",
        "SELECT CONCAT(s, '!'), LOWER(t) FROM src",
        "SELECT CONCAT(s, '!'), COUNT(*) FROM src GROUP BY CONCAT(s, '!')"
      })
  void unqualifiedConsumersDoNotReceiveTheBinarySchema(String sql) throws Exception {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var table = source(metrics);
      NativePlanner.install(table);
      assertFalse(table.explainSql(sql).contains("byteStrings="));
    }
  }

  static List<String> run(String sql, boolean nativeRun) throws Exception {
    return run(sql, nativeRun, false);
  }

  static List<String> runIntegers(String sql, boolean nativeRun) throws Exception {
    return run(sql, nativeRun, true);
  }

  private static List<String> run(String sql, boolean nativeRun, boolean integer) throws Exception {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var table = source(metrics);
      var scan = nativeRun ? NativePlanner.install(table) : null;
      if (nativeRun) {
        String plan = table.explainSql(sql);
        assertTrue(plan.contains("byteStrings="), plan);
        assertTrue(plan.contains("NativeCalc"), plan);
        assertTrue(plan.contains("RowDataToArrow"), plan);
      }
      var output =
          table.<RowData>toDataStream(
              table.sqlQuery(sql),
              ROW(FIELD("v", integer ? INT() : STRING())).bridgedTo(RowData.class));
      var actual = new ArrayList<String>();
      try (var results =
          output
              .map(
                  row ->
                      row.isNullAt(0)
                          ? "NULL"
                          : integer
                              ? Integer.toString(row.getInt(0))
                              : Base64.getEncoder().encodeToString(row.getString(0).toBytes()))
              .returns(org.apache.flink.api.common.typeinfo.Types.STRING)
              .executeAndCollect()) {
        results.forEachRemaining(actual::add);
      }
      if (nativeRun) {
        assertTrue(scan.fallbackReasons().isEmpty(), scan.fallbackReasons().toString());
        assertFalse(scan.functionExecutions().isEmpty());
        assertTrue(
            scan.functionExecutions().stream().allMatch(route -> route.contains("jvm=[]")),
            scan.functionExecutions().toString());
        RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeCalc");
      }
      return actual;
    }
  }

  private static StreamTableEnvironment source(RecoveryExecutionMetrics metrics) {
    var env = metrics.environment(new org.apache.flink.configuration.Configuration(), 1, 0);
    var table = StreamTableEnvironment.create(env);
    var type =
        ROW(
            FIELD("id", INT()),
            FIELD("s", STRING()),
            FIELD("t", STRING()),
            FIELD("unused", STRING()));
    List<RowData> rows = new ArrayList<>();
    for (int i = 0; i < 2053; i++) {
      byte[] value = VALUES[i % VALUES.length];
      rows.add(
          GenericRowData.of(
              i,
              value == null ? null : BinaryStringData.fromBytes(value),
              i % 7 == 0 ? null : BinaryStringData.fromBytes(new byte[] {(byte) 0xa9}),
              BinaryStringData.fromBytes(new byte[] {(byte) 0xff})));
    }
    table.createTemporaryView(
        "src",
        tech.streamfusion.compat.FlinkTestSources.fromData(
            env, rows, InternalTypeInfo.of((RowType) type.getLogicalType())),
        Schema.newBuilder().fromRowDataType(type).build());
    return table;
  }
}
