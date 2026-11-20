package tech.streamfusion;

import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.api.DataTypes.INT;
import static org.apache.flink.table.api.DataTypes.ROW;
import static org.apache.flink.table.api.DataTypes.STRING;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.compat.FlinkTestSources;
import tech.streamfusion.planner.NativePlanner;

class FlinkPairwiseStringComparisonSqlHarnessTest {
  private static final int ROWS = 4101;

  @Test
  void projectionPreservesByteAndCachedOrderingEqualityAndNulls() throws Exception {
    String sql =
        "SELECT id, s < t, s <= t, s > t, s >= t, s = t, s <> t,"
            + " CHAR_LENGTH(s) = CHAR_LENGTH(t) FROM src";
    var stock = run(sql, false);
    assertEquals(ROWS, stock.size());
    var less = List.of(true, true, false, false, false, true);
    var greater = List.of(false, false, true, true, false, true);
    var equal = List.of(false, true, false, true, true, false);
    assertEquals(greater, stock.get(0).subList(0, 6));
    assertEquals(less, stock.get(1).subList(0, 6));
    assertEquals(greater, stock.get(2).subList(0, 6));
    assertEquals(greater, stock.get(3).subList(0, 6));
    assertEquals(equal, stock.get(4).subList(0, 6));
    assertEquals(greater, stock.get(5).subList(0, 6));
    assertEquals(less, stock.get(6).subList(0, 6));
    assertEquals(Collections.nCopies(6, null), stock.get(7).subList(0, 6));
    assertEquals(equal, stock.get(8).subList(0, 6));
    assertEquals(greater, stock.get(1024).subList(0, 6));
    assertEquals(less, stock.get(2048).subList(0, 6));
    assertEquals(greater, stock.get(3072).subList(0, 6));
    assertEquals(stock, run(sql, true));
  }

  @ParameterizedTest
  @ValueSource(strings = {"CHAR_LENGTH(s) + CHAR_LENGTH(t)", "POSITION(s IN t)"})
  void filterUsesTheSamePairwiseDecision(String byteWork) throws Exception {
    String sql = "SELECT id FROM src WHERE s < t AND " + byteWork + " >= 0";
    var stock = run(sql, false);
    assertEquals(
        IntStream.range(0, ROWS)
            .filter(id ->
                (id < 1024 && (id % 9 == 1 || id % 9 == 6)) || (id >= 2048 && id < 3072))
            .boxed()
            .collect(Collectors.toList()),
        new ArrayList<>(stock.keySet()));
    assertEquals(stock, run(sql, true));
  }

  @Test
  void equalityCanReplaceTheExistingStandaloneAdapter() throws Exception {
    String sql = "SELECT id, s = t, s <> t FROM src";
    assertEquals(run(sql, false), run(sql, true));
  }

  @Test
  void standaloneOrderingKeepsTheRowPlan() throws Exception {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var table = source(metrics);
      NativePlanner.install(table);
      for (String sql : List.of("SELECT s < t FROM src", "SELECT s < t, CHAR_LENGTH(s) FROM src")) {
        String plan = table.explainSql(sql);
        assertFalse(plan.contains("RowDataToArrow"), plan);
      }
    }
  }

  @Test
  void calcThatMayDecodeAComparisonOperandDoesNotSpecialize() throws Exception {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var table = source(metrics);
      NativePlanner.install(table);
      String plan =
          table.explainSql("SELECT UPPER(s), s < t, CHAR_LENGTH(s) + CHAR_LENGTH(t) FROM src");
      assertFalse(plan.contains("pairwiseStringComparison"), plan);
    }
  }

  private static Map<Integer, List<Boolean>> run(String sql, boolean nativeEnabled)
      throws Exception {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var table = source(metrics);
      var scan = nativeEnabled ? NativePlanner.install(table) : null;
      if (scan != null) {
        String plan = table.explainSql(sql);
        assertTrue(
            plan.contains("pairwiseStringComparison=[true]"), () -> plan + scan.fallbackReasons());
        assertTrue(plan.contains("stringCaches=[true]"), plan);
        assertTrue(plan.contains("byteStrings="), plan);
      }
      Map<Integer, List<Boolean>> result = new TreeMap<>();
      try (var rows = table.toDataStream(table.sqlQuery(sql)).executeAndCollect()) {
        while (rows.hasNext()) {
          var row = rows.next();
          List<Boolean> comparisons = new ArrayList<>();
          for (int field = 1; field < row.getArity(); field++) {
            comparisons.add(row.getFieldAs(field));
          }
          result.put(row.getFieldAs(0), comparisons);
        }
      }
      if (scan != null) {
        assertEquals(0, metrics.counterValue("flinkTextRows"), sql);
        RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeCalc");
      }
      return result;
    }
  }

  private static StreamTableEnvironment source(RecoveryExecutionMetrics metrics) {
    var env = metrics.environment(new Configuration(), 1, 0);
    // Construct in the task after serialization of the source elements. Keep the freshly
    // constructed cache through the chained input edge in both the stock and native jobs.
    env.getConfig().enableObjectReuse();
    var table = StreamTableEnvironment.create(env);
    var type = ROW(FIELD("id", INT()), FIELD("s", STRING()), FIELD("t", STRING()));
    var ids = IntStream.range(0, ROWS).boxed().collect(Collectors.toList());
    var stream = FlinkTestSources.fromData(env, ids, Types.INT)
        .map(FlinkPairwiseStringComparisonSqlHarnessTest::input)
        .returns(InternalTypeInfo.of((RowType) type.getLogicalType()));
    table.createTemporaryView("src", stream, Schema.newBuilder().fromRowDataType(type).build());
    return table;
  }

  private static RowData input(int id) {
    String astral = "\ud800\udc00";
    String bmp = "\ue000";
    if (id >= 1024) {
      // Exercise consecutive batches whose sidecar schema changes with the cache distribution.
      boolean allCached = id >= 2048 && id < 3072;
      boolean disjoint = id >= 3072;
      return GenericRowData.of(
          id,
          allCached || disjoint && id % 2 == 0 ? BinaryStringData.fromString(astral) : bytes(astral),
          allCached || disjoint && id % 2 != 0 ? BinaryStringData.fromString(bmp) : bytes(bmp));
    }
    int example = id % 9;
    if (example == 4 || example == 5) {
      var invalid = BinaryStringData.fromBytes(new byte[] {(byte) 0xff});
      if (example == 4) invalid.toString();
      return GenericRowData.of(id, invalid, BinaryStringData.fromString("\ufffd"));
    }
    if (example == 6) {
      return GenericRowData.of(
          id, BinaryStringData.fromString("\ud800"), BinaryStringData.fromString("\ud801"));
    }
    if (example == 7) return GenericRowData.of(id, null, bytes(""));
    if (example == 8) return GenericRowData.of(id, bytes(""), bytes(""));
    return GenericRowData.of(
        id,
        example == 1 || example == 2 ? BinaryStringData.fromString(astral) : bytes(astral),
        example == 1 || example == 3 ? BinaryStringData.fromString(bmp) : bytes(bmp));
  }

  private static BinaryStringData bytes(String value) {
    return BinaryStringData.fromBytes(value.getBytes(StandardCharsets.UTF_8));
  }
}
