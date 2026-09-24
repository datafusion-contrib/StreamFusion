package tech.streamfusion;

import static org.apache.flink.table.api.DataTypes.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static tech.streamfusion.compat.FlinkTestSources.fromData;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.runtime.typeutils.ExternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.planner.NativePlanner;

class FlinkNarrowGroupAggregateSqlHarnessTest {
  private static final DataType INPUT =
      ROW(FIELD("k", INT()), FIELD("b", TINYINT()), FIELD("s", SMALLINT()));

  private static final String AGGREGATES =
      "SUM(b), MIN(b), MAX(b), SUM(s), MIN(s), MAX(s), COUNT(*)";

  @ParameterizedTest
  @ValueSource(ints = {0, 2, 5})
  void narrowResultsWrapAndPreserveNullsAcrossPartialMerges(int bundleSize) throws Exception {
    assertParity(bundleSize, false, "SELECT k, " + AGGREGATES + " FROM src GROUP BY k");
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 2})
  void filtersAndGlobalAggregatesPreserveNullAndEmptyResults(int bundleSize) throws Exception {
    assertParity(
        bundleSize,
        false,
        "SELECT SUM(b) FILTER (WHERE k = 1), MIN(s) FILTER (WHERE k = 3),"
            + " MAX(b) FILTER (WHERE k = 99), SUM(s) FILTER (WHERE k = 2) FROM src");
    assertParity(bundleSize, false, "SELECT " + AGGREGATES + " FROM src WHERE k = 99");
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 2})
  void distinctNarrowSumsMergeDuplicatesAndWrap(int bundleSize) throws Exception {
    assertParity(
        bundleSize,
        false,
        "SELECT k, SUM(DISTINCT b), SUM(DISTINCT s),"
            + " SUM(DISTINCT b) FILTER (WHERE s > 0) FROM src GROUP BY k");
  }

  @Test
  void retractionsUndoOverflowAndKeepDuplicateExtremaUntilLastRemoval() throws Exception {
    assertParity(0, true, "SELECT k, " + AGGREGATES + " FROM src GROUP BY k");
    assertParity(
        0,
        true,
        "SELECT k, SUM(DISTINCT b), SUM(DISTINCT s), MIN(DISTINCT b), MAX(DISTINCT s)"
            + " FROM src GROUP BY k");
  }

  @Test
  void retractingPartialLayoutsRetainFallback() throws Exception {
    NativeParity.assertFallbackReasonContains(
        () -> environment(2, true),
        "SELECT k, " + AGGREGATES + " FROM src GROUP BY k",
        "retracting merge");
  }

  private static TableEnvironment environment(int bundleSize, boolean retracting) {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    var table = StreamTableEnvironment.create(env);
    table
        .getConfig()
        .set("table.optimizer.agg-phase-strategy", bundleSize == 0 ? "ONE_PHASE" : "TWO_PHASE");
    table.getConfig().set("table.exec.mini-batch.enabled", Boolean.toString(bundleSize > 0));
    if (bundleSize > 0) {
      table.getConfig().set("table.exec.mini-batch.allow-latency", "1 h");
      table.getConfig().set("table.exec.mini-batch.size", Integer.toString(bundleSize));
    }
    List<Row> rows =
        new ArrayList<>(
            List.of(
                row(1, 127, 32767),
                row(1, 1, 1),
                Row.of(1, null, null),
                row(2, -128, -32768),
                row(2, -1, -1),
                Row.of(3, null, null),
                Row.of(3, null, null),
                row(4, 127, 32767),
                row(4, 127, 32767),
                row(4, 1, 1)));
    if (retracting) {
      for (Row row : new ArrayList<>(rows)) {
        Row deletion = Row.copy(row);
        deletion.setKind(RowKind.DELETE);
        rows.add(deletion);
      }
      rows.add(row(1, 7, 9));
    }
    var stream = fromData(env, rows, ExternalTypeInfo.<Row>of(INPUT));
    var schema = Schema.newBuilder().fromRowDataType(INPUT).build();
    table.createTemporaryView(
        "src",
        retracting
            ? table.fromChangelogStream(stream, schema)
            : table.fromDataStream(stream, schema));
    return table;
  }

  private static Row row(int key, int tiny, int small) {
    return Row.of(key, (byte) tiny, (short) small);
  }

  private static void assertParity(int bundleSize, boolean retracting, String sql)
      throws Exception {
    var host = environment(bundleSize, retracting);
    var nativeTable = environment(bundleSize, retracting);
    var scan = NativePlanner.install(nativeTable);
    assertEquals(
        host.sqlQuery(sql).getResolvedSchema(), nativeTable.sqlQuery(sql).getResolvedSchema());
    String plan = NativePlanner.explain(environment(bundleSize, retracting), sql);
    assertTrue(plan.contains("NativeColumnarGroupAggregate"), "Missing native aggregate: " + plan);
    if (bundleSize > 0) {
      assertTrue(
          plan.contains("NativeColumnarLocalGroupAggregate"),
          "Missing native partial aggregation: " + plan);
    }
    var expected = collect(host, sql);
    var actual = collect(nativeTable, sql);
    assertTrue(scan.substitutions() > 0, "Missing native aggregate: " + scan.fallbackReasons());
    if (bundleSize > 0) {
      assertEquals(materialize(expected), materialize(actual), sql);
    } else {
      assertEquals(expected, actual, sql);
    }
  }

  private static List<Row> collect(TableEnvironment table, String sql) throws Exception {
    List<Row> rows = new ArrayList<>();
    try (var iterator = table.executeSql(sql).collect()) {
      while (iterator.hasNext()) rows.add(Row.copy(iterator.next()));
    }
    rows.sort(Comparator.comparing(Row::toString));
    return rows;
  }

  private static Map<Row, Long> materialize(List<Row> rows) {
    Map<Row, Long> result = new HashMap<>();
    for (Row row : rows) {
      long delta =
          row.getKind() == RowKind.DELETE || row.getKind() == RowKind.UPDATE_BEFORE ? -1 : 1;
      Row value = Row.copy(row);
      value.setKind(RowKind.INSERT);
      result.merge(value, delta, Long::sum);
    }
    result.values().removeIf(count -> count == 0);
    return result;
  }
}
