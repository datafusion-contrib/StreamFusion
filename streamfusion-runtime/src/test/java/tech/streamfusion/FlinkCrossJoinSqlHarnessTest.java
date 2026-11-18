package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.function.Supplier;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.planner.NativePlanner;

class FlinkCrossJoinSqlHarnessTest {
  private static final String CROSS = "SELECT a.v, a.s, b.v, b.s FROM A a CROSS JOIN B b";

  @ParameterizedTest
  @CsvSource({"5003,7", "7,5003", "0,7", "7,0", "0,0"})
  void duplicatesNullsAndEmptySidesPreserveCardinality(int left, int right) throws Exception {
    assertNative(() -> environment(left, right, false), CROSS, left * right);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void residualConditionPreservesThreeValuedLogic(boolean miniBatch) throws Exception {
    int expected = 0;
    for (int left = 0; left < 5003; left++) {
      for (int right = 0; right < 7; right++) {
        if (left % 5 != 0 && right % 5 != 0 && left % 11 < right % 11) expected++;
      }
    }
    assertNative(() -> environment(5003, 7, miniBatch),
        "SELECT a.v, a.s, b.v, b.s FROM A a JOIN B b ON a.v < b.v", expected);
  }

  @Test
  void miniBatchCrossProductRetainsEveryDuplicate() throws Exception {
    assertNative(() -> environment(5003, 7, true), CROSS, 5003 * 7);
  }

  @Test
  void countRewrittenIntoUpdatingInputsFallsBack() throws Exception {
    String sql = "SELECT COUNT(*) FROM A CROSS JOIN B";
    NativeParity.assertFallbackReasonContains(() -> environment(11, 7, false), sql,
        "keyless joins require INNER and two insert-only inputs");
  }

  @Test
  void countWithResidualMatchesHost() throws Exception {
    String sql = "SELECT COUNT(*) FROM A a JOIN B b ON a.v < b.v";
    String plan = NativePlanner.explain(environment(5003, 7, false), sql);
    assertTrue(plan.contains("NativeColumnarUpdatingJoin"), plan);
    NativeParity.assertChangelogParity(() -> environment(5003, 7, false), sql);
  }

  @ParameterizedTest
  @ValueSource(strings = {"LEFT", "RIGHT", "FULL"})
  void keylessOuterJoinFallsBack(String kind) throws Exception {
    NativeParity.assertFallbackReasonContains(() -> environment(11, 7, false),
        "SELECT a.v, b.v FROM A a " + kind + " JOIN B b ON a.v < b.v",
        "keyless joins require INNER and two insert-only inputs");
  }

  @Test
  void keylessUpdatingInputFallsBack() throws Exception {
    NativeParity.assertFallbackReasonContains(() -> environment(11, 7, false),
        "SELECT a.s, a.total, b.v FROM (SELECT s, SUM(v) AS total FROM A GROUP BY s) a CROSS JOIN B b",
        "keyless joins require INNER and two insert-only inputs");
  }

  private static void assertNative(Supplier<TableEnvironment> environment, String sql, int count)
      throws Exception {
    String plan = NativePlanner.explain(environment.get(), sql);
    assertTrue(plan.contains("NativeColumnarUpdatingJoin"), plan);
    assertTrue(plan.contains("NativeColumnarExchange"), plan);
    List<Row> host = collect(environment.get(), sql);
    assertEquals(count, host.size(), "host cross-product cardinality");
    TableEnvironment nativeEnvironment = environment.get();
    var scan = NativePlanner.install(nativeEnvironment);
    List<Row> nativeRows = collect(nativeEnvironment, sql);
    assertTrue(scan.substitutions() > 0, scan.explainSummary());
    assertEquals(host, nativeRows);
  }

  private static List<Row> collect(TableEnvironment environment, String sql) throws Exception {
    List<Row> rows = new ArrayList<>();
    try (var iterator = environment.executeSql(sql).collect()) {
      while (iterator.hasNext()) {
        Row row = iterator.next();
        assertEquals(RowKind.INSERT, row.getKind());
        rows.add(row);
      }
    }
    rows.sort(Comparator.comparing(Row::toString));
    return rows;
  }

  private static TableEnvironment environment(int left, int right, boolean miniBatch) {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(2);
    var table = StreamTableEnvironment.create(env);
    if (miniBatch) {
      table.getConfig().set("table.exec.mini-batch.enabled", "true");
      table.getConfig().set("table.exec.mini-batch.allow-latency", "1 s");
      table.getConfig().set("table.exec.mini-batch.size", "127");
    }
    int[] sizes = {left, right};
    for (int side = 0; side < sizes.length; side++) {
      int size = sizes[side];
      table.createTemporaryView(side == 0 ? "A" : "B", env.fromSequence(0, Math.max(0, size - 1))
          .filter(i -> i < size)
          .map(i -> Row.of(i % 5 == 0 ? null : i % 11, i % 3 == 0 ? null : "s" + (i % 3)))
          .returns(Types.ROW_NAMED(new String[] {"v", "s"}, Types.LONG, Types.STRING)));
    }
    return table;
  }
}
