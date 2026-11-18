package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static tech.streamfusion.compat.FlinkTestSources.fromData;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import tech.streamfusion.planner.NativePlanner;

class FlinkFirstNSqlHarnessTest {
  @ParameterizedTest
  @CsvSource({
    "1,true,false",
    "1,false,false",
    "2,true,false",
    "2,false,false",
    "10,true,false",
    "2,true,true",
    "2,false,true"
  })
  void firstArrivalsMatchFlinkInSequence(int n, boolean rank, boolean miniBatch) throws Exception {
    String sql = query(n, rank);
    String plan = NativePlanner.explain(environment(miniBatch), sql);
    assertTrue(plan.contains(n == 1 ? "NativeDeduplicate" : "NativeFirstN"), plan);
    List<Row> host = collect(environment(miniBatch), sql);
    TableEnvironment nativeTable = environment(miniBatch);
    NativePlanner.install(nativeTable);
    assertEquals(host, collect(nativeTable, sql));
    assertEquals(n == 1 ? 3 : n == 2 ? 6 : 9, host.size());
  }

  @Test
  void singletonFirstNMatchesFlink() throws Exception {
    String sql = query(2, true).replace("PARTITION BY k ", "");
    assertTrue(NativePlanner.explain(environment(false), sql).contains("NativeFirstN"));
    List<Row> host = collect(environment(false), sql);
    TableEnvironment nativeTable = environment(false);
    NativePlanner.install(nativeTable);
    assertEquals(host, collect(nativeTable, sql));
    assertEquals(2, host.size());
  }

  @Test
  void unsupportedTimeOrdersAndOffsetStayOnHost() {
    for (String sql :
        List.of(
            query(2, true).replace("pt ASC", "pt DESC"),
            query(2, true).replace("rn <= 2", "rn BETWEEN 2 AND 3"),
            query(Integer.MAX_VALUE, true).replace("2147483647", "2147483648"))) {
      String plan = NativePlanner.explain(environment(false), sql);
      assertTrue(!plan.contains("NativeFirstN"), plan);
    }
  }

  private static String query(int n, boolean rank) {
    return "SELECT k, v"
        + (rank ? ", rn" : "")
        + " FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY k ORDER BY pt ASC) AS rn FROM src)"
        + " WHERE rn <= "
        + n;
  }

  private static TableEnvironment environment(boolean miniBatch) {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    var table = StreamTableEnvironment.create(env);
    if (miniBatch) {
      table.getConfig().set("table.exec.mini-batch.enabled", "true");
      table.getConfig().set("table.exec.mini-batch.size", "3");
      table.getConfig().set("table.exec.mini-batch.allow-latency", "1 s");
    }
    table.createTemporaryView(
        "src",
        fromData(
            env,
            Types.ROW_NAMED(new String[] {"k", "v"}, Types.LONG, Types.LONG),
            Row.of(1L, 30L),
            Row.of(null, 90L),
            Row.of(2L, 50L),
            Row.of(1L, 20L),
            Row.of(2L, null),
            Row.of(null, 80L),
            Row.of(1L, 10L),
            Row.of(2L, 40L),
            Row.of(null, 70L)),
        Schema.newBuilder()
            .column("k", DataTypes.BIGINT())
            .column("v", DataTypes.BIGINT())
            .columnByExpression("pt", "PROCTIME()")
            .build());
    return table;
  }

  private static List<Row> collect(TableEnvironment table, String sql) throws Exception {
    List<Row> rows = new ArrayList<>();
    try (CloseableIterator<Row> iterator = table.executeSql(sql).collect()) {
      iterator.forEachRemaining(rows::add);
    }
    return rows;
  }
}
