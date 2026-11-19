package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;
import static tech.streamfusion.compat.FlinkTestSources.fromData;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.planner.NativePlanner;

class FlinkDropUpdateBeforeSqlHarnessTest {
  private static final String QUERY =
      "SELECT id, e FROM src LEFT JOIN UNNEST(ARRAY[vs[1], id]) AS u(e) ON TRUE";

  @ParameterizedTest
  @ValueSource(
      strings = {
        "SELECT id, vs FROM src",
        "SELECT id, e FROM src LEFT JOIN UNNEST(ARRAY[vs[1], id]) AS u(e) ON TRUE",
        "SELECT id, e FROM src CROSS JOIN UNNEST(vs) AS u(e)",
        "SELECT id, e FROM src LEFT JOIN UNNEST(vs) AS u(e) ON TRUE",
        "SELECT id, e FROM src LEFT JOIN UNNEST(ARRAY[CAST(TO_TIMESTAMP_LTZ(id * 86400000, 3)"
            + " AS DATE), CAST(TO_TIMESTAMP_LTZ((id + 1) * 86400000, 3) AS DATE)]) AS u(e) ON TRUE"
      })
  void sinkReceivesExactRetainedChangelog(String sql) throws Exception {
    var host = execute(false, false, sql);
    var actual = execute(true, false, sql);
    assertEquals(host, actual);
    assertTrue(actual.stream().noneMatch(row -> row.get(0).equals("-U")), actual.toString());
    assertTrue(actual.stream().anyMatch(row -> row.get(0).equals("+U")), actual.toString());
    assertTrue(actual.stream().anyMatch(row -> row.get(0).equals("-D")), actual.toString());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void standaloneFilterHonorsExplicitOverride(boolean enabled) throws Exception {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var env = metrics.environment(new Configuration(), 1, 0);
      var table = StreamTableEnvironment.create(env);
      table
          .getConfig()
          .set("streamfusion.dropUpdateBefore.standalone.enabled", Boolean.toString(enabled));
      var source =
          fromData(
              env,
              Types.ROW_NAMED(
                  new String[] {"id", "vs"}, Types.LONG, Types.OBJECT_ARRAY(Types.LONG)),
              input().toArray(new Row[0]));
      table.createTemporaryView(
          "src",
          table.fromChangelogStream(source, Schema.newBuilder().build(), ChangelogMode.all()));
      var scan = NativePlanner.install(table);
      var query = table.sqlQuery("SELECT id, vs FROM src");
      table.createTemporaryTable(
          "snk",
          TableDescriptor.forConnector("blackhole")
              .schema(Schema.newBuilder().fromResolvedSchema(query.getResolvedSchema()).build())
              .build());
      String plan = table.explainSql("INSERT INTO snk SELECT id, vs FROM src");
      if (enabled) {
        assertTrue(
            scan.substitutions() > 0 && scan.fallbackReasons().isEmpty(), scan.explainSummary());
      } else {
        assertEquals(0, scan.substitutions(), plan);
        assertTrue(
            scan.fallbackReasons().stream().anyMatch(r -> r.contains("explicitly disabled")),
            scan.explainSummary());
      }
    }
  }

  @Test
  void operatorSwitchRetainsStockFallbackAndChangelog() throws Exception {
    assertEquals(execute(false, false, QUERY), execute(true, true, QUERY));
  }

  @ParameterizedTest
  @ValueSource(strings = {"hashmap", "tech.streamfusion.state.RocksDBNativeStateBackendFactory"})
  void checkpointedSourceAndSinkRetainExactChangelogAcrossTwoRestores(String backend)
      throws Exception {
    try (var recovery =
        new PortableSqlRecovery(
                backend,
                input(),
                Types.ROW_NAMED(
                    new String[] {"id", "vs"}, Types.LONG, Types.OBJECT_ARRAY(Types.LONG)),
                Schema.newBuilder().build(),
                true,
                24,
                72)
            .withExecutionMetrics()) {
      var host =
          capture(recovery.uninterrupted(), false, false, QUERY.replace("src", "recovery_input"));
      var actual = capture(recovery.get(), true, false, QUERY.replace("src", "recovery_input"));
      assertEquals(host, actual);
      recovery.verifyRepeatedRecovery();
      RecoveryExecutionMetrics.assertConsumed(recovery.nativeExecution(), "NativeDropUpdateBefore");
      RecoveryExecutionMetrics.assertConsumed(recovery.nativeExecution(), "NativeUnnest");
    }
  }

  private static List<List<Object>> execute(boolean nativeRun, boolean disabled, String sql)
      throws Exception {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var env = metrics.environment(new Configuration(), 1, 0);
      var table = StreamTableEnvironment.create(env);
      var source =
          fromData(
              env,
              Types.ROW_NAMED(
                  new String[] {"id", "vs"}, Types.LONG, Types.OBJECT_ARRAY(Types.LONG)),
              input().toArray(new Row[0]));
      table.createTemporaryView(
          "src",
          table.fromChangelogStream(source, Schema.newBuilder().build(), ChangelogMode.all()));
      if (sql.equals("SELECT id, vs FROM src")) {}
      var result = capture(table, nativeRun, disabled, sql);
      if (nativeRun && !disabled) {
        RecoveryExecutionMetrics.assertConsumed(
            metrics.nativeOperators(), "NativeDropUpdateBefore");
        if (sql.contains("UNNEST"))
          RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeUnnest");
      }
      return result;
    }
  }

  private static List<List<Object>> capture(
      TableEnvironment table, boolean nativeRun, boolean disabled, String sql) throws Exception {
    table.getConfig().set("streamfusion.transpose.batchRows", "7");
    table.getConfig().set("table.local-time-zone", "America/New_York");
    if (disabled) table.getConfig().set("streamfusion.operator.dropUpdateBefore.enabled", "false");
    var scan = nativeRun ? NativePlanner.install(table) : null;
    var query = table.sqlQuery(sql);
    table.createTemporaryTable(
        "result_sink",
        TableDescriptor.forConnector("recovery-results")
            .schema(Schema.newBuilder().fromResolvedSchema(query.getResolvedSchema()).build())
            .option("drop-update-before", "true")
            .build());
    String insert = "INSERT INTO result_sink " + sql;
    String plan = table.explainSql(insert);
    if (nativeRun && !disabled) {
      assertTrue(plan.contains("NativeDropUpdateBefore"), plan);
      assertTrue(plan.contains("RowDataToArrow") && plan.contains("ArrowToRowData"), plan);
    } else if (nativeRun) {
      assertEquals(0, scan.substitutions(), plan);
      assertTrue(
          scan.fallbackReasons().stream()
              .anyMatch(r -> r.contains("dropUpdateBefore: disabled by config")),
          scan.explainSummary());
    } else assertTrue(plan.contains("DropUpdateBefore"), plan);
    var result =
        table.executeSql(insert).getJobClient().orElseThrow().getJobExecutionResult().get();
    return RecoveryResultTableFactory.results(
        result, query.getResolvedSchema().toPhysicalRowDataType());
  }

  private static List<Row> input() {
    var rows = new ArrayList<Row>();
    for (int i = 0; i < 24; i++) {
      Long[] before = i % 5 == 0 ? null : i % 7 == 0 ? new Long[0] : new Long[] {(long) i, null};
      Long[] after = new Long[] {(long) i + 100, null, (long) i + 101};
      rows.add(Row.ofKind(RowKind.INSERT, (long) i, before));
      rows.add(Row.ofKind(RowKind.UPDATE_BEFORE, (long) i, before));
      rows.add(Row.ofKind(RowKind.UPDATE_AFTER, (long) i, after));
      rows.add(Row.ofKind(RowKind.DELETE, (long) i, after));
    }
    return rows;
  }
}
