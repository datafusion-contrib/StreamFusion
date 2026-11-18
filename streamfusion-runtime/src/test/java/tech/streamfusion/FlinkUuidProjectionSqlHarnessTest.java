package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.runtime.typeutils.ExternalTypeInfo;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.planner.NativePlanner;
import tech.streamfusion.planner.PhysicalPlanScan;

class FlinkUuidProjectionSqlHarnessTest {
  @ParameterizedTest
  @ValueSource(ints = {1, 127, 1024})
  void niladicProjectionKeepsRowCountFixedCharacterTypeAndFreshValues(int batchRows)
      throws Exception {
    for (boolean nativeRun : new boolean[] {false, true}) {
      try (var metrics = new RecoveryExecutionMetrics()) {
        var table = environment(5003, batchRows, metrics);
        var scan = nativeRun ? NativePlanner.install(table) : null;
        String sql = "SELECT UUID() AS v FROM src";
        assertEquals(
            List.of(DataTypes.CHAR(36).notNull()),
            table.sqlQuery(sql).getResolvedSchema().getColumnDataTypes());
        if (nativeRun) assertNativePlan(table, sql);
        Set<String> distinct = new HashSet<>();
        int count = 0;
        try (var rows = table.executeSql(sql).collect()) {
          while (rows.hasNext()) {
            String value = (String) rows.next().getField(0);
            assertUuid(value);
            assertTrue(distinct.add(value), "UUID repeated within or across native batches");
            count++;
          }
        }
        assertEquals(5003, count);
        if (nativeRun) assertNativeWork(scan, metrics);
      }
    }
  }

  @Test
  void independentCallSitesCaseAndCoalesceRetainSelectionAndNullability() throws Exception {
    for (boolean nativeRun : new boolean[] {false, true}) {
      try (var metrics = new RecoveryExecutionMetrics()) {
        var table = environment(1031, 127, metrics);
        var scan = nativeRun ? NativePlanner.install(table) : null;
        String sentinel = "00000000-0000-4000-8000-000000000000";
        String sql =
            "SELECT id, UUID() AS a, UUID() AS b, CASE WHEN flag IS NULL THEN CAST(NULL AS"
                + " CHAR(36)) WHEN flag THEN UUID() ELSE '"
                + sentinel
                + "' END AS c, COALESCE(CASE WHEN flag THEN UUID() END, UUID()) AS d FROM src";
        var types = table.sqlQuery(sql).getResolvedSchema().getColumnDataTypes();
        assertEquals(DataTypes.CHAR(36).notNull(), types.get(1));
        assertEquals(DataTypes.CHAR(36).notNull(), types.get(2));
        assertEquals(DataTypes.CHAR(36), types.get(3));
        Set<String> values = new HashSet<>();
        int count = 0;
        try (var rows = table.executeSql(sql).collect()) {
          while (rows.hasNext()) {
            Row row = rows.next();
            long id = (Long) row.getField(0);
            for (int index : new int[] {1, 2, 4}) {
              String value = (String) row.getField(index);
              assertUuid(value);
              assertTrue(values.add(value), "Call sites must remain independently volatile");
            }
            if (id % 7 == 0) assertNull(row.getField(3));
            else if (id % 2 != 0) assertEquals(sentinel, row.getField(3));
            else {
              assertUuid((String) row.getField(3));
              assertTrue(values.add((String) row.getField(3)));
            }
            count++;
          }
        }
        assertEquals(1031, count);
        if (nativeRun) assertNativeWork(scan, metrics, false);
      }
    }
  }

  static TableEnvironment environment(int count, int batchRows, RecoveryExecutionMetrics metrics) {
    var env =
        metrics == null
            ? org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
                .getExecutionEnvironment()
            : metrics.environment(new Configuration(), 1, 0);
    env.setParallelism(1);
    var table = StreamTableEnvironment.create(env);
    table.getConfig().set("streamfusion.transpose.batchRows", Integer.toString(batchRows));
    var type =
        DataTypes.ROW(
            DataTypes.FIELD("id", DataTypes.BIGINT().notNull()),
            DataTypes.FIELD("flag", DataTypes.BOOLEAN()));
    var source =
        env.fromSequence(0, Math.max(0, count - 1))
            .filter(i -> i < count)
            .map(i -> Row.of(i, i % 7 == 0 ? null : Boolean.valueOf(i % 2 == 0)))
            .returns(ExternalTypeInfo.<Row>of(type));
    table.createTemporaryView(
        "src", table.fromDataStream(source, Schema.newBuilder().fromRowDataType(type).build()));
    return table;
  }

  static void assertUuid(String value) {
    assertNotNull(value);
    assertTrue(
        value.matches("[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}"),
        value);
    UUID parsed = UUID.fromString(value);
    assertEquals(4, parsed.version());
    assertEquals(2, parsed.variant());
  }

  private static void assertNativePlan(TableEnvironment table, String sql) {
    String plan = table.explainSql(sql);
    assertTrue(plan.contains("NativeCalc") && plan.contains("RowDataToArrow"), plan);
  }

  private static void assertNativeWork(PhysicalPlanScan scan, RecoveryExecutionMetrics metrics) {
    assertNativeWork(scan, metrics, true);
  }

  private static void assertNativeWork(
      PhysicalPlanScan scan, RecoveryExecutionMetrics metrics, boolean onlyNative) {
    assertTrue(scan.substitutions() > 0, scan.explainSummary());
    assertTrue(
        scan.functionExecutions().stream()
            .anyMatch(
                e ->
                    e.indexOf("UUID") >= 0
                        && e.indexOf("UUID") < e.indexOf(", jvm=")
                        && (!onlyNative || e.contains("jvm=[]"))),
        scan.functionExecutions().toString() + scan.explainSummary());
    var observations = metrics.nativeOperators();
    RecoveryExecutionMetrics.assertConsumed(observations, "NativeCalc");
    assertTrue(
        observations.stream()
                .filter(row -> row.get("operator").toString().startsWith("NativeCalc"))
                .mapToLong(row -> ((Number) row.get("recordsOut")).longValue())
                .sum()
            > 0,
        observations.toString());
  }
}
