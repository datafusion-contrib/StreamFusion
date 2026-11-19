package tech.streamfusion;

import static org.apache.flink.table.api.DataTypes.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static tech.streamfusion.compat.FlinkTestSources.fromData;

import java.math.BigDecimal;
import java.util.ArrayList;
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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import tech.streamfusion.planner.NativePlanner;

class FlinkSharedAggregateSqlHarnessTest {
  @ParameterizedTest
  @CsvSource({"hashmap,0,BIGINT", "hashmap,2,BIGINT", "hashmap,0,DECIMAL", "hashmap,2,DECIMAL",
      "streamfusion-rocksdb,0,BIGINT", "streamfusion-rocksdb,2,BIGINT",
      "streamfusion-rocksdb,0,DECIMAL", "streamfusion-rocksdb,2,DECIMAL"})
  void repeatedRecoveryPreservesNullAndOverflowedSharedState(String backend, int bundle, String type) {
    if (backend.equals("streamfusion-rocksdb")) backend = "tech.streamfusion.state.RocksDBNativeStateBackendFactory";
    DataType rowType = ROW(FIELD("k", INT()), FIELD("v", type.equals("BIGINT") ? BIGINT() : DECIMAL(38,0)));
    Object max = type.equals("BIGINT") ? Long.valueOf(Long.MAX_VALUE)
        : new BigDecimal("99999999999999999999999999999999999999");
    Object one = type.equals("BIGINT") ? Long.valueOf(1) : BigDecimal.ONE;
    var input = List.of(Row.of(1, (Object) null), Row.of(2, (Object) null), Row.of(1, (Object) null),
        Row.of(2, (Object) null), Row.of(1, max), Row.of(1, one), Row.of(2, one),
        Row.of(2, (Object) null), Row.of(1, one), Row.of(2, one));
    try (var recovery = new PortableSqlRecovery(backend, input, ExternalTypeInfo.<Row>of(rowType),
        Schema.newBuilder().fromRowDataType(rowType).build(), false, 4, 8).withExecutionMetrics()) {
      var runs = NativeFailureParity.runRecovery(
          () -> configure(recovery.uninterrupted(), bundle),
          () -> configure(recovery.get(), bundle),
          "SELECT k, COUNT(v), AVG(v), SUM(v) FROM recovery_input GROUP BY k");
      runs.assertSuccess(NativeFailureParity.Route.NATIVE);
      assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
      recovery.verifyRepeatedRecovery();
      RecoveryExecutionMetrics.assertConsumed(recovery.nativeExecution(), "NativeColumnarGroupAggregate");
      if (bundle > 0) {
        assertTrue(runs.nativeRun().plan().contains("NativeColumnarLocalGroupAggregate"), runs.toString());
        RecoveryExecutionMetrics.assertConsumed(recovery.nativeExecution(), "NativeColumnarLocalGroupAggregate");
      }
    }
  }

  private static TableEnvironment configure(TableEnvironment table, int bundle) {
    table.getConfig().set("table.optimizer.agg-phase-strategy", bundle == 0 ? "ONE_PHASE" : "TWO_PHASE");
    table.getConfig().set("table.exec.mini-batch.enabled", Boolean.toString(bundle > 0));
    table.getConfig().set("table.exec.mini-batch.allow-latency", "1 h");
    table.getConfig().set("table.exec.mini-batch.size", Integer.toString(Math.max(1, bundle)));
    return table;
  }

  @ParameterizedTest
  @CsvSource({"INT,0", "INT,2", "BIGINT,0", "BIGINT,2", "DECIMAL,0", "DECIMAL,2", "FLOAT,0", "FLOAT,2", "DOUBLE,0", "DOUBLE,2"})
  void sharedReductionsKeepNullGroupsFiltersAndEmptyInput(String type, int bundle) throws Exception {
    for (String sql : List.of(
        "SELECT k, COUNT(v), AVG(v), SUM(v) FROM src GROUP BY k",
        "SELECT COUNT(v), AVG(v), SUM(v) FROM src",
        "SELECT k, COUNT(v) FILTER (WHERE k=1), AVG(v) FILTER (WHERE k=1),"
            + " SUM(v) FILTER (WHERE k=1) FROM src GROUP BY k",
        "SELECT COUNT(v), AVG(v), SUM(v) FROM src WHERE k=99")) {
      assertParity(type, bundle, false, false, sql);
    }
  }

  @ParameterizedTest
  @CsvSource({"INT,0", "INT,2", "BIGINT,0", "BIGINT,2", "DECIMAL,0", "DECIMAL,2", "FLOAT,0", "FLOAT,2", "DOUBLE,0", "DOUBLE,2"})
  void sharedReductionsRetractAndRecreateGroups(String type, int bundle) throws Exception {
    assertParity(type, bundle, true, false,
        "SELECT k, COUNT(v), AVG(v), SUM(v) FROM src GROUP BY k");
  }

  @ParameterizedTest
  @CsvSource({"DECIMAL,0", "DECIMAL,2", "BIGINT,0", "BIGINT,2"})
  void sharedSumsKeepExactOverflowBehavior(String type, int bundle) throws Exception {
    assertParity(type, bundle, false, true,
        "SELECT k, COUNT(v), AVG(v), SUM(v) FROM src GROUP BY k");
  }

  @ParameterizedTest
  @CsvSource({"FLOAT,0", "FLOAT,2", "DOUBLE,0", "DOUBLE,2"})
  void directFloatingZeroSumsPreserveNonNullZero(String type, int bundle) throws Exception {
    var runs = NativeFailureParity.run(() -> {
      var table = environment(type, bundle, false, false);
      var result = table.from("src")
          .groupBy(org.apache.flink.table.api.Expressions.$("k"))
          .select(org.apache.flink.table.api.Expressions.$("k"),
              org.apache.flink.table.api.Expressions.$("v").sum0().as("z"));
      table.createTemporaryView("sum_zero_result", result);
      return table;
    }, "SELECT * FROM sum_zero_result");
    runs.assertSuccess(NativeFailureParity.Route.NATIVE);
    assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
    assertEquals(2, runs.nativeRun().rows().stream()
        .filter(row -> row.get(0).equals("+I")).count());
  }

  private static void assertParity(String type, int bundle, boolean retracting,
      boolean overflow, String sql) throws Exception {
    var host = environment(type, bundle, retracting, overflow);
    var nativeTable = environment(type, bundle, retracting, overflow);
    var scan = NativePlanner.install(nativeTable);
    assertEquals(host.sqlQuery(sql).getResolvedSchema(), nativeTable.sqlQuery(sql).getResolvedSchema());
    String plan = NativePlanner.explain(environment(type, bundle, retracting, overflow), sql);
    assertTrue(plan.contains("NativeColumnarGroupAggregate"), plan);
    if (bundle > 0) assertTrue(plan.contains("NativeColumnarLocalGroupAggregate"), plan);
    assertEquals(collect(host, sql), collect(nativeTable, sql), sql);
    assertTrue(scan.substitutions() > 0, scan.fallbackReasons().toString());
  }

  private static TableEnvironment environment(String type, int bundle, boolean retracting,
      boolean overflow) {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    var table = StreamTableEnvironment.create(env);
    table.getConfig().set("table.optimizer.agg-phase-strategy", bundle == 0 ? "ONE_PHASE" : "TWO_PHASE");
    table.getConfig().set("table.exec.mini-batch.enabled", Boolean.toString(bundle > 0));
    table.getConfig().set("table.exec.mini-batch.allow-latency", "1 h");
    table.getConfig().set("table.exec.mini-batch.size", Integer.toString(Math.max(1, bundle)));
    DataType valueType = type.equals("INT") ? INT() : type.equals("BIGINT") ? BIGINT()
        : type.equals("FLOAT") ? FLOAT() : type.equals("DOUBLE") ? DOUBLE()
        : overflow ? DECIMAL(38, 0) : DECIMAL(7, 2);
    DataType rowType = ROW(FIELD("k", INT().notNull()), FIELD("v", valueType));
    Object first = type.equals("FLOAT") ? (Object) Float.valueOf(1.25f)
        : type.equals("DOUBLE") ? (Object) Double.valueOf(1.25)
        : type.equals("INT") ? Integer.valueOf(1)
        : type.equals("BIGINT") ? Long.valueOf(overflow ? Long.MAX_VALUE : 1)
        : new BigDecimal(overflow ? "99999999999999999999999999999999999999" : "1.25");
    Object second = type.equals("FLOAT") ? (Object) Float.valueOf(2.5f)
        : type.equals("DOUBLE") ? (Object) Double.valueOf(2.5)
        : type.equals("INT") ? Integer.valueOf(2)
        : type.equals("BIGINT") ? Long.valueOf(overflow ? 1 : 2)
        : new BigDecimal(overflow ? "1" : "2.50");
    List<Row> rows = new ArrayList<>(List.of(
        Row.of(1, first), Row.of(1, second), Row.of(1, (Object) null),
        Row.of(2, (Object) null), Row.of(2, (Object) null)));
    if (overflow) rows.add(Row.of(1, second));
    if (retracting) {
      for (Row row : new ArrayList<>(rows)) {
        Row deleted = Row.copy(row);
        deleted.setKind(RowKind.DELETE);
        rows.add(deleted);
      }
      rows.add(Row.of(1, second));
    }
    var stream = fromData(env, rows, ExternalTypeInfo.<Row>of(rowType));
    var schema = Schema.newBuilder().fromRowDataType(rowType).build();
    table.createTemporaryView("src", retracting
        ? table.fromChangelogStream(stream, schema) : table.fromDataStream(stream, schema));
    return table;
  }

  private static Map<Row, Long> collect(TableEnvironment table, String sql) throws Exception {
    Map<Row, Long> result = new HashMap<>();
    try (var rows = table.executeSql(sql).collect()) {
      while (rows.hasNext()) {
        Row row = Row.copy(rows.next());
        result.merge(row, 1L, Long::sum);
      }
    }
    return result;
  }
}
