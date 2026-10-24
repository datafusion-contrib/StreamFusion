package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class FlinkCollectSqlHarnessTest {
  private static final RecoveryExecutionMetrics METRICS = new RecoveryExecutionMetrics();

  @AfterAll
  static void closeCluster() {
    METRICS.close();
  }

  private static TableEnvironment environment() {
    var env = METRICS.environment(new Configuration(), 1, 0);
    env.getConfig().enableObjectReuse();
    var table = StreamTableEnvironment.create(env);
    table.getConfig().set("table.optimizer.agg-phase-strategy", "ONE_PHASE");
    table.getConfig().set("streamfusion.transpose.batchRows", "3");
    var input =
        tech.streamfusion.compat.FlinkTestSources.fromData(
            env,
            Types.ROW_NAMED(new String[] {"k", "v"}, Types.INT, Types.LONG),
            Row.of(0, 1L),
            Row.of(0, 1L),
            Row.of(0, null),
            Row.of(1, null),
            Row.of(null, Long.MIN_VALUE),
            Row.ofKind(RowKind.UPDATE_BEFORE, 0, 1L),
            Row.ofKind(RowKind.UPDATE_AFTER, 0, Long.MAX_VALUE),
            Row.ofKind(RowKind.DELETE, 0, 1L),
            Row.ofKind(RowKind.DELETE, 1, null),
            Row.of(1, 7L));
    table.createTemporaryView(
        "src",
        table.fromChangelogStream(
            input,
            Schema.newBuilder()
                .column("k", DataTypes.INT())
                .column("v", DataTypes.BIGINT())
                .build()));
    return table;
  }

  @org.junit.jupiter.api.Test
  void unsupportedTypesAndDisabledGroupRuntimeRetainFallback() {
    NativeFailureParity.run(
            FlinkCollectSqlHarnessTest::environment,
            "SELECT k,COLLECT(CAST(v AS STRING)) FROM src GROUP BY k")
        .assertSuccess(NativeFailureParity.Route.FALLBACK);
    NativeFailureParity.run(
            FlinkCollectSqlHarnessTest::environment,
            "SELECT k,m FROM (SELECT k,m,ROW_NUMBER() OVER (ORDER BY k) AS rn FROM "
                + "(SELECT k,COLLECT(v) AS m FROM src GROUP BY k)) WHERE rn<=2")
        .assertSuccess(NativeFailureParity.Route.FALLBACK);
    NativeFailureParity.run(
            FlinkCollectSqlHarnessTest::environment,
            "SELECT CAST(k AS STRING),COLLECT(v) FROM src GROUP BY CAST(k AS STRING)")
        .assertSuccess(NativeFailureParity.Route.FALLBACK);
    String property = "streamfusion.experimental.jvmGroupAggregates";
    String previous = System.getProperty(property);
    try {
      System.setProperty(property, "false");
      NativeFailureParity.run(
              FlinkCollectSqlHarnessTest::environment, "SELECT k,COLLECT(v) FROM src GROUP BY k")
          .assertSuccess(NativeFailureParity.Route.FALLBACK);
    } finally {
      if (previous == null) System.clearProperty(property);
      else System.setProperty(property, previous);
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"hashmap", "tech.streamfusion.state.RocksDBNativeStateBackendFactory"})
  void managedCollectViewsRestoreAfterRepeatedCheckpoints(String backend) throws Exception {
    var rows = new java.util.ArrayList<Row>();
    for (int i = 0; i < 48; i++) rows.add(Row.of(i % 3, i % 5 == 0 ? null : (long) (i % 7)));
    rows.add(17, Row.ofKind(RowKind.DELETE, 0, 99L));
    for (int i = 0; i < 48; i++)
      rows.add(Row.ofKind(RowKind.DELETE, i % 3, i % 5 == 0 ? null : (long) (i % 7)));
    rows.add(70, Row.of(0, 99L));
    for (int i = 0; i < 12; i++) rows.add(Row.of(i % 3, (long) i));
    var schema =
        Schema.newBuilder().column("k", DataTypes.INT()).column("v", DataTypes.BIGINT()).build();
    try (var recovery =
        new PortableSqlRecovery(
                backend,
                rows,
                Types.ROW_NAMED(new String[] {"k", "v"}, Types.INT, Types.LONG),
                schema,
                true,
                16,
                64)
            .withExecutionMetrics()) {
      var runs =
          NativeFailureParity.runRecovery(
              () -> onePhase(recovery.uninterrupted()),
              () -> onePhase(recovery.get()),
              "SELECT k,COLLECT(v),COLLECT(DISTINCT v),COLLECT(v) FILTER (WHERE v>0),COUNT(*) FROM"
                  + " recovery_input GROUP BY k");
      runs.assertSuccess(NativeFailureParity.Route.NATIVE);
      assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
      recovery.verifyRepeatedRecovery();
      RecoveryExecutionMetrics.assertConsumed(recovery.nativeExecution(), "NativeJvmGroup");
      SharedFlinkCluster.assertNativeMemoryReleased("integer-collect-recovery");
    }
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "SELECT k,COLLECT(v) FROM src WHERE k<0 GROUP BY k",
        "SELECT COLLECT(v),COUNT(*) FROM src WHERE k<0"
      })
  void emptyInputRetainsReleasedGlobalAndGroupedResults(String sql) {
    var previous =
        METRICS.nativeOperators().stream()
            .map(row -> row.get("jobId"))
            .collect(java.util.stream.Collectors.toSet());
    var runs = NativeFailureParity.run(FlinkCollectSqlHarnessTest::environment, sql);
    runs.assertSuccess(NativeFailureParity.Route.NATIVE);
    assertTrue(runs.nativeRun().plan().contains("NativeJvmGroup"), runs::toString);
    assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
    RecoveryExecutionMetrics.assertConsumed(
        METRICS.nativeOperators().stream()
            .filter(row -> !previous.contains(row.get("jobId")))
            .collect(java.util.stream.Collectors.toList()),
        "NativeFilter");
  }

  private static TableEnvironment onePhase(TableEnvironment table) {
    table.getConfig().set("table.optimizer.agg-phase-strategy", "ONE_PHASE");
    return table;
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "SELECT k,COLLECT(v),COUNT(*) FROM src GROUP BY k",
        "SELECT COLLECT(v),COUNT(*) FROM src",
        "SELECT k,COLLECT(DISTINCT v),COLLECT(v) FILTER (WHERE v>0) FROM src GROUP BY k",
        "SELECT k,COLLECT(CAST(v AS INT)) FROM src GROUP BY k",
        "SELECT k,CAST(COLLECT(v) AS STRING) FROM src GROUP BY k"
      })
  void integerCollectRetainsReleasedCountsChangelogAndNulls(String sql) {
    var previous =
        METRICS.nativeOperators().stream()
            .map(row -> row.get("jobId"))
            .collect(java.util.stream.Collectors.toSet());
    var runs = NativeFailureParity.run(FlinkCollectSqlHarnessTest::environment, sql);
    runs.assertSuccess(NativeFailureParity.Route.NATIVE);
    assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
    assertTrue(runs.nativeRun().plan().contains("NativeJvmGroup"), runs::toString);
    RecoveryExecutionMetrics.assertConsumed(
        METRICS.nativeOperators().stream()
            .filter(row -> !previous.contains(row.get("jobId")))
            .collect(java.util.stream.Collectors.toList()),
        "NativeJvmGroup");
  }
}
