package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class FlinkFloatingDistinctPartialsSqlHarnessTest {
  @ParameterizedTest
  @ValueSource(strings = {"FLOAT", "DOUBLE"})
  void localAndGlobalDistinctArithmeticPreservesHostMergeOrder(String type) {
    try (var metrics = new RecoveryExecutionMetrics()) {
      String value = type.equals("FLOAT") ? "f" : "v";
      var comparison =
          NativeFailureParity.run(
              () -> environment(metrics),
              "SELECT k, SUM(DISTINCT "
                  + value
                  + "), AVG(DISTINCT "
                  + value
                  + "), COUNT(DISTINCT "
                  + value
                  + "), SUM(DISTINCT "
                  + value
                  + ") FILTER (WHERE keep), AVG(DISTINCT "
                  + value
                  + ") FILTER (WHERE NOT keep), AVG(v), COUNT(*) FROM src GROUP BY k");
      comparison.assertSuccess(NativeFailureParity.Route.NATIVE);
      assertEquals(comparison.host().resultTypes(), comparison.nativeRun().resultTypes());
      assertTrue(
          comparison.nativeRun().plan().contains("NativeColumnarLocalGroupAggregate"),
          comparison::toString);
      RecoveryExecutionMetrics.assertConsumed(
          metrics.nativeOperators(), "NativeColumnarLocalGroupAggregate");
      RecoveryExecutionMetrics.assertConsumed(
          metrics.nativeOperators(), "NativeColumnarGroupAggregate");
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void exceptionalValuesAndEmptyGlobalInputKeepTheReleasedResult(boolean empty) {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var comparison =
          NativeFailureParity.run(
              () -> {
                var table = environment(metrics, true);
                table.getConfig().setIdleStateRetention(java.time.Duration.ofHours(1));
                table.createTemporaryView(
                    "special", table.sqlQuery("SELECT k, v AS d, f FROM src"));
                return table;
              },
              "SELECT SUM(DISTINCT d), AVG(DISTINCT d), SUM(DISTINCT f), AVG(DISTINCT f), "
                  + "COUNT(DISTINCT d), COUNT(*) FROM special"
                  + (empty ? " WHERE k < 0" : ""));
      comparison.assertSuccess(NativeFailureParity.Route.NATIVE);
      assertEquals(comparison.host().resultTypes(), comparison.nativeRun().resultTypes());
      if (!empty) {
        RecoveryExecutionMetrics.assertConsumed(
            metrics.nativeOperators(), "NativeColumnarLocalGroupAggregate");
        RecoveryExecutionMetrics.assertConsumed(
            metrics.nativeOperators(), "NativeColumnarGroupAggregate");
      }
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"hashmap", "tech.streamfusion.state.RocksDBNativeStateBackendFactory"})
  void membershipAndFloatingPartialSumsSurviveTwoRestores(String backend) throws Exception {
    var input = new java.util.ArrayList<Row>();
    for (int id = 0; id < 96; id++) {
      Double value = id % 7 == 0 ? null : (id % 13 - 6) / 4.0;
      input.add(Row.of(id % 3, value, value == null ? null : value.floatValue()));
    }
    try (var recovery =
        new PortableSqlRecovery(
                backend,
                input,
                Types.ROW_NAMED(new String[] {"k", "d", "f"}, Types.INT, Types.DOUBLE, Types.FLOAT),
                Schema.newBuilder()
                    .column("k", DataTypes.INT())
                    .column("d", DataTypes.DOUBLE())
                    .column("f", DataTypes.FLOAT())
                    .build(),
                false,
                24,
                72)
            .withExecutionMetrics()) {
      java.util.function.UnaryOperator<TableEnvironment> configure =
          table -> {
            configure(table);
            return table;
          };
      var comparison =
          NativeFailureParity.runRecovery(
              () -> configure.apply(recovery.uninterrupted()),
              () -> configure.apply(recovery.get()),
              "SELECT k, SUM(DISTINCT d), AVG(DISTINCT d), SUM(DISTINCT f), AVG(DISTINCT f), "
                  + "COUNT(DISTINCT d), COUNT(*) FROM recovery_input GROUP BY k");
      assertNull(comparison.host().failure(), comparison::toString);
      assertNull(comparison.nativeRun().failure(), comparison::toString);
      assertEquals(
          NativeFailureParity.Route.NATIVE, comparison.nativeRun().route(), comparison::toString);
      // Checkpoints drain partial bundles and can expose additional valid intermediate updates.
      assertEquals(
          SqlAuditHarness.materialized(comparison.host().rows()),
          SqlAuditHarness.materialized(comparison.nativeRun().rows()),
          comparison::toString);
      assertEquals(comparison.host().resultTypes(), comparison.nativeRun().resultTypes());
      recovery.verifyRepeatedRecovery();
      RecoveryExecutionMetrics.assertConsumed(
          recovery.nativeExecution(), "NativeColumnarLocalGroupAggregate");
      RecoveryExecutionMetrics.assertConsumed(
          recovery.nativeExecution(), "NativeColumnarGroupAggregate");
      SharedFlinkCluster.assertNativeMemoryReleased("floating-distinct-partials-recovery");
    }
  }

  private static TableEnvironment environment(RecoveryExecutionMetrics metrics) {
    return environment(metrics, false);
  }

  private static TableEnvironment environment(RecoveryExecutionMetrics metrics, boolean special) {
    var env = metrics.environment(new Configuration(), 1, 0);
    var table = StreamTableEnvironment.create(env);
    configure(table);
    var source =
        env.fromSequence(0, 479)
            .map(
                id -> {
                  int position = (int) (id % 80);
                  double[] cancellation = {
                    0x1.0p60,
                    1.0,
                    -0x1.0p60,
                    3.0,
                    0x1.0p30,
                    -0x1.0p30,
                    -0.0,
                    0.0,
                    Double.MIN_VALUE,
                    -Double.MIN_VALUE
                  };
                  Double value =
                      position == 79
                          ? null
                          : position < cancellation.length
                              ? cancellation[position]
                              : (double) (position * 17 - 500);
                  if (special && position == 20)
                    value = Double.longBitsToDouble(0x7ff8000000000001L);
                  if (special && position == 21)
                    value = Double.longBitsToDouble(0xfff8000000000002L);
                  if (special && position == 22) value = Double.POSITIVE_INFINITY;
                  if (special && position == 23) value = Double.NEGATIVE_INFINITY;
                  return Row.of(
                      id % 7 == 0 ? null : (int) (id % 3),
                      value,
                      value == null ? null : value.floatValue(),
                      id % 5 == 0 ? null : id % 2 == 0);
                })
            .returns(
                Types.ROW_NAMED(
                    new String[] {"k", "v", "f", "keep"},
                    Types.INT,
                    Types.DOUBLE,
                    Types.FLOAT,
                    Types.BOOLEAN));
    table.createTemporaryView(
        "src",
        source,
        Schema.newBuilder()
            .column("k", DataTypes.INT())
            .column("v", DataTypes.DOUBLE())
            .column("f", DataTypes.FLOAT())
            .column("keep", DataTypes.BOOLEAN())
            .build());
    return table;
  }

  private static void configure(TableEnvironment table) {
    table.getConfig().set("table.optimizer.agg-phase-strategy", "TWO_PHASE");
    table.getConfig().set("table.exec.mini-batch.enabled", "true");
    table.getConfig().set("table.exec.mini-batch.allow-latency", "100 s");
    table.getConfig().set("table.exec.mini-batch.size", "37");
    table.getConfig().set("streamfusion.transpose.batchRows", "7");
  }
}
