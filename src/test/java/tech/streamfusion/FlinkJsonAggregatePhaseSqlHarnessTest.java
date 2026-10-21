package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class FlinkJsonAggregatePhaseSqlHarnessTest {
  @ParameterizedTest
  @ValueSource(strings = {"NULL ON NULL", "ABSENT ON NULL"})
  void arrayWithoutMergeRemainsSinglePhaseWhenTwoPhasesAreRequested(String nulls) throws Exception {
    String sql = "SELECT g, JSON_ARRAYAGG(v " + nulls + ") FROM src GROUP BY g";
    var host = environment();
    assertFalse(host.explainSql(sql).contains("LocalGroupAggregate"));
    try (var metrics = new RecoveryExecutionMetrics()) {
      var comparison = NativeFailureParity.run(() -> environment(metrics), sql);
      comparison.assertSuccess(NativeFailureParity.Route.NATIVE);
      assertEquals(comparison.host().resultTypes(), comparison.nativeRun().resultTypes());
      assertTrue(comparison.nativeRun().plan().contains("NativeColumnarGroupAggregate"));
      assertFalse(comparison.nativeRun().plan().contains("NativeColumnarLocalGroupAggregate"));
      RecoveryExecutionMetrics.assertConsumed(
          metrics.nativeOperators(), "NativeColumnarGroupAggregate");
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"NULL ON NULL", "ABSENT ON NULL"})
  void objectWithMergeExecutesBothNativePhases(String nulls) throws Exception {
    String sql = "SELECT g, JSON_OBJECTAGG(KEY k VALUE v " + nulls + ") FROM src GROUP BY g";
    var host = environment();
    String plan = host.explainSql(sql);
    assertTrue(plan.contains("LocalGroupAggregate"), plan);
    try (var metrics = new RecoveryExecutionMetrics()) {
      var comparison = NativeFailureParity.run(() -> environment(metrics), sql);
      comparison.assertSuccess(NativeFailureParity.Route.NATIVE);
      assertEquals(comparison.host().resultTypes(), comparison.nativeRun().resultTypes());
      assertTrue(comparison.nativeRun().plan().contains("NativeColumnarLocalGroupAggregate"));
      RecoveryExecutionMetrics.assertConsumed(
          metrics.nativeOperators(), "NativeColumnarLocalGroupAggregate");
      RecoveryExecutionMetrics.assertConsumed(
          metrics.nativeOperators(), "NativeColumnarGroupAggregate");
    }
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "SELECT g, JSON_ARRAYAGG(DISTINCT v) FROM src GROUP BY g",
        "SELECT g, JSON_OBJECTAGG(DISTINCT KEY k VALUE v) FROM src GROUP BY g",
        "SELECT g, JSON_OBJECTAGG(KEY k VALUE v) WITHIN GROUP (ORDER BY k) FROM src GROUP BY g",
        "SELECT g, LISTAGG(v, ',') WITHIN GROUP (ORDER BY k) FROM src GROUP BY g"
      })
  void unsupportedReleasedSyntaxRetainsHostFailure(String sql) {
    var comparison =
        NativeFailureParity.run(FlinkJsonAggregatePhaseSqlHarnessTest::environment, sql);
    assertNotNull(comparison.host().failure());
    assertNotNull(comparison.nativeRun().failure());
    assertEquals(
        comparison.host().rootCause().getClass(), comparison.nativeRun().rootCause().getClass());
    assertEquals(comparison.host().phase(), comparison.nativeRun().phase());
  }

  private static TableEnvironment environment() {
    return environment(null);
  }

  private static TableEnvironment environment(RecoveryExecutionMetrics metrics) {
    var env =
        metrics == null
            ? StreamExecutionEnvironment.getExecutionEnvironment()
            : metrics.environment(new org.apache.flink.configuration.Configuration(), 1, 0);
    env.setParallelism(1);
    var table = StreamTableEnvironment.create(env);
    table.getConfig().set("table.optimizer.agg-phase-strategy", "TWO_PHASE");
    table.getConfig().set("table.exec.mini-batch.enabled", "true");
    table.getConfig().set("table.exec.mini-batch.allow-latency", "1 h");
    table.getConfig().set("table.exec.mini-batch.size", "7");
    var source =
        env.fromSequence(0, 23)
            .map(id -> Row.of((int) (id % 3), "k" + id, id % 5 == 0 ? null : "v" + id))
            .returns(
                Types.ROW_NAMED(
                    new String[] {"g", "k", "v"}, Types.INT, Types.STRING, Types.STRING));
    table.createTemporaryView(
        "src",
        table.fromDataStream(
            source,
            Schema.newBuilder()
                .column("g", DataTypes.INT())
                .column("k", DataTypes.STRING())
                .column("v", DataTypes.STRING())
                .build()));
    return table;
  }
}
