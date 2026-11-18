package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class FlinkAggregateContextAdmissionSqlHarnessTest {
  @ParameterizedTest
  @ValueSource(
      strings = {
        "FIRST_VALUE(v), LAST_VALUE(v)",
        "FIRST_VALUE(DISTINCT v), LAST_VALUE(DISTINCT v)",
        "JSON_ARRAYAGG(v NULL ON NULL)"
      })
  void releasedUnmergeableAggregatesStayNativeUnderTwoPhasePolicy(String reductions) {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var comparison =
          NativeFailureParity.run(() -> environment(metrics), "SELECT " + reductions + " FROM src");
      comparison.assertSuccess(NativeFailureParity.Route.NATIVE);
      assertEquals(comparison.host().resultTypes(), comparison.nativeRun().resultTypes());
      assertFalse(comparison.host().plan().contains("LocalGroupAggregate"), comparison::toString);
      assertFalse(
          comparison.nativeRun().plan().contains("NativeColumnarLocalGroupAggregate"),
          comparison::toString);
      RecoveryExecutionMetrics.assertConsumed(
          metrics.nativeOperators(), "NativeColumnarGroupAggregate");
    }
  }

  private static TableEnvironment environment(RecoveryExecutionMetrics metrics) {
    var env = metrics.environment(new Configuration(), 1, 0);
    var table = StreamTableEnvironment.create(env);
    var source =
        env.fromSequence(0, 3)
            .map(id -> Row.of(new String[] {"a", "b", "a", null}[id.intValue()]))
            .returns(Types.ROW_NAMED(new String[] {"v"}, Types.STRING));
    table.createTemporaryView(
        "src", source, Schema.newBuilder().column("v", DataTypes.STRING()).build());
    table.getConfig().set("table.optimizer.agg-phase-strategy", "TWO_PHASE");
    table.getConfig().set("table.exec.mini-batch.enabled", "true");
    table.getConfig().set("table.exec.mini-batch.allow-latency", "1 h");
    table.getConfig().set("table.exec.mini-batch.size", "2");
    return table;
  }
}
