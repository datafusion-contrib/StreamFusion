package tech.streamfusion;

import static org.apache.flink.table.api.DataTypes.*;
import static tech.streamfusion.compat.FlinkTestSources.fromData;

import java.math.BigDecimal;
import java.util.Arrays;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.runtime.typeutils.ExternalTypeInfo;
import org.apache.flink.types.Row;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class FlinkDecimalAverageAdmissionSqlHarnessTest {
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void scale38RetainsTheReleasedHostPlanningFailure(boolean twoPhase) {
    for (String sql : Arrays.asList("SELECT AVG(d) FROM src", "SELECT SUM(d), AVG(d) FROM src",
        "SELECT k, COUNT(d), AVG(d), SUM(d) FROM src GROUP BY k")) {
      NativeFailureParity.run(() -> environment(38, twoPhase), sql).assertFailure(
          IllegalArgumentException.class, "Cannot convert 0 to DECIMAL(38, 38)",
          NativeFailureParity.Phase.PLANNING, NativeFailureParity.Route.FALLBACK);
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {2, 6, 37})
  void lowerScalesContinueToRunNatively(int scale) throws Exception {
    NativeParity.assertChangelogParity(() -> environment(scale, false), "SELECT AVG(d) FROM src");
    NativeParity.assertChangelogParity(() -> environment(scale, true), "SELECT k, AVG(d) FROM src GROUP BY k");
  }

  private static TableEnvironment environment(int scale, boolean twoPhase) {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    var table = StreamTableEnvironment.create(env);
    table.getConfig().set("table.optimizer.agg-phase-strategy", twoPhase ? "TWO_PHASE" : "ONE_PHASE");
    table.getConfig().set("table.exec.mini-batch.enabled", Boolean.toString(twoPhase));
    table.getConfig().set("table.exec.mini-batch.allow-latency", "1 h");
    table.getConfig().set("table.exec.mini-batch.size", "2");
    var type = ROW(FIELD("k", INT()), FIELD("d", DECIMAL(38, scale)));
    var data = fromData(env, Arrays.asList(Row.of(1, new BigDecimal("0.1")),
        Row.of(1, new BigDecimal("0.2")), Row.of(2, (Object) null)), ExternalTypeInfo.<Row>of(type));
    table.createTemporaryView("src", table.fromDataStream(data, Schema.newBuilder().fromRowDataType(type).build()));
    return table;
  }
}
