package tech.streamfusion.planner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import tech.streamfusion.operator.NativeColumnarGroupAggregateOperator;
import java.util.List;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

class FlinkKeyGroupUtilsTest {

  @Test
  void honorsProgramWideMaxParallelism() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setMaxParallelism(257);

    assertEquals(257, FlinkKeyGroupUtils.maxParallelism(env, 2));
    assertEquals(257, FlinkKeyGroupUtils.maxParallelism(env, 300));
  }

  @Test
  void usesFlinkDefaultWhenMaxParallelismIsUnset() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    assertEquals(
        KeyGroupRangeAssignment.computeDefaultMaxParallelism(4),
        FlinkKeyGroupUtils.maxParallelism(env, 4));
  }

  @Test
  void nativeSqlPlanKeepsConfiguredMaxParallelism() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(2);
    env.setMaxParallelism(257);
    StreamTableEnvironment table = StreamTableEnvironment.create(env);
    table.createTemporaryView(
        "src",
        env.fromData(
            Types.ROW_NAMED(new String[] {"k", "v"}, Types.LONG, Types.LONG),
            Row.of(1L, 1L),
            Row.of(1L, 2L),
            Row.of(2L, 3L)),
        Schema.newBuilder()
            .column("k", DataTypes.BIGINT())
            .column("v", DataTypes.BIGINT())
            .build());
    NativePlanner.install(table);
    table.toChangelogStream(table.sqlQuery("SELECT k, SUM(v) FROM src GROUP BY k"));

    List<org.apache.flink.streaming.api.graph.StreamNode> nativeKeyedNodes =
        env.getStreamGraph().getStreamNodes().stream()
            .filter(node -> node.getOperatorFactory() != null)
            .filter(
                node ->
                    node.getOperatorFactory()
                        .getStreamOperatorClass(Thread.currentThread().getContextClassLoader())
                        .equals(NativeColumnarGroupAggregateOperator.class))
            .toList();
    assertFalse(nativeKeyedNodes.isEmpty(), "group aggregate was not planned natively");
    for (var node : nativeKeyedNodes) {
      assertEquals(257, node.getMaxParallelism(), node.getOperatorName());
    }
  }
}
