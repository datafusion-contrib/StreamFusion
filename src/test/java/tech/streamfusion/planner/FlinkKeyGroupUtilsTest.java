package tech.streamfusion.planner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static tech.streamfusion.compat.FlinkTestSources.fromData;

import java.util.List;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import tech.streamfusion.compat.ListCollectors;
import tech.streamfusion.operator.NativeColumnarGroupAggregateOperator;

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
        fromData(
            env,
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

    var graph = env.getStreamGraph();
    List<org.apache.flink.streaming.api.graph.StreamNode> nativeKeyedNodes =
        graph.getStreamNodes().stream()
            .filter(node -> node.getOperatorFactory() != null)
            .filter(
                node ->
                    node.getOperatorFactory()
                        .getStreamOperatorClass(Thread.currentThread().getContextClassLoader())
                        .equals(NativeColumnarGroupAggregateOperator.class))
            .collect(ListCollectors.toList());
    assertFalse(nativeKeyedNodes.isEmpty(), "group aggregate was not planned natively");
    var hashes =
        new org.apache.flink.streaming.api.graph.StreamGraphHasherV2()
            .traverseStreamGraphAndGenerateHashes(graph);
    var job = graph.getJobGraph();
    for (var node : nativeKeyedNodes) {
      var id = new org.apache.flink.runtime.jobgraph.OperatorID(hashes.get(node.getId()));
      var vertex =
          java.util.stream.StreamSupport.stream(job.getVertices().spliterator(), false)
              .filter(
                  candidate ->
                      candidate.getOperatorIDs().stream()
                          .anyMatch(pair -> pair.getGeneratedOperatorID().equals(id)))
              .findFirst()
              .orElseThrow();
      assertEquals(257, vertex.getMaxParallelism(), node.getOperatorName());
    }
  }
}
