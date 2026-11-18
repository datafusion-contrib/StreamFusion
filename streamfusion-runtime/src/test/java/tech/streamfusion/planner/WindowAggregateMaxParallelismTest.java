package tech.streamfusion.planner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashSet;
import java.util.Set;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import tech.streamfusion.compat.StreamTestSinks;

class WindowAggregateMaxParallelismTest {
  @ParameterizedTest
  @CsvSource({"false,false", "false,true", "true,false", "true,true"})
  void platformMaximumKeepsParallelLocalAndSingletonGlobalSeparate(
      boolean grouped, boolean recoverable) {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(32);
    env.setMaxParallelism(32768);
    env.enableCheckpointing(1000);
    if (recoverable) env.getCheckpointConfig().enableUnalignedCheckpoints();
    var table = StreamTableEnvironment.create(env);
    table.getConfig().set("table.optimizer.agg-phase-strategy", "TWO_PHASE");
    NativePlanner.install(table);
    table.executeSql(
        "CREATE TABLE orders (order_id BIGINT, goods_id BIGINT, goods_sales DOUBLE, "
            + "order_time TIMESTAMP(3), "
            + "WATERMARK FOR order_time AS order_time - INTERVAL '5' SECOND) "
            + "WITH ('connector'='datagen')");
    String key = grouped ? "goods_id, " : "";
    StreamTestSinks.discard(
        table.toChangelogStream(
            table.sqlQuery(
                "SELECT " + key + "window_start, window_end, SUM(goods_sales) FROM TABLE("
                    + "CUMULATE(TABLE orders, DESCRIPTOR(order_time), INTERVAL '10' SECOND, "
                    + "INTERVAL '60' SECOND)) GROUP BY " + key + "window_start, window_end")));

    var graph = env.getStreamGraph();
    int targetMaximum = grouped ? 32768 : 1;
    Set<String> stages = new HashSet<>();
    for (var node : graph.getStreamNodes()) {
      for (var edge : node.getOutEdges()) {
        if (edge.getPartitioner() instanceof ColumnarKeyGroupPartitioner) {
          var partitioner = (ColumnarKeyGroupPartitioner) edge.getPartitioner();
          assertEquals(targetMaximum, partitioner.maxParallelism());
          assertEquals(!grouped, partitioner.requiresSingleton());
          assertEquals(recoverable, partitioner.recoverable());
          stages.add("shuffle");
        }
      }
    }
    for (var vertex : graph.getJobGraph().getVertices()) {
      if (vertex.getName().contains("NativeColumnarLocalWindowAgg")) {
        assertEquals(32, vertex.getParallelism());
        assertEquals(32768, vertex.getMaxParallelism());
        assertTrue(vertex.getName().contains("Source:"), vertex.getName());
        stages.add("local");
      }
      if (vertex.getName().contains("NativeColumnarGlobalWindowAgg")) {
        assertEquals(grouped ? 32 : 1, vertex.getParallelism());
        assertEquals(targetMaximum, vertex.getMaxParallelism());
        stages.add("global");
      }
    }
    assertEquals(Set.of("local", "shuffle", "global"), stages);
  }
}
