package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.runtime.partitioner.RebalancePartitioner;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import tech.streamfusion.compat.StreamTestSinks;
import tech.streamfusion.operator.OrderedKeyGroupReassembler;
import tech.streamfusion.planner.ColumnarKeyGroupPartitioner;
import tech.streamfusion.planner.NativePlanner;

class SingletonColumnarWindowShuffleTest {
  private static final int ROWS = 4096;
  private static final List<Row> RESULTS = Collections.synchronizedList(new ArrayList<>());

  @ParameterizedTest(name = "CUMULATE: recoverable={0}, maxParallelism={1}")
  @CsvSource({"false,128", "false,256", "false,32768", "true,128", "true,256", "true,32768"})
  void lateAdjustmentPreservesSingletonButAllowsParallelOutput(
      boolean recoverable, int maxParallelism) throws Exception {
    List<Row> expected = new ArrayList<>();
    var start = LocalDateTime.of(1970, 1, 1, 0, 0);
    for (int seconds = 10; seconds <= 60; seconds += 10) {
      double total = 0;
      for (int id = 0; id < ROWS; id++) {
        if (id % 60 < seconds) total += id;
      }
      expected.add(Row.of(start, start.plusSeconds(seconds), total));
    }
    assertEquals(expected, runJob(false, recoverable, maxParallelism));
    assertEquals(expected, runJob(true, recoverable, maxParallelism));
  }

  private static List<Row> runJob(boolean nativeEnabled, boolean recoverable, int maxParallelism)
      throws Exception {
    RESULTS.clear();
    try (var metrics = new RecoveryExecutionMetrics()) {
      var config = new Configuration();
      config.set(RestartStrategyOptions.RESTART_STRATEGY, "disable");
      if (nativeEnabled) {
        config.setString(
            "state.backend.type", "tech.streamfusion.state.RocksDBNativeStateBackendFactory");
      }
      var env = metrics.environment(config, 2, 0);
      env.setMaxParallelism(maxParallelism);
      env.enableCheckpointing(50);
      env.getCheckpointConfig().setTolerableCheckpointFailureNumber(0);
      if (recoverable) env.getCheckpointConfig().enableUnalignedCheckpoints();
      var table = StreamTableEnvironment.create(env);
      table.getConfig().setLocalTimeZone(ZoneOffset.UTC);
      table.getConfig().set("table.optimizer.agg-phase-strategy", "TWO_PHASE");
      if (nativeEnabled) NativePlanner.install(table);
      // Keep all events behind the watermark until bounded input finishes, independent of arrival order.
      table.executeSql(
          "CREATE TABLE gen (id BIGINT, goods_sales AS CAST(id AS DOUBLE), "
              + "rt AS TO_TIMESTAMP_LTZ(MOD(id, 60) * 1000, 3), "
              + "WATERMARK FOR rt AS rt - INTERVAL '1' DAY) WITH ('connector'='datagen',"
              + "'number-of-rows'='"
              + ROWS
              + "','rows-per-second'='1024',"
              + "'fields.id.kind'='sequence','fields.id.start'='0',"
              + "'fields.id.end'='"
              + (ROWS - 1)
              + "')");
      var windows =
          table.toChangelogStream(
              table.sqlQuery(
                  "SELECT window_start, window_end, SUM(goods_sales) FROM TABLE("
                      + "CUMULATE(TABLE gen, DESCRIPTOR(rt), INTERVAL '10' SECOND, "
                      + "INTERVAL '60' SECOND)) GROUP BY window_start, window_end"));
      StreamTestSinks.discard(
          windows
              .map(new CollectResult())
              .returns(windows.getType())
              .name("collect-windows")
              .setParallelism(2));
      StreamGraph graph = env.getStreamGraph();
      if (nativeEnabled) {
        adjustNativeGraph(graph, recoverable, maxParallelism);
      }
      var execution = env.execute(graph);
      String jobId = execution.getJobID().toString();
      assertTrue(metrics.completedCheckpoints(jobId) > 0, "must complete a real checkpoint");
      if (nativeEnabled) {
        RecoveryExecutionMetrics.assertConsumed(
            metrics.nativeOperators(), "NativeColumnarLocalWindowAggregate");
        RecoveryExecutionMetrics.assertConsumed(
            metrics.nativeOperators(), "NativeColumnarGlobalWindowAggregate");
      }
      var result = new ArrayList<>(RESULTS);
      result.sort(Comparator.comparing(row -> (LocalDateTime) row.getField(1)));
      return result;
    }
  }

  private static void adjustNativeGraph(
      StreamGraph graph, boolean recoverable, int maxParallelism) {
    String plan = graph.getStreamingPlanAsJSON();
    assertTrue(plan.contains("NativeColumnarLocalWindowAgg"), plan);
    StreamNode aggregate =
        graph.getStreamNodes().stream()
            .filter(node -> node.getOperatorName().contains("NativeColumnarGlobalWindowAgg"))
            .findFirst()
            .orElseThrow();
    var exchanges = new ArrayList<StreamEdge>();
    graph.getStreamNodes().forEach(node ->
        node.getOutEdges().forEach(edge -> {
          if (edge.getPartitioner() instanceof ColumnarKeyGroupPartitioner) exchanges.add(edge);
        }));
    assertEquals(1, exchanges.size());
    var exchange = exchanges.get(0);
    var partitioner = (ColumnarKeyGroupPartitioner) exchange.getPartitioner();
    assertTrue(partitioner.requiresSingleton());
    assertEquals(1, partitioner.maxParallelism());
    assertEquals(recoverable, partitioner.recoverable());
    StreamNode receiver = graph.getStreamNode(exchange.getTargetId());
    assertEquals(1, receiver.getParallelism());
    assertEquals(1, aggregate.getParallelism());
    if (recoverable) {
      assertTrue(receiver.getOperatorFactory() instanceof SimpleOperatorFactory);
      assertTrue(
          ((SimpleOperatorFactory<?>) receiver.getOperatorFactory()).getOperator()
              instanceof OrderedKeyGroupReassembler);
      assertTrue(receiver.getId() != aggregate.getId());
    } else {
      assertEquals(aggregate.getId(), receiver.getId());
    }

    // Model a late resource adjustment, preserving both singleton receiver and stateful consumer.
    graph.getStreamNodes().forEach(node -> node.setParallelism(4));
    receiver.setParallelism(1);
    aggregate.setParallelism(1);
    aggregate.getOutEdges().forEach(edge -> edge.setPartitioner(new RebalancePartitioner<>()));
    assertEquals(4, graph.getStreamNode(exchange.getSourceId()).getParallelism());
    aggregate.getOutEdges().forEach(
        edge -> assertEquals(4, graph.getStreamNode(edge.getTargetId()).getParallelism()));
    for (var vertex : graph.getJobGraph().getVertices()) {
      if (vertex.getName().contains("NativeColumnarLocalWindowAgg")) {
        assertEquals(4, vertex.getParallelism());
        assertEquals(maxParallelism, vertex.getMaxParallelism());
      }
      if (vertex.getName().contains("NativeColumnarGlobalWindowAgg")
          || vertex.getName().contains("reassemble")) {
        assertEquals(1, vertex.getParallelism());
        assertEquals(1, vertex.getMaxParallelism());
      }
    }
  }

  private static final class CollectResult extends RichMapFunction<Row, Row> {
    @Override
    public Row map(Row row) {
      assertEquals(RowKind.INSERT, row.getKind());
      RESULTS.add(Row.copy(row));
      return row;
    }
  }
}
