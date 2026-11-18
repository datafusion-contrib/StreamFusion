package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import tech.streamfusion.compat.CheckpointTestCapabilities;
import tech.streamfusion.compat.RuntimeCompat;
import tech.streamfusion.compat.StreamTestSinks;
import tech.streamfusion.planner.ColumnarKeyGroupPartitioner;
import tech.streamfusion.planner.NativePlanner;

class SingleKeyGroupColumnarShuffleTest {
  private static final int ROWS = 1024;
  private static final List<Row> RESULTS = Collections.synchronizedList(new ArrayList<>());

  @ParameterizedTest(name = "sort: recoverable={0}, configuredMax={1}")
  @CsvSource({"false,0", "false,256", "true,0", "true,256"})
  void temporalSortKeepsItsSingleKeyGroup(boolean recoverable, int configuredMax) throws Exception {
    assertJob(true, recoverable, configuredMax);
  }

  @ParameterizedTest(name = "keyless join: recoverable={0}, configuredMax={1}")
  @CsvSource({"false,0", "false,256", "true,0", "true,256"})
  void keylessJoinKeepsItsSingleKeyGroup(boolean recoverable, int configuredMax) throws Exception {
    assertJob(false, recoverable, configuredMax);
  }

  private static void assertJob(boolean sort, boolean recoverable, int configuredMax)
      throws Exception {
    RESULTS.clear();
    try (RecoveryExecutionMetrics metrics = new RecoveryExecutionMetrics()) {
      Configuration config = new Configuration();
      config.set(RestartStrategyOptions.RESTART_STRATEGY, "disable");
      var env = metrics.environment(config, 2, 0);
      if (configuredMax > 0) env.setMaxParallelism(configuredMax);
      env.enableCheckpointing(50);
      env.getCheckpointConfig().setTolerableCheckpointFailureNumber(0);
      CheckpointTestCapabilities.configure(env, recoverable);
      var table = StreamTableEnvironment.create(env);
      NativePlanner.install(table);
      createSource(table, "left_input", ROWS, sort);
      if (!sort) createSource(table, "right_input", 3, false);
      String sql =
          sort
              ? "SELECT id FROM left_input ORDER BY rt"
              : "SELECT a.id AS left_id, b.id AS right_id "
                  + "FROM left_input a CROSS JOIN right_input b";
      StreamTestSinks.discard(
          table.toChangelogStream(table.sqlQuery(sql)).map(new CollectResult()).setParallelism(1));
      StreamGraph graph = env.getStreamGraph();
      String operator = sort ? "NativeTemporalSort" : "NativeColumnarUpdatingJoin";
      assertTrue(graph.getStreamingPlanAsJSON().contains(operator));
      long exchanges =
          graph.getStreamNodes().stream()
              .flatMap(node -> node.getOutEdges().stream())
              .filter(edge -> edge.getPartitioner() instanceof ColumnarKeyGroupPartitioner)
              .peek(
                  edge -> {
                    var target = graph.getStreamNode(edge.getTargetId());
                    assertEquals(1, target.getParallelism());
                    assertTrue(
                        ((ColumnarKeyGroupPartitioner) edge.getPartitioner()).requiresSingleton());
                    assertEquals(
                        1, ((ColumnarKeyGroupPartitioner) edge.getPartitioner()).maxParallelism());
                  })
              .count();
      assertEquals(sort ? 1 : 2, exchanges);

      var execution = env.execute(graph);
      String jobId = execution.getJobID().toString();
      assertEquals(sort ? ROWS : ROWS + 3, metrics.recordsIn(jobId, operator));
      assertTrue(metrics.completedCheckpoints(jobId) > 0, "must complete a real checkpoint");
      List<Row> expected = new ArrayList<>();
      for (long id = 0; id < ROWS; id++) {
        if (sort) {
          expected.add(Row.of(id));
        } else {
          for (long right = 0; right < 3; right++) expected.add(Row.of(id, right));
        }
      }
      if (!sort) {
        expected.sort(Comparator.comparing(Row::toString));
        RESULTS.sort(Comparator.comparing(Row::toString));
      }
      assertEquals(expected, RESULTS);
    }
  }

  private static void createSource(
      StreamTableEnvironment table, String name, int rows, boolean rowtime) {
    table.executeSql(
        "CREATE TABLE "
            + name
            + " (id BIGINT"
            + (rowtime
                ? ", rt AS TO_TIMESTAMP_LTZ(id * 1000, 3), "
                    + "WATERMARK FOR rt AS rt - INTERVAL '10' SECOND"
                : "")
            + ") WITH ('connector'='datagen','number-of-rows'='"
            + rows
            + "','rows-per-second'='512','fields.id.kind'='sequence',"
            + "'fields.id.start'='0','fields.id.end'='"
            + (rows - 1)
            + "')");
  }

  private static final class CollectResult extends RichMapFunction<Row, Row> {
    @Override
    public Row map(Row row) {
      assertEquals(1, RuntimeCompat.parallelism(getRuntimeContext()));
      assertEquals(RowKind.INSERT, row.getKind());
      RESULTS.add(Row.copy(row));
      return row;
    }
  }
}
