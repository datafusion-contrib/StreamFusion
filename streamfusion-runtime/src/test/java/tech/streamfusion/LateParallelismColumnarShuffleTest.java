package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayDeque;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.compat.CheckpointTestCapabilities;
import tech.streamfusion.compat.RuntimeCompat;
import tech.streamfusion.compat.StreamTestSinks;
import tech.streamfusion.planner.ColumnarKeyGroupPartitioner;
import tech.streamfusion.planner.NativePlanner;

class LateParallelismColumnarShuffleTest {
  private static final int ROWS = 16_384;
  private static final int KEYS = 256;
  private static final int MAX_PARALLELISM = 128;
  private static final Map<Long, long[]> VALUES = new ConcurrentHashMap<>();
  private static final Map<Long, Integer> OWNERS = new ConcurrentHashMap<>();

  @ParameterizedTest(name = "planned={0}, target={1}, producer={2}")
  @CsvSource({"1,4,1", "2,4,2", "4,3,2", "3,1,3", "1,5,2"})
  void bindsShuffleToFinalOutputParallelism(int planned, int target, int producer)
      throws Exception {
    assertJob(planned, target, producer, false, false);
  }

  @ParameterizedTest(name = "checkpoint after late 1->4, recoverable={0}")
  @ValueSource(booleans = {false, true})
  void checkpointsCorrectlyOwnedNativeStateAfterLateAdjustment(boolean recoverable)
      throws Exception {
    assertJob(1, 4, 1, true, recoverable);
  }

  @ParameterizedTest(name = "global aggregate with 256 key groups, recoverable={0}")
  @ValueSource(booleans = {false, true})
  void singletonUsesTheDownstreamStateKeyGroupCount(boolean recoverable) throws Exception {
    VALUES.clear();
    try (RecoveryExecutionMetrics metrics = new RecoveryExecutionMetrics()) {
      Configuration config = new Configuration();
      config.set(RestartStrategyOptions.RESTART_STRATEGY, "disable");
      var env = metrics.environment(config, 4, 0);
      env.setMaxParallelism(256);
      env.enableCheckpointing(50);
      env.getCheckpointConfig().setTolerableCheckpointFailureNumber(0);
      CheckpointTestCapabilities.configure(env, recoverable);
      var table = StreamTableEnvironment.create(env);
      table.getConfig().set("table.optimizer.agg-phase-strategy", "ONE_PHASE");
      NativePlanner.install(table);
      table.executeSql(
          "CREATE TABLE gen (id BIGINT) WITH ('connector'='datagen',"
              + "'number-of-rows'='"
              + ROWS
              + "','rows-per-second'='2048',"
              + "'fields.id.kind'='sequence','fields.id.start'='0',"
              + "'fields.id.end'='"
              + (ROWS - 1)
              + "')");
      StreamTestSinks.discard(
          table
              .toChangelogStream(table.sqlQuery("SELECT COUNT(*), SUM(id) FROM gen"))
              .map(new VerifyGlobalResult())
              .setParallelism(1));
      var execution = env.execute();
      String jobId = execution.getJobID().toString();
      assertEquals(ROWS, metrics.recordsIn(jobId, "NativeColumnarGroupAgg"));
      assertTrue(metrics.completedCheckpoints(jobId) > 0);
      assertEquals(ROWS, VALUES.get(0L)[0]);
      assertEquals((long) ROWS * (ROWS - 1) / 2, VALUES.get(0L)[1]);
    }
  }

  private static void assertJob(
      int planned, int target, int producer, boolean checkpointing, boolean recoverable)
      throws Exception {
    VALUES.clear();
    OWNERS.clear();
    Configuration config = new Configuration();
    config.set(RestartStrategyOptions.RESTART_STRATEGY, "disable");
    try (RecoveryExecutionMetrics metrics = new RecoveryExecutionMetrics()) {
      var env = metrics.environment(config, planned, 0);
      env.setMaxParallelism(MAX_PARALLELISM);
      if (checkpointing) {
        env.enableCheckpointing(50);
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(0);
        CheckpointTestCapabilities.configure(env, recoverable);
      }
      var table = StreamTableEnvironment.create(env);
      table.getConfig().set("table.optimizer.agg-phase-strategy", "ONE_PHASE");
      table.getConfig().set("table.exec.mini-batch.enabled", "false");
      NativePlanner.install(table);
      table.executeSql(
          "CREATE TABLE gen (id BIGINT) WITH ('connector'='datagen',"
              + "'number-of-rows'='"
              + ROWS
              + "','rows-per-second'='"
              + (checkpointing ? 4096 : 1_000_000_000)
              + "',"
              + "'fields.id.kind'='sequence','fields.id.start'='0',"
              + "'fields.id.end'='"
              + (ROWS - 1)
              + "')");
      var result =
          table.sqlQuery(
              "SELECT k, COUNT(*) AS c, SUM(id) AS s FROM "
                  + "(SELECT CAST(MOD(id, "
                  + KEYS
                  + ") AS BIGINT) AS k, id FROM gen) GROUP BY k");
      StreamTestSinks.discard(
          table.toChangelogStream(result).map(new VerifyOwnership()).name("verify-key-owner"));

      StreamGraph graph = env.getStreamGraph();
      assertTrue(graph.getStreamingPlanAsJSON().contains("NativeColumnarGroupAgg"));
      graph.getStreamNodes().forEach(node -> node.setParallelism(target));
      Set<Integer> producers = new HashSet<>();
      var pending = new ArrayDeque<Integer>();
      graph
          .getStreamNodes()
          .forEach(
              node ->
                  node.getOutEdges()
                      .forEach(
                          edge -> {
                            if (edge.getPartitioner() instanceof ColumnarKeyGroupPartitioner) {
                              assertFalse(
                                  ((ColumnarKeyGroupPartitioner) edge.getPartitioner())
                                      .requiresSingleton());
                              pending.add(node.getId());
                            }
                          }));
      assertFalse(pending.isEmpty(), "test requires an actual columnar shuffle");
      while (!pending.isEmpty()) {
        int id = pending.remove();
        if (producers.add(id)) {
          var node = graph.getStreamNode(id);
          node.setParallelism(producer);
          node.getInEdges().forEach(edge -> pending.add(edge.getSourceId()));
        }
      }

      var execution = env.execute(graph);
      assertEquals(
          ROWS, metrics.recordsIn(execution.getJobID().toString(), "NativeColumnarGroupAgg"));
      if (checkpointing) {
        assertTrue(
            metrics.completedCheckpoints(execution.getJobID().toString()) > 0,
            "native keyed state must complete a real checkpoint");
      }
      assertEquals(KEYS, VALUES.size());
      assertEquals(target, new HashSet<>(OWNERS.values()).size());
      for (long key = 0; key < KEYS; key++) {
        long count = ROWS / KEYS;
        assertEquals(count, VALUES.get(key)[0], "count for key " + key);
        assertEquals(count * key + KEYS * count * (count - 1) / 2, VALUES.get(key)[1]);
      }
    }
  }

  private static final class VerifyOwnership extends RichMapFunction<Row, Long> {
    private final RowDataSerializer keys = new RowDataSerializer(RowType.of(new BigIntType()));

    @Override
    public Long map(Row row) {
      long key = (Long) row.getField(0);
      int group =
          KeyGroupRangeAssignment.computeKeyGroupForKeyHash(
              keys.toBinaryRow(GenericRowData.of(key)).hashCode(), MAX_PARALLELISM);
      int owner = RuntimeCompat.subtask(getRuntimeContext());
      int expected =
          KeyGroupRangeAssignment.computeOperatorIndexForKeyGroup(
              MAX_PARALLELISM, RuntimeCompat.parallelism(getRuntimeContext()), group);
      if (owner != expected) {
        throw new IllegalStateException(
            "key " + key + " reached " + owner + " instead of " + expected);
      }
      Integer previous = OWNERS.putIfAbsent(key, owner);
      if (previous != null && previous != owner) {
        throw new IllegalStateException("key " + key + " reached multiple owners");
      }
      if (row.getKind() == RowKind.INSERT || row.getKind() == RowKind.UPDATE_AFTER) {
        VALUES.put(key, new long[] {(Long) row.getField(1), (Long) row.getField(2)});
      } else if (row.getKind() == RowKind.DELETE) {
        VALUES.remove(key);
      }
      return key;
    }
  }

  private static final class VerifyGlobalResult extends RichMapFunction<Row, Long> {
    @Override
    public Long map(Row row) {
      if (RuntimeCompat.parallelism(getRuntimeContext()) != 1) {
        throw new IllegalStateException("global aggregate must have exactly one owner");
      }
      if (row.getKind() == RowKind.INSERT || row.getKind() == RowKind.UPDATE_AFTER) {
        VALUES.put(0L, new long[] {(Long) row.getField(0), (Long) row.getField(1)});
      }
      return 0L;
    }
  }
}
