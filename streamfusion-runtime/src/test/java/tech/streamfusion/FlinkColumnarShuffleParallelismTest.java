package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import tech.streamfusion.operator.ArrowBatchHandles;
import tech.streamfusion.operator.BatchCoalescer;
import java.time.ZoneId;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

/**
 * Exercises the columnar keyed shuffle across multiple channels end to end at parallelism 2. The
 * input is written as several Parquet files; Flink's source shards them across two subtasks and
 * transposes each source stream at the native boundary. A keyed window runs at parallelism 2, so
 * the columnar exchange splits each batch by key and routes the sub-batches to two downstream
 * window subtasks. With eight keys spread across both channels, this is the real cross-channel
 * routing the p=1 pipeline never reaches — and it must still match the host.
 */
class FlinkColumnarShuffleParallelismTest {

  private static final String WINDOW_QUERY =
      "SELECT k, window_start, window_end, SUM(v) AS total "
          + "FROM TABLE(TUMBLE(TABLE t, DESCRIPTOR(rt), INTERVAL '1' SECOND)) "
          + "GROUP BY k, window_start, window_end";

  @Test
  void keyedWindowAtParallelismTwoMatchesHost() throws Exception {
    Path input = Files.createTempDirectory("cshuffle-p2-in");
    writeInput(input);
    long registeredBefore = ArrowBatchHandles.registered();
    NativeParity.assertParity(() -> readEnvironment(input, "ONE_PHASE", false), WINDOW_QUERY);
    // Local execution must take the zero-copy shuffle: batches were parked in the handle table
    // (the exchange really moved ownership, not IPC bytes) and every one was claimed back.
    assertTrue(ArrowBatchHandles.registered() > registeredBefore);
    assertEquals(0, ArrowBatchHandles.inFlight());
  }

  /**
   * The two-phase shape at parallelism 2: the local window aggregate is chained upstream of the
   * exchange, so its keyed context cannot come from a batch destination (pre-shuffle batches carry
   * none) and its buffered slices cannot survive a barrier (pre-shuffle keys span all key groups).
   * Checkpointing is on so barrier drains actually fire mid-stream on every subtask.
   */
  @Test
  void twoPhaseWindowAggAtParallelismTwoMatchesHost() throws Exception {
    Path input = Files.createTempDirectory("cshuffle-p2-2phase-in");
    writeInput(input);
    NativeParity.assertParity(() -> readEnvironment(input, "TWO_PHASE", true), WINDOW_QUERY);
  }

  @Test
  void plannerSelectedUnalignedShuffleMatchesHostAtParallelismTwo() throws Exception {
    Path input = Files.createTempDirectory("cshuffle-p2-unaligned-in");
    writeInput(input);
    NativeParity.assertParity(() -> readUnalignedEnvironment(input), WINDOW_QUERY);
  }

  /**
   * A changelog aggregate at parallelism 2: the keyed exchange fragments every source batch into
   * per-channel sub-batches, and the post-exchange coalescer must reassemble processing-sized
   * batches without changing the per-record changelog. No watermark is declared and the latency
   * backstop is parked, so small batches accumulate until the row target or end of input.
   * The engagement counter must observe an actual merge. The input exceeds several 1,024-row transpose batches even if one source subtask
   * reads every file; file boundaries alone do not flush the transpose.
   * {@code COUNT(*)} keeps the intermediate changelog order-insensitive: at parallelism 2 the two
   * source subtasks race, so a value-accumulating aggregate's intermediates differ run to run in
   * both engines.
   */
  @Test
  void changelogAggregateAtParallelismTwoCoalescesSubBatches() throws Exception {
    Path input = Files.createTempDirectory("cshuffle-p2-agg-in");
    writeInput(input, 8192);
    String latencyBefore =
        System.setProperty("streamfusion.exchange.coalesceLatencyMs", "600000");
    try {
      long mergedBefore = BatchCoalescer.merged();
      NativeParity.assertParity(
          () -> readChangelogEnvironment(input), "SELECT k, COUNT(*) AS total FROM t GROUP BY k");
      assertTrue(
          BatchCoalescer.merged() > mergedBefore,
          "the native aggregate must merge multiple shuffle batches");
    } finally {
      if (latencyBefore == null) {
        System.clearProperty("streamfusion.exchange.coalesceLatencyMs");
      } else {
        System.setProperty("streamfusion.exchange.coalesceLatencyMs", latencyBefore);
      }
    }
  }

  private static TableEnvironment readChangelogEnvironment(Path directory) {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(2);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    tEnv.getConfig().setLocalTimeZone(ZoneId.of("UTC"));
    tEnv.executeSql(
        "CREATE TABLE t (k BIGINT, v BIGINT) WITH ('connector' = 'filesystem', 'path' = '"
            + directory.toUri()
            + "', 'format' = 'parquet')");
    return tEnv;
  }

  private static TableEnvironment readUnalignedEnvironment(Path directory) {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(2);
    env.enableCheckpointing(100);
    env.getCheckpointConfig().enableUnalignedCheckpoints();
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    tEnv.getConfig().setLocalTimeZone(ZoneId.of("UTC"));
    tEnv.getConfig().set("table.optimizer.agg-phase-strategy", "ONE_PHASE");
    tEnv.executeSql(
        "CREATE TABLE t (k BIGINT, v BIGINT, rt TIMESTAMP_LTZ(3), "
            + "WATERMARK FOR rt AS rt - INTERVAL '10' SECOND) WITH ('connector' = 'filesystem', "
            + "'path' = '"
            + directory.toUri()
            + "', 'format' = 'parquet')");
    return tEnv;
  }

  private static void writeInput(Path directory) throws Exception {
    writeInput(directory, 48);
  }

  /** Writes two independent halves so the parallel reader has multiple files available. */
  private static void writeInput(Path directory, int rows) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(2);
    env.enableCheckpointing(100);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    tEnv.getConfig().setLocalTimeZone(ZoneId.of("UTC"));
    // Eight keys; the default 48-row fixture spans a few 1-second windows.
    DataStream<Row> source =
        env.fromSequence(0, rows - 1L)
            .map(
                i ->
                    Row.of(
                        i % 8,
                        i,
                        Instant.ofEpochMilli(1704067200000L + (i / 8) * 1000L + (i % 8) * 100L)))
            .returns(Types.ROW_NAMED(new String[] {"k", "v", "rt"}, Types.LONG, Types.LONG, Types.INSTANT));
    tEnv.createTemporaryView(
        "s",
        source,
        Schema.newBuilder()
            .column("k", DataTypes.BIGINT())
            .column("v", DataTypes.BIGINT())
            .column("rt", DataTypes.TIMESTAMP_LTZ(3))
            .build());
    tEnv.executeSql(
        "CREATE TABLE in_write (k BIGINT, v BIGINT, rt TIMESTAMP_LTZ(3)) WITH ('connector' = "
            + "'filesystem', 'path' = '"
            + directory.toUri()
            + "', 'format' = 'parquet')");
    tEnv.executeSql("INSERT INTO in_write SELECT * FROM s WHERE v < " + rows / 2).await();
    tEnv.executeSql("INSERT INTO in_write SELECT * FROM s WHERE v >= " + rows / 2).await();
  }

  private static TableEnvironment readEnvironment(
      Path directory, String aggPhaseStrategy, boolean checkpointing) {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(2);
    if (checkpointing) {
      env.enableCheckpointing(100);
    }
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    tEnv.getConfig().setLocalTimeZone(ZoneId.of("UTC"));
    tEnv.getConfig().set("table.optimizer.agg-phase-strategy", aggPhaseStrategy);
    // Delay larger than the whole data span (~5.7s) so no window closes before end-of-input MAX —
    // keeps this test about the shuffle routing only, independent of watermark-driven late dropping
    // (which has its own coverage; see divergences/09 and the out-of-order parity test).
    tEnv.executeSql(
        "CREATE TABLE t (k BIGINT, v BIGINT, rt TIMESTAMP_LTZ(3), "
            + "WATERMARK FOR rt AS rt - INTERVAL '10' SECOND) WITH ('connector' = 'filesystem', "
            + "'path' = '"
            + directory.toUri()
            + "', 'format' = 'parquet')");
    return tEnv;
  }
}
