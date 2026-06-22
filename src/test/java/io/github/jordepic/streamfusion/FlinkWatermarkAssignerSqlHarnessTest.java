package io.github.jordepic.streamfusion;

import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * A windowed aggregate over a Parquet source carrying a {@code WATERMARK} clause. A filesystem
 * source does not push watermarks down, so the plan has a watermark-assigner node; fed by the native
 * (columnar) Parquet source, it routes to the native columnar assigner, and the whole query must
 * still match the host.
 */
class FlinkWatermarkAssignerSqlHarnessTest {

  private static final String WINDOW_QUERY =
      "SELECT window_start, window_end, SUM(v) AS total "
          + "FROM TABLE(TUMBLE(TABLE t, DESCRIPTOR(rt), INTERVAL '1' SECOND)) "
          + "GROUP BY window_start, window_end";

  @Test
  void windowedAggregateOverParquetSourceMatchesHost() throws Exception {
    Path input = Files.createTempDirectory("wm-in");
    writeInput(input);
    // The 2-second watermark delay (see the DDL) keeps every window open until end-of-input MAX, so
    // no window closes mid-batch — the case where per-batch watermark assignment would diverge from
    // the host's per-row dropping (divergences/09). This exercises the full columnar pipeline (native
    // source → native watermark assigner → native window) at true parity, including the source's
    // timestamp time-zone normalization.
    NativeParity.assertParity(() -> readEnvironment(input), WINDOW_QUERY);
  }

  /** Writes the input Parquet directory the read table scans, via the host filesystem+parquet sink. */
  private static void writeInput(Path directory) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    env.enableCheckpointing(100);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    tEnv.executeSql(
        "CREATE TABLE in_write (k BIGINT, v BIGINT, rt TIMESTAMP(3)) WITH ('connector' = "
            + "'filesystem', 'path' = '"
            + directory.toUri()
            + "', 'format' = 'parquet')");
    tEnv.executeSql(
            "INSERT INTO in_write VALUES "
                + "(1, 10, TIMESTAMP '1970-01-01 00:00:00.000'), "
                + "(2, 20, TIMESTAMP '1970-01-01 00:00:00.500'), "
                + "(3, 30, TIMESTAMP '1970-01-01 00:00:01.000'), "
                + "(4, 40, TIMESTAMP '1970-01-01 00:00:01.500'), "
                + "(5, 50, TIMESTAMP '1970-01-01 00:00:02.500')")
        .await();
  }

  /** A fresh environment with the Parquet read table (rowtime + bounded-out-of-orderness watermark). */
  private static TableEnvironment readEnvironment(Path directory) {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    tEnv.getConfig().set("table.optimizer.agg-phase-strategy", "ONE_PHASE");
    tEnv.executeSql(
        "CREATE TABLE t (k BIGINT, v BIGINT, rt TIMESTAMP(3), "
            + "WATERMARK FOR rt AS rt - INTERVAL '2' SECOND) WITH ('connector' = 'filesystem', "
            + "'path' = '"
            + directory.toUri()
            + "', 'format' = 'parquet')");
    return tEnv;
  }
}
