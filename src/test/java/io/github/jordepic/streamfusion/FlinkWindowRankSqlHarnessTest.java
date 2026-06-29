package io.github.jordepic.streamfusion;

import java.time.Duration;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
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
 * Window Top-N and window deduplication over a windowing TVF: per window (and partition key) the
 * native ranker keeps the top-N rows by the order key and emits them when the watermark closes the
 * window. Deduplication is the rank-1 keep-first/keep-last case. All append-only.
 */
class FlinkWindowRankSqlHarnessTest {

  private static final String TVF =
      "TABLE(TUMBLE(TABLE src, DESCRIPTOR(rt), INTERVAL '1' SECOND))";

  @Test
  void windowTopNWithRankNumberMatchesHost() throws Exception {
    // Per window, top 2 by v DESC, projecting the rank number (outputRankNumber = true).
    NativeParity.assertParity(
        FlinkWindowRankSqlHarnessTest::environment,
        "SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY window_start, window_end"
            + " ORDER BY v DESC) AS rn FROM " + TVF + ") WHERE rn <= 2");
  }

  @Test
  void windowTopNPartitionedByKeyMatchesHost() throws Exception {
    // Top 1 by v DESC per (window, k), rank number not projected (outputRankNumber = false).
    NativeParity.assertParity(
        FlinkWindowRankSqlHarnessTest::environment,
        "SELECT k, v, window_start FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY window_start,"
            + " window_end, k ORDER BY v DESC) AS rn FROM " + TVF + ") WHERE rn <= 1");
  }

  @Test
  void windowDeduplicationKeepFirstMatchesHost() throws Exception {
    // Per (window, k), keep the first row by rowtime (a WindowDeduplicate node).
    NativeParity.assertParity(
        FlinkWindowRankSqlHarnessTest::environment,
        "SELECT k, v, window_start FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY window_start,"
            + " window_end, k ORDER BY rt ASC) AS rn FROM " + TVF + ") WHERE rn = 1");
  }

  @Test
  void windowDeduplicationKeepLastMatchesHost() throws Exception {
    // Per (window, k), keep the last row by rowtime (keep-last is still append-only over a window).
    NativeParity.assertParity(
        FlinkWindowRankSqlHarnessTest::environment,
        "SELECT k, v, window_start FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY window_start,"
            + " window_end, k ORDER BY rt DESC) AS rn FROM " + TVF + ") WHERE rn = 1");
  }

  private static final String PROCTIME_TVF =
      "TABLE(TUMBLE(TABLE src, DESCRIPTOR(pt), INTERVAL '5' SECOND))";

  @Test
  void proctimeWindowTopNRoutesToNative() throws Exception {
    // Window Top-N over a proctime TUMBLE: each window closes on a processing-time timer rather than a
    // watermark. Non-deterministic boundaries (see the CLAUDE.md note) — assert it routes and runs;
    // NativeColumnarWindowRankOperatorTest pins the close on a controlled clock.
    NativeParity.assertRoutes(
        FlinkWindowRankSqlHarnessTest::proctimeEnvironment,
        "SELECT k, v, window_start FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY window_start,"
            + " window_end ORDER BY v DESC) AS rn FROM " + PROCTIME_TVF + ") WHERE rn <= 2");
  }

  @Test
  void proctimeWindowDeduplicationRoutesToNative() throws Exception {
    // Window deduplication (rank-1 keep-first by proctime order) over a proctime TUMBLE.
    NativeParity.assertRoutes(
        FlinkWindowRankSqlHarnessTest::proctimeEnvironment,
        "SELECT k, v, window_start FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY window_start,"
            + " window_end, k ORDER BY pt ASC) AS rn FROM " + PROCTIME_TVF + ") WHERE rn = 1");
  }

  private static TableEnvironment proctimeEnvironment() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    DataStream<Row> source =
        env.fromData(
            Types.ROW_NAMED(new String[] {"k", "v"}, Types.LONG, Types.LONG),
            Row.of(1L, 10L),
            Row.of(1L, 30L),
            Row.of(2L, 20L));
    tEnv.createTemporaryView(
        "src",
        source,
        Schema.newBuilder()
            .column("k", DataTypes.BIGINT())
            .column("v", DataTypes.BIGINT())
            .columnByExpression("pt", "PROCTIME()")
            .build());
    return tEnv;
  }

  private static TableEnvironment environment() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

    // Two 1s windows; distinct v per window so the rank order is unambiguous, and repeated k within a
    // window so deduplication has something to collapse.
    DataStream<Row> source =
        env.fromData(
                Types.ROW_NAMED(new String[] {"k", "v", "ts"}, Types.LONG, Types.LONG, Types.LONG),
                Row.of(1L, 10L, 100L),
                Row.of(1L, 30L, 800L),
                Row.of(2L, 20L, 300L),
                Row.of(1L, 50L, 1200L),
                Row.of(2L, 40L, 1500L),
                Row.of(2L, 45L, 1800L))
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<Row>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                    .withTimestampAssigner((row, ts) -> (Long) row.getField(2)));
    tEnv.createTemporaryView(
        "src",
        source,
        Schema.newBuilder()
            .column("k", DataTypes.BIGINT())
            .column("v", DataTypes.BIGINT())
            .column("ts", DataTypes.BIGINT())
            .columnByMetadata("rt", DataTypes.TIMESTAMP_LTZ(3), "rowtime")
            .watermark("rt", "SOURCE_WATERMARK()")
            .build());
    return tEnv;
  }
}
