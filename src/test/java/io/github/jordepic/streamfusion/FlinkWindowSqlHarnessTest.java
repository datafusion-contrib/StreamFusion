package io.github.jordepic.streamfusion;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.github.jordepic.streamfusion.planner.NativePlanner;
import io.github.jordepic.streamfusion.planner.PhysicalPlanScan;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Test;

class FlinkWindowSqlHarnessTest {

  @Test
  void intAvgFallsBackToHost() throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    tEnv.getConfig().set("table.optimizer.agg-phase-strategy", "ONE_PHASE");
    PhysicalPlanScan scan = NativePlanner.install(tEnv);

    DataStream<Row> source =
        env.fromData(
                Types.ROW_NAMED(new String[] {"value", "ts"}, Types.INT, Types.LONG),
                Row.of(1, 0L),
                Row.of(2, 500L))
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<Row>forMonotonousTimestamps()
                    .withTimestampAssigner((row, ts) -> (Long) row.getField(1)));
    tEnv.createTemporaryView(
        "ints",
        source,
        Schema.newBuilder()
            .column("value", DataTypes.INT())
            .column("ts", DataTypes.BIGINT())
            .columnByMetadata("rt", DataTypes.TIMESTAMP_LTZ(3), "rowtime")
            .watermark("rt", "SOURCE_WATERMARK()")
            .build());

    TableResult result =
        tEnv.executeSql(
            "SELECT window_start, window_end, AVG(`value`) AS mean "
                + "FROM TABLE(TUMBLE(TABLE ints, DESCRIPTOR(rt), INTERVAL '1' SECOND)) "
                + "GROUP BY window_start, window_end");
    List<Long> avgs = new ArrayList<>();
    try (CloseableIterator<Row> rows = result.collect()) {
      while (rows.hasNext()) {
        avgs.add(((Number) rows.next().getField(2)).longValue());
      }
    }

    // Integer AVG over an int column is not natively supported (its truncating average is not yet
    // wired), so it must run on the host, not crash. The host returns the integer-truncated 1.
    assertEquals(0, scan.substitutions(), "int AVG should fall back");
    assertEquals(List.of(1L), avgs);
  }

  @Test
  void tumblingSumMatchesHost() throws Exception {
    assertWindowParity("SUM(`value`)");
  }

  @Test
  void tumblingMaxMatchesHost() throws Exception {
    assertWindowParity("MAX(`value`)");
  }

  @Test
  void tumblingAvgMatchesHost() throws Exception {
    assertWindowParity("AVG(`value`)");
  }

  @Test
  void stringKeyTumblingMatchesHost() throws Exception {
    // A string grouping key, keyed natively as a scalar and emitted back as a string.
    NativeParity.assertParity(
        FlinkWindowSqlHarnessTest::environmentWithSource,
        "SELECT s, window_start, window_end, SUM(`value`) AS sm, COUNT(`value`) AS c "
            + "FROM TABLE(TUMBLE(TABLE src, DESCRIPTOR(rt), INTERVAL '1' SECOND)) "
            + "GROUP BY s, window_start, window_end");
  }

  @Test
  void stringAndBigintKeyTumblingMatchesHost() throws Exception {
    // Mixed key types: a string and a bigint key together.
    NativeParity.assertParity(
        FlinkWindowSqlHarnessTest::environmentWithSource,
        "SELECT s, k, window_start, window_end, SUM(`value`) AS sm "
            + "FROM TABLE(TUMBLE(TABLE src, DESCRIPTOR(rt), INTERVAL '1' SECOND)) "
            + "GROUP BY s, k, window_start, window_end");
  }

  @Test
  void stringKeyTwoPhaseMatchesHost() throws Exception {
    NativeParity.assertParity(
        FlinkWindowSqlHarnessTest::environmentTwoPhase,
        "SELECT s, window_start, window_end, SUM(`value`) AS sm "
            + "FROM TABLE(TUMBLE(TABLE src, DESCRIPTOR(rt), INTERVAL '1' SECOND)) "
            + "GROUP BY s, window_start, window_end");
  }

  @Test
  void intKeyTumblingMatchesHost() throws Exception {
    // An int grouping key: keyed natively as int64 but emitted back as INT to match the host.
    NativeParity.assertParity(
        FlinkWindowSqlHarnessTest::environmentWithSource,
        "SELECT qty, window_start, window_end, SUM(`value`) AS s "
            + "FROM TABLE(TUMBLE(TABLE src, DESCRIPTOR(rt), INTERVAL '1' SECOND)) "
            + "GROUP BY qty, window_start, window_end");
  }

  @Test
  void twoKeyTumblingMatchesHost() throws Exception {
    // GROUP BY two bigint keys plus the window: the native composite key must match the host.
    NativeParity.assertParity(
        FlinkWindowSqlHarnessTest::environmentWithSource,
        "SELECT k, g, window_start, window_end, SUM(`value`) AS s, COUNT(`value`) AS c "
            + "FROM TABLE(TUMBLE(TABLE src, DESCRIPTOR(rt), INTERVAL '1' SECOND)) "
            + "GROUP BY k, g, window_start, window_end");
  }

  @Test
  void twoKeySessionMatchesHost() throws Exception {
    NativeParity.assertParity(
        FlinkWindowSqlHarnessTest::environmentWithSource,
        "SELECT k, g, window_start, window_end, SUM(`value`) AS s "
            + "FROM TABLE(SESSION(TABLE src PARTITION BY (k, g), DESCRIPTOR(rt), INTERVAL '1' SECOND)) "
            + "GROUP BY k, g, window_start, window_end");
  }

  @Test
  void twoPhaseTwoKeyTumblingMatchesHost() throws Exception {
    NativeParity.assertParity(
        FlinkWindowSqlHarnessTest::environmentTwoPhase,
        "SELECT k, g, window_start, window_end, SUM(`value`) AS s "
            + "FROM TABLE(TUMBLE(TABLE src, DESCRIPTOR(rt), INTERVAL '1' SECOND)) "
            + "GROUP BY k, g, window_start, window_end");
  }

  @Test
  void keyedTumblingSumMatchesHost() throws Exception {
    NativeParity.assertParity(
        FlinkWindowSqlHarnessTest::environmentWithSource,
        "SELECT k, window_start, window_end, SUM(`value`) AS total "
            + "FROM TABLE(TUMBLE(TABLE src, DESCRIPTOR(rt), INTERVAL '1' SECOND)) "
            + "GROUP BY k, window_start, window_end");
  }

  @Test
  void twoPhaseTumblingSumMatchesHost() throws Exception {
    // Default (two-phase) planning: a native local pre-aggregate and global merge must agree
    // with the host's local+global aggregation.
    NativeParity.assertParity(
        FlinkWindowSqlHarnessTest::environmentTwoPhase,
        "SELECT k, window_start, window_end, SUM(`value`) AS total "
            + "FROM TABLE(TUMBLE(TABLE src, DESCRIPTOR(rt), INTERVAL '1' SECOND)) "
            + "GROUP BY k, window_start, window_end");
  }

  @Test
  void hoppingSumMatchesHost() throws Exception {
    // One-phase HOP: a row falls in two overlapping 2s windows sliding every 1s.
    NativeParity.assertParity(
        FlinkWindowSqlHarnessTest::environmentWithSource,
        "SELECT window_start, window_end, SUM(`value`) AS s "
            + "FROM TABLE(HOP(TABLE src, DESCRIPTOR(rt), INTERVAL '1' SECOND, INTERVAL '2' SECOND)) "
            + "GROUP BY window_start, window_end");
  }

  @Test
  void keyedHoppingMultiAggregateMatchesHost() throws Exception {
    NativeParity.assertParity(
        FlinkWindowSqlHarnessTest::environmentWithSource,
        "SELECT k, window_start, window_end, SUM(`value`) AS s, COUNT(`value`) AS c "
            + "FROM TABLE(HOP(TABLE src, DESCRIPTOR(rt), INTERVAL '1' SECOND, INTERVAL '2' SECOND)) "
            + "GROUP BY k, window_start, window_end");
  }

  @Test
  void doubleValueAggregateMatchesHost() throws Exception {
    // One-phase double value: SUM/MAX over a double column, plus a bigint COUNT alongside.
    NativeParity.assertParity(
        FlinkWindowSqlHarnessTest::environmentWithSource,
        "SELECT window_start, window_end, SUM(amount) AS s, MAX(amount) AS m, COUNT(amount) AS c "
            + "FROM TABLE(TUMBLE(TABLE src, DESCRIPTOR(rt), INTERVAL '1' SECOND)) "
            + "GROUP BY window_start, window_end");
  }

  @Test
  void sessionSumMatchesHost() throws Exception {
    // Session windows: consecutive rows within the gap form one window; a larger gap splits them.
    NativeParity.assertParity(
        FlinkWindowSqlHarnessTest::environmentWithSource,
        "SELECT window_start, window_end, SUM(`value`) AS s, COUNT(`value`) AS c "
            + "FROM TABLE(SESSION(TABLE src, DESCRIPTOR(rt), INTERVAL '1' SECOND)) "
            + "GROUP BY window_start, window_end");
  }

  @Test
  void keyedSessionMultiAggregateMatchesHost() throws Exception {
    // Per-key sessions: each key's gaps are independent, partitioned by the session TVF.
    NativeParity.assertParity(
        FlinkWindowSqlHarnessTest::environmentWithSource,
        "SELECT k, window_start, window_end, SUM(`value`) AS s, MAX(`value`) AS m "
            + "FROM TABLE(SESSION(TABLE src PARTITION BY k, DESCRIPTOR(rt), INTERVAL '1' SECOND)) "
            + "GROUP BY k, window_start, window_end");
  }

  @Test
  void sessionMergeMatchesHost() throws Exception {
    // An out-of-order element lands between two open sessions and bridges them into one; the native
    // merge of the two windows' accumulators must match the host's merging assigner.
    NativeParity.assertParity(
        FlinkWindowSqlHarnessTest::environmentForSessionMerge,
        "SELECT window_start, window_end, SUM(`value`) AS s, COUNT(`value`) AS c "
            + "FROM TABLE(SESSION(TABLE src, DESCRIPTOR(rt), INTERVAL '1' SECOND)) "
            + "GROUP BY window_start, window_end");
  }

  @Test
  void cumulativeSumMatchesHost() throws Exception {
    // Cumulative windows: nested windows sharing a bucket start, ends growing by the step.
    NativeParity.assertParity(
        FlinkWindowSqlHarnessTest::environmentWithSource,
        "SELECT window_start, window_end, SUM(`value`) AS s, COUNT(`value`) AS c "
            + "FROM TABLE(CUMULATE(TABLE src, DESCRIPTOR(rt), INTERVAL '1' SECOND, INTERVAL '2' SECOND)) "
            + "GROUP BY window_start, window_end");
  }

  @Test
  void keyedCumulativeMultiAggregateMatchesHost() throws Exception {
    NativeParity.assertParity(
        FlinkWindowSqlHarnessTest::environmentWithSource,
        "SELECT k, window_start, window_end, SUM(`value`) AS s, MAX(`value`) AS m "
            + "FROM TABLE(CUMULATE(TABLE src, DESCRIPTOR(rt), INTERVAL '1' SECOND, INTERVAL '2' SECOND)) "
            + "GROUP BY k, window_start, window_end");
  }

  @Test
  void multiAggregateMatchesHost() throws Exception {
    NativeParity.assertParity(
        FlinkWindowSqlHarnessTest::environmentWithSource,
        "SELECT window_start, window_end, SUM(`value`) AS s, COUNT(`value`) AS c, MAX(`value`) AS m "
            + "FROM TABLE(TUMBLE(TABLE src, DESCRIPTOR(rt), INTERVAL '1' SECOND)) "
            + "GROUP BY window_start, window_end");
  }

  @Test
  void twoPhaseHoppingSumMatchesHost() throws Exception {
    // Default-planned HOP is two-phase and slice-shared: a local per-slice pre-aggregate, the
    // shuffle, and a global that combines each window's slices must agree with the host.
    NativeParity.assertParity(
        FlinkWindowSqlHarnessTest::environmentTwoPhase,
        "SELECT window_start, window_end, SUM(`value`) AS s "
            + "FROM TABLE(HOP(TABLE src, DESCRIPTOR(rt), INTERVAL '1' SECOND, INTERVAL '2' SECOND)) "
            + "GROUP BY window_start, window_end");
  }

  @Test
  void twoPhaseKeyedHoppingMultiAggregateMatchesHost() throws Exception {
    NativeParity.assertParity(
        FlinkWindowSqlHarnessTest::environmentTwoPhase,
        "SELECT k, window_start, window_end, SUM(`value`) AS s, COUNT(`value`) AS c "
            + "FROM TABLE(HOP(TABLE src, DESCRIPTOR(rt), INTERVAL '1' SECOND, INTERVAL '2' SECOND)) "
            + "GROUP BY k, window_start, window_end");
  }

  @Test
  void twoPhaseDoubleValueMatchesHost() throws Exception {
    // Default-planned (two-phase) aggregation over a double value: the local emits a double sum
    // partial alongside a bigint count partial, and the global merges the mixed-type partials.
    NativeParity.assertParity(
        FlinkWindowSqlHarnessTest::environmentTwoPhase,
        "SELECT window_start, window_end, SUM(amount) AS s, MAX(amount) AS m, COUNT(amount) AS c "
            + "FROM TABLE(TUMBLE(TABLE src, DESCRIPTOR(rt), INTERVAL '1' SECOND)) "
            + "GROUP BY window_start, window_end");
  }

  @Test
  void twoPhaseMultiAggregateMatchesHost() throws Exception {
    NativeParity.assertParity(
        FlinkWindowSqlHarnessTest::environmentTwoPhase,
        "SELECT k, window_start, window_end, SUM(`value`) AS s, COUNT(`value`) AS c "
            + "FROM TABLE(TUMBLE(TABLE src, DESCRIPTOR(rt), INTERVAL '1' SECOND)) "
            + "GROUP BY k, window_start, window_end");
  }

  @Test
  void twoPhaseTumblingCountMatchesHost() throws Exception {
    // COUNT exercises the local=count / global=sum split.
    NativeParity.assertParity(
        FlinkWindowSqlHarnessTest::environmentTwoPhase,
        "SELECT window_start, window_end, COUNT(`value`) AS total "
            + "FROM TABLE(TUMBLE(TABLE src, DESCRIPTOR(rt), INTERVAL '1' SECOND)) "
            + "GROUP BY window_start, window_end");
  }

  @Test
  void intValueAggregatesMatchHost() throws Exception {
    // SUM/MIN/MAX/COUNT over a 32-bit int. SUM uses the native wrapping int32 accumulator so it
    // keeps the host's narrow type; AVG over int would truncate and stays on the host.
    NativeParity.assertParity(
        FlinkWindowSqlHarnessTest::environmentWithSource,
        "SELECT window_start, window_end, SUM(qty) AS s, MIN(qty) AS lo, MAX(qty) AS hi, "
            + "COUNT(qty) AS c "
            + "FROM TABLE(TUMBLE(TABLE src, DESCRIPTOR(rt), INTERVAL '1' SECOND)) "
            + "GROUP BY window_start, window_end");
  }

  private static void assertWindowParity(String aggregate) throws Exception {
    NativeParity.assertParity(
        FlinkWindowSqlHarnessTest::environmentWithSource,
        "SELECT window_start, window_end, "
            + aggregate
            + " AS agg "
            + "FROM TABLE(TUMBLE(TABLE src, DESCRIPTOR(rt), INTERVAL '1' SECOND)) "
            + "GROUP BY window_start, window_end");
  }

  private static TableEnvironment environmentWithSource() {
    return buildEnvironment(true);
  }

  private static TableEnvironment environmentForSessionMerge() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

    // Out-of-order: the row at ts=700 arrives after the rows at 0 and 1500, which would otherwise
    // be separate sessions (gap 1s), and its [700, 1700) window bridges them into [0, 2500).
    DataStream<Row> source =
        env.fromData(
                Types.ROW_NAMED(new String[] {"value", "ts"}, Types.LONG, Types.LONG),
                Row.of(1L, 0L),
                Row.of(2L, 1500L),
                Row.of(4L, 700L),
                Row.of(9L, 5000L))
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<Row>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                    .withTimestampAssigner((row, ts) -> (Long) row.getField(1)));
    tEnv.createTemporaryView(
        "src",
        source,
        Schema.newBuilder()
            .column("value", DataTypes.BIGINT())
            .column("ts", DataTypes.BIGINT())
            .columnByMetadata("rt", DataTypes.TIMESTAMP_LTZ(3), "rowtime")
            .watermark("rt", "SOURCE_WATERMARK()")
            .build());
    return tEnv;
  }

  private static TableEnvironment environmentTwoPhase() {
    return buildEnvironment(false);
  }

  private static TableEnvironment buildEnvironment(boolean onePhase) {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    if (onePhase) {
      tEnv.getConfig().set("table.optimizer.agg-phase-strategy", "ONE_PHASE");
    }

    DataStream<Row> source =
        env.fromData(
                Types.ROW_NAMED(
                    new String[] {"k", "value", "ts", "amount", "qty", "g", "s"},
                    Types.LONG,
                    Types.LONG,
                    Types.LONG,
                    Types.DOUBLE,
                    Types.INT,
                    Types.LONG,
                    Types.STRING),
                Row.of(7L, 1L, 0L, 1.5, 10, 100L, "a"),
                Row.of(7L, 2L, 500L, 2.5, 20, 100L, "a"),
                Row.of(9L, 3L, 600L, 3.0, 30, 200L, "b"),
                Row.of(7L, 4L, 1500L, 4.5, 40, 100L, "a"),
                Row.of(9L, 5L, 2500L, 5.5, 50, 200L, "b"))
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<Row>forMonotonousTimestamps()
                    .withTimestampAssigner((row, ts) -> (Long) row.getField(2)));

    tEnv.createTemporaryView(
        "src",
        source,
        Schema.newBuilder()
            .column("k", DataTypes.BIGINT())
            .column("value", DataTypes.BIGINT())
            .column("ts", DataTypes.BIGINT())
            .column("amount", DataTypes.DOUBLE())
            .column("qty", DataTypes.INT())
            .column("g", DataTypes.BIGINT())
            .column("s", DataTypes.STRING())
            .columnByMetadata("rt", DataTypes.TIMESTAMP_LTZ(3), "rowtime")
            .watermark("rt", "SOURCE_WATERMARK()")
            .build());
    return tEnv;
  }
}
