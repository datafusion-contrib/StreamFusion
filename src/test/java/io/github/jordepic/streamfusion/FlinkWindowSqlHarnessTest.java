package io.github.jordepic.streamfusion;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.github.jordepic.streamfusion.planner.NativePlanner;
import io.github.jordepic.streamfusion.planner.PhysicalPlanScan;
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
  void intValueAggregateFallsBackToHost() throws Exception {
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
            "SELECT window_start, window_end, SUM(`value`) AS total "
                + "FROM TABLE(TUMBLE(TABLE ints, DESCRIPTOR(rt), INTERVAL '1' SECOND)) "
                + "GROUP BY window_start, window_end");
    List<Long> totals = new ArrayList<>();
    try (CloseableIterator<Row> rows = result.collect()) {
      while (rows.hasNext()) {
        totals.add(((Number) rows.next().getField(2)).longValue());
      }
    }

    // The int value column is not natively supported, so it must run on the host, not crash.
    assertEquals(0, scan.substitutions(), "int value column should fall back");
    assertEquals(List.of(3L), totals);
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
  void multiAggregateMatchesHost() throws Exception {
    NativeParity.assertParity(
        FlinkWindowSqlHarnessTest::environmentWithSource,
        "SELECT window_start, window_end, SUM(`value`) AS s, COUNT(`value`) AS c, MAX(`value`) AS m "
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
                    new String[] {"k", "value", "ts", "amount"},
                    Types.LONG,
                    Types.LONG,
                    Types.LONG,
                    Types.DOUBLE),
                Row.of(7L, 1L, 0L, 1.5),
                Row.of(7L, 2L, 500L, 2.5),
                Row.of(9L, 3L, 600L, 3.0),
                Row.of(7L, 4L, 1500L, 4.5),
                Row.of(9L, 5L, 2500L, 5.5))
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
            .columnByMetadata("rt", DataTypes.TIMESTAMP_LTZ(3), "rowtime")
            .watermark("rt", "SOURCE_WATERMARK()")
            .build());
    return tEnv;
  }
}
