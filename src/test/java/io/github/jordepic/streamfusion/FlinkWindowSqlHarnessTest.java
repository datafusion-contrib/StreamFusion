package io.github.jordepic.streamfusion;

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

class FlinkWindowSqlHarnessTest {

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
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    tEnv.getConfig().set("table.optimizer.agg-phase-strategy", "ONE_PHASE");

    DataStream<Row> source =
        env.fromData(
                Types.ROW_NAMED(
                    new String[] {"k", "value", "ts"}, Types.LONG, Types.LONG, Types.LONG),
                Row.of(7L, 1L, 0L),
                Row.of(7L, 2L, 500L),
                Row.of(9L, 3L, 600L),
                Row.of(7L, 4L, 1500L),
                Row.of(9L, 5L, 2500L))
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
            .columnByMetadata("rt", DataTypes.TIMESTAMP_LTZ(3), "rowtime")
            .watermark("rt", "SOURCE_WATERMARK()")
            .build());
    return tEnv;
  }
}
