package io.github.jordepic.streamfusion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Test;

class FlinkWindowSqlHarnessTest {

  @Test
  void routesTumblingSumToNative() throws Exception {
    // Per window: [0,1k)=1+2, [1k,2k)=3+4, [2k,3k)=5.
    assertRoutesToNative("SUM(`value`)", List.of(3L, 5L, 7L));
  }

  @Test
  void routesTumblingMaxToNative() throws Exception {
    // Per window: [0,1k)=max(1,2), [1k,2k)=max(3,4), [2k,3k)=max(5).
    assertRoutesToNative("MAX(`value`)", List.of(2L, 4L, 5L));
  }

  private static void assertRoutesToNative(String aggregate, List<Long> expectedSorted)
      throws Exception {
    StreamTableEnvironment tEnv = environmentWithSource();
    PhysicalPlanScan scan = NativePlanner.install(tEnv);

    TableResult result =
        tEnv.executeSql(
            "SELECT window_start, window_end, "
                + aggregate
                + " AS agg "
                + "FROM TABLE(TUMBLE(TABLE src, DESCRIPTOR(rt), INTERVAL '1' SECOND)) "
                + "GROUP BY window_start, window_end");

    List<Long> values = new ArrayList<>();
    try (CloseableIterator<Row> rows = result.collect()) {
      while (rows.hasNext()) {
        values.add((Long) rows.next().getField(2));
      }
    }
    values.sort(null);

    assertTrue(scan.substitutions() > 0, "window aggregate was not routed to native execution");
    assertEquals(expectedSorted, values);
  }

  private static StreamTableEnvironment environmentWithSource() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    tEnv.getConfig().set("table.optimizer.agg-phase-strategy", "ONE_PHASE");

    DataStream<Row> source =
        env.fromData(
                Types.ROW_NAMED(new String[] {"ts", "value"}, Types.LONG, Types.LONG),
                Row.of(0L, 1L),
                Row.of(500L, 2L),
                Row.of(1000L, 3L),
                Row.of(1500L, 4L),
                Row.of(2500L, 5L))
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<Row>forMonotonousTimestamps()
                    .withTimestampAssigner((row, ts) -> (Long) row.getField(0)));

    tEnv.createTemporaryView(
        "src",
        source,
        Schema.newBuilder()
            .column("ts", DataTypes.BIGINT())
            .column("value", DataTypes.BIGINT())
            .columnByMetadata("rt", DataTypes.TIMESTAMP_LTZ(3), "rowtime")
            .watermark("rt", "SOURCE_WATERMARK()")
            .build());
    return tEnv;
  }
}
