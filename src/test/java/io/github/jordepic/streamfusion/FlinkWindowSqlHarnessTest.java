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

  private static final String WINDOW_SQL =
      "SELECT window_start, window_end, SUM(`value`) AS total "
          + "FROM TABLE(TUMBLE(TABLE src, DESCRIPTOR(rt), INTERVAL '1' SECOND)) "
          + "GROUP BY window_start, window_end";

  @Test
  void routesTumblingWindowSqlToNative() throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    tEnv.getConfig().set("table.optimizer.agg-phase-strategy", "ONE_PHASE");
    PhysicalPlanScan scan = NativePlanner.install(tEnv);

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

    TableResult result = tEnv.executeSql(WINDOW_SQL);
    List<Long> totals = new ArrayList<>();
    try (CloseableIterator<Row> rows = result.collect()) {
      while (rows.hasNext()) {
        totals.add((Long) rows.next().getField(2));
      }
    }
    totals.sort(null);

    // Bounded source emits a final watermark closing every window: [0,1k)=3, [1k,2k)=7, [2k,3k)=5.
    assertEquals(List.of(3L, 5L, 7L), totals);
    assertTrue(scan.substitutions() > 0, "window aggregate was not routed to native execution");
  }
}
