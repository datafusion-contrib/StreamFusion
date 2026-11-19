package tech.streamfusion;

import static tech.streamfusion.compat.FlinkTestSources.fromData;

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
 * Event-time sort (`ORDER BY rowtime`): the native sorter buffers rows and releases them in rowtime
 * order as the watermark advances. The parity harness compares the (order-independent) row set, which
 * confirms the sort emits exactly the host's rows with no drop or duplication; the emitted ascending
 * order is checked directly in {@code NativeColumnarTemporalSortOperatorTest}.
 */
class FlinkTemporalSortSqlHarnessTest {

  @Test
  void lateRowsAndTimestampTiesAreDroppedAfterEmission() throws Exception {
    String sql = "SELECT k, v, rt, CURRENT_WATERMARK(rt) FROM src ORDER BY rt";
    String plan = tech.streamfusion.planner.NativePlanner.explain(lateEnvironment(), sql);
    org.junit.jupiter.api.Assertions.assertTrue(plan.contains("NativeTemporalSort"), plan);
    NativeParity.assertOrderedKindedParity(FlinkTemporalSortSqlHarnessTest::lateEnvironment, sql);
  }

  private static TableEnvironment lateEnvironment() {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    var table = StreamTableEnvironment.create(env);
    WatermarkStrategy<Row> watermarks =
        context ->
            new org.apache.flink.api.common.eventtime.WatermarkGenerator<>() {
              private long maximum = Long.MIN_VALUE;

              @Override
              public void onEvent(
                  Row row,
                  long timestamp,
                  org.apache.flink.api.common.eventtime.WatermarkOutput output) {
                maximum = Math.max(maximum, timestamp);
                output.emitWatermark(new org.apache.flink.api.common.eventtime.Watermark(maximum));
              }

              @Override
              public void onPeriodicEmit(
                  org.apache.flink.api.common.eventtime.WatermarkOutput output) {}
            };
    var source =
        fromData(
                env,
                Types.ROW_NAMED(new String[] {"k", "v", "ts"}, Types.LONG, Types.LONG, Types.LONG),
                Row.of(1L, 1L, 100L),
                Row.of(1L, 3L, 300L),
                Row.of(1L, 2L, 200L),
                Row.of(1L, 4L, 300L),
                Row.of(1L, 5L, 400L))
            .assignTimestampsAndWatermarks(
                watermarks.withTimestampAssigner((row, previous) -> (Long) row.getField(2)));
    table.createTemporaryView(
        "src",
        source,
        Schema.newBuilder()
            .column("k", DataTypes.BIGINT())
            .column("v", DataTypes.BIGINT())
            .column("ts", DataTypes.BIGINT())
            .columnByMetadata("rt", DataTypes.TIMESTAMP_LTZ(3), "rowtime")
            .watermark("rt", "SOURCE_WATERMARK()")
            .build());
    return table;
  }

  @Test
  void eventTimeSortMatchesHost() throws Exception {
    NativeParity.assertParity(
        FlinkTemporalSortSqlHarnessTest::environment, "SELECT k, v, rt FROM src ORDER BY rt");
  }

  @Test
  void eventTimeSortOverProjectionMatchesHost() throws Exception {
    // A filter/projection (native Calc) feeds the sort — the whole pipeline stays one columnar island.
    NativeParity.assertParity(
        FlinkTemporalSortSqlHarnessTest::environment,
        "SELECT k, v + 1 AS v1, rt FROM src WHERE v > 10 ORDER BY rt");
  }

  @Test
  void eventTimeSortAtParallelismTwoMatchesHost() throws Exception {
    NativeParity.assertParity(
        () -> environment(2), "SELECT k, v, rt FROM src ORDER BY rt");
  }

  private static TableEnvironment environment() {
    return environment(1);
  }

  private static TableEnvironment environment(int parallelism) {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(parallelism);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

    // Out-of-order timestamps so the sort actually reorders; the 2s bounded-out-of-orderness keeps
    // every row open until end-of-input MAX, so all rows are released together (no mid-stream cut).
    DataStream<Row> source =
        fromData(
                env,
                Types.ROW_NAMED(new String[] {"k", "v", "ts"}, Types.LONG, Types.LONG, Types.LONG),
                Row.of(1L, 30L, 2000L),
                Row.of(2L, 10L, 500L),
                Row.of(3L, 50L, 1500L),
                Row.of(1L, 20L, 0L),
                Row.of(2L, 40L, 1000L))
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
