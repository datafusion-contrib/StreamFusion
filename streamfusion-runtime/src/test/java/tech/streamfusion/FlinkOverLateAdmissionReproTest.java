package tech.streamfusion;

import static tech.streamfusion.compat.FlinkTestSources.fromData;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

class FlinkOverLateAdmissionReproTest {
  @Test
  void equalWatermarkRows() throws Exception {
    check(false, "ROWS BETWEEN 1 PRECEDING AND CURRENT ROW");
  }

  @Test
  void equalWatermarkRange() throws Exception {
    check(false, "RANGE BETWEEN INTERVAL '1' SECOND PRECEDING AND CURRENT ROW");
  }

  @Test
  void equalWatermarkUnbounded() throws Exception {
    check(false, "RANGE UNBOUNDED PRECEDING");
  }

  @Test
  void newPartitionRows() throws Exception {
    check(true, "ROWS BETWEEN 1 PRECEDING AND CURRENT ROW");
  }

  @Test
  void newPartitionRange() throws Exception {
    check(true, "RANGE BETWEEN INTERVAL '1' SECOND PRECEDING AND CURRENT ROW");
  }

  @Test
  void newPartitionUnbounded() throws Exception {
    check(true, "RANGE UNBOUNDED PRECEDING");
  }

  @Test
  void boundedFramesRejectEpochZero() throws Exception {
    for (String frame : new String[] {"ROWS BETWEEN 1 PRECEDING AND CURRENT ROW",
        "RANGE BETWEEN INTERVAL '1' SECOND PRECEDING AND CURRENT ROW"}) {
      NativeParity.assertParity(() -> input(new Row[] {Row.of(1, 0L, 10L), Row.of(1, 1L, 20L)}),
          "SELECT g, v, SUM(v) OVER (PARTITION BY g ORDER BY rt " + frame + ") FROM n");
    }
  }

  @Test
  void rangeCleanupResetsAdmissionAtItsHysteresisDeadline() throws Exception {
    NativeParity.assertParity(() -> input(new Row[] {
        Row.of(1, 1000L, 10L), Row.of(1, 1400L, 20L), Row.of(2, 2501L, 30L),
        Row.of(1, 1300L, 40L), Row.of(2, 3000L, 50L)}),
        "SELECT g, v, SUM(v) OVER (PARTITION BY g ORDER BY rt "
            + "RANGE BETWEEN INTERVAL '1' SECOND PRECEDING AND CURRENT ROW) FROM n");
  }

  @Test
  void unboundedRowsRejectLateRowsAndCountEveryAdmittedRow() throws Exception {
    for (boolean newPartition : new boolean[] {false, true}) {
      NativeParity.assertParity(() -> input(newPartition),
          "SELECT g, v, COUNT(*) OVER w, SUM(v) OVER w FROM n "
              + "WINDOW w AS (PARTITION BY g ORDER BY rt ROWS UNBOUNDED PRECEDING)");
    }
  }

  private static void check(boolean newPartition, String frame) throws Exception {
    NativeParity.assertParity(() -> input(newPartition),
        "SELECT g, v, SUM(v) OVER (PARTITION BY g ORDER BY rt " + frame + ") FROM n");
  }

  private static TableEnvironment input(boolean newPartition) {
    long base = java.time.Instant.parse("2026-01-01T00:00:00Z").toEpochMilli();
    Row[] rows = newPartition
        ? new Row[] {Row.of(1, base + 2000, 10L), Row.of(2, base + 1000, 20L), Row.of(1, base + 3000, 30L)}
        : new Row[] {Row.of(1, base + 1000, 10L), Row.of(1, base + 1000, 20L), Row.of(1, base + 2000, 30L)};
    return input(rows);
  }

  private static TableEnvironment input(Row[] rows) {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment table = StreamTableEnvironment.create(env);
    table.getConfig().setLocalTimeZone(java.time.ZoneOffset.UTC);
    var stream =
        fromData(
                env,
                Types.ROW_NAMED(
                    new String[] {"g", "millis", "v"}, Types.INT, Types.LONG, Types.LONG),
                rows)
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<Row>forGenerator(
                        context ->
                            new WatermarkGenerator<Row>() {
                              @Override
                              public void onEvent(
                                  Row event, long timestamp, WatermarkOutput output) {
                                output.emitWatermark(new Watermark(timestamp));
                              }

                              @Override
                              public void onPeriodicEmit(WatermarkOutput output) {}
                            })
                    .withTimestampAssigner((event, previous) -> (Long) event.getField(1)));
    table.createTemporaryView("n", stream,
        Schema.newBuilder().column("g", DataTypes.INT()).column("millis", DataTypes.BIGINT())
            .column("v", DataTypes.BIGINT())
            .columnByMetadata("rt", DataTypes.TIMESTAMP_LTZ(3), "rowtime")
            .watermark("rt", "SOURCE_WATERMARK()").build());
    return table;
  }
}
