package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static tech.streamfusion.compat.FlinkTestSources.fromData;

import java.time.Instant;
import java.time.ZoneId;
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
import tech.streamfusion.planner.NativePlanner;

class FlinkCurrentTimeSqlHarnessTest {
  @Test
  void sourceWatermarksAndCurrentWatermarkMatchHost() throws Exception {
    NativeParity.assertParity(
        FlinkCurrentTimeSqlHarnessTest::watermarks,
        "SELECT id, rt, CURRENT_WATERMARK(rt) FROM events");
  }

  @Test
  void watermarkFilterUsesTheCalcRuntimeContext() throws Exception {
    NativeParity.assertParity(
        FlinkCurrentTimeSqlHarnessTest::watermarks,
        "SELECT id, rt FROM events WHERE CURRENT_WATERMARK(rt) IS NULL OR rt >"
            + " CURRENT_WATERMARK(rt)");
  }

  @Test
  void currentTimeFunctionsExecuteAndReturnRecentValues() throws Exception {
    var tables = FlinkTemporalFunctionsSqlHarnessTest.environment();
    String sql =
        "SELECT CURRENT_TIMESTAMP, NOW(), CURRENT_ROW_TIMESTAMP(), "
            + "CURRENT_DATE, CURRENT_TIME, LOCALTIME, LOCALTIMESTAMP, UNIX_TIMESTAMP() "
            + "FROM temporal_inputs";
    long before = System.currentTimeMillis();
    var scan = NativePlanner.install(tables);
    int count = 0;
    try (var rows = tables.executeSql(sql).collect()) {
      while (rows.hasNext()) {
        Row row = rows.next();
        long after = System.currentTimeMillis();
        for (int column = 0; column < 3; column++) {
          long value = ((Instant) row.getField(column)).toEpochMilli();
          assertTrue(value >= before && value <= after);
        }
        for (int column = 3; column < row.getArity(); column++) {
          assertTrue(row.getField(column) != null);
        }
        count++;
      }
    }
    assertTrue(count > 0);
    assertTrue(scan.substitutions() > 0, () -> scan.fallbackReasons().toString());
  }

  private static TableEnvironment watermarks() {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    var tables = StreamTableEnvironment.create(env);
    tables.getConfig().setLocalTimeZone(ZoneId.of("America/New_York"));
    var type = Types.ROW_NAMED(new String[] {"id", "rt"}, Types.INT, Types.INSTANT);
    var source =
        fromData(
                env,
                type,
                Row.of(0, Instant.ofEpochMilli(1000)),
                Row.of(1, Instant.ofEpochMilli(3000)),
                Row.of(2, Instant.ofEpochMilli(500)),
                Row.of(3, Instant.ofEpochMilli(5000)))
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<Row>forGenerator(
                        context ->
                            new WatermarkGenerator<Row>() {
                              private long maximum = Long.MIN_VALUE;

                              @Override
                              public void onEvent(Row row, long timestamp, WatermarkOutput output) {
                                maximum = Math.max(maximum, timestamp);
                                output.emitWatermark(new Watermark(maximum - 1));
                              }

                              @Override
                              public void onPeriodicEmit(WatermarkOutput output) {}
                            })
                    .withTimestampAssigner(
                        (row, previous) -> ((Instant) row.getField(1)).toEpochMilli()));
    tables.createTemporaryView(
        "events",
        source,
        Schema.newBuilder()
            .column("id", DataTypes.INT())
            .column("rt", DataTypes.TIMESTAMP_LTZ(3))
            .watermark("rt", "SOURCE_WATERMARK()")
            .build());
    return tables;
  }
}
