package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.time.ZoneOffset;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import tech.streamfusion.compat.FlinkTestSources;

class FlinkWindowCountTypesSqlHarnessTest {
  @ParameterizedTest
  @CsvSource({
    "TUMBLE,ONE_PHASE,false", "TUMBLE,TWO_PHASE,false",
    "HOP,ONE_PHASE,false", "HOP,TWO_PHASE,false",
    "CUMULATE,ONE_PHASE,false", "CUMULATE,TWO_PHASE,false",
    "TUMBLE,ONE_PHASE,true", "TUMBLE,TWO_PHASE,true",
    "HOP,ONE_PHASE,true", "HOP,TWO_PHASE,true",
    "CUMULATE,ONE_PHASE,true", "CUMULATE,TWO_PHASE,true"
  })
  void typedValidityCountsKeepNullsFiltersAndLocalGlobalPartials(
      String shape, String phase, boolean filtered) {
    String window = shape + "(TABLE src, DESCRIPTOR(rt), "
        + (shape.equals("TUMBLE") ? "" : "INTERVAL '1' SECOND, ")
        + "INTERVAL '3' SECOND)";
    String reductions = java.util.stream.Stream.of(
        "s", "t", "instant_value", "day_value", "time_value", "flag")
        .map(column -> "COUNT(" + column + ")" + (filtered ? " FILTER (WHERE id % 3 = 0)" : ""))
        .collect(java.util.stream.Collectors.joining(", "));
    String query = "SELECT k, window_start, window_end, " + reductions
        + (filtered ? "" : ", COUNT(*)") + " FROM TABLE(" + window
        + ") GROUP BY k, window_start, window_end";
    var result = NativeFailureParity.run(() -> {
      var table = FlinkOverBasicCountsSqlHarnessTest.environment(5003);
      table.getConfig().set("table.optimizer.agg-phase-strategy", phase);
      table.getConfig().set("table.exec.mini-batch.enabled", "true");
      table.getConfig().set("table.exec.mini-batch.allow-latency", "100 ms");
      table.getConfig().set("table.exec.mini-batch.size", "127");
      return table;
    }, query);
    result.assertSuccess(NativeFailureParity.Route.NATIVE);
    assertEquals(result.host().resultTypes(), result.nativeRun().resultTypes());
    assertTrue(result.nativeRun().plan().contains(phase.equals("TWO_PHASE")
        ? "NativeColumnarGlobalWindowAggregate" : "NativeColumnarWindowAggregate"), result::toString);
    if (phase.equals("TWO_PHASE")) {
      assertTrue(result.nativeRun().plan().contains("NativeColumnarLocalWindowAggregate"), result::toString);
    }
  }

  @ParameterizedTest
  @CsvSource({"TUMBLE,ONE_PHASE", "TUMBLE,TWO_PHASE", "HOP,ONE_PHASE", "HOP,TWO_PHASE",
      "CUMULATE,ONE_PHASE", "CUMULATE,TWO_PHASE"})
  void signedTypedCountsKeepNullOnlyGroupsAndRemoveEmptyGroups(String shape, String phase) {
    org.junit.jupiter.api.Assumptions.assumeTrue(
        tech.streamfusion.compat.FlinkTestCapabilities.RETRACTING_WINDOW_TVF);
    for (boolean filtered : new boolean[] {false, true}) {
      String window = shape + "(TABLE typed_changes, DESCRIPTOR(rt), "
          + (shape.equals("TUMBLE") ? "" : "INTERVAL '1' SECOND, ")
          + "INTERVAL '3' SECOND)";
      String reductions = java.util.stream.Stream.of(
          "CAST(v AS STRING)", "TO_TIMESTAMP_LTZ(v, 3)",
          "CAST(TO_TIMESTAMP_LTZ(v, 3) AS DATE)",
          "CAST(TO_TIMESTAMP_LTZ(v, 3) AS TIME(3))", "v > 10")
          .map(value -> "COUNT(" + value + ")" + (filtered ? " FILTER (WHERE v > 15)" : ""))
          .collect(java.util.stream.Collectors.joining(", "));
      String query = "SELECT k, window_start, window_end, " + reductions
          + " FROM TABLE(" + window + ") GROUP BY k, window_start, window_end";
      var result = NativeFailureParity.run(() -> {
        var env = StreamExecutionEnvironment
            .getExecutionEnvironment();
        env.setParallelism(1);
        var table = StreamTableEnvironment.create(env);
        Row[] rows = {
          Row.ofKind(RowKind.INSERT, 1, 10L, 100L),
          Row.ofKind(RowKind.INSERT, 1, null, 100L),
          Row.ofKind(RowKind.INSERT, 2, null, 100L),
          Row.ofKind(RowKind.INSERT, 3, 20L, 200L),
          Row.ofKind(RowKind.DELETE, 3, 20L, 200L),
          Row.ofKind(RowKind.UPDATE_BEFORE, 1, 10L, 100L),
          Row.ofKind(RowKind.UPDATE_AFTER, 1, 20L, 100L),
          Row.ofKind(RowKind.INSERT, 1, 20L, 300L),
          Row.ofKind(RowKind.DELETE, 1, 20L, 300L)
        };
        var input = FlinkTestSources.fromData(env,
            Types.ROW_NAMED(new String[] {"k", "v", "ts"},
                Types.INT,
                Types.LONG,
                Types.LONG), rows)
            .assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .<Row>forBoundedOutOfOrderness(Duration.ofDays(1))
                    .withTimestampAssigner((row, previous) -> (Long) row.getField(2)));
        table.createTemporaryView("typed_changes", table.fromChangelogStream(input,
            Schema.newBuilder()
                .column("k", DataTypes.INT())
                .column("v", DataTypes.BIGINT())
                .column("ts", DataTypes.BIGINT())
                .columnByMetadata("rt", DataTypes.TIMESTAMP_LTZ(3), "rowtime")
                .watermark("rt", "SOURCE_WATERMARK()").build()));
        table.getConfig().setLocalTimeZone(ZoneOffset.UTC);
        table.getConfig().set("table.optimizer.agg-phase-strategy", phase);
        table.getConfig().set("table.exec.mini-batch.enabled", "true");
        table.getConfig().set("table.exec.mini-batch.allow-latency", "1 h");
        table.getConfig().set("table.exec.mini-batch.size", "3");
        return table;
      }, query);
      result.assertSuccess(NativeFailureParity.Route.NATIVE);
      assertEquals(result.host().resultTypes(), result.nativeRun().resultTypes());
    }
  }
}
