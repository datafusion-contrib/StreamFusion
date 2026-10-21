package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class FlinkOverPrimitiveAveragesSqlHarnessTest {
  @ParameterizedTest
  @ValueSource(
      strings = {
        "rt ROWS UNBOUNDED PRECEDING",
        "rt RANGE UNBOUNDED PRECEDING",
        "rt ROWS BETWEEN 3 PRECEDING AND CURRENT ROW",
        "rt RANGE BETWEEN INTERVAL '1' SECOND PRECEDING AND CURRENT ROW",
        "pt ROWS UNBOUNDED PRECEDING",
        "pt RANGE UNBOUNDED PRECEDING",
        "pt ROWS BETWEEN 3 PRECEDING AND CURRENT ROW"
      })
  void primitiveAverageFramesRetainResultWidthsAndDistinctMembership(String frame) {
    try (var metrics = new RecoveryExecutionMetrics()) {
      String reductions =
          Stream.of("t", "s", "i", "b", "f", "d")
              .flatMap(
                  value ->
                      Stream.of("AVG(" + value + ") OVER w", "AVG(DISTINCT " + value + ") OVER w"))
              .collect(Collectors.joining(", "));
      var comparison =
          NativeFailureParity.run(
              () -> environment(metrics),
              "SELECT id, k, "
                  + reductions
                  + " FROM src WINDOW w AS (PARTITION BY k ORDER BY "
                  + frame
                  + ")");
      comparison.assertSuccess(NativeFailureParity.Route.NATIVE);
      assertEquals(comparison.host().resultTypes(), comparison.nativeRun().resultTypes());
      RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeOverAggregate");
    }
  }

  private static TableEnvironment environment(RecoveryExecutionMetrics metrics) {
    var env = metrics.environment(new Configuration(), 1, 0);
    var table = StreamTableEnvironment.create(env);
    table.getConfig().set("streamfusion.transpose.batchRows", "7");
    var source =
        env.fromSequence(0, 95)
            .map(
                id -> {
                  long[] values = {Long.MAX_VALUE, 7, Long.MIN_VALUE, -7, 1, 0, -1, 3};
                  Long value = id % 5 == 0 ? null : values[(int) (id % values.length)];
                  return Row.of(
                      id.intValue(),
                      id % 11 == 0 ? null : (int) (id % 3),
                      value == null ? null : value.byteValue(),
                      value == null ? null : value.shortValue(),
                      value == null ? null : value.intValue(),
                      value,
                      value == null ? null : value.floatValue(),
                      value == null ? null : value.doubleValue(),
                      LocalDateTime.ofEpochSecond(
                          1 + id / 9, (int) (id % 9 * 1_000_000), ZoneOffset.UTC));
                })
            .returns(
                Types.ROW_NAMED(
                    new String[] {"id", "k", "t", "s", "i", "b", "f", "d", "rt"},
                    Types.INT,
                    Types.INT,
                    Types.BYTE,
                    Types.SHORT,
                    Types.INT,
                    Types.LONG,
                    Types.FLOAT,
                    Types.DOUBLE,
                    Types.LOCAL_DATE_TIME))
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<Row>forBoundedOutOfOrderness(Duration.ofDays(1))
                    .withTimestampAssigner(
                        (row, previous) ->
                            ((LocalDateTime) row.getField(8))
                                .toInstant(ZoneOffset.UTC)
                                .toEpochMilli()));
    table.createTemporaryView(
        "src",
        source,
        Schema.newBuilder()
            .column("id", DataTypes.INT())
            .column("k", DataTypes.INT())
            .column("t", DataTypes.TINYINT())
            .column("s", DataTypes.SMALLINT())
            .column("i", DataTypes.INT())
            .column("b", DataTypes.BIGINT())
            .column("f", DataTypes.FLOAT())
            .column("d", DataTypes.DOUBLE())
            .column("rt", DataTypes.TIMESTAMP(3))
            .watermark("rt", "SOURCE_WATERMARK()")
            .columnByExpression("pt", "PROCTIME()")
            .build());
    return table;
  }
}
