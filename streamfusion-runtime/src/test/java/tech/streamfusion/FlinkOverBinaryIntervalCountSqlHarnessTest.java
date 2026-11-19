package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.time.Period;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.runtime.typeutils.ExternalTypeInfo;
import org.apache.flink.types.Row;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class FlinkOverBinaryIntervalCountSqlHarnessTest {
  @ParameterizedTest
  @CsvSource({
    "rt,ROWS UNBOUNDED PRECEDING",
    "rt,RANGE UNBOUNDED PRECEDING",
    "rt,ROWS BETWEEN 3 PRECEDING AND CURRENT ROW",
    "rt,RANGE BETWEEN INTERVAL '0.3' SECOND PRECEDING AND CURRENT ROW",
    "pt,ROWS UNBOUNDED PRECEDING",
    "pt,RANGE UNBOUNDED PRECEDING",
    "pt,ROWS BETWEEN 3 PRECEDING AND CURRENT ROW"
  })
  void ordinaryCountsUseValidityWithoutChangingBinaryOrIntervalPayloads(
      String order, String frame) {
    try (var metrics = new RecoveryExecutionMetrics()) {
      String window = " OVER (PARTITION BY k ORDER BY " + order + " " + frame + ")";
      var result =
          NativeFailureParity.run(
              () -> environment(metrics, 1031),
              "SELECT id, k, b, d, m, COUNT(b)"
                  + window
                  + ", COUNT(d)"
                  + window
                  + ", COUNT(m)"
                  + window
                  + ", COUNT(*)"
                  + window
                  + " FROM src");
      result.assertSuccess(NativeFailureParity.Route.NATIVE);
      assertEquals(result.host().resultTypes(), result.nativeRun().resultTypes());
      assertEquals(1031, result.nativeRun().rows().size());
      RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeOverAggregate");
    }
  }

  static StreamTableEnvironment environment(RecoveryExecutionMetrics metrics, int count) {
    var env = metrics.environment(new org.apache.flink.configuration.Configuration(), 1, 0);
    env.setParallelism(1);
    env.getConfig().setAutoWatermarkInterval(0);
    var table = StreamTableEnvironment.create(env);
    table.getConfig().set("streamfusion.transpose.batchRows", "127");
    int keys = Integer.getInteger("over.keys", 3);
    byte[] bytes = new byte[Integer.getInteger("over.width", 8)];
    java.util.Arrays.fill(bytes, (byte) 0xff);
    if (bytes.length > 0) bytes[0] = 0;
    var type =
        DataTypes.ROW(
            DataTypes.FIELD("id", DataTypes.BIGINT()),
            DataTypes.FIELD("k", DataTypes.INT()),
            DataTypes.FIELD("b", DataTypes.BYTES()),
            DataTypes.FIELD("d", DataTypes.INTERVAL(DataTypes.DAY(6), DataTypes.SECOND(3))),
            DataTypes.FIELD("m", DataTypes.INTERVAL(DataTypes.YEAR(4), DataTypes.MONTH())));
    var input =
        env.fromSequence(0, Math.max(0, count - 1))
            .filter(i -> i < count)
            .map(
                i ->
                    Row.of(
                        i,
                        (int) (i % keys),
                        i % 7 == 0 ? null : i % 5 == 0 ? new byte[0] : bytes,
                        i % 7 == 0 ? null : Duration.ofMillis((i % 5 - 2) * 1234),
                        i % 7 == 0 ? null : Period.ofMonths((int) (i % 7 - 3))))
            .returns(ExternalTypeInfo.<Row>of(type))
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<Row>forBoundedOutOfOrderness(Duration.ZERO)
                    .withTimestampAssigner(
                        (row, previous) -> 1000 + (Long) row.getField(0) / 3 * 100));
    table.createTemporaryView(
        "src",
        table.fromDataStream(
            input,
            Schema.newBuilder()
                .fromRowDataType(type)
                .columnByMetadata("rt", DataTypes.TIMESTAMP_LTZ(3), "rowtime")
                .watermark("rt", "SOURCE_WATERMARK()")
                .columnByExpression("pt", "PROCTIME()")
                .build()));
    return table;
  }
}
