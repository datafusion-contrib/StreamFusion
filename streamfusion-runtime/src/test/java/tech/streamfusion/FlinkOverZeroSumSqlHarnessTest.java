package tech.streamfusion;

import static org.apache.flink.table.api.Expressions.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static tech.streamfusion.compat.FlinkTestSources.fromData;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.ApiExpression;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Over;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.runtime.typeutils.ExternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.planner.NativePlanner;

class FlinkOverZeroSumSqlHarnessTest {
  static Stream<Arguments> cases() {
    return Stream.of("FLOAT", "DOUBLE", "INT", "BIGINT", "SMALLINT", "TINYINT")
        .flatMap(
            type ->
                Stream.of(
                        "ROWS",
                        "RANGE",
                        "BOUNDED_ROWS",
                        "BOUNDED_RANGE",
                        "PROC_ROWS",
                        "PROC_RANGE",
                        "PROC_BOUNDED_ROWS")
                    .flatMap(
                        frame ->
                            Stream.of(false, true)
                                .map(distinct -> Arguments.of(type, frame, distinct))));
  }

  @ParameterizedTest
  @MethodSource("cases")
  void zeroInitializedOverSumsKeepNullOnlyPrefixesAndPositiveZero(
      String type, String frame, boolean distinct) {
    try (var metrics = new RecoveryExecutionMetrics()) {
      if (!distinct && flink118()) {
        var host = environment(metrics, type, frame, false);
        var expected =
            assertThrows(
                AssertionError.class, () -> host.sqlQuery("SELECT * FROM over_zero_result"));
        var nativeTable = environment(metrics, type, frame, false);
        NativePlanner.install(nativeTable);
        var actual =
            assertThrows(
                AssertionError.class, () -> nativeTable.sqlQuery("SELECT * FROM over_zero_result"));
        assertTrue(
            expected
                .getMessage()
                .startsWith("Conversion to relational algebra failed to preserve datatypes:"));
        assertEquals(expected.getMessage().split("rel:")[0], actual.getMessage().split("rel:")[0]);
        return;
      }
      var runs =
          NativeFailureParity.run(
              () -> environment(metrics, type, frame, distinct), "SELECT * FROM over_zero_result");
      runs.assertSuccess(NativeFailureParity.Route.NATIVE);
      assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
      assertEquals(10, runs.nativeRun().rows().size());
      RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeOverAggregate");
    }
  }

  static Stream<Arguments> recoveryCases() {
    return Stream.of("FLOAT", "DOUBLE")
        .flatMap(
            type ->
                Stream.of(false, true)
                    .flatMap(
                        rocks ->
                            Stream.of("BOUNDED_ROWS", "BOUNDED_RANGE")
                                .flatMap(
                                    frame ->
                                        Stream.of(false, true)
                                            .filter(distinct -> distinct || !flink118())
                                            .map(
                                                distinct ->
                                                    Arguments.of(type, rocks, frame, distinct)))));
  }

  @ParameterizedTest
  @MethodSource("recoveryCases")
  void boundedZeroSumsKeepRoundingHistoryThroughTwoRecoveries(
      String type, boolean rocks, String frame, boolean distinct) throws Exception {
    var rowType =
        DataTypes.ROW(
            DataTypes.FIELD("id", DataTypes.BIGINT()),
            DataTypes.FIELD("k", DataTypes.INT()),
            DataTypes.FIELD("v", type.equals("FLOAT") ? DataTypes.FLOAT() : DataTypes.DOUBLE()),
            DataTypes.FIELD("rt", DataTypes.TIMESTAMP_LTZ(3)));
    double high = type.equals("FLOAT") ? 16_777_216.0 : 9_007_199_254_740_992.0;
    Double[] values = {null, null, null, high, 1.0, -high, null, high, 1.0, -high, null};
    var input =
        IntStream.range(0, values.length)
            .mapToObj(
                id ->
                    Row.of(
                        (long) id,
                        1,
                        values[id] == null ? null : number(type, values[id]),
                        Instant.ofEpochMilli(1000L + id * 100L)))
            .collect(tech.streamfusion.compat.ListCollectors.toList());
    var schema =
        Schema.newBuilder().fromRowDataType(rowType).watermark("rt", "SOURCE_WATERMARK()").build();
    try (var recovery =
        new PortableSqlRecovery(
                rocks ? "tech.streamfusion.state.RocksDBNativeStateBackendFactory" : "hashmap",
                input,
                ExternalTypeInfo.<Row>of(rowType),
                schema,
                false,
                3,
                6)
            .withWatermarks(Map.of(3, 1200L, 6, 1500L, 11, 2200L))
            .withExecutionMetrics()) {
      UnaryOperator<TableEnvironment> configure =
          table -> resultView(table, "recovery_input", frame, distinct);
      var runs =
          NativeFailureParity.runRecovery(
              () -> configure.apply(recovery.uninterrupted()),
              () -> configure.apply(recovery.get()),
              "SELECT * FROM over_zero_result");
      runs.assertSuccess(NativeFailureParity.Route.NATIVE);
      assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
      recovery.verifyRepeatedRecovery();
      RecoveryExecutionMetrics.assertConsumed(recovery.nativeExecution(), "NativeOverAggregate");
      SharedFlinkCluster.assertNativeMemoryReleased("over-zero-sum-recovery");
    }
  }

  private static boolean flink118() {
    return org.apache.flink.runtime.util.EnvironmentInformation.getVersion().startsWith("1.18");
  }

  private static TableEnvironment environment(
      RecoveryExecutionMetrics metrics, String type, String frame, boolean distinct) {
    StreamExecutionEnvironment env =
        metrics.environment(new org.apache.flink.configuration.Configuration(), 1, 0);
    env.getConfig().setAutoWatermarkInterval(0);
    var table = StreamTableEnvironment.create(env);
    DataType valueType;
    switch (type) {
      case "FLOAT":
        valueType = DataTypes.FLOAT();
        break;
      case "DOUBLE":
        valueType = DataTypes.DOUBLE();
        break;
      case "TINYINT":
        valueType = DataTypes.TINYINT();
        break;
      case "SMALLINT":
        valueType = DataTypes.SMALLINT();
        break;
      case "INT":
        valueType = DataTypes.INT();
        break;
      case "BIGINT":
        valueType = DataTypes.BIGINT();
        break;
      default:
        throw new IllegalArgumentException(type);
    }
    var rowType =
        DataTypes.ROW(
            DataTypes.FIELD("id", DataTypes.BIGINT()),
            DataTypes.FIELD("k", DataTypes.INT()),
            DataTypes.FIELD("v", valueType),
            DataTypes.FIELD("rt", DataTypes.TIMESTAMP_LTZ(3)));
    double high = type.equals("FLOAT") ? 16_777_216.0 : 9_007_199_254_740_992.0;
    var input =
        List.of(
            Row.of(0L, 1, number(type, -0.0), Instant.ofEpochMilli(1000)),
            Row.of(1L, 2, null, Instant.ofEpochMilli(1100)),
            Row.of(2L, 1, null, Instant.ofEpochMilli(1200)),
            Row.of(3L, 1, number(type, 0.0), Instant.ofEpochMilli(1300)),
            Row.of(4L, 2, null, Instant.ofEpochMilli(1400)),
            Row.of(5L, 3, number(type, 1.0), Instant.ofEpochMilli(1500)),
            Row.of(6L, 4, number(type, high), Instant.ofEpochMilli(1600)),
            Row.of(7L, 4, number(type, 1.0), Instant.ofEpochMilli(1700)),
            Row.of(8L, 4, number(type, -high), Instant.ofEpochMilli(1800)),
            Row.of(9L, 4, null, Instant.ofEpochMilli(1900)));
    table.createTemporaryView(
        "src",
        table.fromDataStream(
            fromData(env, input, ExternalTypeInfo.<Row>of(rowType)),
            Schema.newBuilder()
                .fromRowDataType(rowType)
                .columnByExpression("pt", "PROCTIME()")
                .watermark("rt", "rt - INTERVAL '1' SECOND")
                .build()));
    return resultView(table, "src", frame, distinct);
  }

  private static TableEnvironment resultView(
      TableEnvironment table, String source, String frame, boolean distinct) {
    ApiExpression sum = $("v").sum0();
    if (distinct) sum = sum.distinct();
    table.createTemporaryView(
        "over_zero_result",
        table
            .from(source)
            .window(
                Over.partitionBy($("k"))
                    .orderBy($(frame.startsWith("PROC") ? "pt" : "rt"))
                    .preceding(
                        frame.equals("ROWS") || frame.equals("PROC_ROWS")
                            ? UNBOUNDED_ROW
                            : (frame.equals("RANGE") || frame.equals("PROC_RANGE"))
                                ? UNBOUNDED_RANGE
                                : frame.endsWith("BOUNDED_ROWS")
                                    ? rowInterval(2L)
                                    : lit(200L).millis())
                    .as("w"))
            .select($("id"), $("k"), sum.over($("w")).as("total")));
    return table;
  }

  private static Object number(String type, double value) {
    switch (type) {
      case "FLOAT":
        return Float.valueOf((float) value);
      case "DOUBLE":
        return Double.valueOf(value);
      case "TINYINT":
        return Byte.valueOf((byte) value);
      case "SMALLINT":
        return Short.valueOf((short) value);
      case "INT":
        return Integer.valueOf((int) value);
      case "BIGINT":
        return Long.valueOf((long) value);
      default:
        throw new IllegalArgumentException(type);
    }
  }
}
