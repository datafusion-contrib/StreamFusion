package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.runtime.typeutils.ExternalTypeInfo;
import org.apache.flink.types.Row;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.compat.FlinkTestSources;
import tech.streamfusion.planner.NativePlanner;

class FlinkFloatingNonTimeRankSqlHarnessTest {
  static java.util.stream.Stream<org.junit.jupiter.params.provider.Arguments> cases() {
    return java.util.stream.Stream.of("FLOAT", "DOUBLE")
        .flatMap(
            type ->
                java.util.stream.Stream.of("FIRST", "LAST")
                    .flatMap(
                        nulls ->
                            java.util.stream.Stream.of(1, 7)
                                .flatMap(
                                    batch ->
                                        java.util.stream.Stream.of(false, true)
                                            .map(
                                                ieee ->
                                                    org.junit.jupiter.params.provider.Arguments.of(
                                                        type, nulls, batch, ieee)))));
  }

  @ParameterizedTest
  @MethodSource("cases")
  void sortTiesAndReturnedPayloadBitsMatchReleasedFlink(
      String type, String nulls, int batch, boolean ieee) throws Exception {
    org.junit.jupiter.api.Assumptions.assumeTrue(
        tech.streamfusion.compat.FlinkTestCapabilities.NON_TIME_OVER);
    String sql =
        "SELECT k, v, id, ROW_NUMBER() OVER (PARTITION BY k ORDER BY v ASC NULLS "
            + nulls
            + ") AS rn FROM src";
    try (var metrics = new RecoveryExecutionMetrics()) {
      var host = environment(metrics, type, batch, ieee);
      var nativeTable = environment(metrics, type, batch, ieee);
      NativePlanner.install(nativeTable);
      assertTrue(nativeTable.explainSql(sql).contains("NativeColumnarTopN"));
      assertEquals(collect(host, sql, type), collect(nativeTable, sql, type));
      RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeColumnarTopN");
    }
  }

  static java.util.stream.Stream<org.junit.jupiter.params.provider.Arguments> recoveryCases() {
    return java.util.stream.Stream.of("FLOAT", "DOUBLE")
        .flatMap(
            type ->
                java.util.stream.Stream.of("FIRST", "LAST")
                    .flatMap(
                        nulls ->
                            java.util.stream.Stream.of(
                                    "hashmap",
                                    "tech.streamfusion.state.RocksDBNativeStateBackendFactory")
                                .map(
                                    backend ->
                                        org.junit.jupiter.params.provider.Arguments.of(
                                            type, nulls, backend))));
  }

  @ParameterizedTest
  @MethodSource("recoveryCases")
  void nanGroupsAndRowIdentitySurviveTwoRestores(String type, String nulls, String backend)
      throws Exception {
    org.junit.jupiter.api.Assumptions.assumeTrue(
        tech.streamfusion.compat.FlinkTestCapabilities.NON_TIME_OVER);
    boolean single = type.equals("FLOAT");
    var dataType =
        DataTypes.ROW(
            DataTypes.FIELD("k", DataTypes.INT()),
            DataTypes.FIELD("v", single ? DataTypes.FLOAT() : DataTypes.DOUBLE()),
            DataTypes.FIELD("id", DataTypes.INT()));
    var rows = new ArrayList<Row>();
    for (int i = 0; i < 96; i++) {
      Number value;
      if (single)
        value =
            Float.intBitsToFloat(
                new int[] {
                      0x7fc04321, 0x80000000, 0, 0xc0000000, 0xffc01234, 0x3f800000, 0xff800000, 1
                    }
                    [i % 8]);
      else
        value =
            Double.longBitsToDouble(
                new long[] {
                      0x7ff8000000004321L,
                      Long.MIN_VALUE,
                      0L,
                      0xc000000000000000L,
                      0xfff8000000001234L,
                      0x3ff0000000000000L,
                      0xfff0000000000000L,
                      1L
                    }
                    [i % 8]);
      rows.add(Row.of(i % 11 == 0 ? null : i % 3, i % 13 == 0 ? null : value, i));
    }
    try (var recovery =
        new PortableSqlRecovery(
                backend,
                rows,
                ExternalTypeInfo.<Row>of(dataType),
                Schema.newBuilder().fromRowDataType(dataType).build(),
                false,
                24,
                63)
            .withExecutionMetrics()) {
      java.util.function.UnaryOperator<org.apache.flink.table.api.TableEnvironment> config =
          table -> {
            table.getConfig().set("streamfusion.transpose.batchRows", "7");
            return table;
          };
      var result =
          NativeFailureParity.runRecovery(
              () -> config.apply(recovery.uninterrupted()),
              () -> config.apply(recovery.get()),
              "SELECT k, v, id, ROW_NUMBER() OVER (PARTITION BY k ORDER BY v ASC NULLS "
                  + nulls
                  + ") AS rn FROM recovery_input");
      result.assertSuccess(NativeFailureParity.Route.NATIVE);
      recovery.verifyRepeatedRecovery();
      RecoveryExecutionMetrics.assertConsumed(recovery.nativeExecution(), "NativeColumnarTopN");
      SharedFlinkCluster.assertNativeMemoryReleased("floating-non-time-rank-recovery");
    }
  }

  @ParameterizedTest
  @org.junit.jupiter.params.provider.CsvSource({
    "FLOAT,1,true",
    "DOUBLE,1,true",
    "FLOAT,3,false",
    "DOUBLE,3,false"
  })
  void miniBatchAndThreeWayShuffleKeepTheSameRankChanges(
      String type, int parallelism, boolean miniBatch) throws Exception {
    org.junit.jupiter.api.Assumptions.assumeTrue(
        tech.streamfusion.compat.FlinkTestCapabilities.NON_TIME_OVER);
    try (var metrics = new RecoveryExecutionMetrics()) {
      NativeParity.assertChangelogParity(
          () -> {
            var table = environment(metrics, type, 7, true, parallelism);
            if (miniBatch) {
              table.getConfig().set("table.exec.mini-batch.enabled", "true");
              table.getConfig().set("table.exec.mini-batch.allow-latency", "10 ms");
              table.getConfig().set("table.exec.mini-batch.size", "3");
            }
            return table;
          },
          "SELECT k, v, id, ROW_NUMBER() OVER (PARTITION BY k ORDER BY v ASC NULLS LAST) AS rn FROM"
              + " src");
      RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeColumnarTopN");
    }
  }

  @ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(strings = {"FLOAT", "DOUBLE"})
  void olderHostRetainsNonTimeValidation(String type) {
    org.junit.jupiter.api.Assumptions.assumeFalse(
        tech.streamfusion.compat.FlinkTestCapabilities.NON_TIME_OVER);
    try (var metrics = new RecoveryExecutionMetrics()) {
      var result =
          NativeFailureParity.run(
              () -> environment(metrics, type, 7, true),
              "SELECT ROW_NUMBER() OVER (PARTITION BY k ORDER BY v) FROM src");
      assertNotNull(result.host().failure());
      assertNotNull(result.nativeRun().failure());
      assertEquals(result.host().rootCause().getClass(), result.nativeRun().rootCause().getClass());
      assertEquals(
          result.host().rootCause().getMessage(), result.nativeRun().rootCause().getMessage());
      assertEquals(result.host().phase(), result.nativeRun().phase());
    }
  }

  private static List<String> collect(StreamTableEnvironment table, String sql, String type)
      throws Exception {
    var result = new ArrayList<String>();
    try (var iterator = table.executeSql(sql).collect()) {
      while (iterator.hasNext()) {
        var row = iterator.next();
        Object value = row.getField(1);
        String bits =
            value == null
                ? "null"
                : type.equals("FLOAT")
                    ? Integer.toHexString(Float.floatToRawIntBits((Float) value))
                    : Long.toHexString(Double.doubleToRawLongBits((Double) value));
        result.add(
            row.getKind()
                + ":"
                + row.getField(0)
                + ":"
                + bits
                + ":"
                + row.getField(2)
                + ":"
                + row.getField(3));
      }
    }
    return result;
  }

  private static StreamTableEnvironment environment(
      RecoveryExecutionMetrics metrics, String type, int batch, boolean ieee) {
    return environment(metrics, type, batch, ieee, 1);
  }

  private static StreamTableEnvironment environment(
      RecoveryExecutionMetrics metrics, String type, int batch, boolean ieee, int parallelism) {
    boolean single = type.equals("FLOAT");
    var dataType =
        DataTypes.ROW(
            DataTypes.FIELD("k", DataTypes.INT()),
            DataTypes.FIELD("v", single ? DataTypes.FLOAT() : DataTypes.DOUBLE()),
            DataTypes.FIELD("id", DataTypes.INT()));
    var values = new ArrayList<Number>();
    if (single) {
      for (int bits :
          new int[] {
            0x3f800000,
            0x80000000,
            0x00000000,
            0xc0000000,
            0x7f800000,
            0xff800000,
            0x00000001,
            0x80000001,
            0x7f7fffff,
            0xff7fffff
          }) values.add(Float.intBitsToFloat(bits));
      if (ieee) {
        values.add(0, Float.intBitsToFloat(0xffc01234));
        values.add(1, Float.intBitsToFloat(0x7fc00001));
        values.add(4, Float.intBitsToFloat(0x7fc04321));
        values.add(Float.NaN);
      }
    } else {
      for (long bits :
          new long[] {
            0x3ff0000000000000L,
            0x8000000000000000L,
            0L,
            0xc000000000000000L,
            0x7ff0000000000000L,
            0xfff0000000000000L,
            1L,
            0x8000000000000001L,
            0x7fefffffffffffffL,
            0xffefffffffffffffL
          }) values.add(Double.longBitsToDouble(bits));
      if (ieee) {
        values.add(0, Double.longBitsToDouble(0xfff8000000001234L));
        values.add(1, Double.longBitsToDouble(0x7ff8000000000001L));
        values.add(4, Double.longBitsToDouble(0x7ff8000000004321L));
        values.add(Double.NaN);
      }
    }
    var rows = new ArrayList<Row>();
    for (int i = 0; i < values.size() * 3; i++)
      rows.add(
          Row.of(i % 7 == 0 ? null : i % 2, i % 11 == 0 ? null : values.get(i % values.size()), i));
    var env =
        metrics.environment(new org.apache.flink.configuration.Configuration(), parallelism, 0);
    var table = StreamTableEnvironment.create(env);
    table.getConfig().set("streamfusion.transpose.batchRows", Integer.toString(batch));
    var source =
        FlinkTestSources.fromData(env, ExternalTypeInfo.<Row>of(dataType), rows.toArray(new Row[0]))
            .setParallelism(1);
    table.createTemporaryView("src", source, Schema.newBuilder().fromRowDataType(dataType).build());
    return table;
  }
}
