package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;
import static tech.streamfusion.compat.FlinkTestSources.fromData;

import java.time.*;
import java.util.*;
import java.util.stream.*;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.runtime.typeutils.ExternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.*;

public class FlinkScalarKeySqlHarnessTest {
  static Stream<String> types() {
    return Stream.of(
        System.getProperty(
                "scalar.keys.types",
                "tinyint,smallint,time,time0,binary,varbinary,months,millis,float,double")
            .split(","));
  }

  static Stream<Arguments> cases() {
    return types()
        .flatMap(
            type ->
                Stream.of(
                        System.getProperty(
                                "scalar.keys.shapes",
                                "tumble,hop,cumulate,session,over_rows,over_range,over_bounded_rows,over_bounded_range,over_proc_rows,over_proc_range,over_proc_bounded,interval,window_join,temporal")
                            .split(","))
                    .map(shape -> Arguments.of(type, shape)));
  }

  @ParameterizedTest
  @MethodSource("cases")
  void typedCompositeKeysKeepEqualityAndOutputTypes(String type, String shape) {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var runs = NativeFailureParity.run(() -> environment(metrics, type), typedSql(type, shape));
      runs.assertSuccess(
          Boolean.getBoolean("scalar.keys.expectFallback")
              ? NativeFailureParity.Route.FALLBACK
              : NativeFailureParity.Route.NATIVE);
      assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
      assertFalse(runs.host().rows().isEmpty());
      if (!Boolean.getBoolean("scalar.keys.expectFallback"))
        RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), operator(shape));
    }
  }

  @ParameterizedTest
  @MethodSource("types")
  void nonTimeRankingKeepsTypedPartitions(String type) {
    org.junit.jupiter.api.Assumptions.assumeTrue(
        tech.streamfusion.compat.FlinkTestCapabilities.NON_TIME_OVER);
    try (var metrics = new RecoveryExecutionMetrics()) {
      var runs =
          NativeFailureParity.run(
              () -> environment(metrics, type),
              "SELECT id,k,shard,ROW_NUMBER() OVER (PARTITION BY k,shard ORDER BY v ASC) FROM src");
      runs.assertSuccess(NativeFailureParity.Route.NATIVE);
      assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
      RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeColumnarTopN");
    }
  }

  static Stream<Arguments> twoPhase() {
    return types()
        .flatMap(
            type -> Stream.of("tumble", "hop", "cumulate").map(shape -> Arguments.of(type, shape)));
  }

  @ParameterizedTest
  @MethodSource("twoPhase")
  void localGlobalPartialsKeepTypedKeys(String type, String shape) {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var runs =
          NativeFailureParity.run(
              () -> {
                var table = environment(metrics, type);
                table.getConfig().set("table.optimizer.agg-phase-strategy", "TWO_PHASE");
                table.getConfig().set("table.exec.mini-batch.enabled", "true");
                table.getConfig().set("table.exec.mini-batch.allow-latency", "1 h");
                table.getConfig().set("table.exec.mini-batch.size", "7");
                return table;
              },
              typedSql(type, shape));
      runs.assertSuccess(NativeFailureParity.Route.NATIVE);
      assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
      assertTrue(
          runs.nativeRun().plan().contains("NativeColumnarLocalWindowAggregate"),
          runs.nativeRun().plan());
      RecoveryExecutionMetrics.assertConsumed(
          metrics.nativeOperators(), "NativeColumnarGlobalWindowAggregate");
    }
  }

  static Stream<Arguments> recoveryCases() {
    return types()
        .flatMap(
            type ->
                Stream.of(false, true)
                    .flatMap(
                        rocks ->
                            Stream.of(
                                    "tumble",
                                    "session",
                                    "over_bounded_rows",
                                    "interval",
                                    "window_join")
                                .map(shape -> Arguments.of(type, rocks, shape))));
  }

  @ParameterizedTest
  @MethodSource("recoveryCases")
  void retainedKeysSurviveTwoRestores(String type, boolean rocks, String shape) throws Exception {
    var dtype =
        DataTypes.ROW(
            DataTypes.FIELD("id", DataTypes.BIGINT()),
            DataTypes.FIELD("k", keyType(type)),
            DataTypes.FIELD("shard", DataTypes.INT()),
            DataTypes.FIELD("v", DataTypes.BIGINT()),
            DataTypes.FIELD("rt", DataTypes.TIMESTAMP_LTZ(3)));
    var rows =
        LongStream.range(0, 80)
            .mapToObj(
                i ->
                    Row.of(
                        i,
                        i % 11 == 0 ? null : key(type, i),
                        (int) (i % 2),
                        i % 7 == 0 ? null : i,
                        Instant.ofEpochMilli(1000 + i / 8 * 100)))
            .collect(tech.streamfusion.compat.ListCollectors.toList());
    var schema =
        Schema.newBuilder()
            .fromRowDataType(dtype)
            .watermark("rt", "SOURCE_WATERMARK()")
            .columnByExpression("pt", "PROCTIME()")
            .build();
    try (var recovery =
        new PortableSqlRecovery(
                rocks ? "tech.streamfusion.state.RocksDBNativeStateBackendFactory" : "hashmap",
                rows,
                ExternalTypeInfo.<Row>of(dtype),
                schema,
                false,
                40,
                64)
            .withWatermarks(Map.of(24, 1200L, 56, 1600L, 80, 9999L))
            .withExecutionMetrics()) {
      if (type.equals("float") || type.equals("double"))
        recovery.withInputMapping(row -> rawFloatingKey(row, type));
      java.util.function.UnaryOperator<TableEnvironment> configure =
          table -> {
            table.getConfig().set("streamfusion.transpose.batchRows", "7");
            table.getConfig().set("table.optimizer.agg-phase-strategy", "ONE_PHASE");
            registerKeyBits(table, type);
            table.createTemporaryView("src", table.from("recovery_input"));
            return table;
          };
      var runs =
          NativeFailureParity.runRecovery(
              () -> configure.apply(recovery.uninterrupted()),
              () -> configure.apply(recovery.get()),
              typedSql(type, shape));
      runs.assertSuccess(NativeFailureParity.Route.NATIVE);
      assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
      recovery.verifyRepeatedRecovery();
      RecoveryExecutionMetrics.assertConsumed(recovery.nativeExecution(), operator(shape));
      SharedFlinkCluster.assertNativeMemoryReleased("typed-scalar-key-recovery");
    }
  }

  static DataType keyType(String type) {
    switch (type) {
      case "float":
        return DataTypes.FLOAT();
      case "double":
        return DataTypes.DOUBLE();
      case "tinyint":
        return DataTypes.TINYINT();
      case "smallint":
        return DataTypes.SMALLINT();
      case "time":
        return DataTypes.TIME(3);
      case "time0":
        return DataTypes.TIME(0);
      case "binary":
        return DataTypes.BINARY(4);
      case "varbinary":
        return DataTypes.BYTES();
      case "months":
        return DataTypes.INTERVAL(DataTypes.YEAR(4), DataTypes.MONTH());
      case "millis":
        return DataTypes.INTERVAL(DataTypes.DAY(3), DataTypes.SECOND(3));
      default:
        throw new IllegalArgumentException(type);
    }
  }

  static Object key(String type, long row) {
    int i = (int) (row / 2 % 5);
    switch (type) {
      case "float":
        return Float.intBitsToFloat(
            new int[] {
                  0x80000000, 0, 0x7fc00001, 0x7fc00002, 0xffc00001,
                  0x7f800000, 0xff800000, 1, 0x80000001, 0x3f800000
                }
                [(int) (row / 2 % 10)]);
      case "double":
        return Double.longBitsToDouble(
            new long[] {
                  0x8000000000000000L,
                  0,
                  0x7ff8000000000001L,
                  0x7ff8000000000002L,
                  0xfff8000000000001L,
                  0x7ff0000000000000L,
                  0xfff0000000000000L,
                  1,
                  0x8000000000000001L,
                  0x3ff0000000000000L
                }
                [(int) (row / 2 % 10)]);
      case "tinyint":
        return new byte[] {Byte.MIN_VALUE, -1, 0, 1, Byte.MAX_VALUE}[i];
      case "smallint":
        return new short[] {Short.MIN_VALUE, -1, 0, 1, Short.MAX_VALUE}[i];
      case "time":
      case "time0":
        return LocalTime.ofNanoOfDay(
            new long[] {0, 1, 123, 45_296_123, 86_399_999}[i] * 1_000_000L);
      case "binary":
      case "varbinary":
        byte[] value = new byte[new int[] {0, 1, 4, 9, 4097}[i]];
        for (int j = 0; j < value.length; j++) value[j] = (byte) (i + j);
        return value;
      case "months":
        return Period.ofMonths(new int[] {-1200, -1, 0, 1, 1200}[i]);
      case "millis":
        return Duration.ofMillis(new long[] {-86_400_123, -1, 0, 1, 86_400_123}[i]);
      default:
        throw new IllegalArgumentException(type);
    }
  }

  private static Row rawFloatingKey(Row row, String type) {
    Row copy = Row.copy(row);
    long id = (Long) row.getField(0);
    copy.setField(1, id % 11 == 0 ? null : key(type, id));
    return copy;
  }

  static TableEnvironment environment(RecoveryExecutionMetrics metrics, String type) {
    boolean floating = type.equals("float") || type.equals("double");
    var env =
        metrics.environment(
            new org.apache.flink.configuration.Configuration(), floating ? 1 : 3, 0);
    env.getConfig().setAutoWatermarkInterval(0);
    env.getConfig().enableObjectReuse();
    var table = StreamTableEnvironment.create(env);
    registerKeyBits(table, type);
    table.getConfig().set("table.optimizer.agg-phase-strategy", "ONE_PHASE");
    table.getConfig().set("streamfusion.transpose.batchRows", "7");
    var dtype =
        DataTypes.ROW(
            DataTypes.FIELD("id", DataTypes.BIGINT()),
            DataTypes.FIELD("k", keyType(type)),
            DataTypes.FIELD("shard", DataTypes.INT()),
            DataTypes.FIELD("v", DataTypes.BIGINT()),
            DataTypes.FIELD("rt", DataTypes.TIMESTAMP_LTZ(3)));
    var rows =
        LongStream.range(0, 80)
            .mapToObj(
                i ->
                    Row.of(
                        i,
                        i % 11 == 0 ? null : key(type, i),
                        (int) (i % 2),
                        i % 7 == 0 ? null : i,
                        Instant.ofEpochMilli(1000 + i / 8 * 100)))
            .collect(tech.streamfusion.compat.ListCollectors.toList());
    org.apache.flink.streaming.api.datastream.DataStream<Row> input =
        fromData(env, rows, ExternalTypeInfo.<Row>of(dtype));
    if (floating) {
      // Source serialization canonicalizes NaNs; create the raw payloads after deserialization.
      input = input.map(row -> rawFloatingKey(row, type)).returns(ExternalTypeInfo.<Row>of(dtype));
    }
    table.createTemporaryView(
        "src",
        table.fromDataStream(
            input,
            Schema.newBuilder()
                .fromRowDataType(dtype)
                .watermark("rt", "rt - INTERVAL '1' SECOND")
                .columnByExpression("pt", "PROCTIME()")
                .build()));
    if (type != null) {
      var dimType =
          DataTypes.ROW(
              DataTypes.FIELD("k", keyType(type).notNull()),
              DataTypes.FIELD("shard", DataTypes.INT().notNull()),
              DataTypes.FIELD("v", DataTypes.BIGINT()),
              DataTypes.FIELD("rt", DataTypes.TIMESTAMP_LTZ(3)));
      var dims =
          LongStream.range(0, 20)
              .mapToObj(
                  i -> Row.of(key(type, i), (int) (i % 2), i, Instant.ofEpochMilli(900 + i * 100)))
              .collect(tech.streamfusion.compat.ListCollectors.toList());
      org.apache.flink.streaming.api.datastream.DataStream<Row> dimension =
          fromData(env, dims, ExternalTypeInfo.<Row>of(dimType));
      if (floating) {
        dimension =
            dimension
                .map(
                    row -> {
                      Row copy = Row.copy(row);
                      copy.setField(0, key(type, (Long) row.getField(2)));
                      return copy;
                    })
                .returns(ExternalTypeInfo.<Row>of(dimType));
      }
      table.createTemporaryView(
          "dim",
          table.fromDataStream(
              dimension,
              Schema.newBuilder()
                  .fromRowDataType(dimType)
                  .watermark("rt", "rt - INTERVAL '1' SECOND")
                  .primaryKey("k", "shard")
                  .build()));
    }
    return table;
  }

  private static void registerKeyBits(TableEnvironment table, String type) {
    if (type.equals("float")) table.createTemporarySystemFunction("key_bits", new FloatBits());
    if (type.equals("double")) table.createTemporarySystemFunction("key_bits", new DoubleBits());
  }

  public static final class FloatBits extends org.apache.flink.table.functions.ScalarFunction {
    public Long eval(Float value) {
      return value == null ? null : Integer.toUnsignedLong(Float.floatToRawIntBits(value));
    }
  }

  public static final class DoubleBits extends org.apache.flink.table.functions.ScalarFunction {
    public Long eval(Double value) {
      return value == null ? null : Double.doubleToRawLongBits(value);
    }
  }

  private static String typedSql(String type, String shape) {
    String query = sql(shape);
    return type.equals("float") || type.equals("double")
        ? "SELECT *,key_bits(k) AS raw_key_bits FROM (" + query + ")"
        : query;
  }

  static String window(String shape) {
    return shape.equals("tumble")
        ? "TUMBLE(TABLE src,DESCRIPTOR(rt),INTERVAL '1' SECOND)"
        : shape.equals("hop")
            ? "HOP(TABLE src,DESCRIPTOR(rt),INTERVAL '0.5' SECOND,INTERVAL '1' SECOND)"
            : "CUMULATE(TABLE src,DESCRIPTOR(rt),INTERVAL '0.5' SECOND,INTERVAL '1' SECOND)";
  }

  static String sql(String shape) {
    if (shape.startsWith("over_")) {
      String order = shape.contains("proc") ? "pt" : "rt";
      String frame =
          shape.equals("over_bounded_range")
              ? "RANGE BETWEEN INTERVAL '0.2' SECOND PRECEDING AND CURRENT ROW"
              : shape.contains("bounded")
                  ? "ROWS BETWEEN 2 PRECEDING AND CURRENT ROW"
                  : shape.endsWith("range")
                      ? "RANGE UNBOUNDED PRECEDING"
                      : "ROWS UNBOUNDED PRECEDING";
      return "SELECT id,k,shard,COUNT(v) OVER w,SUM(v) OVER w FROM src WINDOW w AS (PARTITION BY"
          + " k,shard ORDER BY "
          + order
          + " "
          + frame
          + ")";
    }
    if (shape.equals("session"))
      return "SELECT k,shard,COUNT(*),SESSION_START(rt,INTERVAL '0.2'"
          + " SECOND),SESSION_END(rt,INTERVAL '0.2' SECOND) FROM src GROUP BY"
          + " k,shard,SESSION(rt,INTERVAL '0.2' SECOND)";
    if (shape.equals("interval"))
      return "SELECT a.id AS left_id,b.id AS right_id,a.k,a.shard FROM src a JOIN src b ON a.k=b.k AND a.shard=b.shard AND"
          + " a.rt BETWEEN b.rt - INTERVAL '0.1' SECOND AND b.rt + INTERVAL '0.1' SECOND";
    if (shape.equals("window_join"))
      return "SELECT a.id AS left_id,b.id AS right_id,a.k,a.shard FROM (SELECT * FROM TABLE(TUMBLE(TABLE"
          + " src,DESCRIPTOR(rt),INTERVAL '1' SECOND))) a JOIN (SELECT * FROM"
          + " TABLE(TUMBLE(TABLE src,DESCRIPTOR(rt),INTERVAL '1' SECOND))) b ON a.k=b.k AND"
          + " a.shard=b.shard AND a.window_start=b.window_start AND"
          + " a.window_end=b.window_end";
    if (shape.equals("temporal"))
      return "SELECT f.id,f.k,f.shard,d.v FROM src f LEFT JOIN dim FOR SYSTEM_TIME AS OF f.rt AS d"
          + " ON f.k=d.k AND f.shard=d.shard";
    return "SELECT k,shard,window_start,window_end,COUNT(*),SUM(v) FROM TABLE("
        + window(shape)
        + ") GROUP BY k,shard,window_start,window_end";
  }

  static String operator(String shape) {
    if (shape.startsWith("over_")) return "NativeOverAggregate";
    if (shape.equals("interval")) return "NativeIntervalJoin";
    if (shape.equals("window_join")) return "NativeWindowJoin";
    if (shape.equals("temporal")) return "NativeTemporalJoin";
    if (shape.equals("session")) return "NativeColumnarSessionWindowAgg";
    return "NativeColumnarWindowAgg";
  }
}
