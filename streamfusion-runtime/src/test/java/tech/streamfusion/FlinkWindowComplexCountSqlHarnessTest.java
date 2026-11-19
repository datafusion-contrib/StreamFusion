package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.junit.jupiter.params.ParameterizedTest;
import tech.streamfusion.compat.FlinkTestSources;

class FlinkWindowComplexCountSqlHarnessTest {
  static java.util.stream.Stream<org.junit.jupiter.params.provider.Arguments> eventTimeCases() {
    var cases = new java.util.ArrayList<org.junit.jupiter.params.provider.Arguments>();
    for (String type : new String[] {"array", "map", "row", "multiset"})
      for (String shape : new String[] {"TUMBLE", "HOP", "CUMULATE"})
        for (String phase : new String[] {"ONE_PHASE", "TWO_PHASE"})
          for (boolean filtered : new boolean[] {false, true})
            cases.add(org.junit.jupiter.params.provider.Arguments.of(type, shape, phase, filtered));
    return cases.stream();
  }

  @ParameterizedTest
  @org.junit.jupiter.params.provider.MethodSource("eventTimeCases")
  void complexValuesCountOnlyParentNulls(
      String type, String shape, String phase, boolean filtered) {
    String window =
        shape
            + "(TABLE src, DESCRIPTOR(rt), "
            + (shape.equals("TUMBLE") ? "" : "INTERVAL '1' SECOND, ")
            + "INTERVAL '3' SECOND)";
    var comparison =
        NativeFailureParity.run(
            () -> environment(type, phase),
            "SELECT k, window_start, window_end, COUNT(v)"
                + (filtered ? " FILTER (WHERE ts <> 200)" : "")
                + ", COUNT(*) FROM TABLE("
                + window
                + ") GROUP BY k, window_start, window_end");
    comparison.assertSuccess(NativeFailureParity.Route.NATIVE);
    assertEquals(comparison.host().resultTypes(), comparison.nativeRun().resultTypes());
    assertFalse(comparison.host().rows().isEmpty());
    for (var result : comparison.host().rows()) {
      int key = ((Number) result.get(1)).intValue();
      assertEquals(key == 1 ? (filtered ? 2L : 3L) : key == 2 ? 0L : 1L, result.get(4));
      assertEquals(key == 1 ? 4L : key == 2 ? 2L : 1L, result.get(5));
    }
    assertTrue(
        comparison
            .nativeRun()
            .plan()
            .contains(
                phase.equals("TWO_PHASE")
                    ? "NativeColumnarGlobalWindowAggregate"
                    : "NativeColumnarWindowAggregate"),
        comparison::toString);
    System.out.println(
        "[complex-window-count] type="
            + type
            + " shape="
            + shape
            + " phase="
            + phase
            + " filter="
            + filtered
            + " route="
            + comparison.nativeRun().route());
  }

  private static StreamTableEnvironment environment(String type, String phase) {
    return environment(type, phase, false);
  }

  private static StreamTableEnvironment environment(String type, String phase, boolean signed) {
    var spec = spec(type);
    TypeInformation<?> info = spec.info;
    DataType dataType = spec.dataType;
    Object empty = spec.empty, childNull = spec.childNull, value = spec.value;
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    var table = StreamTableEnvironment.create(env);
    table.getConfig().setLocalTimeZone(java.time.ZoneOffset.UTC);
    table.getConfig().set("table.optimizer.agg-phase-strategy", phase);
    table.getConfig().set("table.exec.mini-batch.enabled", "true");
    table.getConfig().set("table.exec.mini-batch.size", "3");
    table.getConfig().set("table.exec.mini-batch.allow-latency", "1 h");
    Row[] rows = {
      Row.of(1, null, 100L),
      Row.of(1, empty, 200L),
      Row.of(1, childNull, 300L),
      Row.of(1, value, 400L),
      Row.of(2, null, 500L),
      Row.of(2, null, 600L),
      Row.of(3, value, 2500L)
    };
    var source =
        FlinkTestSources.fromData(
                env,
                Types.ROW_NAMED(new String[] {"k", "v", "ts"}, Types.INT, info, Types.LONG),
                rows)
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<Row>forBoundedOutOfOrderness(Duration.ofDays(1))
                    .withTimestampAssigner((r, previous) -> (Long) r.getField(2)));
    if (signed) {
      rows =
          new Row[] {
            Row.of(1, value, 100L),
            Row.of(1, null, 100L),
            Row.of(2, null, 100L),
            Row.of(3, childNull, 200L),
            Row.ofKind(org.apache.flink.types.RowKind.DELETE, 3, childNull, 200L),
            Row.ofKind(org.apache.flink.types.RowKind.UPDATE_BEFORE, 1, value, 100L),
            Row.ofKind(org.apache.flink.types.RowKind.UPDATE_AFTER, 1, childNull, 100L),
            Row.of(1, empty, 300L),
            Row.ofKind(org.apache.flink.types.RowKind.DELETE, 1, empty, 300L)
          };
      source =
          FlinkTestSources.fromData(
                  env,
                  Types.ROW_NAMED(new String[] {"k", "v", "ts"}, Types.INT, info, Types.LONG),
                  rows)
              .assignTimestampsAndWatermarks(
                  WatermarkStrategy.<Row>forBoundedOutOfOrderness(Duration.ofDays(1))
                      .withTimestampAssigner((r, previous) -> (Long) r.getField(2)));
    }
    var schema =
        Schema.newBuilder()
            .column("k", DataTypes.INT())
            .column("v", dataType)
            .column("ts", DataTypes.BIGINT())
            .columnByExpression("rt", "TO_TIMESTAMP_LTZ(ts, 3)")
            .watermark("rt", "rt - INTERVAL '1' DAY")
            .build();
    if (signed)
      schema =
          Schema.newBuilder()
              .column("k", DataTypes.INT())
              .column("v", dataType)
              .column("ts", DataTypes.BIGINT())
              .columnByMetadata("rt", DataTypes.TIMESTAMP_LTZ(3), "rowtime")
              .watermark("rt", "SOURCE_WATERMARK()")
              .build();
    table.createTemporaryView(
        "src",
        signed ? table.fromChangelogStream(source, schema) : table.fromDataStream(source, schema));
    return table;
  }

  @ParameterizedTest
  @org.junit.jupiter.params.provider.MethodSource("legacyCases")
  void legacyAndSessionCounts(String type, String shape) {
    String bounds =
        shape.equals("HOP") ? "INTERVAL '1' SECOND, INTERVAL '3' SECOND" : "INTERVAL '3' SECOND";
    var result =
        NativeFailureParity.run(
            () -> environment(type, "ONE_PHASE"),
            "SELECT k, COUNT(v), COUNT(*) FROM src GROUP BY k, " + shape + "(rt, " + bounds + ")");
    result.assertSuccess(NativeFailureParity.Route.NATIVE);
    assertEquals(result.host().resultTypes(), result.nativeRun().resultTypes());
  }

  static java.util.stream.Stream<org.junit.jupiter.params.provider.Arguments> legacyCases() {
    return java.util.stream.Stream.of("array", "map", "row", "multiset")
        .flatMap(
            type ->
                java.util.stream.Stream.of("TUMBLE", "HOP", "SESSION")
                    .map(shape -> org.junit.jupiter.params.provider.Arguments.of(type, shape)));
  }

  @ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(strings = {"array", "map", "row", "multiset"})
  void sessionTableFunction(String type) {
    var result =
        NativeFailureParity.run(
            () -> environment(type, "ONE_PHASE"),
            "SELECT k, window_start, window_end, COUNT(v), COUNT(*) FROM TABLE("
                + "SESSION(TABLE src PARTITION BY k, DESCRIPTOR(rt), INTERVAL '3' SECOND))"
                + " GROUP BY k, window_start, window_end");
    if (tech.streamfusion.compat.FlinkTestCapabilities.SESSION_TABLE_FUNCTION) {
      result.assertSuccess(NativeFailureParity.Route.NATIVE);
    } else {
      assertNotNull(result.host().failure());
      assertNotNull(result.nativeRun().failure());
      assertEquals(result.host().rootCause().getClass(), result.nativeRun().rootCause().getClass());
      assertEquals(
          result.host().rootCause().getMessage(), result.nativeRun().rootCause().getMessage());
    }
  }

  @ParameterizedTest
  @org.junit.jupiter.params.provider.MethodSource("eventTimeCases")
  void signedContainersKeepNullOnlyGroups(
      String type, String shape, String phase, boolean filtered) {
    org.junit.jupiter.api.Assumptions.assumeTrue(
        tech.streamfusion.compat.FlinkTestCapabilities.RETRACTING_WINDOW_TVF);
    String window =
        shape
            + "(TABLE src, DESCRIPTOR(rt), "
            + (shape.equals("TUMBLE") ? "" : "INTERVAL '1' SECOND, ")
            + "INTERVAL '3' SECOND)";
    var result =
        NativeFailureParity.run(
            () -> environment(type, phase, true),
            "SELECT k, window_start, window_end, COUNT(v)"
                + (filtered ? " FILTER (WHERE ts <> 300)" : "")
                + ", COUNT(*) FROM TABLE("
                + window
                + ") GROUP BY k, window_start, window_end");
    result.assertSuccess(NativeFailureParity.Route.NATIVE);
  }

  static java.util.stream.Stream<org.junit.jupiter.params.provider.Arguments> recoveryCases() {
    return java.util.stream.Stream.of("array", "map", "row", "multiset")
        .flatMap(
            type ->
                java.util.stream.Stream.of("ONE_PHASE", "TWO_PHASE")
                    .flatMap(
                        phase ->
                            java.util.stream.Stream.of(
                                    "hashmap",
                                    "tech.streamfusion.state.RocksDBNativeStateBackendFactory")
                                .map(
                                    backend ->
                                        org.junit.jupiter.params.provider.Arguments.of(
                                            type, phase, backend))));
  }

  @ParameterizedTest
  @org.junit.jupiter.params.provider.MethodSource("recoveryCases")
  void countsSurviveTwoRestores(String type, String phase, String backend) throws Exception {
    var spec = spec(type);
    var rows = new java.util.ArrayList<Row>();
    for (int i = 0; i < 180; i++)
      rows.add(
          Row.of(
              i % 4,
              i % 7 == 0 ? null : i % 3 == 0 ? spec.empty : spec.childNull,
              java.time.LocalDateTime.ofInstant(
                  java.time.Instant.ofEpochMilli(i / 40 * 1600L + i % 40 * 20L),
                  java.time.ZoneOffset.UTC)));
    var types =
        Types.ROW_NAMED(new String[] {"k", "v", "rt"}, Types.INT, spec.info, Types.LOCAL_DATE_TIME);
    var schema =
        Schema.newBuilder()
            .column("k", DataTypes.INT())
            .column("v", spec.dataType)
            .column("rt", DataTypes.TIMESTAMP(3))
            .watermark("rt", "SOURCE_WATERMARK()")
            .build();
    try (var recovery =
        new PortableSqlRecovery(backend, rows, types, schema, false, 60, 120)
            .withExecutionMetrics()
            .withWatermarks(Map.of(40, 700L, 80, 2300L, 120, 3900L, 180, 9000L))) {
      java.util.function.UnaryOperator<org.apache.flink.table.api.TableEnvironment> config =
          table -> {
            table.getConfig().setLocalTimeZone(java.time.ZoneOffset.UTC);
            table.getConfig().set("table.optimizer.agg-phase-strategy", phase);
            table.getConfig().set("table.exec.mini-batch.enabled", "true");
            table.getConfig().set("table.exec.mini-batch.size", "13");
            table.getConfig().set("table.exec.mini-batch.allow-latency", "1 h");
            table.getConfig().set("streamfusion.transpose.batchRows", "7");
            return table;
          };
      var result =
          NativeFailureParity.runRecovery(
              () -> config.apply(recovery.uninterrupted()),
              () -> config.apply(recovery.get()),
              "SELECT k, window_start, window_end, COUNT(v), COUNT(*) FROM TABLE(HOP(TABLE"
                  + " recovery_input, DESCRIPTOR(rt), INTERVAL '1' SECOND, INTERVAL '3' SECOND))"
                  + " GROUP BY k, window_start, window_end");
      result.assertSuccess(NativeFailureParity.Route.NATIVE);
      recovery.verifyRepeatedRecovery();
      RecoveryExecutionMetrics.assertConsumed(
          recovery.nativeExecution(),
          phase.equals("TWO_PHASE")
              ? "NativeColumnarGlobalWindowAggregate"
              : "NativeColumnarWindowAggregate");
      if (phase.equals("TWO_PHASE"))
        RecoveryExecutionMetrics.assertConsumed(
            recovery.nativeExecution(), "NativeColumnarLocalWindowAggregate");
      SharedFlinkCluster.assertNativeMemoryReleased("complex-count-recovery");
    }
  }

  private static final class Spec {
    final TypeInformation<?> info;
    final DataType dataType;
    final Object empty, childNull, value;

    Spec(TypeInformation<?> info, DataType dataType, Object empty, Object childNull, Object value) {
      this.info = info;
      this.dataType = dataType;
      this.empty = empty;
      this.childNull = childNull;
      this.value = value;
    }
  }

  private static Spec spec(String type) {
    TypeInformation<?> info;
    DataType dataType;
    Object empty, childNull, value;
    switch (type) {
      case "array":
        info = Types.OBJECT_ARRAY(Types.LONG);
        dataType = DataTypes.ARRAY(DataTypes.BIGINT());
        empty = new Long[0];
        childNull = new Long[] {null};
        value = new Long[] {1L, 2L};
        break;
      case "map":
        info = Types.MAP(Types.STRING, Types.LONG);
        dataType = DataTypes.MAP(DataTypes.STRING(), DataTypes.BIGINT());
        empty = Map.of();
        var map = new LinkedHashMap<String, Long>();
        map.put("nil", null);
        map.put(null, 9L);
        childNull = map;
        value = Map.of("a", 1L, "b", 2L);
        break;
      case "multiset":
        dataType = DataTypes.MULTISET(DataTypes.STRING());
        info = org.apache.flink.table.runtime.typeutils.ExternalTypeInfo.of(dataType);
        empty = Map.of();
        var multiset = new LinkedHashMap<String, Integer>();
        multiset.put(null, 3);
        childNull = multiset;
        value = Map.of("a", 5, "b", 2);
        break;
      case "row":
        info = Types.ROW_NAMED(new String[] {"child"}, Types.LONG);
        dataType = DataTypes.ROW(DataTypes.FIELD("child", DataTypes.BIGINT()));
        empty = Row.of((Object) null);
        childNull = Row.of((Object) null);
        value = Row.of(2L);
        break;
      default:
        throw new IllegalArgumentException(type);
    }
    return new Spec(info, dataType, empty, childNull, value);
  }
}
