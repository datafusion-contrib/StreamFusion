package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

class FlinkWindowFloatingDistinctSqlHarnessTest {
  @ParameterizedTest
  @CsvSource({"ONE_PHASE,TUMBLE,false", "TWO_PHASE,TUMBLE,false", "ONE_PHASE,HOP,false", "TWO_PHASE,HOP,false",
      "ONE_PHASE,CUMULATE,false", "TWO_PHASE,CUMULATE,false", "ONE_PHASE,TUMBLE,true", "TWO_PHASE,TUMBLE,true",
      "ONE_PHASE,HOP,true", "TWO_PHASE,HOP,true", "ONE_PHASE,CUMULATE,true", "TWO_PHASE,CUMULATE,true"})
  void floatingMembershipMatchesReleasedHost(String phase, String shape, boolean filtered) {
    String filter = filtered ? " FILTER (WHERE MOD(ts, 800) = 200)" : "";
    try (var metrics = new RecoveryExecutionMetrics()) {
      var comparison = NativeFailureParity.run(() -> environment(metrics, phase),
          "SELECT k, window_start, window_end, COUNT(DISTINCT f)" + filter + ", COUNT(DISTINCT d)" + filter
              + ", COUNT(DISTINCT fb)" + filter + ", COUNT(DISTINCT db)" + filter + ", COUNT(*)"
              + " FROM TABLE(" + window(shape) + ") GROUP BY k, window_start, window_end");
      comparison.assertSuccess(NativeFailureParity.Route.NATIVE);
      assertEquals(comparison.host().resultTypes(), comparison.nativeRun().resultTypes());
      for (var row : comparison.host().rows()) {
        assertEquals(row.get(4), row.get(6));
        assertEquals(row.get(5), row.get(7));
      }
      RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), phase.equals("TWO_PHASE")
          ? "NativeColumnarGlobalWindowAggregate" : "NativeColumnarWindowAggregate");
      if (phase.equals("TWO_PHASE"))
        RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeColumnarLocalWindowAggregate");
    }
  }

  @ParameterizedTest
  @CsvSource({"ONE_PHASE,TUMBLE", "TWO_PHASE,TUMBLE", "ONE_PHASE,HOP", "TWO_PHASE,HOP",
      "ONE_PHASE,SESSION", "TWO_PHASE,SESSION"})
  void legacyWindowSyntaxRetainsFloatingMembership(String phase, String shape) {
    try (var metrics = new RecoveryExecutionMetrics()) {
      String bounds = shape.equals("HOP") ? "INTERVAL '1' SECOND, INTERVAL '2' SECOND" : "INTERVAL '2' SECOND";
      var comparison = NativeFailureParity.run(() -> environment(metrics, phase),
          "SELECT k, COUNT(DISTINCT f), COUNT(DISTINCT d), COUNT(DISTINCT fb), COUNT(DISTINCT db)"
              + " FROM src GROUP BY k, " + shape + "(rt, " + bounds + ")");
      comparison.assertSuccess(NativeFailureParity.Route.NATIVE);
      assertEquals(comparison.host().resultTypes(), comparison.nativeRun().resultTypes());
      for (var row : comparison.host().rows()) {
        assertEquals(row.get(2), row.get(4));
        assertEquals(row.get(3), row.get(5));
      }
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"ONE_PHASE", "TWO_PHASE"})
  void sessionTableFunctionRetainsReleasedAvailability(String phase) {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var comparison = NativeFailureParity.run(() -> environment(metrics, phase),
          "SELECT k, window_start, window_end, COUNT(DISTINCT f), COUNT(DISTINCT d)"
              + " FROM TABLE(SESSION(TABLE src PARTITION BY k, DESCRIPTOR(rt), INTERVAL '2' SECOND))"
              + " GROUP BY k, window_start, window_end");
      if (!tech.streamfusion.compat.FlinkTestCapabilities.SESSION_TABLE_FUNCTION) {
        assertNotNull(comparison.host().failure());
        assertNotNull(comparison.nativeRun().failure());
        assertEquals(comparison.host().rootCause().getClass(), comparison.nativeRun().rootCause().getClass());
        assertEquals(comparison.host().rootCause().getMessage(), comparison.nativeRun().rootCause().getMessage());
        assertEquals(comparison.host().phase(), comparison.nativeRun().phase());
      } else {
        comparison.assertSuccess(NativeFailureParity.Route.NATIVE);
        assertEquals(comparison.host().resultTypes(), comparison.nativeRun().resultTypes());
        RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeColumnarSessionWindowAggregate");
      }
    }
  }

  @ParameterizedTest
  @CsvSource({"ONE_PHASE,TUMBLE", "TWO_PHASE,TUMBLE", "ONE_PHASE,HOP", "TWO_PHASE,HOP",
      "ONE_PHASE,CUMULATE", "TWO_PHASE,CUMULATE"})
  void splitDistinctRetainsCanonicalMembershipAcrossPhases(String phase, String shape) {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var comparison = NativeFailureParity.run(() -> {
        var table = environment(metrics, phase);
        table.getConfig().set("table.optimizer.distinct-agg.split.enabled", "true");
        return table;
      }, "SELECT k, window_start, window_end, COUNT(DISTINCT f), COUNT(DISTINCT d),"
          + " COUNT(DISTINCT fb), COUNT(DISTINCT db) FROM TABLE(" + window(shape)
          + ") GROUP BY k, window_start, window_end");
      comparison.assertSuccess(NativeFailureParity.Route.NATIVE);
      assertEquals(comparison.host().resultTypes(), comparison.nativeRun().resultTypes());
      for (var row : comparison.host().rows()) {
        assertEquals(row.get(4), row.get(6));
        assertEquals(row.get(5), row.get(7));
      }
      if (phase.equals("TWO_PHASE")) {
        RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeColumnarGlobalWindowAggregate");
        RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeColumnarLocalWindowAggregate");
      }
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"hashmap", "tech.streamfusion.state.RocksDBNativeStateBackendFactory"})
  void splitCumulativeBoundsSurviveTwoRestores(String backend) throws Exception {
    var rows = java.util.stream.IntStream.range(0, 180).mapToObj(id -> {
      var r = row(id);
      return Row.of(r.getField(0), r.getField(1), r.getField(2), r.getField(3), r.getField(4), r.getField(5),
          java.time.LocalDateTime.ofInstant(java.time.Instant.ofEpochMilli((Long) r.getField(3)), java.time.ZoneOffset.UTC));
    }).collect(tech.streamfusion.compat.ListCollectors.toList());
    var types = Types.ROW_NAMED(new String[] {"k", "f", "d", "ts", "fb", "db", "rt"},
        Types.INT, Types.FLOAT, Types.DOUBLE, Types.LONG, Types.INT, Types.LONG, Types.LOCAL_DATE_TIME);
    var schema = Schema.newBuilder().column("k", DataTypes.INT()).column("f", DataTypes.FLOAT())
        .column("d", DataTypes.DOUBLE()).column("ts", DataTypes.BIGINT()).column("fb", DataTypes.INT())
        .column("db", DataTypes.BIGINT()).column("rt", DataTypes.TIMESTAMP(3))
        .watermark("rt", "SOURCE_WATERMARK()").build();
    try (var recovery = new PortableSqlRecovery(backend, rows, types, schema, false, 60, 120)
        .withExecutionMetrics().withWatermarks(java.util.Map.of(40, 1900L, 80, 3500L, 120, 5100L, 180, 9000L))) {
      java.util.function.UnaryOperator<org.apache.flink.table.api.TableEnvironment> configure = table -> {
        table.getConfig().setLocalTimeZone(java.time.ZoneOffset.UTC);
        table.getConfig().set("table.optimizer.agg-phase-strategy", "TWO_PHASE");
        table.getConfig().set("table.optimizer.distinct-agg.split.enabled", "true");
        table.getConfig().set("table.exec.mini-batch.enabled", "true");
        table.getConfig().set("table.exec.mini-batch.size", "13");
        table.getConfig().set("table.exec.mini-batch.allow-latency", "1 h");
        table.getConfig().set("streamfusion.transpose.batchRows", "7");
        return table;
      };
      var comparison = NativeFailureParity.runRecovery(() -> configure.apply(recovery.uninterrupted()),
          () -> configure.apply(recovery.get()),
          "SELECT k, window_start, window_end, COUNT(DISTINCT f), COUNT(DISTINCT d), COUNT(DISTINCT fb), COUNT(DISTINCT db)"
              + " FROM TABLE(CUMULATE(TABLE recovery_input, DESCRIPTOR(rt), INTERVAL '1' SECOND, INTERVAL '2' SECOND))"
              + " GROUP BY k, window_start, window_end");
      comparison.assertSuccess(NativeFailureParity.Route.NATIVE);
      assertEquals(comparison.host().resultTypes(), comparison.nativeRun().resultTypes());
      recovery.verifyRepeatedRecovery();
      RecoveryExecutionMetrics.assertConsumed(recovery.nativeExecution(), "NativeColumnarLocalWindowAggregate");
      RecoveryExecutionMetrics.assertConsumed(recovery.nativeExecution(), "NativeColumnarGlobalWindowAggregate");
      SharedFlinkCluster.assertNativeMemoryReleased("floating-split-cumulate-recovery");
    }
  }

  private static Row row(long id) {
      int slot = (int) (id / 5 % 12);
      float f;
      double d;
      switch (slot) {
        case 2:
          f = Float.intBitsToFloat(0xffc00002); d = Double.longBitsToDouble(0xfff8000000000002L); break;
        case 0: case 1: case 10:
          f = Float.intBitsToFloat(0x7fc00001 + slot);
          d = Double.longBitsToDouble(0x7ff8000000000001L + slot);
          break;
        case 3: case 11: f = 0.0f; d = 0.0; break;
        case 4: f = -0.0f; d = -0.0; break;
        case 5: f = Float.NEGATIVE_INFINITY; d = Double.NEGATIVE_INFINITY; break;
        case 6: f = Float.POSITIVE_INFINITY; d = Double.POSITIVE_INFINITY; break;
        case 7: f = Float.MIN_VALUE; d = Double.MIN_VALUE; break;
        case 8: f = -1; d = -1; break;
        default: f = Float.MAX_VALUE; d = Double.MAX_VALUE;
      }
      boolean empty = id % 5 == 0 || id % 11 == 0;
      return Row.of(id % 5 == 0 ? null : (int) (id % 5), empty ? null : f, empty ? null : d,
          1000L + id / 10 * 400L, empty ? null : Float.floatToIntBits(f),
          empty ? null : Double.doubleToLongBits(d));

  }

  private static String window(String shape) {
    if (shape.equals("TUMBLE")) return "TUMBLE(TABLE src, DESCRIPTOR(rt), INTERVAL '2' SECOND)";
    if (shape.equals("HOP")) return "HOP(TABLE src, DESCRIPTOR(rt), INTERVAL '1' SECOND, INTERVAL '2' SECOND)";
    return "CUMULATE(TABLE src, DESCRIPTOR(rt), INTERVAL '1' SECOND, INTERVAL '2' SECOND)";
  }

  private static StreamTableEnvironment environment(RecoveryExecutionMetrics metrics, String phase) {
    var env = metrics.environment(new Configuration(), 1, 0);
    var table = StreamTableEnvironment.create(env);
    table.getConfig().set("table.optimizer.agg-phase-strategy", phase);
    table.getConfig().set("streamfusion.transpose.batchRows", "7");
    var input = env.fromSequence(0, 179).map(FlinkWindowFloatingDistinctSqlHarnessTest::row).returns(Types.ROW_NAMED(new String[] {"k", "f", "d", "ts", "fb", "db"}, Types.INT, Types.FLOAT, Types.DOUBLE, Types.LONG, Types.INT, Types.LONG))
        .assignTimestampsAndWatermarks(WatermarkStrategy.<Row>forBoundedOutOfOrderness(Duration.ofDays(1))
            .withTimestampAssigner((row, previous) -> (Long) row.getField(3)));
    table.createTemporaryView("src", input, Schema.newBuilder().column("k", DataTypes.INT())
        .column("f", DataTypes.FLOAT()).column("d", DataTypes.DOUBLE()).column("ts", DataTypes.BIGINT())
        .column("fb", DataTypes.INT()).column("db", DataTypes.BIGINT())
        .columnByMetadata("rt", DataTypes.TIMESTAMP_LTZ(3), "rowtime").watermark("rt", "SOURCE_WATERMARK()").build());
    return table;
  }
}
