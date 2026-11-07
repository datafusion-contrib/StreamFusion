package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class FlinkJvmOverSqlHarnessTest {
  @ParameterizedTest
  @CsvSource({
    "rt,ROWS UNBOUNDED PRECEDING",
    "rt,RANGE UNBOUNDED PRECEDING",
    "rt,ROWS BETWEEN 3 PRECEDING AND CURRENT ROW",
    "rt,RANGE BETWEEN INTERVAL '1' SECOND PRECEDING AND CURRENT ROW",
    "pt,ROWS UNBOUNDED PRECEDING",
    "pt,RANGE UNBOUNDED PRECEDING",
    "pt,ROWS BETWEEN 3 PRECEDING AND CURRENT ROW"
  })
  void allVerifiedFramesConsumeJvmInputAndMatchValues(String order, String frame) throws Exception {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var runs =
          NativeFailureParity.run(
              () -> environment(metrics, 1),
              "SELECT id,k,test_aggregate(v) OVER w,COUNT(*) OVER w,view_aggregate(v) OVER w FROM"
                  + " src WINDOW w AS (PARTITION BY k ORDER BY "
                  + order
                  + " "
                  + frame
                  + ")");
      runs.assertSuccess(NativeFailureParity.Route.NATIVE);
      assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
      assertTrue(runs.nativeRun().plan().contains("NativeJvmOver"), runs::toString);
      RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeJvmOver");
      assertEquals(96, metrics.counterValue("jvmOverRows"));
      SharedFlinkCluster.assertNativeMemoryReleased("jvm-over-" + order + "-" + frame);
    }
  }

  @ParameterizedTest
  @CsvSource({
    "rt,ROWS UNBOUNDED PRECEDING",
    "rt,RANGE UNBOUNDED PRECEDING",
    "rt,ROWS BETWEEN 3 PRECEDING AND CURRENT ROW",
    "rt,RANGE BETWEEN INTERVAL '1' SECOND PRECEDING AND CURRENT ROW",
    "pt,ROWS UNBOUNDED PRECEDING",
    "pt,RANGE UNBOUNDED PRECEDING",
    "pt,ROWS BETWEEN 3 PRECEDING AND CURRENT ROW"
  })
  void distinctViewsConstantsAndSharedCallsUseReleasedGeneration(String order, String frame) {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var runs =
          NativeFailureParity.run(
              () -> environment(metrics, 1),
              "SELECT id,test_aggregate(DISTINCT v) OVER w,test_aggregate(v) OVER w,"
                  + "test_aggregate(CAST(3 AS BIGINT)) OVER w,view_aggregate(DISTINCT v) OVER w"
                  + " FROM src WINDOW w AS (PARTITION BY k ORDER BY "
                  + order
                  + " "
                  + frame
                  + ")");
      runs.assertSuccess(NativeFailureParity.Route.NATIVE);
      assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
      RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeJvmOver");
    }
  }

  @ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(
      strings = {"open", "accumulate", "value", "retract", "close"})
  void lifecycleAndRetractionFailuresKeepReleasedCauses(String phase) throws Exception {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var runs =
          NativeFailureParity.run(
              () -> {
                var table = environment(metrics, 1);
                table.createTemporarySystemFunction("failure", new OverFailure(phase));
                return table;
              },
              "SELECT id,failure(v) OVER (PARTITION BY k ORDER BY rt ROWS BETWEEN 1 PRECEDING AND"
                  + " CURRENT ROW) FROM src");
      {
        assertNotNull(runs.host().failure(), runs::toString);
        assertNotNull(runs.nativeRun().failure(), runs::toString);
        assertEquals(runs.host().rootCause().getClass(), runs.nativeRun().rootCause().getClass());
        assertEquals(
            runs.host().rootCause().getMessage(), runs.nativeRun().rootCause().getMessage());
        assertEquals(NativeFailureParity.Route.NATIVE, runs.nativeRun().route());
      }
      SharedFlinkCluster.assertNativeMemoryReleased("jvm-over-failure");
    }
  }

  public static class OverFailure extends FlinkJvmTwoPhaseSqlHarnessTest.FailureAggregate {
    private final String phase;

    public OverFailure(String phase) {
      super(phase);
      this.phase = phase;
    }

    @Override
    public void retract(PortableSqlFixtures.SumState state, Long value) {
      super.retract(state, value);
      if (phase.equals("retract")) throw new IllegalStateException("OVER retract failure");
    }
  }

  @ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(strings = {"open", "row"})
  void unpartitionedMutableOutputAndJavaStringsRemainOwned(String shape) {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var runs =
          NativeFailureParity.run(
              () -> {
                var table = environment(metrics, 1);
                table.createTemporarySystemFunction("mutating", new MutableBytes());
                table.createTemporarySystemFunction("java_string", new OriginalString());
                return table;
              },
              "SELECT id,mutating(v) OVER w,java_string(v) OVER w,mutating(v+1) OVER w FROM src"
                  + " WINDOW w AS (ORDER BY "
                  + (shape.equals("open") ? "pt" : "rt")
                  + " ROWS UNBOUNDED PRECEDING)");
      runs.assertSuccess(NativeFailureParity.Route.NATIVE);
      RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeJvmOver");
    }
  }

  @org.apache.flink.table.annotation.FunctionHint(
      input = @org.apache.flink.table.annotation.DataTypeHint("BIGINT"),
      output = @org.apache.flink.table.annotation.DataTypeHint("BYTES"))
  public static class MutableBytes
      extends org.apache.flink.table.functions.AggregateFunction<
          byte[], PortableSqlFixtures.SumState> {
    private final byte[] bytes = new byte[16];
    private long calls;

    @Override
    public PortableSqlFixtures.SumState createAccumulator() {
      return new PortableSqlFixtures.SumState();
    }

    public void accumulate(PortableSqlFixtures.SumState acc, Long v) {
      acc.sum += (v == null ? 0 : v) + calls++;
    }

    @Override
    public byte[] getValue(PortableSqlFixtures.SumState acc) {
      java.nio.ByteBuffer.wrap(bytes).putLong(acc.sum).putLong(calls);
      return bytes;
    }
  }

  @org.apache.flink.table.annotation.FunctionHint(
      input = @org.apache.flink.table.annotation.DataTypeHint("BIGINT"),
      output = @org.apache.flink.table.annotation.DataTypeHint("STRING"))
  public static class OriginalString
      extends org.apache.flink.table.functions.AggregateFunction<
          String, PortableSqlFixtures.SumState> {
    @Override
    public PortableSqlFixtures.SumState createAccumulator() {
      return new PortableSqlFixtures.SumState();
    }

    public void accumulate(PortableSqlFixtures.SumState acc, Long v) {
      acc.sum += v == null ? 0 : v;
    }

    @Override
    public String getValue(PortableSqlFixtures.SumState acc) {
      return "unpaired-" + (char) 0xD800 + "-" + acc.sum;
    }
  }

  @ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(
      strings = {"streamfusion.operator.jvmOver.enabled", "streamfusion.operator.over.enabled"})
  void disablingEitherOverSwitchKeepsHostFallback(String property) {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var runs =
          NativeFailureParity.run(
              () -> {
                var table = environment(metrics, 1);
                table.getConfig().set(property, "false");
                return table;
              },
              "SELECT id,test_aggregate(v) OVER (PARTITION BY k ORDER BY rt ROWS UNBOUNDED"
                  + " PRECEDING) FROM src");
      runs.assertSuccess(NativeFailureParity.Route.FALLBACK);
      assertFalse(runs.nativeRun().plan().contains("NativeJvmOver"));
      assertEquals(0, metrics.counterValue("jvmOverRows"));
    }
  }

  @org.junit.jupiter.api.Test
  void boundedProcessingTimeRangeRetainsExplicitFallback(@TempDir Path directory) {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var runs =
          NativeFailureParity.run(
              () ->
                  proctimeFallbackEnvironment(
                      metrics, directory.resolve(UUID.randomUUID() + ".complete").toString()),
              "SELECT id,test_aggregate(v) OVER (PARTITION BY id ORDER BY pt RANGE BETWEEN INTERVAL"
                  + " '1' SECOND PRECEDING AND CURRENT ROW) FROM src");
      runs.assertSuccess(NativeFailureParity.Route.FALLBACK);
      var expected = new ArrayList<java.util.List<Object>>();
      for (int id = 0; id < 96; id++) {
        expected.add(Arrays.asList("+I", id, id % 7 == 0 ? null : (long) id % 13));
      }
      assertEquals(NativeParity.multiset(expected), NativeParity.multiset(runs.host().rows()));
      assertFalse(runs.nativeRun().plan().contains("NativeJvmOver"));
      assertEquals(0, metrics.counterValue("jvmOverRows"));
    }
  }

  private static TableEnvironment proctimeFallbackEnvironment(
      RecoveryExecutionMetrics metrics, String completion) {
    var env = metrics.environment(new Configuration(), 1, 0);
    env.disableOperatorChaining();
    var source =
        env.fromSequence(0, 95)
            .flatMap(
                (Long id, org.apache.flink.util.Collector<Row> output) -> {
                  output.collect(Row.of(id.intValue(), id % 7 == 0 ? null : id % 13));
                  if (id == 95) {
                    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
                    while (!Files.exists(Path.of(completion))) {
                      if (System.nanoTime() >= deadline) {
                        throw new IllegalStateException(
                            "Processing-time RANGE did not evaluate all 96 rows before end of input");
                      }
                      Thread.sleep(10);
                    }
                  }
                })
            .returns(Types.ROW_NAMED(new String[] {"id", "v"}, Types.INT, Types.LONG));
    var table = StreamTableEnvironment.create(env);
    table.createTemporaryView(
        "src",
        source,
        Schema.newBuilder()
            .column("id", DataTypes.INT())
            .column("v", DataTypes.BIGINT())
            .columnByExpression("pt", "PROCTIME()")
            .build());
    table.createTemporarySystemFunction("test_aggregate", new CompletingSum(completion));
    return table;
  }

  @org.apache.flink.table.annotation.FunctionHint(
      input = @org.apache.flink.table.annotation.DataTypeHint("BIGINT"),
      output = @org.apache.flink.table.annotation.DataTypeHint("BIGINT"))
  public static class CompletingSum extends PortableSqlFixtures.Sum {
    private final String completion;
    private int evaluated;

    public CompletingSum(String completion) {
      this.completion = completion;
    }

    @Override
    public Long getValue(PortableSqlFixtures.SumState state) {
      Long result = super.getValue(state);
      if (++evaluated == 96) {
        try {
          Files.createFile(Path.of(completion));
        } catch (IOException failure) {
          throw new UncheckedIOException(failure);
        }
      }
      return result;
    }
  }

  @org.junit.jupiter.api.Test
  void emptyInputStillUsesReleasedLifecycleAndProducesNoOverRows() {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var runs =
          NativeFailureParity.run(
              () -> environment(metrics, 1),
              "SELECT test_aggregate(v) OVER (ORDER BY rt ROWS UNBOUNDED PRECEDING) FROM src WHERE"
                  + " id<0");
      runs.assertSuccess(NativeFailureParity.Route.NATIVE);
      assertTrue(runs.host().rows().isEmpty());
      assertTrue(runs.nativeRun().rows().isEmpty());
      assertTrue(runs.nativeRun().plan().contains("NativeJvmOver"));
      assertEquals(0, metrics.counterValue("jvmOverRows"));
    }
  }

  @ParameterizedTest
  @CsvSource({"1,ROWS", "2,ROWS", "1,RANGE", "2,RANGE"})
  void releasedUnboundedImplementationsKeepTheirOwnPeerAndTimerSemantics(
      int version, String frame) {
    org.junit.jupiter.api.Assumptions.assumeTrue(
        version == 1
            || !org.apache.flink.runtime.util.EnvironmentInformation.getVersion()
                .startsWith("1.18"));
    try (var metrics = new RecoveryExecutionMetrics()) {
      var runs =
          NativeFailureParity.run(
              () -> {
                var table = environment(metrics, 1);
                table
                    .getConfig()
                    .set("table.exec.unbounded-over.version", Integer.toString(version));
                return table;
              },
              "SELECT id,test_aggregate(v) OVER w,view_aggregate(v) OVER w FROM src WINDOW w AS"
                  + " (PARTITION BY k ORDER BY rt "
                  + frame
                  + " UNBOUNDED PRECEDING)");
      runs.assertSuccess(NativeFailureParity.Route.NATIVE);
      RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeJvmOver");
      assertTrue(metrics.counterValue("jvmOverEventTimers") > 0);
    }
  }

  static java.util.stream.Stream<org.junit.jupiter.params.provider.Arguments> typedFrames() {
    return java.util.stream.Stream.of(
            "STRING",
            "CHAR(4)",
            "BOOLEAN",
            "TINYINT",
            "SMALLINT",
            "INT",
            "BIGINT",
            "FLOAT",
            "DOUBLE",
            "VARBINARY",
            "BINARY(4)",
            "DECIMAL(18,8)",
            "DECIMAL(38,18)",
            "DATE",
            "TIME",
            "TIMESTAMP(9)",
            "TIMESTAMP_LTZ(9)",
            "DAY_TIME",
            "YEAR_MONTH")
        .flatMap(
            type ->
                java.util.stream.Stream.of(
                        "rt,ROWS UNBOUNDED PRECEDING",
                        "rt,RANGE UNBOUNDED PRECEDING",
                        "rt,ROWS BETWEEN 3 PRECEDING AND CURRENT ROW",
                        "rt,RANGE BETWEEN INTERVAL '1' SECOND PRECEDING AND CURRENT ROW",
                        "pt,ROWS UNBOUNDED PRECEDING",
                        "pt,RANGE UNBOUNDED PRECEDING",
                        "pt,ROWS BETWEEN 3 PRECEDING AND CURRENT ROW")
                    .map(
                        frame ->
                            org.junit.jupiter.params.provider.Arguments.of(
                                type, frame.split(",", 2)[0], frame.split(",", 2)[1])));
  }

  @ParameterizedTest
  @org.junit.jupiter.params.provider.MethodSource("typedFrames")
  void scalarArgumentsAndResultsRemainOwnedAcrossAllFrames(
      String type, String order, String frame) {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var runs =
          NativeFailureParity.run(
              () -> typedEnvironment(type, false, 1, metrics),
              "SELECT id,k,remember(v) OVER w,COUNT(*) OVER w FROM src WINDOW w AS (PARTITION BY k"
                  + " ORDER BY "
                  + order
                  + " "
                  + frame
                  + ")");
      runs.assertSuccess(NativeFailureParity.Route.NATIVE);
      assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
      RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeJvmOver");
    }
  }

  @ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(
      strings = {
        "STRING",
        "CHAR(4)",
        "BOOLEAN",
        "TINYINT",
        "SMALLINT",
        "INT",
        "BIGINT",
        "FLOAT",
        "DOUBLE",
        "VARBINARY",
        "BINARY(4)",
        "DECIMAL(18,8)",
        "DECIMAL(38,18)",
        "DATE",
        "TIME",
        "TIMESTAMP(9)",
        "TIMESTAMP_LTZ(9)",
        "DAY_TIME",
        "YEAR_MONTH"
      })
  void scalarPartitionKeysUseReleasedProjectionAtOneAndThreeTasks(String type) {
    for (int parallelism : new int[] {1, 3}) {
      try (var metrics = new RecoveryExecutionMetrics()) {
        var runs =
            NativeFailureParity.run(
                () -> typedEnvironment(type, true, parallelism, metrics),
                "SELECT id,k,remember(v) OVER w,COUNT(*) OVER w FROM src WINDOW w AS"
                    + " (PARTITION BY k ORDER BY rt ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)");
        runs.assertSuccess(NativeFailureParity.Route.NATIVE);
        assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
        RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeJvmOver");
      }
    }
  }

  static TableEnvironment typedEnvironment(
      String name, boolean typedKey, int parallelism, RecoveryExecutionMetrics metrics) {
    var valueType = FlinkTypedTableFunctionSqlHarnessTest.valueType(name);
    var keyType = typedKey ? valueType : DataTypes.INT();
    var resultType = typedKey ? DataTypes.INT() : valueType;
    var type =
        DataTypes.ROW(
            DataTypes.FIELD("id", DataTypes.INT()),
            DataTypes.FIELD("k", keyType),
            DataTypes.FIELD("v", resultType),
            DataTypes.FIELD("rt", DataTypes.TIMESTAMP(3)));
    var rows = new java.util.ArrayList<Row>();
    for (int i = 0; i < 48; i++) {
      Object value = FlinkTypedTableFunctionSqlHarnessTest.value(name, i / 4);
      rows.add(
          Row.of(
              i,
              typedKey ? value : i % 5 == 0 ? null : i % 3,
              typedKey ? i : value,
              LocalDateTime.ofInstant(
                  java.time.Instant.ofEpochMilli(1000 + i / 6 * 400), ZoneOffset.UTC)));
    }
    var env = metrics.environment(new Configuration(), parallelism, 0);
    env.getConfig().enableObjectReuse();
    var table = StreamTableEnvironment.create(env);
    table.getConfig().setLocalTimeZone(ZoneOffset.UTC);
    table.getConfig().set("streamfusion.transpose.batchRows", "5");
    var source =
        tech.streamfusion.compat.FlinkTestSources.fromData(
                env,
                org.apache.flink.table.runtime.typeutils.ExternalTypeInfo.<Row>of(type),
                rows.toArray(new Row[0]))
            .setParallelism(1)
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<Row>forBoundedOutOfOrderness(Duration.ofDays(1))
                    .withTimestampAssigner(
                        (row, previous) ->
                            ((LocalDateTime) row.getField(3))
                                .toInstant(ZoneOffset.UTC)
                                .toEpochMilli()));
    table.createTemporaryView(
        "src",
        source,
        Schema.newBuilder()
            .fromRowDataType(type)
            .watermark("rt", "SOURCE_WATERMARK()")
            .columnByExpression("pt", "PROCTIME()")
            .build());
    table.createTemporarySystemFunction(
        "remember", new FlinkTypedJvmAggregateSqlHarnessTest.Remember(resultType));
    return table;
  }

  static TableEnvironment environment(RecoveryExecutionMetrics metrics, int parallelism) {
    var env = metrics.environment(new Configuration(), parallelism, 0);
    var table = StreamTableEnvironment.create(env);
    table.getConfig().setLocalTimeZone(ZoneOffset.UTC);
    table.getConfig().set("streamfusion.transpose.batchRows", "7");
    var source =
        env.fromSequence(0, 95)
            .setParallelism(1)
            .map(
                id ->
                    Row.of(
                        id.intValue(),
                        id % 11 == 0 ? null : (int) (id % 3),
                        id % 7 == 0 ? null : id % 13,
                        LocalDateTime.ofInstant(
                            java.time.Instant.ofEpochMilli(1000 + id / 6 * 400), ZoneOffset.UTC)))
            .returns(
                Types.ROW_NAMED(
                    new String[] {"id", "k", "v", "rt"},
                    Types.INT,
                    Types.INT,
                    Types.LONG,
                    Types.LOCAL_DATE_TIME))
            .setParallelism(1)
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<Row>forBoundedOutOfOrderness(Duration.ofDays(1))
                    .withTimestampAssigner(
                        (value, previous) ->
                            ((LocalDateTime) value.getField(3))
                                .toInstant(ZoneOffset.UTC)
                                .toEpochMilli()));
    table.createTemporaryView(
        "src",
        source,
        Schema.newBuilder()
            .column("id", DataTypes.INT())
            .column("k", DataTypes.INT())
            .column("v", DataTypes.BIGINT())
            .column("rt", DataTypes.TIMESTAMP(3))
            .watermark("rt", "SOURCE_WATERMARK()")
            .columnByExpression("pt", "PROCTIME()")
            .build());
    table.createTemporarySystemFunction("test_aggregate", new PortableSqlFixtures.Sum());
    table.createTemporarySystemFunction(
        "view_aggregate", new FlinkJvmAggregateSqlHarnessTest.ViewAggregate());
    return table;
  }
}
