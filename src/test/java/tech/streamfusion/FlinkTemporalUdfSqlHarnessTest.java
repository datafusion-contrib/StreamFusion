package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.runtime.typeutils.ExternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.inference.TypeStrategies;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

class FlinkTemporalUdfSqlHarnessTest {
  @ParameterizedTest(name = "precision={0},zone={1}")
  @CsvSource({
    "0,UTC", "3,UTC", "6,UTC", "9,UTC",
    "0,America/Los_Angeles", "3,America/Los_Angeles", "6,America/Los_Angeles",
        "9,America/Los_Angeles",
    "0,Asia/Shanghai", "3,Asia/Shanghai", "6,Asia/Shanghai", "9,Asia/Shanghai"
  })
  void externalTemporalArgumentsAndResultsKeepPrecisionAndZoneContracts(
      int precision, String zone) {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var comparison =
          NativeFailureParity.run(
              () -> environment(precision, zone, 1031, metrics),
              "SELECT id, ts_identity(t), ts_text(t), ltz_identity(z), ltz_epoch(z) FROM src");
      comparison.assertSuccess(NativeFailureParity.Route.NATIVE);
      assertEquals(comparison.host().resultTypes(), comparison.nativeRun().resultTypes());
      assertEquals(1031, comparison.nativeRun().rows().size());
      var nativeMetrics = metrics.nativeOperators();
      RecoveryExecutionMetrics.assertConsumed(nativeMetrics, "NativeCalc");
      assertTrue(
          nativeMetrics.stream()
                  .filter(row -> row.get("operator").toString().startsWith("NativeCalc"))
                  .mapToLong(row -> ((Number) row.get("recordsOut")).longValue())
                  .sum()
              > 0,
          nativeMetrics.toString());
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 3, 6, 9})
  void temporalOnlyColumnsKeepOwnershipAndLateChanges(int precision) throws Exception {
    for (String sql :
        new String[] {
          "SELECT ts_identity(t), ltz_identity(z) FROM src",
          "SELECT ts_text(t) FROM src",
          "SELECT ltz_epoch(z) FROM src",
          "SELECT ts_identity(t) FROM src WHERE t IS NOT NULL",
          "SELECT ts_change(t) FROM src"
        }) {
      try (var metrics = new RecoveryExecutionMetrics()) {
        NativeParity.assertParity(
            () -> {
              var table = environment(precision, "America/Los_Angeles", 1031, metrics);
              table.createTemporarySystemFunction("ts_change", new ChangingTimestamp(precision));
              return table;
            },
            sql);
        if (Boolean.getBoolean("streamfusion.experimental.directTemporalCalc"))
          assertTrue(metrics.counterValue("directTemporalRows") > 0, sql);
        if (Boolean.getBoolean("streamfusion.experimental.temporalAliases")
            && sql.contains("ltz_identity"))
          assertTrue(metrics.counterValue("directTemporalAliasedValues") > 0, sql);
      }
    }
  }

  @Test
  void builtinTemporalControlRemainsNative() throws Exception {
    NativeParity.assertParity(
        () -> environment(9, "America/Los_Angeles", 1031, null, false),
        "SELECT id, t, z, CAST(z AS STRING) FROM src");
  }

  @Test
  void defaultAdmissionKeepsTheVerifiedHostRoute() throws Exception {
    for (String expression :
        new String[] {"ts_identity(t)", "ts_text(t)", "ltz_identity(z)", "ltz_epoch(z)"}) {
      NativeParity.assertFallbackReasonContains(
          () -> environment(3, "UTC", 32, null, false),
          "SELECT " + expression + " FROM src",
          "temporal scalar UDFs await whole-job performance admission");
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 3, 6, 9})
  void nullArgumentsArePassedToTheFunctionAndResultsCanEnterNativeGrouping(int precision)
      throws Exception {
    NativeParity.assertParity(
        () -> environment(precision, "UTC", 1031, null), "SELECT id, null_epoch(z) FROM src");
    NativeParity.assertChangelogParity(
        () -> environment(precision, "UTC", 1031, null),
        "SELECT ts_identity(t), COUNT(*) FROM src GROUP BY ts_identity(t)");
  }

  @Test
  void conditionalAndFilteredEvaluationSkipsUnselectedFailures() throws Exception {
    NativeParity.assertParity(
        () -> environment(9, "UTC", 1031, null),
        "SELECT id, CASE WHEN MOD(id,8) = 1 THEN t ELSE temporal_throw(t) END FROM src");
    NativeParity.assertParity(
        () -> environment(9, "UTC", 1031, null),
        "SELECT id, temporal_throw(t) FROM src WHERE MOD(id,8) <> 1");
  }

  @Test
  void callbackFailureKeepsTheReleasedHostException() {
    var comparison =
        NativeFailureParity.run(
            () -> environment(9, "UTC", 32, null), "SELECT temporal_throw(t) FROM src");
    comparison.assertFailure(
        IllegalStateException.class,
        "temporal sentinel",
        NativeFailureParity.Phase.ROW_EVALUATION,
        NativeFailureParity.Route.NATIVE);
  }

  @Test
  void checkedAndErrorFailuresKeepTheOriginalTemporalCause() {
    for (boolean error : new boolean[] {false, true}) {
      var comparison =
          NativeFailureParity.run(
              () -> {
                var table = environment(9, "UTC", 32, null);
                table.createTemporarySystemFunction(
                    "temporal_failure", new ExceptionalTimestamp(error));
                return table;
              },
              "SELECT temporal_failure(t) FROM src");
      comparison.assertFailure(
          error ? AssertionError.class : Exception.class,
          "temporal failure sentinel",
          NativeFailureParity.Phase.ROW_EVALUATION,
          NativeFailureParity.Route.NATIVE);
    }
  }

  @Test
  void emptyAndAllFilteredInputsDoNotInvokeTheCallback() throws Exception {
    NativeParity.assertParity(
        () -> environment(3, "UTC", 0, null), "SELECT ts_identity(t), ltz_identity(z) FROM src");
    NativeParity.assertParity(
        () -> environment(3, "UTC", 1031, null), "SELECT temporal_throw(t) FROM src WHERE id < 0");
  }

  @Test
  void sharedTemporalCallSitesPreserveRowOrderAndLifecycle() throws Exception {
    NativeParity.assertOrderedKindedParity(
        () -> environment(9, "UTC", 1031, null),
        "SELECT id, ts_tick(t), ts_tick(t) FROM src WHERE ts_tick(t) IS NOT NULL");
  }

  @Test
  void unsupportedConversionClassesAndSpecializationKeepFallback() throws Exception {
    NativeParity.assertFallbackReasonContains(
        () -> {
          var table = environment(3, "UTC", 32, null);
          table.createTemporarySystemFunction("legacy_temporal", new LegacyTimestamp());
          return table;
        },
        "SELECT legacy_temporal(t) FROM src",
        "unsupported Java conversion class");
    NativeParity.assertFallbackReasonContains(
        () -> {
          var table = environment(3, "UTC", 32, null);
          table.createTemporarySystemFunction(
              "special_temporal", new FlinkUdfLifecycleSqlHarnessTest.SpecializedTypeOf());
          return table;
        },
        "SELECT special_temporal(t) FROM src",
        "UDF specialization requires Flink's code-generation context");
  }

  static TableEnvironment environment(
      int precision, String zone, int count, RecoveryExecutionMetrics metrics) {
    return environment(precision, zone, count, metrics, true);
  }

  private static TableEnvironment environment(
      int precision, String zone, int count, RecoveryExecutionMetrics metrics, boolean enabled) {
    var env =
        metrics == null
            ? StreamExecutionEnvironment.getExecutionEnvironment()
            : metrics.environment(new Configuration(), 1, 0);
    env.setParallelism(1);
    var table = StreamTableEnvironment.create(env);
    if (enabled) table.getConfig().set("streamfusion.calc.temporalUdf.enabled", "true");
    table.getConfig().setLocalTimeZone(ZoneId.of(zone));
    table.getConfig().set("streamfusion.transpose.batchRows", "127");
    table.createTemporarySystemFunction("ts_identity", new TimestampIdentity(precision));
    table.createTemporarySystemFunction("ts_text", new TimestampText(precision));
    table.createTemporarySystemFunction("ltz_identity", new LtzIdentity(precision));
    table.createTemporarySystemFunction("ltz_epoch", new LtzEpoch(precision, false));
    table.createTemporarySystemFunction("null_epoch", new LtzEpoch(precision, true));
    table.createTemporarySystemFunction("temporal_throw", new ThrowingTimestamp(precision));
    table.createTemporarySystemFunction("ts_tick", new SequencedTimestamp(precision));
    var type =
        DataTypes.ROW(
            DataTypes.FIELD("id", DataTypes.INT().notNull()),
            DataTypes.FIELD("t", DataTypes.TIMESTAMP(precision)),
            DataTypes.FIELD("z", DataTypes.TIMESTAMP_LTZ(precision)));
    int cardinality = Integer.getInteger("temporal.cardinality", 8);
    if (cardinality < 1)
      throw new IllegalArgumentException("temporal.cardinality must be positive");
    LocalDateTime[] values =
        java.util.stream.IntStream.range(0, cardinality)
            .mapToObj(
                index -> {
                  if (cardinality == 8) return timestamp(index, precision);
                  if (index % 8 == 7) return null;
                  int unit = (int) Math.pow(10, 9 - precision);
                  int nano = (int) ((long) index * 123457 % 1_000_000_000);
                  return LocalDateTime.of(2026, 1, 1, 0, 0)
                      .plusSeconds(index)
                      .withNano(nano / unit * unit);
                })
            .toArray(LocalDateTime[]::new);
    var source =
        env.fromSequence(0, Math.max(0, count - 1))
            .filter(i -> i < count)
            .map(
                i -> {
                  LocalDateTime value = values[(int) (i % values.length)];
                  return Row.of(
                      i.intValue(),
                      value,
                      value == null ? null : value.toInstant(java.time.ZoneOffset.UTC));
                })
            .returns(ExternalTypeInfo.<Row>of(type));
    table.createTemporaryView(
        "src", table.fromDataStream(source, Schema.newBuilder().fromRowDataType(type).build()));
    return table;
  }

  private static LocalDateTime timestamp(int index, int precision) {
    String[] values = {
      "0001-01-01T00:00:00.123456789",
      "1969-12-31T23:59:59.999999999",
      "1970-01-01T00:00:00.000000001",
      "2026-03-08T01:59:59.999999999",
      "2026-03-08T02:30:00.123456789",
      "2026-11-01T01:30:00.999999999",
      "9999-12-31T23:59:59.999999999"
    };
    if (index == values.length) return null;
    LocalDateTime value = LocalDateTime.parse(values[index]);
    int unit = (int) Math.pow(10, 9 - precision);
    return value.withNano(value.getNano() / unit * unit);
  }

  public abstract static class TemporalFunction extends ScalarFunction {
    private final DataType input;
    private final DataType output;
    private transient boolean opened;

    TemporalFunction(DataType input, DataType output) {
      this.input = input;
      this.output = output;
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory factory) {
      return TypeInference.newBuilder()
          .typedArguments(input)
          .outputTypeStrategy(TypeStrategies.explicit(output))
          .build();
    }

    @Override
    public void open(FunctionContext context) {
      if (opened) throw new IllegalStateException("temporal UDF opened twice");
      opened = true;
    }

    void checkOpen() {
      if (!opened) throw new IllegalStateException("temporal UDF evaluated outside lifecycle");
    }

    @Override
    public void close() {
      if (!opened) throw new IllegalStateException("temporal UDF closed twice");
      opened = false;
    }
  }

  public static class TimestampIdentity extends TemporalFunction {
    TimestampIdentity(int precision) {
      super(DataTypes.TIMESTAMP(precision), DataTypes.TIMESTAMP(precision));
    }

    public LocalDateTime eval(LocalDateTime value) {
      checkOpen();
      return value;
    }
  }

  public static class TimestampText extends TemporalFunction {
    TimestampText(int precision) {
      super(DataTypes.TIMESTAMP(precision), DataTypes.STRING());
    }

    public String eval(LocalDateTime value) {
      checkOpen();
      return value == null ? null : value.toString();
    }
  }

  public static class LtzIdentity extends TemporalFunction {
    LtzIdentity(int precision) {
      super(DataTypes.TIMESTAMP_LTZ(precision), DataTypes.TIMESTAMP_LTZ(precision));
    }

    public Instant eval(Instant value) {
      checkOpen();
      return value;
    }
  }

  public static class LtzEpoch extends TemporalFunction {
    private final boolean nullSentinel;

    LtzEpoch(int precision, boolean nullSentinel) {
      super(DataTypes.TIMESTAMP_LTZ(precision), DataTypes.BIGINT());
      this.nullSentinel = nullSentinel;
    }

    public Long eval(Instant value) {
      checkOpen();
      if (value == null) return nullSentinel ? Long.valueOf(-7L) : null;
      return value.toEpochMilli();
    }
  }

  public static class ThrowingTimestamp extends TemporalFunction {
    ThrowingTimestamp(int precision) {
      super(DataTypes.TIMESTAMP(precision), DataTypes.TIMESTAMP(precision));
    }

    public LocalDateTime eval(LocalDateTime value) {
      checkOpen();
      if (value != null && value.getYear() == 1969)
        throw new IllegalStateException("temporal sentinel");
      return value;
    }
  }

  public static class SequencedTimestamp extends TemporalFunction {
    private int sequence;

    SequencedTimestamp(int precision) {
      super(DataTypes.TIMESTAMP(precision), DataTypes.TIMESTAMP(precision));
    }

    @Override
    public boolean isDeterministic() {
      return false;
    }

    public LocalDateTime eval(LocalDateTime value) {
      checkOpen();
      return LocalDateTime.of(2001, 1, 1, 0, 0).plusSeconds(++sequence);
    }
  }

  public static class ChangingTimestamp extends TemporalFunction {
    private int sequence;

    ChangingTimestamp(int precision) {
      super(DataTypes.TIMESTAMP(precision), DataTypes.TIMESTAMP(precision));
    }

    @Override
    public boolean isDeterministic() {
      return false;
    }

    public LocalDateTime eval(LocalDateTime value) {
      checkOpen();
      if (++sequence <= 17) return value;
      if (sequence % 13 == 0) return null;
      return value == null ? LocalDateTime.of(2001, 1, 1, 0, 0) : value.plusSeconds(1);
    }
  }

  public static class ExceptionalTimestamp extends TemporalFunction {
    private final boolean error;

    ExceptionalTimestamp(boolean error) {
      super(DataTypes.TIMESTAMP(9), DataTypes.TIMESTAMP(9));
      this.error = error;
    }

    public LocalDateTime eval(LocalDateTime value) throws Exception {
      checkOpen();
      if (error) throw new AssertionError("temporal failure sentinel");
      throw new Exception("temporal failure sentinel");
    }
  }

  public static class LegacyTimestamp extends TemporalFunction {
    LegacyTimestamp() {
      super(DataTypes.TIMESTAMP(3), DataTypes.TIMESTAMP(3).bridgedTo(java.sql.Timestamp.class));
    }

    public java.sql.Timestamp eval(LocalDateTime value) {
      checkOpen();
      return value == null ? null : java.sql.Timestamp.valueOf(value);
    }
  }
}
