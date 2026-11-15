package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.runtime.typeutils.ExternalTypeInfo;
import org.apache.flink.types.Row;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

@org.junit.jupiter.api.condition.DisabledIfSystemProperty(
    named = "streamfusion.experimental.jvmGroupAggregates",
    matches = "false")
class FlinkJvmTwoPhaseSqlHarnessTest {
  @org.junit.jupiter.api.Test
  void monotonicBuiltinPartialsKeepTheReleasedLocalRetractionLayout() {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var runs =
          NativeFailureParity.run(
              () -> configure(FlinkJvmMiniBatchSqlHarnessTest.environment(3, metrics), 3),
              "SELECT MOD(k,2),test_aggregate(c),MIN(c),MAX(c) FROM"
                  + " (SELECT k,COUNT(*) AS c FROM inputs GROUP BY k) s GROUP BY MOD(k,2)");
      assertNull(runs.host().failure(), runs::toString);
      assertNull(runs.nativeRun().failure(), runs::toString);
      assertEquals(NativeFailureParity.Route.NATIVE, runs.nativeRun().route());
      // The preceding Rust count can emit different intermediate mini-batch updates.
      assertEquals(
          SqlAuditHarness.materialized(runs.host().rows()),
          SqlAuditHarness.materialized(runs.nativeRun().rows()));
      assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
      assertPhases(metrics.nativeOperators());
    }
  }

  public static class MergeWeightedAverage
      extends FlinkLegacyJvmAggregateSqlHarnessTest.WeightedAverage {
    public void merge(
        FlinkLegacyJvmAggregateSqlHarnessTest.Accumulator state,
        Iterable<FlinkLegacyJvmAggregateSqlHarnessTest.Accumulator> others) {
      for (var other : others) {
        state.sum += other.sum;
        state.weight += other.weight;
      }
    }

    public void resetAccumulator(FlinkLegacyJvmAggregateSqlHarnessTest.Accumulator state) {
      state.sum = 0;
      state.weight = 0;
    }
  }

  @ParameterizedTest
  @CsvSource({"false,false", "false,true", "true,false", "true,true"})
  void releasedLegacyWrapperMergesPojoAndSharedDistinctState(boolean updating, boolean ttl) {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var runs =
          NativeFailureParity.run(
              () -> {
                var table =
                    FlinkLegacyJvmAggregateSqlHarnessTest.environment(updating, ttl, metrics);
                table.dropTemporarySystemFunction("legacy_avg");
                return configure(
                    FlinkLegacyJvmAggregateSqlHarnessTest.register(
                        table, ttl, new MergeWeightedAverage()),
                    3);
              },
              FlinkLegacyJvmAggregateSqlHarnessTest.QUERY);
      runs.assertSuccess(NativeFailureParity.Route.NATIVE);
      assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
      assertPhases(metrics.nativeOperators());
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void emptyInputAndNullOnlyBundlesMatchReleasedFlink(boolean empty) {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var runs =
          NativeFailureParity.run(
              () -> configure(FlinkJvmMiniBatchSqlHarnessTest.environment(3, metrics), 3),
              "SELECT test_aggregate(CAST(NULL AS BIGINT)),COUNT(*) FROM inputs"
                  + (empty ? " WHERE k < -100" : ""));
      runs.assertSuccess(NativeFailureParity.Route.NATIVE);
      assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
      if (!empty) assertPhases(metrics.nativeOperators());
    }
  }

  @org.junit.jupiter.api.Test
  void disabledJvmGroupsKeepTheOriginalTwoPhaseHostPlan() throws Exception {
    String property = "streamfusion.operator.jvmGroupAggregate.enabled";
    String previous = System.getProperty(property);
    try {
      System.setProperty(property, "false");
      NativeParity.assertFallbackReasonContains(
          () ->
              configure(
                  PortableSqlFixtures.environment(
                      org.apache.flink.api.common.RuntimeExecutionMode.STREAMING,
                      java.util.Map.of()),
                  3),
          "SELECT k,test_aggregate(v),COUNT(*) FROM test_input GROUP BY k",
          "jvmGroupAggregate: disabled by config");
    } finally {
      if (previous == null) System.clearProperty(property);
      else System.setProperty(property, previous);
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"STRING", "DECIMAL(38,18)", "TIMESTAMP(9)", "TIMESTAMP_LTZ(9)"})
  void nullableWideAndCollidingKeysKeepLocalAndGlobalRouting(String type) {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var runs =
          NativeFailureParity.run(
              () -> configure(FlinkJvmMiniBatchSqlHarnessTest.environment(7, 3, metrics, type), 7),
              "SELECT k,test_aggregate(v),COUNT(*) FROM inputs GROUP BY k");
      runs.assertSuccess(NativeFailureParity.Route.NATIVE);
      assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
      assertPhases(metrics.nativeOperators());
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void keylessAndNonleadingKeysRetainTheirPhysicalPhases(boolean keyless) {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var runs =
          NativeFailureParity.run(
              () -> configure(FlinkJvmMiniBatchSqlHarnessTest.environment(3, 3, metrics), 3),
              keyless
                  ? "SELECT test_aggregate(v),COUNT(*) FROM inputs"
                  : "SELECT MOD(v,3) AS z,k,test_aggregate(v),COUNT(*) FROM inputs GROUP BY"
                      + " MOD(v,3),k");
      runs.assertSuccess(NativeFailureParity.Route.NATIVE);
      assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
      assertPhases(metrics.nativeOperators());
    }
  }

  public static class FailureAggregate extends PortableSqlFixtures.Sum {
    private final String phase;

    public FailureAggregate(String phase) {
      this.phase = phase;
    }

    private void fail(String actual) {
      if (phase.equals(actual)) throw new IllegalStateException("two-phase failure: " + actual);
    }

    @Override
    public void open(org.apache.flink.table.functions.FunctionContext context) {
      super.open(context);
      fail("open");
    }

    @Override
    public void accumulate(PortableSqlFixtures.SumState state, Long value) {
      super.accumulate(state, value);
      fail("accumulate");
    }

    @Override
    public void merge(
        PortableSqlFixtures.SumState state, Iterable<PortableSqlFixtures.SumState> others) {
      super.merge(state, others);
      fail("merge");
    }

    @Override
    public Long getValue(PortableSqlFixtures.SumState state) {
      fail("value");
      return super.getValue(state);
    }

    @Override
    public void close() {
      super.close();
      fail("close");
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"open", "accumulate", "merge", "value", "close"})
  void lifecycleAndCallbackFailuresRetainReleasedCauses(String phase) throws Exception {
    var runs =
        NativeFailureParity.run(
            () -> {
              var table =
                  configure(
                      PortableSqlFixtures.environment(
                          org.apache.flink.api.common.RuntimeExecutionMode.STREAMING,
                          java.util.Map.of()),
                      3);
              table.createTemporarySystemFunction("failure", new FailureAggregate(phase));
              return table;
            },
            "SELECT k,failure(v) FROM test_input GROUP BY k");
    if (phase.equals("close")) runs.assertSuccess(NativeFailureParity.Route.NATIVE);
    else {
      assertNotNull(runs.host().failure(), runs::toString);
      assertNotNull(runs.nativeRun().failure(), runs::toString);
      assertEquals(runs.host().rootCause().getClass(), runs.nativeRun().rootCause().getClass());
      assertEquals(runs.host().rootCause().getMessage(), runs.nativeRun().rootCause().getMessage());
      assertEquals(NativeFailureParity.Route.NATIVE, runs.nativeRun().route());
    }
    assertTrue(runs.nativeRun().plan().contains("phase=[local]"), runs::toString);
    assertTrue(runs.nativeRun().plan().contains("phase=[global]"), runs::toString);
    SharedFlinkCluster.assertNativeMemoryReleased("two-phase-jvm-failure-" + phase);
  }

  public static class SignedViewState {
    public org.apache.flink.table.api.dataview.MapView<Long, Long> values =
        new org.apache.flink.table.api.dataview.MapView<>();
    public long count;
  }

  @org.apache.flink.table.annotation.FunctionHint(
      input = @org.apache.flink.table.annotation.DataTypeHint("BIGINT"),
      output = @org.apache.flink.table.annotation.DataTypeHint("STRING"))
  public static class SignedViewAggregate
      extends org.apache.flink.table.functions.AggregateFunction<String, SignedViewState> {
    @Override
    public SignedViewState createAccumulator() {
      return new SignedViewState();
    }

    private void add(SignedViewState state, Long value, long delta) throws Exception {
      if (value == null) return;
      Long old = state.values.get(value);
      long count = (old == null ? 0 : old) + delta;
      if (count == 0) state.values.remove(value);
      else state.values.put(value, count);
      state.count += delta;
    }

    public void accumulate(SignedViewState state, Long value) throws Exception {
      add(state, value, 1);
    }

    public void retract(SignedViewState state, Long value) throws Exception {
      add(state, value, -1);
    }

    public void merge(SignedViewState state, Iterable<SignedViewState> others) throws Exception {
      for (var other : others)
        for (var entry : other.values.entries()) add(state, entry.getKey(), entry.getValue());
    }

    public void resetAccumulator(SignedViewState state) {
      state.values.clear();
      state.count = 0;
    }

    @Override
    public String getValue(SignedViewState state) {
      if (state.count == 0) return null;
      try {
        var result = new java.util.ArrayList<Long>();
        for (var entry : state.values.entries())
          for (long n = 0; n < entry.getValue(); n++) result.add(entry.getKey());
        java.util.Collections.sort(result);
        return result.toString();
      } catch (Exception failure) {
        throw new IllegalStateException(failure);
      }
    }
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "INT",
        "BIGINT",
        "STRING",
        "CHAR(4)",
        "BOOLEAN",
        "TINYINT",
        "SMALLINT",
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
  void typedArgumentsResultsAndAccumulatorPartials(String type) {
    for (boolean updating : java.util.List.of(false, true)) {
      try (var metrics = new RecoveryExecutionMetrics()) {
        var runs =
            NativeFailureParity.run(
                () -> {
                  var table =
                      FlinkTypedJvmAggregateSqlHarnessTest.environment(
                          type, updating, false, metrics);
                  var value = FlinkTypedTableFunctionSqlHarnessTest.valueType(type);
                  table.dropTemporarySystemFunction("remember");
                  table.createTemporarySystemFunction("remember", new MergeRemember(value));
                  return configure(table, 7);
                },
                "SELECT k,remember(v),COUNT(*) FROM src GROUP BY k");
        runs.assertSuccess(NativeFailureParity.Route.NATIVE);
        assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
        assertPhases(metrics.nativeOperators());
      }
    }
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "INT",
        "BIGINT",
        "STRING",
        "CHAR(4)",
        "BOOLEAN",
        "TINYINT",
        "SMALLINT",
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
  void scalarGroupingKeysPreserveNullExtremesAndRetractions(String type) {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var runs =
          NativeFailureParity.run(
              () -> {
                var table =
                      FlinkTypedJvmAggregateSqlHarnessTest.environment(type, true, true, metrics, 3);
                table.createTemporarySystemFunction(
                    "test_aggregate", new PortableSqlFixtures.Sum());
                return configure(table, 7);
              },
              "SELECT k,test_aggregate(CAST(v AS BIGINT)),COUNT(*) FROM src GROUP BY k");
      runs.assertSuccess(NativeFailureParity.Route.NATIVE);
      assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
      assertPhases(metrics.nativeOperators());
    }
  }

  public static class MergeRemember extends FlinkTypedJvmAggregateSqlHarnessTest.Remember {
    public MergeRemember(org.apache.flink.table.types.DataType type) {
      super(type);
    }

    public void merge(Row state, Iterable<Row> others) {
      for (Row other : others) {
        long count = (Long) other.getField(1);
        if (count != 0) {
          Object value = other.getField(0);
          state.setField(0, value instanceof byte[] ? ((byte[]) value).clone() : value);
        }
        state.setField(1, (Long) state.getField(1) + count);
      }
      if ((Long) state.getField(1) == 0) state.setField(0, null);
    }

    public void resetAccumulator(Row state) {
      state.setField(0, null);
      state.setField(1, 0L);
    }
  }

  @ParameterizedTest
  @CsvSource({"test_input,1", "cdc_input,1", "test_input,3", "cdc_input,3"})
  void sharedDistinctFilteredAndManagedViews(String source, int size) {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var runs =
          NativeFailureParity.run(
              () -> {
                var table =
                    PortableSqlFixtures.environment(
                        org.apache.flink.api.common.RuntimeExecutionMode.STREAMING,
                        java.util.Map.of(),
                        metrics.environment(
                            new org.apache.flink.configuration.Configuration(), 1, 0));
                table.createTemporarySystemFunction(
                    "view_aggregate", new FlinkJvmAggregateSqlHarnessTest.ViewAggregate());
                return configure(table, size);
              },
              "SELECT k,test_aggregate(DISTINCT v),test_aggregate(v),"
                  + "view_aggregate(DISTINCT v) FILTER (WHERE v >= 0),COUNT(*) FROM "
                  + source
                  + " GROUP BY k");
      runs.assertSuccess(NativeFailureParity.Route.NATIVE);
      assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
      assertTrue(runs.nativeRun().plan().contains("phase=[local]"), runs::toString);
      assertTrue(runs.nativeRun().plan().contains("phase=[global]"), runs::toString);
      assertPhases(metrics.nativeOperators());
    }
  }

  @ParameterizedTest
  @CsvSource({
    "hashmap,false,1",
    "hashmap,true,1",
    "hashmap,false,3",
    "hashmap,true,3",
    "tech.streamfusion.state.RocksDBNativeStateBackendFactory,false,1",
    "tech.streamfusion.state.RocksDBNativeStateBackendFactory,true,1",
    "tech.streamfusion.state.RocksDBNativeStateBackendFactory,false,3",
    "tech.streamfusion.state.RocksDBNativeStateBackendFactory,true,3"
  })
  void partialsDrainBeforeRepeatedRecovery(String backend, boolean updating, int parallelism)
      throws Exception {
    var rows = new java.util.ArrayList<Row>();
    for (int i = 0; i < 80; i++)
      rows.add(Row.of(i % 4 == 0 ? null : i % 4, i % 7 == 0 ? null : (long) i));
    if (updating)
      for (int i = 0; i < 16; i++) {
        var old = Row.of(i % 4 == 0 ? null : i % 4, i % 7 == 0 ? null : (long) i);
        old.setKind(org.apache.flink.types.RowKind.DELETE);
        rows.add(old);
        rows.add(Row.of(i % 4 == 0 ? null : i % 4, 1000L + i));
      }
    var type =
        DataTypes.ROW(
            DataTypes.FIELD("k", DataTypes.INT()), DataTypes.FIELD("v", DataTypes.BIGINT()));
    try (var recovery =
        new PortableSqlRecovery(
                backend,
                rows,
                ExternalTypeInfo.<Row>of(type),
                Schema.newBuilder().fromRowDataType(type).build(),
                updating,
                16,
                64)
            .withRescaleDeployment(parallelism, null, null)
            .withExecutionMetrics()) {
      var runs =
          NativeFailureParity.runRecovery(
              () -> register(configure(recovery.uninterrupted(), 7)),
              () -> register(configure(recovery.get(), 7)),
              "SELECT k,test_aggregate(v),view_aggregate(v),COUNT(*),test_aggregate(DISTINCT v),"
                  + "signed_view(DISTINCT v) FILTER (WHERE v>=0) FROM recovery_input GROUP BY k");
      assertNull(runs.host().failure(), runs::toString);
      assertNull(runs.nativeRun().failure(), runs::toString);
      assertEquals(NativeFailureParity.Route.NATIVE, runs.nativeRun().route());
      assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
      assertEquals(
          SqlAuditHarness.materialized(runs.host().rows()),
          SqlAuditHarness.materialized(runs.nativeRun().rows()));
      recovery.verifyRepeatedRecovery();
      assertPhases(recovery.nativeExecution());
      assertTrue(recovery.counterValue("jvmMiniBatchBundles") > 0);
    }
    SharedFlinkCluster.assertNativeMemoryReleased("two-phase-jvm-recovery");
  }

  static TableEnvironment configure(TableEnvironment table, int size) {
    table.getConfig().set("table.exec.mini-batch.enabled", "true");
    table.getConfig().set("table.exec.mini-batch.size", Integer.toString(size));
    table.getConfig().set("table.exec.mini-batch.allow-latency", "1 h");
    table.getConfig().set("table.optimizer.agg-phase-strategy", "TWO_PHASE");
    table.getConfig().set("streamfusion.transpose.batchRows", "2");
    return table;
  }

  private static TableEnvironment register(TableEnvironment table) {
    table.createTemporarySystemFunction("test_aggregate", new PortableSqlFixtures.Sum());
    table.createTemporarySystemFunction(
        "view_aggregate", new FlinkJvmAggregateSqlHarnessTest.ViewAggregate());
    table.createTemporarySystemFunction("signed_view", new SignedViewAggregate());
    return table;
  }

  static void assertPhases(java.util.List<java.util.Map<String, Object>> operators) {
    RecoveryExecutionMetrics.assertConsumed(operators, "NativeJvmLocalGroup");
    RecoveryExecutionMetrics.assertConsumed(operators, "NativeJvmGlobalGroup");
  }

  @ParameterizedTest
  @CsvSource({"1,1", "3,1", "17,1", "1000,1", "3,3", "17,3"})
  void localAndGlobalConsumeRealBundles(int size, int parallelism) {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var runs =
          NativeFailureParity.run(
              () -> {
                var table = FlinkJvmMiniBatchSqlHarnessTest.environment(size, parallelism, metrics);
                table.getConfig().set("table.optimizer.agg-phase-strategy", "TWO_PHASE");
                return table;
              },
              "SELECT k,test_aggregate(v),view_aggregate(v),COUNT(*) FROM inputs GROUP BY k");
      runs.assertSuccess(NativeFailureParity.Route.NATIVE);
      assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
      assertTrue(runs.host().plan().contains("LocalGroupAggregate"), runs::toString);
      assertTrue(runs.host().plan().contains("GlobalGroupAggregate"), runs::toString);
      assertTrue(runs.nativeRun().plan().contains("phase=[local]"), runs::toString);
      assertTrue(runs.nativeRun().plan().contains("phase=[global]"), runs::toString);
      assertPhases(metrics.nativeOperators());
      assertTrue(metrics.counterValue("jvmMiniBatchRows") > 96);
      assertTrue(metrics.counterValue("jvmMiniBatchBundles") > 0);
    }
  }
}
