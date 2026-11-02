package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.runtime.typeutils.ExternalTypeInfo;
import org.apache.flink.types.Row;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

@org.junit.jupiter.api.condition.DisabledIfSystemProperty(
    named = "streamfusion.experimental.jvmGroupAggregates",
    matches = "false")
class FlinkJvmMiniBatchSqlHarnessTest {
  @ParameterizedTest
  @CsvSource({"false,ONE_PHASE", "true,ONE_PHASE", "true,TWO_PHASE"})
  void aggregateWithoutMergeRetainsItsActualSinglePhase(boolean enabled, String phase) {
    for (String sql :
        java.util.List.of(
            "SELECT k,nonmerge_max(v) FROM inputs GROUP BY k",
            "SELECT v,nonmerge_max(CAST(k AS STRING)) FROM inputs GROUP BY v")) {
      try (var metrics = new RecoveryExecutionMetrics()) {
        var runs =
            NativeFailureParity.run(
                () -> {
                  var table = environment(7, metrics);
                  table.getConfig().set("table.exec.mini-batch.enabled", Boolean.toString(enabled));
                  table.getConfig().set("table.optimizer.agg-phase-strategy", phase);
                  table.createTemporarySystemFunction("nonmerge_max", new NoMergeMaximum());
                  return table;
                },
                sql);
        runs.assertSuccess(NativeFailureParity.Route.NATIVE);
        assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
        assertFalse(runs.host().plan().contains("LocalGroupAggregate"), runs::toString);
        assertFalse(runs.nativeRun().plan().contains("LocalGroupAggregate"), runs::toString);
        RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeJvmGroup");
        assertEquals(enabled, metrics.counterValue("jvmMiniBatchRows") > 0);
      }
    }
  }

  public static class NoMergeMaximum
      extends org.apache.flink.table.functions.AggregateFunction<Object, Row> {
    @Override
    public Row createAccumulator() {
      return new Row(1);
    }

    @org.apache.flink.table.annotation.FunctionHint(
        accumulator = @org.apache.flink.table.annotation.DataTypeHint("ROW<max BIGINT>"),
        output = @org.apache.flink.table.annotation.DataTypeHint("BIGINT"))
    public void accumulate(Row state, Long value) {
      if (value != null && (state.getField(0) == null || value > (Long) state.getField(0)))
        state.setField(0, value);
    }

    @org.apache.flink.table.annotation.FunctionHint(
        accumulator = @org.apache.flink.table.annotation.DataTypeHint("ROW<max STRING>"),
        output = @org.apache.flink.table.annotation.DataTypeHint("STRING"))
    public void accumulate(Row state, String value) {
      if (value != null
          && (state.getField(0) == null || value.compareTo((String) state.getField(0)) > 0))
        state.setField(0, value);
    }

    @Override
    public Object getValue(Row state) {
      return state.getField(0);
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"test_input", "cdc_input"})
  void sharedDistinctAndFilteredViewsRetainBundleChanges(String source) {
    var runs =
        NativeFailureParity.run(
            () -> {
              var table =
                  PortableSqlFixtures.environment(
                      org.apache.flink.api.common.RuntimeExecutionMode.STREAMING,
                      java.util.Map.of(
                          "table.exec.mini-batch.enabled",
                          "true",
                          "table.exec.mini-batch.size",
                          "3",
                          "table.exec.mini-batch.allow-latency",
                          "1 h"));
              table.createTemporarySystemFunction(
                  "view_aggregate", new FlinkJvmAggregateSqlHarnessTest.ViewAggregate());
              return table;
            },
            "SELECT k,test_aggregate(DISTINCT v),test_aggregate(v),"
                + "view_aggregate(DISTINCT v) FILTER (WHERE v >= 0),COUNT(*) FROM "
                + source
                + " GROUP BY k");
    runs.assertSuccess(NativeFailureParity.Route.NATIVE);
    assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
  }

  @ParameterizedTest
  @ValueSource(ints = {-1, 0, -17})
  void invalidMiniBatchSizesKeepReleasedPlanningFailure(int size) {
    var runs =
        NativeFailureParity.run(
            () ->
                PortableSqlFixtures.environment(
                    org.apache.flink.api.common.RuntimeExecutionMode.STREAMING,
                    java.util.Map.of(
                        "table.exec.mini-batch.enabled",
                        "true",
                        "table.exec.mini-batch.size",
                        Integer.toString(size),
                        "table.exec.mini-batch.allow-latency",
                        "1 h")),
            "SELECT k,test_aggregate(v) FROM test_input GROUP BY k");
    assertNotNull(runs.host().failure());
    assertNotNull(runs.nativeRun().failure());
    assertEquals(NativeFailureParity.Phase.PLANNING, runs.host().phase());
    assertEquals(runs.host().phase(), runs.nativeRun().phase());
    assertEquals(runs.host().rootCause().getClass(), runs.nativeRun().rootCause().getClass());
    assertEquals(runs.host().rootCause().getMessage(), runs.nativeRun().rootCause().getMessage());
  }

  @ParameterizedTest
  @ValueSource(strings = {"open", "eval", "close"})
  void releasedLifecycleAndFailuresKeepTheirCause(String phase) throws Exception {
    var runs =
        NativeFailureParity.run(
            () -> {
              var table =
                  PortableSqlFixtures.environment(
                      org.apache.flink.api.common.RuntimeExecutionMode.STREAMING,
                      java.util.Map.of(
                          "table.exec.mini-batch.enabled",
                          "true",
                          "table.exec.mini-batch.size",
                          "3",
                          "table.exec.mini-batch.allow-latency",
                          "1 h"));
              table.createTemporarySystemFunction(
                  "failing_aggregate", new FlinkJvmAggregateSqlHarnessTest.FailingAggregate(phase));
              return table;
            },
            "SELECT k,failing_aggregate(v) FROM test_input GROUP BY k");
    if (phase.equals("close")) {
      // Released bundle operators close RichFunction only; their map function is not rich.
      runs.assertSuccess(NativeFailureParity.Route.NATIVE);
    } else {
      assertNotNull(runs.host().failure());
      assertNotNull(runs.nativeRun().failure());
      assertEquals(runs.host().rootCause().getClass(), runs.nativeRun().rootCause().getClass());
      assertEquals(runs.host().rootCause().getMessage(), runs.nativeRun().rootCause().getMessage());
      assertEquals(NativeFailureParity.Route.NATIVE, runs.nativeRun().route());
    }
    SharedFlinkCluster.assertNativeMemoryReleased("mini-batch-" + phase);
  }

  @org.junit.jupiter.api.Test
  void stateTtlHintRetainsReleasedBundleState() {
    tech.streamfusion.compat.FlinkTestCapabilities.requireStateTtlHint();
    try (var metrics = new RecoveryExecutionMetrics()) {
      var runs =
          NativeFailureParity.run(
              () -> environment(7, metrics),
              "SELECT /*+ STATE_TTL('inputs'='1 h') */ k,test_aggregate(v),COUNT(*)"
                  + " FROM inputs GROUP BY k");
      runs.assertSuccess(NativeFailureParity.Route.NATIVE);
      RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeJvmGroup");
      assertTrue(metrics.counterValue("jvmMiniBatchBundles") > 0);
    }
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
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
  void scalarArgumentsResultsAndRetractionsRetainReleasedBundleSemantics(String type) {
    for (boolean updating : java.util.List.of(false, true)) {
      try (var metrics = new RecoveryExecutionMetrics()) {
        var runs =
            NativeFailureParity.run(
                () ->
                    configure(
                        FlinkTypedJvmAggregateSqlHarnessTest.environment(
                            type, updating, false, metrics),
                        7),
                "SELECT k,remember(v),COUNT(*) FROM src GROUP BY k");
        runs.assertSuccess(NativeFailureParity.Route.NATIVE);
        assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
        RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeJvmGroup");
        assertTrue(metrics.counterValue("jvmMiniBatchRows") > 0);
        assertTrue(metrics.counterValue("jvmMiniBatchBundles") > 0);
      }
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"STRING", "DECIMAL(38,18)", "TIMESTAMP(9)", "TIMESTAMP_LTZ(9)"})
  void nullableWideAndCollidingKeysKeepThreeTaskBundleMembership(String keyType) {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var runs =
          NativeFailureParity.run(
              () -> environment(7, 3, metrics, keyType),
              "SELECT k,test_aggregate(v),COUNT(*) FROM inputs GROUP BY k");
      runs.assertSuccess(NativeFailureParity.Route.NATIVE);
      assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
      RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeJvmGroup");
      assertEquals(96, metrics.counterValue("jvmMiniBatchRows"));
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
  void managedDataViewsAndChangelogRecoverNonemptyBundles(
      String backend, boolean updating, int parallelism) throws Exception {
    var rows = new java.util.ArrayList<Row>();
    for (int i = 0; i < 80; i++) rows.add(input(i));
    if (updating)
      for (int i = 0; i < 16; i++) {
        var old = input(i);
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
              () -> configure(recovery.uninterrupted(), 7),
              () -> configure(recovery.get(), 7),
              "SELECT k,test_aggregate(v),view_aggregate(v),COUNT(*) FROM recovery_input GROUP BY"
                  + " k");
      assertNull(runs.host().failure(), runs::toString);
      assertNull(runs.nativeRun().failure(), runs::toString);
      assertEquals(NativeFailureParity.Route.NATIVE, runs.nativeRun().route());
      assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
      // Recovery adds barrier bundles; controlled operator tests pin their exact raw changes.
      assertEquals(
          SqlAuditHarness.materialized(runs.host().rows()),
          SqlAuditHarness.materialized(runs.nativeRun().rows()));
      recovery.verifyRepeatedRecovery();
      RecoveryExecutionMetrics.assertConsumed(recovery.nativeExecution(), "NativeJvmGroup");
      assertTrue(recovery.counterValue("jvmMiniBatchRows") > 0);
      assertTrue(recovery.counterValue("jvmMiniBatchBundles") > 0);
    }
  }

  private static Row input(int i) {
    return Row.of(i % 4 == 0 ? null : i % 4, i % 7 == 0 ? null : (long) i);
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 3, 17, 1000})
  void singlePhaseCountBundlesRetainRawChangesAndNativeWork(int size) {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var runs =
          NativeFailureParity.run(
              () -> environment(size, metrics),
              "SELECT k,test_aggregate(v),COUNT(*) FROM inputs GROUP BY k");
      runs.assertSuccess(NativeFailureParity.Route.NATIVE);
      assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
      RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeJvmGroup");
      assertEquals(96, metrics.counterValue("jvmMiniBatchRows"));
      assertTrue(metrics.counterValue("jvmMiniBatchBundles") > 0);
    }
  }

  @ParameterizedTest
  @CsvSource({"3,1", "17,1", "3,3", "17,3"})
  void orderedSourceRetainsCountBundleMembershipAcrossTasks(int size, int parallelism) {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var runs =
          NativeFailureParity.run(
              () -> environment(size, parallelism, metrics),
              "SELECT k,test_aggregate(v),COUNT(*) FROM inputs GROUP BY k");
      runs.assertSuccess(NativeFailureParity.Route.NATIVE);
      assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
      RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeJvmGroup");
      assertEquals(96, metrics.counterValue("jvmMiniBatchRows"));
      assertTrue(metrics.counterValue("jvmMiniBatchBundles") > 0);
    }
  }

  static TableEnvironment environment(int size, RecoveryExecutionMetrics metrics) {
    return environment(size, 1, metrics);
  }

  static TableEnvironment environment(int size, int parallelism, RecoveryExecutionMetrics metrics) {
    return environment(size, parallelism, metrics, "INT");
  }

  private static TableEnvironment environment(
      int size, int parallelism, RecoveryExecutionMetrics metrics, String keyType) {
    var env = metrics.environment(new Configuration(), parallelism, 0);
    env.getConfig().enableObjectReuse();
    var table = StreamTableEnvironment.create(env);
    configure(table, size);
    var type =
        DataTypes.ROW(
            DataTypes.FIELD(
                "k", FlinkMixedPrimitiveTableFunctionSqlHarnessTest.scalarType(keyType)),
            DataTypes.FIELD("v", DataTypes.BIGINT()));
    var rows =
        env.fromSequence(0, 95)
            .setParallelism(1)
            .map(i -> Row.of(key(keyType, i.intValue()), i % 7 == 0 ? null : i))
            .returns(ExternalTypeInfo.<Row>of(type))
            .setParallelism(1);
    table.createTemporaryView(
        "inputs", table.fromDataStream(rows, Schema.newBuilder().fromRowDataType(type).build()));
    return table;
  }

  private static Object key(String type, int index) {
    if (type.equals("INT")) return index % 8 == 0 ? null : index % 8;
    if (type.equals("STRING"))
      return new String[] {null, "Aa", "BB", "é🙂", "\ud800", "\udfff", "x\ud800y", ""}[index % 8];
    return index % 8 == 0
        ? null
        : FlinkMixedPrimitiveTableFunctionSqlHarnessTest.scalarValue(type, index % 7);
  }

  private static TableEnvironment configure(TableEnvironment table, int size) {
    table.getConfig().set("table.exec.mini-batch.enabled", "true");
    table.getConfig().set("table.exec.mini-batch.size", Integer.toString(size));
    table.getConfig().set("table.exec.mini-batch.allow-latency", "1 h");
    table.getConfig().set("table.optimizer.agg-phase-strategy", "ONE_PHASE");
    table.getConfig().set("streamfusion.transpose.batchRows", "2");
    table.createTemporarySystemFunction("test_aggregate", new PortableSqlFixtures.Sum());
    table.createTemporarySystemFunction(
        "view_aggregate", new FlinkJvmAggregateSqlHarnessTest.ViewAggregate());
    return table;
  }
}
