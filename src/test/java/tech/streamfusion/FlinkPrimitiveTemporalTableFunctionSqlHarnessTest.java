package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;
import static tech.streamfusion.FlinkAlternateTemporalTableFunctionSqlHarnessTest.*;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.runtime.typeutils.ExternalTypeInfo;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class FlinkPrimitiveTemporalTableFunctionSqlHarnessTest {
  static Stream<String> primitives() {
    return Stream.of(
        "time-int",
        "time-primitive-long",
        "ltz-int",
        "ltz-primitive-long",
        "yearmonth-int",
        "daytime-primitive-long");
  }

  static Stream<String> atomicPrimitives() {
    return Stream.of("time-int", "yearmonth-int", "daytime-primitive-long");
  }

  @ParameterizedTest
  @MethodSource("primitives")
  void primitiveArgumentsAndRowResultsKeepValuesTypesAndLeftPadding(String bridge) {
    for (boolean left : List.of(false, true)) {
      try (var metrics = new RecoveryExecutionMetrics()) {
        Supplier<TableEnvironment> factory = () -> environment(bridge, 96, metrics, false);
        String sql = query("src", left);
        if (left && FlinkPrimitiveResultTableFunctionSqlHarnessTest.flink118()) {
          FlinkPrimitiveResultTableFunctionSqlHarnessTest.assertReleasedLeftNullability(
              factory, sql);
          sql = sql.replace("t.v,t.m,t.pos", "t.v,t.pos");
        }
        assertValues(NativeFailureParity.run(factory, sql), metrics);
      }
    }
  }

  @ParameterizedTest
  @MethodSource("atomicPrimitives")
  void atomicPrimitiveResultsKeepReleasedValuesAndLeftPadding(String bridge) {
    for (boolean left : List.of(false, true)) {
      try (var metrics = new RecoveryExecutionMetrics()) {
        var runs =
            NativeFailureParity.run(
                () -> {
                  var table = environment(bridge, 96, metrics, false);
                  table.createTemporarySystemFunction(
                      "atomic_rows",
                      conversion(bridge) == int.class
                          ? new IntAtomicRows(bridge)
                          : new LongAtomicRows(bridge));
                  return table;
                },
                "SELECT s.x,s.p,t.m FROM src s"
                    + (left ? " LEFT JOIN " : ",")
                    + "LATERAL TABLE(atomic_rows(s.x,s.p)) AS t(m)"
                    + (left ? " ON TRUE" : ""));
        assertValues(runs, metrics);
      }
    }
  }

  @ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(
      strings = {"time-primitive-long", "ltz-int", "ltz-primitive-long"})
  void atomicMethodsKeepReleasedCompilerFailures(String bridge) {
    for (boolean typed : List.of(false, true)) {
      try (var metrics = new RecoveryExecutionMetrics()) {
        var runs =
            NativeFailureParity.run(
                () -> {
                  var table = environment(bridge, 32, metrics, false);
                  table.createTemporarySystemFunction(
                      "atomic_rows",
                      typed
                          ? conversion(bridge) == int.class
                              ? new IntAtomicRows(bridge)
                              : new LongAtomicRows(bridge)
                          : new AtomicRows(bridge, true));
                  return table;
                },
                "SELECT s.x,s.p,t.m FROM src s,LATERAL TABLE(atomic_rows(s.x,s.p)) AS t(m)");
        runs.assertFailure(
            org.codehaus.commons.compiler.CompileException.class,
            "Cannot cast",
            NativeFailureParity.Phase.INITIALIZATION,
            NativeFailureParity.Route.NATIVE);
      }
    }
  }

  @ParameterizedTest
  @MethodSource("primitives")
  void nullableSourcesKeepReleasedPrimitiveValidation(String bridge) {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var runs =
          NativeFailureParity.run(
              () -> environment(bridge, 32, metrics, true), query("src", false));
      assertNotNull(runs.host().failure(), runs::toString);
      assertNotNull(runs.nativeRun().failure(), runs::toString);
      assertEquals(NativeFailureParity.Phase.PLANNING, runs.host().phase(), runs::toString);
      assertEquals(runs.host().phase(), runs.nativeRun().phase(), runs::toString);
      assertEquals(runs.host().rootCause().getClass(), runs.nativeRun().rootCause().getClass());
      assertEquals(runs.host().rootCause().getMessage(), runs.nativeRun().rootCause().getMessage());
      assertTrue(metrics.nativeOperators().isEmpty());
    }
  }

  @ParameterizedTest
  @MethodSource("primitives")
  void invalidNullResultsKeepReleasedDefaultsAndFailures(String bridge) {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var runs =
          NativeFailureParity.run(
              () -> {
                var table = environment(bridge, 32, metrics, false);
                table.dropTemporarySystemFunction("temporal_rows");
                table.createTemporarySystemFunction(
                    "temporal_rows", new TemporalRows(bridge, "array", false, false));
                return table;
              },
              "SELECT s.x,t.m,t.pos FROM src s,LATERAL TABLE(temporal_rows(s.x,s.p)) AS"
                  + " t(v,m,pos)");
      if (bridge.startsWith("time-"))
        runs.assertFailure(
            java.time.DateTimeException.class,
            "Invalid value for NanoOfSecond",
            NativeFailureParity.Phase.COLLECTION,
            NativeFailureParity.Route.NATIVE);
      else if (bridge.startsWith("ltz-"))
        runs.assertFailure(
            RuntimeException.class,
            "Column 'm' is NOT NULL",
            NativeFailureParity.Phase.ROW_EVALUATION,
            NativeFailureParity.Route.NATIVE);
      else assertValues(runs, metrics);
    }
  }

  @ParameterizedTest
  @MethodSource("primitives")
  void retainedContainersKeepOwnedValues(String bridge) {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var runs =
          NativeFailureParity.run(
              () -> {
                var table = environment(bridge, 193, metrics, false);
                table.dropTemporarySystemFunction("temporal_rows");
                table.createTemporarySystemFunction("temporal_rows", new PreviousRows(bridge));
                return table;
              },
              query("src", false));
      assertValues(runs, metrics);
    }
  }

  @ParameterizedTest
  @MethodSource("primitives")
  void emptyInputAndCallbackFailuresKeepReleasedSemantics(String bridge) {
    for (int count : new int[] {0, 32}) {
      try (var metrics = new RecoveryExecutionMetrics()) {
        var runs =
            NativeFailureParity.run(
                () -> {
                  var table = environment(bridge, count, metrics, false);
                  table.dropTemporarySystemFunction("temporal_rows");
                  table.createTemporarySystemFunction("temporal_rows", new FailingRows(bridge));
                  return table;
                },
                query("src", false));
        if (count == 0) runs.assertSuccess(NativeFailureParity.Route.NATIVE);
        else
          runs.assertFailure(
              IllegalStateException.class,
              "primitive temporal sentinel",
              NativeFailureParity.Phase.ROW_EVALUATION,
              NativeFailureParity.Route.NATIVE);
      }
    }
  }

  @ParameterizedTest
  @MethodSource("primitives")
  void primitiveValuesAndChangelogSurviveRepeatedRecovery(String bridge) throws Exception {
    String kind = kind(bridge);
    var rows = new ArrayList<Row>();
    for (int i = 0; i < 48; i++) rows.add(input(kind, i));
    for (int i = 0; i < 16; i++)
      for (var change : List.of(RowKind.UPDATE_BEFORE, RowKind.UPDATE_AFTER, RowKind.DELETE)) {
        var row = input(kind, change == RowKind.UPDATE_BEFORE ? i : i + 2);
        row.setKind(change);
        rows.add(row);
      }
    var type = FlinkMixedPrimitiveTableFunctionSqlHarnessTest.rowType(kind, "array", false);
    for (String backend :
        List.of("hashmap", "tech.streamfusion.state.RocksDBNativeStateBackendFactory")) {
      try (var recovery =
          new PortableSqlRecovery(
                  backend,
                  rows,
                  ExternalTypeInfo.<Row>of(type),
                  Schema.newBuilder().fromRowDataType(type).build(),
                  true,
                  24,
                  72)
              .withExecutionMetrics()) {
        var runs =
            NativeFailureParity.runRecovery(
                () -> register(recovery.uninterrupted(), bridge),
                () -> register(recovery.get(), bridge),
                query("recovery_input", false));
        runs.assertSuccess(NativeFailureParity.Route.NATIVE);
        assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
        recovery.verifyRepeatedRecovery();
        RecoveryExecutionMetrics.assertConsumed(recovery.nativeExecution(), "NativeTableFunction");
      }
    }
  }

  private static Row input(String kind, int index) {
    return Row.of(
        FlinkMixedPrimitiveTableFunctionSqlHarnessTest.scalarValue(kind, index),
        FlinkMixedPrimitiveTableFunctionSqlHarnessTest.containerValue("array", index));
  }

  static TableEnvironment environment(
      String bridge, int count, RecoveryExecutionMetrics metrics, boolean nullable) {
    return register(
        FlinkMixedPrimitiveTableFunctionSqlHarnessTest.sourceEnvironment(
            kind(bridge), "array", count, metrics, nullable),
        bridge);
  }

  private static TableEnvironment register(TableEnvironment table, String bridge) {
    table.createTemporarySystemFunction("temporal_rows", new NonNullRows(bridge, false));
    return table;
  }

  private static void assertValues(
      NativeFailureParity.Comparison runs, RecoveryExecutionMetrics metrics) {
    runs.assertSuccess(NativeFailureParity.Route.NATIVE);
    assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
    RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeTableFunction");
  }

  public static class PreviousRows extends NonNullRows {
    private transient Object previous;

    public PreviousRows(String bridge) {
      super(bridge, false);
    }

    @Override
    public void eval(Object scalar, Object value) {
      super.eval(scalar, previous);
      previous = value;
    }

    @Override
    public void close() {
      previous = null;
      super.close();
    }
  }

  public static class FailingRows extends NonNullRows {
    public FailingRows(String bridge) {
      super(bridge, false);
    }

    @Override
    public void eval(Object scalar, Object value) {
      throw new IllegalStateException("primitive temporal sentinel");
    }
  }

  private static org.apache.flink.table.types.inference.TypeInference atomicType(String bridge) {
    var scalar = bridgeType(bridge);
    return org.apache.flink.table.types.inference.TypeInference.newBuilder()
        .typedArguments(
            scalar, FlinkMixedPrimitiveTableFunctionSqlHarnessTest.containerType("array"))
        .outputTypeStrategy(org.apache.flink.table.types.inference.TypeStrategies.explicit(scalar))
        .build();
  }

  public static class IntAtomicRows
      extends org.apache.flink.table.functions.TableFunction<Integer> {
    private final String bridge;

    public IntAtomicRows(String bridge) {
      this.bridge = bridge;
    }

    @Override
    public org.apache.flink.table.types.inference.TypeInference getTypeInference(
        org.apache.flink.table.catalog.DataTypeFactory factory) {
      return atomicType(bridge);
    }

    public void eval(Object scalar, Object value) {
      if (value != null) collect((Integer) scalar);
    }
  }

  public static class LongAtomicRows extends org.apache.flink.table.functions.TableFunction<Long> {
    private final String bridge;

    public LongAtomicRows(String bridge) {
      this.bridge = bridge;
    }

    @Override
    public org.apache.flink.table.types.inference.TypeInference getTypeInference(
        org.apache.flink.table.catalog.DataTypeFactory factory) {
      return atomicType(bridge);
    }

    public void eval(Object scalar, Object value) {
      if (value != null) collect((Long) scalar);
    }
  }
}
