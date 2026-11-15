package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.inference.TypeStrategies;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class FlinkNestedBridgeLifecycleSqlHarnessTest {
  static Stream<Arguments> samples() {
    return Stream.of(
        Arguments.of("row", "primitive-FLOAT"),
        Arguments.of("row_map", "date-sql"),
        Arguments.of("map_key", "ltz-long"),
        Arguments.of("map_row", "timestamp-sql"),
        Arguments.of("map_key", "date-sql"));
  }

  @ParameterizedTest
  @MethodSource("samples")
  void retainedArgumentsRemainOwnedAfterLaterBatches(String shape, String bridge) {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var runs =
          NativeFailureParity.run(
              () -> environment(shape, bridge, 193, metrics, () -> new PreviousRows(shape, bridge)),
              FlinkNestedBridgeTableFunctionSqlHarnessTest.query(true));
      assertValues(runs, metrics);
    }
  }

  @ParameterizedTest
  @MethodSource("samples")
  void chainedCallsConsumeBothNativeOperators(String shape, String bridge) {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var runs =
          NativeFailureParity.run(
              () -> environment(shape, bridge, 96, metrics, () -> new ChainRows(shape, bridge)),
              "SELECT s.id,t.v,u.v FROM src s,LATERAL TABLE(nested_bridges(s.id,s.p)) AS t(v,pos),"
                  + "LATERAL TABLE(nested_bridges(t.pos,t.v)) AS u(v,pos)");
      assertValues(runs, metrics);
      long consumedOperators =
          metrics.nativeOperators().stream()
              .filter(
                  row -> row.get("operator").toString().startsWith("NativeTableFunctionExecNode"))
              .filter(row -> ((Number) row.get("recordsIn")).longValue() > 0)
              .map(row -> row.get("operator").toString())
              .distinct()
              .count();
      assertTrue(consumedOperators >= 2, () -> metrics.nativeOperators().toString());
    }
  }

  @ParameterizedTest
  @MethodSource("samples")
  void callbackFailuresPreserveCallOrderAndReleasedPhase(String shape, String bridge) {
    for (boolean afterCollect : List.of(false, true)) {
      try (var metrics = new RecoveryExecutionMetrics()) {
        var runs =
            NativeFailureParity.run(
                () ->
                    environment(
                        shape,
                        bridge,
                        8,
                        metrics,
                        () -> new FailingRows(shape, bridge, afterCollect)),
                FlinkNestedBridgeTableFunctionSqlHarnessTest.query(false));
        runs.assertFailure(
            IllegalStateException.class,
            "nested-bridge-failure-calls=4",
            NativeFailureParity.Phase.ROW_EVALUATION,
            NativeFailureParity.Route.NATIVE);
      }
    }
  }

  @Test
  void openFailureIsPreservedForEmptyAndPopulatedInput() {
    for (int rows : List.of(0, 4)) {
      try (var metrics = new RecoveryExecutionMetrics()) {
        var runs =
            NativeFailureParity.run(
                () -> environment("row_map", "date-sql", rows, metrics, OpenFailureRows::new),
                FlinkNestedBridgeTableFunctionSqlHarnessTest.query(false));
        runs.assertFailure(
            IllegalStateException.class,
            "nested-bridge-open-failure",
            NativeFailureParity.Phase.INITIALIZATION,
            NativeFailureParity.Route.NATIVE);
      }
    }
  }

  @Test
  void emptyInputNeverInvokesTheCallback() {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var runs =
          NativeFailureParity.run(
              () -> environment("row_map", "date-sql", 0, metrics, EmptyRows::new),
              FlinkNestedBridgeTableFunctionSqlHarnessTest.query(false));
      runs.assertSuccess(NativeFailureParity.Route.NATIVE);
      assertTrue(runs.host().rows().isEmpty());
      assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
    }
  }

  private static TableEnvironment environment(
      String shape,
      String bridge,
      int rows,
      RecoveryExecutionMetrics metrics,
      Supplier<FlinkNestedBridgeTableFunctionSqlHarnessTest.NestedBridges> factory) {
    var table =
        FlinkNestedBridgeTableFunctionSqlHarnessTest.environment(
            shape, bridge, "both", rows, metrics);
    assertTrue(table.dropTemporarySystemFunction("nested_bridges"));
    table.createTemporarySystemFunction("nested_bridges", factory.get());
    return table;
  }

  private static void assertValues(
      NativeFailureParity.Comparison runs, RecoveryExecutionMetrics metrics) {
    runs.assertSuccess(NativeFailureParity.Route.NATIVE);
    assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
    assertEquals(
        NativeParity.multiset(
            FlinkNestedBridgeTableFunctionSqlHarnessTest.rawRows(runs.host().rows())),
        NativeParity.multiset(
            FlinkNestedBridgeTableFunctionSqlHarnessTest.rawRows(runs.nativeRun().rows())));
    RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeTableFunction");
  }

  public static class ChainRows extends FlinkNestedBridgeTableFunctionSqlHarnessTest.NestedBridges {
    private final String shape;
    private final String bridge;

    public ChainRows(String shape, String bridge) {
      super(shape, bridge, "both");
      this.shape = shape;
      this.bridge = bridge;
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory factory) {
      var type =
          FlinkNestedBridgeTableFunctionSqlHarnessTest.container(
              shape, FlinkNestedBridgeTableFunctionSqlHarnessTest.bridgeLeaf(bridge));
      return TypeInference.newBuilder()
          .typedArguments(DataTypes.INT().notNull(), type)
          .outputTypeStrategy(
              TypeStrategies.explicit(
                  DataTypes.ROW(
                      DataTypes.FIELD("v", type),
                      DataTypes.FIELD("pos", DataTypes.INT().notNull()))))
          .build();
    }
  }

  public static class PreviousRows
      extends FlinkNestedBridgeTableFunctionSqlHarnessTest.NestedBridges {
    private transient Object previous;

    public PreviousRows(String shape, String bridge) {
      super(shape, bridge, "both");
    }

    @Override
    public void eval(Integer id, Object payload) {
      Object value = previous;
      previous = payload;
      super.eval(id, value);
    }
  }

  public static class FailingRows
      extends FlinkNestedBridgeTableFunctionSqlHarnessTest.NestedBridges {
    private final boolean afterCollect;
    private transient int calls;

    public FailingRows(String shape, String bridge, boolean afterCollect) {
      super(shape, bridge, "both");
      this.afterCollect = afterCollect;
    }

    @Override
    public void eval(Integer id, Object payload) {
      calls++;
      if (afterCollect) super.eval(id, payload);
      if (id == 3) throw new IllegalStateException("nested-bridge-failure-calls=" + calls);
      if (!afterCollect) super.eval(id, payload);
    }
  }

  public static class OpenFailureRows
      extends FlinkNestedBridgeTableFunctionSqlHarnessTest.NestedBridges {
    public OpenFailureRows() {
      super("row_map", "date-sql", "both");
    }

    @Override
    public void open(FunctionContext context) {
      super.open(context);
      throw new IllegalStateException("nested-bridge-open-failure");
    }
  }

  public static class EmptyRows extends FlinkNestedBridgeTableFunctionSqlHarnessTest.NestedBridges {
    public EmptyRows() {
      super("row_map", "date-sql", "both");
    }

    @Override
    public void eval(Integer id, Object payload) {
      throw new IllegalStateException("callback invoked for empty input");
    }
  }
}
