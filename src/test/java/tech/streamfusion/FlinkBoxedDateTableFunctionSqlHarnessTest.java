package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.runtime.typeutils.ExternalTypeInfo;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.inference.TypeStrategies;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

class FlinkBoxedDateTableFunctionSqlHarnessTest {
  static Stream<String> containers() {
    return FlinkDateIntArgumentTableFunctionSqlHarnessTest.containers();
  }

  @ParameterizedTest
  @MethodSource("containers")
  void nullableEpochDaysKeepOwnedFanoutTypesAndLeftPadding(String container) {
    for (boolean left : List.of(false, true)) {
      try (var metrics = new RecoveryExecutionMetrics()) {
        var runs =
            NativeFailureParity.run(
                () -> environment(container, 96, metrics, false),
                "SELECT s.x,s.p,t.v,t.epoch_day,t.pos FROM src s" + join(left));
        assertValues(runs, metrics);
        assertDays(runs.host().rows());
      }
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"array", "list"})
  void atomicNullableResultsKeepNullEmissionsAndLeftPadding(String container) {
    for (boolean left : List.of(false, true)) {
      try (var metrics = new RecoveryExecutionMetrics()) {
        var runs =
            NativeFailureParity.run(
                () -> {
                  var table = environment(container, 96, metrics, false);
                  table.createTemporarySystemFunction("atomic_days", new AtomicDays(container));
                  return table;
                },
                "SELECT s.x,s.p,t.epoch_day FROM src s"
                    + (left ? " LEFT JOIN " : ",")
                    + "LATERAL TABLE(atomic_days(s.x,s.p)) AS t(epoch_day)"
                    + (left ? " ON TRUE" : ""));
        assertValues(runs, metrics);
        assertTrue(runs.host().rows().stream().anyMatch(row -> row.get(3) == null));
        for (var row : runs.host().rows())
          if (row.get(3) != null) assertEquals(row.get(1), row.get(3));
      }
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"array", "list"})
  void nullDateLiteralsReachTheCallback(String container) {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var runs =
          NativeFailureParity.run(
              () -> environment(container, 32, metrics, false),
              "SELECT t.v,t.epoch_day,t.pos FROM src s,LATERAL"
                  + " TABLE(boxed_days(CAST(NULL AS DATE),s.p)) AS t(v,epoch_day,pos)");
      assertValues(runs, metrics);
      assertFalse(runs.host().rows().isEmpty());
      for (var row : runs.host().rows()) assertNull(row.get(2));
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"array", "list"})
  void retainedContainerArgumentsSurviveClosedInputBatches(String container) {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var runs =
          NativeFailureParity.run(
              () -> {
                var table = environment(container, 193, metrics, false);
                table.createTemporarySystemFunction("boxed_days", new PreviousDays(container));
                return table;
              },
              "SELECT s.x,s.p,t.v,t.epoch_day,t.pos FROM src s" + join(false));
      assertValues(runs, metrics);
      assertDays(runs.host().rows());
    }
  }

  @Test
  void emptyInputAndUserFailuresKeepEvaluationSemantics() {
    for (int count : new int[] {0, 32}) {
      try (var metrics = new RecoveryExecutionMetrics()) {
        var runs =
            NativeFailureParity.run(
                () -> environment("array", count, metrics, true),
                "SELECT s.x,s.p,t.v,t.epoch_day,t.pos FROM src s" + join(false));
        if (count == 0) runs.assertSuccess(NativeFailureParity.Route.NATIVE);
        else
          runs.assertFailure(
              IllegalStateException.class,
              "boxed date sentinel",
              NativeFailureParity.Phase.ROW_EVALUATION,
              NativeFailureParity.Route.NATIVE);
      }
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"array", "list"})
  void nullableEpochDaysAndChangelogSurviveRepeatedRecovery(String container) throws Exception {
    var rows = new ArrayList<Row>();
    for (int i = 0; i < 48; i++) rows.add(input(container, i));
    for (int i = 0; i < 16; i++)
      for (var kind : List.of(RowKind.UPDATE_BEFORE, RowKind.UPDATE_AFTER, RowKind.DELETE)) {
        var row = input(container, kind == RowKind.UPDATE_BEFORE ? i : i + 2);
        row.setKind(kind);
        rows.add(row);
      }
    var type =
        FlinkDateIntArgumentTableFunctionSqlHarnessTest.rowType(
            FlinkMixedPrimitiveTableFunctionSqlHarnessTest.sourceContainer(container), true);
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
                () -> register(recovery.uninterrupted(), container, false),
                () -> register(recovery.get(), container, false),
                "SELECT s.x,s.p,t.v,t.epoch_day,t.pos FROM recovery_input s" + join(true));
        runs.assertSuccess(NativeFailureParity.Route.NATIVE);
        assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
        assertDays(runs.host().rows());
        recovery.verifyRepeatedRecovery();
        RecoveryExecutionMetrics.assertConsumed(recovery.nativeExecution(), "NativeTableFunction");
      }
    }
  }

  private static Row input(String container, int index) {
    var row = FlinkDateIntArgumentTableFunctionSqlHarnessTest.input(container, index);
    if (index % 7 == 0) row.setField(0, null);
    return row;
  }

  private static void assertValues(
      NativeFailureParity.Comparison runs, RecoveryExecutionMetrics metrics) {
    runs.assertSuccess(NativeFailureParity.Route.NATIVE);
    assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
    RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeTableFunction");
  }

  private static void assertDays(List<List<Object>> rows) {
    assertFalse(rows.isEmpty());
    for (var row : rows) {
      var pos = (Integer) row.get(row.size() - 1);
      Object actual = row.get(row.size() - 2);
      if (pos == null) {
        assertNull(actual);
        continue;
      }
      var day = (LocalDate) row.get(1);
      assertEquals(pos == 0 && day != null ? day : null, actual);
    }
  }

  static String join(boolean left) {
    String call = "LATERAL TABLE(boxed_days(s.x,s.p)) AS t(v,epoch_day,pos)";
    return left ? " LEFT JOIN " + call + " ON TRUE" : "," + call;
  }

  static TableEnvironment environment(
      String container, int count, RecoveryExecutionMetrics metrics, boolean failing) {
    return register(
        FlinkDateIntArgumentTableFunctionSqlHarnessTest.sourceEnvironment(
            container, count, metrics, true),
        container,
        failing);
  }

  private static TableEnvironment register(
      TableEnvironment table, String container, boolean failing) {
    table.createTemporarySystemFunction("boxed_days", new BoxedDays(container, failing));
    return table;
  }

  public static class BoxedDays extends TableFunction<Row> {
    private final String container;
    private final boolean failing;
    private transient Row result;

    public BoxedDays(String container, boolean failing) {
      this.container = container;
      this.failing = failing;
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory factory) {
      return TypeInference.newBuilder()
          .typedArguments(
              DataTypes.DATE().bridgedTo(Integer.class),
              FlinkMixedPrimitiveTableFunctionSqlHarnessTest.containerType(container))
          .outputTypeStrategy(
              TypeStrategies.explicit(
                  DataTypes.ROW(
                      DataTypes.FIELD(
                          "v",
                          FlinkMixedPrimitiveTableFunctionSqlHarnessTest.containerType(container)),
                      DataTypes.FIELD("epoch_day", DataTypes.DATE().bridgedTo(Integer.class)),
                      DataTypes.FIELD("pos", DataTypes.INT()))))
          .build();
    }

    @Override
    public void open(FunctionContext context) {
      if (result != null) throw new IllegalStateException("boxed date opened twice");
      result = new Row(3);
    }

    public void eval(Integer day, Object value) {
      if (result == null) throw new IllegalStateException("boxed date not open");
      if (failing && day != null && day == 0)
        throw new IllegalStateException("boxed date sentinel");
      if (value == null) return;
      result.setField(0, value);
      result.setField(1, day);
      result.setField(2, 0);
      collect(result);
      result.setField(0, null);
      result.setField(1, null);
      result.setField(2, 1);
      collect(result);
      result.setField(1, 42);
      result.setField(2, -1);
    }

    @Override
    public void close() {
      if (result == null) throw new IllegalStateException("boxed date closed twice");
      result = null;
    }
  }

  public static class PreviousDays extends BoxedDays {
    private transient Object previous;

    public PreviousDays(String container) {
      super(container, false);
    }

    @Override
    public void eval(Integer day, Object value) {
      super.eval(day, previous);
      previous = value;
    }
  }

  public static class AtomicDays extends TableFunction<Integer> {
    private final String container;

    public AtomicDays(String container) {
      this.container = container;
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory factory) {
      return TypeInference.newBuilder()
          .typedArguments(
              DataTypes.DATE().bridgedTo(Integer.class),
              FlinkMixedPrimitiveTableFunctionSqlHarnessTest.containerType(container))
          .outputTypeStrategy(TypeStrategies.explicit(DataTypes.DATE().bridgedTo(Integer.class)))
          .build();
    }

    public void eval(Integer day, Object value) {
      if (value != null) {
        collect(day);
        collect(null);
      }
    }
  }
}
