package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;
import static tech.streamfusion.FlinkPrimitiveResultTableFunctionSqlHarnessTest.*;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.runtime.typeutils.ExternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.inference.TypeStrategies;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

class FlinkDateIntResultTableFunctionSqlHarnessTest {
  static Stream<String> containers() {
    return FlinkDateIntArgumentTableFunctionSqlHarnessTest.containers();
  }

  @ParameterizedTest
  @MethodSource("containers")
  void rowResultsKeepEpochDaysMutableFanoutAndLeftPadding(String container) {
    for (boolean left : List.of(false, true)) {
      try (var metrics = new RecoveryExecutionMetrics()) {
        Supplier<TableEnvironment> factory = () -> environment(container, 96, metrics, false);
        String sql = "SELECT s.x,s.p,t.v,t.epoch_day,t.pos FROM src s" + join(false, left);
        boolean observesDate = !(left && flink118());
        if (!observesDate) {
          assertReleasedLeftNullability(factory, sql);
          sql = sql.replace("t.v,t.epoch_day,t.pos", "t.v,t.pos");
        }
        var runs = NativeFailureParity.run(factory, sql);
        assertValues(runs, metrics);
        if (observesDate) assertDays(runs.host().rows(), false);
      }
    }
  }

  @ParameterizedTest
  @MethodSource("containers")
  void atomicResultsKeepEpochDaysAndLeftPadding(String container) {
    for (boolean left : List.of(false, true)) {
      try (var metrics = new RecoveryExecutionMetrics()) {
        var runs =
            NativeFailureParity.run(
                () -> environment(container, 96, metrics, true),
                "SELECT s.x,s.p,t.epoch_day FROM src s" + join(true, left));
        assertValues(runs, metrics);
        assertDays(runs.host().rows(), true);
      }
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"array", "list"})
  void emittedNullRowFieldsKeepReleasedProjectionDefaults(String container) {
    for (boolean left : List.of(false, true)) {
      try (var metrics = new RecoveryExecutionMetrics()) {
        Supplier<TableEnvironment> factory =
            () -> {
              var table = source(container, 32, metrics);
              table.createTemporarySystemFunction("result_rows", new DateRows(container, true));
              return table;
            };
        String sql = "SELECT s.x,t.v,t.epoch_day,t.pos FROM src s" + join(false, left);
        boolean observesDate = !(left && flink118());
        if (!observesDate) {
          assertReleasedLeftNullability(factory, sql);
          sql = sql.replace("t.v,t.epoch_day,t.pos", "t.v,t.pos");
        }
        var runs = NativeFailureParity.run(factory, sql);
        assertValues(runs, metrics);
        assertEquals(32, runs.host().rows().size());
        if (observesDate)
          for (var row : runs.host().rows())
            assertEquals(left ? null : LocalDate.ofEpochDay(-1), row.get(row.size() - 2));
      }
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"array", "list"})
  void atomicNullEmissionsKeepReleasedCollectorDefaults(String container) {
    for (boolean left : List.of(false, true)) {
      try (var metrics = new RecoveryExecutionMetrics()) {
        var runs =
            NativeFailureParity.run(
                () -> {
                  var table = source(container, 32, metrics);
                  table.createTemporarySystemFunction(
                      "atomic_rows", new AtomicDateRows(container, true));
                  return table;
                },
                "SELECT s.x,s.p,t.epoch_day FROM src s" + join(true, left));
        assertValues(runs, metrics);
        assertEquals(32, runs.host().rows().size());
        for (var row : runs.host().rows())
          assertEquals(LocalDate.ofEpochDay(-1), row.get(row.size() - 1));
      }
    }
  }

  @Test
  void nullWithoutProjectionKeepsReleasedSinkEnforcement() {
    try (var metrics = new RecoveryExecutionMetrics()) {
      Supplier<TableEnvironment> factory =
          () -> {
            var table = source("array", 16, metrics);
            table.createTemporarySystemFunction("result_rows", new DateRows("array", true));
            return table;
          };
      NativeFailureParity.run(
              factory, "SELECT s.x,s.p,t.v,t.epoch_day,t.pos FROM src s" + join(false, false))
          .assertFailure(
              RuntimeException.class,
              "Column 'epoch_day' is NOT NULL",
              NativeFailureParity.Phase.ROW_EVALUATION,
              NativeFailureParity.Route.NATIVE);
    }
  }

  @Test
  void reorderedResultFieldsKeepReleasedDateDefaults() {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var runs =
          NativeFailureParity.run(
              () -> {
                var table = source("array", 32, metrics);
                table.createTemporarySystemFunction("result_rows", new DateRows("array", true));
                return table;
              },
              "SELECT t.epoch_day,t.v,s.x FROM src s" + join(false, false));
      assertValues(runs, metrics);
      for (var row : runs.host().rows()) assertEquals(LocalDate.ofEpochDay(-1), row.get(1));
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"boxed", "array", "row"})
  void boxedAndNestedEpochDayResultsRetainFallback(String shape) {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var runs =
          NativeFailureParity.run(
              () -> {
                var table = source("array", 32, metrics);
                table.createTemporarySystemFunction("result_rows", new OtherDateRows(shape));
                return table;
              },
              "SELECT s.x,s.p,t.v,t.epoch_day,t.pos FROM src s" + join(false, false));
      runs.assertSuccess(NativeFailureParity.Route.FALLBACK);
      assertTrue(
          runs.nativeRun().fallbackReasons().stream()
              .anyMatch(reason -> reason.contains("verified external Java conversions")),
          runs::toString);
      assertTrue(metrics.nativeOperators().isEmpty());
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"array", "list"})
  void retainedArgumentsStayOwnedAfterInputBatchesClose(String container) {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var runs =
          NativeFailureParity.run(
              () -> {
                var table = source(container, 193, metrics);
                table.createTemporarySystemFunction("result_rows", new PreviousDateRows(container));
                return table;
              },
              "SELECT s.x,t.v,t.epoch_day,t.pos FROM src s" + join(false, false));
      assertValues(runs, metrics);
      assertDays(runs.host().rows(), false);
    }
  }

  @Test
  void emptyInputAndUserFailuresKeepReleasedBehavior() {
    for (int count : new int[] {0, 32}) {
      try (var metrics = new RecoveryExecutionMetrics()) {
        var runs =
            NativeFailureParity.run(
                () -> {
                  var table = source("array", count, metrics);
                  table.createTemporarySystemFunction("result_rows", new FailingDateRows());
                  return table;
                },
                "SELECT s.x,t.v,t.epoch_day,t.pos FROM src s" + join(false, false));
        if (count == 0) runs.assertSuccess(NativeFailureParity.Route.NATIVE);
        else
          runs.assertFailure(
              IllegalStateException.class,
              "date result sentinel",
              NativeFailureParity.Phase.ROW_EVALUATION,
              NativeFailureParity.Route.NATIVE);
      }
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"array", "list"})
  void changelogResultsSurviveRepeatedRecovery(String container) throws Exception {
    for (boolean atomic : List.of(false, true)) {
      var rows = new ArrayList<Row>();
      for (int i = 0; i < 48; i++)
        rows.add(FlinkDateIntArgumentTableFunctionSqlHarnessTest.input(container, i));
      for (int i = 0; i < 16; i++)
        for (var kind : List.of(RowKind.UPDATE_BEFORE, RowKind.UPDATE_AFTER, RowKind.DELETE)) {
          var row =
              FlinkDateIntArgumentTableFunctionSqlHarnessTest.input(
                  container, kind == RowKind.UPDATE_BEFORE ? i : i + 2);
          row.setKind(kind);
          rows.add(row);
        }
      var type =
          FlinkDateIntArgumentTableFunctionSqlHarnessTest.rowType(
              FlinkMixedPrimitiveTableFunctionSqlHarnessTest.sourceContainer(container), false);
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
                  () -> register(recovery.uninterrupted(), container, atomic),
                  () -> register(recovery.get(), container, atomic),
                  "SELECT s.x,"
                      + (atomic ? "t.epoch_day" : "t.v,t.epoch_day,t.pos")
                      + " FROM recovery_input s"
                      + join(atomic, atomic));
          runs.assertSuccess(NativeFailureParity.Route.NATIVE);
          assertDays(runs.host().rows(), atomic);
          recovery.verifyRepeatedRecovery();
          RecoveryExecutionMetrics.assertConsumed(
              recovery.nativeExecution(), "NativeTableFunction");
        }
      }
    }
  }

  private static void assertDays(List<List<Object>> rows, boolean atomic) {
    assertFalse(rows.isEmpty());
    for (var row : rows) {
      LocalDate expected = (LocalDate) row.get(1);
      Object actual = row.get(row.size() - (atomic ? 1 : 2));
      if (actual == null) continue;
      int pos = atomic ? 0 : (Integer) row.get(row.size() - 1);
      assertEquals(
          pos == 0 ? expected : LocalDate.ofEpochDay(adjacent((int) expected.toEpochDay())),
          actual);
    }
  }

  private static int adjacent(int day) {
    return day == 2932896 ? day - 1 : day + 1;
  }

  private static String join(boolean atomic, boolean left) {
    String call =
        "LATERAL TABLE("
            + (atomic ? "atomic_rows" : "result_rows")
            + "(s.x,s.p)) AS t("
            + (atomic ? "epoch_day" : "v,epoch_day,pos")
            + ")";
    return left ? " LEFT JOIN " + call + " ON TRUE" : "," + call;
  }

  static TableEnvironment source(String container, int count, RecoveryExecutionMetrics metrics) {
    return FlinkDateIntArgumentTableFunctionSqlHarnessTest.sourceEnvironment(
        container, count, metrics, false);
  }

  static TableEnvironment environment(
      String container, int count, RecoveryExecutionMetrics metrics, boolean atomic) {
    return register(source(container, count, metrics), container, atomic);
  }

  static TableEnvironment register(TableEnvironment table, String container, boolean atomic) {
    table.createTemporarySystemFunction(
        atomic ? "atomic_rows" : "result_rows",
        atomic ? new AtomicDateRows(container, false) : new DateRows(container, false));
    return table;
  }

  public static class DateRows extends FlinkDateIntArgumentTableFunctionSqlHarnessTest.IntDateRows {
    private final boolean nullDate;
    private transient Row result;

    public DateRows(String container, boolean nullDate) {
      super(container);
      this.nullDate = nullDate;
    }

    @Override
    DataType outputType() {
      return DataTypes.ROW(
          DataTypes.FIELD(
              "v", FlinkMixedPrimitiveTableFunctionSqlHarnessTest.containerType(container)),
          DataTypes.FIELD("epoch_day", DataTypes.DATE().notNull().bridgedTo(int.class)),
          DataTypes.FIELD("pos", DataTypes.INT()));
    }

    @Override
    public void open(FunctionContext context) {
      super.open(context);
      result = new Row(3);
    }

    @Override
    public void eval(int day, Object value) {
      if (result == null) throw new IllegalStateException("date result function not open");
      if (!nullDate && value == null) return;
      result.setField(0, value);
      result.setField(1, nullDate ? null : day);
      result.setField(2, 0);
      collect(result);
      if (!nullDate) {
        result.setField(0, null);
        result.setField(1, adjacent(day));
        result.setField(2, 1);
        collect(result);
      }
      result.setField(0, null);
      result.setField(1, 42);
      result.setField(2, -1);
    }

    @Override
    public void close() {
      super.close();
      result = null;
    }
  }

  public static class PreviousDateRows extends DateRows {
    private transient Object previous;

    public PreviousDateRows(String container) {
      super(container, false);
    }

    @Override
    public void eval(int day, Object value) {
      super.eval(day, previous);
      previous = value;
    }
  }

  public static class OtherDateRows extends DateRows {
    private final String shape;

    public OtherDateRows(String shape) {
      super("array", false);
      this.shape = shape;
    }

    @Override
    DataType outputType() {
      DataType day = DataTypes.DATE().notNull().bridgedTo(int.class);
      if (shape.equals("boxed")) day = day.bridgedTo(Integer.class);
      else if (shape.equals("array")) day = DataTypes.ARRAY(day).bridgedTo(int[].class);
      else day = DataTypes.ROW(DataTypes.FIELD("d", day));
      return DataTypes.ROW(
          DataTypes.FIELD(
              "v", FlinkMixedPrimitiveTableFunctionSqlHarnessTest.containerType("array")),
          DataTypes.FIELD("epoch_day", day),
          DataTypes.FIELD("pos", DataTypes.INT()));
    }

    @Override
    public void eval(int day, Object value) {
      if (value != null)
        collect(
            Row.of(
                value,
                shape.equals("boxed") ? day : shape.equals("array") ? new int[] {day} : Row.of(day),
                0));
    }
  }

  public static class FailingDateRows extends DateRows {
    public FailingDateRows() {
      super("array", false);
    }

    @Override
    public void eval(int day, Object value) {
      if (day == 0) throw new IllegalStateException("date result sentinel");
      super.eval(day, value);
    }
  }

  public static class AtomicDateRows extends TableFunction<Object> {
    private final String container;
    private final boolean nullDate;
    private transient boolean opened;

    public AtomicDateRows(String container, boolean nullDate) {
      this.container = container;
      this.nullDate = nullDate;
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory factory) {
      return TypeInference.newBuilder()
          .typedArguments(
              DataTypes.DATE().notNull().bridgedTo(int.class),
              FlinkMixedPrimitiveTableFunctionSqlHarnessTest.containerType(container))
          .outputTypeStrategy(
              TypeStrategies.explicit(DataTypes.DATE().notNull().bridgedTo(int.class)))
          .build();
    }

    public void eval(int day, Object value) {
      if (!opened) throw new IllegalStateException("atomic date function not open");
      if (nullDate) collect(null);
      else if (value != null) collect(day);
    }

    @Override
    public void open(FunctionContext context) {
      if (opened) throw new IllegalStateException("atomic date function opened twice");
      opened = true;
    }

    @Override
    public void close() {
      if (!opened) throw new IllegalStateException("atomic date function closed twice");
      opened = false;
    }
  }
}
