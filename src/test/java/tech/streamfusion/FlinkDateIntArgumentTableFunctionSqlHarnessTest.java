package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
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

class FlinkDateIntArgumentTableFunctionSqlHarnessTest {
  static final int[] DAYS = {-719162, -1, 0, 1, 2932896, 20000, -20000};

  static Stream<String> containers() {
    return Stream.of(System.getProperty("date.cases", "array;primitive;list;map;row").split(";"));
  }

  @ParameterizedTest
  @MethodSource("containers")
  void primitiveDateArgumentsRetainEpochDaysNestedValuesAndLeftPadding(String container) {
    for (boolean left : List.of(false, true)) {
      try (var metrics = new RecoveryExecutionMetrics()) {
        var runs =
            NativeFailureParity.run(
                () -> environment(container, 96, metrics, false),
                "SELECT s.x,s.p,t.v,t.epoch_day,t.pos FROM src s" + join("day_rows", left));
        runs.assertSuccess(NativeFailureParity.Route.NATIVE);
        assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
        assertStamps(runs.host().rows());
        RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeTableFunction");
      }
    }
  }

  @Test
  void defaultLocalDateControlRetainsItsExistingNativeRoute() {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var runs =
          NativeFailureParity.run(
              () -> environment("array", 96, metrics, true),
              "SELECT s.x,s.p,t.v,t.epoch_day,t.pos FROM src s" + join("day_rows", false));
      runs.assertSuccess(NativeFailureParity.Route.NATIVE);
      assertStamps(runs.host().rows());
      RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeTableFunction");
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"array", "list"})
  void dateLiteralsKeepReleasedArgumentBinding(String container) {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var runs =
          NativeFailureParity.run(
              () -> environment(container, 32, metrics, false),
              "SELECT s.x,t.v,t.epoch_day,t.pos FROM src s,LATERAL TABLE(day_rows(DATE"
                  + " '1970-01-01',s.p)) AS t(v,epoch_day,pos)");
      runs.assertSuccess(NativeFailureParity.Route.NATIVE);
      for (var row : runs.host().rows()) {
        int pos = (Integer) row.get(row.size() - 1);
        assertEquals(pos == 0 ? 0 : Integer.MIN_VALUE, row.get(row.size() - 2));
      }
      RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeTableFunction");
    }
  }

  @Test
  void nullableDateSourcesKeepReleasedPlanningValidation() {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var runs =
          NativeFailureParity.run(
              () -> {
                var table = sourceEnvironment("array", 16, metrics, true);
                table.createTemporarySystemFunction("day_rows", new IntDateRows("array"));
                return table;
              },
              "SELECT s.x,s.p,t.v,t.epoch_day,t.pos FROM src s" + join("day_rows", false));
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
  @ValueSource(strings = {"array", "list"})
  void retainedArgumentsStayOwnedAcrossClosedInputBatches(String container) {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var runs =
          NativeFailureParity.run(
              () -> {
                var table = sourceEnvironment(container, 193, metrics, false);
                table.createTemporarySystemFunction("previous_rows", new PreviousRows(container));
                return table;
              },
              "SELECT s.x,t.v,t.epoch_day,t.pos FROM src s" + join("previous_rows", false));
      runs.assertSuccess(NativeFailureParity.Route.NATIVE);
      assertStamps(runs.host().rows());
      RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeTableFunction");
    }
  }

  @Test
  void emptyInputAndFailuresKeepReleasedEvaluationOrder() {
    try (var metrics = new RecoveryExecutionMetrics()) {
      for (int count : new int[] {0, 32}) {
        var runs =
            NativeFailureParity.run(
                () -> {
                  var table = sourceEnvironment("array", count, metrics, false);
                  table.createTemporarySystemFunction("failing_rows", new FailingRows());
                  return table;
                },
                "SELECT s.x,t.v,t.epoch_day,t.pos FROM src s" + join("failing_rows", false));
        if (count == 0) runs.assertSuccess(NativeFailureParity.Route.NATIVE);
        else
          runs.assertFailure(
              IllegalStateException.class,
              "date int sentinel",
              NativeFailureParity.Phase.ROW_EVALUATION,
              NativeFailureParity.Route.NATIVE);
      }
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"array", "list", "map", "row"})
  void changelogEpochDaysSurviveRepeatedRecovery(String container) throws Exception {
    var rows = new ArrayList<Row>();
    for (int i = 0; i < 48; i++) rows.add(input(container, i));
    for (int i = 0; i < 16; i++)
      for (var kind : List.of(RowKind.UPDATE_BEFORE, RowKind.UPDATE_AFTER, RowKind.DELETE)) {
        int index = kind == RowKind.UPDATE_BEFORE ? i : i + 2;
        var row = input(container, index);
        row.setKind(kind);
        rows.add(row);
      }
    var type =
        rowType(FlinkMixedPrimitiveTableFunctionSqlHarnessTest.sourceContainer(container), false);
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
                "SELECT s.x,t.v,t.epoch_day,t.pos FROM recovery_input s" + join("day_rows", true));
        runs.assertSuccess(NativeFailureParity.Route.NATIVE);
        assertStamps(runs.host().rows());
        recovery.verifyRepeatedRecovery();
        RecoveryExecutionMetrics.assertConsumed(recovery.nativeExecution(), "NativeTableFunction");
      }
    }
  }

  @Test
  void boxedIntegerDateArgumentsRemainOutsideThePrimitiveArgumentGate() {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var runs =
          NativeFailureParity.run(
              () -> {
                var table = sourceEnvironment("array", 32, metrics, false);
                table.createTemporarySystemFunction("day_rows", new BoxedDateRows());
                return table;
              },
              "SELECT s.x,s.p,t.v,t.epoch_day,t.pos FROM src s" + join("day_rows", false));
      runs.assertSuccess(NativeFailureParity.Route.FALLBACK);
      assertUnverifiedConversion(runs);
    }
  }

  @Test
  void primitiveDateResultFieldsRetainFallback() {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var runs =
          NativeFailureParity.run(
              () -> {
                var table = sourceEnvironment("array", 32, metrics, false);
                table.createTemporarySystemFunction("day_rows", new DateResultRows());
                return table;
              },
              "SELECT s.x,s.p,t.v,t.epoch_day,t.pos FROM src s" + join("day_rows", false));
      runs.assertSuccess(NativeFailureParity.Route.FALLBACK);
      assertUnverifiedConversion(runs);
    }
  }

  @Test
  void atomicPrimitiveDateResultsRetainFallback() {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var runs =
          NativeFailureParity.run(
              () -> {
                var table = sourceEnvironment("array", 32, metrics, false);
                table.createTemporarySystemFunction("atomic_rows", new AtomicDateRows());
                return table;
              },
              "SELECT s.x,s.p,t.epoch_day FROM src s,LATERAL TABLE(atomic_rows(s.x,s.p)) AS"
                  + " t(epoch_day)");
      runs.assertSuccess(NativeFailureParity.Route.FALLBACK);
      assertUnverifiedConversion(runs);
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"array", "row"})
  void dateIntegerBridgesInsideContainersRetainFallback(String shape) {
    DataType scalar = DataTypes.DATE().notNull();
    DataType sourceType =
        shape.equals("array")
            ? DataTypes.ARRAY(scalar)
            : DataTypes.ROW(DataTypes.FIELD("d", scalar));
    DataType expectedType =
        shape.equals("array")
            ? DataTypes.ARRAY(scalar.bridgedTo(int.class)).bridgedTo(int[].class)
            : DataTypes.ROW(DataTypes.FIELD("d", scalar.bridgedTo(int.class)));
    Object[] values = new Object[64];
    for (int i = 0; i < values.length; i++)
      values[i] =
          i % 13 == 0
              ? null
              : shape.equals("array") ? new LocalDate[] {date(i), date(i + 1)} : Row.of(date(i));
    try (var metrics = new RecoveryExecutionMetrics()) {
      var runs =
          NativeFailureParity.run(
              () -> {
                var table = sourceEnvironment(sourceType, values, 32, metrics, false);
                table.createTemporarySystemFunction(
                    "nested_days", new NestedDateRows(expectedType));
                return table;
              },
              "SELECT s.x,s.p,t.v FROM src s,LATERAL TABLE(nested_days(s.x,s.p)) AS t(v)");
      runs.assertSuccess(NativeFailureParity.Route.FALLBACK);
      assertUnverifiedConversion(runs);
    }
  }

  private static void assertUnverifiedConversion(NativeFailureParity.Comparison runs) {
    assertTrue(
        runs.nativeRun().fallbackReasons().stream()
            .anyMatch(reason -> reason.contains("verified external Java conversions")),
        runs::toString);
  }

  private static String join(String function, boolean left) {
    String call = "LATERAL TABLE(" + function + "(s.x,s.p)) AS t(v,epoch_day,pos)";
    return left ? " LEFT JOIN " + call + " ON TRUE" : "," + call;
  }

  static void assertStamps(List<List<Object>> rows) {
    for (var row : rows) {
      int posIndex = row.size() - 1, dayIndex = posIndex - 1, vIndex = dayIndex - 1;
      if (row.get(posIndex) == null) {
        assertNull(row.get(dayIndex));
        assertNull(row.get(vIndex));
        continue;
      }
      int pos = (Integer) row.get(posIndex);
      assertTrue(pos == 0 || pos == 1);
      int day = Math.toIntExact(((LocalDate) row.get(1)).toEpochDay());
      assertEquals(pos == 0 ? day : day ^ Integer.MIN_VALUE, row.get(dayIndex));
    }
  }

  static LocalDate date(int index) {
    return LocalDate.ofEpochDay(DAYS[index % DAYS.length]);
  }

  static Row input(String container, int index) {
    return Row.of(
        date(index),
        FlinkMixedPrimitiveTableFunctionSqlHarnessTest.containerValue(container, index));
  }

  static DataType rowType(DataType container, boolean nullableDate) {
    return DataTypes.ROW(
        DataTypes.FIELD("x", nullableDate ? DataTypes.DATE() : DataTypes.DATE().notNull()),
        DataTypes.FIELD("p", container));
  }

  static TableEnvironment sourceEnvironment(
      String container, int count, RecoveryExecutionMetrics metrics, boolean nullableDate) {
    Object[] values =
        java.util.stream.IntStream.range(0, 64)
            .mapToObj(
                i -> FlinkMixedPrimitiveTableFunctionSqlHarnessTest.containerValue(container, i))
            .toArray();
    return sourceEnvironment(
        FlinkMixedPrimitiveTableFunctionSqlHarnessTest.sourceContainer(container),
        values,
        count,
        metrics,
        nullableDate);
  }

  private static TableEnvironment sourceEnvironment(
      DataType container,
      Object[] values,
      int count,
      RecoveryExecutionMetrics metrics,
      boolean nullableDate) {
    var env = metrics.environment(new Configuration(), 1, 0);
    env.getConfig().enableObjectReuse();
    var table = StreamTableEnvironment.create(env);
    table.getConfig().set("streamfusion.transpose.batchRows", "7");
    var type = rowType(container, nullableDate);
    var input =
        env.fromSequence(0, Math.max(0, count - 1))
            .filter(i -> i < count)
            .map(
                i ->
                    Row.of(
                        nullableDate && i % 7 == 0 ? null : date((int) (i % DAYS.length)),
                        values[(int) (i % values.length)]))
            .returns(ExternalTypeInfo.<Row>of(type));
    table.createTemporaryView(
        "src", table.fromDataStream(input, Schema.newBuilder().fromRowDataType(type).build()));
    return table;
  }

  static TableEnvironment environment(
      String container, int count, RecoveryExecutionMetrics metrics, boolean control) {
    return register(sourceEnvironment(container, count, metrics, false), container, control);
  }

  static TableEnvironment register(TableEnvironment table, String container, boolean control) {
    table.createTemporarySystemFunction(
        "day_rows", control ? new LocalDateRows(container) : new IntDateRows(container));
    return table;
  }

  public abstract static class BaseRows extends TableFunction<Row> {
    final String container;
    private final Class<?> argumentClass;
    private transient Row result;

    BaseRows(String container, Class<?> argumentClass) {
      this.container = container;
      this.argumentClass = argumentClass;
    }

    DataType outputType() {
      return DataTypes.ROW(
          DataTypes.FIELD(
              "v", FlinkMixedPrimitiveTableFunctionSqlHarnessTest.containerType(container)),
          DataTypes.FIELD("epoch_day", DataTypes.INT()),
          DataTypes.FIELD("pos", DataTypes.INT()));
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory factory) {
      return TypeInference.newBuilder()
          .typedArguments(
              DataTypes.DATE().notNull().bridgedTo(argumentClass),
              FlinkMixedPrimitiveTableFunctionSqlHarnessTest.containerType(container))
          .outputTypeStrategy(TypeStrategies.explicit(outputType()))
          .build();
    }

    @Override
    public void open(FunctionContext context) {
      if (result != null) throw new IllegalStateException("day function opened twice");
      result = new Row(3);
    }

    void emit(int day, Object value) {
      if (result == null) throw new IllegalStateException("day function not open");
      if (value == null) return;
      result.setField(0, value);
      result.setField(1, day);
      result.setField(2, 0);
      collect(result);
      result.setField(0, null);
      result.setField(1, day ^ Integer.MIN_VALUE);
      result.setField(2, 1);
      collect(result);
      result.setField(1, 42);
      result.setField(2, -1);
    }

    @Override
    public void close() {
      if (result == null) throw new IllegalStateException("day function closed twice");
      result = null;
    }
  }

  public static class IntDateRows extends BaseRows {
    public IntDateRows(String container) {
      super(container, int.class);
    }

    public void eval(int day, Object value) {
      emit(day, value);
    }
  }

  public static class LocalDateRows extends BaseRows {
    public LocalDateRows(String container) {
      super(container, LocalDate.class);
    }

    public void eval(LocalDate day, Object value) {
      emit(Math.toIntExact(day.toEpochDay()), value);
    }
  }

  public static class BoxedDateRows extends BaseRows {
    public BoxedDateRows() {
      super("array", Integer.class);
    }

    public void eval(Integer day, Object value) {
      emit(day, value);
    }
  }

  public static class PreviousRows extends IntDateRows {
    private transient Object previous;

    public PreviousRows(String container) {
      super(container);
    }

    @Override
    public void eval(int day, Object value) {
      super.eval(day, previous);
      previous = value;
    }
  }

  public static class FailingRows extends IntDateRows {
    public FailingRows() {
      super("array");
    }

    @Override
    public void eval(int day, Object value) {
      if (day == 0) throw new IllegalStateException("date int sentinel");
      super.eval(day, value);
    }
  }

  public static class DateResultRows extends IntDateRows {
    public DateResultRows() {
      super("array");
    }

    @Override
    public void eval(int day, Object value) {
      if (value != null) collect(Row.of(value, day, 0));
    }

    @Override
    DataType outputType() {
      return DataTypes.ROW(
          DataTypes.FIELD(
              "v", FlinkMixedPrimitiveTableFunctionSqlHarnessTest.containerType("array")),
          DataTypes.FIELD("epoch_day", DataTypes.DATE().notNull().bridgedTo(int.class)),
          DataTypes.FIELD("pos", DataTypes.INT()));
    }
  }

  public static class AtomicDateRows extends TableFunction<Object> {
    @Override
    public TypeInference getTypeInference(DataTypeFactory factory) {
      return TypeInference.newBuilder()
          .typedArguments(
              DataTypes.DATE().notNull().bridgedTo(int.class),
              FlinkMixedPrimitiveTableFunctionSqlHarnessTest.containerType("array"))
          .outputTypeStrategy(
              TypeStrategies.explicit(DataTypes.DATE().notNull().bridgedTo(int.class)))
          .build();
    }

    public void eval(int day, Object value) {
      if (value != null) collect(day);
    }
  }

  public static class NestedDateRows extends TableFunction<Row> {
    private final DataType container;

    public NestedDateRows(DataType container) {
      this.container = container;
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory factory) {
      return TypeInference.newBuilder()
          .typedArguments(DataTypes.DATE().notNull().bridgedTo(int.class), container)
          .outputTypeStrategy(
              TypeStrategies.explicit(DataTypes.ROW(DataTypes.FIELD("v", DataTypes.INT()))))
          .build();
    }

    public void eval(int day, Object value) {
      if (value == null) return;
      int sum = day;
      if (value instanceof int[]) for (int other : (int[]) value) sum += other;
      else if (value instanceof Row) sum += (Integer) ((Row) value).getField(0);
      else throw new IllegalStateException("unexpected external date container");
      collect(Row.of(sum));
    }
  }
}
