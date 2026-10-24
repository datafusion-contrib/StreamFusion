package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Stream;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.runtime.typeutils.ExternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class FlinkTableFunctionConversionSqlHarnessTest {
  static Stream<String> conversions() {
    String selected = System.getProperty("conversion.cases");
    if (selected != null) return Stream.of(selected.split(";"));
    return Stream.concat(
        Stream.of("BOOLEAN", "TINYINT", "SMALLINT", "INT", "BIGINT", "FLOAT", "DOUBLE")
            .map(type -> "primitive:" + type),
        Stream.of("list:", "arraylist:")
            .flatMap(bridge -> scalarElements().map(type -> bridge + type)));
  }

  private static Stream<String> scalarElements() {
    return Stream.of(
        "BOOLEAN",
        "TINYINT",
        "SMALLINT",
        "INT",
        "BIGINT",
        "FLOAT",
        "DOUBLE",
        "STRING",
        "CHAR(4)",
        "VARBINARY",
        "BINARY(4)",
        "DECIMAL(18,8)",
        "DECIMAL(38,18)",
        "DATE",
        "TIME",
        "TIMESTAMP(9)",
        "TIMESTAMP_LTZ(9)");
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("conversions")
  void externalBridgesRetainTypesNullsAndMutableFanout(String name) {
    for (String join :
        List.of(
            ",LATERAL TABLE(bridge_rows(s.id,s.p)) AS t(v,pos)",
            " LEFT JOIN LATERAL TABLE(bridge_rows(s.id,s.p)) AS t(v,pos) ON TRUE")) {
      try (var metrics = new RecoveryExecutionMetrics()) {
        var runs =
            NativeFailureParity.run(
                () -> environment(name, 96, metrics),
                "SELECT s.id,s.p,t.v,t.pos FROM src s" + join);
        runs.assertSuccess(NativeFailureParity.Route.NATIVE);
        assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
        RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeTableFunction");
      }
    }
  }

  @ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(
      strings = {
        "primitive:INT",
        "primitive:DOUBLE",
        "list:STRING",
        "list:DECIMAL(38,18)",
        "arraylist:STRING",
        "arraylist:DECIMAL(38,18)"
      })
  void retainedArgumentsStayOwnedAcrossLaterBatches(String name) {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var runs =
          NativeFailureParity.run(
              () -> {
                var table = environment(name, 193, metrics);
                table.createTemporarySystemFunction("previous_rows", new PreviousRows(name));
                return table;
              },
              "SELECT s.id,t.v,t.pos FROM src s,LATERAL TABLE(previous_rows(s.id,s.p)) AS"
                  + " t(v,pos)");
      runs.assertSuccess(NativeFailureParity.Route.NATIVE);
      RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeTableFunction");
    }
  }

  @ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(
      strings = {"primitive:INT", "list:STRING", "arraylist:STRING"})
  void filteredCallsAndFailuresKeepReleasedEvaluationOrder(String name) {
    try (var metrics = new RecoveryExecutionMetrics()) {
      java.util.function.Supplier<TableEnvironment> factory =
          () -> {
            var table = environment(name, 32, metrics);
            table.createTemporarySystemFunction("failing_rows", new FailingRows(name));
            return table;
          };
      NativeFailureParity.run(
              factory,
              "SELECT t.v FROM (SELECT * FROM src WHERE id<0) s,LATERAL"
                  + " TABLE(failing_rows(s.id,s.p)) AS t(v,pos)")
          .assertSuccess(NativeFailureParity.Route.FALLBACK);
      NativeFailureParity.run(
              () -> {
                var table = environment(name, 0, metrics);
                table.createTemporarySystemFunction("failing_rows", new FailingRows(name));
                return table;
              },
              "SELECT t.v FROM src s,LATERAL TABLE(failing_rows(s.id,s.p)) AS t(v,pos)")
          .assertSuccess(NativeFailureParity.Route.NATIVE);
      NativeFailureParity.run(
              factory, "SELECT t.v FROM src s,LATERAL TABLE(failing_rows(s.id,s.p)) AS t(v,pos)")
          .assertFailure(
              IllegalStateException.class,
              "table conversion sentinel",
              NativeFailureParity.Phase.ROW_EVALUATION,
              NativeFailureParity.Route.NATIVE);
    }
  }

  @ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(
      strings = {"primitive:INT", "list:STRING", "arraylist:STRING"})
  void concreteJavaMethodSignaturesExecuteInsideTheColumnarOperator(String name) {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var runs =
          NativeFailureParity.run(
              () -> {
                var table = environment(name, 96, metrics);
                table.createTemporarySystemFunction("concrete_rows", concreteRows(name));
                return table;
              },
              "SELECT s.id,t.v,t.pos FROM src s,LATERAL TABLE(concrete_rows(s.id,s.p)) AS"
                  + " t(v,pos)");
      runs.assertSuccess(NativeFailureParity.Route.NATIVE);
      RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeTableFunction");
    }
  }

  @ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(
      strings = {
        "primitive:INT",
        "primitive:DOUBLE",
        "list:STRING",
        "list:DECIMAL(38,18)",
        "arraylist:STRING",
        "arraylist:DECIMAL(38,18)"
      })
  void changelogValuesSurviveRepeatedRecovery(String name) throws Exception {
    var rows = new java.util.ArrayList<Row>();
    for (int i = 0; i < 48; i++) rows.add(Row.of(i, value(name, i)));
    for (int i = 0; i < 16; i++) {
      rows.add(Row.ofKind(org.apache.flink.types.RowKind.UPDATE_BEFORE, i, value(name, i)));
      rows.add(Row.ofKind(org.apache.flink.types.RowKind.UPDATE_AFTER, i, value(name, i + 2)));
      rows.add(Row.ofKind(org.apache.flink.types.RowKind.DELETE, i, value(name, i + 2)));
    }
    var type = rowType(name);
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
                () -> register(recovery.uninterrupted(), name),
                () -> register(recovery.get(), name),
                "SELECT s.id,t.v,t.pos FROM recovery_input s LEFT JOIN LATERAL"
                    + " TABLE(bridge_rows(s.id,s.p)) AS t(v,pos) ON TRUE");
        runs.assertSuccess(NativeFailureParity.Route.NATIVE);
        recovery.verifyRepeatedRecovery();
        RecoveryExecutionMetrics.assertConsumed(recovery.nativeExecution(), "NativeTableFunction");
      }
    }
  }

  @ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(
      strings = {
        "array_nested",
        "array_row",
        "array_map",
        "arraylist:array_nested",
        "arraylist:array_row",
        "arraylist:array_map"
      })
  void listElementsWithUnverifiedContainerConversionsRetainFallback(String name) {
    boolean arrayList = name.startsWith("arraylist:");
    String shape = arrayList ? name.substring("arraylist:".length()) : name;
    try (var metrics = new RecoveryExecutionMetrics()) {
      var runs =
          NativeFailureParity.run(
              () -> {
                var table = FlinkNestedTableFunctionSqlHarnessTest.environment(shape, 32, metrics);
                table.createTemporarySystemFunction(
                    "bridge_rows",
                    new FlinkTypedTableFunctionSqlHarnessTest.TypedRows(
                        FlinkNestedTableFunctionSqlHarnessTest.valueType(shape)
                            .bridgedTo(List.class),
                        FlinkNestedTableFunctionSqlHarnessTest.valueType(shape)
                            .bridgedTo(arrayList ? ArrayList.class : List.class)));
                return table;
              },
              "SELECT t.v FROM src s,LATERAL TABLE(bridge_rows(s.id,s.p)) AS t(v,pos)");
      runs.assertSuccess(NativeFailureParity.Route.FALLBACK);
      assertTrue(
          runs.nativeRun().fallbackReasons().stream()
              .anyMatch(reason -> reason.contains("verified external Java conversions")),
          runs::toString);
    }
  }

  @ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(strings = {"primitive:FLOAT", "primitive:DOUBLE"})
  void rawFloatingBitsSurviveBothArrayConversionBoundaries(String name) {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var runs =
          NativeFailureParity.run(
              () -> {
                var table = environment(name, 96, metrics);
                table.createTemporarySystemFunction("bits_rows", new BitsRows(name));
                return table;
              },
              "SELECT s.id,b.v,b.pos FROM src s,LATERAL TABLE(bridge_rows(s.id,s.p)) AS t(v,pos),"
                  + "LATERAL TABLE(bits_rows(t.pos,t.v)) AS b(v,pos)");
      runs.assertSuccess(NativeFailureParity.Route.NATIVE);
      long nan = name.endsWith("FLOAT") ? 0x7fc00001L : 0x7ff8000000000001L;
      assertTrue(runs.host().rows().stream().anyMatch(row -> row.contains(nan)), runs::toString);
      RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeTableFunction");
    }
  }

  @ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(strings = {"primitive:INT", "list:INT"})
  void bridgesInsideRowArgumentsRemainOutsideTheVerifiedPositions(String name) {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var runs =
          NativeFailureParity.run(
              () -> {
                var env = metrics.environment(new Configuration(), 1, 0);
                var table = StreamTableEnvironment.create(env);
                var type =
                    DataTypes.ROW(
                        DataTypes.FIELD("id", DataTypes.INT()),
                        DataTypes.FIELD(
                            "p", DataTypes.ROW(DataTypes.FIELD("items", sourceArray(name)))));
                var input =
                    tech.streamfusion.compat.FlinkTestSources.fromData(
                        env,
                        List.of(
                            Row.of(1, Row.of(value(name, 1))), Row.of(2, Row.of(value(name, 2)))),
                        ExternalTypeInfo.<Row>of(type));
                table.createTemporaryView(
                    "src",
                    table.fromDataStream(input, Schema.newBuilder().fromRowDataType(type).build()));
                table.createTemporarySystemFunction(
                    "bridge_rows",
                    new FlinkTypedTableFunctionSqlHarnessTest.TypedRows(
                        DataTypes.ROW(DataTypes.FIELD("items", conversion(name)))));
                return table;
              },
              "SELECT t.v FROM src s,LATERAL TABLE(bridge_rows(s.id,s.p)) AS t(v,pos)");
      runs.assertSuccess(NativeFailureParity.Route.FALLBACK);
      assertTrue(
          runs.nativeRun().fallbackReasons().stream()
              .anyMatch(reason -> reason.contains("verified external Java conversions")),
          runs::toString);
    }
  }

  @org.junit.jupiter.api.Test
  void concreteListBridgesInsideResultRowsKeepFallback() {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var runs =
          NativeFailureParity.run(
              () -> {
                var env = metrics.environment(new Configuration(), 1, 0);
                var table = StreamTableEnvironment.create(env);
                var type =
                    DataTypes.ROW(
                        DataTypes.FIELD("id", DataTypes.INT()),
                        DataTypes.FIELD(
                            "p", DataTypes.ROW(DataTypes.FIELD("items", sourceArray("list:INT")))));
                var input =
                    tech.streamfusion.compat.FlinkTestSources.fromData(
                        env,
                        List.of(
                            Row.of(1, Row.of(value("list:INT", 1))),
                            Row.of(2, Row.of(value("list:INT", 2)))),
                        ExternalTypeInfo.<Row>of(type));
                table.createTemporaryView(
                    "src",
                    table.fromDataStream(input, Schema.newBuilder().fromRowDataType(type).build()));
                table.createTemporarySystemFunction(
                    "nested_result", new NestedArrayListResultRows());
                return table;
              },
              "SELECT t.v FROM src s,LATERAL TABLE(nested_result(s.id,s.p)) AS t(v,pos)");
      runs.assertSuccess(NativeFailureParity.Route.FALLBACK);
      assertTrue(
          runs.nativeRun().fallbackReasons().stream()
              .anyMatch(reason -> reason.contains("verified external Java conversions")),
          runs::toString);
    }
  }

  @ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(
      strings = {"primitive:INT", "list:STRING", "arraylist:INT", "arraylist:STRING"})
  void atomicArrayResultsUseTheReleasedWrappingCollector(String name) {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var runs =
          NativeFailureParity.run(
              () -> {
                var table = environment(name, 96, metrics);
                table.createTemporarySystemFunction("atomic_rows", new AtomicRows(name));
                return table;
              },
              "SELECT s.id,t.v FROM src s LEFT JOIN LATERAL TABLE(atomic_rows(s.id,s.p)) AS t(v) ON"
                  + " TRUE");
      runs.assertSuccess(NativeFailureParity.Route.NATIVE);
      assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
      RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeTableFunction");
    }
  }

  @ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(strings = {"arraylist", "linkedlist"})
  void concreteListArgumentsKeepReleasedPlanningRejection(String bridge) {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var runs =
          NativeFailureParity.run(
              () -> {
                var table = environment("list:STRING", 32, metrics);
                table.createTemporarySystemFunction(
                    "invalid_rows", new InvalidListArgumentRows(bridge));
                return table;
              },
              "SELECT s.id,t.v,t.pos FROM src s,LATERAL TABLE(invalid_rows(s.id,s.p)) AS t(v,pos)");
      runs.assertFailure(
          org.apache.flink.table.api.ValidationException.class,
          "does not support an output conversion",
          NativeFailureParity.Phase.PLANNING,
          NativeFailureParity.Route.FALLBACK);
      assertEquals(runs.host().rootCause().getMessage(), runs.nativeRun().rootCause().getMessage());
      assertEquals(0, runs.nativeRun().substitutions());
      assertTrue(metrics.nativeOperators().isEmpty());
    }
  }

  @org.junit.jupiter.api.Test
  void primitiveDateArgumentsAndConcreteArrayListResultsComposeWithoutWideningNestedBridges() {
    for (boolean left : List.of(false, true)) {
      try (var metrics = new RecoveryExecutionMetrics()) {
        var runs =
            NativeFailureParity.run(
                () -> {
                  var table =
                      FlinkDateIntArgumentTableFunctionSqlHarnessTest.sourceEnvironment(
                          "list", 96, metrics, false);
                  table.createTemporarySystemFunction("date_list_rows", new DateArrayListRows());
                  return table;
                },
                "SELECT s.x,t.v,t.epoch_day,t.pos FROM src s"
                    + (left ? " LEFT JOIN " : ",")
                    + "LATERAL TABLE(date_list_rows(s.x,s.p)) AS t(v,epoch_day,pos)"
                    + (left ? " ON TRUE" : ""));
        runs.assertSuccess(NativeFailureParity.Route.NATIVE);
        assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
        FlinkDateIntArgumentTableFunctionSqlHarnessTest.assertStamps(runs.host().rows());
        RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeTableFunction");
      }
    }
  }

  @ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(
      strings = {"list", "arraylist", "wrapped_list", "wrapped_arraylist"})
  void emptyRowsRemainContainerElementsRatherThanScalarLeaves(String bridge) {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var runs =
          NativeFailureParity.run(
              () -> {
                var table = environment("list:INT", 32, metrics);
                table.createTemporarySystemFunction(
                    "empty_row_lists", new EmptyRowListResults(bridge));
                return table;
              },
              "SELECT s.id,t.v,t.pos FROM src s,LATERAL TABLE(empty_row_lists(s.id,s.p)) AS"
                  + " t(v,pos)");
      runs.assertSuccess(NativeFailureParity.Route.FALLBACK);
      assertTrue(
          runs.nativeRun().fallbackReasons().stream()
              .anyMatch(reason -> reason.contains("verified external Java conversions")),
          runs::toString);
      assertTrue(metrics.nativeOperators().isEmpty());
    }
  }

  @org.junit.jupiter.api.Test
  void requiredArrayListResultsKeepReleasedNullabilitySemantics() {
    for (boolean emitNull : List.of(false, true)) {
      try (var metrics = new RecoveryExecutionMetrics()) {
        var runs =
            NativeFailureParity.run(
                () -> {
                  var table = environment("list:INT", 32, metrics);
                  table.createTemporarySystemFunction(
                      "required_rows", new RequiredArrayListRows(emitNull));
                  return table;
                },
                "SELECT s.id,t.v,t.pos FROM src s,LATERAL TABLE(required_rows(s.id,s.p)) AS"
                    + " t(v,pos)");
        if (runs.host().failure() == null) {
          runs.assertSuccess(NativeFailureParity.Route.NATIVE);
          assertEquals(32, runs.host().rows().size());
          for (var row : runs.host().rows()) {
            if (emitNull) assertNull(row.get(2));
            else assertNotNull(row.get(2));
          }
          RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeTableFunction");
        } else {
          assertTrue(emitNull, runs::toString);
          assertTrue(
              runs.host().phase() == NativeFailureParity.Phase.ROW_EVALUATION
                  || runs.host().phase() == NativeFailureParity.Phase.COLLECTION,
              runs::toString);
          assertTrue(
              String.valueOf(runs.host().rootCause().getMessage())
                  .toUpperCase(java.util.Locale.ROOT)
                  .contains("NULL"),
              runs::toString);
          runs.assertFailure(
              runs.host().rootCause().getClass(),
              null,
              runs.host().phase(),
              NativeFailureParity.Route.NATIVE);
        }
        assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
      }
    }
  }

  public static class AtomicRows extends org.apache.flink.table.functions.TableFunction<Object> {
    private final DataType type;
    private final DataType argumentType;

    public AtomicRows(String name) {
      type = conversion(name);
      argumentType = argumentConversion(name);
    }

    @Override
    public org.apache.flink.table.types.inference.TypeInference getTypeInference(
        org.apache.flink.table.catalog.DataTypeFactory factory) {
      return org.apache.flink.table.types.inference.TypeInference.newBuilder()
          .typedArguments(DataTypes.INT(), argumentType)
          .outputTypeStrategy(org.apache.flink.table.types.inference.TypeStrategies.explicit(type))
          .build();
    }

    public void eval(Integer mode, Object value) {
      if (type.getConversionClass() == ArrayList.class
          && value != null
          && value.getClass() != ArrayList.class)
        throw new IllegalStateException("atomic result is not an ArrayList");
      if (mode % 4 != 0) collect(value);
    }
  }

  public static class BitsRows extends FlinkTypedTableFunctionSqlHarnessTest.TypedRows {
    private final boolean floats;

    public BitsRows(String name) {
      super(conversion(name), DataTypes.BIGINT());
      floats = name.endsWith("FLOAT");
    }

    @Override
    public void eval(Integer mode, Object value) {
      if (value == null) {
        super.eval(mode, null);
        return;
      }
      if (floats) {
        for (float element : (float[]) value)
          super.eval(mode, (long) Float.floatToRawIntBits(element));
      } else {
        for (double element : (double[]) value)
          super.eval(mode, Double.doubleToRawLongBits(element));
      }
    }
  }

  public static class IntRows extends MutatingRows {
    private transient boolean called;

    public IntRows() {
      super("primitive:INT");
    }

    public void eval(Integer mode, int[] values) {
      called = true;
      super.eval(mode, values);
    }

    @Override
    public void close() {
      if (!called)
        throw new IllegalStateException("concrete primitive array method was not invoked");
      super.close();
    }
  }

  public static class ListRows extends MutatingRows {
    private transient boolean called;

    public ListRows() {
      super("list:STRING");
    }

    public void eval(Integer mode, List<String> values) {
      called = true;
      super.eval(mode, values);
    }

    @Override
    public void close() {
      if (!called) throw new IllegalStateException("concrete List method was not invoked");
      super.close();
    }
  }

  private static TableFunction<Row> concreteRows(String name) {
    switch (name) {
      case "primitive:INT":
        return new IntRows();
      case "arraylist:STRING":
        return new ArrayListRows();
      default:
        return new ListRows();
    }
  }

  public static class EmptyRowListResults extends FlinkTypedTableFunctionSqlHarnessTest.TypedRows {
    public EmptyRowListResults(String bridge) {
      super(
          conversion("list:INT"),
          DataTypes.ARRAY(emptyRowElement(bridge))
              .bridgedTo(bridge.endsWith("arraylist") ? ArrayList.class : List.class));
    }

    private static DataType emptyRowElement(String bridge) {
      return bridge.startsWith("wrapped_")
          ? new org.apache.flink.table.types.AtomicDataType(
              DataTypes.ROW().getLogicalType(), Row.class)
          : DataTypes.ROW();
    }

    @Override
    public void eval(Integer mode, Object value) {
      super.eval(
          mode,
          new ArrayList<>(
              java.util.Arrays.asList(
                  Row.of(), null, Row.ofKind(org.apache.flink.types.RowKind.DELETE))));
    }
  }

  public static class RequiredArrayListRows
      extends FlinkTypedTableFunctionSqlHarnessTest.TypedRows {
    private final boolean emitNull;

    public RequiredArrayListRows(boolean emitNull) {
      super(conversion("list:INT"), conversion("arraylist:INT").notNull());
      this.emitNull = emitNull;
    }

    @Override
    public void eval(Integer mode, Object value) {
      Object result =
          emitNull ? null : new ArrayList<>(value == null ? List.of() : (List<?>) value);
      collect(Row.of(result, mode));
    }
  }

  public static class ArrayListRows extends MutatingRows {
    private transient boolean called;

    public ArrayListRows() {
      super("arraylist:STRING");
    }

    public void eval(Integer mode, List<String> values) {
      if (values != null && values.getClass() != ArrayList.class)
        throw new IllegalStateException("result is not an ArrayList");
      called = true;
      super.eval(mode, values);
    }

    @Override
    public void close() {
      if (!called)
        throw new IllegalStateException("concrete ArrayList result method was not invoked");
      super.close();
    }
  }

  public static class InvalidListArgumentRows
      extends FlinkTypedTableFunctionSqlHarnessTest.TypedRows {
    public InvalidListArgumentRows(String bridge) {
      super(conversion(bridge + ":STRING"), conversion("list:STRING"));
    }
  }

  public static class NestedArrayListResultRows
      extends FlinkTypedTableFunctionSqlHarnessTest.TypedRows {
    public NestedArrayListResultRows() {
      super(
          DataTypes.ROW(DataTypes.FIELD("items", sourceArray("list:INT"))),
          DataTypes.ROW(DataTypes.FIELD("items", conversion("arraylist:INT"))));
    }

    @Override
    public void eval(Integer mode, Object value) {
      Row input = (Row) value;
      Object items = input.getField(0);
      super.eval(
          mode,
          Row.of(
              items == null ? null : new ArrayList<>(java.util.Arrays.asList((Object[]) items))));
    }
  }

  public static class DateArrayListRows
      extends FlinkDateIntArgumentTableFunctionSqlHarnessTest.IntDateRows {
    private transient boolean called;

    public DateArrayListRows() {
      super("list");
    }

    @Override
    DataType outputType() {
      return DataTypes.ROW(
          DataTypes.FIELD("v", conversion("arraylist:STRING")),
          DataTypes.FIELD("epoch_day", DataTypes.INT()),
          DataTypes.FIELD("pos", DataTypes.INT()));
    }

    public void eval(int epochDay, List<String> values) {
      if (values != null && values.getClass() != ArrayList.class)
        throw new IllegalStateException("DATE result is not an ArrayList");
      called = true;
      super.eval(epochDay, values);
    }

    @Override
    public void close() {
      if (!called) throw new IllegalStateException("DATE/ArrayList result method was not invoked");
      super.close();
    }
  }

  public static class PreviousRows extends FlinkTypedTableFunctionSqlHarnessTest.TypedRows {
    private transient Object previous;

    public PreviousRows(String name) {
      super(argumentConversion(name), conversion(name));
    }

    @Override
    public void eval(Integer mode, Object value) {
      super.eval(mode, previous);
      previous = value;
    }
  }

  public static class FailingRows extends MutatingRows {
    public FailingRows(String name) {
      super(name);
    }

    @Override
    public void eval(Integer mode, Object value) {
      if (mode == 2) throw new IllegalStateException("table conversion sentinel");
      super.eval(mode, value);
    }
  }

  static DataType element(String name) {
    String type = name.substring(name.indexOf(':') + 1);
    if (type.equals("INT")) return DataTypes.INT();
    if (type.equals("BIGINT")) return DataTypes.BIGINT();
    return FlinkTypedTableFunctionSqlHarnessTest.valueType(type);
  }

  static DataType sourceArray(String name) {
    var type = element(name);
    return DataTypes.ARRAY(name.startsWith("primitive:") ? type.notNull() : type);
  }

  static DataType argumentConversion(String name) {
    return name.startsWith("arraylist:")
        ? sourceArray(name).bridgedTo(List.class)
        : conversion(name);
  }

  static DataType conversion(String name) {
    if (name.startsWith("arraylist:")) return sourceArray(name).bridgedTo(ArrayList.class);
    if (name.startsWith("linkedlist:")) return sourceArray(name).bridgedTo(LinkedList.class);
    if (name.startsWith("list:")) return sourceArray(name).bridgedTo(List.class);
    Class<?> primitive;
    switch (name.substring(name.indexOf(':') + 1)) {
      case "BOOLEAN":
        primitive = boolean.class;
        break;
      case "TINYINT":
        primitive = byte.class;
        break;
      case "SMALLINT":
        primitive = short.class;
        break;
      case "INT":
        primitive = int.class;
        break;
      case "BIGINT":
        primitive = long.class;
        break;
      case "FLOAT":
        primitive = float.class;
        break;
      case "DOUBLE":
        primitive = double.class;
        break;
      default:
        throw new IllegalArgumentException(name);
    }
    return sourceArray(name).bridgedTo(Array.newInstance(primitive, 0).getClass());
  }

  static Object scalar(String name, int index) {
    String type = name.substring(name.indexOf(':') + 1);
    if (type.equals("INT"))
      return new int[] {Integer.MIN_VALUE, -1, 0, 1, Integer.MAX_VALUE, 17, -19}[index % 7];
    if (type.equals("BIGINT"))
      return new long[] {Long.MIN_VALUE, -1, 0, 1, Long.MAX_VALUE, 17, -19}[index % 7];
    return FlinkTypedTableFunctionSqlHarnessTest.value(type, index % 7);
  }

  static Object value(String name, int index) {
    if (index % 13 == 0) return null;
    int length = index % 7 == 0 ? 0 : 3;
    Object array = Array.newInstance(element(name).getConversionClass(), length);
    for (int i = 0; i < length; i++)
      Array.set(
          array, i, !name.startsWith("primitive:") && i == 1 ? null : scalar(name, index + i));
    return array;
  }

  static DataType rowType(String name) {
    return DataTypes.ROW(
        DataTypes.FIELD("id", DataTypes.INT()), DataTypes.FIELD("p", sourceArray(name)));
  }

  static TableEnvironment environment(String name, int count, RecoveryExecutionMetrics metrics) {
    var env = metrics.environment(new Configuration(), 1, 0);
    env.getConfig().enableObjectReuse();
    var table = StreamTableEnvironment.create(env);
    table.getConfig().set("streamfusion.transpose.batchRows", "7");
    var type = rowType(name);
    Object[] values =
        java.util.stream.IntStream.range(0, 64).mapToObj(i -> value(name, i)).toArray();
    var source =
        env.fromSequence(0, Math.max(0, count - 1))
            .filter(i -> i < count)
            .map(i -> Row.of(i.intValue(), values[(int) (i % values.length)]))
            .returns(ExternalTypeInfo.<Row>of(type));
    table.createTemporaryView(
        "src", table.fromDataStream(source, Schema.newBuilder().fromRowDataType(type).build()));
    return register(table, name);
  }

  static TableEnvironment register(TableEnvironment table, String name) {
    table.createTemporarySystemFunction("bridge_rows", new MutatingRows(name));
    return table;
  }

  public static class MutatingRows extends FlinkTypedTableFunctionSqlHarnessTest.TypedRows {
    private final String name;

    public MutatingRows(String name) {
      super(argumentConversion(name), conversion(name));
      this.name = name;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void eval(Integer mode, Object value) {
      super.eval(mode, value);
      if (value == null) return;
      if (value instanceof List) {
        var values = (List<Object>) value;
        if (!values.isEmpty()) values.set(0, scalar(name, mode));
        super.eval(mode, value);
        values.clear();
      } else {
        if (Array.getLength(value) > 0) Array.set(value, 0, scalar(name, mode));
        super.eval(mode, value);
        if (Array.getLength(value) > 0) Array.set(value, 0, scalar(name, mode + 1));
      }
    }
  }
}
