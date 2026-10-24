package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class FlinkMixedPrimitiveTableFunctionSqlHarnessTest {
  static Stream<String> signatures() {
    String selected = System.getProperty("mixed.cases");
    if (selected != null) return Stream.of(selected.split(";"));
    return Stream.of("BOOLEAN", "TINYINT", "SMALLINT", "INT", "BIGINT", "FLOAT", "DOUBLE")
        .flatMap(
            kind ->
                Stream.of("array", "primitive", "list", "map", "row")
                    .map(container -> kind + ":" + container));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("signatures")
  void primitiveArgumentsWithContainersRetainValuesTypesAndMutableResults(String signature) {
    String[] parts = signature.split(":");
    for (String join :
        List.of(
            ", LATERAL TABLE(mixed_rows(s.x,s.p)) AS t(v,m)",
            " LEFT JOIN LATERAL TABLE(mixed_rows(s.x,s.p)) AS t(v,m) ON TRUE")) {
      try (var metrics = new RecoveryExecutionMetrics()) {
        var runs =
            NativeFailureParity.run(
                () -> environment(parts[0], parts[1], 96, metrics),
                "SELECT s.x,s.p,t.v,t.m FROM src s" + join);
        runs.assertSuccess(NativeFailureParity.Route.NATIVE);
        assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
        assertEquals(
            NativeParity.multiset(rawRows(runs.host().rows())),
            NativeParity.multiset(rawRows(runs.nativeRun().rows())));
        RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeTableFunction");
      }
    }
  }

  @Test
  void boxedScalarControlKeepsItsExistingNativeRoute() {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var runs =
          NativeFailureParity.run(
              () -> {
                var table = sourceEnvironment("INT", "array", 96, metrics, false);
                table.createTemporarySystemFunction("mixed_rows", new BoxedIntRows("array", false));
                return table;
              },
              "SELECT t.v,t.m FROM src s,LATERAL TABLE(mixed_rows(s.x,s.p)) AS t(v,m)");
      runs.assertSuccess(NativeFailureParity.Route.NATIVE);
      RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeTableFunction");
    }
  }

  @Test
  void primitiveResultMetadataUsesTheVerifiedNativeRoute() {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var runs =
          NativeFailureParity.run(
              () -> {
                var table = sourceEnvironment("INT", "array", 32, metrics, false);
                table.createTemporarySystemFunction("mixed_rows", new BoxedIntRows("array", true));
                return table;
              },
              "SELECT t.v,t.m FROM src s,LATERAL TABLE(mixed_rows(s.x,s.p)) AS t(v,m)");
      runs.assertSuccess(NativeFailureParity.Route.NATIVE);
      RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeTableFunction");
    }
  }

  @Test
  void nullableScalarInputsKeepReleasedPlanningValidation() {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var runs =
          NativeFailureParity.run(
              () -> {
                var table = sourceEnvironment("INT", "array", 16, metrics, true);
                table.createTemporarySystemFunction("mixed_rows", new IntRows("array"));
                return table;
              },
              "SELECT t.v,t.m FROM src s,LATERAL TABLE(mixed_rows(s.x,s.p)) AS t(v,m)");
      assertNotNull(runs.host().failure(), runs::toString);
      assertNotNull(runs.nativeRun().failure(), runs::toString);
      assertEquals(NativeFailureParity.Phase.PLANNING, runs.host().phase(), runs::toString);
      assertEquals(runs.host().phase(), runs.nativeRun().phase(), runs::toString);
      assertEquals(runs.host().rootCause().getClass(), runs.nativeRun().rootCause().getClass());
      assertEquals(runs.host().rootCause().getMessage(), runs.nativeRun().rootCause().getMessage());
      assertTrue(metrics.nativeOperators().isEmpty());
    }
  }

  @Test
  void integerDateArgumentsUseTheVerifiedPrimitiveArgumentGate() {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var runs =
          NativeFailureParity.run(
              () -> {
                var table = sourceEnvironment("DATE", "array", 32, metrics, false);
                table.createTemporarySystemFunction("date_rows", new DateRows());
                return table;
              },
              "SELECT t.v FROM src s,LATERAL TABLE(date_rows(s.x,s.p)) AS t(v)");
      runs.assertSuccess(NativeFailureParity.Route.NATIVE);
      RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeTableFunction");
    }
  }

  @ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(strings = {"array", "list"})
  void literalPrimitiveArgumentsUseReleasedBinding(String container) {
    try (var metrics = new RecoveryExecutionMetrics()) {
      NativeFailureParity.run(
              () -> environment("INT", container, 32, metrics),
              "SELECT s.x,t.v,t.m FROM src s,LATERAL TABLE(mixed_rows(7,s.p)) AS t(v,m)")
          .assertSuccess(NativeFailureParity.Route.NATIVE);
      RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeTableFunction");
    }
  }

  @ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(strings = {"array", "list"})
  void retainedContainersStayOwnedAcrossLaterBatches(String container) {
    try (var metrics = new RecoveryExecutionMetrics()) {
      NativeFailureParity.run(
              () -> {
                var table = sourceEnvironment("INT", container, 193, metrics, false);
                table.createTemporarySystemFunction(
                    "previous_rows", new PreviousIntRows(container));
                return table;
              },
              "SELECT s.x,t.v,t.m FROM src s,LATERAL TABLE(previous_rows(s.x,s.p)) AS t(v,m)")
          .assertSuccess(NativeFailureParity.Route.NATIVE);
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
                  var table = sourceEnvironment("INT", "array", count, metrics, false);
                  table.createTemporarySystemFunction("failing_rows", new FailingIntRows());
                  return table;
                },
                "SELECT s.x,t.v,t.m FROM src s,LATERAL TABLE(failing_rows(s.x,s.p)) AS t(v,m)");
        if (count == 0) runs.assertSuccess(NativeFailureParity.Route.NATIVE);
        else
          runs.assertFailure(
              IllegalStateException.class,
              "mixed primitive sentinel",
              NativeFailureParity.Phase.ROW_EVALUATION,
              NativeFailureParity.Route.NATIVE);
      }
    }
  }

  @ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(
      strings = {"BOOLEAN:map", "INT:list", "FLOAT:primitive", "DOUBLE:row"})
  void changelogValuesSurviveRepeatedRecovery(String signature) throws Exception {
    String[] parts = signature.split(":");
    String kind = parts[0], container = parts[1];
    var rows = new ArrayList<Row>();
    for (int i = 0; i < 48; i++)
      rows.add(Row.of(scalarValue(kind, i), containerValue(container, i)));
    for (int i = 0; i < 16; i++) {
      rows.add(
          Row.ofKind(
              org.apache.flink.types.RowKind.UPDATE_BEFORE,
              scalarValue(kind, i),
              containerValue(container, i)));
      rows.add(
          Row.ofKind(
              org.apache.flink.types.RowKind.UPDATE_AFTER,
              scalarValue(kind, i + 2),
              containerValue(container, i + 2)));
      rows.add(
          Row.ofKind(
              org.apache.flink.types.RowKind.DELETE,
              scalarValue(kind, i + 2),
              containerValue(container, i + 2)));
    }
    var type = rowType(kind, container, false);
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
                () -> register(recovery.uninterrupted(), kind, container),
                () -> register(recovery.get(), kind, container),
                "SELECT s.x,t.v,t.m FROM recovery_input s LEFT JOIN LATERAL"
                    + " TABLE(mixed_rows(s.x,s.p)) AS t(v,m) ON TRUE");
        runs.assertSuccess(NativeFailureParity.Route.NATIVE);
        assertEquals(
            NativeParity.multiset(rawRows(runs.host().rows())),
            NativeParity.multiset(rawRows(runs.nativeRun().rows())));
        recovery.verifyRepeatedRecovery();
        RecoveryExecutionMetrics.assertConsumed(recovery.nativeExecution(), "NativeTableFunction");
      }
    }
  }

  static List<List<Object>> rawRows(List<List<Object>> rows) {
    var result = new ArrayList<List<Object>>();
    for (var row : rows) {
      var values = new ArrayList<Object>();
      for (Object value : row) {
        if (value instanceof Float) values.add(Float.floatToRawIntBits((Float) value));
        else if (value instanceof Double) values.add(Double.doubleToRawLongBits((Double) value));
        else values.add(value);
      }
      result.add(values);
    }
    return result;
  }

  static DataType scalarType(String kind) {
    if (kind.equals("INT")) return DataTypes.INT();
    if (kind.equals("BIGINT")) return DataTypes.BIGINT();
    return FlinkTypedTableFunctionSqlHarnessTest.valueType(kind);
  }

  static Class<?> primitiveClass(String kind) {
    switch (kind) {
      case "BOOLEAN":
        return boolean.class;
      case "TINYINT":
        return byte.class;
      case "SMALLINT":
        return short.class;
      case "INT":
        return int.class;
      case "BIGINT":
        return long.class;
      case "FLOAT":
        return float.class;
      case "DOUBLE":
        return double.class;
      default:
        throw new IllegalArgumentException(kind);
    }
  }

  static Object scalarValue(String kind, int index) {
    return FlinkTableFunctionConversionSqlHarnessTest.scalar("list:" + kind, index % 7);
  }

  static String containerName(String container) {
    switch (container) {
      case "array":
        return "array_int";
      case "list":
        return "array_string";
      case "map":
        return "map";
      case "row":
        return "row_STRING";
      default:
        throw new IllegalArgumentException(container);
    }
  }

  static DataType sourceContainer(String container) {
    if (container.equals("primitive"))
      return FlinkTableFunctionConversionSqlHarnessTest.sourceArray("primitive:INT");
    return FlinkNestedTableFunctionSqlHarnessTest.valueType(containerName(container));
  }

  static DataType containerType(String container) {
    if (container.equals("primitive")) return sourceContainer(container).bridgedTo(int[].class);
    if (container.equals("list")) return sourceContainer(container).bridgedTo(List.class);
    return sourceContainer(container);
  }

  static Object containerValue(String container, int index) {
    if (container.equals("primitive"))
      return FlinkTableFunctionConversionSqlHarnessTest.value("primitive:INT", index);
    return FlinkNestedTableFunctionSqlHarnessTest.value(containerName(container), index);
  }

  static DataType rowType(String kind, String container, boolean nullableScalar) {
    var scalar = scalarType(kind);
    return DataTypes.ROW(
        DataTypes.FIELD("x", nullableScalar ? scalar : scalar.notNull()),
        DataTypes.FIELD("p", sourceContainer(container)));
  }

  static TableEnvironment sourceEnvironment(
      String kind,
      String container,
      int count,
      RecoveryExecutionMetrics metrics,
      boolean nullableScalar) {
    var env = metrics.environment(new Configuration(), 1, 0);
    env.getConfig().enableObjectReuse();
    var table = StreamTableEnvironment.create(env);
    table.getConfig().set("streamfusion.transpose.batchRows", "7");
    var type = rowType(kind, container, nullableScalar);
    Object[] scalars =
        java.util.stream.IntStream.range(0, 7).mapToObj(i -> scalarValue(kind, i)).toArray();
    Object[] values =
        java.util.stream.IntStream.range(0, 64)
            .mapToObj(i -> containerValue(container, i))
            .toArray();
    var source =
        env.fromSequence(0, Math.max(0, count - 1))
            .filter(i -> i < count)
            .map(
                i ->
                    Row.of(
                        nullableScalar && i % 7 == 0 ? null : scalars[(int) (i % scalars.length)],
                        values[(int) (i % values.length)]))
            .returns(ExternalTypeInfo.<Row>of(type));
    table.createTemporaryView(
        "src", table.fromDataStream(source, Schema.newBuilder().fromRowDataType(type).build()));
    return table;
  }

  static TableEnvironment environment(
      String kind, String container, int count, RecoveryExecutionMetrics metrics) {
    return register(sourceEnvironment(kind, container, count, metrics, false), kind, container);
  }

  static TableEnvironment register(TableEnvironment table, String kind, String container) {
    table.createTemporarySystemFunction("mixed_rows", function(kind, container));
    return table;
  }

  static BaseRows function(String kind, String container) {
    switch (kind) {
      case "BOOLEAN":
        return new BooleanRows(container);
      case "TINYINT":
        return new ByteRows(container);
      case "SMALLINT":
        return new ShortRows(container);
      case "INT":
        return new IntRows(container);
      case "BIGINT":
        return new LongRows(container);
      case "FLOAT":
        return new FloatRows(container);
      case "DOUBLE":
        return new DoubleRows(container);
      default:
        throw new IllegalArgumentException(kind);
    }
  }

  public abstract static class BaseRows extends TableFunction<Row> {
    private final String kind;
    private final String container;
    private final boolean primitiveArgument;
    private final boolean primitiveResult;
    private transient Row result;

    BaseRows(String kind, String container) {
      this(kind, container, true, false);
    }

    BaseRows(String kind, String container, boolean primitiveArgument, boolean primitiveResult) {
      this.kind = kind;
      this.container = container;
      this.primitiveArgument = primitiveArgument;
      this.primitiveResult = primitiveResult;
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory factory) {
      var input = scalarType(kind).notNull();
      if (primitiveArgument) input = input.bridgedTo(primitiveClass(kind));
      var output =
          primitiveResult
              ? scalarType(kind).notNull().bridgedTo(primitiveClass(kind))
              : scalarType(kind);
      return TypeInference.newBuilder()
          .typedArguments(input, containerType(container))
          .outputTypeStrategy(
              TypeStrategies.explicit(
                  DataTypes.ROW(
                      DataTypes.FIELD("v", containerType(container)),
                      DataTypes.FIELD("m", output))))
          .build();
    }

    @Override
    public void open(FunctionContext context) {
      if (result != null) throw new IllegalStateException("mixed function opened twice");
      result = new Row(2);
    }

    protected void emit(Object scalar, Object value) {
      if (result == null) throw new IllegalStateException("mixed function not open");
      if (value == null) return;
      result.setField(0, value);
      result.setField(1, scalar);
      collect(result);
      result.setField(0, null);
      result.setField(1, primitiveResult ? scalarValue(kind, 1) : null);
      collect(result);
      result.setField(1, scalarValue(kind, 0));
    }

    @Override
    public void close() {
      if (result == null) throw new IllegalStateException("mixed function closed twice");
      result = null;
    }
  }

  public static class BooleanRows extends BaseRows {
    public BooleanRows(String container) {
      super("BOOLEAN", container);
    }

    public void eval(boolean scalar, Object value) {
      emit(scalar, value);
    }
  }

  public static class ByteRows extends BaseRows {
    public ByteRows(String container) {
      super("TINYINT", container);
    }

    public void eval(byte scalar, Object value) {
      emit(scalar, value);
    }
  }

  public static class ShortRows extends BaseRows {
    public ShortRows(String container) {
      super("SMALLINT", container);
    }

    public void eval(short scalar, Object value) {
      emit(scalar, value);
    }
  }

  public static class IntRows extends BaseRows {
    public IntRows(String container) {
      super("INT", container);
    }

    public void eval(int scalar, Object value) {
      emit(scalar, value);
    }
  }

  public static class LongRows extends BaseRows {
    public LongRows(String container) {
      super("BIGINT", container);
    }

    public void eval(long scalar, Object value) {
      emit(scalar, value);
    }
  }

  public static class FloatRows extends BaseRows {
    public FloatRows(String container) {
      super("FLOAT", container);
    }

    public void eval(float scalar, Object value) {
      emit(scalar, value);
    }
  }

  public static class DoubleRows extends BaseRows {
    public DoubleRows(String container) {
      super("DOUBLE", container);
    }

    public void eval(double scalar, Object value) {
      emit(scalar, value);
    }
  }

  public static class PreviousIntRows extends IntRows {
    private transient Object previous;

    public PreviousIntRows(String container) {
      super(container);
    }

    @Override
    public void eval(int scalar, Object value) {
      super.eval(scalar, previous);
      previous = value;
    }
  }

  public static class FailingIntRows extends IntRows {
    public FailingIntRows() {
      super("array");
    }

    @Override
    public void eval(int scalar, Object value) {
      if (scalar == 1) throw new IllegalStateException("mixed primitive sentinel");
      super.eval(scalar, value);
    }
  }

  public static class BoxedIntRows extends BaseRows {
    public BoxedIntRows(String container, boolean primitiveResult) {
      super("INT", container, false, primitiveResult);
    }

    public void eval(Integer scalar, Object value) {
      emit(scalar, value);
    }
  }

  public static class DateRows extends TableFunction<Row> {
    @Override
    public TypeInference getTypeInference(DataTypeFactory factory) {
      return TypeInference.newBuilder()
          .typedArguments(DataTypes.DATE().notNull().bridgedTo(int.class), sourceContainer("array"))
          .outputTypeStrategy(
              TypeStrategies.explicit(
                  DataTypes.ROW(DataTypes.FIELD("v", sourceContainer("array")))))
          .build();
    }

    public void eval(int scalar, Object value) {
      collect(Row.of(value));
    }
  }
}
