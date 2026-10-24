package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import java.util.stream.Stream;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.runtime.typeutils.ExternalTypeInfo;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.inference.TypeStrategies;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class FlinkPrimitiveResultTableFunctionSqlHarnessTest {
  static Stream<String> signatures() {
    return FlinkMixedPrimitiveTableFunctionSqlHarnessTest.signatures();
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("signatures")
  void directPrimitiveResultFieldsRetainTypesBitsMutableRowsAndLeftPadding(String signature) {
    var parts = signature.split(":");
    for (boolean left : List.of(false, true)) {
      try (var metrics = new RecoveryExecutionMetrics()) {
        java.util.function.Supplier<TableEnvironment> factory =
            () -> environment(parts[0], parts[1], 96, metrics);
        String sql = "SELECT s.x,s.p,t.v,t.m FROM src s" + join("result_rows", "v,m", left);
        if (left && flink118()) {
          assertReleasedLeftNullability(factory, sql);
          sql = sql.replace("s.x,s.p,t.v,t.m", "s.x,s.p,t.v");
        }
        assertValues(NativeFailureParity.run(factory, sql), metrics);
      }
    }
  }

  @ParameterizedTest(name = "null {0}")
  @MethodSource("signatures")
  void emittedNullPrimitiveMetadataKeepsReleasedBehavior(String signature) {
    var parts = signature.split(":");
    for (boolean left : List.of(false, true)) {
      try (var metrics = new RecoveryExecutionMetrics()) {
        java.util.function.Supplier<TableEnvironment> factory =
            () -> {
              var table =
                  FlinkMixedPrimitiveTableFunctionSqlHarnessTest.sourceEnvironment(
                      parts[0], parts[1], 32, metrics, false);
              table.createTemporarySystemFunction("null_rows", new NullRows(parts[0], parts[1]));
              return table;
            };
        String sql = "SELECT s.x,t.v,t.m FROM src s" + join("null_rows", "v,m", left);
        boolean observesScalar = !(left && flink118());
        if (!observesScalar) {
          assertReleasedLeftNullability(factory, sql);
          sql = sql.replace("s.x,t.v,t.m", "s.x,t.v");
        }
        var runs = NativeFailureParity.run(factory, sql);
        assertValues(runs, metrics);
        assertEquals(32, runs.host().rows().size());
        if (observesScalar) {
          Object expected = left ? null : primitiveDefault(parts[0]);
          for (var row : runs.host().rows()) assertEquals(expected, row.get(row.size() - 1));
        }
      }
    }
  }

  @ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(
      strings = {"BOOLEAN", "TINYINT", "SMALLINT", "INT", "BIGINT", "FLOAT", "DOUBLE"})
  void nullWithoutAProjectionRetainsSinkEnforcement(String kind) {
    for (boolean left : List.of(false, true)) {
      try (var metrics = new RecoveryExecutionMetrics()) {
        java.util.function.Supplier<TableEnvironment> factory =
            () -> {
              var table =
                  FlinkMixedPrimitiveTableFunctionSqlHarnessTest.sourceEnvironment(
                      kind, "array", 16, metrics, false);
              table.createTemporarySystemFunction("null_rows", new NullRows(kind, "array"));
              return table;
            };
        String sql = "SELECT s.x,s.p,t.v,t.m FROM src s" + join("null_rows", "v,m", left);
        if (left && flink118()) assertReleasedLeftNullability(factory, sql);
        else if (!left)
          NativeFailureParity.run(factory, sql)
              .assertFailure(
                  RuntimeException.class,
                  "Column 'm' is NOT NULL",
                  NativeFailureParity.Phase.ROW_EVALUATION,
                  NativeFailureParity.Route.NATIVE);
        else {
          var runs = NativeFailureParity.run(factory, sql);
          assertValues(runs, metrics);
          assertEquals(16, runs.host().rows().size());
          for (var row : runs.host().rows()) assertNull(row.get(row.size() - 1));
        }
      }
    }
  }

  @Test
  void reorderedNonNullFieldsKeepReleasedProjectionDefaults() {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var runs =
          NativeFailureParity.run(
              () -> {
                var table =
                    FlinkMixedPrimitiveTableFunctionSqlHarnessTest.sourceEnvironment(
                        "INT", "array", 32, metrics, false);
                table.createTemporarySystemFunction("null_rows", new NullRows("INT", "array"));
                return table;
              },
              "SELECT t.m,t.v,s.x FROM src s" + join("null_rows", "v,m", false));
      assertValues(runs, metrics);
      assertEquals(32, runs.host().rows().size());
      for (var row : runs.host().rows()) assertEquals(-1, row.get(1));
    }
  }

  @Test
  void boxedNonNullResultMetadataKeepsTheSameProjectionSemantics() {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var runs =
          NativeFailureParity.run(
              () -> {
                var table =
                    FlinkMixedPrimitiveTableFunctionSqlHarnessTest.sourceEnvironment(
                        "INT", "array", 32, metrics, false);
                table.createTemporarySystemFunction("boxed_null_rows", new BoxedNullRows());
                return table;
              },
              "SELECT s.x,t.v,t.m FROM src s" + join("boxed_null_rows", "v,m", false));
      assertValues(runs, metrics);
      assertEquals(32, runs.host().rows().size());
      for (var row : runs.host().rows()) assertEquals(-1, row.get(row.size() - 1));
    }
  }

  @ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(strings = {"array", "list"})
  void retainedArgumentsKeepTheirValuesAfterInputBatchesClose(String container) {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var runs =
          NativeFailureParity.run(
              () -> {
                var table =
                    FlinkMixedPrimitiveTableFunctionSqlHarnessTest.sourceEnvironment(
                        "INT", container, 193, metrics, false);
                table.createTemporarySystemFunction("previous_rows", new PreviousRows(container));
                return table;
              },
              "SELECT s.x,t.v,t.m FROM src s" + join("previous_rows", "v,m", false));
      assertValues(runs, metrics);
    }
  }

  @Test
  void emptyInputAndUserFailuresKeepEvaluationOrder() {
    try (var metrics = new RecoveryExecutionMetrics()) {
      for (int count : new int[] {0, 32}) {
        var runs =
            NativeFailureParity.run(
                () -> {
                  var table =
                      FlinkMixedPrimitiveTableFunctionSqlHarnessTest.sourceEnvironment(
                          "INT", "array", count, metrics, false);
                  table.createTemporarySystemFunction("failing_rows", new FailingRows());
                  return table;
                },
                "SELECT s.x,t.v,t.m FROM src s" + join("failing_rows", "v,m", false));
        if (count == 0) runs.assertSuccess(NativeFailureParity.Route.NATIVE);
        else
          runs.assertFailure(
              IllegalStateException.class,
              "primitive result sentinel",
              NativeFailureParity.Phase.ROW_EVALUATION,
              NativeFailureParity.Route.NATIVE);
      }
    }
  }

  @ParameterizedTest(name = "atomic {0}")
  @MethodSource("signatures")
  void atomicPrimitiveResultsRetainTypesBitsAndLeftPadding(String signature) {
    var parts = signature.split(":");
    for (boolean left : List.of(false, true)) {
      try (var metrics = new RecoveryExecutionMetrics()) {
        java.util.function.Supplier<TableEnvironment> factory =
            () -> {
              var table =
                  FlinkMixedPrimitiveTableFunctionSqlHarnessTest.sourceEnvironment(
                      parts[0], parts[1], 96, metrics, false);
              return registerAtomic(table, parts[0], parts[1]);
            };
        String sql = "SELECT s.x,s.p,t.m FROM src s" + join("atomic_rows", "m", left);
        assertValues(NativeFailureParity.run(factory, sql), metrics);
      }
    }
  }

  @ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(
      strings = {"BOOLEAN", "TINYINT", "SMALLINT", "INT", "BIGINT", "FLOAT", "DOUBLE"})
  void atomicNullEmissionsUseReleasedCollectorDefaults(String kind) {
    for (boolean left : List.of(false, true)) {
      try (var metrics = new RecoveryExecutionMetrics()) {
        java.util.function.Supplier<TableEnvironment> factory =
            () -> {
              var table =
                  FlinkMixedPrimitiveTableFunctionSqlHarnessTest.sourceEnvironment(
                      kind, "array", 32, metrics, false);
              table.createTemporarySystemFunction(
                  "atomic_rows", new AtomicRows(kind, "array", true));
              return table;
            };
        String sql = "SELECT s.x,s.p,t.m FROM src s" + join("atomic_rows", "m", left);
        var runs = NativeFailureParity.run(factory, sql);
        assertValues(runs, metrics);
        assertEquals(32, runs.host().rows().size());
        for (var row : runs.host().rows())
          assertEquals(primitiveDefault(kind), row.get(row.size() - 1));
      }
    }
  }

  @ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(
      strings = {"BOOLEAN:map", "INT:list", "FLOAT:primitive", "DOUBLE:row"})
  void changelogResultsSurviveRepeatedRecovery(String signature) throws Exception {
    checkRecovery(signature, false);
  }

  @ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(
      strings = {"BOOLEAN:map", "INT:list", "FLOAT:primitive", "DOUBLE:row"})
  void atomicChangelogResultsSurviveRepeatedRecovery(String signature) throws Exception {
    checkRecovery(signature, true);
  }

  private static void checkRecovery(String signature, boolean atomic) throws Exception {
    var parts = signature.split(":");
    String kind = parts[0], container = parts[1];
    var rows = new java.util.ArrayList<Row>();
    for (int i = 0; i < 48; i++)
      rows.add(
          Row.of(
              FlinkMixedPrimitiveTableFunctionSqlHarnessTest.scalarValue(kind, i),
              FlinkMixedPrimitiveTableFunctionSqlHarnessTest.containerValue(container, i)));
    for (int i = 0; i < 16; i++)
      for (var rowKind : List.of(RowKind.UPDATE_BEFORE, RowKind.UPDATE_AFTER, RowKind.DELETE)) {
        int index = rowKind == RowKind.UPDATE_BEFORE ? i : i + 2;
        rows.add(
            Row.ofKind(
                rowKind,
                FlinkMixedPrimitiveTableFunctionSqlHarnessTest.scalarValue(kind, index),
                FlinkMixedPrimitiveTableFunctionSqlHarnessTest.containerValue(container, index)));
      }
    var type = FlinkMixedPrimitiveTableFunctionSqlHarnessTest.rowType(kind, container, false);
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
                () ->
                    atomic
                        ? registerAtomic(recovery.uninterrupted(), kind, container)
                        : register(recovery.uninterrupted(), kind, container),
                () ->
                    atomic
                        ? registerAtomic(recovery.get(), kind, container)
                        : register(recovery.get(), kind, container),
                "SELECT s.x,"
                    + (atomic ? "t.m" : "t.v,t.m")
                    + " FROM recovery_input s"
                    + join(atomic ? "atomic_rows" : "result_rows", atomic ? "m" : "v,m", atomic));
        runs.assertSuccess(NativeFailureParity.Route.NATIVE);
        assertEquals(
            NativeParity.multiset(
                FlinkMixedPrimitiveTableFunctionSqlHarnessTest.rawRows(runs.host().rows())),
            NativeParity.multiset(
                FlinkMixedPrimitiveTableFunctionSqlHarnessTest.rawRows(runs.nativeRun().rows())));
        recovery.verifyRepeatedRecovery();
        RecoveryExecutionMetrics.assertConsumed(recovery.nativeExecution(), "NativeTableFunction");
      }
    }
  }

  @Test
  void nestedPrimitiveResultBridgesRetainFallback() {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var runs =
          NativeFailureParity.run(
              () -> {
                var table =
                    FlinkMixedPrimitiveTableFunctionSqlHarnessTest.sourceEnvironment(
                        "INT", "array", 32, metrics, false);
                table.createTemporarySystemFunction("nested_rows", new NestedRows());
                return table;
              },
              "SELECT s.x,s.p,t.v,t.m FROM src s" + join("nested_rows", "v,m", false));
      runs.assertSuccess(NativeFailureParity.Route.FALLBACK);
      assertTrue(
          runs.nativeRun().fallbackReasons().stream()
              .anyMatch(reason -> reason.contains("verified external Java conversions")),
          runs::toString);
    }
  }

  private static String join(String function, String columns, boolean left) {
    String application = "LATERAL TABLE(" + function + "(s.x,s.p)) AS t(" + columns + ")";
    return left ? " LEFT JOIN " + application + " ON TRUE" : "," + application;
  }

  static boolean flink118() {
    return org.apache.flink.runtime.util.EnvironmentInformation.getVersion().startsWith("1.18.");
  }

  static void assertValues(NativeFailureParity.Comparison runs, RecoveryExecutionMetrics metrics) {
    runs.assertSuccess(NativeFailureParity.Route.NATIVE);
    assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
    assertEquals(
        NativeParity.multiset(
            FlinkMixedPrimitiveTableFunctionSqlHarnessTest.rawRows(runs.host().rows())),
        NativeParity.multiset(
            FlinkMixedPrimitiveTableFunctionSqlHarnessTest.rawRows(runs.nativeRun().rows())));
    RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeTableFunction");
  }

  static void assertReleasedLeftNullability(
      java.util.function.Supplier<TableEnvironment> factory, String sql) {
    String host = null;
    for (boolean nativeRun : List.of(false, true)) {
      var table = factory.get();
      var scan = nativeRun ? tech.streamfusion.planner.NativePlanner.install(table) : null;
      var failure = assertThrows(AssertionError.class, () -> table.sqlQuery(sql));
      assertTrue(
          failure
              .getMessage()
              .contains("Conversion to relational algebra failed to preserve datatypes"),
          failure::getMessage);
      String types = failure.getMessage().split("\nrel:", 2)[0];
      if (nativeRun) {
        assertEquals(host, types);
        assertEquals(0, scan.substitutions());
      } else host = types;
    }
  }

  static TableEnvironment registerAtomic(TableEnvironment table, String kind, String container) {
    table.createTemporarySystemFunction("atomic_rows", new AtomicRows(kind, container));
    return table;
  }

  static Object primitiveDefault(String kind) {
    switch (kind) {
      case "BOOLEAN":
        return false;
      case "TINYINT":
        return (byte) -1;
      case "SMALLINT":
        return (short) -1;
      case "INT":
        return -1;
      case "BIGINT":
        return -1L;
      case "FLOAT":
        return -1.0f;
      case "DOUBLE":
        return -1.0d;
      default:
        throw new IllegalArgumentException(kind);
    }
  }

  static TableEnvironment environment(
      String kind, String container, int count, RecoveryExecutionMetrics metrics) {
    var table =
        FlinkMixedPrimitiveTableFunctionSqlHarnessTest.sourceEnvironment(
            kind, container, count, metrics, false);
    return register(table, kind, container);
  }

  static TableEnvironment register(TableEnvironment table, String kind, String container) {
    table.createTemporarySystemFunction("result_rows", new ResultRows(kind, container));
    return table;
  }

  public static class ResultRows extends FlinkMixedPrimitiveTableFunctionSqlHarnessTest.BaseRows {
    public ResultRows(String kind, String container) {
      super(kind, container, false, true);
    }

    public void eval(Object scalar, Object value) {
      emit(scalar, value);
    }
  }

  public static class PreviousRows extends ResultRows {
    private transient Object previous;

    public PreviousRows(String container) {
      super("INT", container);
    }

    @Override
    public void eval(Object scalar, Object value) {
      super.eval(scalar, previous);
      previous = value;
    }
  }

  public static class FailingRows extends ResultRows {
    public FailingRows() {
      super("INT", "array");
    }

    @Override
    public void eval(Object scalar, Object value) {
      if (scalar.equals(1)) throw new IllegalStateException("primitive result sentinel");
      super.eval(scalar, value);
    }
  }

  public static class AtomicRows extends TableFunction<Object> {
    private final String kind, container;
    private final boolean nullResult;
    private transient boolean opened;

    public AtomicRows(String kind, String container) {
      this(kind, container, false);
    }

    public AtomicRows(String kind, String container, boolean nullResult) {
      this.kind = kind;
      this.container = container;
      this.nullResult = nullResult;
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory factory) {
      var scalar = FlinkMixedPrimitiveTableFunctionSqlHarnessTest.scalarType(kind).notNull();
      return TypeInference.newBuilder()
          .typedArguments(
              scalar, FlinkMixedPrimitiveTableFunctionSqlHarnessTest.containerType(container))
          .outputTypeStrategy(
              TypeStrategies.explicit(
                  scalar.bridgedTo(
                      FlinkMixedPrimitiveTableFunctionSqlHarnessTest.primitiveClass(kind))))
          .build();
    }

    public void eval(Object scalar, Object value) {
      if (!opened) throw new IllegalStateException("atomic function not open");
      if (nullResult) collect(null);
      else if (value != null) collect(scalar);
    }

    @Override
    public void open(org.apache.flink.table.functions.FunctionContext context) {
      if (opened) throw new IllegalStateException("atomic function opened twice");
      opened = true;
    }

    @Override
    public void close() {
      if (!opened) throw new IllegalStateException("atomic function closed twice");
      opened = false;
    }
  }

  public static class BoxedNullRows extends TableFunction<Row> {
    @Override
    public TypeInference getTypeInference(DataTypeFactory factory) {
      return TypeInference.newBuilder()
          .typedArguments(
              DataTypes.INT().notNull(),
              FlinkMixedPrimitiveTableFunctionSqlHarnessTest.containerType("array"))
          .outputTypeStrategy(
              TypeStrategies.explicit(
                  DataTypes.ROW(
                      DataTypes.FIELD(
                          "v",
                          FlinkMixedPrimitiveTableFunctionSqlHarnessTest.containerType("array")),
                      DataTypes.FIELD("m", DataTypes.INT().notNull()))))
          .build();
    }

    public void eval(Object scalar, Object value) {
      collect(Row.of(value, null));
    }
  }

  public static class NestedRows extends TableFunction<Row> {
    @Override
    public TypeInference getTypeInference(DataTypeFactory factory) {
      return TypeInference.newBuilder()
          .typedArguments(
              DataTypes.INT().notNull(),
              FlinkMixedPrimitiveTableFunctionSqlHarnessTest.containerType("array"))
          .outputTypeStrategy(
              TypeStrategies.explicit(
                  DataTypes.ROW(
                      DataTypes.FIELD(
                          "v",
                          FlinkMixedPrimitiveTableFunctionSqlHarnessTest.containerType("array")),
                      DataTypes.FIELD(
                          "m",
                          DataTypes.ROW(
                              DataTypes.FIELD(
                                  "q", DataTypes.INT().notNull().bridgedTo(int.class)))))))
          .build();
    }

    public void eval(Object scalar, Object value) {
      collect(Row.of(value, Row.of(scalar)));
    }
  }

  public static class NullRows extends ResultRows {
    public NullRows(String kind, String container) {
      super(kind, container);
    }

    @Override
    public void eval(Object scalar, Object value) {
      collect(Row.of(value, null));
    }
  }
}
