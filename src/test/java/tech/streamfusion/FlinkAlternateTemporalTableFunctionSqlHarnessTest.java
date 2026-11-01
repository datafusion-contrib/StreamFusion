package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.time.ZoneId;
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
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.inference.TypeStrategies;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class FlinkAlternateTemporalTableFunctionSqlHarnessTest {
  static Stream<String> bridges() {
    return Stream.of(
        System.getProperty(
                "temporal.bridge.cases",
                "date-sql;time-integer;time-long;time-sql;timestamp-sql;ltz-integer;ltz-long;ltz-sql;yearmonth-integer;daytime-long")
            .split(";"));
  }

  static Stream<Arguments> signatures() {
    return bridges()
        .flatMap(
            bridge ->
                Stream.of("array", "list")
                    .flatMap(
                        container ->
                            (bridge.contains("timestamp") || bridge.startsWith("ltz")
                                    ? Stream.of("UTC", "America/New_York", "Asia/Shanghai")
                                    : Stream.of("UTC"))
                                .map(zone -> Arguments.of(bridge, container, zone))));
  }

  @ParameterizedTest(name = "{0} {1} {2}")
  @MethodSource("signatures")
  void directTemporalConversionsRetainTypesNullsOwnedFanoutAndZones(
      String bridge, String container, String zone) {
    for (boolean left : List.of(false, true)) {
      try (var metrics = new RecoveryExecutionMetrics()) {
        var runs =
            NativeFailureParity.run(
                () -> environment(bridge, container, zone, 96, metrics, true, false),
                query("src", left));
        assertValues(runs, metrics);
        assertFalse(runs.host().rows().isEmpty());
      }
    }
  }

  @ParameterizedTest
  @MethodSource("bridges")
  void nonNullArgumentsAndRetainedContainersUseReleasedConversions(String bridge) {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var runs =
          NativeFailureParity.run(
              () -> environment(bridge, "array", "America/New_York", 193, metrics, false, true),
              query("src", false));
      assertValues(runs, metrics);
    }
  }

  @ParameterizedTest
  @MethodSource("bridges")
  void atomicResultsKeepReleasedNullEmissionAndLeftPadding(String bridge) {
    for (boolean left : List.of(false, true)) {
      try (var metrics = new RecoveryExecutionMetrics()) {
        var runs =
            NativeFailureParity.run(
                () -> {
                  var table =
                      environment(bridge, "array", "America/New_York", 96, metrics, true, false);
                  table.createTemporarySystemFunction("atomic_rows", new AtomicRows(bridge));
                  return table;
                },
                "SELECT s.x,s.p,t.m FROM src s"
                    + (left ? " LEFT JOIN " : ",")
                    + "LATERAL TABLE(atomic_rows(s.x,s.p)) AS t(m)"
                    + (left ? " ON TRUE" : ""));
        assertValues(runs, metrics);
        assertTrue(runs.host().rows().stream().anyMatch(row -> row.get(3) == null));
      }
    }
  }

  @ParameterizedTest
  @MethodSource("bridges")
  void atomicNonNullResultsKeepReleasedValuesAndLeftPadding(String bridge) {
    for (boolean left : List.of(false, true)) {
      try (var metrics = new RecoveryExecutionMetrics()) {
        var runs =
            NativeFailureParity.run(
                () -> {
                  var table = environment(bridge, "array", "UTC", 96, metrics, false, false);
                  table.createTemporarySystemFunction("atomic_rows", new AtomicRows(bridge, true));
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
  @MethodSource("bridges")
  void nonNullResultMetadataKeepsReleasedProjectionDefaults(String bridge) {
    for (boolean left : List.of(false, true)) {
      try (var metrics = new RecoveryExecutionMetrics()) {
        java.util.function.Supplier<TableEnvironment> factory =
            () -> {
              var table = environment(bridge, "array", "UTC", 32, metrics, true, false);
              table.dropTemporarySystemFunction("temporal_rows");
              table.createTemporarySystemFunction("temporal_rows", new NonNullRows(bridge));
              return table;
            };
        String sql =
            "SELECT s.x,t.m,t.pos FROM src s"
                + (left ? " LEFT JOIN " : ",")
                + "LATERAL TABLE(temporal_rows(s.x,s.p)) AS t(v,m,pos)"
                + (left ? " ON TRUE" : "");
        if (left && FlinkPrimitiveResultTableFunctionSqlHarnessTest.flink118()) {
          FlinkPrimitiveResultTableFunctionSqlHarnessTest.assertReleasedLeftNullability(
              factory, sql);
          sql = sql.replace("s.x,t.m,t.pos", "s.x,t.pos");
        }
        var runs = NativeFailureParity.run(factory, sql);
        if (!left && bridge.startsWith("time-")) {
          runs.assertFailure(
              java.time.DateTimeException.class,
              "Invalid value for NanoOfSecond",
              NativeFailureParity.Phase.COLLECTION,
              NativeFailureParity.Route.NATIVE);
        } else if (!left && (bridge.startsWith("timestamp-") || bridge.startsWith("ltz-"))) {
          runs.assertFailure(
              RuntimeException.class,
              "Column 'm' is NOT NULL",
              NativeFailureParity.Phase.ROW_EVALUATION,
              NativeFailureParity.Route.NATIVE);
        } else {
          assertValues(runs, metrics);
        }
      }
    }
  }

  @ParameterizedTest
  @MethodSource("bridges")
  void nonNullResultMetadataRetainsSuccessfulValues(String bridge) {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var runs =
          NativeFailureParity.run(
              () -> {
                var table = environment(bridge, "array", "UTC", 96, metrics, false, false);
                table.dropTemporarySystemFunction("temporal_rows");
                table.createTemporarySystemFunction(
                    "temporal_rows", new NonNullRows(bridge, false));
                return table;
              },
              query("src", false));
      assertValues(runs, metrics);
    }
  }

  @ParameterizedTest
  @MethodSource("bridges")
  void emptyInputAndCallbackFailuresKeepReleasedSemantics(String bridge) {
    for (int count : new int[] {0, 32}) {
      try (var metrics = new RecoveryExecutionMetrics()) {
        var runs =
            NativeFailureParity.run(
                () -> {
                  var table = environment(bridge, "array", "UTC", count, metrics, true, false);
                  table.dropTemporarySystemFunction("temporal_rows");
                  table.createTemporarySystemFunction("temporal_rows", new FailingRows(bridge));
                  return table;
                },
                query("src", false));
        if (count == 0) runs.assertSuccess(NativeFailureParity.Route.NATIVE);
        else
          runs.assertFailure(
              IllegalStateException.class,
              "temporal bridge sentinel",
              NativeFailureParity.Phase.ROW_EVALUATION,
              NativeFailureParity.Route.NATIVE);
      }
    }
  }

  @ParameterizedTest
  @MethodSource("bridges")
  void changelogAndTemporalConversionsSurviveRepeatedRecovery(String bridge) throws Exception {
    String kind = kind(bridge);
    var rows = new ArrayList<Row>();
    for (int i = 0; i < 48; i++) rows.add(input(kind, i));
    for (int i = 0; i < 16; i++) {
      for (var change : List.of(RowKind.UPDATE_BEFORE, RowKind.UPDATE_AFTER, RowKind.DELETE)) {
        var row = input(kind, change == RowKind.UPDATE_BEFORE ? i : i + 2);
        row.setKind(change);
        rows.add(row);
      }
    }
    var type = FlinkMixedPrimitiveTableFunctionSqlHarnessTest.rowType(kind, "array", true);
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
                    register(
                        recovery.uninterrupted(), bridge, "array", "America/New_York", true, false),
                () -> register(recovery.get(), bridge, "array", "America/New_York", true, false),
                query("recovery_input", true));
        runs.assertSuccess(NativeFailureParity.Route.NATIVE);
        assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
        recovery.verifyRepeatedRecovery();
        RecoveryExecutionMetrics.assertConsumed(recovery.nativeExecution(), "NativeTableFunction");
      }
    }
  }

  private static Row input(String kind, int index) {
    return Row.of(
        index % 7 == 0
            ? null
            : FlinkMixedPrimitiveTableFunctionSqlHarnessTest.scalarValue(kind, index),
        FlinkMixedPrimitiveTableFunctionSqlHarnessTest.containerValue("array", index));
  }

  private static void assertValues(
      NativeFailureParity.Comparison runs, RecoveryExecutionMetrics metrics) {
    runs.assertSuccess(NativeFailureParity.Route.NATIVE);
    assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
    RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeTableFunction");
  }

  static String query(String source, boolean left) {
    return "SELECT s.x,s.p,t.v,t.m,t.pos FROM "
        + source
        + " s"
        + (left ? " LEFT JOIN " : ",")
        + "LATERAL TABLE(temporal_rows(s.x,s.p)) AS t(v,m,pos)"
        + (left ? " ON TRUE" : "");
  }

  static TableEnvironment environment(
      String bridge,
      String container,
      String zone,
      int count,
      RecoveryExecutionMetrics metrics,
      boolean nullable,
      boolean previous) {
    return register(
        FlinkMixedPrimitiveTableFunctionSqlHarnessTest.sourceEnvironment(
            kind(bridge), container, count, metrics, nullable),
        bridge,
        container,
        zone,
        nullable,
        previous);
  }

  private static TableEnvironment register(
      TableEnvironment table,
      String bridge,
      String container,
      String zone,
      boolean nullable,
      boolean previous) {
    table.getConfig().setLocalTimeZone(ZoneId.of(zone));
    table.createTemporarySystemFunction(
        "temporal_rows", new TemporalRows(bridge, container, nullable, previous));
    return table;
  }

  static String kind(String bridge) {
    if (bridge.startsWith("yearmonth-")) return "YEAR_MONTH";
    if (bridge.startsWith("daytime-")) return "DAY_TIME";
    if (bridge.startsWith("date-")) return "DATE";
    if (bridge.startsWith("time-")) return "TIME";
    return bridge.startsWith("ltz-") ? "TIMESTAMP_LTZ(9)" : "TIMESTAMP(9)";
  }

  static Class<?> conversion(String bridge) {
    if (bridge.endsWith("-integer")) return Integer.class;
    if (bridge.endsWith("-long")) return Long.class;
    if (bridge.startsWith("date-")) return java.sql.Date.class;
    if (bridge.startsWith("time-")) return java.sql.Time.class;
    return java.sql.Timestamp.class;
  }

  public static class TemporalRows extends TableFunction<Row> {
    private final String bridge;
    private final String container;
    private final boolean nullable;
    private final boolean previous;
    private transient Row result;
    private transient Object retained;

    public TemporalRows(String bridge, String container, boolean nullable, boolean previous) {
      this.bridge = bridge;
      this.container = container;
      this.nullable = nullable;
      this.previous = previous;
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory factory) {
      DataType scalar =
          FlinkMixedPrimitiveTableFunctionSqlHarnessTest.scalarType(kind(bridge))
              .bridgedTo(conversion(bridge));
      return TypeInference.newBuilder()
          .typedArguments(
              nullable ? scalar : scalar.notNull(),
              FlinkMixedPrimitiveTableFunctionSqlHarnessTest.containerType(container))
          .outputTypeStrategy(
              TypeStrategies.explicit(
                  DataTypes.ROW(
                      DataTypes.FIELD(
                          "v",
                          FlinkMixedPrimitiveTableFunctionSqlHarnessTest.containerType(container)),
                      DataTypes.FIELD("m", resultScalar(scalar)),
                      DataTypes.FIELD("pos", DataTypes.INT()))))
          .build();
    }

    protected DataType resultScalar(DataType scalar) {
      return scalar;
    }

    @Override
    public void open(FunctionContext context) {
      if (result != null) throw new IllegalStateException("temporal bridge opened twice");
      result = new Row(3);
    }

    public void eval(Object scalar, Object value) {
      if (result == null) throw new IllegalStateException("temporal bridge not open");
      if (scalar != null && !conversion(bridge).isInstance(scalar))
        throw new IllegalStateException("wrong temporal bridge class " + scalar.getClass());
      Object output = previous ? retained : value;
      retained = value;
      if (output == null) return;
      result.setField(0, output);
      result.setField(1, scalar);
      result.setField(2, 0);
      collect(result);
      if (scalar instanceof java.util.Date) ((java.util.Date) scalar).setTime(42);
      result.setField(0, null);
      result.setField(1, null);
      result.setField(2, 1);
      collect(result);
      result.setField(2, -1);
    }

    @Override
    public void close() {
      if (result == null) throw new IllegalStateException("temporal bridge closed twice");
      result = null;
      retained = null;
    }
  }

  public static class NonNullRows extends TemporalRows {
    private final boolean emitNulls;

    public NonNullRows(String bridge) {
      this(bridge, true);
    }

    public NonNullRows(String bridge, boolean emitNulls) {
      super(bridge, "array", emitNulls, false);
      this.emitNulls = emitNulls;
    }

    @Override
    protected DataType resultScalar(DataType scalar) {
      return scalar.notNull();
    }

    @Override
    public void eval(Object scalar, Object value) {
      if (emitNulls) super.eval(scalar, value);
      else if (value != null) {
        collect(Row.of(value, scalar, 0));
        if (scalar instanceof java.util.Date) ((java.util.Date) scalar).setTime(42);
      }
    }
  }

  public static class AtomicRows extends TableFunction<Object> {
    private final String bridge;
    private final boolean notNull;

    public AtomicRows(String bridge) {
      this(bridge, false);
    }

    public AtomicRows(String bridge, boolean notNull) {
      this.bridge = bridge;
      this.notNull = notNull;
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory factory) {
      var scalar =
          FlinkMixedPrimitiveTableFunctionSqlHarnessTest.scalarType(kind(bridge))
              .bridgedTo(conversion(bridge));
      if (notNull) scalar = scalar.notNull();
      return TypeInference.newBuilder()
          .typedArguments(
              scalar, FlinkMixedPrimitiveTableFunctionSqlHarnessTest.containerType("array"))
          .outputTypeStrategy(TypeStrategies.explicit(scalar))
          .build();
    }

    public void eval(Object scalar, Object value) {
      if (value == null) return;
      collect(scalar);
      if (scalar instanceof java.util.Date) ((java.util.Date) scalar).setTime(42);
      if (!notNull) collect(null);
    }
  }

  public static class FailingRows extends TemporalRows {
    public FailingRows(String bridge) {
      super(bridge, "array", true, false);
    }

    @Override
    public void eval(Object scalar, Object value) {
      if (scalar == null) throw new IllegalStateException("temporal bridge sentinel");
      super.eval(scalar, value);
    }
  }
}
