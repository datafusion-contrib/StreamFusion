package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.runtime.typeutils.ExternalTypeInfo;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.inference.TypeStrategies;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class FlinkRecursiveScalarBoundarySqlHarnessTest {
  static Stream<Arguments> samples() {
    return Stream.of(
        Arguments.of("list/row", "default-STRING", "both"),
        Arguments.of("row/primitive", "primitive-FLOAT", "both"),
        Arguments.of("array/row", "default-DOUBLE", "both"),
        Arguments.of("map/list", "timestamp-sql", "both"),
        Arguments.of("array/list", "ltz-long", "both"),
        Arguments.of("row/custom/map", "default-DECIMAL(38,18)", "result"));
  }

  @ParameterizedTest(name = "{0} {1} {2}")
  @MethodSource("samples")
  void sharedCallsRetainedArgumentsAndNestedCallsKeepReleasedOwnership(
      String shape, String bridge, String position) {
    for (String mode : List.of("shared", "retained", "nested", "short-circuit", "filtered")) {
      try (var metrics = new RecoveryExecutionMetrics()) {
        String sql;
        switch (mode) {
          case "shared":
            sql = "SELECT id,recursive_scalar(id,p),recursive_scalar(id,p) FROM src";
            break;
          case "retained":
            sql = "SELECT id,previous_scalar(id,p) FROM src";
            break;
          case "nested":
            // Distinct conversions compose through the resolved logical type.
            sql = "SELECT id,recursive_scalar(id,recursive_scalar(id,p)) FROM src";
            break;
          case "short-circuit":
            sql =
                "SELECT id,CASE WHEN id=3 THEN CAST(NULL AS "
                    + FlinkRecursiveTableFunctionSqlHarnessTest.sourceType(shape, bridge)
                        .getChildren()
                        .get(1)
                        .getLogicalType()
                        .asSerializableString()
                    + ") ELSE failing_scalar(id,p) END FROM src";
            break;
          default:
            sql = "SELECT failing_scalar(id,p) FROM src WHERE id<0";
        }
        var runs =
            NativeFailureParity.run(
                () -> {
                  var table =
                      FlinkRecursiveScalarSqlHarnessTest.environment(
                          shape, bridge, position, 193, metrics);
                  table.createTemporarySystemFunction(
                      "previous_scalar", new PreviousScalar(shape, bridge, position));
                  table.createTemporarySystemFunction(
                      "failing_scalar", new FailingScalar(shape, bridge, position, "eval"));
                  return table;
                },
                sql);
        runs.assertSuccess(NativeFailureParity.Route.NATIVE);
        assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
        RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeCalc");
      }
    }
  }

  static Stream<Arguments> floatingSamples() {
    return Stream.of("row/primitive", "list/map", "map/list", "key/primitive")
        .flatMap(shape -> Stream.of("FLOAT", "DOUBLE").map(kind -> Arguments.of(shape, kind)));
  }

  @ParameterizedTest(name = "{0} {1}")
  @MethodSource("floatingSamples")
  void aSecondScalarObservesEveryFloatingBitInsideContainersAndMapKeys(String shape, String kind) {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var runs =
          NativeFailureParity.run(
              () -> {
                var table =
                    FlinkRecursiveScalarSqlHarnessTest.environment(
                        shape, "primitive-" + kind, "both", 193, metrics);
                table.createTemporarySystemFunction("bits_scalar", new BitsScalar(shape, kind));
                return table;
              },
              "SELECT id,bits_scalar(recursive_scalar(id,p)) FROM src");
      runs.assertSuccess(NativeFailureParity.Route.NATIVE);
      assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
      assertTrue(
          runs.host().rows().stream()
              .map(row -> row.get(2))
              .filter(value -> value instanceof List)
              .flatMap(value -> ((List<?>) value).stream())
              .anyMatch(
                  bit ->
                      kind.equals("FLOAT")
                          ? Float.isNaN(Float.intBitsToFloat(((Number) bit).intValue()))
                          : Double.isNaN(Double.longBitsToDouble(((Number) bit).longValue()))),
          runs::toString);
      RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeCalc");
    }
  }

  @ParameterizedTest(name = "{0} {1} {2}")
  @MethodSource("samples")
  void emptyInputAndFailuresKeepReleasedLifecycleAndInvocationOrder(
      String shape, String bridge, String position) {
    for (String failure : List.of("empty", "before", "after", "open")) {
      try (var metrics = new RecoveryExecutionMetrics()) {
        var runs =
            NativeFailureParity.run(
                () -> {
                  var table =
                      FlinkRecursiveScalarSqlHarnessTest.environment(
                          shape, bridge, position, failure.equals("empty") ? 0 : 16, metrics);
                  table.dropTemporarySystemFunction("recursive_scalar");
                  table.createTemporarySystemFunction(
                      "recursive_scalar", new FailingScalar(shape, bridge, position, failure));
                  return table;
                },
                "SELECT id,recursive_scalar(id,p) FROM src");
        if (failure.equals("empty")) runs.assertSuccess(NativeFailureParity.Route.NATIVE);
        else
          runs.assertFailure(
              IllegalStateException.class,
              failure.equals("open") ? "recursive-scalar-open" : "recursive-scalar-calls=4",
              failure.equals("open")
                  ? NativeFailureParity.Phase.INITIALIZATION
                  : NativeFailureParity.Phase.ROW_EVALUATION,
              NativeFailureParity.Route.NATIVE);
      }
    }
  }

  @ParameterizedTest(name = "{0} {1} {2}")
  @MethodSource("samples")
  void allChangelogKindsSurviveTwoRestoresWithBothStateBackends(
      String shape, String bridge, String position) throws Exception {
    var type = FlinkRecursiveTableFunctionSqlHarnessTest.sourceType(shape, bridge);
    var rows = new ArrayList<Row>();
    for (int i = 0; i < 32; i++)
      rows.add(FlinkRecursiveTableFunctionSqlHarnessTest.input(shape, bridge, i));
    for (int i = 0; i < 8; i++)
      for (var kind : List.of(RowKind.UPDATE_BEFORE, RowKind.UPDATE_AFTER, RowKind.DELETE)) {
        var row =
            FlinkRecursiveTableFunctionSqlHarnessTest.input(
                shape, bridge, kind == RowKind.UPDATE_BEFORE ? i : i + 34);
        row.setField(0, i);
        row.setKind(kind);
        rows.add(row);
      }
    for (String backend :
        List.of("hashmap", "tech.streamfusion.state.RocksDBNativeStateBackendFactory")) {
      try (var recovery =
          new PortableSqlRecovery(
                  backend,
                  rows,
                  ExternalTypeInfo.<Row>of(type),
                  Schema.newBuilder().fromRowDataType(type).build(),
                  true,
                  16,
                  40)
              .withExecutionMetrics()) {
        var runs =
            NativeFailureParity.runRecovery(
                () ->
                    FlinkRecursiveScalarSqlHarnessTest.register(
                        recovery.uninterrupted(), shape, bridge, position),
                () ->
                    FlinkRecursiveScalarSqlHarnessTest.register(
                        recovery.get(), shape, bridge, position),
                "SELECT id,recursive_scalar(id,p) FROM recovery_input");
        runs.assertSuccess(NativeFailureParity.Route.NATIVE);
        assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
        recovery.verifyRepeatedRecovery();
        RecoveryExecutionMetrics.assertConsumed(recovery.nativeExecution(), "NativeCalc");
        SharedFlinkCluster.assertNativeMemoryReleased("recursive-scalar-recovery");
      }
    }
  }

  public static class PreviousScalar extends FlinkRecursiveScalarSqlHarnessTest.RecursiveScalar {
    private transient Object retained;

    public PreviousScalar(String shape, String bridge, String position) {
      super(shape, bridge, position);
    }

    @Override
    public Object eval(Integer id, Object payload) {
      Object previous = retained;
      retained = payload;
      return super.eval(id, previous);
    }
  }

  public static class FailingScalar extends FlinkRecursiveScalarSqlHarnessTest.RecursiveScalar {
    private final String failure;
    private transient int calls;

    public FailingScalar(String shape, String bridge, String position, String failure) {
      super(shape, bridge, position);
      this.failure = failure;
    }

    @Override
    public void open(FunctionContext context) {
      super.open(context);
      if (failure.equals("open")) throw new IllegalStateException("recursive-scalar-open");
    }

    @Override
    public Object eval(Integer id, Object payload) {
      calls++;
      Object result = failure.equals("after") ? super.eval(id, payload) : null;
      if (id == 3) throw new IllegalStateException("recursive-scalar-calls=" + calls);
      return failure.equals("after") ? result : super.eval(id, payload);
    }
  }

  public static class BitsScalar extends ScalarFunction {
    private final String shape;
    private final String kind;

    public BitsScalar(String shape, String kind) {
      this.shape = shape;
      this.kind = kind;
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory factory) {
      return TypeInference.newBuilder()
          .typedArguments(
              FlinkRecursiveTableFunctionSqlHarnessTest.container(
                  shape,
                  FlinkRecursiveTableFunctionSqlHarnessTest.bridgeLeaf("primitive-" + kind),
                  false))
          .outputTypeStrategy(
              TypeStrategies.explicit(
                  DataTypes.ARRAY(DataTypes.BIGINT().notNull().bridgedTo(long.class))
                      .bridgedTo(long[].class)))
          .build();
    }

    public long[] eval(Object payload) {
      var bits = new ArrayList<Long>();
      observe(payload, bits);
      return bits.stream().sorted().mapToLong(Long::longValue).toArray();
    }

    private void observe(Object value, List<Long> bits) {
      if (value instanceof Float) bits.add((long) Float.floatToRawIntBits((Float) value));
      else if (value instanceof Double) bits.add(Double.doubleToRawLongBits((Double) value));
      else if (value instanceof Row) observe(((Row) value).getField(0), bits);
      else if (value instanceof Map)
        ((Map<?, ?>) value)
            .forEach(
                (k, v) -> {
                  observe(k, bits);
                  observe(v, bits);
                });
      else if (value instanceof List) for (Object child : (List<?>) value) observe(child, bits);
      else if (value != null && value.getClass().isArray())
        for (int i = 0; i < Array.getLength(value); i++) observe(Array.get(value, i), bits);
    }
  }
}
