package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import java.util.stream.Stream;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.data.conversion.DataStructureConverter;
import org.apache.flink.table.data.conversion.DataStructureConverters;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.runtime.typeutils.ExternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.inference.TypeStrategies;
import org.apache.flink.types.Row;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class FlinkRecursiveScalarSqlHarnessTest {
  static Stream<Arguments> signatures() {
    String selected = System.getProperty("recursive.scalar.signatures");
    if (selected != null)
      return Stream.of(selected.split(";")).map(s -> Arguments.of((Object[]) s.split(":")));
    var rowArrays =
        FlinkRecursiveTableFunctionSqlHarnessTest.defaultScalars()
            .flatMap(
                kind ->
                    Stream.of("array/row", "array/map", "array/array/row", "row/map")
                        .map(shape -> Arguments.of(shape, "default-" + kind, "both")));
    var rowMaps =
        FlinkNestedBridgeTableFunctionSqlHarnessTest.bridges()
            .flatMap(
                bridge ->
                    Stream.of("row", "map", "row/map", "map/row", "key")
                        .flatMap(
                            shape ->
                                Stream.of("argument", "result", "both")
                                    .map(position -> Arguments.of(shape, bridge, position))));
    return Stream.of(FlinkRecursiveTableFunctionSqlHarnessTest.signatures(), rowArrays, rowMaps)
        .flatMap(signatures -> signatures);
  }

  @ParameterizedTest(name = "{0} {1} {2}")
  @MethodSource("signatures")
  void releasedConversionsKeepTypesNullsKindsAndOwnedResults(
      String shape, String bridge, String position) {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var runs =
          NativeFailureParity.run(
              () -> environment(shape, bridge, position, 48, metrics),
              "SELECT id,p,recursive_scalar(id,p) AS v FROM src");
      if ((shape.equals("array") || shape.equals("map/array"))
          && List.of("time-primitive-long", "ltz-int", "ltz-primitive-long").contains(bridge)) {
        runs.assertFailure(
            ClassCastException.class,
            "cannot be cast",
            NativeFailureParity.Phase.ROW_EVALUATION,
            NativeFailureParity.Route.FALLBACK);
        assertEquals(
            runs.host().rootCause().getMessage(), runs.nativeRun().rootCause().getMessage());
        assertTrue(metrics.nativeOperators().isEmpty());
        return;
      }
      boolean temporalResult =
          !position.equals("argument")
              && List.of("date-int", "time-int", "yearmonth-int", "daytime-primitive-long")
                  .contains(bridge)
              && List.of("array", "row/array").contains(shape);
      boolean fallback = Boolean.getBoolean("recursive.scalar.expectFallback") || temporalResult;
      runs.assertSuccess(
          fallback ? NativeFailureParity.Route.FALLBACK : NativeFailureParity.Route.NATIVE);
      assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
      assertEquals(
          NativeParity.multiset(
              FlinkNestedBridgeTableFunctionSqlHarnessTest.rawRows(runs.host().rows())),
          NativeParity.multiset(
              FlinkNestedBridgeTableFunctionSqlHarnessTest.rawRows(runs.nativeRun().rows())));
      assertFalse(runs.host().rows().isEmpty());
      if (!fallback)
        RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeCalc");
    }
  }

  static TableEnvironment environment(
      String shape, String bridge, String position, int count, RecoveryExecutionMetrics metrics) {
    var env = metrics.environment(new Configuration(), 1, 0);
    env.getConfig().enableObjectReuse();
    var table = StreamTableEnvironment.create(env);
    var type = FlinkRecursiveTableFunctionSqlHarnessTest.sourceType(shape, bridge);
    var input =
        env.fromSequence(0, Math.max(0, count - 1))
            .filter(i -> i < count)
            .map(
                i ->
                    Row.of(
                        i.intValue(),
                        FlinkRecursiveTableFunctionSqlHarnessTest.input(
                                shape, bridge, (int) (i % 48))
                            .getField(1)))
            .returns(ExternalTypeInfo.<Row>of(type));
    table.createTemporaryView(
        "src", table.fromDataStream(input, Schema.newBuilder().fromRowDataType(type).build()));
    return register(table, shape, bridge, position);
  }

  static TableEnvironment register(
      TableEnvironment table, String shape, String bridge, String position) {
    table.getConfig().set("streamfusion.transpose.batchRows", "7");
    table.getConfig().setLocalTimeZone(java.time.ZoneId.of("America/New_York"));
    table.createTemporarySystemFunction(
        "recursive_scalar", new RecursiveScalar(shape, bridge, position));
    return table;
  }

  public static class RecursiveScalar extends ScalarFunction {
    private final String shape;
    private final String bridge;
    private final String position;
    private transient DataType argumentType;
    private transient DataType resultType;
    private transient DataStructureConverter<Object, Object> arguments;
    private transient DataStructureConverter<Object, Object> defaults;
    private transient FlinkRecursiveTableFunctionSqlHarnessTest.ResultValues results;
    private transient Object previous;
    private transient boolean opened;

    public RecursiveScalar(String shape, String bridge, String position) {
      this.shape = shape;
      this.bridge = bridge;
      this.position = position;
    }

    DataType argumentType() {
      var type =
          FlinkRecursiveTableFunctionSqlHarnessTest.container(
              shape, FlinkRecursiveTableFunctionSqlHarnessTest.bridgeLeaf(bridge), false);
      return position.equals("result")
          ? FlinkRecursiveTableFunctionSqlHarnessTest.defaults(type)
          : type;
    }

    DataType resultType() {
      var type =
          FlinkRecursiveTableFunctionSqlHarnessTest.container(
              shape, FlinkRecursiveTableFunctionSqlHarnessTest.bridgeLeaf(bridge), true);
      return position.equals("argument")
          ? FlinkRecursiveTableFunctionSqlHarnessTest.defaults(type)
          : type;
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory factory) {
      return TypeInference.newBuilder()
          .typedArguments(DataTypes.INT().notNull(), argumentType())
          .outputTypeStrategy(TypeStrategies.explicit(resultType()))
          .build();
    }

    @Override
    public void open(FunctionContext context) {
      if (opened) throw new IllegalStateException("recursive scalar opened twice");
      opened = true;
      argumentType = argumentType();
      resultType = resultType();
      if (!argumentType.equals(resultType)) {
        arguments = DataStructureConverters.getConverter(argumentType);
        defaults =
            DataStructureConverters.getConverter(
                FlinkRecursiveTableFunctionSqlHarnessTest.defaults(resultType));
        arguments.open(context.getUserCodeClassLoader());
        defaults.open(context.getUserCodeClassLoader());
        results =
            new FlinkRecursiveTableFunctionSqlHarnessTest.ResultValues(
                resultType, context.getUserCodeClassLoader());
      }
    }

    public Object eval(Integer id, Object payload) {
      if (!opened) throw new IllegalStateException("recursive scalar not open");
      FlinkRecursiveTableFunctionSqlHarnessTest.invalidate(previous);
      FlinkRecursiveTableFunctionSqlHarnessTest.assertClasses(argumentType, payload);
      Object result =
          arguments == null
              ? payload
              : results.convert(defaults.toExternalOrNull(arguments.toInternalOrNull(payload)));
      FlinkRecursiveTableFunctionSqlHarnessTest.assertClasses(resultType, result);
      previous = result;
      return result;
    }

    @Override
    public void close() {
      if (!opened) throw new IllegalStateException("recursive scalar closed twice");
      opened = false;
      previous = null;
    }

    @Override
    public boolean isDeterministic() {
      return false;
    }
  }
}
