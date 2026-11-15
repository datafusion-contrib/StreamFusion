package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.util.stream.Stream;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.runtime.typeutils.ExternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.inference.TypeStrategies;
import org.apache.flink.types.Row;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class FlinkMixedRecursiveScalarSqlHarnessTest {
  static Stream<Arguments> signatures() {
    String selected = System.getProperty("recursive.scalar.mixed");
    if (selected != null)
      return Stream.of(selected.split(";")).map(s -> Arguments.of((Object[]) s.split(":")));
    return Stream.concat(
            FlinkNestedBridgeTableFunctionSqlHarnessTest.bridges(),
            FlinkRecursiveTableFunctionSqlHarnessTest.defaultScalars()
                .map(kind -> "default-" + kind))
        .flatMap(
            bridge ->
                Stream.of("array", "list", "row", "map").map(shape -> Arguments.of(shape, bridge)));
  }

  @ParameterizedTest(name = "{0} {1}")
  @MethodSource("signatures")
  void scalarPrimitiveAndTemporalBridgesComposeWithRecursiveArguments(String shape, String bridge) {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var runs =
          NativeFailureParity.run(
              () -> {
                var env = metrics.environment(new Configuration(), 1, 0);
                env.getConfig().enableObjectReuse();
                var table = StreamTableEnvironment.create(env);
                table.getConfig().set("streamfusion.transpose.batchRows", "7");
                table.getConfig().setLocalTimeZone(java.time.ZoneId.of("America/New_York"));
                var scalar = FlinkRecursiveTableFunctionSqlHarnessTest.bridgeLeaf(bridge);
                var nested =
                    FlinkRecursiveTableFunctionSqlHarnessTest.sourceType(shape, bridge)
                        .getChildren()
                        .get(1);
                var type =
                    DataTypes.ROW(
                        DataTypes.FIELD("id", DataTypes.INT().notNull()),
                        DataTypes.FIELD(
                            "x", FlinkRecursiveTableFunctionSqlHarnessTest.defaults(scalar)),
                        DataTypes.FIELD("p", nested));
                var input =
                    env.fromSequence(0, 47)
                        .map(
                            i -> {
                              Object x =
                                  scalar.getLogicalType().isNullable() && i % 11 == 10
                                      ? null
                                      : FlinkMixedPrimitiveTableFunctionSqlHarnessTest.scalarValue(
                                          bridge.startsWith("default-")
                                              ? bridge.substring(8)
                                              : FlinkNestedBridgeTableFunctionSqlHarnessTest.kind(
                                                  bridge),
                                          i.intValue());
                              return Row.of(
                                  i.intValue(),
                                  x,
                                  FlinkRecursiveTableFunctionSqlHarnessTest.input(
                                          shape, bridge, i.intValue())
                                      .getField(1));
                            })
                        .returns(ExternalTypeInfo.<Row>of(type));
                table.createTemporaryView(
                    "src",
                    table.fromDataStream(input, Schema.newBuilder().fromRowDataType(type).build()));
                table.createTemporarySystemFunction("mixed_scalar", new MixedScalar(shape, bridge));
                return table;
              },
              "SELECT id,mixed_scalar(x,p) FROM src");
      runs.assertSuccess(NativeFailureParity.Route.NATIVE);
      assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
      RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeCalc");
    }
  }

  public static class MixedScalar extends ScalarFunction {
    private final String shape;
    private final String bridge;

    public MixedScalar(String shape, String bridge) {
      this.shape = shape;
      this.bridge = bridge;
    }

    private DataType scalar() {
      return FlinkRecursiveTableFunctionSqlHarnessTest.bridgeLeaf(bridge);
    }

    private DataType nested() {
      return FlinkRecursiveTableFunctionSqlHarnessTest.container(
          shape, FlinkRecursiveTableFunctionSqlHarnessTest.defaults(scalar()), false);
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory factory) {
      return TypeInference.newBuilder()
          .typedArguments(scalar(), nested())
          .outputTypeStrategy(TypeStrategies.explicit(scalar()))
          .build();
    }

    public Object eval(Object scalar, Object payload) {
      FlinkRecursiveTableFunctionSqlHarnessTest.assertClasses(scalar(), scalar);
      FlinkRecursiveTableFunctionSqlHarnessTest.assertClasses(nested(), payload);
      return scalar;
    }
  }
}
