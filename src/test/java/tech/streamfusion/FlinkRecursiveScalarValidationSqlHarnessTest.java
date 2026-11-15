package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.inference.TypeStrategies;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class FlinkRecursiveScalarValidationSqlHarnessTest {
  @ParameterizedTest
  @ValueSource(strings = {"row/linked", "list/custom", "map/linked"})
  void nestedConcreteListArgumentsKeepReleasedValidation(String shape) {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var runs =
          NativeFailureParity.run(
              () -> {
                var table =
                    FlinkRecursiveScalarSqlHarnessTest.environment(
                        shape, "default-INT", "result", 8, metrics);
                table.createTemporarySystemFunction("invalid_scalar", new ConcreteArgument(shape));
                return table;
              },
              "SELECT invalid_scalar(id,p) FROM src");
      runs.assertFailure(
          ValidationException.class,
          "does not support an output conversion",
          NativeFailureParity.Phase.PLANNING,
          NativeFailureParity.Route.FALLBACK);
      assertEquals(runs.host().rootCause().getMessage(), runs.nativeRun().rootCause().getMessage());
      assertEquals(0, runs.nativeRun().substitutions());
      assertTrue(metrics.nativeOperators().isEmpty());
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"array", "map", "row"})
  void internalNestedContainersRetainExplicitFallback(String kind) {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var runs =
          NativeFailureParity.run(
              () -> {
                var table =
                    FlinkRecursiveScalarSqlHarnessTest.environment(
                        "list/row", "default-INT", "both", 8, metrics);
                table.createTemporarySystemFunction("internal_scalar", new InternalResult(kind));
                return table;
              },
              "SELECT internal_scalar(id) FROM src");
      runs.assertSuccess(NativeFailureParity.Route.FALLBACK);
      assertTrue(
          runs.nativeRun().fallbackReasons().stream()
              .anyMatch(reason -> reason.contains("unverified Java conversions")),
          runs::toString);
      assertTrue(metrics.nativeOperators().isEmpty());
    }
  }

  @Test
  void customListResultKeepsReleasedToArrayFailure() {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var runs =
          NativeFailureParity.run(
              () -> {
                var table =
                    FlinkRecursiveScalarSqlHarnessTest.environment(
                        "row/list", "default-INT", "both", 8, metrics);
                table.createTemporarySystemFunction("failed_scalar", new FailedResult());
                return table;
              },
              "SELECT failed_scalar(id) FROM src");
      runs.assertFailure(
          IllegalStateException.class,
          "recursive scalar toArray sentinel",
          NativeFailureParity.Phase.ROW_EVALUATION,
          NativeFailureParity.Route.NATIVE);
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"array", "row/array"})
  void primitiveTemporalResultsRetainTheirUnverifiedConsumerFallback(String shape) {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var runs =
          NativeFailureParity.run(
              () ->
                  FlinkRecursiveScalarSqlHarnessTest.environment(
                      shape, "date-int", "result", 16, metrics),
              "SELECT recursive_scalar(id,p),recursive_scalar(id,p) FROM src");
      runs.assertSuccess(NativeFailureParity.Route.FALLBACK);
      assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
      assertTrue(metrics.nativeOperators().isEmpty());
    }
  }

  public static class ConcreteArgument extends ScalarFunction {
    private final String shape;

    public ConcreteArgument(String shape) {
      this.shape = shape;
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory factory) {
      var concrete =
          FlinkRecursiveTableFunctionSqlHarnessTest.container(shape, DataTypes.INT(), true);
      return TypeInference.newBuilder()
          .typedArguments(DataTypes.INT().notNull(), concrete)
          .outputTypeStrategy(
              TypeStrategies.explicit(FlinkRecursiveTableFunctionSqlHarnessTest.defaults(concrete)))
          .build();
    }

    public Object eval(Integer id, Object value) {
      return value;
    }
  }

  public static class InternalResult extends ScalarFunction {
    private final String kind;

    public InternalResult(String kind) {
      this.kind = kind;
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory factory) {
      DataType child;
      switch (kind) {
        case "array":
          child = DataTypes.ARRAY(DataTypes.INT()).bridgedTo(ArrayData.class);
          break;
        case "map":
          child = DataTypes.MAP(DataTypes.INT(), DataTypes.INT()).bridgedTo(MapData.class);
          break;
        default:
          child = DataTypes.ROW(DataTypes.FIELD("q", DataTypes.INT())).bridgedTo(RowData.class);
      }
      return TypeInference.newBuilder()
          .typedArguments(DataTypes.INT().notNull())
          .outputTypeStrategy(TypeStrategies.explicit(DataTypes.ROW(DataTypes.FIELD("v", child))))
          .build();
    }

    public Row eval(Integer id) {
      switch (kind) {
        case "array":
          return Row.of(new GenericArrayData(new Integer[] {id, null}));
        case "map":
          return Row.of(new GenericMapData(java.util.Collections.singletonMap(id, id)));
        default:
          return Row.of(GenericRowData.of(id));
      }
    }
  }

  public static class FailedResult extends ScalarFunction {
    @Override
    public TypeInference getTypeInference(DataTypeFactory factory) {
      return TypeInference.newBuilder()
          .typedArguments(DataTypes.INT().notNull())
          .outputTypeStrategy(
              TypeStrategies.explicit(
                  DataTypes.ROW(
                      DataTypes.FIELD(
                          "v", DataTypes.ARRAY(DataTypes.INT()).bridgedTo(ThrowingList.class)))))
          .build();
    }

    public Row eval(Integer id) {
      return Row.of(new ThrowingList<>());
    }
  }

  public static class ThrowingList<E> extends ArrayList<E> {
    @Override
    public <T> T[] toArray(T[] array) {
      throw new IllegalStateException("recursive scalar toArray sentinel");
    }
  }
}
