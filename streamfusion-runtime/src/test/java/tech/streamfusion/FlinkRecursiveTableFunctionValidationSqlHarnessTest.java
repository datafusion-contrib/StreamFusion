package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.inference.TypeStrategies;
import org.apache.flink.types.Row;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class FlinkRecursiveTableFunctionValidationSqlHarnessTest {
  @ParameterizedTest
  @ValueSource(strings = {"row/linked", "list/custom", "map/linked"})
  void nestedConcreteListArgumentsKeepReleasedValidation(String shape) {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var runs =
          NativeFailureParity.run(
              () -> {
                var table =
                    FlinkRecursiveTableFunctionSqlHarnessTest.environment(
                        shape, "default-INT", "result", 8, metrics);
                table.createTemporarySystemFunction("invalid_rows", new ConcreteArgument(shape));
                return table;
              },
              "SELECT t.v FROM src s,LATERAL TABLE(invalid_rows(s.id,s.p)) AS t(v)");
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
  @ValueSource(strings = {"array", "map"})
  void internalNestedContainersRetainFallback(String kind) {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var runs =
          NativeFailureParity.run(
              () -> {
                var table =
                    FlinkRecursiveTableFunctionSqlHarnessTest.environment(
                        "list/row", "default-INT", "both", 8, metrics);
                table.createTemporarySystemFunction("internal_rows", new InternalRows(kind));
                return table;
              },
              "SELECT t.v FROM src s,LATERAL TABLE(internal_rows(s.id)) AS t(v)");
      runs.assertSuccess(NativeFailureParity.Route.FALLBACK);
      assertTrue(
          runs.nativeRun().fallbackReasons().stream()
              .anyMatch(reason -> reason.contains("verified external Java conversions")));
      assertTrue(metrics.nativeOperators().isEmpty());
    }
  }

  @org.junit.jupiter.api.Test
  void nestedCustomToArrayFailureKeepsReleasedConversionOrder() {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var runs =
          NativeFailureParity.run(
              () -> {
                var table =
                    FlinkRecursiveTableFunctionSqlHarnessTest.environment(
                        "row/list", "default-INT", "both", 8, metrics);
                table.createTemporarySystemFunction("failed_rows", new FailedRows());
                return table;
              },
              "SELECT t.v FROM src s,LATERAL TABLE(failed_rows(s.id)) AS t(v)");
      runs.assertFailure(
          IllegalStateException.class,
          "nested toArray sentinel",
          NativeFailureParity.Phase.ROW_EVALUATION,
          NativeFailureParity.Route.NATIVE);
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"int", "date"})
  void projectionsRetainReleasedPrimitiveDefaultsAndTemporalCopyFailures(String kind) {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var runs =
          NativeFailureParity.run(
              () -> {
                var table =
                    FlinkRecursiveTableFunctionSqlHarnessTest.environment(
                        "row/list", "default-INT", "both", 8, metrics);
                table.createTemporarySystemFunction("null_rows", new NullPrimitiveRows(kind));
                return table;
              },
              "SELECT t.m,t.v FROM src s,LATERAL TABLE(null_rows(s.id)) AS t(v,m)");
      if (kind.equals("date")) {
        runs.assertFailure(
            RuntimeException.class,
            "Unknown type: DATE NOT NULL",
            NativeFailureParity.Phase.ROW_EVALUATION,
            NativeFailureParity.Route.FALLBACK);
        assertTrue(metrics.nativeOperators().isEmpty());
        return;
      }
      runs.assertSuccess(NativeFailureParity.Route.NATIVE);
      assertTrue(
          runs.host().rows().stream().allMatch(row -> row.get(1).equals(-1)), runs::toString);
      RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeTableFunction");
    }
  }

  public static class NullPrimitiveRows extends TableFunction<Row> {
    private final String kind;

    public NullPrimitiveRows(String kind) {
      this.kind = kind;
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory factory) {
      return TypeInference.newBuilder()
          .typedArguments(DataTypes.INT().notNull())
          .outputTypeStrategy(
              TypeStrategies.explicit(
                  DataTypes.ROW(
                      DataTypes.FIELD(
                          "v",
                          DataTypes.ARRAY(
                                  (kind.equals("date") ? DataTypes.DATE() : DataTypes.INT())
                                      .notNull()
                                      .bridgedTo(int.class))
                              .bridgedTo(int[].class)),
                      DataTypes.FIELD("m", DataTypes.INT().notNull().bridgedTo(int.class)))))
          .build();
    }

    public void eval(Integer id) {
      collect(Row.of(new int[] {id}, null));
    }
  }

  public static class ConcreteArgument extends TableFunction<Integer> {
    private final String shape;

    public ConcreteArgument(String shape) {
      this.shape = shape;
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory factory) {
      return TypeInference.newBuilder()
          .typedArguments(
              DataTypes.INT().notNull(),
              FlinkRecursiveTableFunctionSqlHarnessTest.container(shape, DataTypes.INT(), true))
          .outputTypeStrategy(TypeStrategies.explicit(DataTypes.INT()))
          .build();
    }

    public void eval(Integer id, Object payload) {
      collect(id);
    }
  }

  public static class InternalRows extends TableFunction<Row> {
    private final String kind;

    public InternalRows(String kind) {
      this.kind = kind;
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory factory) {
      DataType type =
          kind.equals("array")
              ? DataTypes.ARRAY(DataTypes.INT()).bridgedTo(ArrayData.class)
              : DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()).bridgedTo(MapData.class);
      return TypeInference.newBuilder()
          .typedArguments(DataTypes.INT().notNull())
          .outputTypeStrategy(
              TypeStrategies.explicit(
                  DataTypes.ROW(DataTypes.FIELD("v", DataTypes.ARRAY(type).bridgedTo(List.class)))))
          .build();
    }

    public void eval(Integer id) {
      collect(
          Row.of(
              List.of(
                  kind.equals("array")
                      ? new GenericArrayData(new Integer[] {id, null})
                      : new GenericMapData(
                          java.util.Map.of(
                              org.apache.flink.table.data.StringData.fromString("k"), id)))));
    }
  }

  public static class FailedRows extends TableFunction<Row> {
    @Override
    public TypeInference getTypeInference(DataTypeFactory factory) {
      return TypeInference.newBuilder()
          .typedArguments(DataTypes.INT().notNull())
          .outputTypeStrategy(
              TypeStrategies.explicit(
                  DataTypes.ROW(
                      DataTypes.FIELD(
                          "v",
                          DataTypes.ARRAY(
                                  DataTypes.ARRAY(DataTypes.INT()).bridgedTo(FailedList.class))
                              .bridgedTo(List.class)))))
          .build();
    }

    public void eval(Integer id) {
      FailedList list = new FailedList();
      list.add(id);
      collect(Row.of(List.of(list)));
    }
  }

  public static class FailedList extends ArrayList<Integer> {
    @Override
    public <T> T[] toArray(T[] target) {
      throw new IllegalStateException("nested toArray sentinel");
    }
  }
}
