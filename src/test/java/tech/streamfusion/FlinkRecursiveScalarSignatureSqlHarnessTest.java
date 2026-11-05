package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.inference.TypeStrategies;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class FlinkRecursiveScalarSignatureSqlHarnessTest {
  @ParameterizedTest
  @ValueSource(strings = {"primitive", "varargs", "overloaded", "list-row", "list-argument"})
  void releasedGenerationSelectsConcreteJavaSignatures(String mode) {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var runs =
          NativeFailureParity.run(
              () -> {
                var table =
                    FlinkRecursiveScalarSqlHarnessTest.environment(
                        mode.equals("list-argument") ? "array/row/primitive" : "array",
                        "primitive-INT",
                        "both",
                        48,
                        metrics);
                ScalarFunction function;
                switch (mode) {
                  case "primitive":
                    function = new PrimitiveIdentity();
                    break;
                  case "varargs":
                    function = new PrimitiveVarargs();
                    break;
                  case "overloaded":
                    function = new Overloaded();
                    break;
                  case "list-argument":
                    function = new ListArgument();
                    break;
                  default:
                    function = new ListRows();
                }
                table.createTemporarySystemFunction("typed_scalar", function);
                return table;
              },
              mode.equals("list-row")
                  ? "SELECT id,typed_scalar(id) FROM src"
                  : mode.equals("varargs")
                      ? "SELECT id,typed_scalar(id,p,p) FROM src"
                      : "SELECT id,typed_scalar(id,p) FROM src");
      runs.assertSuccess(NativeFailureParity.Route.NATIVE);
      assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
      RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeCalc");
    }
  }

  @Test
  void actualNullElementsKeepReleasedPrimitiveArgumentFailure() {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var runs =
          NativeFailureParity.run(
              () -> {
                var table =
                    FlinkRecursiveScalarSqlHarnessTest.environment(
                        "array", "primitive-INT", "both", 16, metrics);
                table.createTemporarySystemFunction("null_elements", new NullElements());
                table.createTemporarySystemFunction("typed_scalar", new PrimitiveIdentity());
                return table;
              },
              "SELECT typed_scalar(id,null_elements(id)) FROM src");
      runs.assertFailure(
          RuntimeException.class,
          "Primitive array must not contain a null value.",
          NativeFailureParity.Phase.ROW_EVALUATION,
          NativeFailureParity.Route.NATIVE);
      assertEquals(runs.host().rootCause().getMessage(), runs.nativeRun().rootCause().getMessage());
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"list/row", "array/row", "array/list"})
  void untypedNullCaseKeepsReleasedValidation(String shape) {
    try (var metrics = new RecoveryExecutionMetrics()) {
      String bridge = shape.equals("array/list") ? "ltz-long" : "default-STRING";
      var runs =
          NativeFailureParity.run(
              () ->
                  FlinkRecursiveScalarSqlHarnessTest.environment(
                      shape, bridge, "both", 16, metrics),
              "SELECT id,CASE WHEN id=3 THEN NULL ELSE recursive_scalar(id,p) END FROM src");
      if (org.apache.flink.runtime.util.EnvironmentInformation.getVersion().startsWith("1.18")) {
        runs.assertFailure(
            org.apache.calcite.sql.validate.SqlValidatorException.class,
            "Illegal mixing of types in CASE or COALESCE statement",
            NativeFailureParity.Phase.PLANNING,
            NativeFailureParity.Route.UNPLANNED);
        assertEquals(
            runs.host().rootCause().getMessage(), runs.nativeRun().rootCause().getMessage());
      } else {
        runs.assertSuccess(NativeFailureParity.Route.NATIVE);
        RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeCalc");
      }
    }
  }

  static DataType primitiveArray() {
    return DataTypes.ARRAY(DataTypes.INT().notNull()).bridgedTo(int[].class);
  }

  public static class PrimitiveIdentity extends ScalarFunction {
    public int[] eval(Integer id, int[] values) {
      return values;
    }
  }

  public static class PrimitiveVarargs extends ScalarFunction {
    @Override
    public TypeInference getTypeInference(DataTypeFactory factory) {
      return TypeInference.newBuilder()
          .typedArguments(DataTypes.INT().notNull(), primitiveArray(), primitiveArray())
          .outputTypeStrategy(TypeStrategies.explicit(primitiveArray()))
          .build();
    }

    public int[] eval(Integer id, int[]... values) {
      return new int[] {id, values.length, values[0] == null ? -1 : values[0].length};
    }
  }

  public static class Overloaded extends ScalarFunction {
    @Override
    public TypeInference getTypeInference(DataTypeFactory factory) {
      return TypeInference.newBuilder()
          .typedArguments(DataTypes.INT().notNull(), primitiveArray())
          .outputTypeStrategy(TypeStrategies.explicit(DataTypes.INT()))
          .build();
    }

    public Integer eval(Integer id, int[] value) {
      return value == null ? -100 : value.length + 100;
    }

    public Integer eval(Integer id, List<Integer> value) {
      return value == null ? -200 : value.size() + 200;
    }
  }

  @FunctionHint(
      input = @DataTypeHint("INT NOT NULL"),
      output =
          @DataTypeHint(value = "ARRAY<ROW<q INT,tags ARRAY<STRING>>>", bridgedTo = List.class))
  public static class ListRows extends ScalarFunction {
    public List<Row> eval(Integer id) {
      var rows = new ArrayList<Row>();
      rows.add(Row.of(id, new String[] {"é\ud800-" + id, null}));
      rows.add(null);
      return rows;
    }
  }

  @FunctionHint(
      input = {
        @DataTypeHint("INT NOT NULL"),
        @DataTypeHint(value = "ARRAY<ROW<q ARRAY<INT NOT NULL>>>", bridgedTo = List.class)
      },
      output = @DataTypeHint("INT"))
  public static class ListArgument extends ScalarFunction {
    public Integer eval(Integer id, List<Row> rows) {
      if (rows == null) return null;
      int size = 0;
      for (Row row : rows)
        if (row != null && row.getField(0) != null) size += Array.getLength(row.getField(0));
      return size;
    }
  }

  public static class NullElements extends ScalarFunction {
    @Override
    public TypeInference getTypeInference(DataTypeFactory factory) {
      return TypeInference.newBuilder()
          .typedArguments(DataTypes.INT().notNull())
          .outputTypeStrategy(
              TypeStrategies.explicit(
                  DataTypes.ARRAY(DataTypes.INT().notNull()).bridgedTo(Integer[].class)))
          .build();
    }

    public Integer[] eval(Integer id) {
      return new Integer[] {id, null};
    }
  }
}
