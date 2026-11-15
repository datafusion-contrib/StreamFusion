package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import java.util.Map;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.inference.TypeStrategies;
import org.apache.flink.types.Row;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class FlinkNestedBridgeGuardSqlHarnessTest {
  @ParameterizedTest
  @ValueSource(
      strings = {
        "row_primitive_array",
        "row_list",
        "row_internal",
        "map_internal",
        "array_row_primitive"
      })
  void externalNestedConversionsExecuteWhileInternalBridgesRetainFallback(String shape) {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var runs =
          NativeFailureParity.run(
              () -> {
                var table =
                    FlinkNestedBridgeTableFunctionSqlHarnessTest.environment(
                        "row", "primitive-INT", "both", 32, metrics);
                table.createTemporarySystemFunction("guarded_rows", new GuardedRows(shape));
                return table;
              },
              "SELECT s.id,t.v,t.pos FROM src s,LATERAL TABLE(guarded_rows(s.id)) AS t(v,pos)");
      boolean internal = shape.endsWith("internal");
      runs.assertSuccess(
          internal ? NativeFailureParity.Route.FALLBACK : NativeFailureParity.Route.NATIVE);
      assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
      if (internal) {
        assertTrue(metrics.nativeOperators().isEmpty());
        assertTrue(
            runs.nativeRun().fallbackReasons().stream()
                .anyMatch(reason -> reason.contains("verified external Java conversions")),
            runs::toString);
      } else
        RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeTableFunction");
    }
  }

  public static class GuardedRows extends TableFunction<Row> {
    private final String shape;

    public GuardedRows(String shape) {
      this.shape = shape;
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory factory) {
      DataType value;
      switch (shape) {
        case "row_primitive_array":
          value =
              DataTypes.ROW(
                  DataTypes.FIELD(
                      "q", DataTypes.ARRAY(DataTypes.INT().notNull()).bridgedTo(int[].class)));
          break;
        case "row_list":
          value =
              DataTypes.ROW(
                  DataTypes.FIELD(
                      "q", DataTypes.ARRAY(DataTypes.INT()).bridgedTo(java.util.List.class)));
          break;
        case "row_internal":
          value = DataTypes.ROW(DataTypes.FIELD("q", internalRow()));
          break;
        case "map_internal":
          value = DataTypes.MAP(DataTypes.STRING(), internalRow());
          break;
        case "array_row_primitive":
          value =
              DataTypes.ARRAY(
                  DataTypes.ROW(
                      DataTypes.FIELD("q", DataTypes.INT().notNull().bridgedTo(int.class))));
          break;
        default:
          throw new IllegalArgumentException(shape);
      }
      return TypeInference.newBuilder()
          .typedArguments(DataTypes.INT().notNull())
          .outputTypeStrategy(
              TypeStrategies.explicit(
                  DataTypes.ROW(
                      DataTypes.FIELD("v", value), DataTypes.FIELD("pos", DataTypes.INT()))))
          .build();
    }

    private static DataType internalRow() {
      return DataTypes.ROW(DataTypes.FIELD("x", DataTypes.INT())).bridgedTo(RowData.class);
    }

    public void eval(Integer id) {
      Object value;
      switch (shape) {
        case "row_primitive_array":
          value = Row.of(new int[] {id, -id});
          break;
        case "row_list":
          value = Row.of(Arrays.asList(id, null));
          break;
        case "row_internal":
          value = Row.of(GenericRowData.of(id));
          break;
        case "map_internal":
          value = Map.of("value", GenericRowData.of(id));
          break;
        case "array_row_primitive":
          value = new Row[] {Row.of(id)};
          break;
        default:
          throw new IllegalArgumentException(shape);
      }
      collect(Row.of(value, id));
    }
  }
}
