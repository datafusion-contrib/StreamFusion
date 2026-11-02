package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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

class FlinkNestedBridgeTableFunctionSqlHarnessTest {
  static Stream<String> bridges() {
    String selected = System.getProperty("nested.bridge.cases");
    if (selected != null) return Stream.of(selected.split(";"));
    return Stream.concat(
            Stream.of("BOOLEAN", "TINYINT", "SMALLINT", "INT", "BIGINT", "FLOAT", "DOUBLE")
                .map(kind -> "primitive-" + kind),
            Stream.concat(
                Stream.of("date-integer", "date-int"),
                Stream.concat(
                    FlinkAlternateTemporalTableFunctionSqlHarnessTest.bridges(),
                    FlinkPrimitiveTemporalTableFunctionSqlHarnessTest.primitives())))
        .distinct();
  }

  static Stream<Arguments> signatures() {
    return bridges()
        .flatMap(
            bridge ->
                Stream.of(
                        System.getProperty(
                                "nested.bridge.shapes", "row;map;row_map;map_row;map_key")
                            .split(";"))
                    .flatMap(
                        shape ->
                            Stream.of("argument", "result", "both")
                                .map(position -> Arguments.of(shape, bridge, position))));
  }

  @ParameterizedTest(name = "{0} {1} {2}")
  @MethodSource("signatures")
  void nestedConversionsKeepReleasedTypesNullsAndOwnedResults(
      String shape, String bridge, String position) {
    for (boolean left : List.of(false, true)) {
      try (var metrics = new RecoveryExecutionMetrics()) {
        var runs =
            NativeFailureParity.run(
                () -> environment(shape, bridge, position, 96, metrics), query(left));
        boolean expectedNative =
            Boolean.parseBoolean(System.getProperty("nested.bridge.expectNative", "true"));
        runs.assertSuccess(
            expectedNative ? NativeFailureParity.Route.NATIVE : NativeFailureParity.Route.FALLBACK);
        assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
        assertEquals(
            NativeParity.multiset(rawRows(runs.host().rows())),
            NativeParity.multiset(rawRows(runs.nativeRun().rows())));
        assertFalse(runs.host().rows().isEmpty());
        if (expectedNative)
          RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeTableFunction");
        else {
          assertTrue(metrics.nativeOperators().isEmpty());
          assertTrue(
              runs.nativeRun().fallbackReasons().stream()
                  .anyMatch(reason -> reason.contains("verified external Java conversions")),
              runs::toString);
        }
      }
    }
  }

  static String query(boolean left) {
    return "SELECT s.id,s.p,t.v,t.pos FROM src s"
        + (left ? " LEFT JOIN " : ",")
        + "LATERAL TABLE(nested_bridges(s.id,s.p)) AS t(v,pos)"
        + (left ? " ON TRUE" : "");
  }

  static TableEnvironment environment(
      String shape, String bridge, String position, int count, RecoveryExecutionMetrics metrics) {
    var env = metrics.environment(new Configuration(), 1, 0);
    env.getConfig().enableObjectReuse();
    var table = StreamTableEnvironment.create(env);
    table.getConfig().set("streamfusion.transpose.batchRows", "7");
    table.getConfig().setLocalTimeZone(java.time.ZoneId.of("America/New_York"));
    var type = sourceType(shape, bridge);
    Object[] values =
        java.util.stream.IntStream.range(0, 64)
            .mapToObj(i -> input(shape, bridge, i).getField(1))
            .toArray();
    var input =
        env.fromSequence(0, Math.max(0, count - 1))
            .filter(i -> i < count)
            .map(i -> Row.of(i.intValue(), values[(int) (i % values.length)]))
            .returns(ExternalTypeInfo.<Row>of(type));
    table.createTemporaryView(
        "src", table.fromDataStream(input, Schema.newBuilder().fromRowDataType(type).build()));
    return register(table, shape, bridge, position);
  }

  static TableEnvironment register(
      TableEnvironment table, String shape, String bridge, String position) {
    table.getConfig().setLocalTimeZone(java.time.ZoneId.of("America/New_York"));
    table.createTemporarySystemFunction(
        "nested_bridges", new NestedBridges(shape, bridge, position));
    return table;
  }

  static DataType sourceType(String shape, String bridge) {
    return DataTypes.ROW(
        DataTypes.FIELD("id", DataTypes.INT().notNull()),
        DataTypes.FIELD("p", container(shape, sourceLeaf(bridge))));
  }

  static Row input(String shape, String bridge, int index) {
    boolean nullable = sourceLeaf(bridge).getLogicalType().isNullable();
    Object leaf =
        nullable && index % 6 == 0
            ? null
            : FlinkMixedPrimitiveTableFunctionSqlHarnessTest.scalarValue(kind(bridge), index);
    if (shape.equals("map_key") && bridge.equals("date-sql") && index % 16 == 6)
      leaf = java.time.LocalDate.of(1582, 10, 5);
    return Row.of(index, index % 8 == 7 ? null : value(shape, leaf, index));
  }

  static String kind(String bridge) {
    return bridge.startsWith("primitive-")
        ? bridge.substring("primitive-".length())
        : FlinkAlternateTemporalTableFunctionSqlHarnessTest.kind(bridge);
  }

  static DataType bridgeLeaf(String bridge) {
    if (bridge.startsWith("primitive-"))
      return FlinkMixedPrimitiveTableFunctionSqlHarnessTest.scalarType(kind(bridge))
          .notNull()
          .bridgedTo(FlinkMixedPrimitiveTableFunctionSqlHarnessTest.primitiveClass(kind(bridge)));
    return FlinkAlternateTemporalTableFunctionSqlHarnessTest.bridgeType(bridge);
  }

  static DataType sourceLeaf(String bridge) {
    var leaf = FlinkMixedPrimitiveTableFunctionSqlHarnessTest.scalarType(kind(bridge));
    return bridgeLeaf(bridge).getLogicalType().isNullable() ? leaf : leaf.notNull();
  }

  static DataType container(String shape, DataType leaf) {
    switch (shape) {
      case "row":
        return DataTypes.ROW(DataTypes.FIELD("q", leaf));
      case "map":
        return DataTypes.MAP(DataTypes.STRING(), leaf);
      case "map_key":
        return DataTypes.MAP(leaf, DataTypes.INT());
      case "row_map":
        return container("row", container("map", leaf));
      case "map_row":
        return container("map", container("row", leaf));
      default:
        throw new IllegalArgumentException(shape);
    }
  }

  static Object value(String shape, Object leaf, int index) {
    switch (shape) {
      case "row":
        return Row.ofKind(RowKind.values()[index % 4], leaf);
      case "map":
        var map = new LinkedHashMap<String, Object>();
        if (index % 5 != 0) map.put("value", leaf);
        return map;
      case "map_key":
        var keys = new LinkedHashMap<Object, Integer>();
        if (index % 5 != 0) {
          keys.put(leaf, index);
          if (leaf instanceof java.time.Instant) {
            var instant = (java.time.Instant) leaf;
            keys.put(
                instant.getNano() == 999999999 ? instant.minusNanos(1) : instant.plusNanos(1),
                index + 1);
          } else if (java.time.LocalDate.of(1582, 10, 5).equals(leaf)) {
            keys.put(java.time.LocalDate.of(1582, 10, 15), index + 1);
          }
        }
        return keys;
      case "row_map":
        return Row.of(index % 4 == 0 ? null : value("map", leaf, index));
      case "map_row":
        return value("map", index % 4 == 0 ? null : value("row", leaf, index), index);
      default:
        throw new IllegalArgumentException(shape);
    }
  }

  static List<List<Object>> rawRows(List<List<Object>> rows) {
    var result = new ArrayList<List<Object>>();
    for (var row : rows) {
      var fields = new ArrayList<Object>();
      for (Object field : row) fields.add(stamp(field));
      result.add(fields);
    }
    return result;
  }

  private static Object stamp(Object value) {
    if (value instanceof Float) return Float.floatToRawIntBits((Float) value);
    if (value instanceof Double) return Double.doubleToRawLongBits((Double) value);
    if (value instanceof Row) {
      var row = (Row) value;
      var fields = new ArrayList<Object>();
      fields.add(row.getKind());
      for (int i = 0; i < row.getArity(); i++) fields.add(stamp(row.getField(i)));
      return fields;
    }
    if (value instanceof Map) {
      var result = new LinkedHashMap<Object, Object>();
      ((Map<?, ?>) value).forEach((key, field) -> result.put(stamp(key), stamp(field)));
      return result;
    }
    if (value instanceof List) {
      var result = new ArrayList<Object>();
      for (Object item : (List<?>) value) result.add(stamp(item));
      return result;
    }
    return value;
  }

  public static class NestedBridges extends TableFunction<Row> {
    private final String shape;
    private final String bridge;
    private final String position;
    private transient Row result;
    private transient DataStructureConverter<Object, Object> sourceConverter;
    private transient DataStructureConverter<Object, Object> resultConverter;
    private transient Class<?> expectedInputClass;

    public NestedBridges(String shape, String bridge, String position) {
      this.shape = shape;
      this.bridge = bridge;
      this.position = position;
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory factory) {
      var argument =
          container(shape, position.equals("result") ? sourceLeaf(bridge) : bridgeLeaf(bridge));
      var output =
          position.equals("argument") ? DataTypes.INT() : container(shape, bridgeLeaf(bridge));
      return TypeInference.newBuilder()
          .typedArguments(DataTypes.INT().notNull(), argument)
          .outputTypeStrategy(
              TypeStrategies.explicit(
                  DataTypes.ROW(
                      DataTypes.FIELD("v", output), DataTypes.FIELD("pos", DataTypes.INT()))))
          .build();
    }

    @Override
    @SuppressWarnings("unchecked")
    public void open(FunctionContext context) {
      if (result != null) throw new IllegalStateException("nested bridge opened twice");
      result = new Row(2);
      Class<?> conversion =
          (position.equals("result") ? sourceLeaf(bridge) : bridgeLeaf(bridge))
              .getConversionClass();
      expectedInputClass =
          conversion.isPrimitive()
              ? java.lang.reflect.Array.get(java.lang.reflect.Array.newInstance(conversion, 1), 0)
                  .getClass()
              : conversion;
      if (position.equals("result")) {
        sourceConverter =
            (DataStructureConverter<Object, Object>)
                DataStructureConverters.getConverter(container(shape, sourceLeaf(bridge)));
        resultConverter =
            (DataStructureConverter<Object, Object>)
                DataStructureConverters.getConverter(container(shape, bridgeLeaf(bridge)));
        sourceConverter.open(context.getUserCodeClassLoader());
        resultConverter.open(context.getUserCodeClassLoader());
      }
    }

    public void eval(Integer id, Object payload) {
      if (result == null) throw new IllegalStateException("nested bridge not open");
      if (payload == null) return;
      assertClasses(shape, payload);
      Object output = payload;
      if (position.equals("argument")) output = leafCount(payload);
      else if (position.equals("result"))
        output = resultConverter.toExternalOrNull(sourceConverter.toInternalOrNull(payload));
      result.setField(0, output);
      result.setField(1, id);
      collect(result);
      invalidate(output);
      if (output != payload) invalidate(payload);
      result.setField(0, null);
      result.setField(1, -1);
    }

    private void assertClasses(String current, Object payload) {
      if (payload == null) return;
      if (current.equals("row_map")) {
        assertClasses("map", ((Row) payload).getField(0));
      } else if (current.equals("map_row")) {
        for (Object value : ((Map<?, ?>) payload).values()) assertClasses("row", value);
      } else if (current.equals("row")) {
        assertLeaf(((Row) payload).getField(0));
      } else {
        var fields =
            current.equals("map_key")
                ? ((Map<?, ?>) payload).keySet()
                : ((Map<?, ?>) payload).values();
        for (Object value : fields) assertLeaf(value);
      }
    }

    private void assertLeaf(Object value) {
      if (value != null && !expectedInputClass.isInstance(value))
        throw new IllegalStateException("wrong nested bridge class " + value.getClass());
    }

    private static int leafCount(Object value) {
      if (value == null) return 0;
      if (value instanceof Row) {
        int count = 0;
        for (int i = 0; i < ((Row) value).getArity(); i++)
          count += leafCount(((Row) value).getField(i));
        return count;
      }
      if (value instanceof Map) {
        int count = 0;
        for (Object field : ((Map<?, ?>) value).values()) count += leafCount(field);
        return count;
      }
      return 1;
    }

    private static void invalidate(Object value) {
      if (value instanceof java.util.Date) ((java.util.Date) value).setTime(42);
      else if (value instanceof Row) {
        Row row = (Row) value;
        for (int i = 0; i < row.getArity(); i++) {
          invalidate(row.getField(i));
          row.setField(i, null);
        }
      } else if (value instanceof Map) {
        Map<?, ?> map = (Map<?, ?>) value;
        for (var entry : map.entrySet()) {
          invalidate(entry.getKey());
          invalidate(entry.getValue());
        }
        map.clear();
      }
    }

    @Override
    public void close() {
      if (result == null) throw new IllegalStateException("nested bridge closed twice");
      result = null;
      sourceConverter = null;
      resultConverter = null;
    }
  }
}
