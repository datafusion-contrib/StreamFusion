package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedList;
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
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/** External conversions use independently instantiated released converters in both engines. */
class FlinkRecursiveTableFunctionSqlHarnessTest {
  static Stream<Arguments> signatures() {
    String selected = System.getProperty("recursive.signatures");
    if (selected != null)
      return Stream.of(selected.split(";")).map(s -> Arguments.of((Object[]) s.split(":")));
    var defaults = defaultScalars();
    var containers =
        defaults.flatMap(
            kind ->
                Stream.of(
                        "list/row",
                        "list/map",
                        "list/list",
                        "row/list",
                        "map/list",
                        "array/list",
                        "list/array")
                    .map(shape -> Arguments.of(shape, "default-" + kind, "both")));
    var bridges =
        FlinkNestedBridgeTableFunctionSqlHarnessTest.bridges()
            .flatMap(
                bridge ->
                    Stream.of(
                            "array", "list", "list/row", "row/list", "map/array", "array/map/list")
                        .flatMap(
                            shape ->
                                Stream.of("argument", "result", "both")
                                    .map(position -> Arguments.of(shape, bridge, position))));
    var primitives =
        Stream.of("BOOLEAN", "TINYINT", "SMALLINT", "INT", "BIGINT", "FLOAT", "DOUBLE")
            .flatMap(
                kind ->
                    Stream.of("row/primitive", "map/primitive", "list/primitive", "array/primitive")
                        .map(shape -> Arguments.of(shape, "primitive-" + kind, "both")));
    var implementations =
        Stream.of("linked", "custom")
            .flatMap(
                impl ->
                    Stream.of("row/" + impl + "/map", "list/" + impl + "/row", impl + "/array/list")
                        .flatMap(
                            shape ->
                                Stream.of("default-STRING", "primitive-DOUBLE", "timestamp-sql")
                                    .map(bridge -> Arguments.of(shape, bridge, "result"))));
    var keys =
        Stream.of("key/list", "key/primitive", "list/key/row")
            .flatMap(
                shape ->
                    Stream.of("primitive-INT", "primitive-DOUBLE")
                        .map(bridge -> Arguments.of(shape, bridge, "both")));
    var temporalArrays =
        Stream.of("date-int", "time-int", "yearmonth-int", "daytime-primitive-long")
            .flatMap(
                bridge ->
                    Stream.of("row/array", "list/array", "array/array", "map/array")
                        .flatMap(
                            shape ->
                                Stream.of("argument", "result", "both")
                                    .map(position -> Arguments.of(shape, bridge, position))));
    return Stream.of(containers, bridges, primitives, implementations, keys, temporalArrays)
        .flatMap(s -> s);
  }

  static Stream<String> defaultScalars() {
    return Stream.of(
        "BOOLEAN",
        "TINYINT",
        "SMALLINT",
        "INT",
        "BIGINT",
        "FLOAT",
        "DOUBLE",
        "STRING",
        "CHAR(4)",
        "VARBINARY",
        "BINARY(4)",
        "DECIMAL(18,8)",
        "DECIMAL(38,18)",
        "DATE",
        "TIME",
        "TIMESTAMP(9)",
        "TIMESTAMP_LTZ(9)",
        "DAY_TIME",
        "YEAR_MONTH");
  }

  @ParameterizedTest(name = "{0} {1} {2}")
  @MethodSource("signatures")
  void recursiveConversionsKeepTypesNullsKindsAndOwnedFanout(
      String shape, String bridge, String position) {
    for (boolean left : List.of(false, true)) {
      try (var metrics = new RecoveryExecutionMetrics()) {
        var runs =
            NativeFailureParity.runSmallBounded(
                () -> environment(shape, bridge, position, 48, metrics), query(left));
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
          continue;
        }
        boolean temporalResult =
            !position.equals("argument")
                && List.of("date-int", "time-int", "yearmonth-int", "daytime-primitive-long")
                    .contains(bridge)
                && List.of("array", "row/array").contains(shape);
        runs.assertSuccess(
            Boolean.getBoolean("recursive.expectFallback") || temporalResult
                ? NativeFailureParity.Route.FALLBACK
                : NativeFailureParity.Route.NATIVE);
        assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
        assertEquals(
            NativeParity.multiset(
                FlinkNestedBridgeTableFunctionSqlHarnessTest.rawRows(runs.host().rows())),
            NativeParity.multiset(
                FlinkNestedBridgeTableFunctionSqlHarnessTest.rawRows(runs.nativeRun().rows())));
        assertFalse(runs.host().rows().isEmpty());
        if (!Boolean.getBoolean("recursive.expectFallback") && !temporalResult)
          RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeTableFunction");
      }
    }
  }

  static String query(boolean left) {
    return "SELECT s.id,s.p,t.v,t.pos FROM src s"
        + (left ? " LEFT JOIN " : ",")
        + "LATERAL TABLE(recursive_rows(s.id,s.p)) AS t(v,pos)"
        + (left ? " ON TRUE" : "");
  }

  static TableEnvironment environment(
      String shape, String bridge, String position, int count, RecoveryExecutionMetrics metrics) {
    var env = metrics.environment(new Configuration(), 1, 0);
    env.getConfig().enableObjectReuse();
    var table = StreamTableEnvironment.create(env);
    var type = sourceType(shape, bridge);
    var input =
        env.fromSequence(0, Math.max(0, count - 1))
            .filter(i -> i < count)
            .map(i -> Row.of(i.intValue(), input(shape, bridge, (int) (i % 48)).getField(1)))
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
        "recursive_rows", new RecursiveRows(shape, bridge, position));
    return table;
  }

  static DataType sourceType(String shape, String bridge) {
    return DataTypes.ROW(
        DataTypes.FIELD("id", DataTypes.INT().notNull()),
        DataTypes.FIELD("p", defaults(container(shape, bridgeLeaf(bridge), false))));
  }

  static DataType defaults(DataType type) {
    return TypeConversions.fromLogicalToDataType(type.getLogicalType());
  }

  static DataType bridgeLeaf(String bridge) {
    return bridge.startsWith("default-")
        ? FlinkMixedPrimitiveTableFunctionSqlHarnessTest.scalarType(bridge.substring(8))
        : FlinkNestedBridgeTableFunctionSqlHarnessTest.bridgeLeaf(bridge);
  }

  static DataType container(String shape, DataType leaf, boolean result) {
    if (shape.isEmpty()) return leaf;
    int slash = shape.indexOf('/');
    String outer = slash < 0 ? shape : shape.substring(0, slash);
    var child = container(slash < 0 ? "" : shape.substring(slash + 1), leaf, result);
    switch (outer) {
      case "scalar":
        return child;
      case "row":
        return DataTypes.ROW(DataTypes.FIELD("q", child));
      case "map":
        return DataTypes.MAP(DataTypes.STRING(), child);
      case "key":
        return DataTypes.MAP(child, DataTypes.INT());
      case "array":
        return DataTypes.ARRAY(child);
      case "primitive":
        return DataTypes.ARRAY(child)
            .bridgedTo(Array.newInstance(child.getConversionClass(), 0).getClass());
      case "list":
        return DataTypes.ARRAY(child).bridgedTo(List.class);
      case "linked":
        return DataTypes.ARRAY(child).bridgedTo(result ? LinkedList.class : List.class);
      case "custom":
        return DataTypes.ARRAY(child).bridgedTo(result ? CustomList.class : List.class);
      default:
        throw new IllegalArgumentException(shape);
    }
  }

  static Row input(String shape, String bridge, int index) {
    var type = sourceType(shape, bridge).getChildren().get(1);
    String kind =
        bridge.startsWith("default-")
            ? bridge.substring(8)
            : FlinkNestedBridgeTableFunctionSqlHarnessTest.kind(bridge);
    return Row.of(index, index % 11 == 10 ? null : value(type, kind, index, 0));
  }

  private static Object value(DataType type, String kind, int index, int depth) {
    if (type.getLogicalType().isNullable() && (index + depth) % 9 == 8) return null;
    switch (type.getLogicalType().getTypeRoot()) {
      case ROW:
        return Row.ofKind(
            RowKind.values()[index % 4], value(type.getChildren().get(0), kind, index, depth + 1));
      case MAP:
        var map = new LinkedHashMap<Object, Object>();
        if (index % 5 != 0) {
          if (type.getChildren()
              .get(0)
              .getLogicalType()
              .is(org.apache.flink.table.types.logical.LogicalTypeRoot.VARCHAR)) {
            map.put(
                index % 3 == 0 ? null : "k\ud800",
                value(type.getChildren().get(1), kind, index, depth + 1));
            map.put("other", value(type.getChildren().get(1), kind, index + 1, depth + 1));
          } else {
            map.put(value(type.getChildren().get(0), kind, index, depth + 1), index);
            map.put(value(type.getChildren().get(0), kind, index + 1, depth + 1), -index);
          }
        }
        return map;
      case ARRAY:
        int length = index % 5;
        var child = type.getChildren().get(0);
        Object array = Array.newInstance(child.getConversionClass(), length);
        for (int i = 0; i < length; i++)
          Array.set(array, i, value(child, kind, index + i, depth + 1));
        return array;
      default:
        return FlinkMixedPrimitiveTableFunctionSqlHarnessTest.scalarValue(kind, index);
    }
  }

  public static class CustomList<E> extends ArrayList<E> {}

  public static class RecursiveRows extends TableFunction<Row> {
    private final String shape;
    private final String bridge;
    private final String position;
    private transient DataType argumentType;
    private transient DataType resultType;
    private transient DataStructureConverter<Object, Object> arguments;
    private transient DataStructureConverter<Object, Object> defaults;
    private transient ResultValues results;
    private transient Row row;

    public RecursiveRows(String shape, String bridge, String position) {
      this.shape = shape;
      this.bridge = bridge;
      this.position = position;
    }

    DataType argumentType() {
      var type = container(shape, bridgeLeaf(bridge), false);
      return position.equals("result") ? defaults(type) : type;
    }

    DataType resultType() {
      var type = container(shape, bridgeLeaf(bridge), true);
      return position.equals("argument") ? defaults(type) : type;
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory factory) {
      return TypeInference.newBuilder()
          .typedArguments(DataTypes.INT().notNull(), argumentType())
          .outputTypeStrategy(
              TypeStrategies.explicit(
                  DataTypes.ROW(
                      DataTypes.FIELD("v", resultType()), DataTypes.FIELD("pos", DataTypes.INT()))))
          .build();
    }

    @Override
    public void open(FunctionContext context) {
      if (row != null) throw new IllegalStateException("recursive rows opened twice");
      row = new Row(2);
      argumentType = argumentType();
      resultType = resultType();
      if (!argumentType.equals(resultType)) {
        arguments = DataStructureConverters.getConverter(argumentType);
        defaults = DataStructureConverters.getConverter(defaults(resultType));
        arguments.open(context.getUserCodeClassLoader());
        defaults.open(context.getUserCodeClassLoader());
        results = new ResultValues(resultType, context.getUserCodeClassLoader());
      }
    }

    public void eval(Integer id, Object payload) {
      if (row == null) throw new IllegalStateException("recursive rows not open");
      if (payload == null) return;
      assertClasses(argumentType, payload);
      Object result =
          arguments == null
              ? payload
              : results.convert(defaults.toExternalOrNull(arguments.toInternalOrNull(payload)));
      assertClasses(resultType, result);
      for (int i = 0; i < 2; i++) {
        row.setField(0, result);
        row.setField(1, id * 2 + i);
        collect(row);
      }
      invalidate(result);
      invalidate(payload);
      row.setField(0, null);
      row.setField(1, -1);
    }

    @Override
    public void close() {
      if (row == null) throw new IllegalStateException("recursive rows closed twice");
      row = null;
    }
  }

  static void assertClasses(DataType type, Object value) {
    if (value == null) return;
    Class<?> expected = type.getConversionClass();
    if (expected.isPrimitive()) expected = Array.get(Array.newInstance(expected, 1), 0).getClass();
    if (!expected.isInstance(value))
      throw new IllegalStateException("Expected " + expected + ", got " + value.getClass());
    switch (type.getLogicalType().getTypeRoot()) {
      case ROW:
        assertClasses(type.getChildren().get(0), ((Row) value).getField(0));
        break;
      case MAP:
        ((Map<?, ?>) value)
            .forEach(
                (k, v) -> {
                  assertClasses(type.getChildren().get(0), k);
                  assertClasses(type.getChildren().get(1), v);
                });
        break;
      case ARRAY:
        if (value instanceof List)
          for (Object child : (List<?>) value) assertClasses(type.getChildren().get(0), child);
        else
          for (int i = 0; i < Array.getLength(value); i++)
            assertClasses(type.getChildren().get(0), Array.get(value, i));
        break;
      default:
        break;
    }
  }

  static final class ResultValues {
    private final DataType type;
    private final ResultValues[] children;
    private final DataStructureConverter<Object, Object> source;
    private final DataStructureConverter<Object, Object> target;

    ResultValues(DataType type, ClassLoader loader) {
      this.type = type;
      children =
          type.getChildren().stream()
              .map(child -> new ResultValues(child, loader))
              .toArray(ResultValues[]::new);
      source = children.length == 0 ? DataStructureConverters.getConverter(defaults(type)) : null;
      target = children.length == 0 ? DataStructureConverters.getConverter(type) : null;
      if (source != null) {
        source.open(loader);
        target.open(loader);
      }
    }

    Object convert(Object value) {
      if (value == null) return null;
      switch (type.getLogicalType().getTypeRoot()) {
        case ROW:
          var row = (Row) value;
          return Row.ofKind(row.getKind(), children[0].convert(row.getField(0)));
        case MAP:
          var map = new LinkedHashMap<Object, Object>();
          ((Map<?, ?>) value)
              .forEach((k, v) -> map.put(children[0].convert(k), children[1].convert(v)));
          return map;
        case ARRAY:
          int length = Array.getLength(value);
          if (List.class.isAssignableFrom(type.getConversionClass())) {
            List<Object> list =
                type.getConversionClass() == LinkedList.class
                    ? new LinkedList<>()
                    : type.getConversionClass() == CustomList.class
                        ? new CustomList<>()
                        : new ArrayList<>();
            for (int i = 0; i < length; i++) list.add(children[0].convert(Array.get(value, i)));
            return list;
          }
          Object array = Array.newInstance(type.getConversionClass().getComponentType(), length);
          for (int i = 0; i < length; i++)
            Array.set(array, i, children[0].convert(Array.get(value, i)));
          return array;
        default:
          return target.toExternalOrNull(source.toInternalOrNull(value));
      }
    }
  }

  static void invalidate(Object value) {
    if (value instanceof Row) {
      var row = (Row) value;
      invalidate(row.getField(0));
      row.setField(0, null);
    } else if (value instanceof Map) {
      ((Map<?, ?>) value).keySet().forEach(FlinkRecursiveTableFunctionSqlHarnessTest::invalidate);
      ((Map<?, ?>) value).values().forEach(FlinkRecursiveTableFunctionSqlHarnessTest::invalidate);
      ((Map<?, ?>) value).clear();
    } else if (value instanceof List) {
      ((List<?>) value).forEach(FlinkRecursiveTableFunctionSqlHarnessTest::invalidate);
      ((List<?>) value).clear();
    } else if (value != null && value.getClass().isArray()) {
      Class<?> component = value.getClass().getComponentType();
      Object cleared =
          component.isPrimitive() ? Array.get(Array.newInstance(component, 1), 0) : null;
      for (int i = 0; i < Array.getLength(value); i++) {
        invalidate(Array.get(value, i));
        Array.set(value, i, cleared);
      }
    } else if (value instanceof java.util.Date) ((java.util.Date) value).setTime(42);
  }
}
