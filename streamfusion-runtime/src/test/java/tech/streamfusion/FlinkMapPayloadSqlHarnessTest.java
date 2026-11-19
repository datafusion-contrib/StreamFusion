package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;
import static tech.streamfusion.compat.FlinkTestSources.fromData;

import java.util.*;
import java.util.stream.*;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.runtime.typeutils.ExternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.*;

public class FlinkMapPayloadSqlHarnessTest {
  static Stream<String> types() {
    return Stream.of(
        System.getProperty(
                "map.payload.types",
                "map,int_map,multiset,array_map,row_map,map_array,map_row,map_map")
            .split(","));
  }

  static Stream<Arguments> cases() {
    return types()
        .flatMap(
            type ->
                Stream.of(
                        System.getProperty(
                                "map.payload.shapes", "inner,topn,sortlimit,offset,limit,firstn")
                            .split(","))
                    .map(shape -> Arguments.of(type, shape)));
  }

  @ParameterizedTest
  @MethodSource("cases")
  void payloadsRetainValuesTypesAndNativeExecution(String type, String shape) {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var runs = NativeFailureParity.run(() -> environment(metrics, type), sql(shape));
      runs.assertSuccess(
          Boolean.getBoolean("map.payload.expectFallback")
              ? NativeFailureParity.Route.FALLBACK
              : NativeFailureParity.Route.NATIVE);
      assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
      assertFalse(runs.host().rows().isEmpty());
      if (!Boolean.getBoolean("map.payload.expectFallback"))
        RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), operator(shape));
    }
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "p['head']",
        "p[CASE WHEN MOD(id,2)=0 THEN 'head' ELSE 'tail' END]",
        "CARDINALITY(p)"
      })
  void retainedMapsComposeWithScalarConsumers(String expression) {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var runs =
          NativeFailureParity.run(
              () -> environment(metrics, "map"),
              "SELECT id," + expression + " FROM (" + sql("topn") + ")");
      runs.assertSuccess(NativeFailureParity.Route.NATIVE);
      RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeColumnarTopN");
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"p[1]", "p[CAST(MOD(id,2) AS INT)]"})
  void integerMapKeysAreNotArrayPositions(String expression) {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var runs =
          NativeFailureParity.run(
              () -> environment(metrics, "int_map"),
              "SELECT id," + expression + " FROM (" + sql("topn") + ")");
      runs.assertSuccess(NativeFailureParity.Route.NATIVE);
      RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeColumnarTopN");
    }
  }

  @ParameterizedTest
  @MethodSource("types")
  void nonTimeRankingCarriesOriginalPayloads(String type) {
    org.junit.jupiter.api.Assumptions.assumeTrue(
        tech.streamfusion.compat.FlinkTestCapabilities.NON_TIME_OVER);
    try (var metrics = new RecoveryExecutionMetrics()) {
      var runs =
          NativeFailureParity.run(
              () -> environment(metrics, type),
              "SELECT id,k,score,p,ROW_NUMBER() OVER (PARTITION BY k ORDER BY score ASC) FROM src");
      runs.assertSuccess(NativeFailureParity.Route.NATIVE);
      RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeColumnarTopN");
    }
  }

  static Stream<Arguments> outerCases() {
    return types()
        .flatMap(
            type ->
                Stream.of("LEFT", "RIGHT", "FULL", "SEMI", "ANTI")
                    .map(join -> Arguments.of(type, join)));
  }

  @ParameterizedTest
  @MethodSource("outerCases")
  void outerSemiAndAntiRetainPayloads(String type, String join) {
    String query =
        join.equals("SEMI") || join.equals("ANTI")
            ? "SELECT a.id,a.p FROM src a WHERE "
                + (join.equals("ANTI") ? "NOT " : "")
                + "EXISTS (SELECT 1 FROM src b WHERE a.k=b.k AND a.id=b.id)"
            : "SELECT a.id AS left_id,b.id AS right_id,a.p AS left_p,b.p AS right_p FROM src a "
                + join
                + " JOIN src b ON a.k=b.k AND a.id=b.id";
    try (var metrics = new RecoveryExecutionMetrics()) {
      var runs = NativeFailureParity.run(() -> environment(metrics, type), query);
      assertNull(runs.host().failure(), runs::toString);
      assertNull(runs.nativeRun().failure(), runs::toString);
      assertEquals(NativeFailureParity.Route.NATIVE, runs.nativeRun().route());
      assertEquals(
          materialized(runs.host().rows()), materialized(runs.nativeRun().rows()), runs::toString);
      assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
      RecoveryExecutionMetrics.assertConsumed(
          metrics.nativeOperators(), "NativeColumnarUpdatingJoin");
    }
  }

  static Map<List<Object>, Long> materialized(List<List<Object>> rows) {
    var result = new HashMap<List<Object>, Long>();
    for (var row : rows) {
      var key = new ArrayList<>(row.subList(1, row.size()));
      long sign = List.of("+I", "+U").contains(row.get(0)) ? 1 : -1;
      result.merge(key, sign, Long::sum);
    }
    result.values().removeIf(count -> count == 0);
    return result;
  }

  @ParameterizedTest
  @ValueSource(strings = {"p", "p['head']", "p[CASE WHEN MOD(id,2)=0 THEN 'head' ELSE 'tail' END]"})
  void mapUdfAndConsumersComposeAfterRank(String expression) {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var runs =
          NativeFailureParity.run(
              () -> {
                var table = environment(metrics, "map");
                table.createTemporarySystemFunction("rank_map", new MapIdentity());
                return table;
              },
              "SELECT id,"
                  + expression
                  + " FROM (SELECT id,k,score,p,rn FROM (SELECT id,k,score,rank_map(p) AS"
                  + " p,ROW_NUMBER() OVER (PARTITION BY k ORDER BY score DESC,id ASC) AS rn FROM"
                  + " src) WHERE rn<=3)");
      boolean enabled = tech.streamfusion.arrow.ArrowConversion.nestedScalarUdfsEnabled();
      runs.assertSuccess(
          enabled ? NativeFailureParity.Route.NATIVE : NativeFailureParity.Route.FALLBACK);
      if (enabled) RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeCalc");
    }
  }

  @org.apache.flink.table.annotation.FunctionHint(
      input = @org.apache.flink.table.annotation.DataTypeHint("MAP<STRING,INT>"),
      output = @org.apache.flink.table.annotation.DataTypeHint("MAP<STRING,INT>"))
  public static class MapIdentity extends org.apache.flink.table.functions.ScalarFunction {
    public Map<String, Integer> eval(Map<String, Integer> value) {
      return value;
    }

    @Override
    public boolean isDeterministic() {
      return false;
    }
  }

  @org.apache.flink.table.annotation.FunctionHint(
      input = @org.apache.flink.table.annotation.DataTypeHint("BIGINT"),
      output = @org.apache.flink.table.annotation.DataTypeHint("ROW<m MAP<STRING,STRING>>"))
  public static class NestedStringMap extends org.apache.flink.table.functions.ScalarFunction {
    public Row eval(Long value) {
      return Row.of(Map.of("a", "value-" + value));
    }

    @Override
    public boolean isDeterministic() {
      return false;
    }
  }

  static DataType payloadType(String type) {
    var map = DataTypes.MAP(DataTypes.STRING(), DataTypes.INT());
    switch (type) {
      case "scalar":
        return DataTypes.INT();
      case "int_map":
        return DataTypes.MAP(DataTypes.INT(), DataTypes.INT());
      case "map":
        return map;
      case "multiset":
        return DataTypes.MULTISET(DataTypes.STRING());
      case "array_map":
        return DataTypes.ARRAY(map);
      case "row_map":
        return DataTypes.ROW(DataTypes.FIELD("m", map), DataTypes.FIELD("raw", DataTypes.BYTES()));
      case "map_array":
        return DataTypes.MAP(DataTypes.STRING(), DataTypes.ARRAY(DataTypes.INT()));
      case "map_row":
        return DataTypes.MAP(
            DataTypes.STRING(),
            DataTypes.ROW(
                DataTypes.FIELD("n", DataTypes.INT()), DataTypes.FIELD("raw", DataTypes.BYTES())));
      case "map_map":
        return DataTypes.MAP(DataTypes.STRING(), map);
      default:
        throw new IllegalArgumentException(type);
    }
  }

  static Object payload(String type, int id) {
    if (id % 13 == 0) return null;
    var map = new LinkedHashMap<String, Integer>();
    if (id % 7 != 0) {
      map.put("tail", id);
      if (id % 5 == 0) map.put(null, -id);
      map.put("head", id % 6 == 0 ? null : -id);
    }
    switch (type) {
      case "scalar":
        return id;
      case "int_map":
        var numeric = new LinkedHashMap<Integer, Integer>();
        numeric.put(10, id);
        numeric.put(null, -id);
        numeric.put(2, id + 2);
        numeric.put(1, id + 1);
        return numeric;
      case "map":
        return map;
      case "multiset":
        var bag = new LinkedHashMap<String, Integer>();
        if (id % 7 != 0) {
          bag.put(null, 3);
          bag.put("head", 1 + id % 4);
        }
        return bag;
      case "array_map":
        return new Map[] {map, null, new LinkedHashMap<>()};
      case "row_map":
        return Row.of(map, new byte[] {0, (byte) id, (byte) 255});
      case "map_array":
        var arrays = new LinkedHashMap<String, Integer[]>();
        arrays.put("z", new Integer[] {id, null, -id});
        arrays.put(null, null);
        arrays.put("a", new Integer[0]);
        return arrays;
      case "map_row":
        var rows = new LinkedHashMap<String, Row>();
        rows.put("z", Row.of(id, new byte[] {0, (byte) id}));
        rows.put(null, null);
        return rows;
      case "map_map":
        var maps = new LinkedHashMap<String, Map<String, Integer>>();
        maps.put("z", map);
        maps.put(null, null);
        maps.put("a", new LinkedHashMap<>());
        return maps;
      default:
        throw new IllegalArgumentException(type);
    }
  }

  static DataType rowType(String type) {
    return DataTypes.ROW(
        DataTypes.FIELD("id", DataTypes.BIGINT()),
        DataTypes.FIELD("k", DataTypes.INT()),
        DataTypes.FIELD("score", DataTypes.BIGINT()),
        DataTypes.FIELD("p", payloadType(type)));
  }

  static List<Row> rows(String type) {
    return IntStream.range(0, 48)
        .mapToObj(
            i ->
                Row.of(
                    (long) i, i % 11 == 0 ? null : i % 4, (long) (i * 17 % 37), payload(type, i)))
        .collect(tech.streamfusion.compat.ListCollectors.toList());
  }

  static TableEnvironment environment(RecoveryExecutionMetrics metrics, String type) {
    var env = metrics.environment(new org.apache.flink.configuration.Configuration(), 1, 0);
    env.getConfig().enableObjectReuse();
    var table = StreamTableEnvironment.create(env);
    table.getConfig().set("streamfusion.transpose.batchRows", "7");
    table.createTemporaryView(
        "src",
        table.fromDataStream(
            fromData(env, rows(type), ExternalTypeInfo.<Row>of(rowType(type))),
            Schema.newBuilder()
                .fromRowDataType(rowType(type))
                .columnByExpression("pt", "PROCTIME()")
                .build()));
    return table;
  }

  static String sql(String shape) {
    switch (shape) {
      case "inner":
        return "SELECT a.id AS left_id,b.id AS right_id,a.p AS left_p,b.p AS right_p FROM src a"
            + " JOIN src b ON a.k=b.k AND a.id=b.id";
      case "firstn":
        return "SELECT id,k,score,p,rn FROM (SELECT *,ROW_NUMBER() OVER (PARTITION BY k ORDER BY"
            + " pt) AS rn FROM src) WHERE rn<=3";
      case "topn":
        return "SELECT id,k,score,p,rn FROM (SELECT *,ROW_NUMBER() OVER (PARTITION BY k ORDER BY"
            + " score DESC,id ASC) AS rn FROM src) WHERE rn<=3";
      case "sortlimit":
        return "SELECT id,k,score,p FROM src ORDER BY score DESC,id ASC LIMIT 7";
      case "offset":
        return "SELECT id,k,score,p FROM src ORDER BY score DESC,id ASC LIMIT 7 OFFSET 3";
      case "limit":
        return "SELECT id,k,score,p FROM src LIMIT 7";
      default:
        throw new IllegalArgumentException(shape);
    }
  }

  static String operator(String shape) {
    return shape.equals("inner")
        ? "NativeColumnarUpdatingJoin"
        : shape.equals("firstn") ? "NativeFirstN" : "NativeColumnarTopN";
  }
}
