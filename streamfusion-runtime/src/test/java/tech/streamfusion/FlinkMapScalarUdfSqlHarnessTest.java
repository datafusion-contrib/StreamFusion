package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;
import static tech.streamfusion.compat.FlinkTestSources.fromData;

import java.math.BigDecimal;
import java.time.*;
import java.util.*;
import java.util.stream.Stream;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.runtime.typeutils.ExternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class FlinkMapScalarUdfSqlHarnessTest {
  static Stream<String> types() {
    return Stream.of(
        System.getProperty(
                "map.udf.types",
                "map,int_map,map_array,map_row,map_map,bool_decimal,float_double,date_time,decimal_timestamp,month_day,row_int")
            .split(","));
  }

  static Stream<Arguments> cases() {
    return types()
        .flatMap(
            type ->
                Stream.of(
                        "SELECT id,map_udf(p) FROM src",
                        "SELECT id,map_udf(p),map_udf(p) FROM src",
                        "SELECT id,CARDINALITY(map_udf(p)) FROM src",
                        "SELECT id,CASE WHEN MOD(id,2)=0 THEN map_udf(p) ELSE p END FROM src",
                        "SELECT id,map_udf(p) FROM src WHERE map_udf(p) IS NOT NULL")
                    .map(sql -> Arguments.of(type, sql)));
  }

  @ParameterizedTest
  @MethodSource("cases")
  void mappedArgumentsResultsAndConsumersUseFlinkEvaluation(String type, String sql)
      throws Exception {
    boolean fallback = Boolean.getBoolean("map.udf.expectFallback");
    try (var metrics = new RecoveryExecutionMetrics()) {
      var runs = NativeFailureParity.run(() -> register(environment(metrics, type), type), sql);
      runs.assertSuccess(
          fallback ? NativeFailureParity.Route.FALLBACK : NativeFailureParity.Route.NATIVE);
      assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
      if (!fallback)
        RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeCalc");
      SharedFlinkCluster.assertNativeMemoryReleased("map-scalar-udf");
    }
  }

  @org.junit.jupiter.api.Test
  void intervalMapsUseCanonicalFlinkUnitsInTableApi() throws Exception {
    List<List<Object>> expected = null;
    for (boolean nativeRun : List.of(false, true)) {
      try (var metrics = new RecoveryExecutionMetrics()) {
        var table = register(environment(metrics, "month_day"), "month_day");
        var scan = nativeRun ? tech.streamfusion.planner.NativePlanner.install(table) : null;
        var query =
            table
                .from("src")
                .select(
                    org.apache.flink.table.api.Expressions.$("id"),
                    org.apache.flink.table.api.Expressions.call(
                        "map_udf", org.apache.flink.table.api.Expressions.$("p")));
        var rows = new ArrayList<List<Object>>();
        try (var iterator = query.execute().collect()) {
          while (iterator.hasNext()) {
            var row = iterator.next();
            rows.add(
                Arrays.asList(
                    row.getKind().shortString(),
                    NativeParity.comparableValue(row.getField(0)),
                    NativeParity.comparableValue(row.getField(1))));
          }
        }
        if (nativeRun) {
          boolean fallback = Boolean.getBoolean("map.udf.expectFallback");
          if (fallback) assertEquals(0, scan.substitutions(), scan.explainSummary());
          else assertTrue(scan.substitutions() > 0, scan.explainSummary());
          assertEquals(expected.size(), rows.size());
          assertEquals(new HashSet<>(expected), new HashSet<>(rows));
          if (!fallback)
            RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeCalc");
        } else expected = rows;
      }
    }
  }

  @ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(
      strings = {
        "SELECT id,map_observe(p) FROM src",
        "SELECT k,SUM(map_observe(p)) FROM src GROUP BY k"
      })
  void mapArgumentsWithScalarResultsComposeWithState(String sql) throws Exception {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var runs =
          NativeFailureParity.run(
              () -> {
                var table = environment(metrics, "map");
                table.createTemporarySystemFunction("map_observe", new Observe());
                return table;
              },
              sql);
      boolean fallback = Boolean.getBoolean("map.udf.expectFallback");
      runs.assertSuccess(
          fallback ? NativeFailureParity.Route.FALLBACK : NativeFailureParity.Route.NATIVE);
      if (!fallback)
        RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeCalc");
      SharedFlinkCluster.assertNativeMemoryReleased("map scalar UDF result");
    }
  }

  @FunctionHint(input = @DataTypeHint("MAP<STRING,INT>"), output = @DataTypeHint("BIGINT"))
  public static class Observe extends ScalarFunction {
    public Long eval(Map<String, Integer> input) {
      if (input == null) return null;
      long result = 0;
      for (var entry : input.entrySet())
        result +=
            31L * (entry.getKey() == null ? 0 : entry.getKey().hashCode())
                + (entry.getValue() == null ? 0 : entry.getValue());
      return result;
    }

    @Override
    public boolean isDeterministic() {
      return false;
    }
  }

  static TableEnvironment register(TableEnvironment table, String type) {
    ScalarFunction function;
    switch (type) {
      case "map":
        function = new StringInt();
        break;
      case "int_map":
        function = new IntInt();
        break;
      case "map_array":
        function = new StringArray();
        break;
      case "map_row":
        function = new StringRow();
        break;
      case "map_map":
        function = new StringMap();
        break;
      case "bool_decimal":
        function = new BoolDecimal();
        break;
      case "float_double":
        function = new FloatDouble();
        break;
      case "date_time":
        function = new DateTime();
        break;
      case "decimal_timestamp":
        function = new DecimalTimestamp();
        break;
      case "month_day":
        function = new MonthDay();
        break;
      case "row_int":
        function = new RowInt();
        break;
      default:
        throw new IllegalArgumentException(type);
    }
    table.createTemporarySystemFunction("map_udf", function);
    return table;
  }

  static DataType rowType(String type) {
    DataType map;
    switch (type) {
      case "bool_decimal":
        map = DataTypes.MAP(DataTypes.BOOLEAN(), DataTypes.DECIMAL(38, 9));
        break;
      case "float_double":
        map = DataTypes.MAP(DataTypes.FLOAT(), DataTypes.DOUBLE());
        break;
      case "date_time":
        map = DataTypes.MAP(DataTypes.DATE(), DataTypes.TIME(3));
        break;
      case "decimal_timestamp":
        map = DataTypes.MAP(DataTypes.DECIMAL(38, 9), DataTypes.TIMESTAMP(9));
        break;
      case "month_day":
        map =
            DataTypes.MAP(
                DataTypes.INTERVAL(DataTypes.YEAR(), DataTypes.MONTH()),
                DataTypes.INTERVAL(DataTypes.DAY(), DataTypes.SECOND(3)));
        break;
      case "row_int":
        map =
            DataTypes.MAP(
                DataTypes.ROW(
                    DataTypes.FIELD("n", DataTypes.INT()),
                    DataTypes.FIELD("tag", DataTypes.STRING())),
                DataTypes.INT());
        break;
      default:
        return FlinkMapPayloadSqlHarnessTest.rowType(type);
    }
    return DataTypes.ROW(
        DataTypes.FIELD("id", DataTypes.BIGINT()),
        DataTypes.FIELD("k", DataTypes.INT()),
        DataTypes.FIELD("score", DataTypes.BIGINT()),
        DataTypes.FIELD("p", map));
  }

  static Object value(String type, int id) {
    if (Set.of("map", "int_map", "map_array", "map_row", "map_map").contains(type))
      return FlinkMapPayloadSqlHarnessTest.payload(type, id);
    if (id % 13 == 0) return null;
    Map<Object, Object> map = new LinkedHashMap<>();
    if (id % 7 == 0) return map;
    map.put(null, null);
    switch (type) {
      case "bool_decimal":
        map.put(false, new BigDecimal("12345678901234567890123456789.123456789"));
        map.put(true, id % 3 == 0 ? null : BigDecimal.valueOf(-id, 9));
        break;
      case "float_double":
        map.put(0.0f, -0.0d);
        map.put(-0.0f, 0.0d);
        map.put(Float.NaN, Double.NaN);
        map.put(Float.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY);
        break;
      case "date_time":
        map.put(LocalDate.of(1900, 1, 1).plusDays(id), LocalTime.of(23, 59, 59, 123000000));
        map.put(LocalDate.of(2400, 12, 31), null);
        break;
      case "decimal_timestamp":
        map.put(BigDecimal.valueOf(id, 9), LocalDateTime.of(1969, 12, 31, 23, 59, 59, 123456789));
        map.put(new BigDecimal("-12345678901234567890123456789.123456789"), null);
        break;
      case "month_day":
        map.put(Period.ofMonths(id - 24), Duration.ofMillis(-123456789L + id));
        map.put(Period.ofMonths(12000), null);
        break;
      case "row_int":
        map.put(Row.ofKind(RowKind.values()[id % 4], id, "é中😀-" + id), id);
        map.put(Row.of(null, null), null);
        break;
      default:
        throw new IllegalArgumentException(type);
    }
    return map;
  }

  static List<Row> rows(String type) {
    var rows = new ArrayList<Row>();
    for (int i = 0; i < 48; i++)
      rows.add(Row.of((long) i, i % 11 == 0 ? null : i % 4, (long) i, value(type, i)));
    return rows;
  }

  static TableEnvironment environment(RecoveryExecutionMetrics metrics, String type) {
    var env = metrics.environment(new Configuration(), 1, 0);
    env.getConfig().enableObjectReuse();
    var table = StreamTableEnvironment.create(env);
    table.getConfig().set("streamfusion.transpose.batchRows", "7");
    var dtype = rowType(type);
    table.createTemporaryView(
        "src",
        table.fromDataStream(
            fromData(env, rows(type), ExternalTypeInfo.<Row>of(dtype)),
            Schema.newBuilder().fromRowDataType(dtype).build()));
    return table;
  }

  public static class Identity extends ScalarFunction {
    public Map<?, ?> eval(Map<?, ?> input) {
      return input;
    }

    @Override
    public boolean isDeterministic() {
      return false;
    }
  }

  @FunctionHint(input = @DataTypeHint("MAP<STRING,INT>"), output = @DataTypeHint("MAP<STRING,INT>"))
  public static class StringInt extends Identity {}

  @FunctionHint(input = @DataTypeHint("MAP<INT,INT>"), output = @DataTypeHint("MAP<INT,INT>"))
  public static class IntInt extends Identity {}

  @FunctionHint(
      input = @DataTypeHint("MAP<STRING,ARRAY<INT>>"),
      output = @DataTypeHint("MAP<STRING,ARRAY<INT>>"))
  public static class StringArray extends Identity {}

  @FunctionHint(
      input = @DataTypeHint("MAP<STRING,ROW<n INT,`raw` BYTES>>"),
      output = @DataTypeHint("MAP<STRING,ROW<n INT,`raw` BYTES>>"))
  public static class StringRow extends Identity {}

  @FunctionHint(
      input = @DataTypeHint("MAP<STRING,MAP<STRING,INT>>"),
      output = @DataTypeHint("MAP<STRING,MAP<STRING,INT>>"))
  public static class StringMap extends Identity {}

  @FunctionHint(
      input = @DataTypeHint("MAP<BOOLEAN,DECIMAL(38,9)>"),
      output = @DataTypeHint("MAP<BOOLEAN,DECIMAL(38,9)>"))
  public static class BoolDecimal extends Identity {}

  @FunctionHint(
      input = @DataTypeHint("MAP<FLOAT,DOUBLE>"),
      output = @DataTypeHint("MAP<FLOAT,DOUBLE>"))
  public static class FloatDouble extends Identity {}

  @FunctionHint(
      input = @DataTypeHint("MAP<DATE,TIME(0)>"),
      output = @DataTypeHint("MAP<DATE,TIME(0)>"))
  public static class DateTime extends Identity {}

  @FunctionHint(
      input = @DataTypeHint("MAP<DECIMAL(38,9),TIMESTAMP(9)>"),
      output = @DataTypeHint("MAP<DECIMAL(38,9),TIMESTAMP(9)>"))
  public static class DecimalTimestamp extends Identity {}

  @FunctionHint(
      input = @DataTypeHint("MAP<INTERVAL MONTH,INTERVAL SECOND(3)>"),
      output = @DataTypeHint("MAP<INTERVAL MONTH,INTERVAL SECOND(3)>"))
  public static class MonthDay extends Identity {}

  @FunctionHint(
      input = @DataTypeHint("MAP<ROW<n INT,tag STRING>,INT>"),
      output = @DataTypeHint("MAP<ROW<n INT,tag STRING>,INT>"))
  public static class RowInt extends Identity {}
}
