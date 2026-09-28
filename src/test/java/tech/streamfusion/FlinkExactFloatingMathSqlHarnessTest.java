package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static tech.streamfusion.compat.FlinkTestSources.fromData;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.planner.NativePlanner;

class FlinkExactFloatingMathSqlHarnessTest {
  private static final String[] UNARY = {
    "EXP", "LN", "LOG10", "SIN", "COS", "TAN", "ASIN", "ACOS", "ATAN", "COSH", "SINH", "TANH",
    "COT", "DEGREES", "RADIANS", "LOG", "LOG2"
  };

  static Stream<Arguments> unaryCases() {
    return Stream.of(UNARY)
        .flatMap(
            fn ->
                Stream.of("DOUBLE", "FLOAT", "BIGINT", "DECIMAL(20,6)")
                    .map(type -> Arguments.of(fn, type)));
  }

  @ParameterizedTest
  @MethodSource("unaryCases")
  void exactMathKeepsTypesAndSpecialValues(String function, String type) throws Exception {
    assertFalse(Boolean.getBoolean("streamfusion.expression.allowIncompatible"));
    assertFalse(Boolean.getBoolean("streamfusion.expression." + function + ".allowIncompatible"));
    String operand = "CAST(x AS " + type + ")";
    compare(
        () -> input(type.equals("DOUBLE") || type.equals("FLOAT")),
        "SELECT id, "
            + function
            + "("
            + operand
            + "), "
            + "CASE WHEN id < 0 THEN "
            + function
            + "("
            + operand
            + ") ELSE 1E0 END FROM src");
  }

  @ParameterizedTest
  @ValueSource(strings = {"TINYINT", "SMALLINT", "INT", "BIGINT", "FLOAT", "DECIMAL"})
  void directNumericInputsUseTheirResolvedOverloads(String type) throws Exception {
    String expressions =
        Stream.of(UNARY).map(fn -> fn + "(v)").collect(java.util.stream.Collectors.joining(", "));
    compare(() -> typedInput(type), "SELECT " + expressions + " FROM src");
  }

  private static TableEnvironment typedInput(String type) {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    var table = StreamTableEnvironment.create(env);
    var info =
        switch (type) {
          case "TINYINT" -> Types.BYTE;
          case "SMALLINT" -> Types.SHORT;
          case "INT" -> Types.INT;
          case "BIGINT" -> Types.LONG;
          case "FLOAT" -> Types.FLOAT;
          default -> Types.BIG_DEC;
        };
    Row[] rows = new Row[4];
    int[] values = {-1, 0, 42};
    for (int i = 0; i < 3; i++) {
      int n = values[i];
      Object value =
          switch (type) {
            case "TINYINT" -> (byte) n;
            case "SMALLINT" -> (short) n;
            case "INT" -> n;
            case "BIGINT" -> (long) n;
            case "FLOAT" -> (float) n;
            default -> java.math.BigDecimal.valueOf(n);
          };
      rows[i] = Row.of(value);
    }
    rows[3] = Row.of((Object) null);
    table.createTemporaryView(
        "src", fromData(env, Types.ROW_NAMED(new String[] {"v"}, info), rows));
    return table;
  }

  static Stream<Arguments> binaryCases() {
    return Stream.of("ATAN2", "LOG")
        .flatMap(
            fn ->
                Stream.of("DOUBLE", "FLOAT", "BIGINT", "DECIMAL(20,6)")
                    .map(type -> Arguments.of(fn, type)));
  }

  @ParameterizedTest
  @MethodSource("binaryCases")
  void binaryMathPreservesDomainResults(String function, String type) throws Exception {
    compare(
        () -> input(type.equals("DOUBLE") || type.equals("FLOAT")),
        "SELECT id, " + function + "(CAST(x AS " + type + "), CAST(y AS " + type + ")) FROM src");
  }

  static Stream<Arguments> roundingCases() {
    return Stream.of("ROUND", "TRUNCATE")
        .flatMap(
            fn ->
                Stream.of("FLOAT", "DOUBLE", "TINYINT", "SMALLINT", "INT", "BIGINT")
                    .filter(
                        type ->
                            !fn.equals("TRUNCATE")
                                || !(type.equals("TINYINT") || type.equals("SMALLINT")))
                    .map(type -> Arguments.of(fn, type)));
  }

  @ParameterizedTest
  @MethodSource("roundingCases")
  void roundingKeepsRuntimeScalesAndNestedConsumers(String function, String type) throws Exception {
    assertFalse(Boolean.getBoolean("streamfusion.expression.ROUND.allowIncompatible"));
    String x = "CAST(x AS " + type + ")";
    compare(
        () -> input(false),
        "SELECT id, "
            + function
            + "("
            + x
            + "), "
            + function
            + "("
            + x
            + ", n), "
            + function
            + "("
            + x
            + ", -2), "
            + "CAST("
            + function
            + "("
            + x
            + ", n) AS STRING) FROM src");
  }

  @ParameterizedTest
  @ValueSource(strings = {"TINYINT", "SMALLINT"})
  void narrowIntegerTruncateRetainsHostCompilationFailure(String type) {
    var comparison =
        NativeFailureParity.run(
            () -> input(false), "SELECT TRUNCATE(CAST(x AS " + type + "), n) FROM src");
    assertTrue(comparison.host().failure() != null, comparison.toString());
    comparison.assertFailure(
        comparison.host().rootCause().getClass(),
        "Assignment conversion not possible",
        comparison.host().phase(),
        NativeFailureParity.Route.FALLBACK);
  }

  static Stream<Arguments> failureCases() {
    return Stream.of("ROUND", "TRUNCATE")
        .flatMap(
            fn ->
                Stream.of(Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY)
                    .flatMap(
                        value ->
                            Stream.of("DOUBLE", "FLOAT")
                                .map(type -> Arguments.of(fn, value, type))));
  }

  @ParameterizedTest
  @MethodSource("failureCases")
  void roundingFailuresAndUnselectedBranchesMatchHost(String function, double value, String type)
      throws Exception {
    Supplier<TableEnvironment> source = () -> environment(Row.of(0, value, 1.0, 2));
    String call = function + "(CAST(x AS " + type + "), n)";
    var comparison = NativeFailureParity.run(source, "SELECT " + call + " FROM src");
    assertTrue(comparison.host().failure() != null, comparison.toString());
    var cause = comparison.host().rootCause();
    comparison.assertFailure(
        cause.getClass(),
        String.valueOf(cause.getMessage()),
        NativeFailureParity.Phase.ROW_EVALUATION,
        NativeFailureParity.Route.NATIVE);
    compare(source, "SELECT CASE WHEN id = 0 THEN 1E0 ELSE " + call + " END FROM src");
    compare(source, "SELECT id FROM src WHERE id <> 0 AND " + call + " > 0", "NativeFilter");
  }

  @ParameterizedTest
  @ValueSource(strings = {"TAN(x)", "COSH(x)", "TRUNCATE(x, n)"})
  void exactMathComposesWithGroupedIntegerSums(String expression) throws Exception {
    Supplier<TableEnvironment> source =
        () -> {
          var table = input(false);
          table.getConfig().set("table.exec.mini-batch.enabled", "true");
          table.getConfig().set("table.exec.mini-batch.allow-latency", "1 h");
          table.getConfig().set("table.exec.mini-batch.size", "32");
          return table;
        };
    String sql =
        "SELECT MOD(id, 4), SUM(CAST("
            + expression
            + " * 1000 AS BIGINT)) FROM src GROUP BY MOD(id, 4)";
    compare(source, sql, "NativeColumnarGroupAggregate");
    assertTrue(NativePlanner.explain(source.get(), sql).contains("NativeCalc"));
  }

  private static void compare(Supplier<TableEnvironment> source, String sql) throws Exception {
    compare(source, sql, "NativeCalc");
  }

  private static void compare(Supplier<TableEnvironment> source, String sql, String operator)
      throws Exception {
    var expected = source.get().sqlQuery(sql).getResolvedSchema().getColumnDataTypes();
    var table = source.get();
    NativePlanner.install(table);
    assertEquals(expected, table.sqlQuery(sql).getResolvedSchema().getColumnDataTypes());
    String plan = NativePlanner.explain(source.get(), sql);
    NativeParity.assertParity(source, sql);
    assertTrue(plan.contains(operator), plan);
  }

  private static TableEnvironment input(boolean special) {
    List<Double> values =
        new ArrayList<>(
            java.util.Arrays.asList(
                null,
                0.0,
                -0.0,
                1.0,
                -1.0,
                0.46,
                -0.46,
                2.675,
                -2.675,
                42.0,
                123.456,
                -123.456,
                1.2345678901234567,
                Double.MIN_VALUE,
                Double.MIN_NORMAL));
    if (special)
      values.addAll(
          List.of(
              Double.MAX_VALUE,
              -Double.MAX_VALUE,
              Double.NaN,
              Double.POSITIVE_INFINITY,
              Double.NEGATIVE_INFINITY));
    Integer[] scales = {null, -3, -1, 0, 1, 2, 6, 18};
    Row[] rows = new Row[values.size() * values.size()];
    for (int i = 0; i < rows.length; i++) {
      rows[i] =
          Row.of(
              i,
              values.get(i / values.size()),
              values.get(i % values.size()),
              scales[i % scales.length]);
    }
    return environment(rows);
  }

  private static TableEnvironment environment(Row... rows) {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    var table = StreamTableEnvironment.create(env);
    table.createTemporaryView(
        "src",
        fromData(
            env,
            Types.ROW_NAMED(
                new String[] {"id", "x", "y", "n"},
                Types.INT,
                Types.DOUBLE,
                Types.DOUBLE,
                Types.INT),
            rows));
    return table;
  }
}
