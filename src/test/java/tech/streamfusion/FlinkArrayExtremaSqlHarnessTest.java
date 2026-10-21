package tech.streamfusion;

import static org.apache.flink.table.api.DataTypes.*;
import static org.junit.jupiter.api.Assertions.*;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.planner.NativePlanner;

class FlinkArrayExtremaSqlHarnessTest {
  @ParameterizedTest
  @MethodSource("integerCases")
  void integerExtremaExecuteInRustAndMatchReleasedOrdering(String function, String type) throws Exception {
    tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction(function);
    assertRustFunction(FlinkArrayDistinctSqlHarnessTest.environment(type), function);
    BuiltinFunctionParity.assertParity(
        () -> FlinkArrayDistinctSqlHarnessTest.environment(type),
        "SELECT id, " + function + "(a), " + function + "(ARRAY_DISTINCT(a)) FROM src");
    BuiltinFunctionParity.assertParity(
        () -> FlinkArrayDistinctSqlHarnessTest.environment(type),
        "SELECT " + function + "(a) FROM src WHERE " + function + "(a) < 0");
    BuiltinFunctionParity.assertParity(
        () -> FlinkArrayDistinctSqlHarnessTest.environment(type),
        "SELECT " + function + "(a) FROM src WHERE id < 0");
  }

  @ParameterizedTest
  @MethodSource("decimalCases")
  void exactWideDecimalsKeepPrecisionScaleAndNullableResults(String function, int scale) throws Exception {
    tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction(function);
    assertRustFunction(decimals(scale), function);
    BuiltinFunctionParity.assertParity(() -> decimals(scale),
        "SELECT id, " + function + "(a) FROM src");
    BuiltinFunctionParity.assertParity(() -> decimals(scale),
        "SELECT id, " + function + "(a) FROM src WHERE " + function + "(a) < 0");
    BuiltinFunctionParity.assertParity(() -> decimals(scale),
        "SELECT " + function + "(a) FROM src WHERE id < 0");
  }

  @ParameterizedTest
  @ValueSource(strings = {"ARRAY_MIN", "ARRAY_MAX"})
  void fallibleArrayOperandsAndAllFilteredInputsKeepFlinkEvaluation(String function) {
    tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction(function);
    java.util.function.Supplier<TableEnvironment> source = () -> BuiltinFunctionParity.environment(
        ROW(FIELD("id", INT())), List.of(Row.of(2), Row.of(0), Row.of((Object) null)));
    var failed = NativeFailureParity.run(source,
        "SELECT " + function + "(ARRAY[1/id]) FROM src");
    assertNotNull(failed.host().failure());
    failed.assertFailure(failed.host().rootCause().getClass(), failed.host().phase(),
        NativeFailureParity.Route.NATIVE);
    NativeFailureParity.run(source,
        "SELECT " + function + "(ARRAY[1/id]) FROM src WHERE id=99")
        .assertSuccess(NativeFailureParity.Route.NATIVE);
    NativeFailureParity.run(source,
        "SELECT CASE WHEN id=0 THEN NULL ELSE " + function + "(ARRAY[1/id]) END FROM src")
        .assertSuccess(NativeFailureParity.Route.NATIVE);
  }

  @ParameterizedTest
  @ValueSource(strings = {"ARRAY_MIN", "ARRAY_MAX"})
  void disabledCollectionExtensionRetainsFallback(String function) throws Exception {
    tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction(function);
    NativeParity.assertFallback(() -> {
      var table = decimals(9);
      table.getConfig().set("streamfusion.experimental.collectionExpressions", "false");
      return table;
    }, "SELECT " + function + "(a) FROM src");
  }

  private static void assertRustFunction(TableEnvironment table, String function) {
    var scan = NativePlanner.install(table);
    String plan = table.explainSql("SELECT " + function + "(a) FROM src");
    assertTrue(plan.contains("NativeCalc"), plan);
    assertTrue(scan.functionExecutions().stream().anyMatch(execution ->
        execution.contains("native=[" + function + "]") && execution.contains("jvm=[]")),
        scan.functionExecutions().toString());
  }

  private static TableEnvironment decimals(int scale) {
    List<Row> rows = new ArrayList<>();
    BigInteger limit = BigInteger.TEN.pow(38).subtract(BigInteger.ONE);
    BigInteger[] pool = {BigInteger.ZERO, null, limit.negate(), limit,
        new BigInteger("9007199254740993"), new BigInteger("9007199254740994"),
        BigInteger.valueOf(Long.MIN_VALUE).subtract(BigInteger.ONE),
        BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE)};
    int[] lengths = {0, 1, 2, 8, 31, 32, 33, 64, 65};
    for (int row = 0; row < 5003; row++) {
      BigDecimal[] values = null;
      if (row % 7 != 0) {
        values = new BigDecimal[lengths[row % lengths.length]];
        if (row % 11 != 0) {
          for (int i = 0; i < values.length; i++) {
            BigInteger value = pool[(row + i) % pool.length];
            values[i] = value == null ? null : new BigDecimal(value, scale);
          }
        }
      }
      rows.add(Row.of(row, values));
    }
    return BuiltinFunctionParity.environment(
        ROW(FIELD("id", INT()), FIELD("a", ARRAY(DECIMAL(38, scale)))), rows);
  }

  @ParameterizedTest
  @ValueSource(strings = {"ARRAY_MIN", "ARRAY_MAX"})
  void longAndNonNullElementArraysKeepNullableResults(String function) throws Exception {
    tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction(function);
    Integer[] values = new Integer[65];
    for (int i = 0; i < values.length; i++) values[i] = 32 - i;
    java.util.function.Supplier<TableEnvironment> source = () ->
            BuiltinFunctionParity.environment(
                ROW(FIELD("a", ARRAY(INT().notNull()))),
                List.of(
                    Row.of((Object) values),
                    Row.of((Object) new Integer[] {}),
                    Row.of((Object) null)));
    assertGeneratedFunction(source.get(), function);
    BuiltinFunctionParity.assertParity(source, "SELECT " + function + "(a) FROM src");
  }

  @ParameterizedTest
  @ValueSource(strings = {"ARRAY_MIN", "ARRAY_MAX"})
  void largeNullableArraysPreserveNulls(String function) throws Exception {
    tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction(function);
    Long[] values = new Long[65];
    for (int i = 0; i < values.length; i++) values[i] = i % 7 == 0 ? null : (long) (32 - i);
    BuiltinFunctionParity.assertParity(
        () ->
            BuiltinFunctionParity.environment(
                ROW(FIELD("a", ARRAY(BIGINT()))),
                List.of(Row.of((Object) values), Row.of((Object) new Long[65]))),
        "SELECT " + function + "(a) FROM src");
  }

  @ParameterizedTest
  @ValueSource(strings = {"ARRAY_MIN", "ARRAY_MAX"})
  void floatingExtremaUseReleasedGeneratedOrdering(String function) throws Exception {
    tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction(function);
    java.util.function.Supplier<TableEnvironment> source = () ->
            BuiltinFunctionParity.environment(
                ROW(FIELD("a", ARRAY(DOUBLE()))),
                List.of(Row.of((Object) new Double[] {0.0, -0.0, Double.NaN, null})));
    assertGeneratedFunction(source.get(), function);
    BuiltinFunctionParity.assertParity(source, "SELECT " + function + "(a) FROM src");
  }

  private static void assertGeneratedFunction(TableEnvironment table, String function) {
    var scan = NativePlanner.install(table);
    table.explainSql("SELECT " + function + "(a) FROM src");
    assertTrue(scan.functionExecutions().stream().anyMatch(execution ->
        execution.contains("native=[]") && execution.contains("jvm=[" + function + "]")),
        scan.functionExecutions().toString());
  }

  static Stream<Arguments> integerCases() {
    return Stream.of("ARRAY_MIN", "ARRAY_MAX")
        .flatMap(
            function ->
                Stream.of("TINYINT", "SMALLINT", "INT", "BIGINT")
                    .map(type -> Arguments.of(function, type)));
  }

  static Stream<Arguments> decimalCases() {
    return Stream.of("ARRAY_MIN", "ARRAY_MAX")
        .flatMap(function -> Stream.of(0, 9, 18, 38)
            .map(scale -> Arguments.of(function, scale)));
  }
}
