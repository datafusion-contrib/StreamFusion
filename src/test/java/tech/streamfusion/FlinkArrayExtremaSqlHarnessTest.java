package tech.streamfusion;

import static org.apache.flink.table.api.DataTypes.*;

import java.util.List;
import java.util.stream.Stream;
import org.apache.flink.types.Row;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

class FlinkArrayExtremaSqlHarnessTest {
  @ParameterizedTest
  @MethodSource("integerCases")
  void integerExtremaKeepMeasuredHostFallback(String function, String type) throws Exception {
    tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction(function);
    assertFallback(
        () -> FlinkArrayDistinctSqlHarnessTest.environment(type),
        "SELECT id, " + function + "(a), " + function + "(ARRAY_DISTINCT(a)) FROM src");
    assertFallback(
        () -> FlinkArrayDistinctSqlHarnessTest.environment(type),
        "SELECT " + function + "(a) FROM src WHERE " + function + "(a) < 0");
    assertFallback(
        () -> FlinkArrayDistinctSqlHarnessTest.environment(type),
        "SELECT " + function + "(a) FROM src WHERE id < 0");
  }

  @ParameterizedTest
  @ValueSource(strings = {"ARRAY_MIN", "ARRAY_MAX"})
  void longAndNonNullElementArraysKeepNullableResults(String function) throws Exception {
    tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction(function);
    Integer[] values = new Integer[65];
    for (int i = 0; i < values.length; i++) values[i] = 32 - i;
    assertFallback(
        () ->
            BuiltinFunctionParity.environment(
                ROW(FIELD("a", ARRAY(INT().notNull()))),
                List.of(
                    Row.of((Object) values),
                    Row.of((Object) new Integer[] {}),
                    Row.of((Object) null))),
        "SELECT " + function + "(a) FROM src");
  }

  @ParameterizedTest
  @ValueSource(strings = {"ARRAY_MIN", "ARRAY_MAX"})
  void largeNullableArraysKeepHostFallback(String function) throws Exception {
    tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction(function);
    Long[] values = new Long[65];
    for (int i = 0; i < values.length; i++) values[i] = i % 7 == 0 ? null : (long) (32 - i);
    assertFallback(
        () ->
            BuiltinFunctionParity.environment(
                ROW(FIELD("a", ARRAY(BIGINT()))),
                List.of(Row.of((Object) values), Row.of((Object) new Long[65]))),
        "SELECT " + function + "(a) FROM src");
  }

  @ParameterizedTest
  @ValueSource(strings = {"ARRAY_MIN", "ARRAY_MAX"})
  void unverifiedOrderingKeepsFallback(String function) throws Exception {
    tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction(function);
    NativeParity.assertFallbackReasonContains(
        () ->
            BuiltinFunctionParity.environment(
                ROW(FIELD("a", ARRAY(DOUBLE()))),
                List.of(Row.of((Object) new Double[] {0.0, -0.0, Double.NaN, null}))),
        "SELECT " + function + "(a) FROM src",
        "unsupported function/operator: " + function);
  }

  private static void assertFallback(
      java.util.function.Supplier<org.apache.flink.table.api.TableEnvironment> environment,
      String sql)
      throws Exception {
    String function = sql.contains("ARRAY_MIN") ? "ARRAY_MIN" : "ARRAY_MAX";
    NativeParity.assertFallbackReasonContains(
        environment, sql, "unsupported function/operator: " + function);
  }

  static Stream<Arguments> integerCases() {
    return Stream.of("ARRAY_MIN", "ARRAY_MAX")
        .flatMap(
            function ->
                Stream.of("TINYINT", "SMALLINT", "INT", "BIGINT")
                    .map(type -> Arguments.of(function, type)));
  }
}
