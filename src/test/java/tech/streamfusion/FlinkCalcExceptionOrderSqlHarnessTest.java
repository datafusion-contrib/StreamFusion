package tech.streamfusion;

import static org.apache.flink.table.api.DataTypes.*;
import static tech.streamfusion.NativeFailureParity.Phase.ROW_EVALUATION;
import static tech.streamfusion.NativeFailureParity.Route.NATIVE;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

class FlinkCalcExceptionOrderSqlHarnessTest {
  private String previousFlushLatency;

  @BeforeEach
  void disableTimerFlush() {
    previousFlushLatency = System.getProperty("streamfusion.transpose.flushLatencyMs");
    System.setProperty("streamfusion.transpose.flushLatencyMs", "0");
  }

  @AfterEach
  void restoreTimerFlush() {
    if (previousFlushLatency == null) System.clearProperty("streamfusion.transpose.flushLatencyMs");
    else System.setProperty("streamfusion.transpose.flushLatencyMs", previousFlushLatency);
  }

  static Stream<Arguments> failureCases() {
    return Stream.of("", "_long", "_decimal", "_text")
        .flatMap(
            suffix ->
                Stream.of(1, 3, 2051)
                    .flatMap(
                        count ->
                            Stream.of(
                                    "SELECT f%s(id), g%s(id) FROM src",
                                    "SELECT g%s(id), f%s(id) FROM src",
                                    "SELECT g%s(id) FROM src WHERE f%s(id) IS NOT NULL",
                                    "SELECT f%s(id) FROM src WHERE g%s(id) IS NOT NULL")
                                .map(sql -> Arguments.of(count, sql.formatted(suffix, suffix)))));
  }

  @ParameterizedTest
  @MethodSource("failureCases")
  void independentExpressionsKeepTheFirstFailingRow(int count, String sql) {
    NativeFailureParity.run(() -> environment(count), sql)
        .assertFailure(IllegalArgumentException.class, "g-row-1", ROW_EVALUATION, NATIVE);
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "SELECT f(id) + g(id) FROM src",
        "SELECT f(g(id)) FROM src",
        "SELECT 10 / (2 - id), g(id) FROM src",
        "SELECT CAST(10 AS BIGINT) / (2 - id), g(id) FROM src",
        "SELECT CAST(10 AS DECIMAL(10,2)) / (2 - id), g(id) FROM src",
        "SELECT CAST(CASE WHEN id = 2 THEN 'bad' ELSE '1' END AS INT), g(id) FROM src",
        "SELECT CAST(CASE WHEN id = 2 THEN '1.2.3' ELSE '1' END AS DECIMAL(10,2)), g(id) FROM src",
        "SELECT g(id) FROM src WHERE 10 / (2 - id) > 0"
      })
  void nativeFailureSitesCannotOvertakeAnEarlierUdfFailure(String sql) {
    NativeFailureParity.run(() -> environment(3), sql)
        .assertFailure(IllegalArgumentException.class, "g-row-1", ROW_EVALUATION, NATIVE);
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "SELECT CAST(a AS INT), CAST(b AS INT) FROM src",
        "SELECT CAST(a AS BIGINT), CAST(b AS BIGINT) FROM src",
        "SELECT CAST(b AS INT) FROM src WHERE CAST(a AS INT) > 0"
      })
  void nativeCastsKeepTheFirstFailingRow(String sql) {
    NativeFailureParity.run(
            () ->
                BuiltinFunctionParity.environment(
                    ROW(FIELD("a", STRING()), FIELD("b", STRING())),
                    List.of(Row.of("1", "bad-first-row"), Row.of("bad-second-row", "1"))),
            sql)
        .assertFailure(NumberFormatException.class, "bad-first-row", ROW_EVALUATION, NATIVE);
  }

  @Test
  void binaryBackedStringsKeepFailureOrderThroughFallback() {
    var comparison =
        NativeFailureParity.run(
            () ->
                BuiltinFunctionParity.environment(
                    ROW(FIELD("a", STRING()), FIELD("b", STRING())),
                    List.of(Row.of("YQ==", "!!"), Row.of("$$", "YQ=="))),
            "SELECT FROM_BASE64(a), FROM_BASE64(b) FROM src");
    comparison.assertFailure(
        IllegalArgumentException.class,
        "Illegal base64 character 21",
        ROW_EVALUATION,
        NativeFailureParity.Route.FALLBACK);
    org.junit.jupiter.api.Assertions.assertTrue(
        comparison.nativeRun().fallbackReasons().stream()
            .anyMatch(
                reason ->
                    reason.contains("binary-backed STRING requires a final scalar projection")));
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "SELECT f(id), g(id) FROM src WHERE id = 99",
        "SELECT CASE WHEN id = 2 THEN 0 ELSE f(id) END, "
            + "CASE WHEN id = 1 THEN 0 ELSE g(id) END FROM src",
        "SELECT f(id), g(id) FROM src WHERE id IS NULL",
        "SELECT f(id), g(id) FROM src WHERE id = 0",
        "SELECT COALESCE(f(id), 0), COALESCE(g(id), 0) FROM src WHERE id IS NULL",
        "SELECT f_long(id), g_decimal(id), f_text(id) FROM src WHERE id = 0 OR id IS NULL"
      })
  void filtersNullsAndUntakenBranchesRemainCorrect(String sql) {
    NativeFailureParity.run(() -> environment(2051), sql).assertSuccess(NATIVE);
  }

  private static TableEnvironment environment(int count) {
    List<Row> rows = new ArrayList<>();
    for (int i = 3; i < count; i++) rows.add(Row.of(0));
    if (count > 1) rows.add(Row.of((Object) null));
    rows.add(Row.of(1));
    if (count > 1) rows.add(Row.of(2));
    TableEnvironment table = BuiltinFunctionParity.environment(ROW(FIELD("id", INT())), rows);
    table.createTemporarySystemFunction("f", new FailOnTwo());
    table.createTemporarySystemFunction("g", new FailOnOne());
    table.createTemporarySystemFunction("f_long", new LongFailOnTwo());
    table.createTemporarySystemFunction("g_long", new LongFailOnOne());
    table.createTemporarySystemFunction("f_decimal", new DecimalFailOnTwo());
    table.createTemporarySystemFunction("g_decimal", new DecimalFailOnOne());
    table.createTemporarySystemFunction("f_text", new TextFailOnTwo());
    table.createTemporarySystemFunction("g_text", new TextFailOnOne());
    return table;
  }

  public static class FailOnTwo extends ScalarFunction {
    public Integer eval(Integer x) {
      if (x != null && x == 2) throw new IllegalArgumentException("f-row-2");
      return x;
    }
  }

  public static class FailOnOne extends ScalarFunction {
    public Integer eval(Integer x) {
      if (x != null && x == 1) throw new IllegalArgumentException("g-row-1");
      return x;
    }
  }

  public static class LongFailOnTwo extends ScalarFunction {
    public Long eval(Integer x) {
      Integer value = new FailOnTwo().eval(x);
      return value == null ? null : value.longValue();
    }
  }

  public static class LongFailOnOne extends ScalarFunction {
    public Long eval(Integer x) {
      Integer value = new FailOnOne().eval(x);
      return value == null ? null : value.longValue();
    }
  }

  public static class DecimalFailOnTwo extends ScalarFunction {
    public @DataTypeHint("DECIMAL(10,2)") BigDecimal eval(Integer x) {
      Integer value = new FailOnTwo().eval(x);
      return value == null ? null : BigDecimal.valueOf(value).setScale(2);
    }
  }

  public static class DecimalFailOnOne extends ScalarFunction {
    public @DataTypeHint("DECIMAL(10,2)") BigDecimal eval(Integer x) {
      Integer value = new FailOnOne().eval(x);
      return value == null ? null : BigDecimal.valueOf(value).setScale(2);
    }
  }

  public static class TextFailOnTwo extends ScalarFunction {
    public String eval(Integer x) {
      Integer value = new FailOnTwo().eval(x);
      return value == null ? null : value.toString();
    }
  }

  public static class TextFailOnOne extends ScalarFunction {
    public String eval(Integer x) {
      Integer value = new FailOnOne().eval(x);
      return value == null ? null : value.toString();
    }
  }
}
