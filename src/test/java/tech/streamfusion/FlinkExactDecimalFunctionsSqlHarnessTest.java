package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static tech.streamfusion.compat.FlinkTestSources.fromData;

import java.math.BigDecimal;
import java.util.List;
import java.util.stream.IntStream;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import tech.streamfusion.planner.NativePlanner;
import tech.streamfusion.planner.PhysicalPlanScan;

class FlinkExactDecimalFunctionsSqlHarnessTest {
  @Test
  void roundHalfwayValuesAndLiteralScalesMatchHost() throws Exception {
    assertNull(System.getProperty("streamfusion.expression.ROUND.allowIncompatible"));
    NativeParity.assertParity(
        () ->
            decimals(
                7, 3, "1.235", "-1.235", "0.005", "-0.005", "9999.999", "-9999.999", "0", null),
        "SELECT id, ROUND(a), ROUND(a, -5), ROUND(a, -1), ROUND(a, 1), ROUND(a, 2), "
            + "ROUND(a, 3), ROUND(a, 6), ROUND(a, 38), ROUND(a, CAST(NULL AS INT)) FROM t");
  }

  @Test
  void roundUsesFlinksDeclaredPrecisionScaleAndNullability() throws Exception {
    String sql = "SELECT ROUND(a), ROUND(a, -1), ROUND(a, 2), ROUND(a, 6) FROM t";
    var schema = decimals(7, 3, "1.235", null).sqlQuery(sql).getResolvedSchema();
    assertEquals(
        List.of(
            DataTypes.DECIMAL(5, 0),
            DataTypes.DECIMAL(5, 0),
            DataTypes.DECIMAL(7, 2),
            DataTypes.DECIMAL(7, 3)),
        schema.getColumnDataTypes());
    NativeParity.assertParity(() -> decimals(7, 3, "1.235", null), sql);
  }

  @Test
  void roundPrecision38AndOverflowMatchHost() throws Exception {
    NativeParity.assertParity(
        () ->
            decimals(
                38,
                3,
                "99999999999999999999999999999999999.999",
                "-99999999999999999999999999999999999.999",
                "0.005",
                "-0.005",
                null),
        "SELECT id, ROUND(a, 2), ROUND(a, 6), ROUND(a, -1), "
            + "CAST(ROUND(a, 2) AS DECIMAL(38,3)) FROM t");
    NativeParity.assertParity(
        () ->
            decimals(
                38,
                0,
                "99999999999999999999999999999999999999",
                "-99999999999999999999999999999999999999",
                "15",
                "-15",
                "0",
                null),
        "SELECT id, ROUND(a, -1), ROUND(a, -1) IS NULL, ROUND(a, -38), ROUND(a, -39) FROM t");
    NativeParity.assertParity(
        () ->
            decimals(
                38,
                38,
                "0.99999999999999999999999999999999999999",
                "-0.99999999999999999999999999999999999999",
                "0.00000000000000000000000000000000000005",
                null),
        "SELECT id, ROUND(a, 0), ROUND(a, 37), ROUND(a, 38) FROM t");
  }

  @Test
  void extremeNegativeRoundPositionsKeepHostValuesAndErrors() throws Exception {
    NativeParity.assertParity(
        () -> decimals(7, 3, "12.345", "-12.345", "0", null),
        "SELECT id, ROUND(a, -39), ROUND(a, -100), ROUND(a, 2147483647), "
            + "CASE WHEN id < 0 THEN ROUND(a, CAST(-2147483648 AS INT)) ELSE a END FROM t");
    for (boolean nativeRun : new boolean[] {false, true}) {
      TableEnvironment table = decimals(7, 3, "1.234");
      PhysicalPlanScan scan = nativeRun ? NativePlanner.install(table) : null;
      Exception error =
          assertThrows(
              Exception.class,
              () -> {
                try (var rows =
                    table
                        .executeSql("SELECT ROUND(a, CAST(-2147483648 AS INT)) FROM t")
                        .collect()) {
                  while (rows.hasNext()) {
                    rows.next();
                  }
                }
              });
      StringBuilder causes = new StringBuilder();
      for (Throwable cause = error; cause != null; cause = cause.getCause()) {
        causes.append(cause).append('\n');
      }
      assertTrue(causes.toString().contains("Underflow"), causes.toString());
      if (scan != null) {
        assertTrue(scan.substitutions() > 0, scan.fallbackReasons().toString());
      }
    }
    NativeParity.assertFallbackReasonContains(
        () -> decimals(7, 3, "1.234"),
        "SELECT id >= 0 OR ROUND(a, CAST(-2147483648 AS INT)) = 0 FROM t",
        "short-circuit");
  }

  @Test
  void runtimeRoundScaleRetainsAnExplicitFallback() throws Exception {
    String sql = "SELECT ROUND(a, id) FROM t";
    for (boolean nativeRun : new boolean[] {false, true}) {
      TableEnvironment table = decimals(7, 3, "1.234", "-1.234", null);
      PhysicalPlanScan scan = nativeRun ? NativePlanner.install(table) : null;
      Exception error =
          assertThrows(
              Exception.class,
              () -> {
                try (var rows = table.executeSql(sql).collect()) {
                  while (rows.hasNext()) {
                    rows.next();
                  }
                }
              });
      StringBuilder causes = new StringBuilder();
      for (Throwable cause = error; cause != null; cause = cause.getCause()) {
        causes.append(cause).append('\n');
      }
      assertTrue(causes.toString().contains("AssertionError"), causes.toString());
      if (scan != null) {
        assertEquals(0, scan.substitutions());
        assertTrue(scan.fallbackReasons().toString().contains("runtime scale"));
      }
    }
  }

  @Test
  void plannerLiteralsRoundHalfUpToTheirDeclaredScale() throws Exception {
    NativeParity.assertParity(
        () -> decimals(22, 9, "5000.123456789", "5000.123456788", "5000.123456790", "-1.235", null),
        "SELECT id, a >= CAST(5000.123456789122 AS DECIMAL(22,9)), "
            + "a > CAST(-1.2345 AS DECIMAL(7,3)), "
            + "a + CAST(1.2345 AS DECIMAL(7,3)), "
            + "a + CAST(-1.2345 AS DECIMAL(7,3)) FROM t");
  }

  @Test
  void roundedLiteralOverflowMatchesHost() throws Exception {
    NativeParity.assertParity(
        () -> decimals(7, 3, "1.234", "-1.234", null),
        "SELECT id, a + CAST(999.995 AS DECIMAL(5,2)), "
            + "a > CAST(-999.995 AS DECIMAL(5,2)) FROM t");
  }

  @Test
  void decimalToIntegerTruncatesThenWrapsLikeHost() throws Exception {
    for (int scale : new int[] {0, 3}) {
      NativeParity.assertParity(
          () ->
              decimals(
                  38,
                  scale,
                  "0",
                  "0.999",
                  "-0.999",
                  "1.999",
                  "-1.999",
                  "127.999",
                  "128.001",
                  "-129.001",
                  "32767.999",
                  "32768.001",
                  "2147483647.999",
                  "2147483648.001",
                  "-2147483649.001",
                  "9223372036854775807.999",
                  "9223372036854775808.001",
                  "-9223372036854775809.001",
                  "99999999999999999999999999999999999.999",
                  null),
          "SELECT id, CAST(a AS TINYINT), CAST(a AS SMALLINT), CAST(a AS INT), CAST(a AS BIGINT)"
              + " FROM t");
    }
  }

  @Test
  void roundedValuesRemainNativeInFiltersAndGroupKeys() throws Exception {
    NativeParity.assertParity(
        () -> decimals(7, 3, "1.235", "-1.235", "0.005", "-0.005", null),
        "SELECT id, ROUND(ROUND(a, 2), 1) FROM t WHERE ROUND(a, 2) >= CAST(0 AS DECIMAL(7,2))");
    NativeParity.assertChangelogParity(
        () -> decimals(7, 3, "1.235", "1.234", "1.231", "-1.235", null),
        "SELECT ROUND(a, 2), COUNT(*) FROM t GROUP BY ROUND(a, 2)");
  }

  @Test
  void aggregateDerivedDecimalToBigintRemainsNative() throws Exception {
    String sql = "SELECT MOD(id, 2), CAST(AVG(a) AS BIGINT) FROM t GROUP BY MOD(id, 2)";
    var factory =
        (java.util.function.Supplier<TableEnvironment>)
            () -> decimals(10, 2, "10.50", "-10.50", "20.01", "-20.01", "0.99", null);
    String plan = NativePlanner.explain(factory.get(), sql);
    assertTrue(plan.contains("NativeCalc"), plan);
    assertTrue(plan.contains("NativeColumnarGroupAggregate"), plan);
    NativeParity.assertChangelogParity(factory, sql);
  }

  private static TableEnvironment decimals(int precision, int scale, String... values) {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment table = StreamTableEnvironment.create(env);
    Row[] rows =
        IntStream.range(0, values.length)
            .mapToObj(i -> Row.of(i, values[i] == null ? null : new BigDecimal(values[i])))
            .toArray(Row[]::new);
    table.createTemporaryView(
        "t",
        fromData(env, Types.ROW_NAMED(new String[] {"id", "a"}, Types.INT, Types.BIG_DEC), rows),
        Schema.newBuilder()
            .column("id", DataTypes.INT().notNull())
            .column("a", DataTypes.DECIMAL(precision, scale))
            .build());
    return table;
  }
}
