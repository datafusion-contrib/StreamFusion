package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static tech.streamfusion.compat.FlinkTestSources.fromData;

import java.math.BigDecimal;
import java.time.Instant;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.planner.NativePlanner;

/** Decimal overflow must be SQL NULL, including when observed by a downstream expression. */
class FlinkDecimalOverflowSqlHarnessTest {

  @Test
  void decimalArithmeticComposesWithTemporalLiteralsAndEvaluation() throws Exception {
    NativeParity.assertParity(
        () -> decimals(6, 3, "999.995", "-999.995", "0", null),
        "SELECT a + a, a - a, a * a, DATE '2024-02-29', TIME '12:34:56', "
            + "INTERVAL '1' DAY, INTERVAL '1' MONTH, "
            + "EXTRACT(YEAR FROM TO_TIMESTAMP(CAST(a AS STRING), 'yyyy')) FROM t");
  }

  @Test
  void decimalAndClockKindsRemainDistinct() throws Exception {
    var table = decimals(6, 3, "1.000");
    String sql = "SELECT a + a, a - a, a * a, CURRENT_TIMESTAMP FROM t";
    assertTrue(NativePlanner.explain(table, sql).contains("NativeCalc"));
    NativePlanner.install(table);
    Instant before = Instant.now().minusSeconds(1);
    try (var rows = table.executeSql(sql).collect()) {
      assertTrue(rows.hasNext());
      Row row = rows.next();
      assertEquals(new BigDecimal("2.000"), row.getField(0));
      assertEquals(new BigDecimal("0.000"), row.getField(1));
      assertEquals(new BigDecimal("1.000000"), row.getField(2));
      Instant timestamp = (Instant) row.getField(3);
      assertTrue(!timestamp.isBefore(before));
      assertTrue(!timestamp.isAfter(Instant.now().plusSeconds(1)));
    }
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "SELECT id, CAST(a AS DECIMAL(5, 2)) FROM t",
        "SELECT id, CAST(a AS DECIMAL(5, 2)) IS NULL FROM t",
        "SELECT id FROM t WHERE CAST(a AS DECIMAL(5, 2)) IS NULL",
        "SELECT id, CAST(a AS DECIMAL(38, 38)) FROM t"
      })
  void narrowingCastRoundsBeforeCheckingPrecision(String sql) throws Exception {
    NativeParity.assertParity(
        () ->
            decimals(
                6, 3, "999.995", "-999.995", "999.994", "-999.994", "0.005", "-0.005", "0", null),
        sql);
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "SELECT id, CAST(id AS DECIMAL(1, 1)) FROM t",
        "SELECT id, CAST(id AS DECIMAL(38, 38)) IS NULL FROM t"
      })
  void integerCastOverflowProducesNull(String sql) throws Exception {
    NativeParity.assertParity(() -> decimals(1, 0, "0", "1", "-1"), sql);
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "SELECT id, a + a, a - (0 - a), a * a FROM t",
        "SELECT id, a + 1.000, a - 1.000, a * 2.000 FROM t",
        "SELECT id, (a + a) IS NULL, (a - (0 - a)) IS NULL, (a * a) IS NULL FROM t",
        "SELECT id FROM t WHERE (a + a) IS NULL",
        "SELECT id, CASE WHEN (a + a) IS NULL THEN 0.000 ELSE a + a END FROM t"
      })
  void overflowingArithmeticRemainsComposable(String sql) throws Exception {
    NativeParity.assertParity(
        () ->
            decimals(
                38,
                3,
                "99999999999999999999999999999999999.999",
                "-99999999999999999999999999999999999.999",
                "1.000",
                "-1.000",
                "0",
                null),
        sql);
  }

  @Test
  void multiplicationCanHaveAValidResultAfterAWideIntermediate() throws Exception {
    NativeParity.assertParity(
        () ->
            decimals(
                38,
                20,
                "999999999999999999.99999999999999999999",
                "-999999999999999999.99999999999999999999",
                "0.12345678901234567895",
                "0",
                null),
        "SELECT id, a * CAST('0.12345678901234567895' AS DECIMAL(38,20)), a * a FROM t");
  }

  @Test
  void overflowedGroupKeysStayNative() throws Exception {
    String sql = "SELECT a + a, COUNT(*) FROM t GROUP BY a + a";
    java.util.function.Supplier<TableEnvironment> environment =
        () ->
            decimals(
                38,
                3,
                "99999999999999999999999999999999999.999",
                "-99999999999999999999999999999999999.999",
                "1.000",
                "1.000",
                "0",
                null);
    String plan = NativePlanner.explain(environment.get(), sql);
    assertTrue(plan.contains("NativeCalc"), plan);
    assertTrue(plan.contains("NativeColumnarGroupAggregate"), plan);
    NativeParity.assertChangelogParity(environment, sql);
  }

  @Test
  void nonNullableResultsKeepFlinksConstraintFailure() {
    String sql = "SELECT a + a FROM t";
    for (boolean nativeRun : new boolean[] {false, true}) {
      TableEnvironment table = decimalRows(38, 3, false, "99999999999999999999999999999999999.999");
      if (nativeRun) {
        assertTrue(NativePlanner.explain(table, sql).contains("NativeCalc"));
        NativePlanner.install(table);
      }
      Exception failure =
          assertThrows(
              Exception.class,
              () -> {
                try (var rows = table.executeSql(sql).collect()) {
                  while (rows.hasNext()) {
                    rows.next();
                  }
                }
              },
              "native=" + nativeRun);
      StringBuilder causes = new StringBuilder();
      for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
        causes.append(cause.getMessage()).append('\n');
      }
      assertTrue(
          causes.toString().contains("is NOT NULL, however, a null value is being written"),
          causes.toString());
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {
      "SELECT a+a, a/CAST(3 AS DECIMAL(38,0)), a/CAST(2 AS DECIMAL(38,0)) FROM t",
      "SELECT a+a, a-a, a*a, a/CAST(3 AS DECIMAL(38,0)), MOD(a,CAST(3 AS DECIMAL(38,0))) FROM t"
  })
  void rowOrderedOverflowReachesTheDropConstraintEnforcer(String sql) throws Exception {
    NativeParity.assertParity(() -> {
      var table = decimalRows(38, 0, false, "99999999999999999999999999999999999999", "1");
      table.getConfig().set("table.exec.sink.not-null-enforcer", "DROP");
      return table;
    }, sql);
  }

  @Test
  void rowOrderedOverflowKeepsTheSinkConstraintError() {
    var runs = NativeFailureParity.run(
        () -> decimalRows(38, 0, false, "99999999999999999999999999999999999999", "1"),
        "SELECT a+a, a/CAST(3 AS DECIMAL(38,0)), a/CAST(2 AS DECIMAL(38,0)) FROM t");
    org.junit.jupiter.api.Assertions.assertNotNull(runs.host().rootCause(), runs.toString());
    runs.assertFailure(runs.host().rootCause().getClass(), "is NOT NULL",
        NativeFailureParity.Phase.ROW_EVALUATION, NativeFailureParity.Route.NATIVE);
  }

  private static TableEnvironment decimals(int precision, int scale, String... values) {
    return decimalRows(precision, scale, true, values);
  }

  private static TableEnvironment decimalRows(
      int precision, int scale, boolean nullable, String... values) {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment table = StreamTableEnvironment.create(env);
    Row[] rows = new Row[values.length];
    for (int i = 0; i < values.length; i++) {
      rows[i] = Row.of((long) i, values[i] == null ? null : new BigDecimal(values[i]));
    }
    table.createTemporaryView(
        "t",
        fromData(env, Types.ROW_NAMED(new String[] {"id", "a"}, Types.LONG, Types.BIG_DEC), rows),
        Schema.newBuilder()
            .column("id", DataTypes.BIGINT())
            .column(
                "a",
                nullable
                    ? DataTypes.DECIMAL(precision, scale)
                    : DataTypes.DECIMAL(precision, scale).notNull())
            .build());
    return table;
  }
}
