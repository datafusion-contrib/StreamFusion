package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static tech.streamfusion.compat.FlinkTestSources.fromData;

import java.math.BigDecimal;
import java.util.List;
import java.util.Locale;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.planner.NativePlanner;
import tech.streamfusion.planner.PhysicalPlanScan;

class FlinkIfNullSqlHarnessTest {
  @Test
  void valuesAndCommonTypesMatchHost() throws Exception {
    NativeParity.assertParity(
        FlinkIfNullSqlHarnessTest::environment,
        "SELECT id, IFNULL(i, 7), IFNULL(n, CAST(-1 AS BIGINT)), IFNULL(s, 'missing'), "
            + "IFNULL(d, CAST(0 AS DECIMAL(12,3))), IFNULL(i, n), IFNULL(b, FALSE) FROM src");
  }

  @Test
  void nestedCallsAndTypedNullsMatchHost() throws Exception {
    NativeParity.assertParity(
        FlinkIfNullSqlHarnessTest::environment,
        "SELECT id, IFNULL(IFNULL(i, CAST(NULL AS INT)), 9), "
            + "IFNULL(CAST(NULL AS STRING), s), IFNULL(s, CAST(NULL AS STRING)), "
            + "IFNULL(d, CAST(NULL AS DECIMAL(12,3))), IFNULL(id, 99) FROM src");
  }

  @Test
  void resultSchemaRetainsFlinksNullabilityAndDecimalScale() throws Exception {
    String sql =
        "SELECT IFNULL(i, 7), IFNULL(s, 'missing'), "
            + "IFNULL(d, CAST(0 AS DECIMAL(12,3))), IFNULL(i, n) FROM src";
    var types = environment().sqlQuery(sql).getResolvedSchema().getColumnDataTypes();
    assertEquals(DataTypes.INT().notNull(), types.get(0));
    assertEquals(tech.streamfusion.compat.FlinkTestCapabilities.ifNullStringType(), types.get(1));
    assertEquals(DataTypes.DECIMAL(12, 3).notNull(), types.get(2));
    assertEquals(DataTypes.BIGINT(), types.get(3));
    NativeParity.assertParity(FlinkIfNullSqlHarnessTest::environment, sql);
  }

  @Test
  void predicateAndTopOneStayNative() throws Exception {
    String sql =
        "SELECT k, id FROM (SELECT IFNULL(n, CAST(-1 AS BIGINT)) AS k, id, "
            + "ROW_NUMBER() OVER (PARTITION BY IFNULL(n, CAST(-1 AS BIGINT)) "
            + "ORDER BY id DESC) AS rn FROM src WHERE IFNULL(i, 0) >= 0) WHERE rn <= 1";
    String plan = NativePlanner.explain(environment(), sql);
    assertTrue(plan.contains("NativeCalc"), plan);
    assertTrue(plan.contains("NativeColumnarTopN"), plan);
    NativeParity.assertChangelogParity(FlinkIfNullSqlHarnessTest::environment, sql);
  }

  @Test
  void replacementStillEvaluatesWhenInputIsPresent() {
    String sql = "SELECT IFNULL(i, 1 / z) FROM src";
    for (boolean nativeRun : new boolean[] {false, true}) {
      TableEnvironment table = environment(Row.of(1, 5, 1L, "a", BigDecimal.ONE, 0, true));
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
        causes.append(cause.getMessage()).append('\n');
      }
      assertTrue(causes.toString().toLowerCase(Locale.ROOT).contains("zero"), causes.toString());
      if (scan != null) {
        assertTrue(scan.substitutions() > 0, scan.fallbackReasons().toString());
      }
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"id", "CAST(id AS BIGINT)", "CAST(id AS DECIMAL(12,3))"})
  void allFilteredRowsSkipFailingScalarReplacement(String input) throws Exception {
    NativeParity.assertKindedParity(
        FlinkIfNullSqlHarnessTest::nullableIds,
        "SELECT IFNULL(" + input + ", 1 / 0) FROM src WHERE id = 99",
        List.of());
  }

  @Test
  void replacementOnlyEvaluatesForSurvivingRows() throws Exception {
    NativeParity.assertParity(
        FlinkIfNullSqlHarnessTest::nullableIds, "SELECT IFNULL(id, 1 / id) FROM src WHERE id = 2");
  }

  private static TableEnvironment nullableIds() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment table = StreamTableEnvironment.create(env);
    table.createTemporaryView(
        "src",
        fromData(
            env,
            Types.ROW_NAMED(new String[] {"id"}, Types.INT),
            Row.of(2),
            Row.of(0),
            Row.of((Object) null)),
        Schema.newBuilder().column("id", DataTypes.INT()).build());
    return table;
  }

  @Test
  void inputIsEvaluatedOnce() throws Exception {
    NativeParity.assertParity(
        () -> {
          TableEnvironment table = environment();
          table.createTemporarySystemFunction("call_sequence", CallSequence.class);
          return table;
        },
        "SELECT id, IFNULL(call_sequence(i), 0) FROM src");
  }

  @Test
  void userFunctionNamedIfNullKeepsItsOwnMeaning() throws Exception {
    NativeParity.assertParity(
        () -> {
          TableEnvironment table = environment();
          table.createTemporarySystemFunction("IFNULL", Replacement.class);
          return table;
        },
        "SELECT id, IFNULL(i, 99) FROM src");
  }

  public static class CallSequence extends ScalarFunction {
    private int calls;

    public Integer eval(Integer value) {
      return ++calls;
    }

    @Override
    public boolean isDeterministic() {
      return false;
    }
  }

  public static class Replacement extends ScalarFunction {
    public Integer eval(Integer value, Integer replacement) {
      return replacement;
    }
  }

  private static TableEnvironment environment() {
    return environment(
        Row.of(0, null, null, null, null, 1, null),
        Row.of(1, 5, 1L, "", new BigDecimal("12.340"), 2, true),
        Row.of(2, 0, null, "\u4e2d\ud83d\ude00", new BigDecimal("-0.005"), 1, false),
        Row.of(
            3, Integer.MIN_VALUE, Long.MIN_VALUE, "a\u0000b",
            new BigDecimal("999999999.999"), 3, null),
        Row.of(
            4, Integer.MAX_VALUE, Long.MAX_VALUE, "x",
            new BigDecimal("-999999999.999"), 4, true),
        Row.of(5, 7, 1L, null, null, 1, false));
  }

  private static TableEnvironment environment(Row... rows) {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment table = StreamTableEnvironment.create(env);
    table.createTemporaryView(
        "src",
        fromData(
            env,
            Types.ROW_NAMED(
                new String[] {"id", "i", "n", "s", "d", "z", "b"},
                Types.INT,
                Types.INT,
                Types.LONG,
                Types.STRING,
                Types.BIG_DEC,
                Types.INT,
                Types.BOOLEAN),
            rows),
        Schema.newBuilder()
            .column("id", DataTypes.INT().notNull())
            .column("i", DataTypes.INT())
            .column("n", DataTypes.BIGINT())
            .column("s", DataTypes.STRING())
            .column("d", DataTypes.DECIMAL(12, 3))
            .column("z", DataTypes.INT())
            .column("b", DataTypes.BOOLEAN())
            .build());
    return table;
  }
}
