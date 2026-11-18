package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static tech.streamfusion.compat.FlinkTestSources.fromData;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;
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

class FlinkIfSqlHarnessTest {
  @ParameterizedTest
  @ValueSource(
      strings = {
        "IF(flag, i, 7)",
        "IF(flag, CAST(i AS TINYINT), CAST(i AS TINYINT))",
        "IF(flag, CAST(i AS SMALLINT), CAST(i AS SMALLINT))",
        "IF(flag, CAST(i AS FLOAT), CAST(i AS FLOAT))",
        "IF(flag, CAST(i AS DOUBLE), CAST(i AS DOUBLE))",
        "IF(flag, CAST(t AS TIME), CAST(t AS TIME))",
        "IF(flag, n, CAST(-1 AS BIGINT))",
        "IF(flag, d, CAST(1.234 AS DECIMAL(20,3)))",
        "IF(flag, s, other)",
        "IF(flag, t, CAST('1969-12-31 23:59:59.999999999' AS TIMESTAMP(9)))",
        "IF(flag, CAST(t AS TIMESTAMP(3)), CAST(t AS TIMESTAMP(3)))",
        "IF(flag, CAST(t AS DATE), DATE '2000-01-01')",
        "IF(flag, `bytes`, `bytes`)",
        "IF(flag, i, n)",
        "IF(flag, n, d)",
        "IF(flag, CAST(d AS DECIMAL(12,1)), CAST(d AS DECIMAL(20,3)))",
        "IF(flag, CAST(s AS CHAR(8)), CAST(other AS VARCHAR(20)))",
        "IF(flag, CAST(NULL AS INT), i)",
        "IF(flag, IF(i > 0, s, other), IF(i IS NULL, 'missing', s))"
      })
  void resolvedValuesAndTypesMatchFlink(String expression) throws Exception {
    org.junit.jupiter.api.Assumptions.assumeTrue(
        tech.streamfusion.compat.FlinkTestCapabilities.NEGATIVE_TIMESTAMP_TO_TIME
            || !expression.contains("AS TIME)"),
        "Flink 1.18 external TIME conversion rejects a negative pre-epoch fraction");
    NativeParity.assertParity(
        FlinkIfSqlHarnessTest::environment, "SELECT id, " + expression + " FROM src");
  }

  @Test
  void nullConditionsSelectTheElseBranch() throws Exception {
    NativeParity.assertKindedParity(
        FlinkIfSqlHarnessTest::environment,
        "SELECT IF(flag, 1, 2) FROM src WHERE flag IS NULL",
        List.of(List.of("+I", 2), List.of("+I", 2)));
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "IF(id < 0, 1 / (id - id), id)",
        "IF(id >= 0, id, 1 / (id - id))",
        "IF(flag, i, 1 / (id + 1))"
      })
  void unselectedRuntimeErrorsAreNotEvaluated(String expression) throws Exception {
    NativeParity.assertParity(
        FlinkIfSqlHarnessTest::environment, "SELECT " + expression + " FROM src");
  }

  @Test
  void selectedRuntimeErrorsStillFail() {
    var comparison =
        NativeFailureParity.run(
            FlinkIfSqlHarnessTest::environment, "SELECT IF(id >= 0, 1 / (id - id), id) FROM src");
    for (var outcome : List.of(comparison.host(), comparison.nativeRun())) {
      assertTrue(outcome.failure() != null, outcome.toString());
      assertTrue(
          outcome.rootCause().getMessage().toLowerCase(java.util.Locale.ROOT).contains("zero"),
          outcome.toString());
      assertEquals(NativeFailureParity.Phase.ROW_EVALUATION, outcome.phase());
    }
    assertEquals(NativeFailureParity.Route.NATIVE, comparison.nativeRun().route());
  }

  @Test
  void allFilteredRowsSkipFailingBranches() throws Exception {
    NativeParity.assertKindedParity(
        FlinkIfSqlHarnessTest::environment,
        "SELECT IF(flag, 1 / (id - id), id) FROM src WHERE s = 'absent'",
        List.of());
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "SELECT id, IF(MOD(id, 2) = 0, tick(id), 99) FROM src",
        "SELECT id, IF(MOD(id, 2) = 0, 99, null_tick(id)) FROM src",
        "SELECT id, IF(MOD(id, 2) = 0, text_tick(id), 'missing') FROM src",
        "SELECT id, IF(MOD(id, 2) = 0, RAND_INTEGER(42, 100), 99) FROM src",
        "SELECT id FROM src WHERE IF(MOD(id, 2) = 0, tick(id), 99) > 1"
      })
  void statefulAndVolatileBranchesMatchAcrossBatches(String sql) throws Exception {
    NativeParity.assertParity(() -> FlinkCoalesceEvaluationSqlHarnessTest.environment(5003), sql);
  }

  @Test
  void composesWithNativeTopNAndFilter() throws Exception {
    String sql =
        "SELECT id, s FROM (SELECT id, s, ROW_NUMBER() OVER "
            + "(PARTITION BY IF(flag, i, 0) ORDER BY id DESC) AS rn FROM src "
            + "WHERE IF(flag, i, 1) > 0) WHERE rn <= 1";
    String plan = NativePlanner.explain(environment(), sql);
    assertTrue(plan.contains("NativeCalc"), plan);
    assertTrue(plan.contains("NativeColumnarTopN"), plan);
    NativeParity.assertChangelogParity(FlinkIfSqlHarnessTest::environment, sql);
  }

  @Test
  void sameNamedUserFunctionKeepsItsImplementation() throws Exception {
    NativeParity.assertKindedParity(
        () -> {
          TableEnvironment table = environment();
          table.createTemporarySystemFunction("IF", new NamedIf());
          return table;
        },
        "SELECT `IF`(flag, i, 0) FROM src WHERE id = 0",
        List.of(List.of("+I", 12345)));
  }

  @Test
  void mixedDecimalResultRetainsDeclaredScale() throws Exception {
    String sql = "SELECT IF(flag, CAST(d AS DECIMAL(12,1)), CAST(d AS DECIMAL(20,3))) FROM src";
    var hostType = environment().sqlQuery(sql).getResolvedSchema().getColumnDataTypes();
    TableEnvironment table = environment();
    NativePlanner.install(table);
    assertEquals(hostType, table.sqlQuery(sql).getResolvedSchema().getColumnDataTypes());
    NativeParity.assertParity(FlinkIfSqlHarnessTest::environment, sql);
  }

  public static class NamedIf extends ScalarFunction {
    public Integer eval(Boolean condition, Integer left, Integer right) {
      return 12345;
    }
  }

  private static TableEnvironment environment() {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    var table = StreamTableEnvironment.create(env);
    var timestamp = LocalDateTime.of(1969, 12, 31, 23, 59, 59, 999999999);
    table.createTemporaryView(
        "src",
        fromData(
            env,
            Types.ROW_NAMED(
                new String[] {"id", "flag", "i", "n", "d", "s", "other", "t", "bytes"},
                Types.INT,
                Types.BOOLEAN,
                Types.INT,
                Types.LONG,
                Types.BIG_DEC,
                Types.STRING,
                Types.STRING,
                Types.LOCAL_DATE_TIME,
                Types.PRIMITIVE_ARRAY(Types.BYTE)),
            Row.of(
                0,
                true,
                2,
                Long.MAX_VALUE,
                new BigDecimal("123.456"),
                "abc",
                "other",
                timestamp,
                new byte[] {0, -1}),
            Row.of(
                1,
                false,
                -3,
                Long.MIN_VALUE,
                new BigDecimal("-123.456"),
                "",
                "\ud83d\ude00",
                timestamp,
                new byte[0]),
            Row.of(2, null, 0, 0L, BigDecimal.ZERO, "\u4e2d", "", timestamp, null),
            Row.of(3, true, null, null, null, null, "missing", null, null),
            Row.of(4, false, 4, 4L, new BigDecimal("999999999.999"), "x", null, null, null),
            Row.of(5, null, null, null, null, null, null, null, null)),
        Schema.newBuilder()
            .column("id", DataTypes.INT())
            .column("flag", DataTypes.BOOLEAN())
            .column("i", DataTypes.INT())
            .column("n", DataTypes.BIGINT())
            .column("d", DataTypes.DECIMAL(20, 3))
            .column("s", DataTypes.STRING())
            .column("other", DataTypes.STRING())
            .column("t", DataTypes.TIMESTAMP(9))
            .column("bytes", DataTypes.BYTES())
            .build());
    return table;
  }
}
