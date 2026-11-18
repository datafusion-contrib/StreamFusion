package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static tech.streamfusion.compat.FlinkTestSources.fromData;

import java.math.BigDecimal;
import java.util.List;
import java.util.stream.Stream;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;

class FlinkDecimalTruncateSqlHarnessTest {
  @TestFactory
  Stream<DynamicTest> literalPositionsAndConsumersMatchHostAcrossBatches() {
    return Stream.of(
            "SELECT id, TRUNCATE(a), TRUNCATE(a, -5), TRUNCATE(a, -1), TRUNCATE(a, 1),"
                + " TRUNCATE(a, 2), TRUNCATE(a, 3), TRUNCATE(a, 6), TRUNCATE(a, 38) FROM t",
            "SELECT id, TRUNCATE(a, CAST(NULL AS INT)), TRUNCATE(a, -38), TRUNCATE(a, -39),"
                + " TRUNCATE(a, -100), TRUNCATE(a, 2147483647) FROM t",
            "SELECT id, TRUNCATE(TRUNCATE(a, 2), 1) FROM t WHERE TRUNCATE(a, 2) >= 0",
            "SELECT id, CASE WHEN a < 0 THEN TRUNCATE(a, 2) ELSE TRUNCATE(a, -1) END FROM t",
            "SELECT id, COALESCE(TRUNCATE(a, 2), 7), TRUNCATE(a, 2) IS NULL FROM t",
            "SELECT TRUNCATE(a, 2), COUNT(*) FROM t GROUP BY TRUNCATE(a, 2)",
            "SELECT SUM(TRUNCATE(a, 2)), COUNT(TRUNCATE(a, -1)) FROM t",
            "SELECT TRUNCATE(a, CAST(-2147483648 AS INT)) FROM t WHERE id < 0")
        .map(
            sql ->
                DynamicTest.dynamicTest(
                    sql,
                    () ->
                        NativeParity.assertChangelogParity(
                            FlinkDecimalTruncateSqlHarnessTest::ordinary, sql)));
  }

  @Test
  void truncationAndFloatingCastsComposeInOneNativeProjection() throws Exception {
    NativeParity.assertParity(
        FlinkDecimalTruncateSqlHarnessTest::ordinary,
        "SELECT id, TRUNCATE(a, 2), CAST(a AS FLOAT), CAST(a AS DOUBLE),"
            + " CAST(TRUNCATE(a, 2) AS DOUBLE),"
            + " TRUNCATE(CAST(CAST(a AS DOUBLE) AS DECIMAL(7,3)), 2) FROM t");
  }

  @Test
  void preservesFlinksDeclaredResultTypes() throws Exception {
    String sql = "SELECT TRUNCATE(a), TRUNCATE(a, -1), TRUNCATE(a, 2), TRUNCATE(a, 6) FROM t";
    assertEquals(
        List.of(
            DataTypes.DECIMAL(5, 0),
            DataTypes.DECIMAL(5, 0),
            DataTypes.DECIMAL(7, 2),
            DataTypes.DECIMAL(7, 3)),
        ordinary().sqlQuery(sql).getResolvedSchema().getColumnDataTypes());
    NativeParity.assertParity(FlinkDecimalTruncateSqlHarnessTest::ordinary, sql);
  }

  @TestFactory
  Stream<DynamicTest> precision38MatchesHost() {
    return Stream.of(0, 3, 38)
        .map(
            scale ->
                DynamicTest.dynamicTest(
                    "scale=" + scale,
                    () -> {
                      String max =
                          scale == 38
                              ? "0." + "9".repeat(38)
                              : "9".repeat(38 - scale)
                                  + (scale == 0 ? "" : "." + "9".repeat(scale));
                      NativeParity.assertParity(
                          () -> decimals(38, scale, 7, max, "-" + max, "0", null),
                          "SELECT id, TRUNCATE(a), TRUNCATE(a, 2), TRUNCATE(a, -1), TRUNCATE(a,"
                              + " -38), TRUNCATE(a, -39), TRUNCATE(a, 37), TRUNCATE(a, 38) FROM t");
                    }));
  }

  @Test
  void extremeScalePreservesRuntimeFailure() {
    NativeFailureParity.run(
            () -> decimals(7, 3, 1, "1.234"), "SELECT TRUNCATE(a, CAST(-2147483648 AS INT)) FROM t")
        .assertFailure(
            ArithmeticException.class,
            "Underflow",
            NativeFailureParity.Phase.ROW_EVALUATION,
            NativeFailureParity.Route.NATIVE);
  }

  @Test
  void extremeScaleKeepsCaseAndBooleanShortCircuiting() throws Exception {
    NativeParity.assertParity(
        FlinkDecimalTruncateSqlHarnessTest::ordinary,
        "SELECT CASE WHEN id < 0 THEN TRUNCATE(a, CAST(-2147483648 AS INT)) ELSE a END FROM t");
    NativeParity.assertFallbackReasonContains(
        FlinkDecimalTruncateSqlHarnessTest::ordinary,
        "SELECT id >= 0 OR TRUNCATE(a, CAST(-2147483648 AS INT)) = 0 FROM t",
        "short-circuit");
  }

  @Test
  void identityScaleRetainsCompactParserRoundingCarry() throws Exception {
    NativeParity.assertParity(
        () -> {
          var env = StreamExecutionEnvironment.getExecutionEnvironment();
          env.setParallelism(1);
          var table = StreamTableEnvironment.create(env);
          table.createTemporaryView(
              "t",
              fromData(
                  env,
                  Types.ROW_NAMED(new String[] {"s"}, Types.STRING),
                  Row.of("999.995"),
                  Row.of("-999.995"),
                  Row.of("bad")));
          return table;
        },
        "SELECT TRUNCATE(TRY_CAST(s AS DECIMAL(5,2)), 2),"
            + " TRUNCATE(TRY_CAST(s AS DECIMAL(5,2)), 5) FROM t");
  }

  private static TableEnvironment ordinary() {
    return decimals(
        7, 3, 2051, "1.235", "-1.235", "0.005", "-0.005", "9999.999", "-9999.999", "0", null);
  }

  private static TableEnvironment decimals(int precision, int scale, int count, String... values) {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    var table = StreamTableEnvironment.create(env);
    Row[] rows = new Row[count];
    for (int i = 0; i < count; i++) {
      String value = values[i % values.length];
      rows[i] = Row.of(i, value == null ? null : new BigDecimal(value));
    }
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
