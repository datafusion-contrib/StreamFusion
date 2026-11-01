package tech.streamfusion;

import static org.apache.flink.table.api.DataTypes.*;
import static org.junit.jupiter.api.Assertions.*;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.runtime.typeutils.ExternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.planner.NativePlanner;

class FlinkNativeNumericClassificationSqlHarnessTest {
  static java.util.stream.Stream<Arguments> functionsAndTypes() {
    return java.util.stream.Stream.of("IS_ALPHA", "IS_DIGIT").flatMap(function ->
        java.util.stream.Stream.of("TINYINT", "SMALLINT", "INT", "BIGINT", "DECIMAL(18,0)",
            "DECIMAL(18,2)", "DECIMAL(38,18)", "DECIMAL(38,38)")
            .map(type -> Arguments.of(function, type)));
  }

  @ParameterizedTest
  @MethodSource("functionsAndTypes")
  void numericInputsNullsAndBooleanConsumersMatchReleasedSql(String function, String declaration) throws Exception {
    for (boolean nullable : new boolean[] {false, true}) {
      var type = type(declaration);
      if (!nullable) type = type.notNull();
      var rowType = ROW(FIELD("id", INT().notNull()), FIELD("v", type));
      List<Row> input = new ArrayList<>();
      for (int id = 0; id < 5003; id++)
        input.add(Row.of(id, nullable && id % 7 == 0 ? null : value(declaration, id)));
      String sql = "SELECT id,IS_ALPHA(v),IS_ALPHA(CAST(NULL AS " + declaration + "))"
          + ",IS_ALPHA(CAST(1 AS " + declaration + ")),NOT IS_ALPHA(v)"
          + ",CASE WHEN id>=0 THEN IS_ALPHA(v) ELSE TRUE END FROM src";
      sql = sql.replace("IS_ALPHA", function);
      boolean expected = function.equals("IS_DIGIT") && !declaration.startsWith("DECIMAL");
      for (boolean nativeEnabled : new boolean[] {false, true}) {
        try (var metrics = new RecoveryExecutionMetrics()) {
          var env = metrics.environment(new org.apache.flink.configuration.Configuration(), 1, 0);
          var table = StreamTableEnvironment.create(env);
          table.createTemporaryView("src", tech.streamfusion.compat.FlinkTestSources.fromData(env,
              input, ExternalTypeInfo.<Row>of(rowType)),
              Schema.newBuilder().fromRowDataType(rowType).build());
          var scan = nativeEnabled ? NativePlanner.install(table) : null;
          var schema = table.sqlQuery(sql).getResolvedSchema();
          for (int field = 1; field < schema.getColumnCount(); field++)
            assertEquals(BOOLEAN().notNull(), schema.getColumnDataTypes().get(field));
          String plan = table.explainSql(sql);
          if (nativeEnabled) {
            assertTrue(plan.contains("NativeCalc"), plan);
            assertTrue(scan.fallbackReasons().isEmpty(), scan.fallbackReasons().toString());
            assertFalse(scan.functionExecutions().isEmpty());
            assertTrue(scan.functionExecutions().stream().allMatch(route -> route.contains("jvm=[]")),
                scan.functionExecutions().toString());
          }
          int count = 0;
          boolean[] seen = new boolean[input.size()];
          try (var output = table.executeSql(sql).collect()) {
            while (output.hasNext()) {
              var row = output.next();
              int id = (Integer) row.getField(0);
              assertTrue(id >= 0 && id < seen.length);
              assertFalse(seen[id], "duplicate id " + id);
              seen[id] = true;
              for (int field = 1; field < row.getArity(); field++)
                assertEquals(field == 4 ? !expected : expected, row.getField(field),
                    function + " " + declaration + " id=" + id + " field=" + field);
              count++;
            }
          }
          assertEquals(input.size(), count);
          if (nativeEnabled) RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeCalc");
        }
      }
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"IS_ALPHA", "IS_DIGIT"})
  void filtersGroupingAndComposedOperandsMatchReleasedSql(String function) throws Exception {
    for (String sql : List.of(
        "SELECT id,IS_ALPHA(v+1),IS_ALPHA(CAST(v AS DECIMAL(18,2))) FROM src",
        "SELECT id FROM src WHERE IS_ALPHA(v)",
        "SELECT id FROM src WHERE NOT IS_ALPHA(v)",
        "SELECT IS_ALPHA(v),COUNT(*) FROM src GROUP BY IS_ALPHA(v)",
        "SELECT id,IS_ALPHA(v),v IS NULL FROM src WHERE id<0"))
      BuiltinFunctionParity.assertParity(this::environment, sql.replace("IS_ALPHA", function));
  }

  @ParameterizedTest
  @ValueSource(strings = {"IS_ALPHA", "IS_DIGIT"})
  void failingOperandsAreNotDiscardedAndUnselectedBranchesStayLazy(String function) throws Exception {
    BuiltinFunctionParity.assertParity(this::environment,
        "SELECT CASE WHEN id>=0 THEN IS_ALPHA(v) ELSE IS_ALPHA(v/(id-id)) END FROM src".replace("IS_ALPHA", function));
    BuiltinFunctionParity.assertParity(this::environment,
        "SELECT IS_ALPHA(v/(id-id)) FROM src WHERE id<0".replace("IS_ALPHA", function));
    NativeFailureParity.run(this::environment, "SELECT IS_ALPHA(v/(id-id)) FROM src".replace("IS_ALPHA", function))
        .assertCompetingFailures(NativeFailureParity::isDivisionByZero,
            NativeFailureParity.Phase.ROW_EVALUATION, NativeFailureParity.Route.NATIVE);
  }

  private TableEnvironment environment() {
    return BuiltinFunctionParity.environment(ROW(FIELD("id", INT().notNull()), FIELD("v", INT())),
        List.of(Row.of(0, 2), Row.of(1, -2), Row.of(2, null)));
  }

  private static DataType type(String declaration) {
    switch (declaration) {
      case "TINYINT": return TINYINT();
      case "SMALLINT": return SMALLINT();
      case "INT": return INT();
      case "BIGINT": return BIGINT();
      case "DECIMAL(18,0)": return DECIMAL(18, 0);
      case "DECIMAL(18,2)": return DECIMAL(18, 2);
      case "DECIMAL(38,18)": return DECIMAL(38, 18);
      case "DECIMAL(38,38)": return DECIMAL(38, 38);
      default: throw new IllegalArgumentException(declaration);
    }
  }

  private static Object value(String declaration, int id) {
    long number = id % 17 == 0 ? Long.MIN_VALUE : id % 17 == 1 ? Long.MAX_VALUE
        : id % 2 == 0 ? -id * 7919L : 9007199254740993L + id * 7919L;
    switch (declaration) {
      case "TINYINT": return (byte) number;
      case "SMALLINT": return (short) number;
      case "INT": return (int) number;
      case "BIGINT": return number;
      default:
        int precision = declaration.startsWith("DECIMAL(18") ? 18 : 38;
        int scale = declaration.endsWith(",0)") ? 0 : declaration.endsWith(",2)") ? 2
            : declaration.endsWith(",18)") ? 18 : 38;
        BigInteger unscaled = id % 17 == 0 ? BigInteger.TEN.pow(precision).subtract(BigInteger.ONE)
            : id % 17 == 1 ? BigInteger.TEN.pow(precision).negate().add(BigInteger.ONE)
            : BigInteger.valueOf(number);
        return new BigDecimal(unscaled, scale);
    }
  }
}
