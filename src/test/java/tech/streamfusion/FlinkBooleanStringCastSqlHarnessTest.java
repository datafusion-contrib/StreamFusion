package tech.streamfusion;

import static org.apache.flink.table.api.DataTypes.*;
import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import java.util.stream.Stream;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.runtime.typeutils.ExternalTypeInfo;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.planner.NativePlanner;

class FlinkBooleanStringCastSqlHarnessTest {
  @ParameterizedTest
  @MethodSource("castModes")
  void runtimeBooleanCastsConsumeNativeWorkAcrossBatches(String operator, boolean legacy) throws Exception {
    String sql = "SELECT id,CAST(b AS STRING),CAST(b AS VARCHAR(1)),CAST(b AS VARCHAR(3)),"
        + "CAST(b AS VARCHAR(4)),CAST(b AS VARCHAR(5)),CAST(b AS VARCHAR(8)),"
        + "CAST(b AS CHAR(1)),CAST(b AS CHAR(3)),CAST(b AS CHAR(4)),CAST(b AS CHAR(5)),"
        + "CAST(b AS CHAR(8)),CAST(b AS CHAR(32)) FROM src";
    sql = sql.replace("CAST(", operator + "(");
    assertRuntimeBooleanCasts(sql, legacy);
  }

  @ParameterizedTest
  @MethodSource("castModes")
  void logicalBooleanCastsConsumeNativeWorkAcrossBatches(String operator, boolean legacy) throws Exception {
    assertRuntimeBooleanCasts(
        ("SELECT id,CAST(NOT b AS STRING),CAST(b AND c AS CHAR(8)),"
            + "CAST(b OR c AS VARCHAR(3)),CAST(b IS TRUE AS STRING),"
            + "CAST(b IS NOT TRUE AS CHAR(8)),CAST(b IS FALSE AS VARCHAR(4)),"
            + "CAST(b IS NOT FALSE AS STRING),CAST(b IS NULL AS CHAR(8)),"
            + "CAST(b IS NOT NULL AS STRING),CAST((NOT b) OR (c IS TRUE) AS CHAR(8)),"
            + "CAST(b AND c AND (b OR NOT c) AS STRING) FROM src")
                .replace("CAST(", operator + "("), legacy);
  }

  @ParameterizedTest
  @ValueSource(strings = {"TINYINT", "SMALLINT", "INT", "BIGINT"})
  void integerComparisonCastsConsumeNativeWorkAcrossBatches(String type) throws Exception {
    String sql = ("SELECT id,CAST(n=m AS STRING),CAST(n<>m AS VARCHAR(3)),"
        + "CAST(n<m AS CHAR(8)),CAST(n<=m AS STRING),CAST(n>m AS VARCHAR(4)),"
        + "CAST(n>=m AS CHAR(8)),CAST(NOT(n<m) AS STRING),"
        + "CAST(n>=m OR n=m AS CHAR(8)),CAST((n<m) IS NULL AS STRING),"
        + "CAST((n<m) AND (n>m OR n=m) AS CHAR(8)),"
        + "CAST(n+CAST(1 AS $TYPE)<m-CAST(1 AS $TYPE) AS CHAR(8)),"
        + "CAST(CAST(n AS BIGINT)<CAST(m AS BIGINT) AS STRING) FROM src").replace("$TYPE", type);
    for (boolean legacy : new boolean[] {false, true}) {
      for (boolean nullable : new boolean[] {false, true}) {
        var integer = type.equals("TINYINT") ? TINYINT() : type.equals("SMALLINT") ? SMALLINT()
            : type.equals("BIGINT") ? BIGINT() : INT();
        if (!nullable) integer = integer.notNull();
        var rowType = ROW(FIELD("id", INT().notNull()), FIELD("n", integer), FIELD("m", integer));
        var rows = new ArrayList<Row>();
        for (int id = 0; id < 5003; id++) {
          rows.add(Row.of(id, nullable && id % 7 == 0 ? null : integerValue(type, id),
              nullable && id % 5 == 0 ? null : integerValue(type, (id * 13 + 17) % 5003)));
        }
        assertRuntimeCasts(sql, legacy, rowType, rows);
      }
    }
  }

  private static Object integerValue(String type, int row) {
    long value = (row % 2 == 0 ? -1L : 1L) * row * 7919L;
    switch (type) {
      case "TINYINT": return row % 17 == 0 ? Byte.MIN_VALUE
          : row % 17 == 1 ? Byte.MAX_VALUE : (byte) value;
      case "SMALLINT": return row % 17 == 0 ? Short.MIN_VALUE
          : row % 17 == 1 ? Short.MAX_VALUE : (short) value;
      case "INT": return row % 17 == 0 ? Integer.MIN_VALUE
          : row % 17 == 1 ? Integer.MAX_VALUE : (int) value;
      case "BIGINT": return row % 17 == 0 ? Long.MIN_VALUE
          : row % 17 == 1 ? Long.MAX_VALUE : (row % 2 == 0 ? -1L : 1L) * (9007199254740993L + row * 7919L);
      default: throw new IllegalArgumentException(type);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void integerComparisonConsumersAndEmptyBranchesMatchFlink(boolean legacy) throws Exception {
    java.util.function.Supplier<TableEnvironment> factory = () -> {
      var table = environment();
      table.getConfig().set("table.exec.legacy-cast-behaviour", legacy ? "ENABLED" : "DISABLED");
      return table;
    };
    for (String sql : List.of(
        "SELECT n,CHAR_LENGTH(CAST(n<1 AS CHAR(8))),CONCAT(CAST(n>=1 AS STRING),'!') FROM src",
        "SELECT CAST(n<1 AS CHAR(8)),COUNT(*) FROM src GROUP BY CAST(n<1 AS CHAR(8))",
        "SELECT CAST(n<1 AS CHAR(8)) FROM src WHERE n=99",
        "SELECT CASE WHEN n>=0 THEN CAST(n<1 AS CHAR(8)) ELSE CAST(n/(n-n)>0 AS CHAR(8)) END FROM src")) {
      BuiltinFunctionParity.assertParity(factory, sql);
    }
  }

  private static void assertRuntimeBooleanCasts(String sql, boolean legacy) throws Exception {
    for (boolean nullable : new boolean[] {false, true}) {
      var bool = nullable ? BOOLEAN() : BOOLEAN().notNull();
      var rowType = ROW(FIELD("id", INT().notNull()), FIELD("b", bool), FIELD("c", bool));
      var rows = new ArrayList<Row>();
      for (int id = 0; id < 5003; id++)
        rows.add(Row.of(id, nullable && id % 7 == 0 ? null : id % 2 == 0,
            nullable && id % 5 == 0 ? null : id % 3 == 0));
      assertRuntimeCasts(sql, legacy, rowType, rows);
    }
  }

  private static void assertRuntimeCasts(String sql, boolean legacy,
      org.apache.flink.table.types.DataType rowType, List<Row> rows) throws Exception {
    TreeMap<Integer, List<Object>> stock = null;
    List<org.apache.flink.table.types.DataType> stockTypes = null;
    for (boolean nativeRun : new boolean[] {false, true}) {
      try (var metrics = new RecoveryExecutionMetrics()) {
        var env = metrics.environment(new org.apache.flink.configuration.Configuration(), 1, 0);
        var table = StreamTableEnvironment.create(env);
        table.getConfig().set("table.exec.legacy-cast-behaviour", legacy ? "ENABLED" : "DISABLED");
        table.createTemporaryView("src", tech.streamfusion.compat.FlinkTestSources.fromData(env,
            rows, ExternalTypeInfo.<Row>of(rowType)),
            Schema.newBuilder().fromRowDataType(rowType).build());
        var scan = nativeRun ? NativePlanner.install(table) : null;
        var types = table.sqlQuery(sql).getResolvedSchema().getColumnDataTypes();
        if (nativeRun) {
          assertEquals(stockTypes, types);
          String plan = table.explainSql(sql);
          assertTrue(plan.contains("NativeCalc"), plan);
          assertTrue(scan.fallbackReasons().isEmpty(), scan.fallbackReasons().toString());
          assertFalse(scan.functionExecutions().isEmpty());
          assertTrue(scan.functionExecutions().stream().allMatch(route -> route.contains("jvm=[]")),
              scan.functionExecutions().toString());
        } else stockTypes = types;
        var actual = new TreeMap<Integer, List<Object>>();
        try (var output = table.executeSql(sql).collect()) {
          while (output.hasNext()) {
            Row row = output.next();
            int id = (Integer) row.getField(0);
            assertTrue(id >= 0 && id < rows.size());
            var values = new ArrayList<Object>();
            for (int field = 1; field < row.getArity(); field++) values.add(row.getField(field));
            assertNull(actual.put(id, values), "duplicate id " + id);
          }
        }
        assertEquals(rows.size(), actual.size());
        if (nativeRun) {
          assertEquals(stock, actual);
          RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeCalc");
        } else stock = actual;
      }
    }
  }

  @ParameterizedTest
  @MethodSource("castModes")
  void stringConsumersRetainTheReleasedRepresentationRules(String operator, boolean legacy) throws Exception {
    java.util.function.Supplier<TableEnvironment> factory = () -> {
      var table = BuiltinFunctionParity.environment(
          ROW(FIELD("b", BOOLEAN()), FIELD("n", INT()), FIELD("trim_set", STRING())),
          List.of(Row.of(true, 0, ""), Row.of(false, 1, "F"), Row.of(null, 2, null)));
      table.getConfig().set("table.exec.legacy-cast-behaviour", legacy ? "ENABLED" : "DISABLED");
      return table;
    };
    BuiltinFunctionParity.assertParity(factory,
        ("SELECT n,CHAR_LENGTH(CAST(b AS CHAR(8))),CHAR_LENGTH(CAST(b AS STRING)),"
            + "CONCAT(CAST(b AS VARCHAR(3)),'!'),REPLACE(CAST(b AS STRING),'F','f'),"
            + "CAST(b AS STRING) LIKE '%R%',UPPER(CAST(b AS CHAR(8))),"
            + "LTRIM(CAST(b AS STRING),trim_set),LTRIM(CAST(b AS STRING),'') FROM src")
                .replace("CAST(", operator + "("));
  }

  @ParameterizedTest
  @MethodSource("castModes")
  void computedFailingOperandsRemainLazyAndErrorsAreNotDiscarded(String operator, boolean legacy) throws Exception {
    java.util.function.Supplier<TableEnvironment> factory = () -> {
      var table = environment();
      table.getConfig().set("table.exec.legacy-cast-behaviour", legacy ? "ENABLED" : "DISABLED");
      return table;
    };
    BuiltinFunctionParity.assertParity(factory,
        "SELECT CASE WHEN n>=0 THEN CAST(b AS CHAR(8)) ELSE CAST(n/(n-n)=1 AS CHAR(8)) END FROM src"
            .replace("CAST(", operator + "("));
    BuiltinFunctionParity.assertParity(factory,
        "SELECT CAST(n/(n-n)=1 AS STRING) FROM src WHERE n=99".replace("CAST(", operator + "("));
    String shortCircuit = "SELECT CAST(n>=0 OR n/(n-n)=1 AS CHAR(8)) FROM src"
        .replace("CAST(", operator + "(");
    if (operator.equals("CAST"))
      NativeParity.assertFallbackReasonContains(factory, shortCircuit,
          "Fallible expressions under AND/OR");
    else BuiltinFunctionParity.assertParity(factory, shortCircuit);
    BuiltinFunctionParity.assertParity(factory,
        "SELECT CAST(NOT(n/(n-n)=1) AS CHAR(8)) FROM src WHERE n=99"
            .replace("CAST(", operator + "("));
    NativeFailureParity.run(factory, "SELECT CAST(n/(n-n)=1 AS STRING) FROM src"
            .replace("CAST(", operator + "("))
        .assertCompetingFailures(NativeFailureParity::isDivisionByZero,
            NativeFailureParity.Phase.ROW_EVALUATION, NativeFailureParity.Route.NATIVE);
    NativeFailureParity.run(factory, "SELECT CAST(NOT(n/(n-n)=1) AS CHAR(8)) FROM src"
            .replace("CAST(", operator + "("))
        .assertCompetingFailures(NativeFailureParity::isDivisionByZero,
            NativeFailureParity.Phase.ROW_EVALUATION, NativeFailureParity.Route.NATIVE);
  }

  @Test
  void castsPreserveCaseLengthsPaddingAndNulls() throws Exception {
    BuiltinFunctionParity.assertParity(
        this::environment,
        "SELECT CAST(b AS STRING), CAST(b AS VARCHAR(2)), CAST(b AS VARCHAR(5)), "
            + "CAST(b AS CHAR(2)), CAST(b AS CHAR(8)), TRY_CAST(b AS VARCHAR(1)) FROM src");
  }

  @Test
  void consumersAndEmptyBatchesMatchFlink() throws Exception {
    BuiltinFunctionParity.assertParity(
        this::environment,
        "SELECT CONCAT(CAST(b AS STRING), '!') FROM src WHERE CAST(b AS VARCHAR(1)) = 'T'");
    BuiltinFunctionParity.assertParity(
        this::environment, "SELECT CAST(b AS CHAR(8)) FROM src WHERE n = 99");
  }

  private TableEnvironment environment() {
    return BuiltinFunctionParity.environment(
        ROW(FIELD("b", BOOLEAN()), FIELD("n", INT())),
        List.of(Row.of(true, 0), Row.of(false, 1), Row.of(null, 2)));
  }

  private static Stream<Arguments> castModes() {
    return Stream.of(Arguments.of("CAST", false), Arguments.of("CAST", true),
        Arguments.of("TRY_CAST", false), Arguments.of("TRY_CAST", true));
  }
}
