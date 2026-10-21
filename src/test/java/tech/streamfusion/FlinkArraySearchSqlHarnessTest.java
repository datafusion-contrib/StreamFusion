package tech.streamfusion;

import static org.apache.flink.table.api.DataTypes.*;
import static org.junit.jupiter.api.Assertions.*;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.planner.NativePlanner;

class FlinkArraySearchSqlHarnessTest {
  @ParameterizedTest
  @ValueSource(strings = {"BOOLEAN", "TINYINT", "SMALLINT", "INT", "BIGINT", "DECIMAL"})
  void basicSearchesExecuteInRustAndMatchFlink(String kind) throws Exception {
    requireHost();
    String constant = kind.equals("BOOLEAN") ? "TRUE" : "CAST(7 AS " + sqlType(kind) + ")";
    String sql = "SELECT id, ARRAY_CONTAINS(a,n), ARRAY_POSITION(a,n),"
        + " ARRAY_POSITION(a," + constant + "),"
        + " ARRAY_CONTAINS(a,CAST(NULL AS " + sqlType(kind) + ")),"
        + " ARRAY_POSITION(a,CAST(NULL AS " + sqlType(kind) + ")) FROM src";
    var table = environment(kind);
    var scan = NativePlanner.install(table);
    String plan = table.explainSql(sql);
    assertTrue(plan.contains("NativeCalc"), plan);
    assertTrue(scan.functionExecutions().stream().anyMatch(execution ->
        execution.contains("native=[ARRAY_CONTAINS, ARRAY_POSITION]")
            && execution.contains("jvm=[]")), scan.functionExecutions().toString());
    BuiltinFunctionParity.assertParity(() -> environment(kind), sql);
    BuiltinFunctionParity.assertParity(() -> environment(kind),
        "SELECT id, ARRAY_POSITION(a,n) FROM src WHERE ARRAY_CONTAINS(a,n) IS TRUE");
    BuiltinFunctionParity.assertParity(() -> environment(kind),
        "SELECT ARRAY_CONTAINS(a,n), ARRAY_POSITION(a,n) FROM src WHERE id < 0");
  }

  @ParameterizedTest
  @ValueSource(strings = {"BOOLEAN", "TINYINT", "SMALLINT", "INT", "BIGINT", "DECIMAL"})
  void constantContainsPreservesReleasedFlinkNullElementBehavior(String kind) {
    requireHost();
    String constant = kind.equals("BOOLEAN") ? "TRUE" : "CAST(7 AS " + sqlType(kind) + ")";
    String sql = "SELECT id, ARRAY_CONTAINS(a," + constant + ") FROM src";
    var table = environment(kind);
    var scan = NativePlanner.install(table);
    table.explainSql(sql);
    if (kind.equals("DECIMAL")) {
      assertTrue(scan.fallbackReasons().stream().anyMatch(reason ->
          reason.contains("binary DECIMAL array getter")), scan.fallbackReasons().toString());
    } else {
      assertTrue(scan.functionExecutions().stream().anyMatch(execution ->
          execution.contains("native=[]") && execution.contains("ARRAY_CONTAINS")),
          scan.functionExecutions().toString());
    }
    var runs = NativeFailureParity.run(() -> environment(kind), sql);
    var route = kind.equals("DECIMAL")
        ? NativeFailureParity.Route.FALLBACK : NativeFailureParity.Route.NATIVE;
    if (runs.host().failure() != null) {
      runs.assertFailure(runs.host().rootCause().getClass(), runs.host().phase(),
          route);
    } else {
      runs.assertSuccess(route);
    }
    NativeFailureParity.run(() -> environment(kind), sql + " WHERE id < 0").assertSuccess(route);
  }

  @ParameterizedTest
  @ValueSource(strings = {"ARRAY_CONTAINS", "ARRAY_POSITION"})
  void fallibleArgumentsAndEmptyFilteredBatchesKeepFlinkEvaluation(String function) {
    requireHost();
    String projection = function + "(a,1/id)";
    var failed = NativeFailureParity.run(() -> environment("INT"),
        "SELECT " + projection + " FROM src");
    assertNotNull(failed.host().failure());
    failed.assertFailure(failed.host().rootCause().getClass(), failed.host().phase(),
        NativeFailureParity.Route.NATIVE);
    NativeFailureParity.run(() -> environment("INT"),
        "SELECT " + projection + " FROM src WHERE id < 0")
        .assertSuccess(NativeFailureParity.Route.NATIVE);
    NativeFailureParity.run(() -> environment("INT"),
        "SELECT CASE WHEN id=0 THEN NULL ELSE " + projection + " END FROM src")
        .assertSuccess(NativeFailureParity.Route.NATIVE);
  }

  @ParameterizedTest
  @ValueSource(strings = {"STRING", "NOT_NULL"})
  void representationSensitiveSearchesKeepTheGeneratedEvaluator(String kind) throws Exception {
    requireHost();
    var element = kind.equals("STRING") ? STRING() : INT().notNull();
    Object values = kind.equals("STRING") ? new String[] {"a", "b"} : new Integer[] {1, 2};
    Object needle = kind.equals("STRING") ? "b" : 2;
    java.util.function.Supplier<TableEnvironment> factory = () -> BuiltinFunctionParity.environment(
        ROW(FIELD("a", ARRAY(element)), FIELD("n", element)), List.of(Row.of(values, needle)));
    var table = factory.get();
    var scan = NativePlanner.install(table);
    String sql = "SELECT ARRAY_CONTAINS(a,n), ARRAY_POSITION(a,n) FROM src";
    table.explainSql(sql);
    assertTrue(scan.functionExecutions().stream().anyMatch(execution ->
        execution.contains("jvm=[ARRAY_CONTAINS, ARRAY_POSITION]")),
        scan.functionExecutions().toString());
    BuiltinFunctionParity.assertParity(factory, sql);
  }

  @Test
  void disabledCollectionExtensionRetainsFallback() throws Exception {
    requireHost();
    NativeParity.assertFallback(() -> {
      var table = environment("INT");
      table.getConfig().set("streamfusion.experimental.collectionExpressions", "false");
      return table;
    }, "SELECT ARRAY_CONTAINS(a,n), ARRAY_POSITION(a,n) FROM src");
  }

  private static void requireHost() {
    tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction("ARRAY_CONTAINS");
    tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction("ARRAY_POSITION");
  }

  private static String sqlType(String kind) {
    return kind.equals("DECIMAL") ? "DECIMAL(38,9)" : kind;
  }

  private static TableEnvironment environment(String kind) {
    DataType element;
    switch (kind) {
      case "BOOLEAN": element = BOOLEAN(); break;
      case "TINYINT": element = TINYINT(); break;
      case "SMALLINT": element = SMALLINT(); break;
      case "INT": element = INT(); break;
      case "BIGINT": element = BIGINT(); break;
      default: element = DECIMAL(38,9);
    }
    List<Row> rows = new ArrayList<>();
    Object[] pool = {value(kind, 3), value(kind, 1), value(kind, 3), null,
        value(kind, -1), value(kind, Long.MIN_VALUE), value(kind, Long.MAX_VALUE)};
    if (kind.equals("DECIMAL")) {
      pool = new BigDecimal[] {new BigDecimal("9007199254740993.000000001"),
          new BigDecimal("9007199254740993.000000002"), null,
          new BigDecimal("99999999999999999999999999999.999999999"),
          new BigDecimal("-99999999999999999999999999999.999999999")};
    }
    for (int row = 0; row < 5003; row++) {
      Object[] array = null;
      if (row % 7 != 0) {
        int length = new int[] {0, 2, 8, 64}[row % 4];
        array = (Object[]) java.lang.reflect.Array.newInstance(pool[0].getClass(), length);
        for (int i = 0; i < length; i++) array[i] = pool[(row + i) % pool.length];
      }
      rows.add(Row.of(row, array, row % 5 == 0 ? null : pool[(row / 5) % pool.length]));
    }
    return BuiltinFunctionParity.environment(
        ROW(FIELD("id", INT()), FIELD("a", ARRAY(element)), FIELD("n", element)), rows);
  }

  private static Object value(String kind, long value) {
    switch (kind) {
      case "BOOLEAN": return value > 0;
      case "TINYINT": return (byte) Math.max(Byte.MIN_VALUE, Math.min(Byte.MAX_VALUE, value));
      case "SMALLINT": return (short) Math.max(Short.MIN_VALUE, Math.min(Short.MAX_VALUE, value));
      case "INT": return (int) Math.max(Integer.MIN_VALUE, Math.min(Integer.MAX_VALUE, value));
      case "BIGINT": return value;
      default: return BigDecimal.valueOf(value).setScale(9);
    }
  }
}
