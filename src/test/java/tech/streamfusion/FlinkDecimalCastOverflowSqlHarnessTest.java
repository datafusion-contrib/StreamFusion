package tech.streamfusion;

import static org.apache.flink.table.api.DataTypes.*;
import static org.junit.jupiter.api.Assertions.*;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.runtime.typeutils.ExternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import tech.streamfusion.planner.NativePlanner;

class FlinkDecimalCastOverflowSqlHarnessTest {
  @ParameterizedTest
  @CsvSource({"18,3,false,false", "18,3,false,true", "18,3,true,false", "18,3,true,true",
      "38,18,false,false", "38,18,false,true", "38,18,true,false", "38,18,true,true"})
  void runtimeProjectionsPreserveDefaultsNullsAndEveryRow(
      int precision, int scale, boolean legacy, boolean nullable) throws Exception {
    String sql = "SELECT id,CAST(d*d AS STRING),CAST(d*d AS CHAR(8)),CAST(d*d AS VARCHAR(3)),"
        + "CASE WHEN MOD(id,2)=0 THEN CAST(d*d AS STRING) ELSE 'other' END,"
        + "CASE WHEN MOD(id,2)=0 THEN CAST(d*d AS STRING) ELSE CAST(NULL AS STRING) END,"
        + "CAST(ABS(d) AS STRING),CAST(ABS(d*d) AS STRING),CAST(-(d*d) AS STRING) FROM src";
    for (String policy : List.of("ERROR", "DROP")) {
      TreeMap<Integer, List<Object>> expected = null;
      List<DataType> expectedTypes = null;
      for (boolean nativeRun : new boolean[] {false, true}) {
        try (var metrics = new RecoveryExecutionMetrics()) {
          var env = metrics.environment(new Configuration(), 1, 0);
          var table = StreamTableEnvironment.create(env);
          var decimal = nullable ? DECIMAL(precision, scale) : DECIMAL(precision, scale).notNull();
          var type = ROW(FIELD("id", INT().notNull()), FIELD("d", decimal));
          var input = rows(precision, scale, nullable, 5003);
          table.getConfig().set("table.exec.legacy-cast-behaviour", legacy ? "ENABLED" : "DISABLED");
          table.getConfig().set("table.exec.sink.not-null-enforcer", policy);
          table.createTemporaryView("src", tech.streamfusion.compat.FlinkTestSources.fromData(env,
              input, ExternalTypeInfo.<Row>of(type)),
              Schema.newBuilder().fromRowDataType(type).build());
          var scan = nativeRun ? NativePlanner.install(table) : null;
          var types = table.sqlQuery(sql).getResolvedSchema().getColumnDataTypes();
          var actual = new TreeMap<Integer, List<Object>>();
          try (var output = table.executeSql(sql).collect()) {
            while (output.hasNext()) {
              Row row = output.next();
              int id = (Integer) row.getField(0);
              assertTrue(id >= 0 && id < input.size());
              var fields = new ArrayList<Object>();
              for (int i = 1; i < row.getArity(); i++) fields.add(row.getField(i));
              assertNull(actual.put(id, fields), "duplicate id " + id);
            }
          }
          assertEquals(input.size(), actual.size(), "DROP must not hide lost overflow rows");
          if (nativeRun) {
            assertEquals(expectedTypes, types);
            assertEquals(expected, actual);
            assertTrue(scan.fallbackReasons().isEmpty(), scan.fallbackReasons().toString());
            if (!nullable) assertTrue(scan.functionExecutions().toString().contains("jvm=[CAST"),
                scan.functionExecutions().toString());
            RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeCalc");
          } else {
            expectedTypes = types;
            expected = actual;
            if (precision == 38) assertEquals(nullable ? null : "", actual.get(0).get(0));
          }
        }
      }
    }
  }

  @ParameterizedTest
  @CsvSource({"false,false", "false,true", "true,false", "true,true"})
  void nullFlagsRemainVisibleToConsumersAndOperatorBoundaries(boolean legacy, boolean nullable)
      throws Exception {
    java.util.function.Supplier<TableEnvironment> factory = () -> environment(legacy, nullable);
    BuiltinFunctionParity.assertParity(factory,
        "SELECT id,CHAR_LENGTH(CAST(d*d AS STRING)),CAST(d*d AS STRING)='' FROM src");
    BuiltinFunctionParity.assertParity(factory,
        "SELECT id FROM src WHERE CAST(d*d AS STRING)='' OR id=2");
    BuiltinFunctionParity.assertParity(factory,
        "SELECT id,CASE WHEN id>=0 THEN CAST(d*d AS STRING) "
            + "ELSE CAST(d/(id-id) AS STRING) END FROM src");
    BuiltinFunctionParity.assertParity(factory,
        "SELECT id,CAST(d*d AS STRING),CAST(d/(id-id) AS STRING) FROM src WHERE id<0");
    NativeParity.assertChangelogParity(factory,
        "SELECT c,COUNT(*) FROM (SELECT CAST(d*d AS STRING) AS c FROM src) GROUP BY c");
    // TRY_CAST remains nullable; overflowing operands must not inherit CAST's empty default.
    for (String target : List.of("STRING", "VARCHAR(3)", "CHAR(8)")) {
      NativeFailureParity.run(factory, "SELECT id,TRY_CAST(d*d AS " + target + ") FROM src")
          .assertSuccess(NativeFailureParity.Route.NATIVE);
    }
  }

  @ParameterizedTest
  @CsvSource({"false,ERROR", "true,ERROR", "false,DROP", "true,DROP"})
  void nestedNullResultsStillReachTheConstraintEnforcer(boolean legacy, String policy)
      throws Exception {
    java.util.function.Supplier<TableEnvironment> factory = () -> {
      var table = environment(legacy, false);
      table.getConfig().set("table.exec.sink.not-null-enforcer", policy);
      return table;
    };
    String sql = "SELECT id,CONCAT(CAST(d*d AS STRING),'!') FROM src";
    if (policy.equals("DROP")) BuiltinFunctionParity.assertParity(factory, sql);
    else {
      var runs = NativeFailureParity.run(factory, sql);
      assertNotNull(runs.host().rootCause(), runs.toString());
      runs.assertFailure(runs.host().rootCause().getClass(), "is NOT NULL",
          NativeFailureParity.Phase.ROW_EVALUATION, NativeFailureParity.Route.NATIVE);
    }
  }

  @ParameterizedTest
  @CsvSource({"false,CAST", "true,CAST", "false,TRY_CAST", "true,TRY_CAST"})
  void operandFailuresAreNotReplacedWithDefaults(boolean legacy, String cast) {
    var runs = NativeFailureParity.run(() -> environment(legacy, false),
        "SELECT " + cast + "(d/(id-id) AS STRING) FROM src");
    assertNotNull(runs.host().rootCause(), runs.toString());
    // Unverified TRY_CAST operands retain the released Flink path.
    runs.assertFailure(runs.host().rootCause().getClass(),
        NativeFailureParity.Phase.ROW_EVALUATION,
        cast.equals("CAST") ? NativeFailureParity.Route.NATIVE : NativeFailureParity.Route.FALLBACK);
  }

  @ParameterizedTest
  @CsvSource({"false,ERROR", "true,ERROR", "false,DROP", "true,DROP"})
  void udfNullReturnsKeepReleasedNullabilityAndProjectionDefaults(boolean legacy, String policy) {
    for (boolean nullableResult : new boolean[] {false, true}) {
      var runs = NativeFailureParity.run(() -> {
        var table = environment(legacy, false);
        table.createTemporarySystemFunction("null_text",
            nullableResult ? NullableNullText.class : NullText.class);
        table.getConfig().set("table.exec.sink.not-null-enforcer", policy);
        return table;
      }, "SELECT null_text(CAST(d*d AS STRING)) FROM src");
      runs.assertSuccess(NativeFailureParity.Route.NATIVE);
      assertEquals(5, runs.nativeRun().rows().size());
      for (var row : runs.nativeRun().rows()) assertEquals(nullableResult ? null : "", row.get(1));
    }
  }

  public static final class NullText extends org.apache.flink.table.functions.ScalarFunction {
    @org.apache.flink.table.annotation.DataTypeHint("STRING NOT NULL")
    public String eval(String value) {
      return null;
    }
  }

  public static final class NullableNullText extends org.apache.flink.table.functions.ScalarFunction {
    public String eval(String value) {
      return null;
    }
  }

  private static TableEnvironment environment(boolean legacy, boolean nullable) {
    var decimal = nullable ? DECIMAL(38, 18) : DECIMAL(38, 18).notNull();
    var table = BuiltinFunctionParity.environment(
        ROW(FIELD("id", INT().notNull()), FIELD("d", decimal)), rows(38, 18, nullable, 5));
    table.getConfig().set("table.exec.legacy-cast-behaviour", legacy ? "ENABLED" : "DISABLED");
    return table;
  }

  private static List<Row> rows(int precision, int scale, boolean nullable, int count) {
    var max = new BigDecimal(BigInteger.TEN.pow(precision).subtract(BigInteger.ONE), scale);
    BigDecimal[] values = {max, max.negate(), BigDecimal.ZERO.setScale(scale),
        BigDecimal.ONE.setScale(scale), nullable ? null : BigDecimal.ONE.negate().setScale(scale)};
    var result = new ArrayList<Row>();
    for (int i = 0; i < count; i++) result.add(Row.of(i, values[i % values.length]));
    return result;
  }
}
