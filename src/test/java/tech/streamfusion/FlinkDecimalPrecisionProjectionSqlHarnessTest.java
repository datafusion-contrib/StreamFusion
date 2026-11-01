package tech.streamfusion;

import static org.apache.flink.table.api.DataTypes.*;
import static org.junit.jupiter.api.Assertions.*;

import java.math.BigDecimal;
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
import org.apache.flink.types.RowKind;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import tech.streamfusion.planner.NativePlanner;

class FlinkDecimalPrecisionProjectionSqlHarnessTest {
  @ParameterizedTest
  @CsvSource({
    "x,false,false",
    "x,false,true",
    "x,true,false",
    "x,true,true",
    "d,false,false",
    "d,false,true",
    "d,true,false",
    "d,true,true",
    "r,false,false",
    "r,false,true",
    "r,true,false",
    "r,true,true",
    "f,false,false",
    "f,false,true",
    "f,true,false",
    "f,true,true"
  })
  void narrowingCastsKeepConsumerDefaultsAndEveryRow(
      String source, boolean nullable, boolean legacy) throws Exception {
    String value = "CAST(" + source + " AS DECIMAL(1,0))";
    String sql =
        "SELECT id,"
            + value
            + ">0,NOT("
            + value
            + ">0),"
            + value
            + ">=0,"
            + value
            + "<0,"
            + value
            + "<=0,"
            + value
            + "=0,"
            + value
            + "<>0,"
            + "CAST("
            + value
            + " AS INT),CAST("
            + value
            + " AS BIGINT),"
            + "CAST("
            + value
            + " AS SMALLINT),CAST("
            + value
            + " AS TINYINT),"
            + "CAST("
            + value
            + " AS FLOAT),CAST("
            + value
            + " AS DOUBLE),"
            + "CAST("
            + value
            + ">0 AS STRING),CAST("
            + value
            + ">0 AS VARCHAR(3)),"
            + "CAST("
            + value
            + ">0 AS CHAR(8)),"
            + "("
            + value
            + ">0) AND id>=0,("
            + value
            + ">0) OR id<0,"
            + "CASE WHEN id>=0 THEN NOT("
            + value
            + ">0) ELSE id<0 END,"
            + "CAST("
            + value
            + " AS INT)+2 FROM src";
    for (String policy : List.of("ERROR", "DROP")) {
      TreeMap<Integer, List<Object>> expected = null;
      List<DataType> expectedTypes = null;
      for (boolean nativeRun : new boolean[] {false, true}) {
        try (var metrics = new RecoveryExecutionMetrics()) {
          var env = metrics.environment(new Configuration(), 1, 0);
          var table = StreamTableEnvironment.create(env);
          configure(table, nullable, legacy, policy, env, 5003);
          var scan = nativeRun ? NativePlanner.install(table) : null;
          var types = table.sqlQuery(sql).getResolvedSchema().getColumnDataTypes();
          var actual = new TreeMap<Integer, List<Object>>();
          try (var output = table.executeSql(sql).collect()) {
            while (output.hasNext()) {
              Row row = output.next();
              assertEquals(RowKind.INSERT, row.getKind());
              int id = (Integer) row.getField(0);
              assertTrue(id >= 0 && id < 5003);
              var fields = new ArrayList<Object>();
              for (int i = 1; i < row.getArity(); i++) fields.add(row.getField(i));
              assertNull(actual.put(id, fields), "duplicate id " + id);
            }
          }
          assertEquals(5003, actual.size(), "DROP must not hide lost overflow rows");
          if (nativeRun) {
            assertEquals(expectedTypes, types);
            assertEquals(expected, actual);
            assertTrue(scan.fallbackReasons().isEmpty(), scan.explainSummary());
            assertTrue(
                scan.functionExecutions().toString().contains("jvm=[CAST"), scan.explainSummary());
            RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeCalc");
          } else {
            expected = actual;
            expectedTypes = types;
            assertEquals(nullable ? null : false, actual.get(0).get(0));
            assertEquals(nullable ? null : false, actual.get(0).get(1));
            assertEquals(nullable ? null : -1, actual.get(0).get(7));
            assertEquals(nullable ? null : -1L, actual.get(0).get(8));
            assertEquals(nullable ? null : "", actual.get(0).get(13));
          }
        }
      }
    }
  }

  @ParameterizedTest
  @CsvSource({"x,false", "x,true", "d,false", "d,true"})
  void nullableConsumersFiltersAndLazyBranchesKeepNullFlags(String source, boolean nullable)
      throws Exception {
    var factory = factory(nullable, false, "ERROR");
    String value = "CAST(" + source + " AS DECIMAL(1,0))";
    BuiltinFunctionParity.assertParity(
        factory,
        "SELECT id,("
            + value
            + ">0) OR CAST(NULL AS BOOLEAN),"
            + value
            + " IS NULL,CASE WHEN id<0 THEN "
            + value
            + ">0 ELSE CAST(NULL AS BOOLEAN) END FROM src");
    BuiltinFunctionParity.assertParity(
        factory, "SELECT id FROM src WHERE NOT(" + value + ">0) OR id=2");
    BuiltinFunctionParity.assertParity(
        factory,
        "SELECT id,CASE WHEN id>=0 THEN NOT("
            + value
            + ">0) ELSE "
            + "CAST(x/(id-id) AS DECIMAL(1,0))>0 END FROM src");
    BuiltinFunctionParity.assertParity(
        factory, "SELECT " + value + ">0,CAST(x/(id-id) AS DECIMAL(1,0))>0 FROM src WHERE id<0");
    NativeParity.assertChangelogParity(
        factory, "SELECT v,COUNT(*) FROM (SELECT " + value + ">0 AS v FROM src) GROUP BY v");
  }

  @ParameterizedTest
  @CsvSource({"false,ERROR", "false,DROP", "true,ERROR", "true,DROP"})
  void directDecimalAndTryCastKeepTheirExistingNullContracts(boolean legacy, String policy)
      throws Exception {
    var factory = factory(false, legacy, policy);
    for (String source : List.of("x", "d")) {
      String value = "CAST(" + source + " AS DECIMAL(1,0))";
      String direct = "SELECT id," + value + " FROM src";
      if (policy.equals("DROP")) BuiltinFunctionParity.assertParity(factory, direct);
      else {
        var runs = NativeFailureParity.run(factory, direct);
        assertNotNull(runs.host().rootCause(), runs.toString());
        runs.assertFailure(
            runs.host().rootCause().getClass(),
            "is NOT NULL",
            NativeFailureParity.Phase.ROW_EVALUATION,
            NativeFailureParity.Route.NATIVE);
      }
      for (String target : List.of("STRING", "VARCHAR(3)", "CHAR(8)")) {
        NativeFailureParity.run(
                factory, "SELECT id,TRY_CAST(" + value + " AS " + target + ") FROM src")
            .assertSuccess(NativeFailureParity.Route.FALLBACK);
      }
    }
  }

  @ParameterizedTest
  @CsvSource({"false", "true"})
  void selectedFailuresStillFail(boolean nullable) {
    var runs =
        NativeFailureParity.run(
            factory(nullable, false, "ERROR"), "SELECT CAST(x/(id-id) AS DECIMAL(1,0))>0 FROM src");
    assertNotNull(runs.host().rootCause(), runs.toString());
    for (var outcome : List.of(runs.host(), runs.nativeRun())) {
      assertTrue(outcome.rootCause().getMessage().contains("zero"), outcome.toString());
      assertEquals(NativeFailureParity.Phase.ROW_EVALUATION, outcome.phase());
    }
    assertEquals(NativeFailureParity.Route.NATIVE, runs.nativeRun().route());
  }

  private static java.util.function.Supplier<TableEnvironment> factory(
      boolean nullable, boolean legacy, String policy) {
    return () -> {
      var env =
          org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
              .getExecutionEnvironment();
      env.setParallelism(1);
      var table = StreamTableEnvironment.create(env);
      configure(table, nullable, legacy, policy, env, 7);
      return table;
    };
  }

  private static void configure(
      StreamTableEnvironment table,
      boolean nullable,
      boolean legacy,
      String policy,
      org.apache.flink.streaming.api.environment.StreamExecutionEnvironment env,
      int count) {
    var type =
        ROW(
            FIELD("id", INT().notNull()),
            FIELD("x", nullable ? BIGINT() : BIGINT().notNull()),
            FIELD("d", nullable ? DECIMAL(18, 2) : DECIMAL(18, 2).notNull()),
            FIELD("r", nullable ? DECIMAL(3, 2) : DECIMAL(3, 2).notNull()),
            FIELD("f", nullable ? DOUBLE() : DOUBLE().notNull()));
    Long[] values = {99L, 1L, 0L, -99L, -1L, nullable ? null : 0L};
    var rows = new ArrayList<Row>();
    for (int i = 0; i < count; i++) {
      Long value = values[i % values.length];
      BigDecimal decimal = value == null ? null : BigDecimal.valueOf(value).setScale(2);
      BigDecimal rounding =
          value != null && Math.abs(value) == 99
              ? new BigDecimal(value < 0 ? "-9.99" : "9.99")
              : decimal;
      rows.add(Row.of(i, value, decimal, rounding, value == null ? null : value.doubleValue()));
    }
    table.getConfig().set("table.exec.legacy-cast-behaviour", legacy ? "ENABLED" : "DISABLED");
    table.getConfig().set("table.exec.sink.not-null-enforcer", policy);
    table.createTemporaryView(
        "src",
        tech.streamfusion.compat.FlinkTestSources.fromData(
            env, rows, ExternalTypeInfo.<Row>of(type)),
        Schema.newBuilder().fromRowDataType(type).build());
  }
}
