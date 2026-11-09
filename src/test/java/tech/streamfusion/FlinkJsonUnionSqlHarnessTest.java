package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;
import static tech.streamfusion.compat.FlinkTestSources.fromData;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.nio.file.Path;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import tech.streamfusion.planner.NativePlanner;

/** Final UNION forwarding must match Flink's serialized strings at the row sink. */
class FlinkJsonUnionSqlHarnessTest {
  @TempDir Path directory;
  private static final int ROWS = 5003;
  private static final String UNION =
      "SELECT id, JSON_VALUE(j, '$.v') AS v FROM src WHERE MOD(id, 2) = 0 "
          + "UNION ALL SELECT id, JSON_VALUE(j, '$.v') FROM src WHERE MOD(id, 2) = 1";
  private static final String REASON =
      "JSON string identity requires a final projection or a fused scalar consumer";
  private static final String[] DOCUMENTS = {
    "{\"v\":\"a\"}",
    "{\"v\":\"\\uD800\"}",
    "{\"v\":\"\\uDC00\"}",
    "{\"v\":\"?\"}",
    "{\"v\":\"\\uD83D\\uDE00\"}",
    null,
    "{\"v\":\"\"}",
    "{\"v\":null}",
    "{\"v\":\"\u4e2d\"}",
    "{\"v\":\"\\uD800x\\uDC00\"}",
    "{\"v\":\"\ud800\"}",
    "{\"v\":\"\udc00\"}",
    "{\"v\":\"\ud83d\ude00\"}",
    "{\"v\":\"\ue000\"}",
    "{\"v\":\"\\\ud800\"}",
    "{\"v\":true}",
    "{\"v\":123}",
    "invalid",
    "{}"
  };

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void nativeJsonBranchesMatchSerializedStringsWithAndWithoutChaining(boolean chaining)
      throws Exception {
    check(UNION, 2, true, chaining);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void ordinaryAndJsonBranchesCanArriveInEitherOrder(boolean reverse) throws Exception {
    String json = "SELECT id, JSON_VALUE(j, '$.v') AS v FROM src WHERE MOD(id, 2) = 0";
    String ordinary = "SELECT id, j AS v FROM src WHERE MOD(id, 2) = 1";
    check(reverse ? ordinary + " UNION ALL " + json : json + " UNION ALL " + ordinary,
        2, true, true);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void multipleOutputsAndNestedUnionMatchSerializedStrings(boolean nested) throws Exception {
    String projection = "SELECT id, JSON_VALUE(j, '$.v') AS v, "
        + "JSON_VALUE(j, '$.v' DEFAULT 'empty' ON EMPTY DEFAULT 'error' ON ERROR) AS d FROM src ";
    String sql = projection + "WHERE MOD(id, 3) = 0 UNION ALL "
        + projection + "WHERE MOD(id, 3) = 1 UNION ALL "
        + projection + "WHERE MOD(id, 3) = 2";
    if (nested) sql = "(" + projection + "WHERE MOD(id, 3) = 0 UNION ALL "
        + projection + "WHERE MOD(id, 3) = 1) UNION ALL "
        + projection + "WHERE MOD(id, 3) = 2";
    check(sql, 3, true, true);
  }

  @Test
  void typedNullsCanAccompanyDirectJsonResults() throws Exception {
    String projection = "SELECT id, JSON_VALUE(j, '$.v') AS v, CAST(NULL AS STRING) AS d FROM src ";
    check(projection + "WHERE MOD(id, 2) = 0 UNION ALL "
        + projection + "WHERE MOD(id, 2) = 1", 3, true, true);
  }

  @ParameterizedTest
  @ValueSource(strings = {
    "SELECT id, v, v = '?', v LIKE '?', CASE WHEN v = '?' THEN 'question' ELSE v END FROM (%s)",
    "SELECT v, COUNT(*) FROM (%s) GROUP BY v",
    "SELECT id, JSON_QUERY(j, '$') AS v FROM src WHERE MOD(id, 2) = 0 UNION ALL "
        + "SELECT id, JSON_QUERY(j, '$') FROM src WHERE MOD(id, 2) = 1",
    "SELECT id, CASE WHEN id > 0 THEN JSON_VALUE(j, '$.v') ELSE '?' END AS v FROM src "
        + "WHERE MOD(id, 2) = 0 UNION ALL SELECT id, JSON_VALUE(j, '$.v') FROM src "
        + "WHERE MOD(id, 2) = 1",
    "SELECT id, JSON_VALUE(j, '$.v') AS v, j AS doc_text FROM src WHERE MOD(id, 2) = 0 UNION ALL "
        + "SELECT id, JSON_VALUE(j, '$.v'), j FROM src WHERE MOD(id, 2) = 1",
    "SELECT id, JSON_VALUE(j, '$.v') AS v, CHAR_LENGTH(j) AS n FROM src WHERE MOD(id, 2) = 0 "
        + "UNION ALL SELECT id, JSON_VALUE(j, '$.v'), CHAR_LENGTH(j) FROM src WHERE MOD(id, 2) = 1",
    "SELECT id, JSON_VALUE(j, '$.v') AS v FROM src WHERE MOD(id, 2) = 0 AND JSON_EXISTS(j, '$.v') "
        + "UNION ALL SELECT id, JSON_VALUE(j, '$.v') FROM src WHERE MOD(id, 2) = 1"
  })
  void unverifiedConsumersRetainCompleteQueryFallback(String template) throws Exception {
    String sql = template.contains("%s") ? String.format(template, UNION) : template;
    check(sql, -1, false, true);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void sortedJsonUnionRetainsCompleteQueryFallback(boolean chaining) throws Exception {
    Map<List<Object>, Long> expected = new HashMap<>();
    for (int id : new int[] {5, 7, 14, 17, 18, 24, 26, 33, 36, 37, 43, 45, 52, 55, 56, 62, 64}) {
      expected.put(Arrays.asList(id, null), 1L);
    }
    // UNION arrival order changes intermediate Top-N updates; the unique sort key fixes its final set.
    check("SELECT id, v FROM (" + UNION + ") ORDER BY v NULLS FIRST, id LIMIT 17",
        -1, false, chaining, expected);
  }

  @ParameterizedTest
  @CsvSource({"false,false", "false,true", "true,false", "true,true"})
  void finalSinkMatchesCodeUnitsBeforeResultCollection(boolean duplicated, boolean chaining)
      throws Exception {
    String projection = "SELECT id, JSON_VALUE(j, '$.v') AS v FROM src";
    String sql = duplicated ? projection + " UNION ALL " + projection
        : projection + " WHERE MOD(id, 2) = 0 UNION ALL "
            + "SELECT id, j AS v FROM src WHERE MOD(id, 2) = 1";
    List<String> expected = null;
    for (boolean nativeEnabled : new boolean[] {false, true}) {
      try (var metrics = new RecoveryExecutionMetrics()) {
        var table = (StreamTableEnvironment) environment(metrics, chaining);
        var scan = nativeEnabled ? NativePlanner.install(table) : null;
        table.executeSql("CREATE TABLE out_rows (id INT, v STRING) WITH "
            + "('connector'='json-utf16-results')");
        var result = table.executeSql("INSERT INTO out_rows " + sql);
        var job = result.getJobClient().orElseThrow().getJobExecutionResult().get();
        List<String> rows = job.getAccumulatorResult(Utf16ResultTableFactory.RESULTS);
        assertEquals(duplicated ? 2 * ROWS : ROWS, rows.size());
        assertTrue(rows.contains("+I:2:63"), "Flink serializes the isolated surrogate before the sink");
        rows.sort(String::compareTo);
        if (!nativeEnabled) expected = rows;
        else {
          assertEquals(expected, rows);
          assertTrue(scan.substitutions() >= 3 && scan.fallbackReasons().isEmpty(),
              scan.explainSummary());
          assertTrue(metrics.nativeOperators().stream().anyMatch(operator ->
              ((Number) operator.get("recordsIn")).longValue() > 0));
        }
      }
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 3})
  void jsonUnionSurvivesTwoCheckpointRestores(int parallelism) throws Exception {
    List<Row> input = new ArrayList<>();
    for (int i = 0; i < 64; i++) input.add(Row.of(i, DOCUMENTS[i % DOCUMENTS.length]));
    try (var recovery = new PortableSqlRecovery("hashmap", input,
        Types.ROW_NAMED(new String[] {"id", "j"}, Types.INT, Types.STRING),
        Schema.newBuilder().column("id", DataTypes.INT()).column("j", DataTypes.STRING()).build(),
        false, 16, 48).withRescaleDeployment(parallelism, null, null).withExecutionMetrics()) {
      java.util.function.UnaryOperator<TableEnvironment> setup = table -> {
        table.getConfig().set("streamfusion.transpose.batchRows", "5");
        table.createTemporaryView("src", table.from("recovery_input"));
        return table;
      };
      var comparison = NativeFailureParity.runRecovery(
          () -> setup.apply(recovery.uninterrupted()), () -> setup.apply(recovery.get()), UNION);
      comparison.assertSuccess(NativeFailureParity.Route.NATIVE);
      assertEquals(64, comparison.host().rows().size());
      assertEquals(comparison.host().resultTypes(), comparison.nativeRun().resultTypes());
      recovery.verifyRepeatedRecovery();
      RecoveryExecutionMetrics.assertConsumed(recovery.nativeExecution(), "NativeCalc");
      SharedFlinkCluster.assertNativeMemoryReleased("json-union-recovery");
    }
  }

  @Test
  void updatingJsonRowsKeepEveryKindAcrossUnion() throws Exception {
    List<Row> input = new ArrayList<>();
    for (int id = 0; id < DOCUMENTS.length; id++) {
      for (RowKind kind : RowKind.values()) input.add(Row.ofKind(kind, id, DOCUMENTS[id]));
    }
    List<String> expected = null;
    for (boolean nativeEnabled : new boolean[] {false, true}) {
      try (var metrics = new RecoveryExecutionMetrics()) {
        var env = metrics.environment(new Configuration(), 1, 0);
        var table = StreamTableEnvironment.create(env);
        table.getConfig().set("streamfusion.transpose.batchRows", "5");
        var source = fromData(env,
            Types.ROW_NAMED(new String[] {"id", "j"}, Types.INT, Types.STRING),
            input.toArray(new Row[0]));
        table.createTemporaryView("src", table.fromChangelogStream(source,
            Schema.newBuilder().column("id", DataTypes.INT()).column("j", DataTypes.STRING()).build(),
            ChangelogMode.all()));
        var scan = nativeEnabled ? NativePlanner.install(table) : null;
        table.executeSql("CREATE TABLE out_rows (id INT, v STRING) WITH "
            + "('connector'='json-utf16-results')");
        var result = table.executeSql("INSERT INTO out_rows " + UNION);
        var job = result.getJobClient().orElseThrow().getJobExecutionResult().get();
        List<String> rows = job.getAccumulatorResult(Utf16ResultTableFactory.RESULTS);
        assertEquals(input.size(), rows.size());
        for (RowKind kind : RowKind.values())
          assertTrue(rows.stream().anyMatch(row -> row.startsWith(kind.shortString() + ":")));
        rows.sort(String::compareTo);
        if (!nativeEnabled) expected = rows;
        else {
          assertEquals(expected, rows);
          assertTrue(scan.substitutions() >= 3 && scan.fallbackReasons().isEmpty(), scan.explainSummary());
          assertTrue(metrics.recordsIn(result.getJobClient().orElseThrow().getJobID().toString(),
              "NativeCalc(") >= input.size());
        }
      }
    }
  }

  @Test
  void finalJsonUnionRequiresARowSink() throws Exception {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var table = environment(metrics, true);
      table.executeSql("CREATE TABLE out_rows (id INT, v STRING) WITH ('connector'='filesystem',"
          + "'format'='parquet','path'='" + directory.toUri() + "')");
      var scan = NativePlanner.install(table);
      String plan = table.explainSql("INSERT INTO out_rows " + UNION);
      assertEquals(0, scan.substitutions(), plan + scan.explainSummary());
      assertTrue(scan.fallbackReasons().contains("JSON string identity across UNION requires a row sink"),
          scan.explainSummary());
    }
  }

  private static void check(String sql, int arity, boolean expectedNative, boolean chaining)
      throws Exception {
    check(sql, arity, expectedNative, chaining, null);
  }

  private static void check(String sql, int arity, boolean expectedNative, boolean chaining,
      Map<List<Object>, Long> expectedFinalRows) throws Exception {
    Map<List<Object>, Long> expected = null;
    List<String> expectedTypes = null;
    for (boolean nativeEnabled : new boolean[] {false, true}) {
      try (var metrics = new RecoveryExecutionMetrics()) {
        TableEnvironment table = environment(metrics, chaining);
        var scan = nativeEnabled ? NativePlanner.install(table) : null;
        var query = table.sqlQuery(sql);
        List<String> types = new ArrayList<>();
        query.getResolvedSchema().getColumnDataTypes().forEach(t -> types.add(t.toString()));
        if (expectedNative) {
          String columns = "id INT, v STRING" + (arity == 3 ? ", d STRING" : "");
          table.executeSql("CREATE TABLE out_rows (" + columns + ") WITH ('connector'='blackhole')");
          String plan = table.explainSql("INSERT INTO out_rows " + sql);
          if (nativeEnabled) {
            assertTrue(plan.contains("NativeUnion"), plan);
            assertTrue(plan.contains("NativeCalc"), plan);
            assertTrue(plan.contains("RowDataToArrow") && plan.contains("ArrowToRowData"), plan);
          }
        }
        List<List<Object>> rows = new ArrayList<>();
        try (var iterator = query.execute().collect()) {
          while (iterator.hasNext()) {
            Row row = iterator.next();
            List<Object> fields = new ArrayList<>();
            fields.add(row.getKind().shortString());
            for (int i = 0; i < row.getArity(); i++) fields.add(row.getField(i));
            rows.add(fields);
          }
        }
        if (expectedNative) {
          assertEquals(ROWS, rows.size());
          assertEquals(ROWS, rows.stream().map(row -> row.get(1)).distinct().count());
        }
        Map<List<Object>, Long> contents = expectedFinalRows == null
            ? NativeParity.multiset(rows) : NativeParity.collapsedChangelog(rows);
        if (expectedFinalRows != null) assertEquals(expectedFinalRows, contents);
        if (!nativeEnabled) {
          expected = contents;
          expectedTypes = types;
        } else {
          assertEquals(expectedTypes, types);
          assertEquals(expected, contents);
          if (expectedNative) {
            assertTrue(scan.substitutions() >= 3, scan.explainSummary());
            assertTrue(scan.fallbackReasons().isEmpty(), scan.explainSummary());
            assertTrue(metrics.nativeOperators().stream().anyMatch(operator ->
                operator.get("operator").toString().startsWith("NativeCalc(")
                    && ((Number) operator.get("recordsIn")).longValue() > 0),
                metrics.nativeOperators().toString());
            assertTrue(scan.functionExecutions().stream().noneMatch(line ->
                !line.endsWith("jvm=[]")), scan.explainSummary());
          } else {
            assertEquals(0, scan.substitutions(), scan.explainSummary());
            assertTrue(scan.fallbackReasons().contains(REASON), scan.explainSummary());
            assertTrue(metrics.nativeOperators().isEmpty(), metrics.nativeOperators().toString());
          }
        }
      }
    }
  }

  private static TableEnvironment environment(RecoveryExecutionMetrics metrics, boolean chaining) {
    var env = metrics.environment(new Configuration(), 1, 0);
    env.setParallelism(1);
    if (!chaining) env.disableOperatorChaining();
    var table = StreamTableEnvironment.create(env);
    table.getConfig().setLocalTimeZone(java.time.ZoneOffset.UTC);
    table.getConfig().set("streamfusion.transpose.batchRows", "127");
    var source = env.fromSequence(0, ROWS - 1)
        .map(id -> Row.of(id.intValue(), DOCUMENTS[(int) (id % DOCUMENTS.length)]))
        .returns(Types.ROW_NAMED(new String[] {"id", "j"}, Types.INT, Types.STRING));
    table.createTemporaryView("src", source, Schema.newBuilder()
        .column("id", DataTypes.INT()).column("j", DataTypes.STRING()).build());
    return table;
  }
}
