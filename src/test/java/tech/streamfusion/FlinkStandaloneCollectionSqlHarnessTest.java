package tech.streamfusion;

import static org.apache.flink.table.api.DataTypes.*;
import static org.junit.jupiter.api.Assertions.*;
import static tech.streamfusion.compat.FlinkTestSources.fromData;

import java.math.BigDecimal;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.function.Supplier;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.runtime.typeutils.ExternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.planner.NativePlanner;

class FlinkStandaloneCollectionSqlHarnessTest {
  private static final RecoveryExecutionMetrics METRICS = new RecoveryExecutionMetrics();

  @AfterAll
  static void closeCluster() {
    METRICS.close();
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "CAST(a AS ARRAY<DOUBLE>)",
        "CAST(a AS ARRAY<FLOAT>)",
        "CAST(d AS ARRAY<DOUBLE>)",
        "CAST(d AS ARRAY<FLOAT>)",
        "CAST(a AS ARRAY<STRING>)",
        "CAST(t AS ARRAY<INT>)",
        "TRY_CAST(t AS ARRAY<INT>)",
        "CAST(a AS ARRAY<INT NOT NULL>)",
        "TRY_CAST(a AS ARRAY<INT NOT NULL>)",
        "CAST(nested AS ARRAY<ARRAY<STRING>>)",
        "CAST(m AS MAP<STRING,BIGINT>)",
        "CAST(m AS MAP<STRING,STRING>)",
        "TRY_CAST(m AS MAP<STRING,BIGINT>)",
        "CAST(r AS ROW<x BIGINT,y ARRAY<STRING>>)",
        "CAST(dates AS ARRAY<TIMESTAMP(3)>)",
        "CAST(times AS ARRAY<TIMESTAMP(0)>)"
      })
  void standaloneCastsPreserveReleasedTypesNullsAndErrors(String expression) {
    for (boolean legacy : new boolean[] {false, true}) {
      var runs = run(() -> casts(legacy), "SELECT id," + expression + " FROM src");
      assertEquivalent(runs);
    }
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "SELECT id,SPLIT(s,p) FROM src",
        "SELECT id,SPLIT(s,'') FROM src",
        "SELECT id,SPLIT(s,p)[1],CARDINALITY(SPLIT(s,p)),ARRAY_REVERSE(SPLIT(s,p)) FROM src",
        "SELECT id,SPLIT(s,p) FROM src WHERE CARDINALITY(SPLIT(s,p))>1",
        "SELECT id,SPLIT(s,p) FROM src WHERE id<0",
        "SELECT id,SPLIT(s,'')[1]='?',SPLIT(s,'')[2]='?' FROM src",
        "SELECT id,CASE WHEN id=0 THEN SPLIT(s,p) ELSE SPLIT(s,'') END FROM src"
      })
  void splitPreservesWholeSeparatorsUtf16CachesAndEmptyBatches(String sql) {
    tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction("SPLIT");
    assertEquivalent(run(this::strings, sql));
  }

  @ParameterizedTest
  @ValueSource(strings = {"a", "d", "t", "nested", "m", "r", "dates", "times"})
  void collectionCharacterCastsPreserveFormattingBoundsAndLegacyRules(String field) {
    for (boolean legacy : new boolean[] {false, true}) {
      assertEquivalent(
          run(
              () -> casts(legacy),
              "SELECT id,CAST("
                  + field
                  + " AS STRING),CAST("
                  + field
                  + " AS VARCHAR(2)),TRY_CAST("
                  + field
                  + " AS CHAR(64)),TRY_CAST("
                  + field
                  + " AS VARCHAR(3)) FROM src"));
    }
  }

  @Test
  void characterCastsPreserveUtf16AndUnselectedInputs() {
    Supplier<TableEnvironment> factory =
        () ->
            environment(
                ROW(FIELD("id", INT()), FIELD("a", ARRAY(STRING()))),
                List.of(
                    Row.of(0, new String[] {"\ud83d\ude00", null, "\ud800"}),
                    Row.of(1, new String[] {}),
                    Row.of(2, (Object) null)));
    for (String sql :
        List.of(
            "SELECT id,CAST(a AS VARCHAR(2)),TRY_CAST(a AS CHAR(8)),CAST(a AS STRING) FROM src",
            "SELECT id,CAST(a AS STRING) FROM src WHERE id<0",
            "SELECT id,CASE WHEN id=0 THEN CAST(a AS VARCHAR(2)) ELSE CAST(a AS CHAR(8)) END FROM"
                + " src",
            "SELECT CAST(a AS VARCHAR(2))='[?' FROM src")) {
      assertEquivalent(run(factory, sql));
    }
    for (String cast : List.of("CAST", "TRY_CAST")) {
      var failure = run(() -> casts(false), "SELECT " + cast + "(ARRAY[10/id] AS STRING) FROM src");
      failure.assertFailure(
          ArithmeticException.class, failure.host().phase(), NativeFailureParity.Route.NATIVE);
    }
  }

  @Test
  void characterCastsPreserveContainerRepresentationsAndAllChangelogKinds() {
    assertEquivalent(
        run(
            () -> {
              var table =
                  StreamTableEnvironment.create(METRICS.environment(new Configuration(), 1, 0));
              table.getConfig().set("streamfusion.transpose.batchRows", "2");
              table.executeSql(
                  "CREATE TABLE src (id BIGINT,t ARRAY<STRING>,r ARRAY<ROW<n INT>>,"
                      + "nn ARRAY<INT NOT NULL>,m MAP<BYTES,ARRAY<INT>>)"
                      + " WITH ('connector'='collection-representation','changelog'='true')");
              return table;
            },
            "SELECT id,CAST(t AS STRING),TRY_CAST(r AS CHAR(32)),CAST(nn AS STRING),CAST(m AS"
                + " STRING) FROM src"));
  }

  @Test
  void fixedBinaryMapLeavesStillFallBackForCharacterCasts() {
    var entries = new LinkedHashMap<byte[], byte[]>();
    entries.put(new byte[] {0, -1}, new byte[] {1, 2});
    var runs =
        NativeFailureParity.run(
            () ->
                environment(
                    ROW(FIELD("m", MAP(BINARY(2), BINARY(2)))),
                    List.of(Row.of(entries), Row.of((Object) null))),
            "SELECT CAST(m AS STRING),TRY_CAST(m AS CHAR(16)) FROM src");
    runs.assertSuccess(NativeFailureParity.Route.FALLBACK);
  }

  @Test
  void invalidStringElementNullsTheWholeTryCastArray() {
    var runs = run(() -> casts(false), "SELECT id,TRY_CAST(t AS ARRAY<INT>) FROM src");
    runs.assertSuccess(NativeFailureParity.Route.NATIVE);
    assertTrue(
        runs.host().rows().contains(java.util.Arrays.asList("+I", 0, null)), runs.toString());
  }

  public static class NumericSplit extends org.apache.flink.table.functions.ScalarFunction {
    public Long eval(Long value, Integer increment) {
      return value == null ? null : value + increment;
    }
  }

  @Test
  void userFunctionNamedSplitRetainsItsOwnSignature() {
    var runs =
        run(
            () -> {
              var table =
                  environment(
                      ROW(FIELD("n", BIGINT())), List.of(Row.of(1L), Row.of((Object) null)));
              table.createTemporarySystemFunction("SPLIT", NumericSplit.class);
              return table;
            },
            "SELECT SPLIT(n,1) FROM src");
    assertEquivalent(runs);
  }

  @ParameterizedTest
  @ValueSource(strings = {"CAST", "TRY_CAST"})
  void mapKeyCollisionsAndNestedValuesKeepReleasedMapConstruction(String cast) {
    var values = new LinkedHashMap<Integer, Integer[]>();
    values.put(1, new Integer[] {1, null});
    values.put(257, new Integer[] {-2});
    values.put(null, new Integer[] {});
    var runs =
        run(
            () ->
                environment(
                    ROW(FIELD("m", MAP(INT(), ARRAY(INT())))),
                    List.of(Row.of(values), Row.of(new LinkedHashMap<>()), Row.of((Object) null))),
            "SELECT " + cast + "(m AS MAP<TINYINT,ARRAY<BIGINT>>) FROM src");
    assertEquivalent(runs);
  }

  @ParameterizedTest
  @ValueSource(strings = {"CAST", "TRY_CAST"})
  void inputErrorsAndUnselectedBranchesKeepTheirEvaluationOrder(String cast) {
    Supplier<TableEnvironment> factory = () -> casts(false);
    var failure = run(factory, "SELECT " + cast + "(ARRAY[10/id] AS ARRAY<STRING>) FROM src");
    failure.assertFailure(
        ArithmeticException.class, failure.host().phase(), NativeFailureParity.Route.NATIVE);
    for (String sql :
        List.of(
            "SELECT " + cast + "(t AS ARRAY<INT>) FROM src WHERE id>0",
            "SELECT CASE WHEN id=0 THEN CAST(NULL AS ARRAY<STRING>) ELSE "
                + cast
                + "(ARRAY[10/id] AS ARRAY<STRING>) END FROM src",
            "SELECT " + cast + "(t AS ARRAY<INT>) FROM src WHERE id<0")) {
      assertEquivalent(run(factory, sql));
    }
  }

  @Test
  void collectionSwitchStillControlsTheNewStandaloneSignatures() {
    var table = casts(false);
    table.getConfig().set("streamfusion.experimental.collectionExpressions", "false");
    var scan = NativePlanner.install(table);
    table.explainSql(
        "SELECT CAST(a AS ARRAY<DOUBLE>),CAST(a AS STRING),TRY_CAST(m AS CHAR(8)) FROM src");
    assertEquals(0, scan.substitutions(), scan::explainSummary);
  }

  @Test
  void sensitiveCastsRetainFallbackBeforeUnverifiedStateAndWriters() throws Exception {
    NativeParity.assertFallbackReasonContains(
        () -> casts(false),
        "SELECT COUNT(*) FROM src GROUP BY CAST(a AS ARRAY<STRING>)",
        "collection value representation requires a final projection");
    NativeParity.assertFallbackReasonContains(
        () -> casts(false),
        "SELECT CAST(a AS STRING),COUNT(*) FROM src GROUP BY CAST(a AS STRING)",
        "collection value representation requires a final projection");
    var table = casts(false);
    table.executeSql(
        "CREATE TABLE sink (v ARRAY<STRING>) WITH ('connector'='filesystem',"
            + " 'path'='/tmp/unused-standalone-collection-sink', 'format'='parquet')");
    var scan = NativePlanner.install(table);
    table.explainSql("INSERT INTO sink SELECT CAST(a AS ARRAY<STRING>) FROM src");
    assertEquals(0, scan.substitutions(), scan::explainSummary);
    assertTrue(
        scan.fallbackReasons().stream().anyMatch(reason -> reason.contains("row sink")),
        scan::explainSummary);
  }

  private static NativeFailureParity.Comparison run(
      Supplier<TableEnvironment> factory, String sql) {
    var earlier =
        METRICS.nativeOperators().stream()
            .map(row -> row.get("jobId"))
            .collect(java.util.stream.Collectors.toSet());
    var runs = NativeFailureParity.run(factory, sql);
    if (runs.nativeRun().failure() == null) {
      var work =
          METRICS.nativeOperators().stream()
              .filter(row -> !earlier.contains(row.get("jobId")))
              .collect(java.util.stream.Collectors.toList());
      RecoveryExecutionMetrics.assertConsumed(work, "NativeCalc");
    }
    return runs;
  }

  private static void assertEquivalent(NativeFailureParity.Comparison runs) {
    if (runs.host().failure() == null) runs.assertSuccess(NativeFailureParity.Route.NATIVE);
    else
      runs.assertFailure(
          runs.host().rootCause().getClass(),
          runs.host().phase(),
          NativeFailureParity.Route.NATIVE);
    assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
    assertTrue(runs.nativeRun().plan().contains("NativeCalc"), runs::toString);
  }

  private static TableEnvironment casts(boolean legacy) {
    var map = new LinkedHashMap<String, Integer>();
    map.put("a", 1);
    map.put(null, -2);
    map.put("nil", null);
    var table =
        environment(
            ROW(
                FIELD("id", INT()),
                FIELD("a", ARRAY(INT())),
                FIELD("d", ARRAY(DECIMAL(10, 2))),
                FIELD("t", ARRAY(STRING())),
                FIELD("nested", ARRAY(ARRAY(INT()))),
                FIELD("m", MAP(STRING(), INT())),
                FIELD("r", ROW(FIELD("x", INT()), FIELD("y", ARRAY(INT())))),
                FIELD("dates", ARRAY(DATE())),
                FIELD("times", ARRAY(TIMESTAMP(9)))),
            List.of(
                Row.of(
                    0,
                    new Integer[] {1, null, -2},
                    new BigDecimal[] {new BigDecimal("1.25"), null, new BigDecimal("-2.55")},
                    new String[] {"1", null, "bad"},
                    new Integer[][] {new Integer[] {1, null}, null, new Integer[] {}},
                    map,
                    Row.of(-1, new Integer[] {1, null}),
                    new java.time.LocalDate[] {java.time.LocalDate.of(2024, 2, 29), null},
                    new java.time.LocalDateTime[] {
                      java.time.LocalDateTime.of(2024, 2, 29, 12, 34, 56, 987654321), null
                    }),
                Row.of(
                    1,
                    new Integer[] {},
                    new BigDecimal[] {},
                    new String[] {"-2", null, "3"},
                    new Integer[][] {},
                    new LinkedHashMap<>(),
                    Row.of(null, new Integer[] {}),
                    new java.time.LocalDate[] {},
                    new java.time.LocalDateTime[] {}),
                Row.of(2, null, null, null, null, null, null, null, null)));
    table.getConfig().set("table.exec.legacy-cast-behaviour", legacy ? "ENABLED" : "DISABLED");
    return table;
  }

  private TableEnvironment strings() {
    return environment(
        ROW(FIELD("id", INT()), FIELD("s", STRING()), FIELD("p", STRING())),
        List.of(
            Row.of(0, "|a||", "|"),
            Row.of(1, "", ""),
            Row.of(2, "abc", ""),
            Row.of(3, "\ud83d\ude00é中", ""),
            Row.of(4, "a.*b.*", ".*"),
            Row.of(5, "a中b中", "中"),
            Row.of(6, "aaaaa", "aa"),
            Row.of(7, null, "|"),
            Row.of(8, "a|b", null),
            Row.of(9, "a\ud800b\udc00", ""),
            Row.of(10, "\ud83d\ude00|x", "|")));
  }

  private static TableEnvironment environment(DataType type, List<Row> rows) {
    var env = METRICS.environment(new Configuration(), 1, 0);
    env.getConfig().enableObjectReuse();
    var table = StreamTableEnvironment.create(env);
    table.getConfig().set("streamfusion.transpose.batchRows", "3");
    table.createTemporaryView(
        "src",
        fromData(env, rows, ExternalTypeInfo.<Row>of(type)),
        Schema.newBuilder().fromRowDataType(type).build());
    return table;
  }
}
