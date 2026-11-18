package tech.streamfusion;

import static org.apache.flink.table.api.DataTypes.*;
import static org.junit.jupiter.api.Assertions.*;
import static tech.streamfusion.compat.FlinkTestSources.fromData;

import java.util.LinkedHashMap;
import java.util.List;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.runtime.typeutils.ExternalTypeInfo;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.planner.NativePlanner;

class FlinkMultisetCastSqlHarnessTest {
  private static final RecoveryExecutionMetrics METRICS = new RecoveryExecutionMetrics();

  @AfterAll
  static void closeCluster() {
    METRICS.close();
  }

  private static TableEnvironment environment(boolean legacy) {
    return environment(legacy, false);
  }

  private static TableEnvironment environment(boolean legacy, boolean changelog) {
    var env = METRICS.environment(new Configuration(), 1, 0);
    env.getConfig().enableObjectReuse();
    var table = StreamTableEnvironment.create(env);
    table.getConfig().set("streamfusion.transpose.batchRows", "2");
    table.getConfig().set("table.exec.legacy-cast-behaviour", legacy ? "ENABLED" : "DISABLED");
    var ints = new LinkedHashMap<Integer, Integer>();
    ints.put(1, 2);
    ints.put(257, 3);
    ints.put(null, 4);
    ints.put(-1, 0);
    var text = new LinkedHashMap<String, Integer>();
    text.put("1", 2);
    text.put("bad", 3);
    text.put(null, 1);
    text.put("\ud83d\ude00\ud800", 5);
    var nested = new LinkedHashMap<Row, Integer>();
    nested.put(Row.of(1, new String[] {"\ud83d\ude00", null, "\ud800"}), 2);
    nested.put(Row.of(null, new String[] {}), 3);
    nested.put(null, 1);
    var decimals = new LinkedHashMap<java.math.BigDecimal, Integer>();
    decimals.put(new java.math.BigDecimal("1.25"), 2);
    decimals.put(new java.math.BigDecimal("-2.55"), 3);
    decimals.put(null, 1);
    var type =
        ROW(
            FIELD("id", INT()),
            FIELD("m", MULTISET(INT())),
            FIELD("t", MULTISET(STRING())),
            FIELD("r", MULTISET(ROW(FIELD("x", INT()), FIELD("a", ARRAY(STRING()))))),
            FIELD("d", MULTISET(DECIMAL(10, 2))));
    var rows =
        List.of(
            Row.of(0, ints, text, nested, decimals),
            Row.of(
                1,
                new LinkedHashMap<>(),
                new LinkedHashMap<>(),
                new LinkedHashMap<>(),
                new LinkedHashMap<>()),
            Row.of(2, null, null, null, null));
    var schema = Schema.newBuilder().fromRowDataType(type).build();
    if (changelog) {
      var changes = new java.util.ArrayList<Row>();
      for (var kind : org.apache.flink.types.RowKind.values()) {
        for (var row : rows) {
          var change = Row.copy(row);
          change.setKind(kind);
          changes.add(change);
        }
      }
      table.createTemporaryView(
          "src",
          table.fromChangelogStream(
              fromData(env, changes, ExternalTypeInfo.<Row>of(type)),
              schema,
              org.apache.flink.table.connector.ChangelogMode.all()));
    } else {
      table.createTemporaryView("src", fromData(env, rows, ExternalTypeInfo.<Row>of(type)), schema);
    }
    return table;
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "SELECT id,CAST(m AS MULTISET<BIGINT>),TRY_CAST(m AS MULTISET<TINYINT>) FROM src",
        "SELECT id,CAST(m AS MULTISET<STRING>),CAST(m AS STRING),TRY_CAST(m AS CHAR(12)),CAST(m AS"
            + " VARCHAR(2)) FROM src",
        "SELECT id,TRY_CAST(t AS MULTISET<INT>) FROM src",
        "SELECT id,ROW(m),JSON_QUERY(CAST(id AS STRING),'$') FROM src",
        "SELECT id,ARRAY[m],JSON_QUERY(CAST(id AS STRING),'$') FROM src",
        "SELECT id,CAST(t AS MULTISET<INT>) FROM src",
        "SELECT id,CAST(t AS STRING),TRY_CAST(t AS CHAR(8)) FROM src",
        "SELECT id,CAST(r AS MULTISET<ROW<x BIGINT,a ARRAY<STRING>>>),CAST(r AS STRING) FROM src",
        "SELECT id,CAST(d AS MULTISET<FLOAT>),TRY_CAST(d AS MULTISET<DECIMAL(4,1)>),CAST(d AS"
            + " CHAR(16)) FROM src",
        "SELECT id,CASE WHEN id=0 THEN CAST(NULL AS MULTISET<BIGINT>) ELSE CAST(m AS"
            + " MULTISET<BIGINT>) END FROM src",
        "SELECT CAST(m AS MULTISET<STRING>) FROM src WHERE id<0",
        "SELECT CAST(t AS MULTISET<INT>) FROM src WHERE id>0"
      })
  void standaloneCastsPreserveCountsNullsCollisionsAndFailures(String sql) {
    for (boolean legacy : new boolean[] {false, true}) assertEquivalent(legacy, false, sql);
  }

  private static NativeFailureParity.Comparison assertEquivalent(
      boolean legacy, boolean changelog, String sql) {
    return assertEquivalent(() -> environment(legacy, changelog), sql);
  }

  private static NativeFailureParity.Comparison assertEquivalent(
      java.util.function.Supplier<TableEnvironment> factory, String sql) {
    var previous =
        METRICS.nativeOperators().stream()
            .map(row -> row.get("jobId"))
            .collect(java.util.stream.Collectors.toSet());
    var runs = NativeFailureParity.run(factory, sql);
    if (!sql.equals("SELECT id,CAST(t AS MULTISET<INT>) FROM src"))
      assertNull(runs.host().failure(), runs::toString);
    if (runs.host().failure() == null) {
      runs.assertSuccess(NativeFailureParity.Route.NATIVE);
      RecoveryExecutionMetrics.assertConsumed(
          METRICS.nativeOperators().stream()
              .filter(row -> !previous.contains(row.get("jobId")))
              .collect(java.util.stream.Collectors.toList()),
          "NativeCalc");
    } else {
      assertInstanceOf(NumberFormatException.class, runs.host().rootCause());
      runs.assertFailure(
          runs.host().rootCause().getClass(),
          runs.host().phase(),
          NativeFailureParity.Route.NATIVE);
    }
    assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
    assertTrue(runs.nativeRun().plan().contains("NativeCalc"), runs::toString);
    return runs;
  }

  @Test
  void allInputChangelogKindsPreserveNestedMultisetCasts() {
    var runs =
        assertEquivalent(
            false,
            true,
            "SELECT id,CAST(m AS MULTISET<STRING>),CAST(r AS STRING),TRY_CAST(t AS CHAR(12)) FROM"
                + " src");
    assertEquals(
        java.util.Set.of("+I", "-U", "+U", "-D"),
        runs.nativeRun().rows().stream()
            .map(row -> row.get(0))
            .collect(java.util.stream.Collectors.toSet()));
  }

  @Test
  void invalidElementNullsTheWholeTryCastMultiset() {
    var runs = assertEquivalent(false, false, "SELECT id,TRY_CAST(t AS MULTISET<INT>) FROM src");
    assertTrue(runs.host().rows().contains(java.util.Arrays.asList("+I", 0, null)), runs::toString);
  }

  private static TableEnvironment binaryEnvironment(
      boolean legacy, boolean nullable, boolean changelog) {
    var env = METRICS.environment(new Configuration(), 1, 0);
    env.getConfig().enableObjectReuse();
    var table = StreamTableEnvironment.create(env);
    table.getConfig().set("streamfusion.transpose.batchRows", "3");
    table.getConfig().set("table.exec.legacy-cast-behaviour", legacy ? "ENABLED" : "DISABLED");
    table.executeSql(
        "CREATE TABLE src (id BIGINT,t ARRAY<STRING>,r ARRAY<ROW<n INT>>,"
            + "nn ARRAY<INT NOT NULL>,m MULTISET<BINARY(4)"
            + (nullable ? "" : " NOT NULL")
            + ">) WITH ('connector'='collection-representation','changelog'='"
            + changelog
            + "')");
    return table;
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "SELECT id,m,CAST(m AS STRING),TRY_CAST(m AS CHAR(24)),CAST(m AS VARCHAR(2)) FROM src",
        "SELECT id,CAST(m AS MULTISET<BINARY(2)>),TRY_CAST(m AS MULTISET<BINARY(8)>) FROM src",
        "SELECT id,CAST(m AS MULTISET<VARBINARY(8)>),TRY_CAST(m AS MULTISET<STRING>) FROM src",
        "SELECT id,CASE WHEN id=0 THEN CAST(NULL AS MULTISET<BINARY(4)>) ELSE m END FROM src",
        "SELECT id,ROW(m),ARRAY[m],JSON_QUERY(CAST(id AS STRING),'$') FROM src",
        "SELECT id,CAST(m AS STRING) FROM src WHERE MOD(id,3)=0",
        "SELECT CAST(m AS MULTISET<STRING>) FROM src WHERE id<0"
      })
  void binaryElementsRetainActualLengthsCountsAndReleasedCasts(String sql) {
    for (boolean legacy : new boolean[] {false, true})
      for (boolean nullable : new boolean[] {false, true}) {
        if (nullable
            || org.apache.flink.runtime.util.EnvironmentInformation.getVersion().startsWith("2.2."))
          assertEquivalent(() -> binaryEnvironment(legacy, nullable, true), sql);
        else
          NativeFailureParity.run(() -> binaryEnvironment(legacy, nullable, true), sql)
              .assertSuccess(NativeFailureParity.Route.FALLBACK);
      }
  }

  @Test
  void binaryMultisetCastsRetainInputErrorsAndShortCircuiting() {
    for (String cast : List.of("CAST", "TRY_CAST")) {
      String expression =
          cast
              + "(CASE WHEN 10/id>0 THEN m ELSE CAST(NULL AS MULTISET<BINARY(4)>) END AS"
              + " MULTISET<VARBINARY(8)>)";
      var failure =
          NativeFailureParity.run(
              () -> binaryEnvironment(false, true, false), "SELECT " + expression + " FROM src");
      failure.assertFailure(
          ArithmeticException.class, failure.host().phase(), NativeFailureParity.Route.NATIVE);
      assertEquivalent(
          () -> binaryEnvironment(false, true, false),
          "SELECT id,CASE WHEN id=0 THEN CAST(NULL AS MULTISET<VARBINARY(8)>) ELSE "
              + expression
              + " END FROM src");
    }
  }

  @Test
  void binaryMultisetRetainsLengthDelimitedAndRepresentationGuards() throws Exception {
    NativeParity.assertFallbackReasonContains(
        () -> binaryEnvironment(false, true, false),
        "SELECT CAST(m AS MULTISET<STRING>),COUNT(*) FROM src GROUP BY CAST(m AS MULTISET<STRING>)",
        "collection value representation requires a final projection");
    for (String property :
        List.of(
            "streamfusion.experimental.collectionExpressions",
            "streamfusion.experimental.nestedScalarUdfs")) {
      String previous = System.getProperty(property);
      try {
        System.setProperty(property, "false");
        for (String sql :
            List.of(
                "SELECT CAST(m AS STRING),TRY_CAST(m AS MULTISET<BINARY(8)>) FROM src",
                "SELECT id,CASE WHEN id=0 THEN CAST(NULL AS MULTISET<BINARY(4)>) ELSE m END FROM"
                    + " src")) {
          NativeFailureParity.run(() -> binaryEnvironment(false, true, false), sql)
              .assertSuccess(NativeFailureParity.Route.FALLBACK);
        }
      } finally {
        if (previous == null) System.clearProperty(property);
        else System.setProperty(property, previous);
      }
    }
  }

  @Test
  void collectionSwitchAndUnverifiedStateRetainFallback() throws Exception {
    var table = environment(false);
    table.getConfig().set("streamfusion.experimental.collectionExpressions", "false");
    var scan = NativePlanner.install(table);
    table.explainSql("SELECT CAST(m AS MULTISET<BIGINT>),CAST(m AS STRING) FROM src");
    assertEquals(0, scan.substitutions(), scan::explainSummary);
    NativeParity.assertFallbackReasonContains(
        () -> environment(false),
        "SELECT CAST(m AS MULTISET<BIGINT>),COUNT(*) FROM src GROUP BY CAST(m AS MULTISET<BIGINT>)",
        "collection value representation requires a final projection");
  }
}
