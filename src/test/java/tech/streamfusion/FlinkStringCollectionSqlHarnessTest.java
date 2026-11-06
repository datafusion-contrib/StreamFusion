package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

@TestInstance(TestInstance.Lifecycle.PER_METHOD)
class FlinkStringCollectionSqlHarnessTest {
  private final RecoveryExecutionMetrics metrics = new RecoveryExecutionMetrics();

  @AfterEach
  void closeCluster() {
    metrics.close();
  }

  private static Schema schema() {
    return Schema.newBuilder().column("k", DataTypes.INT()).column("v", DataTypes.STRING()).build();
  }

  private static List<Row> rows() {
    String[] values = {
      null,
      "",
      "a",
      "é🙂",
      "\ud800",
      "\udc00",
      "?",
      "\ue000",
      "\ud800\udc00",
      "x".repeat(4097),
      "a\u0000b"
    };
    RowKind[] kinds = {
      RowKind.INSERT,
      RowKind.INSERT,
      RowKind.UPDATE_BEFORE,
      RowKind.UPDATE_AFTER,
      RowKind.DELETE,
      RowKind.DELETE,
      RowKind.INSERT
    };
    var rows = new ArrayList<Row>();
    for (int cycle = 0; cycle < 2; cycle++)
      for (int phase = 0; phase < kinds.length; phase++)
        for (int key = 0; key < 3; key++)
          for (String value : values)
            rows.add(
                Row.ofKind(
                    kinds[phase],
                    key == 0 ? null : key,
                    phase == 1 || phase == 5
                        ? null
                        : (phase == 3 || phase == 4) && value != null ? "next:" + value : value));
    return rows;
  }

  private TableEnvironment environment(String type, int parallelism) {
    return environment(type, parallelism, false);
  }

  private TableEnvironment environment(String type, int parallelism, boolean notNull) {
    var env = metrics.environment(new Configuration(), parallelism, 0);
    env.getConfig().enableObjectReuse();
    var table = StreamTableEnvironment.create(env);
    table.getConfig().set("table.optimizer.agg-phase-strategy", "ONE_PHASE");
    table.getConfig().set("streamfusion.transpose.batchRows", "3");
    table.createTemporaryView(
        "base",
        table.fromChangelogStream(
            tech.streamfusion.compat.FlinkTestSources.fromData(
                env, rows(), Types.ROW_NAMED(new String[] {"k", "v"}, Types.INT, Types.STRING)),
            notNull
                ? Schema.newBuilder()
                    .column("k", DataTypes.INT())
                    .column("v", DataTypes.STRING().notNull())
                    .build()
                : schema()));
    table.createTemporaryView("src", table.sqlQuery("SELECT k," + value(type) + " AS v FROM base"));
    return table;
  }

  private static String value(String type) {
    return type.equals("STRING") ? "v" : "CAST(v AS " + type + ")";
  }

  private static String operator(String function) {
    return function.equals("COLLECT") ? "NativeJvmGroup" : "NativeColumnarGroupAggregate";
  }

  private static String grouped(String function, boolean global) {
    return "SELECT "
        + (global ? "" : "k,")
        + function
        + "(v),"
        + function
        + "(DISTINCT v),"
        + function
        + "(v) FILTER (WHERE k>0),COUNT(*) FROM src"
        + (global ? "" : " GROUP BY k");
  }

  @ParameterizedTest
  @CsvSource({
    "COLLECT,STRING,1",
    "COLLECT,STRING,3",
    "COLLECT,CHAR(8),1",
    "COLLECT,CHAR(8),3",
    "ARRAY_AGG,STRING,1",
    "ARRAY_AGG,STRING,3",
    "ARRAY_AGG,CHAR(8),1",
    "ARRAY_AGG,CHAR(8),3"
  })
  void stringFamiliesKeepReleasedChangesAndDeclaredTypes(
      String function, String type, int parallelism) {
    check(function, () -> environment(type, parallelism), grouped(function, false), true);
  }

  @ParameterizedTest
  @CsvSource({"COLLECT,STRING", "COLLECT,CHAR(8)", "ARRAY_AGG,STRING", "ARRAY_AGG,CHAR(8)"})
  void globalCollectionsKeepTheSameInputBoundary(String function, String type) {
    check(function, () -> environment(type, 1), grouped(function, true), false);
  }

  @ParameterizedTest
  @CsvSource({"COLLECT,STRING", "COLLECT,CHAR(8)", "ARRAY_AGG,STRING", "ARRAY_AGG,CHAR(8)"})
  void formattingAndScalarConsumersObserveOwnedResults(String function, String type) {
    check(
        function,
        () -> environment(type, 1),
        "SELECT k,CAST(" + function + "(v) AS STRING) FROM src GROUP BY k",
        true);
  }

  @ParameterizedTest
  @CsvSource({"STRING", "CHAR(8)"})
  void arrayNullPoliciesAndNonNullResultsKeepTheirDeclaredElementType(String type) {
    check(
        "ARRAY_AGG",
        () -> environment(type, 1),
        "SELECT k,ARRAY_AGG(v) RESPECT NULLS,ARRAY_AGG(v) IGNORE NULLS,"
            + "ARRAY_AGG(COALESCE(v,'fallback')) FROM src GROUP BY k",
        true);
  }

  @ParameterizedTest
  @CsvSource({
    "COLLECT,STRING,hashmap",
    "COLLECT,CHAR(8),hashmap",
    "COLLECT,STRING,tech.streamfusion.state.RocksDBNativeStateBackendFactory",
    "COLLECT,CHAR(8),tech.streamfusion.state.RocksDBNativeStateBackendFactory",
    "ARRAY_AGG,STRING,hashmap",
    "ARRAY_AGG,CHAR(8),hashmap",
    "ARRAY_AGG,STRING,tech.streamfusion.state.RocksDBNativeStateBackendFactory",
    "ARRAY_AGG,CHAR(8),tech.streamfusion.state.RocksDBNativeStateBackendFactory"
  })
  void stringStateRecoversAcrossTwoNonemptyPrefixes(String function, String type, String backend)
      throws Exception {
    tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction(function);
    try (var recovery =
        new PortableSqlRecovery(
                backend,
                rows(),
                Types.ROW_NAMED(new String[] {"k", "v"}, Types.INT, Types.STRING),
                schema(),
                true,
                33,
                132)
            .withExecutionMetrics()) {
      String query =
          "SELECT k," + function + "(v)," + function + "(DISTINCT v),COUNT(*) FROM src GROUP BY k";
      var runs =
          NativeFailureParity.runRecovery(
              () -> recoveryInput(recovery.uninterrupted(), type),
              () -> recoveryInput(recovery.get(), type),
              query);
      runs.assertSuccess(NativeFailureParity.Route.NATIVE);
      assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
      assertEquals(perKey(runs.host().rows()), perKey(runs.nativeRun().rows()));
      recovery.verifyRepeatedRecovery();
      RecoveryExecutionMetrics.assertConsumed(recovery.nativeExecution(), operator(function));
      SharedFlinkCluster.assertNativeMemoryReleased("string-collection-recovery");
    }
  }

  @ParameterizedTest
  @CsvSource({"COLLECT,STRING", "COLLECT,CHAR(8)", "ARRAY_AGG,STRING", "ARRAY_AGG,CHAR(8)"})
  void emptyInputsKeepCollectionTypesWithoutRequiringNonemptyAggregateWork(
      String function, String type) {
    tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction(function);
    for (boolean keyed : new boolean[] {false, true}) {
      var runs =
          NativeFailureParity.run(
              () -> environment(type, 1),
              "SELECT "
                  + (keyed ? "k," : "")
                  + function
                  + "(v),COUNT(*) FROM src WHERE k<0"
                  + (keyed ? " GROUP BY k" : ""));
      runs.assertSuccess(NativeFailureParity.Route.NATIVE);
      assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
      assertEquals(runs.host().rows(), runs.nativeRun().rows());
    }
  }

  @ParameterizedTest
  @CsvSource({"STRING", "CHAR(8)"})
  void explicitLegacyArrayStateKeepsTheJvmHandler(String type) {
    String property = "streamfusion.experimental.rustCollectionAggregates";
    String previous = System.getProperty(property);
    try {
      System.setProperty(property, "false");
      check(
          "ARRAY_AGG",
          () -> environment(type, 1),
          grouped("ARRAY_AGG", false),
          true,
          "NativeJvmGroup");
    } finally {
      if (previous == null) System.clearProperty(property);
      else System.setProperty(property, previous);
    }
  }

  @ParameterizedTest
  @CsvSource({"COLLECT,STRING", "COLLECT,CHAR(8)", "ARRAY_AGG,STRING", "ARRAY_AGG,CHAR(8)"})
  void materializedNonNullSourceHintsRetainTheHostBoundary(String function, String type) {
    tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction(function);
    var runs =
        NativeFailureParity.run(
            () -> environment(type, 1, true),
            "SELECT CAST(k AS STRING),"
                + function
                + "(v),"
                + function
                + "(COALESCE(k,0)),COUNT(*) FROM src GROUP BY k");
    if (runs.host().failure() == null) runs.assertSuccess(NativeFailureParity.Route.FALLBACK);
    else
      runs.assertFailure(
          runs.host().rootCause().getClass(),
          runs.host().phase(),
          NativeFailureParity.Route.FALLBACK);
    assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
    assertTrue(
        runs.nativeRun().fallbackReasons().stream()
            .anyMatch(reason -> reason.contains("collection input materialization")),
        runs.toString());
  }

  @ParameterizedTest
  @CsvSource({"STRING", "CHAR(8)"})
  void mixedCharacterCollectionsShareTheVerifiedJvmHandler(String type) {
    tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction("ARRAY_AGG");
    check(
        "COLLECT",
        () -> environment(type, 1),
        "SELECT k,COLLECT(v),ARRAY_AGG(v),ARRAY_AGG(DISTINCT v),COUNT(*) FROM src GROUP BY k",
        true);
  }

  @ParameterizedTest
  @CsvSource({
    "STRING,false,1",
    "STRING,true,1",
    "CHAR(8),false,1",
    "CHAR(8),true,1",
    "STRING,false,3",
    "STRING,true,3",
    "CHAR(8),false,3",
    "CHAR(8),true,3"
  })
  void scalarArrayExpansionPreservesOriginalValuesAndChanges(
      String type, boolean nonNull, int parallelism) {
    tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction("ARRAY_AGG");
    String argument = nonNull ? "COALESCE(v,CAST('fallback' AS " + type + "))" : "v";
    String producer = "(SELECT k,ARRAY_AGG(" + argument + ") AS a FROM src GROUP BY k) s ";
    String expansion = nonNull ? "NativeGeneratedUnnest" : "NativeUnnest";
    for (String join :
        List.of("CROSS JOIN UNNEST(a) AS u(x)", "LEFT JOIN UNNEST(a) AS u(x) ON TRUE"))
      check(
          "ARRAY_AGG",
          () -> environment(type, parallelism),
          "SELECT k,a,x FROM " + producer + join,
          true,
          "NativeColumnarGroupAggregate",
          expansion);
    check(
        "ARRAY_AGG",
        () -> environment(type, parallelism),
        "SELECT k,a,x FROM " + producer + "CROSS JOIN UNNEST(a) AS u(x) WHERE x IS NOT NULL",
        true,
        "NativeColumnarGroupAggregate",
        expansion);
    if (tech.streamfusion.compat.FlinkTestCapabilities.UNNEST_ORDINALITY)
      check(
          "ARRAY_AGG",
          () -> environment(type, parallelism),
          "SELECT k,a,x,o FROM " + producer + "CROSS JOIN UNNEST(a) WITH ORDINALITY AS u(x,o)",
          true,
          "NativeColumnarGroupAggregate",
          expansion);
  }

  @ParameterizedTest
  @CsvSource({"STRING,false", "STRING,true", "CHAR(8),false", "CHAR(8),true"})
  void legacyArraysUseTheSameExpansionBoundary(String type, boolean nonNull) {
    tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction("ARRAY_AGG");
    String property = "streamfusion.experimental.rustCollectionAggregates";
    String previous = System.getProperty(property);
    try {
      System.setProperty(property, "false");
      String argument = nonNull ? "COALESCE(v,CAST('fallback' AS " + type + "))" : "v";
      check(
          "ARRAY_AGG",
          () -> environment(type, 3),
          "SELECT k,a,x FROM (SELECT k,ARRAY_AGG("
              + argument
              + ") AS a FROM src GROUP BY k) s CROSS JOIN UNNEST(a) AS u(x)",
          true,
          "NativeJvmGroup",
          nonNull ? "NativeGeneratedUnnest" : "NativeUnnest");
    } finally {
      if (previous == null) System.clearProperty(property);
      else System.setProperty(property, previous);
    }
  }

  @ParameterizedTest
  @CsvSource({"STRING,STRING", "STRING,CHAR(4)", "CHAR(8),STRING", "CHAR(8),CHAR(4)"})
  void expansionKeepsCharacterKeysAndScalarConsumers(String type, String keyType) {
    tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction("ARRAY_AGG");
    String key = "CAST(k AS " + keyType + ")";
    String producer =
        "(SELECT " + key + " AS k,ARRAY_AGG(v) AS a FROM src GROUP BY " + key + ") s ";
    check(
        "ARRAY_AGG",
        () -> environment(type, 3),
        "SELECT k,a,x,CHAR_LENGTH(x),CAST(a AS STRING) FROM "
            + producer
            + "CROSS JOIN UNNEST(a) AS u(x)",
        true,
        "NativeColumnarGroupAggregate",
        "NativeUnnest",
        "NativeCalc");
  }

  @ParameterizedTest
  @CsvSource({"STRING", "CHAR(8)"})
  void mixedArrayExpansionRetainsBothNativeAndGeneratedStages(String type) {
    tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction("ARRAY_AGG");
    check(
        "ARRAY_AGG",
        () -> environment(type, 3),
        "SELECT k,a,b,x,y FROM (SELECT k,ARRAY_AGG(v) AS a,ARRAY_AGG(COALESCE(v,CAST('fallback' AS "
            + type
            + "))) AS b FROM src WHERE v IN (CAST('' AS "
            + type
            + "),CAST('a' AS "
            + type
            + ")) GROUP BY k) s CROSS JOIN UNNEST(a) AS u(x)"
            + " CROSS JOIN UNNEST(b) AS t(y)",
        true,
        "NativeColumnarGroupAggregate",
        "NativeUnnest",
        "NativeGeneratedUnnest");
  }

  @ParameterizedTest
  @CsvSource({
    "STRING,false,hashmap",
    "STRING,true,hashmap",
    "CHAR(8),false,hashmap",
    "CHAR(8),true,hashmap",
    "STRING,false,tech.streamfusion.state.RocksDBNativeStateBackendFactory",
    "STRING,true,tech.streamfusion.state.RocksDBNativeStateBackendFactory",
    "CHAR(8),false,tech.streamfusion.state.RocksDBNativeStateBackendFactory",
    "CHAR(8),true,tech.streamfusion.state.RocksDBNativeStateBackendFactory"
  })
  void expandedArraysRecoverAcrossTwoNonemptyPrefixes(String type, boolean nonNull, String backend)
      throws Exception {
    tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction("ARRAY_AGG");
    try (var recovery =
        new PortableSqlRecovery(
                backend,
                rows(),
                Types.ROW_NAMED(new String[] {"k", "v"}, Types.INT, Types.STRING),
                schema(),
                true,
                33,
                132)
            .withExecutionMetrics()) {
      String argument = nonNull ? "COALESCE(v,CAST('fallback' AS " + type + "))" : "v";
      String sql =
          "SELECT k,a,x FROM (SELECT k,ARRAY_AGG("
              + argument
              + ") AS a FROM src GROUP BY k) s LEFT JOIN UNNEST(a) AS u(x) ON TRUE";
      var runs =
          NativeFailureParity.runRecovery(
              () -> recoveryInput(recovery.uninterrupted(), type),
              () -> recoveryInput(recovery.get(), type),
              sql);
      runs.assertSuccess(NativeFailureParity.Route.NATIVE);
      assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
      assertEquals(perKey(runs.host().rows()), perKey(runs.nativeRun().rows()));
      recovery.verifyRepeatedRecovery();
      RecoveryExecutionMetrics.assertConsumed(
          recovery.nativeExecution(), "NativeColumnarGroupAggregate");
      RecoveryExecutionMetrics.assertConsumed(
          recovery.nativeExecution(), nonNull ? "NativeGeneratedUnnest" : "NativeUnnest");
      SharedFlinkCluster.assertNativeMemoryReleased("expanded-string-collection-recovery");
    }
  }

  @ParameterizedTest
  @CsvSource({"STRING,1", "STRING,3", "CHAR(8),1", "CHAR(8),3"})
  void preservedCalcArraysUseNativeExpansion(String type, int parallelism) {
    String array = "ARRAY[v,CAST(NULL AS " + type + ")]";
    for (String expression :
        List.of(array, "CAST(" + array + " AS ARRAY<CHAR(4)>)", "MAP_VALUES(MAP['key',v])"))
      check(
          "COLLECT",
          () -> environment(type, parallelism),
          "SELECT k,a,x FROM (SELECT k,"
              + expression
              + " AS a FROM src) s CROSS JOIN UNNEST(a) AS u(x)",
          true,
          "NativeCalc",
          "NativeUnnest");
  }

  @ParameterizedTest
  @CsvSource({"STRING,1", "STRING,3", "CHAR(8),1", "CHAR(8),3"})
  void preservedSplitArraysUseNativeExpansion(String type, int parallelism) {
    tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction("SPLIT");
    check(
        "COLLECT",
        () -> environment(type, parallelism),
        "SELECT k,x FROM (SELECT k,CAST(SPLIT(v,'') AS ARRAY<STRING>) AS a FROM src) s CROSS JOIN"
            + " UNNEST(a) AS u(x)",
        true,
        "NativeCalc",
        "NativeUnnest");
  }

  @ParameterizedTest
  @CsvSource({"STRING", "CHAR(8)"})
  void scalarConsumersReadExpandedStringCarriers(String type) {
    String producer =
        "(SELECT k,ARRAY[v,CAST(NULL AS "
            + type
            + ")] AS a FROM src) s CROSS JOIN UNNEST(a) AS u(x)";
    check(
        "COLLECT",
        () -> environment(type, 1),
        "SELECT k,x,COALESCE(x,CAST('fallback' AS "
            + type
            + ")),CHAR_LENGTH(x),CAST(x AS CHAR(4)) FROM "
            + producer,
        true,
        "NativeCalc",
        "NativeUnnest");
    check(
        "COLLECT",
        () -> environment(type, 1),
        "SELECT k,a,x FROM " + producer + " WHERE x<'\ue000'",
        true,
        "NativeCalc",
        "NativeGeneratedUnnest");
  }

  @ParameterizedTest
  @CsvSource({"STRING", "CHAR(8)"})
  void typedCharacterLiteralsKeepPaddingInProjectionsAndSearchFilters(String type) {
    check(
        "COLLECT",
        () -> environment(type, 1),
        "SELECT k,v,CAST('' AS CHAR(8)),CAST('a' AS CHAR(8)),v=CAST('' AS CHAR(8)),v=CAST('a' AS"
            + " CHAR(8)) FROM src",
        true,
        "NativeCalc");
    check(
        "COLLECT",
        () -> environment(type, 1),
        "SELECT k,v FROM src WHERE v IN (CAST('' AS CHAR(8)),CAST('a' AS CHAR(8)))",
        true,
        type.equals("STRING") ? "NativeFilter" : "NativeCalc");
    check(
        "COLLECT",
        () -> environment(type, 1),
        "SELECT k,v,v IN (CAST('' AS CHAR(8)),CAST('a' AS CHAR(8))),v NOT IN (CAST('' AS"
            + " CHAR(8)),CAST('a' AS CHAR(8))) FROM src",
        true,
        "NativeCalc");
  }

  @ParameterizedTest
  @CsvSource({"COLLECT,STRING", "COLLECT,CHAR(8)", "ARRAY_AGG,STRING", "ARRAY_AGG,CHAR(8)"})
  void scalarProjectionsPassCollectionColumnsThroughNativeCalc(String function, String type) {
    check(function, () -> environment(type, 3),
        "SELECT k," + function + "(v),(SUM(k)+1),COUNT(*) FROM src GROUP BY k",
        true, operator(function), "NativeCalc");
  }

  private static TableEnvironment recoveryInput(TableEnvironment table, String type) {
    table.getConfig().set("table.optimizer.agg-phase-strategy", "ONE_PHASE");
    table.getConfig().set("streamfusion.transpose.batchRows", "3");
    table.createTemporaryView(
        "src", table.sqlQuery("SELECT k," + value(type) + " AS v FROM recovery_input"));
    return table;
  }

  private void check(
      String function,
      java.util.function.Supplier<TableEnvironment> factory,
      String sql,
      boolean keyed) {
    check(function, factory, sql, keyed, operator(function));
  }

  private void check(
      String function,
      java.util.function.Supplier<TableEnvironment> factory,
      String sql,
      boolean keyed,
      String... expectedOperators) {
    tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction(function);
    var previous =
        metrics.nativeOperators().stream()
            .map(row -> row.get("jobId"))
            .collect(java.util.stream.Collectors.toSet());
    var runs = NativeFailureParity.run(factory, sql);
    runs.assertSuccess(NativeFailureParity.Route.NATIVE);
    assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
    if (keyed) assertEquals(perKey(runs.host().rows()), perKey(runs.nativeRun().rows()));
    else assertEquals(runs.host().rows(), runs.nativeRun().rows());
    var observations =
        metrics.nativeOperators().stream()
            .filter(row -> !previous.contains(row.get("jobId")))
            .collect(java.util.stream.Collectors.toList());
    for (String expectedOperator : expectedOperators)
      RecoveryExecutionMetrics.assertConsumed(observations, expectedOperator);
  }

  private static java.util.Map<Object, List<List<Object>>> perKey(List<List<Object>> rows) {
    var result = new java.util.HashMap<Object, List<List<Object>>>();
    for (var row : rows) result.computeIfAbsent(row.get(1), ignored -> new ArrayList<>()).add(row);
    return result;
  }
}
