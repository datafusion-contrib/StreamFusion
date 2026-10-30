package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.util.*;
import java.util.function.Supplier;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

@TestInstance(TestInstance.Lifecycle.PER_METHOD)
class FlinkNestedCollectionSqlHarnessTest {
  private final RecoveryExecutionMetrics metrics = new RecoveryExecutionMetrics();

  @AfterEach
  void closeCluster() {
    metrics.close();
  }

  private static List<Row> rows() {
    var rows = new ArrayList<Row>();
    for (int cycle = 0; cycle < 2; cycle++)
      for (RowKind kind :
          List.of(
              RowKind.INSERT,
              RowKind.INSERT,
              RowKind.UPDATE_BEFORE,
              RowKind.UPDATE_AFTER,
              RowKind.DELETE,
              RowKind.DELETE,
              RowKind.INSERT))
        for (int key = 0; key < 3; key++)
          for (int value = 0; value < 7; value++)
            rows.add(
                Row.ofKind(
                    kind,
                    key == 0 ? null : key,
                    value == 0 ? null : value % 3,
                    value == 0
                        ? null
                        : value == 1
                            ? ""
                            : value == 2
                                ? "a"
                                : value == 3
                                    ? "é🙂"
                                    : value == 4 ? "\ud800" : "x".repeat(value == 5 ? 257 : 3)));
    return rows;
  }

  private static String type(String shape) {
    switch (shape) {
      case "ARRAY":
        return "ARRAY<INT>";
      case "MAP":
        return "MAP<STRING,INT>";
      default:
        throw new IllegalArgumentException(shape);
    }
  }

  private static String expression(String shape) {
    switch (shape) {
      case "ARRAY":
        return "ARRAY[n,CAST(NULL AS INT)]";
      case "MAP":
        return "MAP[s,n,'other',n+1]";
      default:
        throw new IllegalArgumentException(shape);
    }
  }

  private static String valueExpression(String shape) {
    return "CASE WHEN k IS NULL AND n IS NULL THEN CAST(NULL AS "
        + type(shape)
        + ") ELSE "
        + expression(shape)
        + " END";
  }

  private TableEnvironment environment(String shape, int parallelism) {
    var env = metrics.environment(new Configuration(), parallelism, 0);
    env.getConfig().enableObjectReuse();
    var table = StreamTableEnvironment.create(env);
    table.getConfig().set("table.optimizer.agg-phase-strategy", "ONE_PHASE");
    table.getConfig().set("streamfusion.transpose.batchRows", "3");
    table.createTemporaryView(
        "base",
        table.fromChangelogStream(
            tech.streamfusion.compat.FlinkTestSources.fromData(
                env,
                rows(),
                Types.ROW_NAMED(new String[] {"k", "n", "s"}, Types.INT, Types.INT, Types.STRING)),
            Schema.newBuilder()
                .column("k", DataTypes.INT())
                .column("n", DataTypes.INT())
                .column("s", DataTypes.STRING())
                .build()));
    table.createTemporaryView(
        "src", table.sqlQuery("SELECT k," + valueExpression(shape) + " AS v FROM base"));
    return table;
  }

  @ParameterizedTest
  @CsvSource({
    "COLLECT,MAP",
    "ARRAY_AGG,MAP",
  })
  void nestedElementsKeepValuesAndChangelog(String function, String shape) {
    check(
        function,
        () -> environment(shape, 1),
        "SELECT k,"
            + function
            + "(v),"
            + function
            + "(v) FILTER(WHERE k>0),COUNT(*) FROM src GROUP BY k",
        true);
  }

  private TableEnvironment internalEnvironment(String shape, boolean binaryMaps) {
    var env = metrics.environment(new Configuration(), 1, 0);
    env.getConfig().enableObjectReuse();
    var table = StreamTableEnvironment.create(env);
    table.getConfig().set("table.optimizer.agg-phase-strategy", "ONE_PHASE");
    table.getConfig().set("streamfusion.transpose.batchRows", "3");
    table.executeSql(
        "CREATE TABLE internal_src (id BIGINT,t ARRAY<STRING>,r ARRAY<ROW<n INT>>,"
            + "nn ARRAY<INT NOT NULL>,m "
            + (shape.equals("MULTISET") ? "STRING MULTISET" : "MAP<STRING,INT>")
            + ") WITH ('connector'='collection-representation','changelog'='true','binary-maps'='"
            + binaryMaps
            + "')");
    String value =
        shape.equals("STRING_ARRAY")
            ? "t"
            : shape.equals("ROW_ARRAY")
                ? "r"
                : shape.equals("DIRECT_ROW") ? "r[2]" : shape.equals("NN_ARRAY") ? "nn" : "m";
    table.createTemporaryView(
        "src", table.sqlQuery("SELECT MOD(id,3) AS k," + value + " AS v FROM internal_src"));
    return table;
  }

  @ParameterizedTest
  @CsvSource({
    "COLLECT,MAP,false",
    "COLLECT,MAP,true",
    "COLLECT,MULTISET,false",
    "COLLECT,MULTISET,true",
    "ARRAY_AGG,MAP,false",
    "ARRAY_AGG,MAP,true",
    "ARRAY_AGG,MULTISET,false",
    "ARRAY_AGG,MULTISET,true"
  })
  void directContainersKeepReleasedRepresentations(
      String function, String shape, boolean binaryMaps) {
    check(
        function,
        () -> internalEnvironment(shape, binaryMaps),
        "SELECT k," + function + "(v),COUNT(*) FROM src GROUP BY k",
        true);
  }

  private TableEnvironment structuredEnvironment() {
    var env = metrics.environment(new Configuration(), 1, 0);
    env.getConfig().enableObjectReuse();
    var table = StreamTableEnvironment.create(env);
    table.getConfig().set("table.optimizer.agg-phase-strategy", "ONE_PHASE");
    table.getConfig().set("streamfusion.transpose.batchRows", "3");
    var input =
        FlinkStructuredCollectionSqlHarnessTest.rows().stream()
            .map(row -> Row.ofKind(row.getKind(), (Integer) row.getField(0) % 3, row.getField(1)))
            .collect(java.util.stream.Collectors.toList());
    table.createTemporaryView(
        "src",
        table.fromChangelogStream(
            tech.streamfusion.compat.FlinkTestSources.fromData(
                env,
                input,
                Types.ROW_NAMED(
                    new String[] {"k", "v"},
                    Types.INT,
                    Types.POJO(FlinkStructuredCollectionSqlHarnessTest.Item.class))),
            Schema.newBuilder()
                .column("k", DataTypes.INT())
                .column("v", FlinkStructuredCollectionSqlHarnessTest.itemType())
                .build()));
    return table;
  }

  @ParameterizedTest
  @CsvSource({"COLLECT", "ARRAY_AGG"})
  void rowAndStructuredElementsKeepTheirUnverifiedStateFallback(String function) {
    tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction(function);
    for (var factory :
        List.<Supplier<TableEnvironment>>of(
            () -> internalEnvironment("DIRECT_ROW", false),
            this::structuredEnvironment)) {
      var runs =
          NativeFailureParity.run(
              factory, "SELECT k," + function + "(v),COUNT(*) FROM src GROUP BY k");
      runs.assertSuccess(NativeFailureParity.Route.FALLBACK);
      assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
      assertPerKey(runs.host().rows(), runs.nativeRun().rows());
    }
  }

  @ParameterizedTest
  @CsvSource({
    "COLLECT,ARRAY",
    "ARRAY_AGG,ARRAY",
    "COLLECT,NN_ARRAY",
    "ARRAY_AGG,NN_ARRAY",
    "COLLECT,ROW_ARRAY",
    "ARRAY_AGG,ROW_ARRAY",
    "COLLECT,STRING_ARRAY",
    "ARRAY_AGG,STRING_ARRAY"
  })
  void arrayElementsKeepTheUnverifiedCopyClassFallback(String function, String shape) {
    tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction(function);
    var table = !shape.equals("ARRAY") ? internalEnvironment(shape, false) : environment(shape, 1);
    var scan = tech.streamfusion.planner.NativePlanner.install(table);
    String plan =
        tech.streamfusion.planner.NativePlanner.explain(
            table, "SELECT k," + function + "(v),COUNT(*) FROM src GROUP BY k");
    assertEquals(0, scan.substitutions(), plan);
  }

  @ParameterizedTest
  @CsvSource({
    "COLLECT,MAP",
    "ARRAY_AGG,MAP",
  })
  void globalAndParallelCollectionsKeepTypes(String function, String shape) {
    check(
        function,
        () -> environment(shape, 1),
        "SELECT " + function + "(v),COUNT(*) FROM src",
        false);
    check(
        function,
        () -> environment(shape, 3),
        "SELECT k," + function + "(v),SUM(k)+1 FROM src GROUP BY k",
        true);
  }

  @ParameterizedTest
  @CsvSource({"MAP"})
  void arrayNullPoliciesAndScalarConsumers(String shape) {
    check(
        "ARRAY_AGG",
        () -> environment(shape, 1),
        "SELECT k,ARRAY_AGG(v) RESPECT NULLS,ARRAY_AGG(v) IGNORE NULLS,"
            + "CAST(ARRAY_AGG(v) AS STRING),CARDINALITY(ARRAY_AGG(v)) FROM src GROUP BY k",
        true);
  }

  @ParameterizedTest
  @CsvSource({"MAP"})
  void unverifiedFormattingAndRetainedPayloadsKeepStockPlans(String shape) {
    for (String sql :
        List.of(
            "SELECT k,CAST(COLLECT(v) AS STRING) FROM src GROUP BY k",
            "SELECT * FROM (SELECT k,COLLECT(v) AS c,SUM(k)+1 AS score FROM src GROUP BY k) ORDER"
                + " BY k LIMIT 2",
            "SELECT * FROM (SELECT k,ARRAY_AGG(v) AS c,SUM(k)+1 AS score FROM src GROUP BY k) ORDER"
                + " BY k LIMIT 2")) {
      if (sql.contains("ARRAY_AGG"))
        tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction("ARRAY_AGG");
      var table = environment(shape, 1);
      var scan = tech.streamfusion.planner.NativePlanner.install(table);
      String plan = tech.streamfusion.planner.NativePlanner.explain(table, sql);
      assertEquals(0, scan.substitutions(), plan);
      assertTrue(
          scan.fallbackReasons().stream()
              .anyMatch(
                  reason ->
                      reason.toLowerCase(java.util.Locale.ROOT).contains("collection")
                          || reason.contains("COLLECT")),
          plan);
    }
  }

  @ParameterizedTest
  @CsvSource({
    "COLLECT,MAP",
    "ARRAY_AGG,MAP",
  })
  void emptyInputsKeepTypes(String function, String shape) {
    tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction(function);
    for (boolean keyed : new boolean[] {false, true}) {
      var runs =
          NativeFailureParity.run(
              () -> environment(shape, 1),
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
  @CsvSource({
    "COLLECT,MAP,hashmap",
    "ARRAY_AGG,MAP,hashmap",
    "COLLECT,MAP,tech.streamfusion.state.RocksDBNativeStateBackendFactory",
    "ARRAY_AGG,MAP,tech.streamfusion.state.RocksDBNativeStateBackendFactory",
  })
  void nestedStateRecoversAcrossTwoPrefixes(String function, String shape, String backend)
      throws Exception {
    tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction(function);
    try (var recovery =
        new PortableSqlRecovery(
                backend,
                rows(),
                Types.ROW_NAMED(new String[] {"k", "n", "s"}, Types.INT, Types.INT, Types.STRING),
                Schema.newBuilder()
                    .column("k", DataTypes.INT())
                    .column("n", DataTypes.INT())
                    .column("s", DataTypes.STRING())
                    .build(),
                true,
                33,
                132)
            .withExecutionMetrics()) {
      var runs =
          NativeFailureParity.runRecovery(
              () -> recoveryInput(recovery.uninterrupted(), shape),
              () -> recoveryInput(recovery.get(), shape),
              "SELECT k," + function + "(v),COUNT(*) FROM src GROUP BY k");
      assertCompleted(runs);
      assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
      assertPerKey(runs.host().rows(), runs.nativeRun().rows());
      recovery.verifyRepeatedRecovery();
      RecoveryExecutionMetrics.assertConsumed(recovery.nativeExecution(), "NativeJvmGroup");
      SharedFlinkCluster.assertNativeMemoryReleased("nested-collection-recovery");
    }
  }

  private static TableEnvironment recoveryInput(TableEnvironment table, String shape) {
    table.getConfig().set("table.optimizer.agg-phase-strategy", "ONE_PHASE");
    table.getConfig().set("streamfusion.transpose.batchRows", "3");
    table.createTemporaryView(
        "src", table.sqlQuery("SELECT k," + valueExpression(shape) + " AS v FROM recovery_input"));
    return table;
  }

  @ParameterizedTest
  @CsvSource({
    "COLLECT,hashmap",
    "ARRAY_AGG,hashmap",
    "COLLECT,tech.streamfusion.state.RocksDBNativeStateBackendFactory",
    "ARRAY_AGG,tech.streamfusion.state.RocksDBNativeStateBackendFactory"
  })
  void multisetStateRecoversAcrossTwoPrefixes(String function, String backend) throws Exception {
    tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction(function);
    var input = new ArrayList<Row>();
    for (var row : rows()) {
      Object value = null;
      if (row.getField(1) != null) {
        var map = new LinkedHashMap<String, Integer>();
        map.put((String) row.getField(2), (Integer) row.getField(1));
        map.put(null, 3);
        value = map;
      }
      input.add(Row.ofKind(row.getKind(), row.getField(0), value));
    }
    var valueType = DataTypes.MULTISET(DataTypes.STRING());
    var valueInfo = org.apache.flink.table.runtime.typeutils.ExternalTypeInfo.of(valueType);
    try (var recovery =
        new PortableSqlRecovery(
                backend,
                input,
                Types.ROW_NAMED(new String[] {"k", "v"}, Types.INT, valueInfo),
                Schema.newBuilder().column("k", DataTypes.INT()).column("v", valueType).build(),
                true,
                33,
                132)
            .withExecutionMetrics()) {
      var runs =
          NativeFailureParity.runRecovery(
              () -> externalRecoveryInput(recovery.uninterrupted()),
              () -> externalRecoveryInput(recovery.get()),
              "SELECT k," + function + "(v),COUNT(*) FROM src GROUP BY k");
      assertCompleted(runs);
      assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
      assertPerKey(runs.host().rows(), runs.nativeRun().rows());
      recovery.verifyRepeatedRecovery();
      RecoveryExecutionMetrics.assertConsumed(recovery.nativeExecution(), "NativeJvmGroup");
      SharedFlinkCluster.assertNativeMemoryReleased("external-collection-recovery");
    }
  }

  private static TableEnvironment externalRecoveryInput(TableEnvironment table) {
    table.getConfig().set("table.optimizer.agg-phase-strategy", "ONE_PHASE");
    table.getConfig().set("streamfusion.transpose.batchRows", "3");
    table.createTemporaryView("src", table.from("recovery_input"));
    return table;
  }

  @ParameterizedTest
  @CsvSource({"COLLECT,MAP", "ARRAY_AGG,MAP", "COLLECT,MULTISET", "ARRAY_AGG,MULTISET"})
  void preservingGroupInputsUpgradeTheExistingSourceTranspose(String function, String shape) {
    tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction(function);
    String sql = "SELECT k," + function + "(v),COUNT(*) FROM src GROUP BY k";
    var table = internalEnvironment(shape, false);
    var scan = tech.streamfusion.planner.NativePlanner.install(table);
    String plan = tech.streamfusion.planner.NativePlanner.explain(table, sql);
    assertTrue(scan.substitutions() > 0, plan);
    assertTrue(plan.contains("RowDataToArrow(collectionRepresentation=[true]"), plan);
    check(function, () -> internalEnvironment(shape, false), sql, true);
  }

  private void check(
      String function, Supplier<TableEnvironment> factory, String sql, boolean keyed) {
    tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction(function);
    var previous =
        metrics.nativeOperators().stream()
            .map(row -> row.get("jobId"))
            .collect(java.util.stream.Collectors.toSet());
    var runs = NativeFailureParity.run(factory, sql);
    assertCompleted(runs);
    assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
    if (keyed) assertPerKey(runs.host().rows(), runs.nativeRun().rows());
    else assertRows(runs.host().rows(), runs.nativeRun().rows());
    RecoveryExecutionMetrics.assertConsumed(
        metrics.nativeOperators().stream()
            .filter(row -> !previous.contains(row.get("jobId")))
            .collect(java.util.stream.Collectors.toList()),
        "NativeJvmGroup");
  }

  private static void assertCompleted(NativeFailureParity.Comparison runs) {
    assertNull(runs.host().failure(), runs::toString);
    assertNull(runs.nativeRun().failure(), runs::toString);
    assertEquals(NativeFailureParity.Route.NATIVE, runs.nativeRun().route(), runs::toString);
  }

  private static void assertPerKey(List<List<Object>> expected, List<List<Object>> actualRows) {
    var wanted = perKey(expected);
    var actual = perKey(actualRows);
    assertEquals(wanted.keySet(), actual.keySet());
    for (var entry : wanted.entrySet()) {
      assertRows(entry.getValue(), actual.get(entry.getKey()));
    }
  }

  private static void assertRows(List<List<Object>> expected, List<List<Object>> actual) {
    assertEquals(expected.size(), actual.size());
    for (int i = 0; i < actual.size(); i++)
      if (!expected.get(i).equals(actual.get(i)))
        fail(
            "change="
                + i
                + " expected="
                + summary(expected.get(i))
                + " actual="
                + summary(actual.get(i)));
  }

  private static String summary(Object value) {
    String s = String.valueOf(value);
    return s.length() < 2400 ? s : s.substring(0, 2400) + "...";
  }

  private static Map<Object, List<List<Object>>> perKey(List<List<Object>> rows) {
    Map<Object, List<List<Object>>> result = new HashMap<>();
    for (var row : rows) result.computeIfAbsent(row.get(1), ignored -> new ArrayList<>()).add(row);
    return result;
  }
}
