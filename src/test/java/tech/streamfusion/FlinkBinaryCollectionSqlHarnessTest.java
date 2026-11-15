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
import tech.streamfusion.planner.NativePlanner;

@TestInstance(TestInstance.Lifecycle.PER_METHOD)
class FlinkBinaryCollectionSqlHarnessTest {
  private final RecoveryExecutionMetrics metrics = new RecoveryExecutionMetrics();

  @AfterEach
  void closeCluster() {
    metrics.close();
  }

  private static Schema schema() {
    return Schema.newBuilder().column("k", DataTypes.INT()).column("v", DataTypes.BYTES()).build();
  }

  private static List<Row> rows() {
    byte[] wide = new byte[4097];
    for (int i = 0; i < wide.length; i++) wide[i] = (byte) (i * 31);
    byte[][] values = {
      null,
      new byte[0],
      new byte[] {0},
      new byte[] {0, 0},
      new byte[] {0, -1, 127},
      new byte[] {0, -1, 127},
      new byte[] {1},
      new byte[] {1, 0},
      wide,
      wide.clone()
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
          for (byte[] value : values)
            rows.add(
                Row.ofKind(
                    kinds[phase],
                    key == 0 ? null : key,
                    phase == 1 || phase == 5
                        ? null
                        : (phase == 3 || phase == 4) && value != null ? changed(value) : value));
    return rows;
  }

  private TableEnvironment environment(String type, int parallelism) {
    return environment(type, parallelism, false);
  }

  private TableEnvironment environment(String type, int parallelism, boolean notNull) {
    var env = metrics.environment(new Configuration(), parallelism, 0);
    env.getConfig().enableObjectReuse();
    var table = StreamTableEnvironment.create(env);
    table.getConfig().set("collect-sink.batch-size.max", "16 mb");
    table.getConfig().set("table.optimizer.agg-phase-strategy", "ONE_PHASE");
    table.getConfig().set("streamfusion.transpose.batchRows", "3");
    table.createTemporaryView(
        "base",
        table.fromChangelogStream(
            tech.streamfusion.compat.FlinkTestSources.fromData(
                env,
                rows(),
                Types.ROW_NAMED(
                    new String[] {"k", "v"}, Types.INT, Types.PRIMITIVE_ARRAY(Types.BYTE))),
            notNull
                ? Schema.newBuilder()
                    .column("k", DataTypes.INT())
                    .column("v", DataTypes.BYTES().notNull())
                    .build()
                : schema()));
    table.createTemporaryView("src", table.sqlQuery("SELECT k," + value(type) + " AS v FROM base"));
    return table;
  }

  private static byte[] changed(byte[] value) {
    byte[] result = java.util.Arrays.copyOf(value, value.length + 1);
    result[value.length] = 42;
    return result;
  }

  private static String value(String type) {
    return "CAST(v AS " + type + ")";
  }

  private static String operator(String function) {
    return "NativeJvmGroup";
  }

  private static String grouped(String function, boolean global) {
    return "SELECT "
        + (global ? "" : "k,")
        + function
        + "(v),"
        + function
        + "(v) FILTER (WHERE k>0),COUNT(*) FROM src"
        + (global ? "" : " GROUP BY k");
  }

  @ParameterizedTest
  @CsvSource({
    "COLLECT,VARBINARY(8192),1",
    "COLLECT,VARBINARY(8192),3",
    "COLLECT,BINARY(8),1",
    "COLLECT,BINARY(8),3",
    "ARRAY_AGG,VARBINARY(8192),1",
    "ARRAY_AGG,VARBINARY(8192),3",
    "ARRAY_AGG,BINARY(8),1",
    "ARRAY_AGG,BINARY(8),3"
  })
  void binaryFamiliesKeepReleasedChangesAndDeclaredTypes(
      String function, String type, int parallelism) {
    check(function, () -> environment(type, parallelism), grouped(function, false), true);
  }

  @ParameterizedTest
  @CsvSource({
    "COLLECT,VARBINARY(8192)",
    "COLLECT,BINARY(8)",
    "ARRAY_AGG,VARBINARY(8192)",
    "ARRAY_AGG,BINARY(8)"
  })
  void globalCollectionsKeepTheSameInputBoundary(String function, String type) {
    check(function, () -> environment(type, 1), grouped(function, true), false);
  }

  @ParameterizedTest
  @CsvSource({
    "COLLECT,VARBINARY(8192)",
    "COLLECT,BINARY(8)",
    "ARRAY_AGG,VARBINARY(8192)",
    "ARRAY_AGG,BINARY(8)"
  })
  void formattingAndScalarConsumersObserveOwnedResults(String function, String type) {
    if (function.equals("COLLECT")) {
      var table = environment(type, 1);
      var scan = NativePlanner.install(table);
      String plan =
          NativePlanner.explain(table, "SELECT k,CAST(COLLECT(v) AS STRING) FROM src GROUP BY k");
      assertEquals(0, scan.substitutions(), plan);
      assertTrue(
          plan.contains("COLLECT formatting over binary or nested elements requires Flink's JVM key order"), plan);
    } else {
      check(
          function,
          () -> environment(type, 1),
          "SELECT k,CAST(" + function + "(v) AS STRING) FROM src GROUP BY k",
          true);
    }
    String cardinality = "SELECT k,CARDINALITY(" + function + "(v)) FROM src GROUP BY k";
    if (function.equals("COLLECT")) {
      var runs = NativeFailureParity.run(() -> environment(type, 1), cardinality);
      assertNotNull(runs.host().failure(), runs.toString());
      runs.assertFailure(
          runs.host().rootCause().getClass(),
          runs.host().phase(),
          NativeFailureParity.Route.FALLBACK);
    } else {
      check(function, () -> environment(type, 1), cardinality, true);
    }
  }

  @ParameterizedTest
  @CsvSource({"VARBINARY(8192)", "BINARY(8)"})
  void arrayNullPoliciesAndNonNullResultsKeepTheirDeclaredElementType(String type) {
    check(
        "ARRAY_AGG",
        () -> environment(type, 1),
        "SELECT k,ARRAY_AGG(v) RESPECT NULLS,ARRAY_AGG(v) IGNORE NULLS,"
            + "ARRAY_AGG(COALESCE(v,X'0102')) FROM src GROUP BY k",
        true);
  }

  @ParameterizedTest
  @CsvSource({
    "COLLECT,VARBINARY(8192),hashmap",
    "COLLECT,BINARY(8),hashmap",
    "COLLECT,VARBINARY(8192),tech.streamfusion.state.RocksDBNativeStateBackendFactory",
    "COLLECT,BINARY(8),tech.streamfusion.state.RocksDBNativeStateBackendFactory",
    "ARRAY_AGG,VARBINARY(8192),hashmap",
    "ARRAY_AGG,BINARY(8),hashmap",
    "ARRAY_AGG,VARBINARY(8192),tech.streamfusion.state.RocksDBNativeStateBackendFactory",
    "ARRAY_AGG,BINARY(8),tech.streamfusion.state.RocksDBNativeStateBackendFactory"
  })
  void binaryStateRecoversAcrossTwoNonemptyPrefixes(String function, String type, String backend)
      throws Exception {
    tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction(function);
    try (var recovery =
        new PortableSqlRecovery(
                backend,
                rows(),
                Types.ROW_NAMED(
                    new String[] {"k", "v"}, Types.INT, Types.PRIMITIVE_ARRAY(Types.BYTE)),
                schema(),
                true,
                33,
                132)
            .withExecutionMetrics()) {
      String query = "SELECT k," + function + "(v),COUNT(*) FROM src GROUP BY k";
      var runs =
          NativeFailureParity.runRecovery(
              () -> recoveryInput(recovery.uninterrupted(), type),
              () -> recoveryInput(recovery.get(), type),
              query);
      assertCompleted(runs, NativeFailureParity.Route.NATIVE);
      assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
      assertPerKey(runs.host().rows(), runs.nativeRun().rows());
      recovery.verifyRepeatedRecovery();
      RecoveryExecutionMetrics.assertConsumed(recovery.nativeExecution(), operator(function));
      SharedFlinkCluster.assertNativeMemoryReleased("string-collection-recovery");
    }
  }

  @ParameterizedTest
  @CsvSource({
    "COLLECT,VARBINARY(8192)",
    "COLLECT,BINARY(8)",
    "ARRAY_AGG,VARBINARY(8192)",
    "ARRAY_AGG,BINARY(8)"
  })
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
  @CsvSource({"VARBINARY(8192)", "BINARY(8)"})
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
  @CsvSource({
    "COLLECT,VARBINARY(8192)",
    "COLLECT,BINARY(8)",
    "ARRAY_AGG,VARBINARY(8192)",
    "ARRAY_AGG,BINARY(8)"
  })
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
  @CsvSource({"VARBINARY(8192)", "BINARY(8)"})
  void mixedBinaryCollectionsShareTheVerifiedJvmHandler(String type) {
    tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction("ARRAY_AGG");
    check(
        "COLLECT",
        () -> environment(type, 1),
        "SELECT k,COLLECT(v),ARRAY_AGG(v),COUNT(*) FROM src GROUP BY k",
        true);
  }

  @ParameterizedTest
  @CsvSource({
    "COLLECT,VARBINARY(8192)",
    "COLLECT,BINARY(8)",
    "ARRAY_AGG,VARBINARY(8192)",
    "ARRAY_AGG,BINARY(8)"
  })
  void distinctBinaryAggregatesRetainReleasedPlanningErrors(String function, String type) {
    tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction(function);
    var runs =
        NativeFailureParity.run(
            () -> environment(type, 1),
            "SELECT k," + function + "(DISTINCT v) FROM src GROUP BY k");
    assertNotNull(runs.host().failure(), runs.toString());
    runs.assertFailure(
        runs.host().rootCause().getClass(),
        runs.host().phase(),
        NativeFailureParity.Route.UNPLANNED);
    assertTrue(
        runs.host()
            .rootCause()
            .getMessage()
            .contains("Distinct aggregate function does not support type"),
        runs.toString());
  }

  public static final class SharedBytes extends org.apache.flink.table.functions.ScalarFunction {
    private final byte[] bytes = {0, -1, 127};

    @org.apache.flink.table.annotation.DataTypeHint("VARBINARY(8192)")
    public byte[] eval(Integer ignored) {
      return bytes;
    }
  }

  @ParameterizedTest
  @CsvSource({
    "COLLECT,VARBINARY(8192)",
    "COLLECT,BINARY(8)",
    "ARRAY_AGG,VARBINARY(8192)",
    "ARRAY_AGG,BINARY(8)"
  })
  void literalAndReusedByteArraysKeepIdentitySemantics(String function, String type) {
    for (boolean keyed : new boolean[] {false, true})
      check(
          function,
          () -> {
            var table = environment(type, 1);
            table.createTemporarySystemFunction("shared_bytes", new SharedBytes());
            return table;
          },
          "SELECT "
              + (keyed ? "k," : "")
              + function
              + "(CAST(shared_bytes(k) AS "
              + type
              + ")),"
              + function
              + "(CAST(X'0001' AS "
              + type
              + ")) FROM src"
              + (keyed ? " GROUP BY k" : ""),
          keyed);
  }

  @ParameterizedTest
  @CsvSource({
    "COLLECT,VARBINARY(8192)",
    "COLLECT,BINARY(8)",
    "ARRAY_AGG,VARBINARY(8192)",
    "ARRAY_AGG,BINARY(8)"
  })
  void binaryGroupingKeysAndScalarResultsKeepTheirDeclaredTypes(String function, String type) {
    check(
        function,
        () -> environment(type, 3),
        "SELECT v," + function + "(v),COUNT(*) FROM src GROUP BY v",
        true);
    check(
        function,
        () -> environment(type, 1),
        "SELECT k," + function + "(v),MIN(k),MAX(k),SUM(k),COUNT(v) FROM src GROUP BY k",
        true);
    for (String extremum : List.of("MIN", "MAX")) {
      var runs =
          NativeFailureParity.run(
              () -> environment(type, 1),
              "SELECT k," + function + "(v)," + extremum + "(v) FROM src GROUP BY k");
      assertNotNull(runs.host().failure(), runs.toString());
      runs.assertFailure(
          runs.host().rootCause().getClass(),
          runs.host().phase(),
          NativeFailureParity.Route.UNPLANNED);
    }
  }

  @ParameterizedTest
  @CsvSource({"false", "true"})
  void variableBinaryCastPreservesReleasedWidthsAndLegacyPolicy(boolean legacy) {
    check(
        "COLLECT",
        () -> {
          var table = environment("VARBINARY(8192)", 1);
          table
              .getConfig()
              .set("table.exec.legacy-cast-behaviour", legacy ? "ENABLED" : "DISABLED");
          return table;
        },
        "SELECT k,CAST(v AS VARBINARY(8)),CAST(CAST(v AS VARBINARY(4)) AS VARBINARY(8)),"
            + "TRY_CAST(v AS VARBINARY(8)) FROM base",
        true,
        "NativeCalc");
  }

  @ParameterizedTest
  @CsvSource({"VARBINARY(8192)", "BINARY(8)"})
  void scalarProjectionDoesNotOpenRetainedMultisetPayloads(String type) {
    var table = environment(type, 1);
    var scan = NativePlanner.install(table);
    String plan =
        NativePlanner.explain(
            table,
            "SELECT * FROM (SELECT k,COLLECT(v) AS c,(SUM(k)+1) AS n FROM src GROUP BY k) ORDER BY"
                + " k LIMIT 2");
    assertEquals(0, scan.substitutions(), plan);
    assertTrue(
        scan.fallbackReasons().stream().anyMatch(reason -> reason.contains("collection")), plan);
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
      String expectedOperator) {
    tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction(function);
    var previous =
        metrics.nativeOperators().stream()
            .map(row -> row.get("jobId"))
            .collect(java.util.stream.Collectors.toSet());
    var runs = NativeFailureParity.run(factory, sql);
    assertCompleted(runs, NativeFailureParity.Route.NATIVE);
    assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
    if (keyed) assertPerKey(runs.host().rows(), runs.nativeRun().rows());
    else assertRows(null, runs.host().rows(), runs.nativeRun().rows());
    RecoveryExecutionMetrics.assertConsumed(
        metrics.nativeOperators().stream()
            .filter(row -> !previous.contains(row.get("jobId")))
            .collect(java.util.stream.Collectors.toList()),
        expectedOperator);
  }

  private static void assertCompleted(
      NativeFailureParity.Comparison runs, NativeFailureParity.Route route) {
    assertNull(runs.host().failure(), runs::toString);
    assertNull(runs.nativeRun().failure(), runs::toString);
    assertEquals(route, runs.nativeRun().route(), runs::toString);
  }

  private static void assertPerKey(List<List<Object>> expected, List<List<Object>> actual) {
    var left = perKey(expected);
    var right = perKey(actual);
    assertEquals(left.keySet(), right.keySet());
    for (var entry : left.entrySet())
      assertRows(entry.getKey(), entry.getValue(), right.get(entry.getKey()));
  }

  private static void assertRows(
      Object key, List<List<Object>> expected, List<List<Object>> actual) {
    assertEquals(expected.size(), actual.size(), "change count for key " + summary(key));
    for (int i = 0; i < expected.size(); i++)
      if (!expected.get(i).equals(actual.get(i)))
        fail(
            "key="
                + summary(key)
                + " change="
                + i
                + " expected="
                + summary(expected.get(i))
                + " actual="
                + summary(actual.get(i)));
  }

  private static String summary(Object value) {
    String text = String.valueOf(value);
    return text.length() <= 2400 ? text : text.substring(0, 2400) + "...";
  }

  private static java.util.Map<Object, List<List<Object>>> perKey(List<List<Object>> rows) {
    var result = new java.util.HashMap<Object, List<List<Object>>>();
    for (var row : rows) result.computeIfAbsent(row.get(1), ignored -> new ArrayList<>()).add(row);
    return result;
  }
}
