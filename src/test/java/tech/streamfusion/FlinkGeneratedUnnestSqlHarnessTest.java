package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;
import static tech.streamfusion.compat.FlinkTestSources.fromData;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

class FlinkGeneratedUnnestSqlHarnessTest {
  @ParameterizedTest
  @ValueSource(
      strings = {
        "MAP['z',id,'a',n,'m',id+1]",
        "MAP[id,n,id,id+1]",
        "MAP[s,id,'Aa',n,'BB',id+1]",
        "MAP[CAST(id AS TINYINT),CAST(n AS SMALLINT),CAST(n AS TINYINT),CAST(id AS SMALLINT)]",
        "MAP[CAST(id AS FLOAT),CAST(n AS DOUBLE),CAST(n AS FLOAT),CAST(id AS DOUBLE)]",
        "MAP[id>2,n,n IS NULL,id]",
        "MAP[CAST(id AS DECIMAL(20,3)),CAST(n AS DECIMAL(20,5)),CAST(n AS DECIMAL(20,3)),CAST(id AS"
            + " DECIMAL(20,5))]",
        "MAP[CAST(TO_TIMESTAMP_LTZ(id,3) AS DATE),CAST(TO_TIMESTAMP_LTZ(n,3) AS TIME(0))]",
        "MAP[TO_TIMESTAMP_LTZ(id,3),TO_TIMESTAMP_LTZ(n,3)]",
        "MAP[id,ROW(s,n),n,ROW(s,id)]",
        "MAP[ROW(s,id),ARRAY[n,id],ROW(s,n),ARRAY[id,n]]",
        "MAP[id,MAP[s,n],n,MAP['x',id]]",
        "MAP[CAST(s AS CHAR(5)),id,CAST('x' AS CHAR(5)),n]"
      })
  void computedMapScalarAndNestedTypesMatchTheReleasedHost(String operand) throws Exception {
    check("SELECT id,mk,mv FROM src CROSS JOIN UNNEST(" + operand + ") AS u(mk,mv)", false);
  }

  @ParameterizedTest
  @MethodSource("basicMapExpansions")
  void basicMapConstructorsUseTheReleasedExpansionRoute(
      String operand, boolean updating, boolean left, boolean ordinality) throws Exception {
    if (ordinality)
      org.junit.jupiter.api.Assumptions.assumeTrue(
          tech.streamfusion.compat.FlinkTestCapabilities.UNNEST_ORDINALITY);
    String sql =
        "SELECT id,mk,mv"
            + (ordinality ? ",o" : "")
            + " FROM src "
            + (left ? "LEFT JOIN" : "CROSS JOIN")
            + " UNNEST("
            + operand
            + ")"
            + (ordinality ? " WITH ORDINALITY" : "")
            + " AS u(mk,mv"
            + (ordinality ? ",o" : "")
            + ")"
            + (left ? " ON TRUE" : "");
    String expected =
        operand.equals("MAP[id,n]") || tech.streamfusion.compat.FlinkCompat.ORDERED_MAP_CONSTRUCTORS
            ? "NativeUnnest"
            : "NativeGeneratedUnnest";
    check(sql, updating, expected);
  }

  private static List<Arguments> basicMapExpansions() {
    List<Arguments> cases = new ArrayList<>();
    for (String operand :
        List.of("MAP[id,n]", "MAP[id,n,n,id+1]", "MAP[CAST(NULL AS BIGINT),id,id,n]"))
      for (boolean updating : new boolean[] {false, true})
        for (boolean left : new boolean[] {false, true})
          for (boolean ordinality : new boolean[] {false, true})
            cases.add(Arguments.of(operand, updating, left, ordinality));
    return cases;
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void computedMapOrdinalityKeepsOrderAndLastSyntacticKey(boolean updating) throws Exception {
    org.junit.jupiter.api.Assumptions.assumeTrue(
        tech.streamfusion.compat.FlinkTestCapabilities.UNNEST_ORDINALITY);
    check(
        "SELECT id,mk,mv,o FROM src LEFT JOIN UNNEST(MAP['z',id,'a',n,'z',n+1, s,id+1])"
            + " WITH ORDINALITY AS u(mk,mv,o) ON TRUE",
        updating);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void upstreamComputedMapKeepsTheOpaqueContainerUntilExpansion(boolean updating) throws Exception {
    check(
        "SELECT id,mk,mv FROM (SELECT id,MAP[s,n,'x',id] AS m FROM src) t"
            + " CROSS JOIN UNNEST(m) AS u(mk,mv)",
        updating);
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "MAP_FROM_ARRAYS(ARRAY[s,'x'],ARRAY[n,id])",
        "MAP_UNION(MAP[s,n],MAP['x',id])",
        "CASE WHEN id=1 THEN CAST(NULL AS MAP<STRING,BIGINT>) ELSE MAP[s,n] END",
        "CASE WHEN id=1 THEN MAP_FROM_ARRAYS(ARRAY_SLICE(ARRAY[s],2,1),"
            + "ARRAY_SLICE(ARRAY[n],2,1)) ELSE MAP[s,n] END"
      })
  void mapFunctionsAndNullableEmptyContainersUseTheReleasedGenerator(String operand)
      throws Exception {
    if (operand.contains("MAP_UNION"))
      tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction("MAP_UNION");
    check("SELECT id,mk,mv FROM src LEFT JOIN UNNEST(" + operand + ") AS u(mk,mv) ON TRUE", false);
  }

  @Test
  void emptyProjectionRetainsExpansionCardinality() throws Exception {
    check("SELECT COUNT(*) FROM src CROSS JOIN UNNEST(MAP[s,n,'x',id]) AS u(mk,mv)", false);
  }

  @Test
  void nestedGeneratedExpansionsPreserveTheIntermediateArray() throws Exception {
    check(
        "SELECT id,e FROM src CROSS JOIN UNNEST(MAP[s,ARRAY[n,id]]) AS u(mk,mv)"
            + " CROSS JOIN UNNEST(mv) AS v(e)",
        false);
  }

  @ParameterizedTest
  @ValueSource(strings = {"ARRAY[mv]", "ARRAY[ROW(n)]"})
  void nestedRowsCrossingPreservingCalcKeepArrayEquality(String array) throws Exception {
    tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction("ARRAY_APPEND");
    check(
        "SELECT id,e FROM (SELECT id,ELEMENT(ARRAY[mv]) AS r,"
            + array
            + " AS a2 FROM src CROSS JOIN UNNEST(MAP['x',ROW(n)]) AS u(mk,mv)) t"
            + " CROSS JOIN UNNEST(ARRAY_DISTINCT(ARRAY_APPEND(a2,r))) AS v(e)",
        false);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void repeatedProjectionAndPushedWhereUseTheGeneratedExpansion(boolean updating) throws Exception {
    check(
        "SELECT mv,mk,mv,id FROM src CROSS JOIN UNNEST(MAP[s,n,'x',id]) AS u(mk,mv)"
            + " WHERE mv > 1",
        updating);
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "SELECT id,e FROM src LEFT JOIN UNNEST(a) AS u(e) ON e > 10",
        "SELECT id,e FROM src LEFT JOIN UNNEST(a) AS u(e) ON e > 1000",
        "SELECT id,e FROM src LEFT JOIN UNNEST(a) AS u(e) ON e IS NULL",
        "SELECT id,mk,mv FROM src LEFT JOIN UNNEST(MAP[s,n,'x',id]) AS u(mk,mv) ON mv > 10"
      })
  void leftPredicatesRetainMatchingAndNullPadding(String sql) throws Exception {
    check(sql, false);
  }

  @Test
  void correlatedOuterPredicateRetainsTheReleasedPlanningError() {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var result =
          NativeFailureParity.run(
              () -> environment(false, metrics),
              "SELECT e FROM src LEFT JOIN UNNEST(a) AS u(e) ON e > id");
      result.assertFailure(
          org.apache.flink.table.api.TableException.class,
          "unexpected correlate variable",
          NativeFailureParity.Phase.PLANNING,
          NativeFailureParity.Route.UNPLANNED);
    }
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "SELECT id,e FROM src CROSS JOIN UNNEST(ARRAY[s,'x']) AS u(e)",
        "SELECT id,t,v FROM src CROSS JOIN UNNEST(ARRAY[ROW(s,n),ROW(s,id)]) AS u(t,v)",
        "SELECT id,e FROM src CROSS JOIN UNNEST(ARRAY[MAP[s,n],MAP['x',id]]) AS u(e)",
        "SELECT id,t,v FROM src CROSS JOIN UNNEST(ARRAY[CASE WHEN id=1 THEN CAST(NULL AS ROW<t"
            + " STRING,v BIGINT>) ELSE ROW(s,n) END,ROW(s,id)]) AS u(t,v)"
      })
  void computedArraysRetainStringAndNestedRepresentations(String sql) throws Exception {
    check(sql, false);
  }

  @ParameterizedTest
  @ValueSource(strings = {"hashmap", "tech.streamfusion.state.RocksDBNativeStateBackendFactory"})
  void generatedMapExpansionFeedsStateAcrossTwoRestores(String backend) throws Exception {
    var rows = new java.util.ArrayList<Row>();
    for (int i = 0; i < 24; i++) {
      rows.add(Row.ofKind(RowKind.INSERT, (long) i, (long) i, "k" + (i % 3)));
      rows.add(Row.ofKind(RowKind.UPDATE_BEFORE, (long) i, (long) i, "k" + (i % 3)));
      rows.add(Row.ofKind(RowKind.UPDATE_AFTER, (long) i, (long) i + 100, "k" + (i % 3)));
      rows.add(Row.ofKind(RowKind.DELETE, (long) i, (long) i + 100, "k" + (i % 3)));
    }
    try (var recovery =
        new PortableSqlRecovery(
                backend,
                rows,
                Types.ROW_NAMED(
                    new String[] {"id", "n", "s"}, Types.LONG, Types.LONG, Types.STRING),
                Schema.newBuilder().build(),
                true,
                24,
                72)
            .withExecutionMetrics()) {
      java.util.function.UnaryOperator<TableEnvironment> configure =
          table -> {
            table.getConfig().set("table.optimizer.agg-phase-strategy", "ONE_PHASE");
            table.getConfig().set("table.exec.mini-batch.enabled", "false");
            table.getConfig().set("streamfusion.transpose.batchRows", "7");
            return table;
          };
      var result =
          NativeFailureParity.runRecovery(
              () -> configure.apply(recovery.uninterrupted()),
              () -> configure.apply(recovery.get()),
              "SELECT mk,COUNT(*),SUM(mv) FROM recovery_input CROSS JOIN"
                  + " UNNEST(MAP[s,n,'constant',id]) AS u(mk,mv) GROUP BY mk");
      result.assertSuccess(NativeFailureParity.Route.NATIVE);
      recovery.verifyRepeatedRecovery();
      RecoveryExecutionMetrics.assertConsumed(recovery.nativeExecution(), "NativeGeneratedUnnest");
      RecoveryExecutionMetrics.assertConsumed(
          recovery.nativeExecution(), "NativeColumnarGroupAggregate");
    }
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "streamfusion.experimental.collectionExpressions",
        "streamfusion.operator.unnest.enabled"
      })
  void disabledCollectionOrUnnestSwitchRetainsTheHost(String option) {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var result =
          NativeFailureParity.run(
              () -> {
                var table = environment(false, metrics);
                table.getConfig().set(option, "false");
                return table;
              },
              "SELECT id,mk,mv FROM src CROSS JOIN UNNEST(MAP['x',id,'y',n]) AS u(mk,mv)");
      result.assertSuccess(NativeFailureParity.Route.FALLBACK);
      assertTrue(
          result.nativeRun().fallbackReasons().stream()
              .anyMatch(
                  reason ->
                      reason.contains(
                          option.equals("streamfusion.operator.unnest.enabled")
                              ? "unnest: disabled by config"
                              : "streamfusion.experimental.collectionExpressions")),
          result.nativeRun().fallbackReasons().toString());
    }
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "SELECT id,mk,mv FROM marked CROSS JOIN UNNEST(MAP[s,id,'x',id+1]) AS u(mk,mv)",
        "SELECT id,DATE_FORMAT(e,'SSSSSSSSS') FROM marked CROSS JOIN UNNEST(ARRAY_APPEND(a,a[1]))"
            + " AS u(e)"
      })
  void watermarkBatchesPreserveGeneratedContainerInputs(String sql) {
    if (sql.contains("ARRAY_APPEND"))
      tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction("ARRAY_APPEND");
    try (var metrics = new RecoveryExecutionMetrics()) {
      var result =
          NativeFailureParity.run(
              () -> {
                var env = metrics.environment(new Configuration(), 1, 0);
                env.getConfig().enableObjectReuse();
                var table = StreamTableEnvironment.create(env);
                var type =
                    org.apache.flink.table.api.DataTypes.ROW(
                        org.apache.flink.table.api.DataTypes.FIELD(
                            "id", org.apache.flink.table.api.DataTypes.BIGINT()),
                        org.apache.flink.table.api.DataTypes.FIELD(
                            "s", org.apache.flink.table.api.DataTypes.STRING()),
                        org.apache.flink.table.api.DataTypes.FIELD(
                            "rt", org.apache.flink.table.api.DataTypes.TIMESTAMP(3)),
                        org.apache.flink.table.api.DataTypes.FIELD(
                            "a",
                            org.apache.flink.table.api.DataTypes.ARRAY(
                                org.apache.flink.table.api.DataTypes.TIMESTAMP(3))));
                var source =
                    env.fromSequence(0, 6)
                        .map(
                            i ->
                                (org.apache.flink.table.data.RowData)
                                    org.apache.flink.table.data.GenericRowData.of(
                                        i,
                                        org.apache.flink.table.data.StringData.fromString(
                                            i % 2 == 0 ? "\ud800" : "\ue000"),
                                        org.apache.flink.table.data.TimestampData.fromEpochMillis(
                                            i * 1000),
                                        new org.apache.flink.table.data.GenericArrayData(
                                            new Object[] {
                                              org.apache.flink.table.data.TimestampData
                                                  .fromEpochMillis(1, 987654),
                                              org.apache.flink.table.data.TimestampData
                                                  .fromEpochMillis(-1, 543210)
                                            })))
                        .returns(
                            org.apache.flink.table.runtime.typeutils.InternalTypeInfo.of(
                                (org.apache.flink.table.types.logical.RowType)
                                    type.getLogicalType()));
                table.createTemporaryView(
                    "marked",
                    source,
                    Schema.newBuilder()
                        .fromRowDataType(type)
                        .watermark("rt", "rt - INTERVAL '1' SECOND")
                        .build());
                table.getConfig().set("streamfusion.transpose.batchRows", "3");
                return table;
              },
              sql);
      result.assertSuccess(NativeFailureParity.Route.NATIVE);
      RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeGeneratedUnnest");
      RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeWatermarkAssigner");
      if (sql.contains("ARRAY_APPEND"))
        assertTrue(
            result.host().rows().toString().contains("987654"), result.host().rows().toString());
    }
  }

  private static void check(String sql, boolean updating) throws Exception {
    check(sql, updating, "NativeGeneratedUnnest");
  }

  private static void check(String sql, boolean updating, String expectedOperator) throws Exception {
    try (var metrics = new RecoveryExecutionMetrics()) {
      if (expectedOperator.equals("NativeUnnest")) {
        var table = environment(updating, metrics);
        var scan = tech.streamfusion.planner.NativePlanner.install(table);
        String plan = table.explainSql(sql);
        assertFalse(plan.contains("NativeGeneratedUnnest"), plan);
        assertTrue(
            scan.functionExecutions().stream()
                .anyMatch(
                    execution ->
                        execution.contains("NativeUnnestOperand")
                            && execution.contains("native=[MAP]")
                            && execution.contains("jvm=[]")),
            plan + scan.functionExecutions());
      }
      var result = NativeFailureParity.run(() -> environment(updating, metrics), sql);
      if (org.apache.flink.runtime.util.EnvironmentInformation.getVersion().startsWith("1.18.")) {
        if (sql.contains("CAST(id AS TINYINT)")) {
          result.assertFailure(
              org.codehaus.commons.compiler.CompileException.class,
              "BinaryArrayWriter.writeByte",
              NativeFailureParity.Phase.INITIALIZATION,
              NativeFailureParity.Route.NATIVE);
          return;
        }
        if (sql.contains("MAP[id,ROW(") || sql.contains("MAP[ROW(")) {
          result.assertFailure(
              IllegalArgumentException.class,
              "Type mismatch",
              NativeFailureParity.Phase.PLANNING,
              NativeFailureParity.Route.UNPLANNED);
          return;
        }
        if (sql.contains("MAP[id,MAP[") || sql.contains("ARRAY[MAP[")) {
          result.assertFailure(
              UnsupportedOperationException.class,
              "Unsupported type when convertTypeToSpec: MAP",
              NativeFailureParity.Phase.PLANNING,
              NativeFailureParity.Route.UNPLANNED);
          return;
        }
        if (sql.contains(" LEFT JOIN ") && !sql.endsWith(" ON TRUE")) {
          result.assertFailure(
              org.apache.calcite.plan.RelOptPlanner.CannotPlanException.class,
              NativeFailureParity.Phase.PLANNING,
              NativeFailureParity.Route.UNPLANNED);
          return;
        }
      }
      result.assertSuccess(NativeFailureParity.Route.NATIVE);
      assertEquals(result.host().resultTypes(), result.nativeRun().resultTypes());
      if (sql.contains("ORDINALITY") || expectedOperator.equals("NativeUnnest"))
        assertEquals(result.host().rows(), result.nativeRun().rows());
      assertTrue(
          result.nativeRun().plan().contains(expectedOperator), result.nativeRun().plan());
      RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), expectedOperator);
      if (expectedOperator.equals("NativeUnnest"))
        RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeCalc");
    }
  }

  private static TableEnvironment environment(boolean updating, RecoveryExecutionMetrics metrics) {
    var env = metrics.environment(new Configuration(), 1, 0);
    env.getConfig().enableObjectReuse();
    var table = StreamTableEnvironment.create(env);
    table.getConfig().set("streamfusion.transpose.batchRows", "2");
    table.getConfig().set("table.local-time-zone", "America/New_York");
    var source =
        fromData(
            env,
            Types.ROW_NAMED(
                new String[] {"id", "n", "s", "a"},
                Types.LONG,
                Types.LONG,
                Types.STRING,
                Types.OBJECT_ARRAY(Types.LONG)),
            Row.of(1L, 4L, "z", new Long[] {1L, 20L, null}),
            Row.of(2L, null, "Aa", new Long[0]),
            Row.of(3L, 3L, "BB", null),
            Row.of(4L, 1L, null, new Long[] {30L, 40L}),
            Row.ofKind(
                updating ? RowKind.UPDATE_BEFORE : RowKind.INSERT,
                1L,
                4L,
                "z",
                new Long[] {1L, 20L, null}),
            Row.ofKind(
                updating ? RowKind.UPDATE_AFTER : RowKind.INSERT, 1L, 6L, "a", new Long[] {50L}),
            Row.ofKind(updating ? RowKind.DELETE : RowKind.INSERT, 2L, null, "Aa", new Long[0]));
    table.createTemporaryView(
        "src",
        updating
            ? table.fromChangelogStream(source, Schema.newBuilder().build(), ChangelogMode.all())
            : table.fromDataStream(source));
    return table;
  }
}
