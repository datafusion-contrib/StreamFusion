package tech.streamfusion;

import static org.apache.flink.table.api.DataTypes.*;
import static org.junit.jupiter.api.Assertions.*;
import static tech.streamfusion.compat.FlinkTestSources.fromData;

import java.math.BigDecimal;
import java.util.List;
import java.util.stream.Stream;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.runtime.typeutils.ExternalTypeInfo;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.planner.NativePlanner;

class FlinkArrayCastSqlHarnessTest {
  private static final RecoveryExecutionMetrics METRICS = new RecoveryExecutionMetrics();

  @AfterAll
  static void closeCluster() {
    METRICS.close();
  }

  static Stream<Arguments> exactCasts() {
    return Stream.of(
        Arguments.of("a", "ARRAY<BIGINT>"),
        Arguments.of("a", "ARRAY<TINYINT>"),
        Arguments.of("a", "ARRAY<SMALLINT>"),
        Arguments.of("a", "ARRAY<DECIMAL(10,2)>"),
        Arguments.of("a", "ARRAY<DECIMAL(38,18)>"),
        Arguments.of("CAST(a AS ARRAY<TINYINT>)", "ARRAY<BIGINT>"),
        Arguments.of("CAST(a AS ARRAY<TINYINT>)", "ARRAY<DECIMAL(5,2)>"),
        Arguments.of("CAST(a AS ARRAY<SMALLINT>)", "ARRAY<BIGINT>"),
        Arguments.of("CAST(a AS ARRAY<SMALLINT>)", "ARRAY<DECIMAL(5,2)>"),
        Arguments.of("b", "ARRAY<INT>"),
        Arguments.of("b", "ARRAY<TINYINT>"),
        Arguments.of("b", "ARRAY<DECIMAL(5,2)>"),
        Arguments.of("b", "ARRAY<DECIMAL(38,18)>"),
        Arguments.of("d", "ARRAY<BIGINT>"),
        Arguments.of("d", "ARRAY<INT>"),
        Arguments.of("d", "ARRAY<TINYINT>"),
        Arguments.of("d", "ARRAY<DECIMAL(10,1)>"),
        Arguments.of("d", "ARRAY<DECIMAL(38,20)>"),
        Arguments.of("q", "ARRAY<DECIMAL(3,1)>"),
        Arguments.of("q", "ARRAY<DECIMAL(10,4)>"),
        Arguments.of("nested", "ARRAY<ARRAY<BIGINT>>"),
        Arguments.of("nested", "ARRAY<ARRAY<DECIMAL(3,1)>>"));
  }

  @ParameterizedTest
  @MethodSource("exactCasts")
  void standaloneExactCastsUseRust(String column, String target) {
    for (boolean legacy : new boolean[] {false, true})
      assertNative(legacy, "SELECT id,CAST(" + column + " AS " + target + ") FROM src");
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "SELECT CAST(a AS ARRAY<BIGINT>) FROM src WHERE id<0",
        "SELECT CAST(a AS ARRAY<DECIMAL(3,1)>) FROM src WHERE id=2",
        "SELECT ARRAY_REVERSE(CAST(a AS ARRAY<BIGINT>)) FROM src",
        "SELECT CAST(CASE WHEN id=0 THEN a ELSE b END AS ARRAY<INT>) FROM src",
        "SELECT CAST(CAST(NULL AS ARRAY<INT>) AS ARRAY<BIGINT>) FROM src"
      })
  void filteredNestedAndScalarOperandsRetainTheirSchema(String sql) {
    assertNative(false, sql);
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "SELECT CAST(t AS ARRAY<INT>) FROM src",
        "SELECT CAST(a AS ARRAY<INT NOT NULL>) FROM src"
      })
  void generatedConversionsRetainReleasedEvaluation(String sql) {
    var runs = NativeFailureParity.run(() -> environment(false), sql);
    if (runs.host().failure() == null) runs.assertSuccess(NativeFailureParity.Route.NATIVE);
    else
      runs.assertFailure(
          runs.host().rootCause().getClass(),
          runs.host().phase(),
          NativeFailureParity.Route.NATIVE);
  }

  @ParameterizedTest
  @ValueSource(strings = {"", " WHERE id>0", " WHERE id<0"})
  void fallibleArrayConstructionRetainsRowEvaluation(String filter) {
    var runs =
        NativeFailureParity.run(
            () -> environment(false),
            "SELECT CAST(ARRAY[10/id] AS ARRAY<BIGINT>) FROM src" + filter);
    if (filter.isEmpty()) {
      runs.assertFailure(
          ArithmeticException.class, runs.host().phase(), NativeFailureParity.Route.NATIVE);
    } else {
      runs.assertSuccess(NativeFailureParity.Route.NATIVE);
      if (filter.contains("<0")) assertEquals(List.of(), runs.nativeRun().rows());
    }
  }

  @Nested
  class TryCasts {
    @ParameterizedTest
    @MethodSource("tech.streamfusion.FlinkArrayCastSqlHarnessTest#exactCasts")
    void exactNumericTryCastsUseRust(String column, String target) {
      for (boolean legacy : new boolean[] {false, true}) {
        assertNative(legacy, "SELECT id,TRY_CAST(" + column + " AS " + target + ") FROM src");
      }
    }

    @ParameterizedTest
    @ValueSource(
        strings = {
          "SELECT TRY_CAST(a AS ARRAY<BIGINT>) FROM src WHERE id<0",
          "SELECT TRY_CAST(a AS ARRAY<DECIMAL(3,1)>) FROM src WHERE id=2",
          "SELECT ARRAY_REVERSE(TRY_CAST(a AS ARRAY<BIGINT>)) FROM src",
          "SELECT TRY_CAST(CAST(NULL AS ARRAY<INT>) AS ARRAY<BIGINT>) FROM src"
        })
    void filteredAndScalarTryCastsRetainNullContainers(String sql) {
      assertNative(false, sql);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void invalidStringElementNullsTheWholeArray(boolean legacy) {
      var runs =
          NativeFailureParity.run(
              () -> environment(legacy), "SELECT id,TRY_CAST(t AS ARRAY<INT>) FROM src");
      runs.assertSuccess(NativeFailureParity.Route.NATIVE);
      assertTrue(runs.host().rows().contains(java.util.Arrays.asList("+I", 0, null)), runs.toString());
      assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
    }

    @ParameterizedTest
    @ValueSource(strings = {"", " WHERE id>0", " WHERE id<0"})
    void tryCastDoesNotCatchInputEvaluationErrors(String filter) {
      var runs =
          NativeFailureParity.run(
              () -> environment(false),
              "SELECT TRY_CAST(ARRAY[10/id] AS ARRAY<BIGINT>) FROM src" + filter);
      if (filter.isEmpty()) {
        runs.assertFailure(
            ArithmeticException.class, runs.host().phase(), NativeFailureParity.Route.NATIVE);
      } else {
        runs.assertSuccess(NativeFailureParity.Route.NATIVE);
        if (filter.contains("<0")) assertEquals(List.of(), runs.nativeRun().rows());
      }
    }
  }

  static void assertNative(boolean legacy, String sql) {
    var earlier =
        METRICS.nativeOperators().stream()
            .map(row -> row.get("jobId"))
            .collect(java.util.stream.Collectors.toSet());
    var runs = NativeFailureParity.run(() -> environment(legacy), sql);
    runs.assertSuccess(NativeFailureParity.Route.NATIVE);
    assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
    var work =
        METRICS.nativeOperators().stream()
            .filter(row -> !earlier.contains(row.get("jobId")))
            .collect(java.util.stream.Collectors.toList());
    RecoveryExecutionMetrics.assertConsumed(work, "NativeCalc");
    var table = environment(legacy);
    var scan = NativePlanner.install(table);
    table.explainSql(sql);
    assertTrue(
        scan.functionExecutions().stream().allMatch(row -> row.contains("jvm=[]")),
        scan.functionExecutions().toString());
  }

  static TableEnvironment environment(boolean legacy) {
    var env = METRICS.environment(new Configuration(), 1, 0);
    env.getConfig().enableObjectReuse();
    var table = StreamTableEnvironment.create(env);
    table.getConfig().set("table.exec.legacy-cast-behaviour", legacy ? "ENABLED" : "DISABLED");
    table.getConfig().set("streamfusion.transpose.batchRows", "3");
    var type =
        ROW(
            FIELD("id", INT()),
            FIELD("a", ARRAY(INT())),
            FIELD("b", ARRAY(BIGINT())),
            FIELD("d", ARRAY(DECIMAL(38, 18))),
            FIELD("q", ARRAY(DECIMAL(5, 2))),
            FIELD("nested", ARRAY(ARRAY(INT()))),
            FIELD("t", ARRAY(STRING())));
    var rows =
        List.of(
            Row.of(
                0,
                new Integer[] {Integer.MIN_VALUE, 128, null, -129, Integer.MAX_VALUE},
                new Long[] {Long.MIN_VALUE, Long.MAX_VALUE, null, 256L, -257L},
                new BigDecimal[] {
                  new BigDecimal("99999999999999999999.999999999999999999"),
                  new BigDecimal("1.25"),
                  null,
                  new BigDecimal("-2.55")
                },
                new BigDecimal[] {
                  new BigDecimal("999.99"),
                  new BigDecimal("-999.99"),
                  new BigDecimal("1.25"),
                  new BigDecimal("-2.55"),
                  null
                },
                new Integer[][] {new Integer[] {1, null, -2}, null, new Integer[] {}},
                new String[] {"1", null, "bad"}),
            Row.of(
                1,
                new Integer[] {},
                new Long[] {},
                new BigDecimal[] {},
                new BigDecimal[] {},
                new Integer[][] {},
                new String[] {}),
            Row.of(2, null, null, null, null, null, null));
    table.createTemporaryView(
        "src",
        fromData(env, rows, ExternalTypeInfo.<Row>of(type)),
        Schema.newBuilder().fromRowDataType(type).build());
    return table;
  }
}
