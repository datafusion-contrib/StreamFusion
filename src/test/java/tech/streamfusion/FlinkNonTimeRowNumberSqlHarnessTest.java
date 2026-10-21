package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;
import static tech.streamfusion.compat.FlinkTestSources.fromData;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

class FlinkNonTimeRowNumberSqlHarnessTest {
  static final String SQL =
      "SELECT k, v, id, ROW_NUMBER() OVER (PARTITION BY k ORDER BY v ASC NULLS FIRST) AS rn FROM"
          + " src";

  @Test
  void releasedHostUsesRowIdentityUpdatesForNonmonotonicInput() throws Exception {
    org.junit.jupiter.api.Assumptions.assumeTrue(
        tech.streamfusion.compat.FlinkTestCapabilities.NON_TIME_OVER);
    var table = environment();
    assertTrue(table.explainSql(SQL).contains("ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"));
    assertEquals(
        DataTypes.BIGINT().notNull(),
        table.sqlQuery(SQL).getResolvedSchema().getColumnDataTypes().get(3));
    List<String> rows = new ArrayList<>();
    try (var input = table.executeSql(SQL).collect()) {
      while (input.hasNext()) rows.add(input.next().toString());
    }
    assertEquals(28, rows.size());
    assertEquals(
        List.of(
            "+I[1, 30, 0, 1]",
            "+I[1, 10, 1, 1]",
            "-U[1, 30, 0, 1]",
            "+U[1, 30, 0, 2]",
            "+I[1, 10, 2, 2]",
            "-U[1, 30, 0, 2]",
            "+U[1, 30, 0, 3]"),
        rows.subList(0, 7));
    assertEquals("+U[1, 30, 0, 6]", rows.get(rows.size() - 1));
    NativeParity.assertOrderedKindedParity(FlinkNonTimeRowNumberSqlHarnessTest::environment, SQL);
  }

  @org.junit.jupiter.params.ParameterizedTest
  @org.junit.jupiter.params.provider.CsvSource({
    "1,false,false",
    "3,false,false",
    "1024,false,false",
    "3,true,false",
    "3,false,true"
  })
  void tiesNullsAndInputPermutationsKeepEveryChangelog(
      int batchRows, boolean miniBatch, boolean reverse) throws Exception {
    org.junit.jupiter.api.Assumptions.assumeTrue(
        tech.streamfusion.compat.FlinkTestCapabilities.NON_TIME_OVER);
    for (String order :
        new String[] {"v ASC NULLS FIRST", "v ASC NULLS LAST", "CAST(v AS INT) ASC NULLS FIRST"}) {
      String sql = SQL.replace("v ASC NULLS FIRST", order);
      java.util.function.Supplier<TableEnvironment> setup =
          () -> {
            var table = environment(reverse);
            table.getConfig().set("streamfusion.transpose.batchRows", Integer.toString(batchRows));
            if (miniBatch) {
              table.getConfig().set("table.exec.mini-batch.enabled", "true");
              table.getConfig().set("table.exec.mini-batch.allow-latency", "10 ms");
              table.getConfig().set("table.exec.mini-batch.size", "3");
            }
            return table;
          };
      assertTrue(
          tech.streamfusion.planner.NativePlanner.explain(setup.get(), sql)
              .contains("NativeColumnarTopN"));
      NativeParity.assertOrderedKindedParity(setup, sql);
    }
  }

  @Test
  void emptyAndGlobalRanksUseTheSameAdmittedContract() throws Exception {
    org.junit.jupiter.api.Assumptions.assumeTrue(
        tech.streamfusion.compat.FlinkTestCapabilities.NON_TIME_OVER);
    NativeParity.assertOrderedKindedParity(
        FlinkNonTimeRowNumberSqlHarnessTest::environment, SQL + " WHERE id < 0");
    NativeParity.assertOrderedKindedParity(
        FlinkNonTimeRowNumberSqlHarnessTest::environment, SQL.replace("PARTITION BY k ", ""));
  }

  @Test
  void partitionedShuffleKeepsRanksAtParallelismThree() throws Exception {
    org.junit.jupiter.api.Assumptions.assumeTrue(
        tech.streamfusion.compat.FlinkTestCapabilities.NON_TIME_OVER);
    NativeParity.assertChangelogParity(() -> environment(false, false, 3), SQL);
  }

  @Test
  void exactIntegerExtremesKeepTheirOrderWithoutFloatingPointConversion() throws Exception {
    org.junit.jupiter.api.Assumptions.assumeTrue(
        tech.streamfusion.compat.FlinkTestCapabilities.NON_TIME_OVER);
    for (String type : new String[] {"INT", "BIGINT"}) {
      String low = type.equals("INT") ? "-2147483648" : "-9223372036854775808";
      String high = type.equals("INT") ? "2147483647" : "9223372036854775807";
      String input =
          "(SELECT k, CASE WHEN id = 0 THEN CAST('"
              + low
              + "' AS "
              + type
              + ") WHEN id = 1 THEN CAST('"
              + high
              + "' AS "
              + type
              + ") ELSE CAST(v AS "
              + type
              + ") END AS v, id FROM src)";
      NativeParity.assertOrderedKindedParity(
          FlinkNonTimeRowNumberSqlHarnessTest::environment,
          SQL.replace("FROM src", "FROM " + input));
    }
  }

  @Test
  void updatingSourceKeepsTheReleasedHostOperator() throws Exception {
    org.junit.jupiter.api.Assumptions.assumeTrue(
        tech.streamfusion.compat.FlinkTestCapabilities.NON_TIME_OVER);
    NativeParity.assertFallbackReasonContains(
        () -> environment(false, true, 1),
        SQL,
        "updating inputs require a separate retained-row contract");
  }

  @Test
  void filteredTopNControlKeepsItsExistingRankSlotContract() throws Exception {
    NativeParity.assertOrderedKindedParity(
        FlinkNonTimeRowNumberSqlHarnessTest::environment,
        "SELECT * FROM (" + SQL + ") WHERE rn <= 2");
  }

  @Test
  void otherContractsKeepTheirHostValidationAndFallback() throws Exception {
    if (!tech.streamfusion.compat.FlinkTestCapabilities.NON_TIME_OVER) {
      var failure = assertThrows(Exception.class, () -> environment().executeSql(SQL));
      assertTrue(failure.toString().contains("time attribute"), failure.toString());
      return;
    }
    NativeParity.assertFallbackReasonContains(
        () -> {
          var table = environment();
          table.getConfig().setIdleStateRetention(java.time.Duration.ofSeconds(10));
          return table;
        },
        SQL,
        "requires disabled idle-state TTL");
    NativeParity.assertFallbackReasonContains(
        FlinkNonTimeRowNumberSqlHarnessTest::environment,
        SQL.replace("v ASC NULLS FIRST", "ARRAY[CAST(v AS DOUBLE)] ASC NULLS FIRST"),
        "ascending supported order column");
  }

  @org.junit.jupiter.params.ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(
      strings = {"hashmap", "tech.streamfusion.state.RocksDBNativeStateBackendFactory"})
  void twoRestoresRetainAllRankedRowsAndTheirTieOrder(String backend) throws Exception {
    org.junit.jupiter.api.Assumptions.assumeTrue(
        tech.streamfusion.compat.FlinkTestCapabilities.NON_TIME_OVER);
    var rows =
        java.util.stream.IntStream.range(0, 96)
            .mapToObj(
                i -> Row.of(i % 11 == 0 ? null : i % 3, i % 7 == 0 ? null : (long) (96 - i) % 13))
            .collect(tech.streamfusion.compat.ListCollectors.toList());
    try (var recovery =
        new PortableSqlRecovery(
                backend,
                rows,
                Types.ROW_NAMED(new String[] {"k", "v"}, Types.INT, Types.LONG),
                Schema.newBuilder()
                    .column("k", DataTypes.INT())
                    .column("v", DataTypes.BIGINT())
                    .build(),
                false,
                24,
                63)
            .withExecutionMetrics()) {
      var comparison =
          NativeFailureParity.runRecovery(
              recovery::uninterrupted,
              recovery,
              "SELECT k, v, ROW_NUMBER() OVER (PARTITION BY k ORDER BY v ASC NULLS FIRST) AS rn"
                  + " FROM recovery_input");
      comparison.assertSuccess(NativeFailureParity.Route.NATIVE);
      recovery.verifyRepeatedRecovery();
      RecoveryExecutionMetrics.assertConsumed(recovery.nativeExecution(), "NativeColumnarTopN");
      SharedFlinkCluster.assertNativeMemoryReleased("non-time-row-number-recovery");
    }
  }

  @Test
  void olderHostKeepsItsValidationWithNativePlanning() {
    org.junit.jupiter.api.Assumptions.assumeFalse(
        tech.streamfusion.compat.FlinkTestCapabilities.NON_TIME_OVER);
    var result = NativeFailureParity.run(FlinkNonTimeRowNumberSqlHarnessTest::environment, SQL);
    assertNotNull(result.host().failure());
    assertNotNull(result.nativeRun().failure());
    assertEquals(result.host().rootCause().getClass(), result.nativeRun().rootCause().getClass());
    assertEquals(result.host().rootCause().getMessage(), result.nativeRun().rootCause().getMessage());
    assertEquals(result.host().phase(), result.nativeRun().phase());
  }

  static TableEnvironment environment() {
    return environment(false);
  }

  static TableEnvironment environment(boolean reverse) {
    return environment(reverse, false, 1);
  }

  private static TableEnvironment environment(boolean reverse, boolean changelog, int parallelism) {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(parallelism);
    var table = StreamTableEnvironment.create(env);
    var rows =
        List.of(
            Row.of(1L, 30L, 0L),
            Row.of(1L, 10L, 1L),
            Row.of(1L, 10L, 2L),
            Row.of(null, null, 3L),
            Row.of(1L, null, 4L),
            Row.of(null, -1L, 5L),
            Row.of(1L, 20L, 6L),
            Row.of(1L, -5L, 7L));
    if (changelog) {
      rows = new ArrayList<>(rows);
      rows.add(Row.ofKind(org.apache.flink.types.RowKind.UPDATE_BEFORE, 1L, 30L, 0L));
      rows.add(Row.ofKind(org.apache.flink.types.RowKind.UPDATE_AFTER, 1L, 15L, 0L));
    }
    if (reverse) {
      rows = new ArrayList<>(rows);
      java.util.Collections.reverse(rows);
    }
    var source =
        fromData(
            env,
            rows,
            Types.ROW_NAMED(new String[] {"k", "v", "id"}, Types.LONG, Types.LONG, Types.LONG));
    source.setParallelism(1);
    var schema =
        Schema.newBuilder()
            .column("k", DataTypes.BIGINT())
            .column("v", DataTypes.BIGINT())
            .column("id", DataTypes.BIGINT())
            .build();
    table.createTemporaryView(
        "src",
        changelog
            ? table.fromChangelogStream(source, schema)
            : table.fromDataStream(source, schema));
    return table;
  }
}
