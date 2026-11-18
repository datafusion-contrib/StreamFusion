package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;
import static tech.streamfusion.compat.FlinkTestSources.fromData;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

/** Mixed append/update UNION must retain physical sidecars before the downstream aggregate. */
class FlinkMixedUnionSqlHarnessTest {
  @ParameterizedTest
  @ValueSource(ints = {1, 3})
  void multiBranchRollupRetainsNullGroupsAndDuplicateKeys(int parallelism) {
    String sql =
        "SELECT k, SUM(v) AS v FROM (SELECT k, v FROM src UNION ALL "
            + "SELECT k, SUM(v) AS v FROM src GROUP BY k UNION ALL "
            + "SELECT k, SUM(v) AS v FROM src GROUP BY ROLLUP(k)) u GROUP BY ROLLUP(k)";
    Supplier<TableEnvironment> environment = () -> environment(3, parallelism);
    var comparison = NativeFailureParity.runRecovery(environment, environment, sql);
    Map<List<Object>, Long> expected = new java.util.HashMap<>();
    expected.put(List.of("a", new BigDecimal("9.00")), 1L);
    expected.put(List.of("b", new BigDecimal("21.00")), 1L);
    expected.put(java.util.Arrays.asList(null, new BigDecimal("10.00")), 1L);
    expected.put(java.util.Arrays.asList(null, new BigDecimal("40.00")), 1L);
    assertResult(comparison, expected);
  }

  @ParameterizedTest(name = "recovery-rocks={0},reverse={1},parallelism={2},zeroCopy={3}")
  @CsvSource({"false,false,1,true", "false,true,3,false", "true,false,3,true", "true,true,1,false"})
  void mixedBranchesRestoreAcrossCheckpointsAndWatermarks(
      boolean rocks, boolean reverse, int parallelism, boolean zeroCopy) throws Exception {
    String previous =
        System.setProperty("streamfusion.exchange.zeroCopyLocal", Boolean.toString(zeroCopy));
    List<Row> input = new java.util.ArrayList<>();
    for (int i = 0; i < 32; i++)
      input.add(Row.of(i % 4 < 2 ? "a" : "b", BigDecimal.valueOf(i % 4 + 1, 0).setScale(2)));
    String backend = rocks ? "tech.streamfusion.state.RocksDBNativeStateBackendFactory" : "hashmap";
    try (var recovery =
        new PortableSqlRecovery(
                backend,
                input,
                Types.ROW_NAMED(new String[] {"k", "v"}, Types.STRING, Types.BIG_DEC),
                Schema.newBuilder()
                    .column("k", DataTypes.VARCHAR(1).notNull())
                    .column("v", DataTypes.DECIMAL(7, 2).notNull())
                    .build(),
                false,
                8,
                24)
            .withRescaleDeployment(parallelism, null, null)
            .withWatermarks(Map.of(8, 1000L, 24, 2000L))
            .withExecutionMetrics()) {
      java.util.function.UnaryOperator<TableEnvironment> configure =
          table -> {
            table.getConfig().set("streamfusion.transpose.batchRows", "3");
            return table;
          };
      var comparison =
          NativeFailureParity.runRecovery(
              () -> configure.apply(recovery.uninterrupted()),
              () -> configure.apply(recovery.get()),
              query("recovery_input", reverse));
      assertResult(
          comparison,
          Map.of(
              List.of("a", new BigDecimal("48.00")),
              1L,
              List.of("b", new BigDecimal("112.00")),
              1L));
      recovery.verifyRepeatedRecovery();
      RecoveryExecutionMetrics.assertConsumed(
          recovery.nativeExecution(), "NativeColumnarGroupAggregate");
      SharedFlinkCluster.assertNativeMemoryReleased("mixed-union-recovery");
    } finally {
      if (previous == null) System.clearProperty("streamfusion.exchange.zeroCopyLocal");
      else System.setProperty("streamfusion.exchange.zeroCopyLocal", previous);
    }
  }

  @ParameterizedTest(
      name = "reverse={0},sink={1},coalesce={2},latency={3},arrow={4},parallelism={5},zeroCopy={6}")
  @CsvSource({
    "false,false,4096,50,4096,1,true", "true,false,4096,50,4096,1,true",
    "false,true,4096,50,4096,1,true", "true,true,4096,50,4096,1,true",
    "false,false,1,50,4096,1,true", "true,false,1,50,4096,1,true",
    "false,true,1,50,4096,1,true", "true,true,1,50,4096,1,true",
    "false,false,4096,0,4096,1,true", "true,false,4096,0,4096,1,true",
    "false,true,4096,0,4096,1,true", "true,true,4096,0,4096,1,true",
    "false,true,5,50,1,3,true", "true,true,5,50,3,3,true",
    "false,true,5,0,1,3,false", "true,true,5,0,3,3,false"
  })
  void mixedBranchesMatchExactHostValuesAndTypes(
      boolean reverse,
      boolean sink,
      int coalesce,
      int latency,
      int batchRows,
      int parallelism,
      boolean zeroCopy) {
    Map<String, String> settings =
        Map.of(
            "streamfusion.exchange.coalesceRows", Integer.toString(coalesce),
            "streamfusion.exchange.coalesceLatencyMs", Integer.toString(latency),
            "streamfusion.exchange.zeroCopyLocal", Boolean.toString(zeroCopy));
    Map<String, String> previous = new java.util.HashMap<>();
    settings.forEach((key, value) -> previous.put(key, System.setProperty(key, value)));
    try {
      Supplier<TableEnvironment> environment = () -> environment(batchRows, parallelism);
      String sql = query("src", reverse);
      var comparison =
          sink
              ? NativeFailureParity.runRecovery(environment, environment, sql)
              : NativeFailureParity.run(environment, sql);
      assertResult(
          comparison,
          Map.of(
              List.of("a", new BigDecimal("6.00")), 1L,
              List.of("b", new BigDecimal("14.00")), 1L));
    } finally {
      previous.forEach(
          (key, value) -> {
            if (value == null) System.clearProperty(key);
            else System.setProperty(key, value);
          });
    }
  }

  static String query(String source, boolean reverse) {
    String append = "SELECT k, v FROM " + source;
    String update = "SELECT k, SUM(v) AS v FROM " + source + " GROUP BY k";
    return "SELECT k, SUM(v) AS v FROM ("
        + (reverse ? update : append)
        + " UNION ALL "
        + (reverse ? append : update)
        + ") u GROUP BY k";
  }

  static void assertResult(
      NativeFailureParity.Comparison comparison, Map<List<Object>, Long> expected) {
    assertNull(comparison.host().failure(), comparison::toString);
    assertNull(comparison.nativeRun().failure(), comparison::toString);
    assertEquals(
        NativeFailureParity.Route.NATIVE, comparison.nativeRun().route(), comparison::toString);
    assertEquals(
        expected, SqlAuditHarness.materialized(comparison.host().rows()), comparison::toString);
    assertEquals(
        expected,
        SqlAuditHarness.materialized(comparison.nativeRun().rows()),
        comparison::toString);
    assertEquals(comparison.host().resultTypes(), comparison.nativeRun().resultTypes());
    String plan = comparison.nativeRun().plan();
    assertTrue(plan.contains("NativeUnion"), plan);
    assertTrue(plan.contains("NativeColumnarGroupAggregate"), plan);
    assertTrue(plan.contains("RowDataToArrow"), plan);
  }

  private static TableEnvironment environment(int batchRows, int parallelism) {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(parallelism);
    var table = StreamTableEnvironment.create(env);
    table.getConfig().setLocalTimeZone(java.time.ZoneOffset.UTC);
    table.getConfig().set("streamfusion.transpose.batchRows", Integer.toString(batchRows));
    var source =
        fromData(
            env,
            Types.ROW_NAMED(new String[] {"k", "v"}, Types.STRING, Types.BIG_DEC),
            Row.of("a", new BigDecimal("1.00")),
            Row.of("a", new BigDecimal("2.00")),
            Row.of("b", new BigDecimal("3.00")),
            Row.of("b", new BigDecimal("4.00")));
    table.createTemporaryView(
        "src",
        source,
        Schema.newBuilder()
            .column("k", DataTypes.VARCHAR(1).notNull())
            .column("v", DataTypes.DECIMAL(7, 2).notNull())
            .build());
    return table;
  }
}
