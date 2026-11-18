package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static tech.streamfusion.compat.FlinkTestSources.fromData;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.testutils.InMemoryReporter;
import org.apache.flink.runtime.testutils.MiniClusterResource;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.util.TestStreamEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.planner.NativePlanner;

class FlinkChangingTopNSqlHarnessTest {
  @ParameterizedTest
  @ValueSource(
      strings = {"bound", "CAST(MOD(bound, 32767) AS INT)", "CAST(MOD(bound, 32767) AS SMALLINT)"})
  void changingBoundsPreserveOriginalPayloadAndOrderedChangelog(String bound) throws Exception {
    for (boolean retracting : new boolean[] {false, true}) {
      for (boolean rank : new boolean[] {false, true}) {
        String sql = query(bound, rank);
        String plan = NativePlanner.explain(environment(false, retracting), sql);
        assertTrue(plan.contains("NativeColumnarTopN"), plan);
        NativeParity.assertOrderedKindedParity(() -> environment(false, retracting), sql);
        String miniBatchPlan = NativePlanner.explain(environment(true, retracting), sql);
        assertTrue(miniBatchPlan.contains("NativeColumnarTopN"), miniBatchPlan);
        NativeParity.assertOrderedKindedParity(() -> environment(true, retracting), sql);
      }
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"hashmap", "tech.streamfusion.state.RocksDBNativeStateBackendFactory"})
  void firstBoundSurvivesSqlRecoveryIncludingEmptySelectedGroups(String backend) throws Exception {
    for (boolean rank : new boolean[] {false, true}) {
      String sql =
          "SELECT k, v"
              + (rank ? ", rn" : "")
              + " FROM (SELECT k, v, MOD(COALESCE(v, 0), 5) AS rank_end,"
              + " ROW_NUMBER() OVER (PARTITION BY k ORDER BY v DESC) AS rn FROM recovery_input)"
              + " WHERE rn <= rank_end";
      try (var recovery = new PortableSqlRecovery(backend)) {
        NativeParity.assertChangelogParity(recovery, sql);
        recovery.verify();
      }
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void mismatchesAreCountedWhileNativeRowsKeepTheirActualBounds(boolean retracting)
      throws Exception {
    var reporter = InMemoryReporter.createWithRetainedMetrics();
    var cluster =
        new MiniClusterResource(
            new MiniClusterResourceConfiguration.Builder()
                .setConfiguration(reporter.addToConfiguration(new Configuration()))
                .setNumberTaskManagers(1)
                .setNumberSlotsPerTaskManager(2)
                .build());
    cluster.before();
    try {
      var input =
          new ArrayList<>(
              List.of(
                  Row.of(1L, 30L, 2L),
                  Row.of(1L, 20L, 1L),
                  Row.of(1L, 10L, 2L),
                  Row.of(1L, 0L, 3L),
                  Row.of(2L, 10L, 0L),
                  Row.of(2L, 0L, 2L)));
      if (retracting) {
        input.add(Row.ofKind(RowKind.UPDATE_BEFORE, 1L, 0L, 3L));
        input.add(Row.ofKind(RowKind.UPDATE_AFTER, 1L, -1L, 5L));
        input.add(Row.ofKind(RowKind.DELETE, 1L, 20L, 1L));
      }
      var table =
          environment(
              new TestStreamEnvironment(cluster.getMiniCluster(), 1), false, input, retracting);
      var scan = NativePlanner.install(table);
      var result = table.executeSql(query("bound", true));
      List<Row> rows = new ArrayList<>();
      try (var changes = result.collect()) {
        while (changes.hasNext()) rows.add(changes.next());
      }
      assertTrue(scan.fallbackReasons().isEmpty(), scan.explainSummary());
      assertTrue(
          rows.stream().anyMatch(row -> Long.valueOf(3).equals(row.getField(2))),
          "the row's proposed bound must remain in its payload");
      var groups =
          reporter.findOperatorMetricGroups(
              result.getJobClient().orElseThrow().getJobID(), "(?i)NativeColumnarTopN");
      assertEquals(1, groups.size());
      var metrics = reporter.getMetricsByGroup(groups.iterator().next());
      assertEquals(retracting ? 6 : 3, ((Counter) metrics.get("topn.invalidTopSize")).getCount());
      assertEquals(input.size(), ((Counter) metrics.get("numRecordsIn")).getCount());
      assertTrue(((Counter) metrics.get("numRecordsOut")).getCount() > 0);
    } finally {
      cluster.after();
    }
  }

  private static String query(String bound, boolean rank) {
    return "SELECT k, score, bound"
        + (rank ? ", rn" : "")
        + " FROM (SELECT k, score, bound, "
        + bound
        + " AS rank_end,"
        + " ROW_NUMBER() OVER (PARTITION BY k ORDER BY score ASC NULLS FIRST) AS rn FROM src)"
        + " WHERE rn <= rank_end";
  }

  private static TableEnvironment environment(boolean miniBatch, boolean retracting) {
    List<Row> rows = new ArrayList<>();
    long key = 0;
    for (long first : new long[] {Long.MIN_VALUE, -2, 0, 1, 2, 200, Long.MAX_VALUE}) {
      rows.add(Row.of(key, 50L, first));
      for (int i = 0; i < 230; i++) {
        Long score = i % 37 == 0 ? null : (i < 110 ? 1000L + i : 1000L - i);
        rows.add(Row.of(key, score, i % 3 == 0 ? first : (long) (i % 5)));
      }
      if (retracting) {
        List<Row> removed = new ArrayList<>();
        for (int i = rows.size() - 231; i < rows.size(); i += 13) {
          Row copy = Row.copy(rows.get(i));
          copy.setKind(RowKind.DELETE);
          removed.add(copy);
        }
        rows.addAll(removed);
      }
      key++;
    }
    return environment(
        StreamExecutionEnvironment.getExecutionEnvironment(), miniBatch, rows, retracting);
  }

  private static TableEnvironment environment(
      StreamExecutionEnvironment env, boolean miniBatch, List<Row> rows, boolean retracting) {
    env.setParallelism(1);
    var table = StreamTableEnvironment.create(env);
    if (miniBatch) {
      table.getConfig().set("table.exec.mini-batch.enabled", "true");
      table.getConfig().set("table.exec.mini-batch.allow-latency", "1 s");
      table.getConfig().set("table.exec.mini-batch.size", "37");
    }
    var source =
        fromData(
            env,
            rows,
            Types.ROW_NAMED(
                new String[] {"k", "score", "bound"}, Types.LONG, Types.LONG, Types.LONG));
    var schema =
        Schema.newBuilder()
            .column("k", DataTypes.BIGINT().notNull())
            .column("score", DataTypes.BIGINT())
            .column("bound", DataTypes.BIGINT().notNull())
            .build();
    table.createTemporaryView(
        "src",
        retracting
            ? table.fromChangelogStream(source, schema)
            : table.fromDataStream(source, schema));
    return table;
  }
}
