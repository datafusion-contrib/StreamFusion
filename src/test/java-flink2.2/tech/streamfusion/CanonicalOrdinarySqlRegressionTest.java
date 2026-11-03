package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.runtime.testutils.InMemoryReporter;
import org.apache.flink.streaming.util.TestStreamEnvironment;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/** Exercises the deployed ordinary SQL path, including families awaiting canonical binding. */
class CanonicalOrdinarySqlRegressionTest {
  static Stream<Arguments> queries() {
    return Stream.of(
        Arguments.of("GroupAgg", "SELECT g,SUM(v),COUNT(*) FROM src GROUP BY g", false),
        Arguments.of("UpdatingJoin", "SELECT a.k,a.v,b.v FROM src a JOIN src b ON a.k=b.k", true),
        Arguments.of(
            "TopN",
            "SELECT g,k,v FROM (SELECT g,k,v,ROW_NUMBER() OVER"
                + " (PARTITION BY g ORDER BY v DESC,k DESC) rn FROM src) WHERE rn<=2",
            false),
        Arguments.of(
            "Deduplicate",
            "SELECT g,k,v FROM (SELECT g,k,v,ROW_NUMBER() OVER"
                + " (PARTITION BY g ORDER BY rt ASC) rn FROM src) WHERE rn=1",
            true),
        Arguments.of(
            "OverAgg", "SELECT g,v,SUM(v) OVER (PARTITION BY g ORDER BY rt) FROM src", false),
        Arguments.of(
            "WindowAgg",
            "SELECT g,window_start,window_end,SUM(v) FROM TABLE(TUMBLE(TABLE src,"
                + "DESCRIPTOR(rt),INTERVAL '4' SECOND)) GROUP BY g,window_start,window_end",
            false),
        Arguments.of("Unnest", "SELECT k,x FROM src CROSS JOIN UNNEST(ARRAY[k,v]) AS u(x)", false),
        Arguments.of("Calc", "SELECT k,v*2 FROM src WHERE MOD(k,2)=0", true));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("queries")
  void ordinarySqlKeepsNativeExecutionAndHostResults(
      String family, String query, boolean bindingSupported) throws Exception {
    var stock = execute(false, family, query, bindingSupported);
    assertFalse(stock.isEmpty());
    assertEquals(stock, execute(true, family, query, bindingSupported));
  }

  private static Map<List<Object>, Long> execute(
      boolean nativeRun, String family, String query, boolean bindingSupported) throws Exception {
    var reporter = InMemoryReporter.createWithRetainedMetrics();
    var config = reporter.addToConfiguration(new Configuration());
    config.setString("rest.port", "0");
    config.setString("restart-strategy.type", "none");
    try (var cluster =
        new MiniCluster(
            new MiniClusterConfiguration.Builder()
                .setConfiguration(config)
                .setNumTaskManagers(1)
                .setNumSlotsPerTaskManager(2)
                .build())) {
      cluster.start();
      var env = new TestStreamEnvironment(cluster, config, 1, List.of(), List.of());
      env.setParallelism(1);
      var table = NativePlannerTestEnvironment.create(env);
      table.getConfig().set("streamfusion.native.enabled", Boolean.toString(nativeRun));
      table.getConfig().set("table.optimizer.agg-phase-strategy", "ONE_PHASE");
      table.executeSql(
          "CREATE TABLE input_table(k BIGINT,v BIGINT,"
              + "rt AS TIMESTAMPADD(SECOND,CAST(k AS INT),TIMESTAMP '2026-01-01 00:00:00'),"
              + "WATERMARK FOR rt AS rt) WITH ('connector'='datagen','number-of-rows'='12',"
              + "'fields.k.kind'='sequence','fields.k.start'='0','fields.k.end'='11',"
              + "'fields.v.kind'='sequence','fields.v.start'='10','fields.v.end'='21')");
      table.executeSql("CREATE TEMPORARY VIEW src AS SELECT *,MOD(k,3) AS g FROM input_table");
      var counts = new HashMap<List<Object>, Long>();
      var result = table.executeSql(query);
      try (var rows = result.collect()) {
        while (rows.hasNext()) {
          var row = rows.next();
          var fields = new ArrayList<>();
          for (int i = 0; i < row.getArity(); i++) fields.add(row.getField(i));
          long delta =
              row.getKind() == RowKind.UPDATE_BEFORE || row.getKind() == RowKind.DELETE ? -1 : 1;
          counts.merge(fields, delta, Long::sum);
          if (counts.get(fields) == 0) counts.remove(fields);
        }
      }
      var execution =
          cluster.getExecutionGraph(result.getJobClient().orElseThrow().getJobID()).get();
      String actualPlan = String.valueOf(execution.getStreamGraphJson());
      var inputs = new HashMap<String, Long>();
      reporter
          .getMetricsByGroup()
          .forEach(
              (group, metrics) -> {
                var variables = group.getAllVariables();
                String operator = variables.getOrDefault("<operator_name>", "");
                if (result
                        .getJobClient()
                        .orElseThrow()
                        .getJobID()
                        .toString()
                        .equals(variables.get("<job_id>"))
                    && operator.startsWith("Native")
                    && metrics.get("numRecordsIn") instanceof Counter)
                  inputs.merge(
                      operator, ((Counter) metrics.get("numRecordsIn")).getCount(), Long::sum);
              });
      if (nativeRun) {
        assertTrue(
            inputs.entrySet().stream()
                .anyMatch(entry -> entry.getKey().contains(family) && entry.getValue() > 0),
            () -> "No positive native input for " + family + ": " + inputs + "\n" + actualPlan);
      } else assertTrue(inputs.isEmpty(), inputs.toString());
      if (nativeRun && (family.equals("UpdatingJoin") || family.equals("Deduplicate"))) {
        assertFalse(
            table
                .getConfig()
                .getConfiguration()
                .getString("streamfusion.canonical.state-contracts", "")
                .isEmpty());
        assertTrue(
            Boolean.parseBoolean(
                table
                    .getConfig()
                    .getConfiguration()
                    .getString("streamfusion.canonical.metadata-projection-active", "false")));
      }
      if (!bindingSupported) {
        assertEquals(
            "",
            table
                .getConfig()
                .getConfiguration()
                .getString("streamfusion.canonical.state-contracts", ""));
        assertFalse(
            Boolean.parseBoolean(
                table
                    .getConfig()
                    .getConfiguration()
                    .getString("streamfusion.canonical.metadata-projection-active", "false")));
      }
      assertTrue(counts.values().stream().allMatch(count -> count > 0), counts.toString());
      return counts;
    } finally {
      reporter.close();
    }
  }
}
