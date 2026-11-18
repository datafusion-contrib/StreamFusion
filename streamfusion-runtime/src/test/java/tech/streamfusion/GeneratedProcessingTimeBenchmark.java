package tech.streamfusion;

import static org.apache.flink.table.api.DataTypes.*;
import static org.junit.jupiter.api.Assertions.*;

import java.time.Instant;
import java.util.Arrays;
import java.util.Locale;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.runtime.typeutils.ExternalTypeInfo;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import tech.streamfusion.planner.NativePlanner;

@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class GeneratedProcessingTimeBenchmark {
  private static final Instant FIXED = Instant.parse("1969-12-31T23:59:59.999234567Z");
  private static final long ROWS = Long.getLong("proctime.rows", 1_000_000L);
  private static final int WARMUP = Integer.getInteger("proctime.warmup", 2);
  private static final int RUNS = Integer.getInteger("proctime.runs", 3);
  private static final boolean EXPECT_NATIVE =
      Boolean.parseBoolean(System.getProperty("proctime.expectNative", "true"));

  @Test
  void completeJobsKeepBothTransposesAndReadTheTaskClock() throws Exception {
    assertTrue(ROWS > 0 && WARMUP >= 1 && RUNS >= 3);
    for (boolean filter : new boolean[] {false, true}) {
      if (Boolean.getBoolean("proctime.filterOnly") && !filter) continue;
      String selection = "COALESCE(ts, CAST(pt AS TIMESTAMP_LTZ(9)))";
      String query =
          "INSERT INTO sink SELECT id, "
              + selection
              + ", CASE WHEN ts IS NOT NULL THEN ts ELSE CAST(pt AS TIMESTAMP_LTZ(9)) END FROM src"
              + (filter ? " WHERE " + selection + " > TO_TIMESTAMP_LTZ(0,3)" : "");
      double[][] times = new double[2][RUNS];
      try (var metrics = new RecoveryExecutionMetrics()) {
        for (int trial = 0; trial < WARMUP + RUNS; trial++) {
          for (int turn = 0; turn < 2; turn++) {
            int engine = (trial + turn) % 2;
            var table = environment(metrics.environment(new Configuration(), 1, 0));
            table.getConfig().set("streamfusion.native.enabled", Boolean.toString(engine == 1));
            var scan = NativePlanner.install(table);
            String plan = table.explainSql(query);
            if (trial == 0 && engine == 1)
              System.out.println(
                  "[generated-processing-time-plan] " + plan + scan.explainSummary());
            if (engine == 1 && EXPECT_NATIVE) {
              assertTrue(
                  plan.contains("NativeCalc")
                      && plan.contains("RowDataToArrow")
                      && plan.contains("ArrowToRowData"),
                  plan + scan.explainSummary());
            } else assertFalse(plan.contains("NativeCalc"), plan);
            System.out.println(
                "[generated-clock-trial-begin] engine="
                    + engine
                    + " filter="
                    + filter
                    + " trial="
                    + trial);
            long start = System.nanoTime();
            var result = table.executeSql(query);
            result.await();
            double seconds = (System.nanoTime() - start) / 1e9;
            System.out.println(
                "[generated-clock-trial-end] engine="
                    + engine
                    + " filter="
                    + filter
                    + " trial="
                    + trial
                    + " seconds="
                    + seconds);
            String jobId = result.getJobClient().orElseThrow().getJobID().toString();
            if (engine == 1 && EXPECT_NATIVE) {
              assertTrue(
                  scan.substitutions() > 0 && scan.fallbackReasons().isEmpty(),
                  scan.explainSummary());
              var observations =
                  metrics.nativeOperators().stream()
                      .filter(
                          row ->
                              row.get("jobId").equals(jobId)
                                  && row.get("operator")
                                      .toString()
                                      .startsWith("NativeCalc("))
                      .collect(tech.streamfusion.compat.ListCollectors.toList());
              assertEquals(1, observations.size(), "clock marker should fuse into its consumer");
              for (var observation : observations) assertEquals(ROWS, observation.get("recordsIn"));
              assertTrue(
                  scan.functionExecutions().stream()
                      .anyMatch(
                          execution ->
                              execution.contains("native=[")
                                  && execution.contains("COALESCE")
                                  && execution.contains("PROCTIME_MATERIALIZE")),
                  scan.explainSummary());
            } else {
              assertEquals(0, scan.substitutions());
              if (engine == 1)
                assertTrue(
                    scan.fallbackReasons().stream()
                        .anyMatch(reason -> reason.contains("host expression cannot be generated")),
                    scan.explainSummary());
            }
            if (trial == 0 && engine == 1)
              System.out.println(
                  "[generated-processing-time-functions] " + scan.functionExecutions());
            if (trial >= WARMUP) times[engine][trial - WARMUP] = seconds;
          }
        }
      }
      System.out.printf(
          Locale.ROOT,
          "[generated-processing-time] rows=%d filter=%s native_route=%s stock_trials=%s"
              + " enabled_trials=%s%n",
          ROWS,
          filter,
          EXPECT_NATIVE,
          Arrays.toString(times[0]),
          Arrays.toString(times[1]));
    }
  }

  private static StreamTableEnvironment environment(StreamExecutionEnvironment env) {
    env.setParallelism(1);
    var table = StreamTableEnvironment.create(env);
    var type = ROW(FIELD("id", INT()), FIELD("ts", TIMESTAMP_LTZ(9)));
    table.createTemporaryView(
        "src",
        env.fromSequence(0L, ROWS - 1)
            .map(i -> Row.of(i.intValue(), i % 2 == 0 ? FIXED : null))
            .returns(ExternalTypeInfo.<Row>of(type)),
        Schema.newBuilder().fromRowDataType(type).columnByExpression("pt", "PROCTIME()").build());
    table.executeSql(
        "CREATE TABLE sink (id INT, a TIMESTAMP_LTZ(9), b TIMESTAMP_LTZ(9)) WITH"
            + " ('connector'='blackhole')");
    return table;
  }
}
