package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Locale;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import tech.streamfusion.planner.NativePlanner;

/**
 * Standalone integer rounding with the row source, both transposes and the rowwise blackhole sink.
 */
@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class IntegralRoundingPipelineBenchmark {
  @Test
  void boundedRowSourceToRowSink() throws Exception {
    int count = Integer.getInteger("rounding.pipeline.rows", 1_000_000);
    int warmups = Integer.getInteger("rounding.pipeline.warmups", 2);
    int runs = Integer.getInteger("rounding.pipeline.runs", 5);
    String engine = System.getProperty("rounding.pipeline.engine", "native");
    boolean nativeRun = !engine.equals("flink");
    String function = System.getProperty("rounding.pipeline.function", "FLOOR");
    assertTrue(java.util.List.of("FLOOR", "CEIL", "CEILING").contains(function));
    String routeName = function.equals("CEILING") ? "CEIL" : function;
    for (int trial = 0; trial < warmups + runs; trial++) {
      try (var metrics = new RecoveryExecutionMetrics()) {
        var env = metrics.environment(new org.apache.flink.configuration.Configuration(), 1, 0);
        var table = StreamTableEnvironment.create(env);
        var source =
            env.fromSequence(0, count - 1)
                .map(
                    i ->
                        Row.of(
                            i % 7 == 0
                                ? null
                                : i % 17 == 0
                                    ? Integer.MIN_VALUE
                                    : (int) ((i % 2 == 0 ? -1 : 1) * i * 7919)))
                .returns(Types.ROW_NAMED(new String[] {"v"}, Types.INT));
        table.createTemporaryView(
            "inputs", source, Schema.newBuilder().column("v", DataTypes.INT()).build());
        table.executeSql("CREATE TABLE output_rows(v INT) WITH ('connector'='blackhole')");
        String sql = "INSERT INTO output_rows SELECT " + function + "(v) FROM inputs";
        if (nativeRun) {
          var scan = NativePlanner.install(table);
          table.explainSql(sql);
          assertTrue(
              scan.functionExecutions().stream()
                  .anyMatch(
                      route ->
                          route.contains(routeName)
                              && (engine.equals("previous")
                                  ? route.contains("jvm=[" + routeName)
                                  : route.contains("jvm=[]"))),
              scan.functionExecutions().toString());
        }
        String plan = table.explainSql(sql);
        if (nativeRun) {
          assertTrue(plan.contains("NativeCalc"), plan);
          assertTrue(plan.contains("RowDataToArrow"), plan);
          assertTrue(plan.contains("ArrowToRowData"), plan);
        }
        long start = System.nanoTime();
        table.executeSql(sql).await();
        double elapsed = (System.nanoTime() - start) / 1e9;
        if (nativeRun)
          RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeCalc");
        System.out.printf(
            Locale.ROOT,
            "[integral-rounding-pipeline] function=%s engine=%s trial=%d warmup=%s rows=%d"
                + " seconds=%.9f%n",
            function,
            engine,
            trial,
            trial < warmups,
            count,
            elapsed);
      }
    }
  }
}
