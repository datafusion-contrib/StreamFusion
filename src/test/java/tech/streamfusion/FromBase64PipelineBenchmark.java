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

/** Standalone FROM_BASE64 with the row source, both transposes and a rowwise sink. */
@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class FromBase64PipelineBenchmark {
  @Test
  void boundedRowSourceToRowSink() throws Exception {
    int count = Integer.getInteger("from.base64.pipeline.rows", 1_000_000);
    int warmups = Integer.getInteger("from.base64.pipeline.warmups", 2);
    int runs = Integer.getInteger("from.base64.pipeline.runs", 5);
    String engine = System.getProperty("from.base64.pipeline.engine", "native");
    boolean nativeRun = !engine.equals("flink");
    String function = "FROM_BASE64";
    String routeName = "FROM_BASE64";
    int width = Integer.getInteger("from.base64.pipeline.width", 16);
    String type = "STRING";
    for (int trial = 0; trial < warmups + runs; trial++) {
      try (var metrics = new RecoveryExecutionMetrics()) {
        var env = metrics.environment(new org.apache.flink.configuration.Configuration(), 1, 0);
        var table = StreamTableEnvironment.create(env);
        var source =
            env.fromSequence(0, count - 1)
                .map(
                    i -> {
                      byte[] bytes = new byte[width];
                      for (int j = 0; j < width; j++) bytes[j] = (byte) (i * 131 + j * 127);
                      return Row.of(
                          i % 7 == 0 ? null : java.util.Base64.getEncoder().encodeToString(bytes));
                    })
                .returns(Types.ROW_NAMED(new String[] {"v"}, Types.STRING));
        table.createTemporaryView(
            "inputs", source, Schema.newBuilder().column("v", DataTypes.STRING()).build());
        table.executeSql("CREATE TABLE output_rows(v STRING) WITH ('connector'='blackhole')");
        String expression = "FROM_BASE64(v)";
        String sql = "INSERT INTO output_rows SELECT " + expression + " FROM inputs";
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
            "[from-base64-pipeline] function=%s type=%s engine=%s trial=%d warmup=%s"
                + " rows=%d seconds=%.9f%n",
            function,
            type,
            engine,
            trial,
            trial < warmups,
            count,
            elapsed);
      }
    }
  }
}
