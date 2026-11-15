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

/** Full bounded jobs retain the row source, both transposes, and rowwise blackhole sink. */
@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class SqlTrimPipelineBenchmark {
  @Test
  void boundedRowSourceToRowSink() throws Exception {
    int count = Integer.getInteger("trim.pipeline.rows", 1_000_000);
    int width = Integer.getInteger("trim.pipeline.width", 64);
    int warmups = Integer.getInteger("trim.pipeline.warmups", 2);
    int runs = Integer.getInteger("trim.pipeline.runs", 5);
    String direction = System.getProperty("trim.pipeline.direction", "BOTH");
    assertTrue(java.util.List.of("BOTH", "LEADING", "TRAILING").contains(direction));
    String payload = "X".repeat(width);
    for (int trial = 0; trial < warmups + runs; trial++) {
      for (int turn = 0; turn < 2; turn++) {
        boolean nativeRun = ((trial + turn) & 1) == 1;
        try (var metrics = new RecoveryExecutionMetrics()) {
          var env = metrics.environment(new org.apache.flink.configuration.Configuration(), 1, 0);
          var table = StreamTableEnvironment.create(env);
          var source = env.fromSequence(0, count - 1)
              .map(i -> Row.of(i % 7 == 0 ? null : " ab" + payload + "ba ",
                  i % 11 == 0 ? null : " ab"))
              .returns(Types.ROW_NAMED(new String[] {"s", "c"}, Types.STRING, Types.STRING));
          table.createTemporaryView("inputs", source,
              Schema.newBuilder().column("s", DataTypes.STRING()).column("c", DataTypes.STRING()).build());
          table.executeSql("CREATE TABLE output_rows(v STRING) WITH ('connector'='blackhole')");
          String sql = "INSERT INTO output_rows SELECT TRIM(" + direction + " c FROM s) FROM inputs";
          var scan = nativeRun ? NativePlanner.install(table) : null;
          String plan = table.explainSql(sql);
          if (nativeRun) {
            assertTrue(plan.contains("NativeCalc"), plan);
            assertTrue(plan.contains("RowDataToArrow"), plan);
            assertTrue(plan.contains("ArrowToRowData"), plan);
            assertTrue(scan.fallbackReasons().isEmpty(), scan.fallbackReasons().toString());
          }
          long start = System.nanoTime();
          table.executeSql(sql).await();
          double seconds = (System.nanoTime() - start) / 1e9;
          if (nativeRun) RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeCalc");
          System.out.printf(Locale.ROOT,
              "[sql-trim-pipeline] direction=%s engine=%s trial=%d warmup=%s rows=%d width=%d seconds=%.9f%n",
              direction, nativeRun ? "native" : "stock_previous", trial, trial < warmups, count, width, seconds);
        }
      }
    }
  }
}
