package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Locale;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import tech.streamfusion.planner.NativePlanner;
import tech.streamfusion.planner.StringClassificationBenchmarkFunctions;

/** Complete row-source jobs retain JNI, startup and both native perimeter transposes. */
@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class StringClassificationPipelineBenchmark {
  @Test
  void stringPredicateFromRowsToBlackhole() throws Exception {
    boolean previous = Boolean.getBoolean("sf.classification.previous");
    assertEquals(previous ? 17 : 6, StringClassificationBenchmarkFunctions.register()[0]);
    StringClassificationBenchmarkFunctions.close();
    int count = Integer.getInteger("classification.pipeline.rows", 1_000_000);
    int warmups = Integer.getInteger("classification.pipeline.warmups", 2);
    int runs = Integer.getInteger("classification.pipeline.runs", 5);
    for (int trial = 0; trial < warmups + runs; trial++) {
      for (int turn = 0; turn < 2; turn++) {
        boolean nativeRun = ((trial + turn) & 1) == 1;
        try (var metrics = new RecoveryExecutionMetrics()) {
          var env = metrics.environment(new org.apache.flink.configuration.Configuration(), 1, 0);
          var table = StreamTableEnvironment.create(env);
          var source = env.fromSequence(0, count - 1).map(row -> Row.of(row % 7 == 0 ? null
              : StringClassificationBenchmarkFunctions.value("unicode", 16, row.intValue())))
              .returns(Types.ROW_NAMED(new String[] {"s"}, Types.STRING));
          table.createTemporaryView("inputs", source,
              Schema.newBuilder().column("s", DataTypes.STRING()).build());
          table.executeSql("CREATE TABLE output_rows(v BOOLEAN NOT NULL) WITH ('connector'='blackhole')");
          String sql = "INSERT INTO output_rows SELECT IS_ALPHA(s) FROM inputs";
          var scan = nativeRun ? NativePlanner.install(table) : null;
          String plan = table.explainSql(sql);
          if (nativeRun) {
            assertTrue(plan.contains("NativeCalc"), plan);
            assertTrue(plan.contains("RowDataToArrow"), plan);
            assertTrue(plan.contains("ArrowToRowData"), plan);
            assertTrue(scan.fallbackReasons().isEmpty(), scan.fallbackReasons().toString());
            assertFalse(scan.functionExecutions().isEmpty());
            assertTrue(scan.functionExecutions().stream()
                .allMatch(route -> previous != route.contains("jvm=[]")),
                scan.functionExecutions().toString());
          }
          long start = System.nanoTime();
          table.executeSql(sql).await();
          double seconds = (System.nanoTime() - start) / 1e9;
          if (nativeRun) RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeCalc");
          System.out.printf(Locale.ROOT,
              "[classification-pipeline] engine=%s trial=%d warmup=%s rows=%d seconds=%.9f%n",
              nativeRun ? previous ? "previous" : "native" : "stock", trial, trial < warmups, count, seconds);
        }
      }
    }
  }
}
